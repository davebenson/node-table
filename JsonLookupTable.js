
// Options:
//    class: .../    
//    directory: ...
//    compare: function(a,b) -> {-1,0,1}
//    merge: function([docs]) -> [docs] (latter possibly empty)
//    
// database_callback(err, db) will be invoked exactly once
//
var fs = require("fs");
var fs_ext = require("fs-ext");
var assert = require("assert");
var util = require("util");

exports.create_database = function(options, database_callback) {
  var c = options.class ? options.class : module.exports.DocDatabaseSimple;
  return new c(options);
}

var DEFAULT_JOURNAL_MAX_ENTRIES = 512;

/// uh what's the right idiom here?
var exports = module.exports;

var newline_buffer = new Buffer("\n");

function _JsonLookupTableSimple_MergeInput() {
  this.input_offset = 0;
  this.input_file = null;
  this.input_pending = false;
  this.input_buffer = new Buffer();
  this.input_peeked_json = null;
  this.input_peeked_json_size = 0;
}
_JsonLookupTableSimple_MergeInput.prototype.toJSON = function() {
  return {
    input_id: this.input_file.id,
    input_offset: this.input_offset
  }
};
_JsonLookupTableSimple_MergeInput.prototype.remove_first = function(n)
{
  for (var i = 0; i < n; i++)
    this.input_offset += this.pending[i][1];
  this.pending.splice(0, n);
  if (this.length === 0
   && this.input_offset >= this.file.size_bytes)
    this.eof = true;
};
function _JsonLookupTableSimple_MergeJob() {
  this.newer = new _JsonLookupTableSimple_MergeInput();
  this.older = new _JsonLookupTableSimple_MergeInput();

  this.output_file = new _JsonLookupTableSimple_File();
  this.output_offset = 0;
}
_JsonLookupTableSimple_MergeJob.prototype.toJSON = function() {
  return {
    output_id: this.output_file.id,
    output_offset: this.output_offset,
    output_n_entries: this.output_file.size_entries,
    older: this.older.toJSON(),
    newer: this.newer.toJSON(),
  };
};

function _JsonLookupTableSimple_File() {
  this.fd = -1;
  this.id = "";
  this.start_input_entry = 0;
  this.n_input_entries = 0;
  this.size_entries = 0;
  this.size_bytes = 0;
  this.newer = null;
  this.older = null;
  this.merge_job = null;
  this.ref_count = 1;
}
_JsonLookupTableSimple_File.prototype.toJSON = function() {
  return {
    id: this.id,
    start_input_entry: this.start_input_entry,
    n_input_entries: this.n_input_entries,
    size_bytes: this.size_bytes,
    size_entries: this.size_entries,
  };
};

function _JsonLookupTableSimple_SortMergeInMemory(compare, merge) {
  this.lol = [];
  this.compare = compare;
  this.merge = merge;
}
_JsonLookupTableSimple_SortMergeInMemory.prototype.add = function(v) {
  var list = [v];
  for (var i = 0; i < this.lol.length; i++) {
    if (this.lol[i] === null) {
      this.lol[i] = list;
      return;
    } else {
      list = this._merge_lists(this.lol[i], list);
      this.lol[i] = null;
    }
  }
  this.lol.push(list);
}
_JsonLookupTableSimple_SortMergeInMemory.prototype._merge_lists =
function(older, newer) {
  var rv = [];
  var oi = 0, ni = 0;
  while (oi < older.length && ni < newer.length) {
    var cmp = this.compare(older[oi], newer[ni]);
    if (cmp < 0)
      rv.push(older[oi++]);
    else if (cmp > 0)
      rv.push(newer[ni++]);
    else {
      var a = older[oi++];
      var b = newer[ni++];
      rv.push(this.merge(a,b));
    }
  }
  for (; oi < older.length; oi++)
    rv.push(older[oi]);
  for (; ni < newer.length; ni++)
    rv.push(newer[ni]);
  return rv;
};

_JsonLookupTableSimple_SortMergeInMemory.prototype.reset = function()
{
  this.lol = [];
};
_JsonLookupTableSimple_SortMergeInMemory.prototype.get_list = function()
{
  var rv = null;
  for (var i = 0; i < lol.length; i++)
    if (lol[i] !== null) {
      if (rv === null)
        rv = lol[i];
      else
        rv = this._merge_lists(lol[i], rv);
    }
  return rv ? rv : [];
};
_JsonLookupTableSimple_SortMergeInMemory.prototype.get = function(curried_comparator)
{
  var rv = null;
  for (var i = 0; i < this.lol.length; i++) {
    if (this.lol[i]) {
      var sub_rv = this._array_lookup(curried_comparator, this.lol[i]);
      if (sub_rv) {
        if (rv === null)
	  rv = sub_rv;
        else
          rv = this.merge(sub_rv, rv); 
      }
    }
  }
  return rv;
};
_JsonLookupTableSimple_SortMergeInMemory.prototype._array_lookup = function(curried_comparator, array)
{
  var start = 0, n = array.length;
  while (n > 0) {
    var mid = start + Math.floor(n / 2);
    var rv = curried_comparator(array[mid]);
    if (rv < 0)
      n = mid - start;
    else if (rv > 0) {
      var end = start + n;
      start = mid + 1;
      n = end - start;
    } else
      return array[mid];
  }
  return null;
};

exports.JsonLookupTableSimple = function(options) {
  this.n_latest = 0;
  this.sort_merged = [];
  this.dir = options.dir;
  assert.equal(typeof(this.dir), 'string');
  this.n_input_entries = 0;
  this.journal_write_pending = false;
  this.journal_write_pending_buffers = [];
  this.journal_write_pending_callbacks = [];
  this.journal_start_input_entry = 0;
  this.journal_max_entries = options.journal_max_entries || DEFAULT_JOURNAL_MAX_ENTRIES;
  //this.writing_level0_block = false;
  //this.pending_writing_level0_files = [];
  this.compare = options.compare;
  this.merge = options.merge;
  this.sort_merger = new _JsonLookupTableSimple_SortMergeInMemory(options.compare, options.merge);
  this.next_file_id = 1;
  this.oldest_file = null;
  this.newest_file = null;
  this.dir_fd = -1;
  this.journal_fd = -1;
  this.journal_offset = 0;

  try {
    this.dir_fd = fs.openSync(this.dir, "r");
  } catch (e) {
    if (e.code !== 'ENOENT') {
      throw(e);
    }
    fs.mkdirSync(this.dir);
    this.dir_fd = fs.openSync(this.dir, "r");
  }
  fs_ext.flockSync(this.dir_fd, "exnb");

  var cp_json;
  var journal_lines = [];
  try {
    var cp_data = fs.readFileSync(this.dir + "/JOURNAL");
    assert(cp_data[cp_data.length - 1] === 0x0a);
    var lines = cp_data.slice(0,cp_data.length - 1).toString().split("\n");
    cp_json = JSON.parse(lines[0]);
    for (var i = 1; i < lines.length; i++)
      journal_lines.push(JSON.parse(lines[i]));
  } catch (e) {
    if (e.code !== 'ENOENT') {
      throw(e);
    }

    // check that there's no data-like stuff
    var files = fs.readdirSync(this.dir);
    for (var fi = 0; fi < files.length; fi++) {
      throw(new Error("directory contained " + files[fi] + " (out of " + files.length + " files), but no JOURNAL"));
    }

    // Create empty checkpoint.
    cp_json = {files: [],
               merge_jobs: [],
               next_file_id: 1};
    var cp_data = new Buffer(JSON.stringify(cp_json) + "\n");
    fs.writeFileSync(this.dir + "/JOURNAL", cp_data);
  }

  // Open all files and merge jobs, restart any journalling -> level_0 writes.

  this.next_file_id = cp_json.next_file_id;
  this.journal_start_input_entry = cp_json.n_input_entries;
  this.n_input_entries = cp_json.n_input_entries;

  // open files; create files list
  var last_f = null;
  for (var i = 0; i < cp_json.files.length; i++) {
    var f = new _JsonLookupTableSimple_File();
    var info = cp_json.files[i];
    f.id = info.id;
    f.start_input_entry = info.start_input_entry;
    f.n_input_entries = info.n_input_entries;
    f.size_bytes = info.size_bytes;
    f.size_entries = info.size_entries;
    f.newer = last_f;
    if (last_f)
      last_f.older = f;
    else
      this.newest_file = f;
    this.oldest_file = f;
    last_f = f;

    f.fd = fs.openSync(this.dir + "/F." + f.id, "r");
  }

  // open merge jobs
  for (var i = 0; i < cp_json.merge_jobs.length; i++) {
    var mj = cp_json.merge_jobs[i];
    for (var f = this.newest_file; f !== null; f = f.older) {
      if (mj.newer.input_id === f.id) {
        assert(f.older.id === mj.older_input_id);
        var merge_job = new _JsonLookupTableSimple_MergeJob();
        merge_job.output_file.id = mj.output_file.id;
        merge_job.older.input_offset = mj.older.input_offset;
        merge_job.newer.input_offset = mj.newer.input_offset;
        merge_job.output_offset = mj.output_offset;
        merge_job.output_file.size_bytes = mj.output_offset;
        merge_job.output_file.size_entries = mj.output_n_entries;
        merge_job.output_file.fd = fs.openSync(this.dir + "/F." + mj.output_offset, "r+");
        merge_job.newer.input_file = f;
        merge_job.older.input_file = f.older;
        fs.ftruncateSync(merge_job.output_file.fd, merge_job.output_offset);
        f.merge_job = n.older.merge_job = merge_job;

        this._start_merge_job(merge_job);

        break;
      }
    }
    if (f === null)
      console.log("WARNING: file " + mj.newer.input_id + " not found");
  }

  for (var i = 0; i < journal_lines.length; i++) {
    for (var j = 0; j < journal_lines[i].length; j++) {
      this.sort_merger.add(journal_lines[i][j]);
    }
  }
};

function write_sync_n(fd, data, position)
{
  var at = 0, rem = data.length, pos = position;
  while (rem > 0) {
    var n = fs.writeSync(fd, data, at, rem, pos);
    at += n;
    rem -= n;
    pos += n;
  }
}

exports.JsonLookupTableSimple.prototype.add = function(doc, callback)
{
  var self = this;
  var pot_at = 0;
  var level0_docs = null;
  var cur_docs;
  if (Array.isArray(doc)) {
    cur_docs = doc;
  } else {
    cur_docs = [doc];
  }

  this.n_input_entries += cur_docs.length;
  this.n_input_transactions += 1;

  if (this.journal_num_entries + cur_docs.length > this.journal_max_entries) {
    this._flush_journal();
  }
  for (var i = 0; i < cur_docs.length; i++)
    this.sort_merger.add(cur_docs[i]);
  this.journal_num_entries += cur_docs.length;

  var pending_requests = 1;

  // write to journal
  var buf = new Buffer(JSON.stringify(doc) + "\n");
  ++pending_requests;
  if (this.journal_write_pending) {
    this.journal_write_pending_buffers.push(buf);
    this.journal_write_pending_callbacks.push(decr_pending_requests);
  } else {
    this.journal_write_pending = true;
    var this_err = null;
    if (this.journal_fd === -1) {
      assert.equal(this.journal_offset, 0);
      this.journal_fd = fs.openSync(this.dir + "/JOURNAL", "w");
    }
    writen_buffer(this.journal_fd, buf, 0, buf.length, this.journal_offset, function(err) {
      if (err) {
        var bufs = self.journal_write_pending_buffers;
        var cbs = self.journal_write_pending_callbacks;
        self.journal_write_pending_buffers = [];
        self.journal_write_pending_callbacks = [];
        for (var i = 0; i < cbs.length; i++)
          cbs[i](err);
        this_err = err;
        decr_pending_requests();
      } else if (self.journal_write_pending_buffers.length > 0) {
        self._flush_pending_write_buffers(function(err) {
          if (err)
	    console.log("_flush_pending_write_buffers failed: " + err);
          decr_pending_requests();
        });
      } else {
        self.journal_write_pending = false;
        decr_pending_requests();
      }
    });
  }

  // remove re-entrance guard
  decr_pending_requests();

  function decr_pending_requests() {
    if (--pending_requests == 0) {
      if (this_err) {
        callback(this_err);
      } else {
        callback(null);
      }
    }
  }
};

exports.JsonLookupTableSimple.prototype._flush_journal = function()
{
  var file_id = this._allocate_file_id();
  //this.pending_writing_level0_files.push(pending_level0_info);
  var level0_data = this._sorted_json_to_binary(level0_docs);
  console.log("writing level0 data to " + file_id);
  fs.writeFileSync(this.dir + "/F." + file_id, level0_data);
  this.journal_fd = fs.openSync(this.dir + "/JOURNAL.tmp", "w");
  var cp_json = this._create_checkpoint_json();
  var cp_data = new Buffer(JSON.stringify(cp_json) + "\n");
  write_sync_n(this.journal_fd, cp_data, 0);
  fs.renameSync(this.dir + "/JOURNAL.tmp", this.dir + "/JOURNAL");

  var f = new _JsonLookupTableSimple_File();
  f.id = file_id;
  f.fd = fs.openSync(this.dir + "/F." + file_id, "r");
  f.size_bytes = level0_data.length;
  f.size_entries = level0_docs.length;
  f.start_input_entry = this.journal_start_input_entry;
  f.n_input_entries = this.journal_num_entries;
  f.older = this.newest_file;
  if (f.older)
    f.older.newer = f;
  else
    this.oldest_file = f;
  this.newest_file = f;
  this.journal_start_input_entry = this.n_input_entries;
  this._maybe_start_merge_jobs();

  this.sort_merger.reset();
};

exports.JsonLookupTableSimple.prototype._flush_pending_write_buffers = function(callback) {
  var self = this;
  assert(this.journal_write_pending_buffers.length > 0);
  assert(this.journal_write_pending);
  var cbs = this.journal_write_pending_callbacks;
  var buf = Buffer.concat(this.journal_write_pending_buffers);
  this.journal_write_pending_callbacks = [];
  this.journal_write_pending_buffers = [];
  this.journal_stream.write(buf, function(err) {
    for (var i = 0; i < cbs.length; i++)
      cbs[i](err);
    if (err) {
      this.journal_write_pending = false;
      callback(err);
    } else if (self.journal_write_pending_callbacks.length > 0)
      self._flush_pending_write_buffers(callback);
    else {
      this.journal_write_pending = false;
      callback(null);
    }
  });
};

exports.JsonLookupTableSimple.prototype._maybe_start_merge_jobs = function() {
  for (var f = this.newest_file; f !== null; f = f.older) {
    if (f.merge_job === null &&
        f.older !== null &&
        f.older.merge_job === null &&
        f.size_bytes > f.older.size_bytes * 0.35)
    {
      var merge_job = new _JsonLookupTableSimple_MergeJob();
      merge_job.newer.input_file = f;
      merge_job.older.input_file = f.older;
      f.merge_job = f.older.merge_job = merge_job;

      merge_job.output_file.id = this._allocate_file_id();
      merge_job.output_file.fd = fs.openSync(this.dir + "/F." + merge_job.output_file.id, "w");

      this._start_merge_job(merge_job);
    }
  }
};

exports.JsonLookupTableSimple.prototype._start_merge_job = function(merge_job)
{
  this._maybe_start_merge_input_read(merge_job, merge_job.newer);
  this._maybe_start_merge_input_read(merge_job, merge_job.older);
};

exports.JsonLookupTableSimple.prototype._maybe_start_merge_input_read = function(merge_job, merge_input)
{
  if (merge_input.eof)
    return;
  if (merge_input.input_offset >= merge_input.file.size_bytes) {
    merge_input.eof = true;
    _try_making_merge_output(this, merge_job);
    return;
  }

  if (merge_input.peekable.length > 16)
    return;

  if (merge_input.input_pending)
    return;

  var read_end = merge_input.input_offset + merge_input.buffer_available;
  var til_end = merge_input.file.size_bytes - read_end;
  if (til_end > 0) {
    var to_read = til_end < 4096 ? til_end : 4096;

    // ensure merge_input.buffer is large enough
    if (merge_input.buffer.length < merge_input.buffer_available + to_read) {
      var new_buf = new Buffer(merge_input.buffer_available + to_read);
      merge_input.buffer.copy(new_buf);
      merge_input.buffer = new_buf;
    }

    merge_input.input_pending = true;
    fs.read(merge_input.file.fd, merge_input.buffer, merge_input.buffer_available,
            read_end, function(err, bytesRead) {
              merge_input.buffer_available += bytesRead;

              // parse out any lines; add JSON/length pair to 'pending'.
              var nl_index;
              var at = 0;
              var n_lines = 0;
              while ((nl_index=find_newline(merge_input.buffer, at, merge_input.buffer_available)) != -1) {
                var str = merge_input.buffer.slice(at, nl_index).toString();
                merge_input.pending.push([JSON.parse(str), nl_index + 1 - at]);
                at = nl_index + 1;
                n_lines++;
              }

              merge_input.input_pending = false;
              if (n_lines === 0) {
                this._maybe_start_merge_input_read(merge_job, merge_input);
              } else {
                _try_making_merge_output(this, merge_job);
              }
            });
  }
};

exports.JsonLookupTableSimple.prototype._try_making_merge_output = function(merge_job)
{
  if (merge_job.newer.eof && merge_job.older.eof) {
    this._finish_merge_job(merge_job);
    return;
  } else {
    var ready = true;
    if (merge_job.newer.peekable.length === 0 && !merge_job.newer.eof) {
      this._maybe_start_merge_input_read(merge_job, newer);
      ready = false;
    }
    if (merge_job.older.peekable.length === 0 && !merge_job.older.eof) {
      this._maybe_start_merge_input_read(merge_job, older);
      ready = false;
    }
    var outputs = [];
    var ni = 0, oi = 0;
    while (ni < merge_job.newer.peekable.length && oi < merge_job.older.peekable.length) {
      var cmp = this.compare(merge_job.newer.pending[ni][0], merge_job.older.pending[oi][0]);
      if (cmp < 0) {
        outputs.push(merge_job.newer.pending[ni++]);
      } else if (cmp > 0) {
        outputs.push(merge_job.older.pending[oi++]);
      } else {
        var o = merge_job.older.pending[oi++];
        var n = merge_job.older.pending[ni++];
        outputs.push(this.merge(o,n));
      }
    }
    while (ni < merge_job.newer.peekable.length && merge_job.older.eof) {
      outputs.push(merge_job.newer.pending[ni++]);
    }
    while (oi < merge_job.older.peekable.length && merge_job.newer.eof) {
      outputs.push(merge_job.older.pending[oi++]);
    }

    if (oi > 0) {
      merge_job.older.remove_first(oi);
    }
    if (ni > 0) {
      merge_job.newer.remove_first(ni);
    }
      
    if (outputs.length > 0) {
      var output_buffers = [];
      for (var i = 0; i < outputs.length; i++) {
        var b = new Buffer(JSON.stringify(outputs[i]));
        output_buffers.push(b);
        output_buffers.push(newline_buffer);
      }
      var total_output = Buffer.concat(output_buffers);
      write_sync_n(merge_job.output_file.fd, total_output, merge_job.output_offset);
      merge_job.output_offset += total_output.length;
      merge_job.output_file.size_bytes += total_output.length;
      merge_job.output_file.size_entries += outputs.length;
    }
  }
};

exports.JsonLookupTableSimple.prototype._finish_merge_job = function(merge_job)
{
  var output_file = merge_job.output_file;

  //TODO convert fd to read-only

  // replace the two input files with the single new output File
  output_file.older = merge_job.older.file.older;
  if (output_file.older)
    output_file.older.newer = output_file;
  else
    this.oldest_file = output_file;
  output_file.newer = merge_job.newer.file.newer;
  if (output_file.newer)
    output_file.newer.older = output_file;
  else
    this.newest_file = output_file;

  // Delete these objects if ready.
  merge_job.older.reduce_ref_count();
  merge_job.newer.reduce_ref_count();
};

exports.JsonLookupTableSimple.prototype._create_file_from_level0_info = function(info, callback)
{
  var filename = this._file_id_to_filename(info.file_id);
  fs.writeFile(filename,
               info.data,
               function(err) {
                 if (err) {
                   callback(err);
                 } else {
                   var file = new _JsonLookupTableSimple_File();
                   file.id = info.file_id;
                   fs.open(filename, "r", function(err, fd) {
                     callback(err);
                     if (!err)
                       file.fd = fd;
                   });
                 }
               });
};

exports.JsonLookupTableSimple.prototype._sorted_json_to_binary = function(array)
{
  var buffers = [];
  for (var i = 0; i < array.length; i++) {
    buffers.push(new Buffer(JSON.stringify(array[i])));
    buffers.push(newline_buffer);
  }
  return Buffer.concat(buffers);
};

exports.JsonLookupTableSimple.prototype._merge_lists = function(older, newer) {
  var oi = 0, ni = 0;
  var merged = [];
  while (oi < older.length && ni < newer.length) {
    var rv = this.compare(older[oi], newer[ni]);
    if (rv < 0)
      merged.push(older[oi++]);
    else if (rv > 0)
      merged.push(newer[ni++]);
    else
      merged.push(this.merge(older[oi++], newer[ni++]));
  }
  return merged;
};

exports.JsonLookupTableSimple.prototype._allocate_file_id = function()
{
  var rv = this.next_file_id.toString();
  this.next_file_id += 1;
  return rv;
};


exports.JsonLookupTableSimple.prototype._create_checkpoint_json = function()
{
  var files = [], merge_jobs = [];
  for (var f = this.newest_file; f !== null; f = f.older) {
    files.push(f.toJSON());
    if (f.merge_job && f.merge_job.newer.input_file === f)
      merge_jobs.push(f.merge_job.toJSON());
  }
  return {files: files,
          merge_jobs: merge_jobs,
          next_file_id:this.next_file_id};
};

// callback(err, result) - both == null: not found.
exports.JsonLookupTableSimple.prototype.get = function(curried_comparator, callback)
{
  var rv = this.sort_merger.get(curried_comparator);
  var self = this;

  var ignore_merge_job = false;
  var file_results = [];
  var pending = 1;
  var first_err = null;
  for (var file = this.newest_file; file !== null; file = file.older) {
    var result_index = file_results.length;
    if (file.merge_job) {
      if (ignore_merge_job) {
        ignore_merge_job = false;
	// fall-through to normal file handling.
      } else if (file.merge_job.last_output) {
        var sub_rv = curried_comparator(file.merge_job.last_output);
	if (sub_rv < 0) {
	  // search in the output
	  file_results.push(null);
	  (function(merge_job, result_index) {
	    ++pending;
	    self._do_search_file(merge_job.output_file, curried_comparator, function(err, res) {
	      file_results[result_index] = res;
	      if (err && first_err === null)
	        first_err = err;
	      decr_pending();
	    });
	  })(file.merge_job, result_index);
	  ignore_merge_job = true;
	  continue;
	} else if (sub_rv === 0) {
	  // happens to be last_output: no blocking
	  file_results.push(file.merge_job.last_output);
	  continue;
	} else {
	  // must search both inputs: fall through
	}
      }
    }

    // launch scan of file
    (function(result_index) {
      ++pending;
      self._do_search_file(file, curried_comparator, function(err, res) {
	file_results[result_index] = res;
	if (err && first_err === null)
	  first_err = err;
	decr_pending();
      });
    })(result_index);
  }
  decr_pending();

  function decr_pending() {
    if (--pending === 0) {
      if (first_err) {
        callback(first_err, null);
	return;
      }
      for (var i = 0; i < file_results.length; i++) {
        if (file_results[i]) {
	  if (rv) {
	    rv = this.merge(file_results[i], rv);
	  } else {
	    rv = file_results[i];
	  }
	}
      }
      callback(null, rv);
    }
  }
};

function readn_buffer(fd, buffer, offset, length, position, callback)
{
  if (length === 0)
    callback(null);
  else
    fs.read(file.fd, buffer, offset, length, start, function(err, bytesRead) {
      if (err)
        callback(err);
      else
        readn_buffer(fd, buffer, offset + bytesRead, length - bytesRead, position + bytesRead, callback);
    });
}
function writen_buffer(fd, buffer, offset, length, position, callback)
{
  if (length === 0)
    callback(null);
  else
    fs.write(fd, buffer, offset, length, position, function(err, written) {
      if (err)
        callback(err);
      else
        writen_buffer(fd, buffer, offset + written, length - written, position + written, callback);
    });
}

exports.JsonLookupTableSimple.prototype._do_search_file = function(file, curried_comparator, callback) {
  var self = this;
  console.log("_do_search_file: file.id=" + file.id + "; byte_size=" + file.size_bytes);
  do_search_range(0, file.size_bytes);

  function do_search_range(start, length) {
    if (length === 0) {
      callback(null, null);
    } else if (length <= 4096) {
      var buf = new Buffer(length);
      readn_buffer(file.fd, buf, 0, length, start, function(err) {
        if (err) {
          callback(err);
        } else {
          // split buffer into lines
          var strs = buf.slice(0,buf.length - 1).toString().split("\n");

          console.log("doing binary search of " + strs.length + " entries from " + file.id);

          // binary search JSON array
          var obj_start = 0;
          var obj_length = strs.length;
          while (obj_length > 0) {
            var obj_mid = obj_start + Math.floor(obj_length / 2);
            var obj = JSON.parse(strs[obj_mid]);
            var cmp = curried_comparator(obj);
            if (cmp < 0) {
              obj_length = obj_mid - obj_start;
            } else if (cmp > 0) {
              obj_length = obj_start + obj_length - (obj_mid + 1);
              obj_start = obj_mid + 1;
            } else {
              callback(null, obj);
            }
          }
          callback(null, null);
        }
      });
    } else {
      // read 4096 bytes from middle of search area
      var buf = new Buffer(4096);
      var buf_position = start + Math.floor((length - 4096) / 2);
      readn_buffer(file.fd, buf, 0, 4096, buf_position, function(err) {
        if (err) {
          callback(err);
        } else {
          try_handle_buf();
        }
      });
      function try_handle_buf() {
        var line_range = find_line_range(buf, buf_position === start);
        if (!line_range) {
          // expand buffer left and right
          var expand_left = buf_position - start;
          if (expand_left > 4096)
            expand_left = 4096;
          var expand_right = (buf_position + buf.length) - (start + length);
          if (expand_right > 4096)
            expand_right = 4096;
          var pending = 1;
          var first_err = null;
          var b = new Buffer(buf.length + expand_left + expand_right);
          buf.copy(b, expand_left, 0, buf.size);
          buf = b;
          buf_position -= expand_left;
          if (expand_left > 0) {
            pending++;
            readn_buffer(file.fd, buf, 0, expand_left, buf_position, function(err) {
              if (err && !first_err)
                first_err = err;
              decr_pending();
            });
          }
          if (expand_right > 0) {
            pending++;
            readn_buffer(file.fd, buf, buf.length - expand_right, expand_right,
                         buf_position + buf.length - expand_right, function(err) {
              if (err && !first_err)
                first_err = err;
              decr_pending();
            });
          }
          decr_pending();
          function decr_pending() {
            if (--pending === 0) {
              if (first_err) {
                callback(first_err);
              } else {
                try_handle_buf();
              }
            }
          }
        } else {
          var obj = JSON.parse(buf.slice(line_range[0], line_range[1]).toString());
          var cmp = curried_comparator(obj);
          if (cmp < 0) {
            do_search_range(start, buf_position + line_range[0] - start);
          } else if (cmp > 0) {
            var s = buf_position + line_range[1];
            do_search_range(s, start + length - s);
          } else {
            callback(null, obj);
          }
        }
      }
    }
  }
};

exports.JsonLookupTableSimple.prototype._close = function(deleteFiles) {
  // Delete files + merge-jobs
  for (var f = this.oldest_file; f !== null; f = f.newer) {
    if (f.merge_job) {
      assert(f.newer.merge_job === f.merge_job);
      fs.closeSync(f.merge_job.output_file.fd);
      if (deleteFiles)
        fs.unlinkSync(this.dir + "/F." + f.merge_job.output_file.id);
    }
    fs.closeSync(f.fd);
    if (deleteFiles)
      fs.unlinkSync(this.dir + "/F." + f.id);
  }

  fs.closeSync(this.dir_fd);
  fs.closeSync(this.journal_fd);
  if (deleteFiles) {
    fs.unlinkSync(this.dir + "/JOURNAL");
    fs.rmdirSync(this.dir);
  }
}
exports.JsonLookupTableSimple.prototype.close = function() {
  this._close(false);
};
exports.JsonLookupTableSimple.prototype.closeAndDelete = function() {
  this._close(true);
};
