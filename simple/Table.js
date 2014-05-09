
var newline_buffer = new Buffer("\n");
var fs_ext = require("fs-ext");
var fs = require("fs");
var assert = require("assert");
var util = require("util");
var common = require("./common");

var SortMergeInMemory = require("./SortMergeInMemory");
var File = require("./File");
var MergeJob = require("./MergeJob");

var DEFAULT_JOURNAL_MAX_ENTRIES = 512;


Table = function(options) {
  this.n_latest = 0;
  this.sort_merged = [];
  this.dir = options.dir;
  assert.equal(typeof(this.dir), 'string');
  this.n_input_entries = 0;
  this.journal_write_pending = false;
  this.journal_write_pending_buffers = [];
  this.journal_write_pending_callbacks = [];

  this.journal_flush_after_write = false;

  this.journal_start_input_entry = 0;
  this.journal_num_entries = 0;
  this.journal_max_entries = options.journal_max_entries || DEFAULT_JOURNAL_MAX_ENTRIES;
  this.last_journal_files = [];
  //this.writing_level0_block = false;
  //this.pending_writing_level0_files = [];
  this.compare = options.compare;
  this.merge = options.merge;
  this.sort_merger = new SortMergeInMemory(options.compare, options.merge);
  this.next_file_id = 1;
  this.oldest_file = null;
  this.newest_file = null;
  this.dir_fd = -1;
  this.journal_fd = -1;
  this.journal_offset = 0;
  this.make_curried_comparator = options.make_curried_comparator;

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
    for (var i = 1; i < lines.length; i++) {
      try {
        journal_lines.push(JSON.parse(lines[i]));
      } catch(se) {
        console.log("corrupted JSON line");
      }
    }
    this.journal_offset = cp_data.length;
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
               n_input_entries: 0,
               next_file_id: 1};
    var cp_data = new Buffer(JSON.stringify(cp_json) + "\n");
    this.journal_offset = cp_data.length;
    fs.writeFileSync(this.dir + "/JOURNAL", cp_data);
  }

  // Open all files and merge jobs, restart any journalling -> level_0 writes.

  this.next_file_id = cp_json.next_file_id;
  this.journal_start_input_entry = cp_json.n_input_entries;
  this.n_input_entries = cp_json.n_input_entries;

  // open files; create files list
  var last_f = null;
  var cp_files = [];
  for (var i = 0; i < cp_json.files.length; i++) {
    var f = new File();
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
    f.filename = this.dir + "/F." + f.id;

    f.fd = fs.openSync(f.filename, "r+");
    console.log("open file " + f.id + " on fd " + f.fd);

    cp_files.push(f);
    f.ref();
  }

  // open merge jobs
  for (var i = 0; i < cp_json.merge_jobs.length; i++) {
    var mj = cp_json.merge_jobs[i];
    for (var f = this.newest_file; f !== null; f = f.older) {
      if (mj.newer.input_id === f.id) {
        assert.equal(f.older.id, mj.older.input_id);
        var merge_job = this._create_merge_job(f.older, {});
        merge_job.output_file.id = mj.output_id;
        merge_job.older.input_offset = mj.older.input_offset;
        merge_job.newer.input_offset = mj.newer.input_offset;
        merge_job.output_offset = mj.output_offset;
        merge_job.output_file.size_bytes = mj.output_offset;
        merge_job.output_file.size_entries = mj.output_n_entries;
        merge_job.output_file.filename = this.dir + "/F." + merge_job.output_file.id;
        merge_job.output_file.fd = fs.openSync(merge_job.output_file.filename, "r+");
        merge_job.output_file.start_input_entry = merge_job.older.input_file.start_input_entry;
        merge_job.output_file.n_input_entries = merge_job.older.input_file.n_input_entries + merge_job.newer.input_file.n_input_entries;
        merge_job.newer.input_file = f;
        merge_job.older.input_file = f.older;
        fs.ftruncateSync(merge_job.output_file.fd, merge_job.output_offset);
        f.merge_job = f.older.merge_job = merge_job;
        cp_files.push(merge_job.output_file);
        merge_job.output_file.ref();

        //this._start_merge_job(merge_job);
        merge_job.start();

        break;
      }
    }
    if (f === null)
      console.log("WARNING: file " + mj.newer.input_id + " not found");
  }

  for (var i = 0; i < journal_lines.length; i++) {
    if (Array.isArray(journal_lines[i])) {
      for (var j = 0; j < journal_lines[i].length; j++) {
        this.sort_merger.add(journal_lines[i][j]);
        this.n_input_entries++;
        this.journal_num_entries++;
      }
    } else {
      this.sort_merger.add(journal_lines[i]);
      this.n_input_entries++;
      this.journal_num_entries++;
    }
  }
  this.last_journal_files = cp_files;
};

Table.prototype.add = function(doc, callback)
{
  var self = this;
  var pot_at = 0;
  var level0_docs = null;
  var cur_docs;
  this._check_invariants("add");
  if (Array.isArray(doc)) {
    cur_docs = doc;
  } else {
    cur_docs = [doc];
  }

  if (this.journal_num_entries + cur_docs.length > this.journal_max_entries) {
    if (this.journal_write_pending) {
      this.journal_flush_after_write = true;
    } else {
      this._flush_journal();
    }
  }
  for (var i = 0; i < cur_docs.length; i++)
    this.sort_merger.add(cur_docs[i]);
  this.journal_num_entries += cur_docs.length;

  this.n_input_entries += cur_docs.length;
  this.n_input_transactions += 1;

  var pending_requests = 1;

  // write to journal
  var buf = new Buffer(JSON.stringify(doc) + "\n");
  ++pending_requests;
  if (this.journal_write_pending) {
    console.log("journal write pending");
    this.journal_write_pending_buffers.push(buf);
    this.journal_write_pending_callbacks.push(decr_pending_requests);
  } else {
    console.log("doing journal write");
    this.journal_write_pending = true;
    var this_err = null;
    if (this.journal_fd === -1) {
      //assert.equal(this.journal_offset, 0);
      this.journal_fd = fs.openSync(this.dir + "/JOURNAL", "r+");
    }
    writen_buffer(this.journal_fd, buf, 0, buf.length, this.journal_offset, function(err) {
      if (err) {
        console.log("failed writing to journal " + self.journal_fd);
        var bufs = self.journal_write_pending_buffers;
        var cbs = self.journal_write_pending_callbacks;
        self.journal_write_pending_buffers = [];
        self.journal_write_pending_callbacks = [];
        for (var i = 0; i < cbs.length; i++)
          cbs[i](err);
        this_err = err;
        decr_pending_requests();
      } else if (self.journal_write_pending_buffers.length > 0) {
        console.log("done writing journal... but " + self.journal_write_pending_buffers.length + " new buffers exist");
        self.journal_offset += buf.length;
        self._flush_pending_journal_write_buffers(function(err) {
          if (err)
	    console.log("_flush_pending_journal_write_buffers failed: " + err);
          decr_pending_requests();
        });
      } else {
        console.log("done writing journal");
        self.journal_offset += buf.length;
        self.journal_write_pending = false;
        if (self.journal_flush_after_write) {
          self.journal_flush_after_write = false;
          self._flush_journal();
        }
        decr_pending_requests();
      }
    });
  }

  // remove re-entrance guard
  decr_pending_requests();

  function decr_pending_requests() {
    if (--pending_requests === 0) {
      if (this_err) {
        callback(this_err);
      } else {
        callback(null);
      }
    }
  }
};

Table.prototype._flush_journal = function()
{
  this._check_invariants("flush_journal");

  fs.closeSync(this.journal_fd);

  var file_id = this._allocate_file_id();
  //this.pending_writing_level0_files.push(pending_level0_info);
  var level0_docs = this.sort_merger.get_list();
  var level0_data = this._sorted_json_to_binary(level0_docs);
  fs.writeFileSync(this.dir + "/F." + file_id, level0_data);
  this.journal_fd = fs.openSync(this.dir + "/JOURNAL.tmp", "w");

  var f = new File();
  f.id = file_id;
  f.filename = this.dir + "/F." + file_id;
  f.fd = fs.openSync(f.filename, "r");
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
  this.journal_num_entries = 0;

  var cp_files = [];
  for (var file = this.newest_file; file !== null; file = file.older) {
    cp_files.push(file);
    file.ref();
    if (file.merge_job && file.merge_job.newer.input_file === file) {
      var out = file.merge_job.output_file;
      cp_files.push(out);
      out.ref();
    }
  }

  var cp_json = this._create_checkpoint_json();
  var cp_data = new Buffer(JSON.stringify(cp_json) + "\n");
  common.write_sync_n(this.journal_fd, cp_data, 0);
  fs.renameSync(this.dir + "/JOURNAL.tmp", this.dir + "/JOURNAL");
  this.journal_offset = cp_data.length;

  this.sort_merger.reset();

  for (var i = 0; i < this.last_journal_files.length; i++)
    this.last_journal_files[i].unref();
  this.last_journal_files = cp_files;

  this._check_invariants("flush_journal done");
};

function writen_sync(fd, buffer, offset, length, position)
{
  var at = 0;
  while (at < length) {
    var written = fs.writeSync(fd,buffer,offset+at, length-at, position+at);
    at += written;
  }
}

// Called internally when a write call to the journal has finished,
// but pending writes have accumulated meanwhile.
Table.prototype._flush_pending_journal_write_buffers =
function(callback) {
  var self = this;
  assert(this.journal_write_pending_buffers.length > 0);
  assert(this.journal_write_pending);
  var cbs = this.journal_write_pending_callbacks;
  var buf = Buffer.concat(this.journal_write_pending_buffers);
  this.journal_write_pending_callbacks = [];
  this.journal_write_pending_buffers = [];

  writen_buffer(this.journal_fd, buf, 0, buf.length, this.journal_offset,
    function(err) {
      for (var i = 0; i < cbs.length; i++) {
        var cb = cbs[i];
        cb(err);
      }
      if (!err)
        self.journal_offset += buf.length;
      if (!err && self.journal_write_pending_buffers.length > 0) {
        console.log("done w/ write, but more buffer meanwhile");
        self._flush_pending_journal_write_buffers(callback);
      } else {
        console.log("done w/ write; err=" + err);
        self.journal_write_pending = false;
        if (self.journal_flush_after_write) {
          self.journal_flush_after_write = false;
          self._flush_journal();
        }
        callback(err);
      }
    }
  );
};

Table.prototype._create_merge_job = function(older_file, options) {
  var self = this;
  var merge_job = new MergeJob();
  merge_job.compare = this.compare;
  merge_job.merge = this.merge;
  merge_job.older.input_file = older_file;
  merge_job.newer.input_file = older_file.newer;
  older_file.merge_job = merge_job;
  older_file.newer.merge_job = merge_job;
  merge_job.on("finished", function() {
    self.merge_job_finished(this);
  });
  return merge_job;
};

Table.prototype._maybe_start_merge_jobs = function() {
  var self = this;
  for (var f = this.newest_file; f !== null; f = f.older) {
    if (f.merge_job === null &&
        f.older !== null &&
        f.older.merge_job === null &&
        f.size_bytes > f.older.size_bytes * 0.35)
    {
      var merge_job = this._create_merge_job(f.older, {});

      merge_job.output_file.id = this._allocate_file_id();
      merge_job.output_file.filename = this.dir + "/F." + merge_job.output_file.id;
      merge_job.output_file.fd = fs.openSync(merge_job.output_file.filename, "w+");
      merge_job.output_file.start_input_entry = merge_job.older.input_file.start_input_entry;
      merge_job.output_file.n_input_entries = merge_job.older.input_file.n_input_entries + merge_job.newer.input_file.n_input_entries;


      merge_job.start();
    }
  }
};

Table.prototype.merge_job_finished = function(merge_job)
{
  var output_file = merge_job.output_file;

  //TODO convert fd to read-only

  // replace the two input files with the single new output File
  output_file.older = merge_job.older.input_file.older;
  if (output_file.older)
    output_file.older.newer = output_file;
  else
    this.oldest_file = output_file;
  output_file.newer = merge_job.newer.input_file.newer;
  if (output_file.newer)
    output_file.newer.older = output_file;
  else
    this.newest_file = output_file;

  // Delete these objects if ready.
  merge_job.older.input_file.unref();
  merge_job.newer.input_file.unref();

  this._check_invariants("finish_merge_job");
};


Table._file_id_to_filename = function(file_id)
{
  return this.dir + "/F." + file_id;
}

Table.prototype._sorted_json_to_binary = function(array)
{
  var buffers = [];
  for (var i = 0; i < array.length; i++) {
    buffers.push(new Buffer(JSON.stringify(array[i])));
    buffers.push(newline_buffer);
  }
  return Buffer.concat(buffers);
};

Table.prototype._merge_lists = function(older, newer) {
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

Table.prototype._allocate_file_id = function()
{
  var rv = this.next_file_id.toString();
  this.next_file_id += 1;
  return rv;
};


Table.prototype._create_checkpoint_json = function()
{
  this._check_invariants("create-checkpoint");
  var files = [], merge_jobs = [];
  for (var f = this.newest_file; f !== null; f = f.older) {
    files.push(f.toJSON());
    if (f.merge_job && f.merge_job.newer.input_file === f)
      merge_jobs.push(f.merge_job.toJSON());
  }
  return {files: files,
          // NOTE: at this point we assume that all journalled data 
          // is consolidated into a sorted-merged file.
          n_input_entries: this.n_input_entries,
          merge_jobs: merge_jobs,
          next_file_id:this.next_file_id};
};

// callback(err, result) - both == null: not found.
Table.prototype.get = function(curried_comparator, callback)
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
	    merge_job.output_file.do_search_file(curried_comparator, function(err, res) {
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
      file.do_search_file(curried_comparator, function(err, res) {
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

Table.prototype._close = function(deleteFiles) {
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
};
Table.prototype.close = function() {
  this._close(false);
};
Table.prototype.closeAndDelete = function() {
  this._close(true);
};

Table.prototype._check_invariants = function(loc)
{
  var last = null;
  var at_input_entry = 0;
  //console.log("_check_invariants: location=" + loc);
  for (var file = this.oldest_file; file !== null; file = file.newer) {
    assert (file.older === last);
    assert(file.start_input_entry === at_input_entry);
    at_input_entry += file.n_input_entries;
    if (file.merge_job !== null) {
      if (file.merge_job.older.input_file === file) {
        assert(file.merge_job.newer.input_file === file.newer);
      } else if (file.merge_job.newer.input_file === file) {
        assert(file.merge_job.older.input_file === file.older);
      } else {
        console.log("merge_job bad: " + util.inspect(file.merge_job));
        assert(false);
      }
    }
    last = file;
  }
  assert.equal(last, this.newest_file);
  assert.equal(at_input_entry, this.journal_start_input_entry);
  assert.equal(at_input_entry + this.journal_num_entries, this.n_input_entries);
};

module.exports = Table;
