
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

var DEFAULT_JOURNAL_MAX_ENTRIES_LOG2 = 9;

/// uh what's the right idiom here?
var exports = module.exports;

var newline_buffer = new Buffer("\n");

function _JsonLookupTableSimple_MergeJob() {
  this.newer_input_offset = 0;
  this.newer_input_file = null;

  this.older_input_offset = 0;
  this.older_input_file = null;

  this.output_id = 0;
  this.output_fd = -1;
  this.output_offset = 0;
}

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
}

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

exports.JsonLookupTableSimple = function(options) {
  this.n_latest = 0;
  this.sort_merged = [];
  this.dir = options.dir;
  assert.equal(typeof(this.dir), 'string');
  this.journal_write_pending = false;
  this.journal_write_pending_buffers = [];
  this.journal_write_pending_callbacks = [];
  this.journal_max_entries_log2 = options.journal_max_entries_log2 || DEFAULT_JOURNAL_MAX_ENTRIES_LOG2;
  for (var i = 0; i < this.journal_max_entries_log2; i++)
    this.sort_merged.push(null);
  this.writing_level0_block = false;
  this.pending_writing_level0_files = [];
  this.compare = options.compare;
  this.merge = options.merge;
  this.next_file_id = 1;
  this.oldest_file = null;
  this.newest_file = null;
  this.oldest_request = null;
  this.newest_request = null;
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
  try {
    var cp_data = fs.readFileSync(this.dir + "/CHECKPOINT");
    cp_json = JSON.parse(cp_data.toString());
  } catch (e) {
    if (e.code !== 'ENOENT') {
      throw(e);
    }

    // check that there's no data-like stuff
    var files = fs.readdirSync(this.dir);
    for (var fi = 0; fi < files.length; fi++) {
      throw(new Error("directory contained " + files[fi] + " (out of " + files.length + " files), but no CHECKPOINT"));
    }

    // Create empty checkpoint.
    cp_json = {pending_level0s: [],
               files: [],
               merge_jobs: [],
               next_file_id: 1};
    var cp_data = new Buffer(JSON.stringify(cp_json));
    fs.writeFileSync(this.dir + "/CHECKPOINT", cp_data);
    fs.writeFileSync(this.dir + "/JOURNAL", new Buffer(0));
  }

  // Open all files and merge jobs, restart any journalling -> level_0 writes.

  // open JOURNAL
  var journal_data = fs.readFileSync(this.dir + "/JOURNAL");
  for (var last_nl_pos = journal_data.length - 1; last_nl_pos >= 0; last_nl_pos--)
    if (journal_data[last_nl_pos] === 0x0a)
      break;
  var clipped_journal_len = last_nl_pos + 1;
  if (clipped_journal_len < journal_data.length) {
    fs.truncateSync(this.dir + "/JOURNAL", clipped_journal_len);
  }

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

  // open JOURNAL.### and writeFileSync(), add to files list
  last_f = this.newest_file;
  for (var i = 0; i < cp_json.pending_level0s.length; i++) {
    var file_id = cp_json.pending_level0s[i].id;
    var journal_file_data = fs.readFileSync(this.dir + "/JOURNAL." + file_id);
    assert(journal_file_data[journal_file_data.length - 1] === 0x0a);
    var journal_file_lines = journal_file_data.slice(0,journal_file_data.length - 1).split("\n");
    var sm = new _JsonLookupTableSimple_SortMergeInMemory(this.compare, this.merge);
    for (var j = 0; j < journal_file_lines.length; j++) {
      sm.add(JSON.parse(journal_file_lines[j]));
    }
    var entries = sm.finish();
    var buffers = [];
    for (var j = 0; j < entries.length; j++) {
      buffers.push(new Buffer(JSON.stringify(entries[j])));
      buffers.push(newline_buffer);
    }
    var output_data = Buffer.concat(buffers);
    fs.writeFileSync(this.dir + "/F." + file_id, output_data);

    var f = new _JsonLookupTableSimple_File();
    f.id = file_id;
    f.start_input_entry = cp_json.pending_level0s[i].start_input_entry;
    f.n_input_entries = journal_file_lines.length;
    f.size_entries = entries.length;
    f.size_bytes = output_data.length;
    f.older = last_f;
    if (last_f)  {
      last_f.newer = f;
    } else {
      this.oldest_file = f;
    }
    this.newest_file = f;

    f.fd = fs.openSync(this.dir + "/F." + f.id, "r");
  }

  // open merge jobs
  for (var i = 0; i < cp_json.merge_jobs.length; i++) {
    var mj = cp_json.merge_jobs[i];
    for (var f = this.newest_file; f !== null; f = f.older) {
      if (mj.newer_input_id === f.id) {
        assert(f.older.id === mj.older_input_id);
        var merge_job = new _JsonLookupTableSimple_MergeJob();
        merge_job.output_id = mj.output_id;
        merge_job.older_input_offset = mj.older_input_offset;
        merge_job.newer_input_offset = mj.newer_input_offset;
        merge_job.output_offset = mj.output_offset;
        merge_job.output_fd = fs.openSync(this.dir + "/F." + mj.output_offset, "r+");
        merge_job.newer_input_file = f;
        merge_job.older_input_file = f.older;
        fs.ftruncateSync(merge_job.output_fd, merge_job.output_offset);
        f.merge_job = n.older.merge_job = merge_job;
        break;
      }
    }
    if (f === null)
      console.log("WARNING: file " + mj.newer_input_id + " not found");
  }

  // Clean up CHECKPOINT to not use pending_level0s, since there are all processed now
  if (cp_json.pending_level0s.length > 0) {
    var new_cp_json = this._create_checkpoint_json();
    var new_cp_data = new Buffer(JSON.stringify(new_cp_json));
    fs.writeFileSync(this.dir + "/CHECKPOINT", new_cp_data);
    for (var i = 0; i < cp_json.pending_level0s.length; i++) {
      var id = cp_json.pending_level0s.id;
      fs.unlinkSync(this.dir + "/JOURNAL." + id);
    }
  }
};


exports.JsonLookupTableSimple.prototype.add = function(doc, callback)
{
  var self = this;
  var pot_at = 0;
  var cur_docs = [doc];
  var level0_docs = null;

  while (true) {
    if (this.sort_merged.length == pot_at) {
      level0_docs = cur_docs;
      break;
    } else if (this.sort_merged[pot_at] === null) {
      this.sort_merged[pot_at] = cur_docs;
      break;
    } else {
      var new_merged = this._merge_lists(this.sort_merged[pot_at], cur_docs);
      this.sort_merged[pot_at] = null;
      pot_at++;
      cur_docs = new_merged;
    }
  }
  self.n_latest++;

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

  if (level0_docs !== null) {
    var file_id = this._allocate_file_id();
    ++pending_requests;
    pending_writing_level0_files.push({
      data: this._sorted_json_to_binary(level0_docs),
      file_id: file_id,
      callback: decr_pending_requests
    });
    if (!this.writing_level0_file) {
      this._write_pending_level0_files();
    }
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

exports.JsonLookupTableSimple.prototype._write_pending_level0_files = function() {
  this.writing_level0_file = true;
  var self = this;
  function write_next() {
    if (self.pending_writing_level0_files.length === 0) {
      self.writing_level0_file = false;
      self._maybe_start_merge_jobs();
      return;
    } else {
      var info = self.pending_writing_level0_files[0];
      self.pending_writing_level0_files.splice(0,1);
      self._create_file_from_level0_info(info, function(err, created_file) {
        write_next();
      });
    }
  }
  write_next();
};

exports.JsonLookupTableSimple.prototype._maybe_start_merge_jobs = function() {
  for (var f = self.newest_file; f !== null; f = f.older) {
    if (f.merge_job === null &&
        f.older !== null &&
        f.older.merge_job === null &&
        f.size_bytes > f.older.size_bytes * 0.35)
    {
      var merge_job = new _JsonLookupTableSimple_MergeJob();
      merge_job.newer_input_file = f;
      merge_job.older_input_file = f.older;
      f.merge_job = f.older.merge_job = merge_job;

      merge_job.output_id = this._allocate_file_id();
      merge_job.output_fd = fs.openSync(this.dir + "/F." + merge_job.output_id, "w");
    }
  }
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
    buffers.push(JSON.stringify(array[i]));
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

exports.JsonLookupTableSimple.prototype._array_lookup = function(curried_comparator, array)
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

// callback(err, result) - both == null: not found.
exports.JsonLookupTableSimple.prototype.get = function(curried_comparator, callback)
{
  var rv = null;
  var self = this;
  for (var i = 0; i < this.sort_merged.length; i++) {
    if (this.sort_merged[i]) {
      var sub_rv = this._array_lookup(curried_comparator, this.sort_merged[i]);
      if (sub_rv) {
        if (rv === null)
	  rv = sub_rv;
        else
        rv = this.merge(sub_rv, rv); 
      }
    }
  }

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
