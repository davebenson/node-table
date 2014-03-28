
// Options:
//    class: .../    
//    directory: ...
//    compare: function(a,b)
//    merge: function([docs]) -> [docs] (latter possibly empty)
//    
// database_callback(err, db) will be invoked exactly once
exports.factory = function(options, database_callback) {
  var c = options.class ? options.class : exports.DocDatabaseSimple;
  return new c(options);
}

var newline_buffer = new Buffer("\n");

function JsonLookupTableSimpleAddRequest(callback) {
  this.callback = callback;
  this.has_result = false;
  this.result = null;
  this.next = null;
}
function JsonLookupTableSimpleMergeJob() {
  this.newer_file = null;
  this.older_file = null;
}

function JsonLookupTableSimpleFile() {
  this.fd = -1;
  this.id = "";
  this.buffers_size = 0;
  this.buffers = [];
  this.newer = null;
  this.older = null;
  this.merge_job = null;
}

exports.JsonLookupTableSimple = function(options, done_creating) {
  this.n_latest = 0;
  this.sort_merged = [];
  this.journal = fs.open(...);
  this.journal_write_pending = false;
  this.journal_write_pending_buffers = [];
  this.journal_write_pending_callbacks = [];
  this.journal_max_records = 512;
  this.writing_level0_block = false;
  this.pending_writing_level0_files = [];
  this.compare = options.compare;
  this.merge = options.merge;
  this.next_file_id = 1;
  this.oldest_file = null;
  this.newest_file = null;
  this.oldest_request = null;
  this.newest_request = null;
};


exports.JsonLookupTableSimple.prototype.add = function(doc, callback)
{
  var self = this;
  var pot_at = 0;
  var cur_docs = [doc];
  while (true) {
    if (this.sort_merged.length == pot_at) {
      this.sort_merged.push(cur_doc);
      break;
    } else if (this.sort_merged[pot_at] === null) {
      this.sort_merged[pot_at] = cur_doc;
      break;
    } else {
      var new_merged = this._merge_lists(this.sort_merged[pot_at], cur_doc);
      this.sort_merged[pot_at] = null;
      pot_at++;
      cur_docs = new_merged;
    }
  }
  self.n_latest++;

  var pending_requests = 1;

  // write to journal
  var buf = new Buffer(JSON.stringify(doc) + "\n");
  if (this.journal_write_pending) {
    this.journal_write_pending_buffers.push(buf);
    this.journal_write_pending_callbacks.push(decr_pending_requests);
  } else {
    this.journal_write_pending = true;
    ++pending_requests;
    var this_err = null;
    this.journal.write(buf, function(err) {
      if (err) {
        var bufs = self.journal_write_pending_buffers;
        var cbs = self.journal_write_pending_callbacks;
        self.journal_write_pending_buffers = [];
        self.journal_write_pending_callbacks = [];
        for (var i = 0; i < cbs.length; i++)
          cbs[i](err);
        this_err = err;
        decr_pending_requests();
      } else if (this.journal_write_pending_buffers.length >== 0) {
        this.journal_write_pending_buffers.push(buf);
        this.journal_write_pending_callbacks.push(decr_pending_requests);
        this._flush_pending_write_buffers(function(err) {
          if (err)
	    console.log("_flush_pending_write_buffers failed: " + err);
        });
      } else {
        this.journal_write_pending = false;
        decr_pending_requests();
      }
    });
  }

  if (this.n_latest >= this.journal_max_records) {
    // sort-merge latest
    var all_sorted = [];
    for (var index = 0; index < this.sort_merged.length; index++)
      if (this.sort_merged[index] !== null) {
        all_sorted = this.sort_merged[index];
	break;
      }
    for (index++; index < this.sort_merged.length; index++)
      if (this.sort_merged[index] !== null) {
        all_sorted = this._merge_lists(sort_merged[index], all_sorted);
	break;
      }
    var block = this._sorted_json_to_binary(all_sorted);

    var file_id = this._allocate_file_id();

    ++pending_requests;
    pending_writing_level0_files.push({
      data:block,
      file_id:file_id,
      callback:decr_pending_requests
    });
    if (!this.writing_level0_file) {
      this._write_pending_level0_files();
    }
  }
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
  if (this.journal_write_pending_buffers.length > 0) {
    var cbs = this.journal_write_pending_callbacks;
    var buf = Buffer.concat(this.journal_write_pending_buffers);
    this.journal_write_pending_callbacks = [];
    this.journal_write_pending_buffers = [];
    this.journal_stream.write(buf, function(err) {
      for (var i = 0; i < cbs.length; i++)
        cbs[i](err);
      callback(err);
    });
  } else {
    callback(null);
  }
};

exports.JsonLookupTableSimple.prototype._write_pending_level0_files = function() {
  this.writing_level0_file = true;
  var self = this;
  function write_next() {
    if (self.pending_writing_level0_files.length === 0) {
      this.writing_level0_file = false;
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

exports.JsonLookupTableSimple.prototype._create_file_from_level0_info = function(info, callback)
{
      fs.writeFile(self.file_id_to_filename(info.file_id),
                   info.block,
                   function(err) {
		     info.callback(err);
		     var file = new JsonLookupTableSimpleFile();
		     file.fd = ...;
		     file.id = info.file_id;
		     write_next();
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
  while (n > 1) {
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
      } else {
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

JsonLookupTableSimple.prototype._do_search_file = function(
