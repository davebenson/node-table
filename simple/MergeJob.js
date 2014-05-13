var File = require("./File.js");
var MergeInput = require("./MergeInput.js");
var assert = require("assert");
var fs = require("fs");
var common = require("./common");
var util = require("util");
var events = require("events");

var newline_buffer = new Buffer("\n");

function MergeJob() {
  this.newer = new MergeInput();
  this.older = new MergeInput();
  this.compare = null;
  this.merge = null;

  this.output_file = new File();
  this.output_offset = 0;
  this.last_output = null;
  this.state = 'UNSTARTED';             // enum: UNSTARTED, RUNNING, FINISHED
}
util.inherits(MergeJob, events.EventEmitter);
function find_newline(buffer, start, end)
{
  for (var i = start; i < end; i++)
    if (buffer[i] === 0x0a)
      return i;
  return -1;
}


MergeJob.prototype.toJSON = function() {
  return {
    output_id: this.output_file.id,
    output_offset: this.output_offset,
    output_n_entries: this.output_file.size_entries,
    output_largest_entry: this.output_file.largest_entry,
    older: this.older.toJSON(),
    newer: this.newer.toJSON(),
  };
};

module.exports = MergeJob;

MergeJob.prototype.start = function() {
  assert(this.state === 'UNSTARTED');
  this.state = 'RUNNING';
  this._maybe_start_merge_input_read(this.newer);
  this._maybe_start_merge_input_read(this.older);
};

MergeJob.prototype._maybe_start_merge_input_read = function(merge_input)
{
  if (merge_input.eof) {
    return;
  }
  if (merge_input.input_offset >= merge_input.input_file.size_bytes) {
    merge_input.eof = true;
    this._try_making_merge_output();
    return;
  }

  if (merge_input.peekable.length > 16) {
    return;
  }

  if (merge_input.input_pending) {
    return;
  }

  var self = this;
  var read_end = merge_input.input_offset + merge_input.buffer_valid_length;
  var til_end = merge_input.input_file.size_bytes - read_end;
  if (til_end > 0) {
    var to_read = til_end < 4096 ? til_end : 4096;

    // ensure merge_input.buffer is large enough
    if (merge_input.buffer.length < merge_input.buffer_valid_length + to_read) {
      var new_buf = new Buffer(merge_input.buffer_valid_length + to_read);
      merge_input.buffer.copy(new_buf);
      merge_input.buffer = new_buf;
    }

    merge_input.input_pending = true;
    //console.log("merge_input " + merge_input.input_file.id + "; calling read(" + merge_input.buffer_valid_length + ", ", + to_read + ", " + read_end + "); fd=" + merge_input.input_file.fd);
    fs.read(merge_input.input_file.fd, merge_input.buffer, merge_input.buffer_valid_length,
            to_read, read_end, function(err, bytesRead) {
              merge_input.buffer_valid_length += bytesRead;

              // parse out any lines; add JSON/length pair to 'pending'.
              var nl_index;
              var at = merge_input.buffer_available;
              var n_lines = 0;
              while ((nl_index=find_newline(merge_input.buffer, at, merge_input.buffer_valid_length)) != -1) {
                var str = merge_input.buffer.slice(at, nl_index).toString();
                merge_input.peekable.push([JSON.parse(str), nl_index + 1 - at]);
                at = nl_index + 1;
                n_lines++;
              }
              merge_input.buffer_available = at;

              merge_input.input_pending = false;
              if (n_lines === 0) {
                self._maybe_start_merge_input_read(merge_input);
              } else {
                self._try_making_merge_output();
              }
            });
  }
};

MergeJob.prototype._try_making_merge_output = function()
{
  assert(this.state !== 'UNSTARTED');
  if (this.state === 'FINISHED')
    return;

  if (this.newer.eof && this.older.eof) {
    this._finish_merge_job();
    return;
  } else {
    var ready = true;
    if (this.newer.peekable.length === 0 && !this.newer.eof) {
      this._maybe_start_merge_input_read(this.newer);
      ready = false;
    }
    if (this.older.peekable.length === 0 && !this.older.eof) {
      this._maybe_start_merge_input_read(this.older);
      ready = false;
    }
    var outputs = [];
    var ni = 0, oi = 0;
    while (ni < this.newer.peekable.length && oi < this.older.peekable.length) {
      var cmp = this.compare(this.newer.peekable[ni][0], this.older.peekable[oi][0]);
      if (cmp < 0) {
        outputs.push(this.newer.peekable[ni++][0]);
      } else if (cmp > 0) {
        outputs.push(this.older.peekable[oi++][0]);
      } else {
        var o = this.older.peekable[oi++][0];
        var n = this.older.peekable[ni++][0];
        outputs.push(this.merge(o,n));
      }
    }
    while (ni < this.newer.peekable.length && this.older.eof) {
      outputs.push(this.newer.peekable[ni++][0]);
    }
    while (oi < this.older.peekable.length && this.newer.eof) {
      outputs.push(this.older.peekable[oi++][0]);
    }

    if (oi > 0) {
      this.older.remove_first(oi);
    }
    if (ni > 0) {
      this.newer.remove_first(ni);
    }

    //console.log("generated " + outputs.length + " outputs from " + oi + " and " + ni + "input entries");
      
    if (outputs.length > 0) {
      var output_buffers = [];
      for (var i = 0; i < outputs.length; i++) {
        var b = new Buffer(JSON.stringify(outputs[i]));
        if (this.output_file.largest_entry < b.length)
          this.output_file.largest_entry = b.length;
        output_buffers.push(b);
        output_buffers.push(newline_buffer);
      }
      var total_output = Buffer.concat(output_buffers);
      common.write_sync_n(this.output_file.fd, total_output, this.output_offset);
      this.last_output = outputs[outputs.length - 1];
      this.output_offset += total_output.length;
      this.output_file.size_bytes += total_output.length;
      this.output_file.size_entries += outputs.length;

      if (!this.newer.eof && this.newer.peekable.length === 0) {
        this._maybe_start_merge_input_read(this.newer);
      }
      if (!this.older.eof && this.older.peekable.length === 0) {
        this._maybe_start_merge_input_read(this.older);
      }
      if (this.newer.eof && this.older.eof) {
        this._finish_merge_job();
      }
    }
  }
};

MergeJob.prototype._finish_merge_job = function()
{
  if (this.finished)
    return;
  this.finished = true;
  this.emit("finished");
};
