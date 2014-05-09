var assert = require("assert");
var fs = require("fs");
var util = require("util");

function File() {
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
  this.filename = null;
}
File.prototype.toJSON = function() {
  assert(this.start_input_entry !== null);
  assert(this.n_input_entries !== null);
  return {
    id: this.id,
    start_input_entry: this.start_input_entry,
    n_input_entries: this.n_input_entries,
    size_bytes: this.size_bytes,
    size_entries: this.size_entries,
  };
};
File.prototype.ref = function() {
  assert(this.ref_count > 0);
  ++this.ref_count;
};
File.prototype.unref = function() {
  console.log("unref of file " + this.id);
  assert(this.ref_count > 0);
  if (--this.ref_count === 0) {
    var self = this;
    fs.unlink(this.filename, function(err) {
      if (err)
        console.log("unlink " + self.filename + " failed: " + err);
    });
    var fd = this.fd;
    this.fd = -1;
    fs.close(fd, function(err) {
      if (err)
        console.log("error closing File fd");
    });
  }
};

function _find_line_range(buffer, pos_0_ok)
{
  var start = Math.floor(buffer.length / 2);
  var end = start;

  while (start >= 0) {
    if (buffer[start] === 0x0a) {
      break;
    }
    start--;
  }
  var has_start = false;
  if (start < 0 && pos_0_ok) {
    start = 0;
    has_start = true;
  } else if (start >= 0) {
    start = start + 1;
    has_start = true;
  }
    
  while (end < buffer.length) {
    if (buffer[end] === 0xa0) {
      break;
    }
    end++;
  }

  var has_end = (end < buffer.length);
  if (has_start && has_end) {
    return [start, end + 1];            // include trailing newline
  } else if (!has_start && !has_end) {
    return null;
  } else if (has_start) {
    end = start;
    for (start = end - 2; start >= 0; start--) {
      if (buffer[start] === 0x0a) {
        return [start+1, end];
      }
    }
    if ((start === -1 && pos_0_ok) || start >= 0) {
      return [start + 1, end];
    }
    return null;
  } else /* has_end */ {
    start = end;
    while (end < buffer.length) {
      if (buffer[end] === 0x0a)
        break;
      end++;
    }
    if (end === buffer.length)
      return null;
    return [start, end + 1];
  }
}

function find_line_range(buffer, pos_0_ok)
{
  console.log("find_line_range");
  var rv = _find_line_range(buffer, pos_0_ok);
  console.log("rv=" + util.inspect(rv));
  return rv;
}

function my_fs_read(fd, buffer, offset, length, position, cb) {
  //console.log("my_fs_read=" + util.inspect(arguments));
  var nread = fs.readSync(fd, buffer, offset, length, position);
  //console.log("done read: " + nread);
  setImmediate(cb, null, nread);
}

function readn_buffer(fd, buffer, offset, length, position, callback)
{
  if (length === 0)
    callback(null);
  else {
    fs.read(fd, buffer, offset, length, position, function(err, bytesRead) {
      if (err) {
        console.log("readn_buffer: failed: " + err);
        callback(err);
      } else
        readn_buffer(fd, buffer, offset + bytesRead, length - bytesRead, position + bytesRead, callback);
    });
  }
}
File.prototype.do_search_file = function(curried_comparator, callback) {
  var self = this;
  self.ref();
  do_search_range(0, self.size_bytes);

  function do_search_range(start, length) {
    console.log("do_search_range: file=" + self.id + "; start=" + start +"; len=" + length);
    if (length === 0) {
      callback(null, null);
    } else if (length <= 4096) {
      var buf = new Buffer(length);
      readn_buffer(self.fd, buf, 0, length, start, function(err) {
        if (err) {
          callback(err);
          self.unref();
        } else {
          // split buffer into lines
          var strs = buf.slice(0,buf.length - 1).toString().split("\n");

          //console.log("doing binary search of " + strs.length + " entries from " + file.id);

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
              self.unref();
              return;
            }
          }
          callback(null, null);
          self.unref();
        }
      });
    } else {
      // read 4096 bytes from middle of search area
      var buf = new Buffer(4096);
      var buf_position = start + Math.floor((length - 4096) / 2);
      //console.log("calling readn_buffer: position=" + buf_position + "; length=" + 4096);
      readn_buffer(self.fd, buf, 0, 4096, buf_position, function(err) {
        //console.log("readn response: err=" + err);
        if (err) {
          callback(err);
          self.unref();
        } else {
          try_handle_buf();
        }
      });
      function try_handle_buf() {
        var line_range = find_line_range(buf, buf_position === start);
        console.log("try_handle_buf: position=" + buf_position + "; length=" + buf.length + "; line-range=" + JSON.stringify(line_range));
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
            readn_buffer(self.fd, buf, 0, expand_left, buf_position, function(err) {
              if (err && !first_err)
                first_err = err;
              decr_pending();
            });
          }
          if (expand_right > 0) {
            pending++;
            readn_buffer(self.fd, buf, buf.length - expand_right, expand_right,
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
                self.unref();
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
            self.unref();
          }
        }
      }
    }
  }
};

module.exports = File;
