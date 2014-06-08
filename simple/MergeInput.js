// MergeInput: holds information about a single input to a MergeJob.
//
// This class is private to MergeJob.
//
// We retain an array of "peekable" values,
// which also include their length (in bytes).

function MergeInput() {
  this.input_offset = 0;
  this.input_file = null;
  this.input_pending = false;
  this.buffer = new Buffer(0);
  this.buffer_available = 0; // amount of buffer in 'peekable'
  this.buffer_valid_length = 0;
  this.peekable = [];
  this.eof = false;
}

MergeInput.prototype.toJSON = function()
{
  return {
    input_id: this.input_file.id,
    input_offset: this.input_offset
  }
};

MergeInput.prototype.remove_first = function(n)
{
  var s = 0;
  for (var i = 0; i < n; i++)
    s += this.peekable[i][1];
  this.input_offset += s;

  // remove 's' bytes from start of buffer
  var new_buffer = new Buffer(this.buffer.length - s);
  this.buffer.copy(new_buffer, 0, s, this.buffer_valid_length);
  this.buffer = new_buffer;
  this.buffer_available -= s;
  this.buffer_valid_length -= s;
  this.peekable.splice(0, n);
  if (this.length === 0
   && this.input_offset >= this.file.size_bytes)
    this.eof = true;
};

module.exports = MergeInput;
