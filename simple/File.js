var assert = require("assert");
var fs = require("fs");

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
  }
};

module.exports = File;
