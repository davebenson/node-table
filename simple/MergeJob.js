var File = require("./File.js");
var MergeInput = require("./MergeInput.js");

function MergeJob() {
  this.newer = new MergeInput();
  this.older = new MergeInput();

  this.output_file = new File();
  this.output_offset = 0;
  this.last_output = null;
  this.finished = false;
}

MergeJob.prototype.toJSON = function() {
  return {
    output_id: this.output_file.id,
    output_offset: this.output_offset,
    output_n_entries: this.output_file.size_entries,
    older: this.older.toJSON(),
    newer: this.newer.toJSON(),
  };
};

module.exports = MergeJob;
