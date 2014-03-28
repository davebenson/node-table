
exports.DocDatabase = function(options) {
  ...
};

// callback is invoked:
//      'created', document
//      'updated', cur_document, old_document
//      'conflicted', cur_document
//      'failed', message
exports.DocDatabase.prototype.update = function(doc, callback) {
...
};

// callback is invoked
//      'found', document
//      'not_found', doc_id
//      'failed, message
exports.DocDatabase.prototype.get = function(doc_id, callback) {
...
};

//      'deleted', doc_id
//      'conflicted', cur_document
//      'failed', message
exports.DocDatabase.prototype.remove = function(doc, callback) {
...
};

