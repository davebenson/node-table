
exports.LOGIN = 0x88997700;
exports.LOGGED_IN = 0x88997701;
exports.LOG_IN_FAILED = 0x88997702;

exports.GET_REQUEST = 0x8899aa00;
exports.GET_RESPONSE_FOUND = 0x8899bb00;
exports.GET_RESPONSE_NOT_FOUND = 0x8899bb01;

exports.UPDATE_REQUEST = 0x8899aa01;
exports.UPDATE_RESPONSE_OK = 0x8899bb02;

exports.TRAP_REQUEST = 0x8899aa02;
exports.UNTRAP_REQUEST = 0x8899aa03;

exports.CHANGED_MESSAGE = 0x8899bb05;

exports.ERROR_RESPONSE = 0x8899cc00;

exports.code_to_name = {};
var codes = ["LOGIN", "LOGGED_IN", "LOG_IN_FAILED",
             "GET_REQUEST", "GET_RESPONSE_FOUND", "GET_RESPONSE_NOT_FOUND",
             "UPDATE_REQUEST", "UPDATE_RESPONSE_OK",
             "TRAP_REQUEST", "UNTRAP_REQUEST",
             "CHANGED_MESSAGE",
             "ERROR_RESPONSE"];
for (var i = 0; i < codes.length; i++) {
  exports.code_to_name[exports[codes[i]]] = codes[i];
}

