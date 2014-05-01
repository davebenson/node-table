
var net = require("net");
var JsonDatabaseProtocol = require("./JsonDatabaseProtocol");
var TrapCache = require("./TrapCache");
var next_connection_id = 1;

function allocate_connection_id()
{
  return "con" + (next_connection_id++);
}

function normalize_payload(payload)
{
  if (Buffer.isBuffer(payload))
    return payload;
  if (typeof(payload) === 'string')
    return new Buffer(payload);
  return new Buffer(JSON.stringify(payload));
}

function handle_socket(socket, server, db)
{
  var aob = [];
  var aob_total = 0;
  var first_buffer_used = 0;
  var trap_cache = new TrapCache();
  var connection_id = allocate_connection_id();
  socket.on("data", function(data) {
    aob.push(data);
    aob_total += data.length;
    while (aob_total >= 12) {
      if (aob[0].length < first_buffer_used + 12) {
        if (first_buffer_used > 0)
          aob[0] = aof[0].slice(first_buffer_used);
        first_buffer_used = 0;
        aob = [Buffer.concat(aob)];
      }

      var payload_len = aob[0].readUInt32LE(4);
      if (aob_total < 12 + payload_len) {
        break;
      }
      if (aob[0].length - first_buffer_used < 12 + payload_len) {
        if (first_buffer_used > 0)
          aob[0] = aof[0].slice(first_buffer_used);
        first_buffer_used = 0;
        aob = [Buffer.concat(aob)];
      }
      handle_input_command(aob[0].readUInt32LE(0),
                           aob[0].readUInt32LE(8),
                           aob[0].slice(12, 12+payload_len));
      first_buffer_used += 12 + payload_len;
    }
  }).on("end", function() {
    // untrap all!
    trap_cache.untrap_target(connection_id);
  });

  function handle_input_command(cmd, request_id, payload)
  {
    switch(cmd) {
      case JsonDatabaseProtocol.GET_REQUEST: // get command
        var id = payload.toString();
        db.get(id, function(err, rv) {
          if (err) { 
            send_error_response(err.toString());
          } else if (rv === null) {
            send_response(JsonDatabaseProtocol.GET_RESPONSE_NOT_FOUND, request_id, "");
          } else {
            send_response(JsonDatabaseProtocol.GET_RESPONSE_FOUND, request_id, rv);
          }
        });
        break;
      case JsonDatabaseProtocol.UPDATE_REQUEST: // update command
        var doc = JSON.parse(payload.toString());
        db.add(doc, function(err) {
          if (err) {
            send_error_response(err.toString());
          } else {
            send_response(JsonDatabaseProtocol.UPDATE_RESPONSE_OK, request_id, "");
          }
        });
        break;
      case JsonDatabaseProtocol.TRAP_REQUEST: // trap command
        trap_cache.trap(payload.toString(), connection_id);
        break;
      case JsonDatabaseProtocol.UNTRAP_REQUEST: // untrap command
        trap_cache.untrap(payload.toString(), connection_id);
        break;
      default:
        // unknown command
        send_error_response("unknown command " + cmd + "; ignoring request");
        break;
    }
  }
  function send_error_response(request_id, message)
  {
    send_response(JsonDatabaseProtocol.ERROR_RESPONSE, request_id, message);
  }
  function send_response(response_type, request_id, payload)
  {
    var header = new Buffer(12);
    payload = normalize_payload(payload);
    header.writeUInt32LE(response_type, 0);
    header.writeUInt32LE(request_id, 4);
    header.writeUInt32LE(payload.length, 8);
    socket.write(header);
    socket.write(payload);
  }
}

exports.create_server = function(options)
{
  var db = options.database;
  var port = options.port;
  var server = net.createServer(function(socket) {
    handle_socket(socket, server, db);
  });
  server.listen(port);
  return server;
};
