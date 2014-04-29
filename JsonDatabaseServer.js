
var net = require("net");

function handle_socket(socket, server, db)
{
  var aob = [];
  var aob_total = 0;
  var first_buffer_used = 0;
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
    db.untrap_connection(connection_id);
  });

  function handle_input_command(cmd, request_id, payload)
  {
    switch(cmd) {
      case 0x8899aa00: // get command
        var id = payload.toString();
        db.get(id, function(err, rv) {
          if (err) { 
            send_error_response(...);
          } else if (rv === null) {
            ...
          } else {
            ...
          }
        });
        break;
      case 0x8899aa01: // update command
        var doc = JSON.parse(payload.toString());
        db.add(doc, function(err) {
          if (err) {
            send_error_response(...);
          } else {
            ...
          }
        });
        break;
      case 0x8899aa02: // trap command
        db.trap_cache.trap(payload.toString(), connection_id, db);
        break;
      case 0x8899aa03: // untrap command
        db.trap_cache.untrap(payload.toString(), connection_id, db);
        break;
      default:
        // unknown command
        send_error_response(...);
        break;
    }
  }
  function send_error_response(...)
  {
  }
}

exports.create_server = function(options)
{
  var db = options.database;
  var port = options.port;
  var server = net.create_server(function(socket) {
    handle_socket(socket, server, db);
  });
  server.listen(port);
  return server;
};
