var net = require("net");
var fs = require("fs");
var JsonDatabaseProtocol = require("./JsonDatabaseProtocol");

// TODO: cut-n-pasted from JsonDatabaseServer
function normalize_payload(payload)
{
  if (Buffer.isBuffer(payload))
    return payload;
  if (typeof(payload) === 'string')
    return new Buffer(payload);
  return new Buffer(JSON.stringify(payload));
}

function JsonDatabaseClient(options)
{
  this.aob = [];
  this.incoming_size = 0;
  this.first_buffer_offset = 0;

  this.state = 'CONNECTING';
  this.port = options.port;
  var self = this;

  this.socket = net.createConnection(options.port, options.host);
  this.socket.on("data", function(data) {
    self.aob.push(data);
    self.incoming_size += data.length;

    while (self.incoming_size >= 12) {
      // compute payload length
      if (self.aob[0].length - self.first_buffer_offset < 12)
        self._consolidate_incoming_buffers();
      var payload_length = self.aob[0].readUInt32(self.first_buffer_offset + 8);

      if (this.incoming_size < 12 + payload_length)
        break;

      if (self.aob[0].length - self.first_buffer_offset < 12 + payload_length)
        self._consolidate_incoming_buffers();

      handle_incoming(self.aob[0].readUInt32(self.first_buffer_offset),
                      self.aob[0].readUInt32(self.first_buffer_offset + 4),
                      self.aob[0].slice(self.first_buffer_offset, payload_length));
      this.incoming_size -= 12 + payload_length;
      this.first_buffer_offset += 12 + payload_length;
    }
  }).on("end", function() {
    // try reconnect?
    ...
  });

  this.send_message(..., 0, {});

  function handle_incoming(response_type, request_id, payload) {
    ...
  };
}
JsonDatabaseClient.prototype.send_message = function(response_type, request_id, payload) {
    payload = normalize_payload);
    var header = new Buffer(12);
    header.writeUInt32LE(response_type, 0);
    header.writeUInt32LE(request_id, 4);
    header.writeUInt32LE(payload.length, 8);
    this.socket.write(header);
    this.socket.write(payload);
  }
};

JsonDatabaseClient.prototype._consolidate_incoming_buffers = function() {
  if (this.first_buffer_offset > 0) {
    this.aob[0] = this.aob[0].slice(this.first_buffer_offset);
    this.first_buffer_offset = 0;
  }
  this.aob = [Buffer.concat(this.aob, this.incoming_size)];
};

exports.create_client = function(options) {
  return new JsonDatabaseClient(options);
};
