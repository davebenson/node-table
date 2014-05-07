var net = require("net");
var fs = require("fs");
var assert = require("assert");
var JsonDatabaseProtocol = require("./JsonDatabaseProtocol");

function Request(request_id, callback)
{
  this.request_id = request_id;
  this.callback = callback;
}

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
  this.host = options.host;
  this.port = options.port;
  this.reconnect_period = 250;
  this.reconnect_timer = null;

  this.pending_requests_by_request_id = {};
  this.next_request_id = 1;

  this._do_connect();
}

JsonDatabaseClient.prototype._do_connect = function()
{
  var self = this;
  var socket = net.createConnection(this.port, this.host);
  this.socket = socket;
  this.socket.on("data", function(data) {
    assert(Buffer.isBuffer(data));
    console.log("got data (" + data.length + " bytes)");
    if (self.socket !== socket)
      return;
    console.log("data=" + data);
    self.aob.push(data);
    self.incoming_size += data.length;

    while (self.incoming_size >= 12) {
      // compute payload length
      console.log("first buffer length=" + (self.aob[0].length - self.first_buffer_offset ));
      if (self.aob[0].length - self.first_buffer_offset < 12)
        self._consolidate_incoming_buffers();
      var payload_length = self.aob[0].readUInt32LE(self.first_buffer_offset + 8);
      console.log("payload_length=" + payload_length);
      console.log(self.aob[0].slice(self.first_buffer_offset,self.first_buffer_offset+12).toString("hex"));

      if (self.incoming_size < 12 + payload_length)
        break;

      if (self.aob[0].length - self.first_buffer_offset < 12 + payload_length)
        self._consolidate_incoming_buffers();

      var payload_start = self.first_buffer_offset + 12;
      var payload_end = payload_start + payload_length;
      handle_incoming(self.aob[0].readUInt32LE(self.first_buffer_offset),
                      self.aob[0].readUInt32LE(self.first_buffer_offset + 4),
                      self.aob[0].slice(payload_start, payload_end));
      self.incoming_size -= 12 + payload_length;
      self.first_buffer_offset += 12 + payload_length;
      if (self.first_buffer_offset === self.aob[0].length) {
        self.aob.splice(0,1);
        self.first_buffer_offset = 0;
      }
    }
  }).on("connect", function() {
    if (self.socket !== socket)
      return;
    if (self.state !== 'CONNECTING')
      console.log("bad state: 'connect' which not 'CONNECTING'");
    self.state = 'CONNECTED';
  }).on("error", function(err) {
    console.log("kudo-db-client: error " + err);
  }).on("close", function() {
    console.log("client: close");
    if (self.socket !== socket)
      return;
    // try reconnect?
    self.state = 'DISCONNECTED';
    assert(self.reconnect_timer === null);
    self.reconnect_timer = setTimeout(function() {
      self.reconnect_timer = null;
      self._do_connect();
    }, 500);
  });

  self.send_message(JsonDatabaseProtocol.LOGIN, 0, {});

  function handle_incoming(response_type, request_id, payload) {
    var request = self.pending_requests_by_request_id[request_id];
    if (request)
      delete self.pending_requests_by_request_id[request_id];
    console.log("> GOT " + JsonDatabaseProtocol.code_to_name[response_type] +
              " [payload size=" + payload.length + "]");
    switch (response_type) {
      case JsonDatabaseProtocol.LOGGED_IN:
        console.log("logged in");
        break;
      case JsonDatabaseProtocol.LOGG_IN_FAILED: 
        console.log("log-in failed");
        break;
      case JsonDatabaseProtocol.GET_RESPONSE_FOUND:
        if (!request) {
          self.notify_error("GET response with no matching request_id");
        } else {
          request.callback(null, JSON.parse(payload.toString()));
        }
        break;
      case JsonDatabaseProtocol.GET_RESPONSE_NOT_FOUND:
        if (!request) {
          self.notify_error("GET response with no matching request_id");
        } else {
          request.callback(null, null);
        }
        break;
      case JsonDatabaseProtocol.UPDATE_RESPONSE_OK:
        if (!request) {
          self.notify_error("UPDATE response with no matching request_id");
        } else {
          request.callback(null);
        }
        break;
      case JsonDatabaseProtocol.ERROR_RESPONSE:
        if (!request) {
          console.log("request=" + request_id + "; msglen=" + payload.length);
          console.log("msg=" + payload.toString());
          self.notify_error("ERROR response with no matching request_id");
        } else {
          var txt = "remote error: " + payload.toString();
          console.log(txt);
          self.notify_error(txt);
          request.callback(new Error(txt));
        }
        break;
      case JsonDatabaseProtocol.CHANGED_MESSAGE: {
        self.object_changed(JSON.parse(payload.toString()));
        break;
      }
      default:
        console.log("unexpected message " + response_type + " from server");
        break;
    }
  }
};
JsonDatabaseClient.prototype.send_message = function(response_type, request_id, payload) {
  console.log("> SEND " + JsonDatabaseProtocol.code_to_name[response_type]);
  payload = normalize_payload(payload);
  var header = new Buffer(12);
  header.writeUInt32LE(response_type, 0);
  header.writeUInt32LE(request_id, 4);
  header.writeUInt32LE(payload.length, 8);
  var packet = Buffer.concat([header, payload]);
  this.socket.write(packet);
};

JsonDatabaseClient.prototype._consolidate_incoming_buffers = function() {
  if (this.first_buffer_offset > 0) {
    this.aob[0] = this.aob[0].slice(this.first_buffer_offset);
    this.first_buffer_offset = 0;
  }
  this.aob = [Buffer.concat(this.aob, this.incoming_size)];
};

JsonDatabaseClient.prototype.notify_error = function(err_msg)
{
};
JsonDatabaseClient.prototype.object_changed = function(err_msg)
{
};

JsonDatabaseClient.prototype._allocate_request_id = function()
{
  return this.next_request_id++;
};
JsonDatabaseClient.prototype.get = function(key, callback)
{
  var request_id = this._allocate_request_id();
  var req = new Request(request_id, callback);
  this.pending_requests_by_request_id[request_id] = req;
  this.send_message(JsonDatabaseProtocol.GET_REQUEST, request_id, key);
};

JsonDatabaseClient.prototype.add = function(object, callback)
{
  var request_id = this._allocate_request_id();
  var req = new Request(request_id, callback);
  this.pending_requests_by_request_id[request_id] = req;
  console.log("sending update message");
  this.send_message(JsonDatabaseProtocol.UPDATE_REQUEST, request_id, object);
};

exports.create_client = function(options) {
  return new JsonDatabaseClient(options);
};
