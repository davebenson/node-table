var fs = require("fs");

exports.write_sync_n = function(fd, data, position)
{
  var at = 0, rem = data.length, pos = position;
  while (rem > 0) {
    var n = fs.writeSync(fd, data, at, rem, pos);
    at += n;
    rem -= n;
    pos += n;
  }
};

