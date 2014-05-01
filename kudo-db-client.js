var readline = require("readline");

var rl = readline.createInterface({
  input: process.stdin,
  output: process.stdout
});

var JsonDatabaseClient = require("./JsonDatabaseClient");
var db = JsonDatabaseClient.create_client({
  host: "localhost",
  port: 91949
});

do_prompt();

function do_prompt()
{
  rl.question(">>> ", function(text) {
    var space = text.indexOf(" ");
    var cmd, extra;
    if (space < 0) {
      cmd = text;
      extra = "";
    } else {
      cmd = text.substring(0,space);
      extra = text.substring(space + 1);
    }
    switch (cmd) {
      case 'get':
        db.get(extra, function(err, result) {
          if (err)
            console.log("! " + err);
          else
            console.log("= " + JSON.stringify(result));
          do_prompt();
        });
        break;
      case 'add':
        db.add(extra, function(err) {
          if (err)
            console.log("! " + err);
          else
            console.log("= OK");
          do_prompt();
        });
        break;
      default:
        console.log("unknown command '" + cmd + "'");
        do_prompt();
        break;
    }
  });
}
