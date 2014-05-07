var assert = require("assert");

var JsonDatabaseClient = require("./JsonDatabaseClient");
var db = JsonDatabaseClient.create_client({
  host: "localhost",
  port: 91949
});

setTimeout(add_one, 1000);
add_one();

function random_string()
{
  var rv = "";
  for (var i = 0; i < 10; i++)
    rv += String.fromCharCode(Math.floor(65 + Math.random() * 26));
  return rv;
}
function add_one()
{
  console.log("add_one");
  db.add({id:random_string(), value:Math.random()},
   function(err) {
     if (err)
       console.log("failed: " + err);
     add_one();
   });
}
