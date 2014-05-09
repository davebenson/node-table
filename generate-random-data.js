var assert = require("assert");

var JsonDatabaseClient = require("./JsonDatabaseClient");
var db = JsonDatabaseClient.create_client({
  host: "localhost",
  port: 91949
});

var n_threads = 3;

for (var th = 0; th < n_threads; th++)
  add_one(th);

function random_string()
{
  var rv = "";
  for (var i = 0; i < 10; i++)
    rv += String.fromCharCode(Math.floor(65 + Math.random() * 26));
  return rv;
}
function add_one(thread_no)
{
  console.log("[" + thread_no + "]: request");
  db.add({id:random_string(), value:Math.random()},
   function(err) {
     console.log("[" + thread_no + "]: " + (err ? "failed" : "done"));
     if (err)
       console.log("failed: " + err);
     //setTimeout(add_one, 10, thread_no);
     add_one(thread_no);
   });
}
