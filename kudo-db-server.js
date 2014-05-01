var JsonDatabaseServer = require("./JsonDatabaseServer");
var KudoCompareMerge = require("./KudoCompareMerge");
var JsonLookupTableSimple = require("./JsonLookupTable").JsonLookupTableSimple;

var table = new JsonLookupTableSimple({
  dir: "kudo-db",
  compare: KudoCompareMerge.compare,
  merge: KudoCompareMerge.merge,
});

JsonDatabaseServer.create_server({database: table, port: 91949});


