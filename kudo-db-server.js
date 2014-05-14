var JsonDatabaseServer = require("./JsonDatabaseServer");
var KudoCompareMerge = require("./KudoCompareMerge");
var JsonLookupTableSimple = require("./JsonLookupTable").JsonLookupTableSimple;

var table = new JsonLookupTableSimple({
  dir: "kudo-db",
  compare: KudoCompareMerge.compare,
  merge: KudoCompareMerge.merge,
  final_merge: KudoCompareMerge.final_merge,
  make_curried_comparator: function(a) {
    return function(b) { return KudoCompareMerge.strcmp(a, b.id); }
  }
});

JsonDatabaseServer.create_server({database: table, port: 91949});


