var lookup_table = require("./JsonLookupTable.js");
var assert = require("assert");
assert(lookup_table);

var tests = [
  {name: "simple",
   steps: [
     ["create_table", {}],
     ["get", "test", null],
     ["add", {key:"test", value:1}],
     ["add", {key:"test", value:2}],
     ["get", "test", {key:"test", value:3}],
   ]
  },
];

var test_index = 0;
var table_dir_seqno = 0;

run_next_test();

function run_next_test() {
  if (test_index === tests.length) {
    console.log("all done");
    process.exit(0);
  }
  run_test(function() {
    test_index++;
    run_next_test();
  });
}

function run_test(callback)
{
  process.stderr.write("Running test " + tests[test_index].name + " ");
  run_test_step(0, {}, callback);
}

function strcmp(a,b) 
{
  return (a < b) ? -1
       : (a > b) ? +1
       : 0;
}

function run_test_step(step_index, info, callback)
{
  var steps = tests[test_index].steps;
  if (step_index === steps.length) {
    process.stderr.write(" done.\n");
    callback();
    return;
  }

  var instr = tests[test_index].steps[step_index];
  process.stderr.write("[" + instr[0] + "] ");
  switch (instr[0]) {
    case 'create_table': {
      var ct_options = {};
      if ("compare" in instr[1])
        ct_options.compare = instr[1].compare;
      else
        ct_options.compare = function(a,b) { return strcmp(a.key, b.key); };
      if ("merge" in instr[1])
        ct_options.merge = instr[1].merge;
      else
        ct_options.merge = function(a,b) { return {key:a.key, value:a.value + b.value}; };
      var table_name = ("table_name" in instr[1]) ? instr[1].table_name : "table";
      info[table_name] = {};
      if ("make_key_comparator" in instr[1])
        info[table_name].make_key_comparator = instr[1].make_key_comparator;
      else
        info[table_name].make_key_comparator = function(a) { return function(b) { return strcmp(a, b.key); }; };
      ct_options.dir = "./test-table-" + table_dir_seqno++;
      info[table_name].table = new lookup_table.JsonLookupTableSimple(ct_options);
      run_test_step(step_index + 1, info, callback);
      break;
    }
    case 'get': {
      var table_name = "table";
      var cc = info[table_name].make_key_comparator(instr[1]);
      var table = info[table_name].table;
      table.get(cc, function(err, value) {
        if (err) {
          console.log("get failed: " + err);
        } else {
          assert.deepEqual(value, instr[2]);
          run_test_step(step_index + 1, info, callback);
        }
      });
      break;
    }
    case 'add': {
      var table_name = "table";
      var table = info[table_name].table;
      table.add(instr[1], function(err) {
        if (err) {
          console.log("add failed: " + err);
        } else {
          run_test_step(step_index + 1, info, callback);
        }
      });
      break;
    }
    default:
      assert(false);
  }
}
