var File = require("./File");
var fs = require("fs");
var assert = require("assert");

function make_curried_comparator(key)
{
  return function(value) {
    var vk = value.key;
    if (key < vk)
      return -1;
    else if (key > vk)
      return 1;
    else
      return 0;
  };
}

function run_test(options, done_callback)
{
  var keys = options.keys;
  var key_to_value = options.key_to_value;
  var missing_keys = options.missing_keys;
  var filename = options.filename;
  var fd = fs.openSync(filename, "w");
  var len = 0;
  for (var i = 0; i < keys.length; i++) {
    var j = key_to_value(keys[i]);
    var b = new Buffer(JSON.stringify(j) + "\n");
    fs.writeSync(fd, b, 0, b.length, len);
    len += b.length;
  }
  fs.closeSync(fd);

  var file = new File();
  file.fd = fs.openSync(filename, "r");
  file.filename = filename;
  file.id = 1;
  file.size_bytes = len;
  file.size_entries = keys.length;

  var key_index = 0;
  test_next_key();
  function test_next_key() {
    var key = keys[key_index];
    key_index++;
    console.log("doing search-file with " + key);
    file.do_search_file(make_curried_comparator(key), function(err, value) {
      assert(err === null);
      assert(value !== null);
      if (key_index === keys.length) {
        key_index = 0;
        test_next_not_found_key();
      } else {
        test_next_key();
      }
    });
  }
  function test_next_not_found_key() {
    var key = missing_keys[key_index];
    key_index++;
    file.do_search_file(make_curried_comparator(key), function(err, value) {
      assert(err === null);
      assert(value === null);
      if (key_index === missing_keys.length) {
        file.unref();
        done_callback();
      } else {
        test_next_not_found_key();
      }
    });
  }
}

var keys_small = [];
var keys_medium = [];
var keys_large = [];
var missing_keys = ["0"];
for (var a = 0; a < 26; a++) {
  var ka = String.fromCharCode(65+a);
  missing_keys.push(ka + ka + ka + ka + ka);
  keys_small.push(ka);
  for (var b = 0; b < 26; b++) {
    var kb = String.fromCharCode(65+b);
    keys_medium.push(ka + kb);
    for (var c = 0; c < 26; c++) {
      var kc = String.fromCharCode(65+c);
      keys_large.push(ka + kb + kc);
    }
  }
}

function k2v__value_equals_key(key) {
  return {key:key, value:key};
}

var tests = [
  {
    keys:keys_small,
    key_to_value: k2v__value_equals_key,
    missing_keys: missing_keys,
    filename: "tmp.1"
  }
];

var test_index = 0;
run_next_test();

function run_next_test() {
  run_test(tests[test_index], function() {
    test_index++;
    if (test_index === tests.length)
      return;
    run_next_test();
  });
}
