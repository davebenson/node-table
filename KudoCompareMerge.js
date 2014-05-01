

exports.strcmp = function(a,b) {
  if (a < b)
    return -1;
  else if (a > b)
    return 1;
  else
    return 0;
};

exports.compare = function(a,b) {
  return exports.strcmp(a.id, b.id);
};

var skip_keys = {"id":true, "ids":true, "remove_ids":true};

exports.merge = function(a,b)
{
  // if the more recent update is a deletion, then ignore the stuff that happened earlier
  if (b._deleted)
    return b;

  // if the less recent update is a deletion,
  // then just treat the second element as the current value.
  if (a._deleted)
    return b;

  if ("ids" in b) {
    if ("ids" in a) {
      for (var i = 0; i < b.ids.length; i++) {
        var ii = a.ids.indexOf(b.ids[i]);
        if (ii >= 0) {
          a.ids.splice(ii, 1);
        }
        a.ids.push(b.ids[i]);
      }
    } else {
      a.ids = b.ids;
    }
  }
  var remove_ids = [];
  if ("remove_ids" in a) {
    for (var i = 0; i < a.remove_ids.length; i++) {
      if (!("ids" in b) || b.ids.indexOf(a.remove_ids[i]) < 0)
        remove_ids.push(a.remove_ids[i]);
    }
  }
  if ("remove_ids" in b) {
    for (var i = 0; i < b.remove_ids.length; i++) {
      if ("ids" in a) {
        var ai = a.ids.indexOf(b.remove_ids[i]);
        if (ai >= 0)
          a.ids.splice(ai, 1);
      }
      if (remove_ids.indexOf(b.remove_ids[i]) < 0)
        remove_ids.push(b.remove_ids[i]);
    }
  }
  a.remove_ids = remove_ids;

  for (var key in b) {
    if (skip_keys[key])
      continue;
    a[key] = b[key];
  }
  return a;
};

exports.make_final_merge_function = function(default_value) {
  return function(a) {
    if (a._deleted)
      return null;
    else
      return a;
  };
};

