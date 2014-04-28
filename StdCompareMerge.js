
exports.strcmp = function(a,b) {
  if (a < b)
    return -1;
  else if (a > b)
    return 1;
  else
    return 0;
};

exports.compare = function(a,b) {
  var rv = exports.strcmp(a.type, b.type);
  if (rv === 0)
    rv = exports.strcmp(a.key, b.key);
  return rv;
};

exports.merge = function(a,b)
{
  // if the more recent update is a deletion, then ignore the stuff that happened earlier
  if (b.deleted)
    return b;

  // if the less recent update is a deletion,
  // then just treat the second element as the current value.
  if (a.deleted)
    return b;

  if ("value" in b)
    return b;
  else if ("value" in a) {
    var b_updates = b.updates ? b.updates : [];
    for (var bi = 0; bi < b_updates.length; bi++)
      apply_update(a.value, b_updates[bi]);
    return a;
  } else {
    var a_updates = a.updates ? a.updates : [];
    var b_updates = b.updates ? b.updates : [];
    for (var bi = 0; bi < b_updates.length; bi++) {
      var handled = false;
      for (var ai = a_updates.length - 1; ai >= 0; ai--) {
        var new_update = coalesce_updates(a_updates[ai], b_updates[bi]);
        if (new_update) {
          if (new_update[0] === 'nop')
            a_updates.splice(ai,1);
          else
            a_updates[ai] = new_update;
          handled = true;
          break;
        }
      }
      if (!handled)
        a_updates.push(b_updates[bi]);
    }
    return a;
  }
};

exports.make_final_merge_function = function(default_value) {
  return function(a) {
    if (a.deleted)
      return null;
    else if (a.value)
      return a;
    else if (a.updates && a.updates.length > 0) {
      a.value = default_value;
      for (var i = 0; i < a.updates.length; i++)
        a.value = apply_update(a.value, a.updates[i]);
      delete a.updates;
      return a;
    } else {
      a.value = default_value;
      delete a.updates;
      return a;
    }
  };
};

// May modify value, and may return 'value' (post-modification).
function apply_update(value, update)
{
  var update_type = update[0];
  var update_path = update[1];
  var update_info = update[2];
  switch (update_type) {
    case 'add':
      if (update_path.length === 0)
        return value + update_info;
      else {
        var v = value;
        for (var p = 0; p < update_path.length - 1; p++) {
          if (!(update_path[p] in v))
            v[update_path[p]] = {};
          v = v[update_path[p]];
        }
        v[update_path[p]] += update_info;
        return value;
      }
    case 'append':
      var v = value;
      for (var p = 0; p < update_path.length; p++) {
        if (!(update_path[p] in v))
          v[update_path[p]] = p == update_path.length - 1 ? [] : {};
        v = v[update_path[p]];
      }
      v.push(update_info);
      return value;
    case 'append_unique':
      var v = value;
      for (var p = 0; p < update_path.length; p++) {
        if (!(update_path[p] in v))
          v[update_path[p]] = p == update_path.length - 1 ? [] : {};
        v = v[update_path[p]];
      }
      var i = v.indexOf(update_info);
      if (i >= 0) {
        if (i != v.length - 1) {
          v.splice(i,1);
          v.push(update_info);
        }
      } else {
        v.push(update_info);
      }
      return value;
    case 'set':
      if (update_path.length === 0) {
        return update_info;
      else {
        for (var p = 0; p < update_path.length - 1; p++) {
          if (!(update_path[p] in v))
            v[update_path[p]] = {};
          v = v[update_path[p]];
        }
        v[update_path[p]] = update_info;
      }
      return value;
    case 'delete':
      assert (update_path.length > 0);
      for (var p = 0; p < update_path.length - 1; p++) {
        if (!(update_path[p] in v))
          return value;
        v = v[update_path[p]];
      }
      delete v[update_path[p]];
      return value;
    case 'clip_list':
      for (var p = 0; p < update_path.length; p++) {
        if (!(update_path[p] in v))
          return value;
        v = v[update_path[p]];
      }
      if (v.length > update_info.max_length) {
        if (update_info.remove_from_front)
          v.splice(0, v.length - update_info.max_length);
        } else {
          v.splice(update_info.max_length, v.length - update_info.max_length);
        }
      }
      return value;
  }
}

// Returns:
//         'null' if the updates are not coalesceable.
// -or-    ["nop"] if the updates cancel
// -or-    [update_type, update_path, update_info] for the reduced update.
function coalesce_updates(update_a, update_b)
{
  //var a_type = update_a[0];
  var a_path = update_a[1];
  //var a_info = update_a[2];
  //var b_type = update_b[0];
  var b_path = update_b[1];
  //var b_info = update_b[2];
  switch (compare_paths(a_path, b_path)) {
    case 'equal':
      return coalesce_updates_to_same_path(update_a, update_b);

    case 'a_contains_b': {
      var a_type = update_a[0];
      var b_type = update_b[1];
      if (!update_type_can_refer_to_object(a_type)) {
        return ['error', ....];
      }
      if (a_type === 'set') {
        ... apply 'b' to subvalue
        return update_a;
      }
      ...
      break;
    }

    case 'b_contains_a': {
      var a_type = update_a[0];
      var b_type = update_b[1];
      if (!update_type_can_refer_to_object(b_type)) {
        return ['error', ....];
      }
      if (b_type === 'set') {
        return update_b;
      }
      if (b_type === 'delete') {
        return update_b;
      }
      assert(false);
    }

    case 'disjoint':
      return null;
  }
}


