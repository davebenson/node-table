var assert = require("assert");

function TrapCache_Object(id)
{
  this.id = id;
  this.object = null;
  this.first_trap = null;
  this.last_trap = null;
}

function TrapCache_Trap()
{
  this.next_in_object = null;
  this.prev_in_object = null;
  this.target = null;
  this.state = 0;
}

function TrapCache_Target(target_id)
{
  this.target_id = target_id;
  this.traps = {};
  this.n_traps = 0;
}

module.exports = function() {
  this.objects = {};
};

module.exports.prototype.trap = function(id, target_id)
{
  var target = this.targets[target_id];
  if (!target) {
    target = new TrapCache_Target(target_id);
    this.targets[target_id] = target;
  }
  if (id in target.traps)
    return;
  var o = this.objects[id];
  if (!o) {
    o = new TrapCache_Object(id);
    this.objects[id] = o;
  }
  var trap = new TrapCache_Trap();
  trap.target = target;
  target.traps[id] = trap;
  ++target.n_traps;
  if (o.last_trap)
    o.last_trap.next_in_object = trap;
  else
    o.first_trap = trap;
  trap.prev_in_object = o.last_trap;
  o.last_trap = trap;
};

function remove_trap_from_object_list(t, o)
{
  if (t.prev_in_object)
    t.prev_in_object.next_in_object = t.next_in_object;
  else
    o.first_trap = t.next_in_object;
  if (t.next_in_object)
    t.next_in_object.next_in_object = t.prev_in_object;
  else
    o.last_trap = t.prev_in_object;
}

module.exports.prototype.untrap = function(id, target_id)
{
  var target = this.targets[target_id];
  if (!target)
    return;
  var trap = target.traps[id];
  if (!trap)
    return;
  if (trap.state === 1 || trap.state === 2) {
    trap.state = 2;
  } else {
    var o = this.objects[id];
    delete target.traps[id];
    if (--target.n_traps === 0)
      delete this.targets[target_id];
    remove_trap_from_object_list(trap, o);
    if (o.first_trap === null)
      delete this.objects[id];
  }
};

module.exports.prototype.notify = function(object)
{
  var o = this.objects[object.id];
  if (!o)
    return;
  var trap = o.first_trap;
  while (trap !== null) {
    assert(trap.state === 0);
    trap.state = 1;
    trap.callback(object);
    if (trap.state === 2) {
      var next = trap.next_in_object;
      remove_trap_from_object_list(trap, o);
      trap = next;
    } else {
      trap.state = 0;
      trap = trap.next_in_object;
    }
  }
};

module.exports.prototype.untrap_target = function(target_id)
{
  var target = this.targets[target_id];
  if (!target)
    return;
  for (var id in target.traps) {
    var trap = trap_cache.traps[id];
    var obj = this.objects[id];
    remove_trap_from_object_list(trap, obj);
  }
  delete this.targets[target_id];
};
