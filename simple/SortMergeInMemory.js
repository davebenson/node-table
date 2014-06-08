//
// SortMergeInMemory: a list-of-lists of values; each list has
// twice as many input elements as the prior, or null.
//
// Example: suppose we add d,c,b,a to a SortMergeInMemory.
//
//    Action List-of-lists (lol member)
//    ------ --------------------------
//           []
//    Add d
//          [[d]]
//    Add c  
//          null,[c,d]]
//    Add b  
//          [b],[c,d]]
//    Add a  
//          [null,null,[a,b,c,d]]
//
// Merging occurs via the "merge" function.
//

function SortMergeInMemory(compare, merge) {
  this.lol = [];
  this.compare = compare;
  this.merge = merge;
}

// Add a value to the sort-merge machine.
SortMergeInMemory.prototype.add = function(v) {
  var list = [v];
  for (var i = 0; i < this.lol.length; i++) {
    if (this.lol[i] === null) {
      this.lol[i] = list;
      return;
    } else {
      list = this._merge_lists(this.lol[i], list);
      this.lol[i] = null;
    }
  }
  this.lol.push(list);
}

// _merge_lists: Merge two sorted-lists of values together.
SortMergeInMemory.prototype._merge_lists =
function(older, newer) {
  var rv = [];
  var oi = 0, ni = 0;
  while (oi < older.length && ni < newer.length) {
    var cmp = this.compare(older[oi], newer[ni]);
    if (cmp < 0)
      rv.push(older[oi++]);
    else if (cmp > 0)
      rv.push(newer[ni++]);
    else {
      var a = older[oi++];
      var b = newer[ni++];
      rv.push(this.merge(a,b));
    }
  }
  for (; oi < older.length; oi++)
    rv.push(older[oi]);
  for (; ni < newer.length; ni++)
    rv.push(newer[ni]);
  return rv;
};

// reset: reset the state without changing compare/merge functions.
SortMergeInMemory.prototype.reset = function()
{
  this.lol = [];
};

// get_list: return a complete list of all elements, sorted and merged.
//
// The most efficient way to use after exactly a power-of-two number
// of elements.  (If you can't hit it exactly, it's better to stop
// a few short of a power-of-two than to go past it.)
SortMergeInMemory.prototype.get_list = function()
{
  var rv = null;
  var lol = this.lol;
  for (var i = 0; i < lol.length; i++)
    if (lol[i] !== null) {
      if (rv === null)
        rv = lol[i];
      else
        rv = this._merge_lists(lol[i], rv);
    }
  return rv ? rv : [];
};

// get: perform a value lookup.
SortMergeInMemory.prototype.get = function(curried_comparator)
{
  var rv = null;
  for (var i = 0; i < this.lol.length; i++) {
    if (this.lol[i]) {
      var sub_rv = this._array_lookup(curried_comparator, this.lol[i]);
      if (sub_rv) {
        if (rv === null)
	  rv = sub_rv;
        else
          rv = this.merge(sub_rv, rv); 
      }
    }
  }
  return rv;
};

// _array_lookup: perform a looking in a single sorted array.
SortMergeInMemory.prototype._array_lookup = function(curried_comparator, array)
{
  var start = 0, n = array.length;
  while (n > 0) {
    var mid = start + Math.floor(n / 2);
    var rv = curried_comparator(array[mid]);
    if (rv < 0)
      n = mid - start;
    else if (rv > 0) {
      var end = start + n;
      start = mid + 1;
      n = end - start;
    } else
      return array[mid];
  }
  return null;
};

module.exports = SortMergeInMemory;
