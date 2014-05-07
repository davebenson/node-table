

function SortMergeInMemory(compare, merge) {
  this.lol = [];
  this.compare = compare;
  this.merge = merge;
}
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

SortMergeInMemory.prototype.reset = function()
{
  this.lol = [];
};
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
