/// Query operators
enum Op {
  // match operators
  and,
  or,
  not,
  //
  lt,
  gt,
  lte,
  gte,
  ne,
  exists, //todo

  //
  inList,
  notInList,
  // update operators
  set,
  unset,
  max,
  min,
  increment,
  multiply,
  rename,
  currentDate,
  add,
  addAll,
  addToSet,
  insert,
  insertAll,
  pop,
  remove,
  removeAt,
  removeWhere,
  clear,
  slice,
  sort
}
