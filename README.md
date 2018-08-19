# DocumentsDB

[![Pub](https://img.shields.io/pub/v/objectdb.svg)](https://pub.dartlang.org/packages/documentsdb)
[![license](https://img.shields.io/github/license/developerandre/documentsDB.svg)](https://github.com/developerandre/documentsDB/blob/master/LICENSE)
[![GitHub stars](https://img.shields.io/github/stars/developerandre/documentsDB.svg?label=Stars)](https://github.com/developerandre/documentsDB/stargazers)


Persistent embedded document-oriented NoSQL database for [Dart](https://www.dartlang.org/) and [Flutter](https://flutter.io/). 100% Dart.

**CAUTION** This plugin is still in development. **Use at your own risk**. If you notice any bugs you can [create](https://github.com/developerandre/documentsDB/issues/new 'Create issue') an issue on GitHub. You're also welcome to contribute using [pull requests](https://github.com/developerandre/documentsDB/compare 'Pull request'). Please open an issue before spending time on any pull request.

**NB**
The project was originally created by netz-chat on https://github.com/netz-chat/objectdb/blob/master/README.md.

- [How to use](#how-to-use)
- [Flutter](#flutter)
- [Methods](#methods)
    - [find](#find)
    - [insert](#insert)
    - [update](#update)
    - [remove](#remove)
- [Query](#query)
- [Operators](#operators)
    - [Logical](#logical)
    - [Comparison](#comparison)
    - [Modify](#modify)
    - [Examples](#examples)
- [Todo's](#todos)



## How to use
```dart
final path = Directory.current.path + '/my.db';

// create database instance and open
final db = DocumentsDB(path);
db.open();

// insert document into database
db.insert({'name': {'first': 'Some', 'last': 'Body'}, 'age': 18, 'active': true);
db.insert({'name': {'first': 'Someone', 'last': 'Else'}, 'age': 25, 'active': false);

// update documents
db.update({Op.gte: {'age': 80}}, {'active': false});

// remove documents
db.remove({'active': false});

// search documents in database
var result = await db.find({'active': true, 'name.first': 'Some'});

// 'tidy up' the db file
db.tidy();

// close db
await db.close();
```

<!--
## Flutter
 Check out [this](https://github.com/netz-chat/flutter_examples/tree/master/objectdb) project for flutter-examples. -->

## Methods
- `Future<DocumentsDB> db.open([bool tidy = true])` opens database
- `Future<void> db.tidy()` 'tidy up' the .db file
- `Future<void> db.close()` closes database (should be awaited to ensure all queries have been executed)

### find
- `Future<List<Map>> db.find(Map query)` List with all matched documents
- `Future<Map> db.first(Map query)` first matched document
- `Future<Map> db.last(Map query)` last matched document

### insert
- `Future<ObjectId> db.insert(Map document)` insert single document
- `Future<List<ObjectId>> db.insertMany(List<Map> documents)` insert many documents

### update
- `Future<int> db.update(Map query, Map changes, [bool replace = false])` update documents that mach `query` with `changes` (optionally replace whole document)

### remove
- `Future<int> db.remove(Map query)` remove documents that match `query`

## Query
```dart
// Match fields in subdocuments
{Op.gte: {
    'birthday.year': 18
}}

// or-operator
{Op.or:{
    'active': true,
    Op.inArray: {'group': ['admin', 'moderator']}
}}

// not equal to
{Op.not: {'active': false}}
```
**NOTE** Querying arrays is not supportet yet.

## Operators
### Logical
- `and` (default operator on first level)
- `or`
- `not`

### Comparison
- `lt`, `lte`: less than, less than or equal
- `gt`, `gte`: greater than, greater than or equal
- `inList`, `notInList`: value in list, value not in list


### Modify
- `set`: set value
- `max`, `min`: set max or min int value
- `increment`, `multiply`: increment/multiply by
- `unset`: unset key/value
- `rename`: rename key
- currentDate, set current timestamp
-  add: push value
-  addAll: push many value
-  addToSet: push value if not exists
-  insert: insert value at any position in List
- insertAll: insert many value at any position in List
-  pop, remove last value in List
- remove, remove value
-  removeAt, remove value at any position
-  removeWhere, remove value where callback return true
-  clear: clear list/Map
-  slice: sublist List
-  sort : sort List

```dart
{Op.set: {'path.to.key': 'value'}} // set entry['path']['to']['key'] = 'value' (path will be created if not exists)
{Op.max: {'path.to.key': 200}} // set value 200 if value is greater than 200
{Op.min: {'path.to.key': 200}} // set value 200 if value is smaller than 200
{Op.increment: {'path.to.key': -5}} // increment value by negative 5
{Op.multiply: {'path.to.key': 2}} // multiply value by 2
{Op.unset: {'path.to.key': true}} // unset key/value at entry['path']['to']['key'] if exists
{Op.rename: {'path.to.key': 'new_key'}} // new value will be at entry['path']['to']['new_key']


db.update({
  'age': RegExp('[18-20]'),
  Op.gt: {'duration': 500},
}, {
  Op.max: {'stats.score': 100},
  Op.increment: {'stats.level': -5},
});
```

### Examples
```dart
// query
var result = db.find({
    'active': true,
    Op.or: {
        Op.inList: {'state': ['Florida', 'Virginia', 'New Jersey']},
        Op.gte: {'age': 30},
    }
});

// same as
var match = (result['active'] == true && (['Florida', 'Virginia', 'New Jersey'].contains(result['state']) || result['age'] >= 30));
```

## Todo's
- [x] regex match
- [ ] encryption
- [ ] querying arrays
- [ ] benchmarks
- [ ] indexing

## License
See [License](https://github.com/developerandre/documentsDB/blob/master/LICENSE)