import 'dart:io';

import 'package:documentsdb/documentsdb.dart';

void main() async {
  final path = Directory.current.path + '/test/';

  /** 
     timestampData: if true fields createdAt and updatedAt will be added on insertion 
     
     inMemoryOnly : if true path will be ignore and all data will be in memory
    **/
  final db =
      DocumentsDB(path + 'test.db', timestampData: true, inMemoryOnly: false);
  await db.open();

  /**   Index field
   *    fieldNames is List of fields to index
   *    unique : if true verify if value of each field of List has unique value
   *    mandatory : if true each field of List is require on insertion and cannot remove on update
   * 
   * expireAfterSeconds options if set will remove indexing on each field on List at specified date
   * 
   * 
   */
  await db.ensureIndex(['type.at'], mandatory: true, unique: true);

  /**  Trigger  
   *   onInsert, onUpdate, onRemove to add trigger on these events
   * 
   *   There are also possibility to set trigger before or after each event
   * 
   *  **/
  Function condition = (dataToInsert) {
    //print('condition $dataToInsert');
    return true;
  };

  db.trigger.onInsert.onBefore.addAll({
    condition: (dataToInsert) {
      //print('trigger on insert $insertedData');
    }
  });

  /*
      Listen insert,update,remove event
    */
  db.onUpdate.listen((data) {
    //print('on update $data');
  });
  db.onInsert.listen((data) {
    //print('on onInsert $data');
  });
  db.onRemove.listen((data) {
    //print('on onRemove $data');
  });
  db.watch().listen((data) {
    //print('db changes without query $data');
  });
  db.watch({"a": "1"}).listen((data) {
    //print('db changes with query $data');
  });

  db.insert({'a': '6'}).then((data) {
    print("insert  $data");
  }).catchError((e) {
    print("catch insert 6 $e");
  });

  db.insertMany([
    {
      'type': {'at': '1'}
    },
    {
      'type': {'at': '2'}
    },
    {
      'type': {'at': '3'}
    },
    {
      'type': {'at': '4'}
    }
  ]).then((data) {
    print("insert many $data");
  }).catchError((e) {
    print("catch insertion many $e");
  });
  db.update({
    "type.at": "4"
  }, {
    'type': {'eee': '4'}
  }, upsert: true).then((data) {
    print("update $data");
  }).catchError((e) {
    print("catch with query `'type.at': '4'` $e");
  });

  db.findAndUpdate({
    "type.at": "four"
  }, {
    'type': {'eee': 'four'}
  }).then((count) {
    print("update where type.at equals four : $count");
  }).catchError((e) {
    print("failed to update where type.at equals four $e");
  });

  db.findOneAndUpdate({
    "type.at": "one"
  }, {
    'type': {'eee': '4'}
  }).then((count) {
    print("update where type.at equals one : $count");
  }).catchError((e) {
    print("failed to update where type.at equals one $e");
  });

  db.update({
    'type.at': '4'
  }, {
    Op.unset: {"type.at": true}
  }, upsert: true).then((data) {
    print("unset where type.at equals 4 : $data");
  }).catchError((e) {
    print("failed to unset where type.at equals 4 : $e");
  });

  print(await db.remove({'a': RegExp('[3-6]')}));

  print(await db.removeOne({'a': RegExp('[3-6]')}));

  db.findOneAndRemove({"type.at": "4"}).then((count) {
    print("findOneAndRemove where at = '4' $count");
  }).catchError((e) {
    print("failed to findOneAndRemove where at = '4': $e");
  });

  print(await db.find({
    Op.exists: {'e.c.d[0]': true, 'a': true}
  }, projection: {
    'e': false,
    'a': false
  }, sort: {
    'e.c.d[0].dsqd': 1,
    'a': 1
  }, skip: 2, limit: 1));

  db.insertMany([
    {'a': '1'},
    {'a': '2'},
    {'a': '3'},
    {'a': '4'},
  ]).then((ids) async {
    print("ids = $ids");
    print(await db.find({'_id': ids[2]}));
  }).catchError((e) {
    print('failed to insert many a: $e');
  });

  /* db.insert({'a': '6'});
  db.insert({'a': '7'});
  db.insert({'a': '8'});
  db.insert({'a': '9'}); */

  db.update({
    Op.gt: {'a': '0'},
    'a': RegExp('[4-7]'),
  }, {
    Op.max: {'n': 100},
    'b': 'c'
  }).then((count) {
    print("update to max where Op.gt $count");
  }).catchError((e) {
    print("failed to update to max where Op.gt $e");
  });

  db.update({
    Op.gt: {'a': '0'},
    'a': RegExp('[4-7]'),
  }, {
    Op.rename: {'b': 'ü'},
    Op.unset: {'n': true},
    Op.addToSet: {"script": 'dart', "year": "2018"},
    Op.currentDate: {"year": true}
  }).then((count) {
    print("update where Op.gt $count");
  }).catchError((e) {
    print("failed to update where Op.gt $e");
  });

  print(await db.last({
    Op.gt: {'a': 0},
  }));

  print(await db.find({
    Op.gt: {'a': 0},
  }));

  db.removeIndex(['type']);
  print(await db.findOneAndRemove({'a': '5'}));
  // count data where match query
  print(await db.count({}));

  /*
  import data from File or Map
   */
  db.importFromFile([
    {"_id": "5b78bdb6702de39b30e6f68b", "b": "6"},
    {"_id": "5b78bdb6702de39b30e6f68c", "b": "12"},
    {"_id": "5b78bdb6702de39b30e6f68d", "b": "script"},
    {
      "b": "102",
      'type': {'at': 'title'},
      "_id": "5b77480133bd7f87d4030e15",
      "o": {"sqdq": []}
    }
  ]).then((data) {
    print('import data');
  }).catchError((e) {
    print("failed to import data $e");
  });

  await db.tidy();
  await db.close();
}
