import 'dart:io';
import 'dart:convert';

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
  String str = "Hello world";
  print(base64.encode(utf8.encode(str)));
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

  //await db.removeIndex(['type']);
  //print(await db.findOneAndRemove({'a': '5'}));
  /* db.importFromFile([
      {"_id": "5b78bdb6702de39b30e6f68b", "b": "6"},
      {"_id": "5b78bdb6702de39b30e6f68c", "b": "6"},
      {"_id": "5b78bdb6702de39b30e6f68d", "b": "6"},
      {
        "b": "10",
        "_id": "5b77480133bd7f87d4030e15",
        "o": {"sqdq": []}
      }
    ]); */
  db.insert({'a': '6'}).then((data) {
    print("insert 6 $data");
  }).catchError((e) {
    print("catchhhhhhhhhh insert 6$e");
  });
  db.findOneAndRemove({"type.at": "4"}).then((data) {
    print("remove 7 $data");
  }).catchError((e) {
    print("catchhhhhhhhhh insert 7 $e");
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
    print("insert $data");
  }).catchError((e) {
    print("catchhhhhhhhhh $e");
  });

  /* db.watch().listen((data) {
      if (data.isNotEmpty) print('without $data');
    });
    db.watch({"a": "1"}).listen((data) {
      if (data.isNotEmpty) print('with query $data');
    }); */
  db.update({
    "type.at": "4"
  }, {
    'type': {'eee': '4'}
  }, upsert: true).then((data) {
    print("update $data");
  }).catchError((e) {
    print("catccccch $e");
  });

  /*  await db.update({
    'type.at': '4'
  }, {
    Op.unset: {"type.at": true}
  }, upsert: true); */

  /*  db.onUpdate.listen((data) {
    print('on update $data');
  });
  db.onInsert.listen((data) {
    print('on onInsert $data');
  });
  db.onRemove.listen((data) {
    print('on onRemove $data');
  }); */
  print((await db.find(
    {
      // Op.exists: {'e.c.d[0]': true, 'a': true}
    },
    projection: {'e': false, 'a': false},
    /*  sort: {'e.c.d[0].dsqd': 1, 'a': 1},
        skip: 2,
        limit: 1 */
  ))
      .length);
  /* var ids = await db.insertMany([
      {'a': '1'},
      {'a': '2'},
      {'a': '3'},
      {'a': '4'},
    ]);

    print(await db.find({'_id': ids[2]}));

    print(await db.insert({'a': '5'}));
    db.insert({'a': '6'});
    db.insert({'a': '7'});
    db.insert({'a': '8'});
    db.insert({'a': '9'});

    db.update({
      Op.gt: {'a': '0'},
      'a': RegExp('[4-7]'),
    }, {
      Op.max: {'n': 100},
      'b': 'c'
    });

    db.update({
      Op.gt: {'a': '0'},
      'a': RegExp('[4-7]'),
    }, {
      Op.rename: {'b': 'ü'},
      Op.unset: {'n': true},
    });

    //print((await db.remove({'a': RegExp('[3-6]')})));

    print(await db.last({
      Op.gt: {'a': 0},
    }));

    print(await db.find({
      Op.gt: {'a': 0},
    }));
 */
  await db.tidy();
  await db.close();
}
