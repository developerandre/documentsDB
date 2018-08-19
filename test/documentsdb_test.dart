import 'dart:async';
import 'dart:io';
import 'dart:math';

import 'package:test/test.dart';

import 'package:documentsdb/documentsdb.dart';

void main() {
  test('adds one to input values', () async {
    final path = Directory.current.path + '/test/';
    //File file;
    /* File file = File(path + 'test.db');
    if (file.existsSync()) {
      file.deleteSync();
    }
    file = File(path + 'init.db');
    file.copySync(path + 'test.db'); */

    final db = DocumentsDB(path + 'test.db');
    await db.open();
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
    /* await db.insertMany([
      {'a': '1'},
      {'a': '2'},
      {'a': '3'},
      {'a': '4'},
    ]); */
    /* db.watch().listen((data) {
      if (data.isNotEmpty) print('without $data');
    });
    db.watch({"a": "1"}).listen((data) {
      if (data.isNotEmpty) print('with query $data');
    }); */
    await db.update({"z": "sqfsq"}, {'e.c.d': Random().nextInt(100)});
    print(await db.last(
      {},
      projection: {'e': false, 'a': false},
      /*  sort: {'e.c.d[0].dsqd': 1, 'a': 1},
        skip: 2,
        limit: 1 */
    ));
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
  });
}
