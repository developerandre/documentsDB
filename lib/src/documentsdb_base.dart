import 'dart:io';
import 'dart:async';
import 'dart:convert';
import 'package:documentsdb/src/deeply.dart';
import 'package:documentsdb/src/documentsdb_operators.dart';
import 'package:documentsdb/src/documentsdb_filter.dart';
import 'package:documentsdb/src/documentsdb_meta.dart';
import 'package:documentsdb/src/documentsdb_objectid.dart';
import 'package:documentsdb/src/documentsdb_exceptions.dart';
import 'package:documentsdb/src/execution_queue.dart';

typedef int SortFunction(a, b);
typedef bool RemoveWhereFunction(item);

/// Database class
class DocumentsDB {
  final String path;
  File _file;
  IOSink _writer;
  List<Map<dynamic, dynamic>> _dbData = [];
  ExecutionQueue _executionQueue = ExecutionQueue();
  Map<String, Op> _operatorMap = Map();
  Meta _meta = Meta(1);
  final String _signature = '\$documentsdb';
  final Map<dynamic, StreamController> _streams = {};
  final StreamController _onInsert = StreamController();
  final StreamController _onUpdate = StreamController();
  final StreamController _onRemove = StreamController();
  final Map<String, _IndexOptions> _indexes = {};
  final _Trigger _trigger = _Trigger();
  final bool timestampData;
  final bool inMemoryOnly;
  DocumentsDB(this.path,
      {this.timestampData = false, this.inMemoryOnly = false}) {
    if (!inMemoryOnly) this._file = File(this.path);

    Op.values.forEach((Op op) {
      _operatorMap[op.toString()] = op;
    });
  }
  List<Map<dynamic, dynamic>> get _data {
    _streams.forEach((dynamic key, StreamController<dynamic> ctrl) async {
      List<Map> finded = await find(key);
      if (!ctrl.isClosed) {
        ctrl.add(finded);
      }
    });
    return _dbData;
  }

  set _data(List<Map<dynamic, dynamic>> newData) {
    if (newData != null) {
      _dbData = newData;
      _streams.forEach((dynamic key, StreamController<dynamic> ctrl) async {
        List<Map> finded = await find(key);
        if (!ctrl.isClosed) {
          ctrl.add(finded);
        }
      });
    }
  }

  /// Opens flat file database
  Future<DocumentsDB> open([bool tidy = false]) {
    return this._executionQueue.add<DocumentsDB>(() => this._open(tidy));
  }

  Future<DocumentsDB> _open(bool tidy) async {
    if (!inMemoryOnly) {
      File backupFile = File(this.path + '.bak');
      if (backupFile.existsSync()) {
        if (this._file.existsSync()) {
          this._file.deleteSync();
        }
        backupFile.renameSync(this.path);
        this._file = File(this.path);
      }

      if (!this._file.existsSync()) {
        this._file.createSync();
      }
      await _openFileAndRead(this._file);

      this._writer = this._file.openWrite(mode: FileMode.writeOnlyAppend);
      if (tidy) {
        return await this._tidy();
      }
    }
    return this;
  }

  Future _openFileAndRead(File file, [isOpen = true]) async {
    Stream<List<int>> reader = file.openRead();
    if (isOpen) this._data = [];
    bool firstLine = true;
    await reader
        .transform(utf8.decoder)
        .transform(LineSplitter())
        .forEach((String line) async {
      if (line != '') {
        if (firstLine) {
          firstLine = false;
          if (line.startsWith(_signature) && isOpen) {
            try {
              this._meta =
                  Meta.fromMap(json.decode(line.substring(_signature.length)));
            } catch (e) {
              // no valid meta -> default meta
            }
            return;
          }
        }
        try {
          await this._fromFile(line);
        } catch (e) {
          // skip invalid line
        }
      }
    });
  }

  Future<DocumentsDB> _tidy() async {
    if (!inMemoryOnly) {
      await this._writer.close();
      await this._file.rename(this.path + '.bak');
      this._file = File(this.path);
      IOSink writer = this._file.openWrite();
      writer.writeln(_signature + this._meta.toString());
      writer.writeAll(this._dbData.map((data) => json.encode(data)), '\n');
      writer.write('\n');
      await writer.flush();
      await writer.close();

      File backupFile = File(this.path + '.bak');
      await backupFile.delete();
    }
    return await this._open(false);
  }

  Future _fromFile(String line) async {
    try {
      switch (line[0]) {
        case '+':
          {
            this._insertData(json.decode(line.substring(1)));
            break;
          }
        case '-':
          {
            this._removeData(this._decode(json.decode(line.substring(1))));
            break;
          }
        case '~':
          {
            var u = json.decode(line.substring(1));
            await this._updateData(
                this._decode(u['q']), this._decode(u['c']), u['r']);
            break;
          }
        case '{':
          {
            this._insertData(json.decode(line));
            break;
          }
      }
    } catch (e) {}
    return null;
  }

  Function _match(query, [Op op = Op.and]) {
    bool match(Map<dynamic, dynamic> test) {
      keyloop:
      for (dynamic i in query.keys) {
        if (i is Op) {
          bool match = this._match(query[i], i)(test);

          if (op == Op.and && match) continue;
          if (op == Op.and && !match) return false;

          if (op == Op.or && !match) continue;
          if (op == Op.or && match) return true;

          return Op.not == op ? !match : match;
        }

        if (query[i] is ObjectId) {
          query[i] = query[i].toString();
        }
        List<String> keyPath = _getKeyPath(i);
        dynamic testVal = test;
        bool isArrayIndex = false;
        for (String o in keyPath) {
          if (RegExp(r"\[(\-?[0-9]+)\]").hasMatch(o)) {
            isArrayIndex = true;
            o = o.toString().replaceAllMapped(
                RegExp(r"\[(\-?[0-9]+)\]"), (Match m) => "${m[1]}");
          }

          if (!(testVal is List<dynamic>)) {
            if (!(testVal is Map<dynamic, dynamic>) ||
                !testVal.containsKey(o)) {
              if (op == Op.exists) {
                bool statement;
                if (testVal is Map<dynamic, dynamic>)
                  statement = testVal.containsKey(o);
                else if (testVal is List<dynamic>)
                  statement = testVal.contains(o);
                if (statement != null) {
                  if ((query[i] == true && testVal != null && statement) ||
                      (query[i] == false && (testVal == null || !statement)))
                    continue keyloop; //return true;
                }
              }
              if (op != Op.or)
                return false;
              else
                continue keyloop;
            }
          }

          try {
            if (isArrayIndex && testVal is List) {
              int index = int.tryParse(o);
              if (index < 0 && testVal.isNotEmpty)
                index = testVal.length + index;
              testVal = testVal?.elementAt(index);
            } else
              testVal = testVal[o];
          } catch (e) {
            testVal = null;
          }

          isArrayIndex = false;
        }

        if (op != Op.inList &&
            op != Op.exists &&
            op != Op.notInList &&
            (!(query[i] is RegExp) && (op != Op.and && op != Op.or)) &&
            testVal.runtimeType != query[i].runtimeType) continue;

        switch (op) {
          case Op.and:
          case Op.not:
            {
              if (query[i] is RegExp) {
                if (!query[i].hasMatch(testVal)) return false;
                break;
              }
              if (testVal != query[i]) return false;
              break;
            }
          case Op.or:
            {
              if (query[i] is RegExp) {
                if (query[i].hasMatch(testVal)) return true;
                break;
              }
              if (testVal == query[i]) return true;
              break;
            }
          case Op.gt:
            {
              if (testVal is String) {
                return testVal.compareTo(query[i]) > 0;
              }
              return testVal > query[i];
            }
          case Op.gte:
            {
              if (testVal is String) {
                return testVal.compareTo(query[i]) >= 0;
              }
              return testVal >= query[i];
            }
          case Op.lt:
            {
              if (testVal is String) {
                return testVal.compareTo(query[i]) < 0;
              }
              return testVal < query[i];
            }
          case Op.lte:
            {
              if (testVal is String) {
                return testVal.compareTo(query[i]) <= 0;
              }
              return testVal <= query[i];
            }
          case Op.ne:
            {
              return testVal != query[i];
            }
          case Op.inList:
            {
              return (query[i] is List) && query[i].contains(testVal);
            }
          case Op.notInList:
            {
              return (query[i] is List) && !query[i].contains(testVal);
            }
          case Op.exists:
            {
              if ((query[i] == true && testVal != null) ||
                  (query[i] == false && testVal == null)) {
                continue keyloop;
              }
              return false;
            }
          default:
            {}
        }
      }
      return op == Op.or ? false : true;
    }

    return match;
  }

  void _insertData(Map data) {
    if (!data.containsKey('_id')) {
      data['_id'] = ObjectId().toString();
    }
    this._data.add(data);
  }

  int _removeData(Map<dynamic, dynamic> query, [bool removeOne = false]) {
    int first = 0;
    int count = 0;

    this._data.removeWhere((Map<dynamic, dynamic> map) {
      bool test = this._match(query)(map);
      if (!test) return false;
      first++;
      _execTrigger(_OperationType.REMOVE, _EventType.ONBEFORE, map);
      test = removeOne ? (first == 1 ? true : false) : test;
      if (test && (inMemoryOnly || this._writer != null)) {
        _onRemove.add(map);
      }
      if (test) {
        count++;
        _execTrigger(_OperationType.REMOVE, _EventType.ONAFTER, map);
      }
      return test;
    });

    return count;
  }

  Future<int> _updateData(
      Map<dynamic, dynamic> query, Map<dynamic, dynamic> changes, bool replace,
      {bool updateOne = false, bool upsert = false}) async {
    Completer<int> completer = Completer<int>();
    int count = 0;
    Function matcher = this._match(query);
    bool hasMatch = false;
    for (int i = 0; i < this._data.length; i++) {
      if (!matcher(this._data[i])) continue;
      hasMatch = true;
      if (count == 1 && updateOne) return count;
      count++;

      if (replace) {
        this._data[i] =
            Map<dynamic, dynamic>.from({'_id': ObjectId().toString()});
      }
      var temp;
      _execTrigger(_OperationType.UPDATE, _EventType.ONBEFORE, this._data[i]);
      for (var o in changes.keys) {
        if (o is Op) {
          for (String p in changes[o].keys) {
            List<String> keyPath = _getKeyPath(p);
            temp = this._data[i];
            switch (o) {
              case Op.set:
                {
                  this._data[i] = updateDeeply(
                      keyPath, this._data[i], (value) => changes[o][p]);
                  break;
                }
              case Op.unset:
                {
                  if (changes[o][p] == true) {
                    this._data[i] = removeDeeply(keyPath, this._data[i]);
                  }
                  break;
                }
              case Op.max:
                {
                  this._data[i] = updateDeeply(
                      keyPath,
                      this._data[i],
                      (value) => value > changes[o][p] ? changes[o][p] : value,
                      0);
                  break;
                }
              case Op.min:
                {
                  this._data[i] = updateDeeply(
                      keyPath,
                      this._data[i],
                      (value) => value < changes[o][p] ? changes[o][p] : value,
                      0);
                  break;
                }
              case Op.increment:
                {
                  this._data[i] = updateDeeply(keyPath, this._data[i],
                      (value) => value += changes[o][p], 0);
                  break;
                }
              case Op.multiply:
                {
                  this._data[i] = updateDeeply(keyPath, this._data[i],
                      (value) => value *= changes[o][p], 0);
                  break;
                }
              case Op.rename:
                {
                  this._data[i] =
                      renameDeeply(keyPath, changes[o][p], this._data[i]);
                  break;
                }
              case Op.add:
                {
                  this._data[i] = updateDeeply(keyPath, this._data[i], (value) {
                    if (value is List) value.add(changes[o][p]);
                    if (value is Map && changes[o][p] is Map)
                      value.addAll(changes[o][p]);
                    return value;
                  }, []);
                  break;
                }
              case Op.addAll:
                {
                  this._data[i] = updateDeeply(keyPath, this._data[i], (value) {
                    if (value is List) {
                      if (changes[o][p] is List)
                        value.addAll(changes[o][p]);
                      else
                        value.add(changes[o][p]);
                    } else if (value is Map && changes[o][p] is Map) {
                      value.addAll(changes[o][p]);
                    }
                    return value;
                  }, []);
                  break;
                }
              case Op.addToSet:
                {
                  this._data[i] = updateDeeply(keyPath, this._data[i], (value) {
                    if (value is List && !value.contains(changes[o][p]))
                      value.add(changes[o][p]);
                    if (value is Map && changes[o][p] is Map) {
                      for (var key in changes[o][p].keys) {
                        value.putIfAbsent(key, () => changes[o][p][key]);
                      }
                    }
                    return value;
                  }, []);
                  break;
                }
              case Op.insert:
                {
                  this._data[i] = updateDeeply(keyPath, this._data[i], (value) {
                    if (value is List) {
                      if (changes[o][p] is Map<String, dynamic>) {
                        int index =
                            int.tryParse(changes[o][p]['position'].toString());
                        try {
                          value.insert(index ?? 0, changes[o][p]['value']);
                        } catch (e) {}
                      }
                    }
                    return value;
                  }, []);
                  break;
                }
              case Op.insertAll:
                {
                  this._data[i] = updateDeeply(keyPath, this._data[i], (value) {
                    if (value is List) {
                      if (changes[o][p] is Map<String, dynamic> &&
                          changes[o][p]['value'] is List) {
                        int index =
                            int.tryParse(changes[o][p]['position'].toString());
                        try {
                          value.insertAll(index ?? 0, changes[o][p]['value']);
                        } catch (e) {}
                      }
                    }
                    return value;
                  }, []);
                  break;
                }
              case Op.clear:
                {
                  this._data[i] = updateDeeply(keyPath, this._data[i], (value) {
                    if (value is List || value is Map) value.clear();
                    return value;
                  }, []);
                  break;
                }
              case Op.pop:
                {
                  this._data[i] = updateDeeply(keyPath, this._data[i], (value) {
                    if ((value is List) && value.isNotEmpty) {
                      if (changes[o][p] == true)
                        value.removeLast();
                      else if (changes[o][p] == false) value.removeAt(0);
                    }
                    return value;
                  }, []);
                  break;
                }
              case Op.remove:
                {
                  this._data[i] = updateDeeply(keyPath, this._data[i], (value) {
                    if ((value is List || value is Map) && value.isNotEmpty) {
                      try {
                        value.remove(changes[o][p]);
                      } catch (e) {}
                    }
                    return value;
                  }, []);
                  break;
                }
              case Op.removeAt:
                {
                  this._data[i] = updateDeeply(keyPath, this._data[i], (value) {
                    if ((value is List) && value.isNotEmpty) {
                      try {
                        if (changes[o][p] is int) value.removeAt(changes[o][p]);
                      } catch (e) {}
                    }
                    return value;
                  }, []);
                  break;
                }
              case Op.removeWhere:
                {
                  this._data[i] = updateDeeply(keyPath, this._data[i], (value) {
                    if ((value is List || value is Map) && value.isNotEmpty) {
                      if (changes[o][p] is Function &&
                          changes[o][p] is! RemoveWhereFunction) {
                        print(
                            'Warning: The value must be a function with 1 dynamic parameter and return bool');
                      }
                      try {
                        if (changes[o][p] is RemoveWhereFunction)
                          value.removeWhere(changes[o][p]);
                      } catch (e) {}
                    }
                    return value;
                  }, []);
                  break;
                }
              case Op.slice:
                {
                  this._data[i] = updateDeeply(keyPath, this._data[i], (value) {
                    if ((value is List) && value.isNotEmpty) {
                      if ((changes[o][p] is List<num>) &&
                          [1, 2].contains(changes[o][p].length)) {
                        try {
                          return value.sublist(
                              changes[o][p][0],
                              changes[o][p].length == 2
                                  ? changes[o][p][1]
                                  : null);
                        } catch (e) {}
                      }
                    }
                    return value;
                  }, []);
                  break;
                }
              case Op.sort:
                {
                  this._data[i] = updateDeeply(keyPath, this._data[i], (value) {
                    if (value is List) {
                      if (changes[o][p] is Function &&
                          changes[o][p] is! SortFunction) {
                        print(
                            'Warning: The value must be a function with 2 dynamics parameters and return int');
                      }
                      if (changes[o][p] is SortFunction)
                        value.sort(changes[o][p]);
                      else
                        value.sort();
                    }
                    return value;
                  }, []);
                  break;
                }
              case Op.currentDate:
                {
                  this._data[i] = updateDeeply(
                      keyPath,
                      this._data[i],
                      (value) => changes[o][p] == true
                          ? DateTime.now().millisecondsSinceEpoch
                          : null,
                      0);
                  break;
                }
              default:
                {
                  //throw 'invalid';
                  completer.completeError("invalid");
                }
            }
          }
        } else {
          Map map = Map.from(this._data[i]);
          map.addAll(changes);
          try {
            await _verifyIndex(map, verifyUnicity: false, temp: temp, i: i);
            this._data[i][o] = changes[o];
          } catch (e) {
            if (temp != null) this._data[i] = temp;
            completer.completeError(e);
          }
        }
      }
      _execTrigger(_OperationType.UPDATE, _EventType.ONAFTER, this._data[i]);
      if (inMemoryOnly || this._writer != null) _onUpdate.add(this._data[i]);
    }
    if (hasMatch) completer.complete(count);
    if (upsert == true && !hasMatch) {
      try {
        runZoned(() {
          _insert(changes).then((data) {
            completer.complete(1);
          }).catchError((e) {
            completer.completeError(e);
          });
        }, onError: (e) {
          if (!completer.isCompleted) completer.completeError(e);
        });
      } catch (e) {
        if (e is DocumentsDBException &&
            e.message == "data contains invalid data types")
          print("Cannot insert data with Op");
        if (!completer.isCompleted) completer.completeError(e);
      }
    } else if (!hasMatch) completer.complete(0);
    //if (!completer.isCompleted) completer.complete(count);
    return completer.future;
  }

  List<Map<dynamic, dynamic>> _buildProjection(
      List<Map<dynamic, dynamic>> list, Map<dynamic, dynamic> projection) {
    if (list != null &&
        list.isNotEmpty &&
        projection != null &&
        projection.isNotEmpty) {
      for (var key in projection.keys) {
        if (projection[key] == false) {
          List<String> keyPath = _getKeyPath(key);
          list = list.map<Map<dynamic, dynamic>>((Map<dynamic, dynamic> map) {
            return removeDeeply(keyPath, map);
          }).toList();
        }
      }
    }
    return list;
  }

  /// Find data in cached database object
  Future _find(query,
      [Filter filter = Filter.all,
      Map<dynamic, dynamic> projection,
      Map<dynamic, int> sort,
      int skip,
      int limit]) async {
    return Future.sync((() {
      Function match = this._match(query);
      List<Map> list = List<Map<dynamic, dynamic>>.from(this._data);
      if (sort != null && sort.isNotEmpty) {
        list.sort((Map<dynamic, dynamic> a, Map<dynamic, dynamic> b) {
          for (var key in sort.keys) {
            List<String> keyPath = _getKeyPath(key);
            dynamic currentData = a;
            dynamic currentBData = b;
            bool isArrayIndex = false;
            for (dynamic o in keyPath) {
              if (RegExp(r"\[(\-?[0-9]+)\]").hasMatch(o)) {
                isArrayIndex = true;
                o = o.toString().replaceAllMapped(
                    RegExp(r"\[(\-?[0-9]+)\]"), (Match m) => "${m[1]}");
              }
              try {
                if (isArrayIndex &&
                    currentData is List &&
                    currentBData is List) {
                  int index = int.tryParse(o);
                  if (index < 0 && currentData.isNotEmpty)
                    index = currentData.length + index;
                  currentData = currentData?.elementAt(index);
                  currentBData = currentBData?.elementAt(index);
                } else {
                  currentData = currentData[o];
                  currentBData = currentBData[o];
                }
              } catch (e) {
                currentData = null;
                currentBData = null;
              }
              isArrayIndex = false;
            }
            int compareTo = 0;
            if (currentBData != null && currentData != null) {
              try {
                bool isComp =
                    currentData is Comparable && currentBData is Comparable;
                if (sort[key] == 1) {
                  if (isComp)
                    compareTo = currentData.compareTo(currentBData);
                  else {
                    compareTo = currentData
                        .toString()
                        .compareTo(currentBData.toString());
                  }
                } else if (sort[key] == -1) {
                  if (isComp) {
                    compareTo = currentBData.compareTo(currentData);
                  } else {
                    compareTo = currentBData
                        .toString()
                        .compareTo(currentData.toString());
                  }
                }
              } catch (e) {}
            }
            if (compareTo != 0) return compareTo;
          }
          return 0;
        });
      }
      if (filter == Filter.all) {
        list = list.where(match).toList();
        if (skip != null && skip > 0) {
          try {
            list = list.sublist(skip);
          } catch (e) {}
        }
        if (limit != null && !limit.isNegative) {
          try {
            list = list.sublist(0, limit);
          } catch (e) {}
        }
        list = _buildProjection(list, projection);
        return list;
      }
      if (filter == Filter.first) {
        Map map = list.firstWhere(match, orElse: () {
          return null;
        });
        return _buildProjection([map], projection)[0];
      } else {
        Map map = list.lastWhere(match, orElse: () {
          return null;
        });
        return _buildProjection([map], projection)[0];
      }
    }));
  }

  /// Insert [data] update cache object and write change to file
  Future<ObjectId> _insert(Map data) async {
    data = Map.from(data);
    ObjectId _id = ObjectId();
    data['_id'] = _id.toString();
    if (timestampData == true) {
      int now = DateTime.now().millisecondsSinceEpoch;
      data['createdAt'] = now;
      data['updatedAt'] = now;
    }
    try {
      await _verifyIndex(data);
      _execTrigger(_OperationType.INSERTION, _EventType.ONBEFORE, data);
      if (!inMemoryOnly) this._writer.writeln('+' + json.encode(data));
      this._insertData(data);
      _onInsert.add(data);
      _execTrigger(_OperationType.INSERTION, _EventType.ONAFTER, data);
    } catch (e) {
      String message = 'data contains invalid data types';
      if (e.toString().startsWith("Index error:")) message = e.toString();
      return Future.error(DocumentsDBException(message));
    }
    return _id;
  }

  /// Replace operator string to corresponding enum
  Map _decode(Map query) {
    Map prepared = Map();
    for (var i in query.keys) {
      dynamic key = i;
      if (this._operatorMap.containsKey(key)) {
        key = this._operatorMap[key];
      }
      if (query[i] is Map && query[i].containsKey('\$type')) {
        if (query[i]['\$type'] == 'regex') {
          prepared[key] = RegExp(query[i]['pattern']);
        }
        continue;
      }

      if (query[i] is Map) {
        prepared[key] = this._decode(query[i]);
      } else if (query[i] is int ||
          query[i] is double ||
          query[i] is bool ||
          query[i] is String ||
          query[i] is List) {
        prepared[key] = query[i];
      } else {
        throw DocumentsDBException('query contains invalid data types');
      }
    }
    return prepared;
  }

  /// Replace operator enum to corresponding string
  Map _encode(Map query) {
    Map prepared = Map();
    for (var i in query.keys) {
      dynamic key = i;
      if (key is Op) {
        key = key.toString();
      }

      prepared[key] = this._encodeValue(query[i]);
    }
    return prepared;
  }

  _encodeValue(dynamic value) {
    if (value is Map) {
      return this._encode(value);
    }
    if (value is String || value is int || value is bool || value is List) {
      return value;
    }
    if (value is RegExp) {
      return {'\$type': 'regex', 'pattern': value.pattern};
    }
  }

  int _remove(Map<dynamic, dynamic> query, [bool removeOne = false]) {
    int removeData = this._removeData(query, removeOne);

    if (removeData > 0) {
      if (!inMemoryOnly)
        this._writer.writeln('-' + json.encode(this._encode(query)));
    }
    return removeData;
  }

  Future<int> _update(
      Map<dynamic, dynamic> query, Map<dynamic, dynamic> changes, bool replace,
      {bool updateOne = false, bool upsert = false}) {
    Completer<int> completer = Completer<int>();
    if (timestampData == true) {
      int now = DateTime.now().millisecondsSinceEpoch;
      changes['updatedAt'] = now;
    }

    try {
      int updateData;
      runZoned(() async {
        updateData = await this._updateData(query, changes, replace,
            updateOne: updateOne, upsert: upsert);
        if (updateData > 0) {
          if (!inMemoryOnly) {
            this._writer.writeln('~' +
                json.encode({
                  'q': this._encode(query),
                  'c': this._encode(changes),
                  'r': replace
                }));
          }
        }
        completer.complete(updateData);
      }, onError: (e) {
        completer.completeError(e);
      });
    } catch (e) {
      completer.completeError(e);
    }
    return completer.future;
  }

  /// get all documents that match [query]
  Future<List<Map<dynamic, dynamic>>> find(Map<dynamic, dynamic> query,
      {Map<dynamic, dynamic> projection,
      Map<dynamic, int> sort,
      int skip,
      int limit}) {
    try {
      return this._executionQueue.add<List<Map<dynamic, dynamic>>>(
          () => this._find(query, Filter.all, projection, sort, skip, limit));
    } catch (e) {
      throw (e);
    }
  }

  /// get first document that matches [query]
  Future<Map<dynamic, dynamic>> first(Map<dynamic, dynamic> query,
      {Map<dynamic, dynamic> projection}) {
    try {
      return this._executionQueue.add<Map<dynamic, dynamic>>(
          () => this._find(query, Filter.first, projection));
    } catch (e) {
      throw (e);
    }
  }

  /// get last document that matches [query]
  Future<Map<dynamic, dynamic>> last(Map<dynamic, dynamic> query,
      {Map<dynamic, dynamic> projection}) {
    try {
      return this._executionQueue.add<Map<dynamic, dynamic>>(
          () => this._find(query, Filter.last, projection));
    } catch (e) {
      throw (e);
    }
  }

  /// insert document
  Future<ObjectId> insert(Map<dynamic, dynamic> doc) {
    return this._executionQueue.add<ObjectId>(() => this._insert(doc));
  }

  /// insert many documents
  Future<List<ObjectId>> insertMany(List<Map<dynamic, dynamic>> docs) {
    return this._executionQueue.add<List<ObjectId>>(() async {
      List<ObjectId> _ids = [];
      for (Map doc in docs) {
        ObjectId objectId = await this._insert(doc);
        _ids.add(objectId);
      }
      return _ids;
    });
  }

  /// remove documents that match [query]
  Future<int> remove(Map<dynamic, dynamic> query) {
    // todo: count
    return this._executionQueue.add<int>(() => this._remove(query));
  }

  /// update database, takes [query], [changes] and an optional [replace] flag
  Future<int> update(Map<dynamic, dynamic> query, Map<dynamic, dynamic> changes,
      {bool replace = false, bool upsert = false}) {
    return this
        ._executionQueue
        .add<int>(() => this._update(query, changes, replace, upsert: upsert));
  }

  /// get first document that matches [query]
  Future<Map<dynamic, dynamic>> findOne(Map<dynamic, dynamic> query,
      {Map<dynamic, dynamic> projection}) {
    return this.first(query, projection: projection);
  }

  Future<int> findAndUpdate(
      Map<dynamic, dynamic> query, Map<dynamic, dynamic> changes,
      [bool replace = false]) {
    return this.update(query, changes, replace: replace);
  }

  Future<int> findAndRemove(Map<dynamic, dynamic> query) {
    return this.remove(query);
  }

  Future<int> findOneAndRemove(Map<dynamic, dynamic> query) {
    return this.removeOne(query);
  }

  Future<int> removeOne(Map<dynamic, dynamic> query) {
    return this._executionQueue.add<int>(() => this._remove(query, true));
  }

  Future<int> findOneAndUpdate(
      Map<dynamic, dynamic> query, Map<dynamic, dynamic> changes,
      [bool replace = false]) {
    return this._executionQueue.add<int>(() async =>
        await this._update(query, changes, replace, updateOne: true));
  }

  Future<dynamic> importFromFile(fileOrMap) async {
    if (fileOrMap is File) {
      _openFileAndRead(fileOrMap, false);
      return this._executionQueue.add<ObjectId>(() => () => ObjectId());
    } else if (fileOrMap is Map) {
      await this._fromFile(fileOrMap.toString());
      return this._executionQueue.add<ObjectId>(() => () => ObjectId());
    } else if (fileOrMap is List<Map>) {
      return insertMany(fileOrMap);
    } else
      return this._executionQueue.add<ObjectId>(() => () => null);
  }

  Future<List<Map<dynamic, dynamic>>> export(
      [Map<dynamic, dynamic> query = const {}, File file]) async {
    List<Map> list = await this.find(query);
    if (file != null) file.writeAsStringSync(list.toString());
    return list;
  }

  Future<int> count([Map<dynamic, dynamic> query = const {}]) async {
    List<Map> list = await this.find(query);
    return list?.length ?? 0;
  }

  Stream<dynamic> watch([Map<dynamic, dynamic> query = const {}]) {
    _streams.addAll({query: StreamController.broadcast()});
    return _streams[query].stream;
  }

  Stream<dynamic> get onInsert {
    return _onInsert.stream;
  }

  Stream<dynamic> get onRemove {
    return _onRemove.stream;
  }

  Stream<dynamic> get onUpdate {
    return _onUpdate.stream;
  }

  /// 'tidy up' .db file
  Future<DocumentsDB> tidy() {
    return this._executionQueue.add<DocumentsDB>(() => this._tidy());
  }

  /// close db
  Future close() {
    return this._executionQueue.add(() async {
      _streams
          .forEach((dynamic key, StreamController<dynamic> streamCtrl) async {
        await streamCtrl?.close();
      });
      _onInsert?.close();
      _onUpdate?.close();
      _onRemove?.close();
      await _writer?.close();
    });
  }

  _Trigger get trigger {
    return _trigger;
  }

  ensureIndex(List<String> fieldNames,
      {bool unique = false,
      bool mandatory = false,
      DateTime expireAfterSeconds}) {
    fieldNames.forEach((String field) {
      if (field != null && field.trim().isNotEmpty) {
        _indexes.addAll(
            {field: _IndexOptions(unique, mandatory, expireAfterSeconds)});
        if (expireAfterSeconds != null &&
            !expireAfterSeconds.difference(DateTime.now()).isNegative) {
          Timer(
              Duration(
                  milliseconds: expireAfterSeconds
                      .difference(DateTime.now())
                      .inMilliseconds), () {
            removeIndex([field]);
          });
        }
      }
    });
  }

  removeIndex(List<String> fieldNames) {
    fieldNames.forEach((String field) {
      _indexes.remove(field);
    });
  }

  Future _verifyIndex(Map data,
      {bool verifyUnicity = true, dynamic temp, int i}) async {
    if (_indexes.isNotEmpty) {
      String formattedI;
      bool isArrayIndex = false;
      _IndexOptions option;
      for (String field in _indexes.keys) {
        option = _indexes[field];
        List<String> keyPath = _getKeyPath(field);
        dynamic testVal = Map.from(data);
        for (dynamic o in keyPath) {
          if (RegExp(r"\[(\-?[0-9]+)\]").hasMatch(o)) {
            isArrayIndex = true;
            o = o.toString().replaceAllMapped(
                RegExp(r"\[(\-?[0-9]+)\]"), (Match m) => "${m[1]}");
          }
          try {
            if (isArrayIndex && testVal is List) {
              int index = int.tryParse(o);
              if (index < 0 && testVal.isNotEmpty)
                index = testVal.length + index;
              testVal = testVal?.elementAt(index);
            } else
              testVal = testVal[o];
          } catch (e) {
            testVal = null;
            if (option != null && option.mandatory == true) {
              if (temp != null && i != null)
                this._data[i] = temp;
              else {
                return Future.error(DocumentsDBException(
                    "Index error: field `$field` is mandatory"));
              }
            }
          }
          isArrayIndex = false;
        }
        if (option != null && option.mandatory == true) {
          if (testVal == null) {
            if (temp != null && i != null)
              this._data[i] = temp;
            else {
              return Future.error(DocumentsDBException(
                  "Index error: field `$field` is mandatory"));
            }
          }
        }
        if (verifyUnicity) {
          if (option != null && option.unique == true) {
            dynamic finded = await _find({field: testVal});
            if (finded != null && (finded is List && finded.isNotEmpty)) {
              return Future.error(DocumentsDBException(
                  "Index error: field `$field` value must be unique"));
            }
          }
        }
      }
    }
    return null;
  }

  void _execTrigger(_OperationType type, _EventType event, dynamic data) {
    if (data is Map)
      data = Map.from(data);
    else if (data is List) data = List.from(data);
    if (type == _OperationType.INSERTION) {
      if (event == _EventType.ONBEFORE &&
          _trigger.onInsert.onBefore.isNotEmpty) {
        _trigger.onInsert.onBefore
            .forEach((StatementFunction condition, ReqFunction req) async {
          if (condition(data) == true) {
            try {
              await Future.sync(() {
                req(data);
              });
            } catch (e) {
              throw e;
            }
          }
        });
      } else if (event == _EventType.ONAFTER &&
          _trigger.onInsert.onAfter.isNotEmpty) {
        _trigger.onInsert.onAfter
            .forEach((StatementFunction condition, ReqFunction req) async {
          if (condition(data) == true) {
            try {
              await Future.sync(() {
                req(data);
              });
            } catch (e) {
              throw e;
            }
          }
        });
      }
    } else if (type == _OperationType.UPDATE) {
      if (event == _EventType.ONBEFORE &&
          _trigger.onUpdate.onBefore.isNotEmpty) {
        _trigger.onUpdate.onBefore
            .forEach((StatementFunction condition, ReqFunction req) async {
          if (condition(data) == true) {
            try {
              await Future.sync(() {
                req(data);
              });
            } catch (e) {
              throw e;
            }
          }
        });
      } else if (event == _EventType.ONAFTER &&
          _trigger.onUpdate.onAfter.isNotEmpty) {
        _trigger.onUpdate.onAfter
            .forEach((StatementFunction condition, ReqFunction req) async {
          if (condition(data) == true) {
            try {
              await Future.sync(() {
                req(data);
              });
            } catch (e) {
              throw e;
            }
          }
        });
      }
    } else if (type == _OperationType.REMOVE) {
      if (event == _EventType.ONBEFORE &&
          _trigger.onRemove.onBefore.isNotEmpty) {
        _trigger.onRemove.onBefore
            .forEach((StatementFunction condition, ReqFunction req) async {
          if (condition(data) == true) {
            try {
              await Future.sync(() {
                req(data);
              });
            } catch (e) {
              throw e;
            }
          }
        });
      } else if (event == _EventType.ONAFTER &&
          _trigger.onRemove.onAfter.isNotEmpty) {
        _trigger.onRemove.onAfter
            .forEach((StatementFunction condition, ReqFunction req) async {
          if (condition(data) == true) {
            try {
              await Future.sync(() {
                req(data);
              });
            } catch (e) {
              throw e;
            }
          }
        });
      }
    }
  }

  List<String> _getKeyPath(key) {
    String formattedI = key
        .toString()
        .replaceAllMapped(RegExp(r"(\[\-?[0-9]+\])"), (Match m) => ".${m[0]}");
    return formattedI.split('.');
  }
}

class _Trigger {
  _TriggerEvent onInsert = _TriggerEvent();
  _TriggerEvent onUpdate = _TriggerEvent();
  _TriggerEvent onRemove = _TriggerEvent();
}

class _TriggerEvent {
  Map<StatementFunction, ReqFunction> onBefore = {};
  Map<StatementFunction, ReqFunction> onAfter = {};
}

typedef bool StatementFunction(dynamic data);
typedef void ReqFunction(dynamic data);
enum _OperationType { INSERTION, UPDATE, REMOVE }
enum _EventType { ONBEFORE, ONAFTER }

class _IndexOptions {
  bool unique = false;
  bool mandatory = false;
  DateTime expireAfterSeconds;
  _IndexOptions(this.unique, this.mandatory, this.expireAfterSeconds);
}
