import 'dart:async';

import 'package:documentsdb/src/documentsdb_exceptions.dart';

class _Item {
  final Completer completer;
  final Function job;
  _Item(this.completer, this.job);
}

class ExecutionQueue {
  List<_Item> _queue = [];
  bool _active = false;

  void _check() async {
    if (!_active && _queue.length > 0) {
      this._active = true;
      _Item item = _queue.removeAt(0);
      try {
        var value = await item.job();
        item.completer.complete(value);
      } catch (e) {
        item.completer.completeError(e.toString());
      }

      this._active = false;
      this._check();
    }
  }

  Future<T> add<T>(Function job) {
    Completer<T> completer = Completer<T>();
    this._queue.add(_Item(completer, job));
    this._check();
    return completer.future;
  }
}
