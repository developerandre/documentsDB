String _formatPath(dynamic path) {
  RegExp regExp = RegExp(r"\[?(\-?[0-9]+)\]?");
  if (regExp.hasMatch(path.toString())) {
    return path.toString().replaceAllMapped(regExp, (Match m) => "${m[1]}");
  }
  return path.toString();
}

dynamic _getCurrentPath(bool isList, path, data) {
  var currentPath;
  if (isList) {
    currentPath = int.tryParse(path.toString()) ?? 0;
    if (currentPath < 0 && data.isNotEmpty)
      currentPath = data.length + currentPath;
  } else
    currentPath = path;
  return currentPath;
}

dynamic updateDeeply(List<dynamic> keyPath, dynamic data, Function updater,
    [dynamic notSetValue, int i = 0]) {
  if (i == keyPath.length) {
    return updater(data == null ? notSetValue : data);
  }

  bool isList = data is List;
  if (!isList && !(data is Map)) {
    data = {};
  }
  if (!isList)
    data = new Map<dynamic, dynamic>.from(data);
  else
    data = List.from(data);
  keyPath[i] = _formatPath(keyPath[i]);
  var currentPath = _getCurrentPath(isList, keyPath[i], data);
  try {
    data[currentPath] =
        updateDeeply(keyPath, data[currentPath], updater, notSetValue, ++i);
  } catch (e) {}
  return data;
}

dynamic removeDeeply(List keyPath, dynamic data, [int i = 0]) {
  bool isList = data is List;
  keyPath[i] = _formatPath(keyPath[i]);
  if (isList) data = List.from(data);
  var currentPath = _getCurrentPath(isList, keyPath[i], data);
  if (data is Map) {
    data = Map.from(data);
    if (!data.containsKey(currentPath)) {
      return data;
    } else {
      if (keyPath.length == i + 1) {
        data.remove(currentPath);
        return data;
      }
    }
  } else if (isList) {
    if (data.length <= currentPath) {
      return data;
    } else {
      if (keyPath.length == i + 1) {
        data.removeAt(currentPath);
        return data;
      }
    }
  } else {
    return data;
  }
  try {
    data[currentPath] = removeDeeply(keyPath, data[currentPath], ++i);
  } catch (e) {}
  return data;
}

dynamic renameDeeply(List keyPath, dynamic newKey, dynamic data, [int i = 0]) {
  bool isList = data is List;
  keyPath[i] = _formatPath(keyPath[i]);
  if (isList) data = List.from(data);
  var currentPath = _getCurrentPath(isList, keyPath[i], data);
  if (data is Map) {
    data = Map.from(data);
    if (!data.containsKey(currentPath)) {
      return data;
    } else {
      if (keyPath.length == i + 1) {
        data[newKey] = data[currentPath];
        data.remove(currentPath);
        return data;
      }
    }
  } else if (isList) {
    if (data.length <= currentPath) {
      return data;
    } else {
      if (keyPath.length == i + 1) {
        data[newKey] = data[currentPath];
        data.removeAt(currentPath);
        return data;
      }
    }
  } else {
    return data;
  }

  try {
    data[currentPath] = renameDeeply(keyPath, newKey, data[currentPath], ++i);
  } catch (e) {}

  return data;
}
