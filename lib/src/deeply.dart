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
  RegExp regExp = RegExp(r"\[?(\-?[0-9]+)\]?");
  if (regExp.hasMatch(keyPath[i])) {
    keyPath[i] =
        keyPath[i].toString().replaceAllMapped(regExp, (Match m) => "${m[1]}");
  }
  var currentPath;
  if (isList) {
    currentPath = int.tryParse(keyPath[i].toString()) ?? 0;
    if (currentPath < 0 && data.isNotEmpty)
      currentPath = data.length + currentPath;
  } else
    currentPath = keyPath[i];
  try {
    data[currentPath] =
        updateDeeply(keyPath, data[currentPath], updater, notSetValue, ++i);
  } catch (e) {}
  return data;
}

dynamic removeDeeply(List keyPath, dynamic data, [int i = 0]) {
  bool isList = data is List;
  RegExp regExp = RegExp(r"\[?(\-?[0-9]+)\]?");
  if (regExp.hasMatch(keyPath[i])) {
    keyPath[i] =
        keyPath[i].toString().replaceAllMapped(regExp, (Match m) => "${m[1]}");
  }
  var currentPath;
  if (isList) {
    data = List.from(data);
    currentPath = int.tryParse(keyPath[i].toString()) ?? 0;
    if (currentPath < 0 && data.isNotEmpty)
      currentPath = data.length + currentPath;
  } else
    currentPath = keyPath[i];
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
  RegExp regExp = RegExp(r"\[?(\-?[0-9]+)\]?");
  if (regExp.hasMatch(keyPath[i])) {
    keyPath[i] =
        keyPath[i].toString().replaceAllMapped(regExp, (Match m) => "${m[1]}");
  }
  var currentPath;
  if (isList) {
    data = List.from(data);
    currentPath = int.tryParse(keyPath[i].toString()) ?? 0;
    if (currentPath < 0 && data.isNotEmpty)
      currentPath = data.length + currentPath;
  } else
    currentPath = keyPath[i];
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
