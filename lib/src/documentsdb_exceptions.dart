class DocumentsDBException implements Exception {
  final message;

  DocumentsDBException(this.message);
  @override
  String toString() {
    // TODO: implement toString
    return message;
  }
}

const Message_Invalid_Param = '';
