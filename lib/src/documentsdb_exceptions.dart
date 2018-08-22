class DocumentsDBException implements Exception {
  final message;

  DocumentsDBException(this.message);
  @override
  String toString() {
    return message;
  }
}

const Message_Invalid_Param = '';
