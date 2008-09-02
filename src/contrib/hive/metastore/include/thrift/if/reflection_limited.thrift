#!/usr/local/bin/thrift -php -java -cpp -py

// NOTICE!!!
// DO NOT FORGET to run regen.sh if you change this file
// (or if you change the compiler).

// This interface is deprecated.
// There is no replacement yet, but I hate it so much that
// I'm deprecating it before it's done.
// @author I'm too ashamed to say.

// dreiss naively thinks he knows how to do this better,
// so talk to him if you are interested in taking it on,
// or if you just want someone to make it better for you.

namespace cpp facebook.thrift.reflection.limited
namespace java com.facebook.thrift.reflection.limited
py_module thrift.reflection.limited

enum TTypeTag {
  T_VOID   = 1,
  T_BOOL   = 2,
  T_BYTE   = 3,
  T_I16    = 6,
  T_I32    = 8,
  T_I64    = 10,
  T_DOUBLE = 4,
  T_STRING = 11,
  T_STRUCT = 12,
  T_MAP    = 13,
  T_SET    = 14,
  T_LIST   = 15,
  // This doesn't exist in TBinaryProtocol, but it could be useful for reflection.
  T_ENUM   = 101,
  T_NOT_REFLECTED = 102,
}

struct SimpleType {
  1: TTypeTag ttype,
  2: string name,  // For structs and emums.
}

struct ContainerType {
  1: TTypeTag ttype,
  2:          SimpleType subtype1,
  3: optional SimpleType subtype2,
}

struct ThriftType {
  1: bool is_container,
  2: optional SimpleType simple_type,
  3: optional ContainerType container_type,
}

struct Argument {
  1: i16 key,
  2: string name,
  3: ThriftType type,
}

struct Method {
  1: string name,
  2: ThriftType return_type,
  3: list<Argument> arguments,
}

struct Service {
  1: string name,
  2: list<Method> methods,
  3: bool fully_reflected,
}
