// Copyright (c) 2006- Facebook
// Distributed under the Thrift Software License
//
// See accompanying file LICENSE or visit the Thrift site at:
// http://developers.facebook.com/thrift/

#ifndef _THRIFT_TREFLECTIONLOCAL_H_
#define _THRIFT_TREFLECTIONLOCAL_H_ 1

#include <stdint.h>
#include <cstring>
#include <protocol/TProtocol.h>

/**
 * Local Reflection is a blanket term referring to the the structure
 * and generation of this particular representation of Thrift types.
 * (It is called local because it cannot be serialized by Thrift).
 *
 * @author David Reiss <dreiss@facebook.com>
 */

namespace facebook { namespace thrift { namespace reflection { namespace local {

using facebook::thrift::protocol::TType;

// We include this many bytes of the structure's fingerprint when serializing
// a top-level structure.  Long enough to make collisions unlikely, short
// enough to not significantly affect the amount of memory used.
const int FP_PREFIX_LEN = 4;

struct FieldMeta {
  int16_t tag;
  bool is_optional;
};

struct TypeSpec {
  TType ttype;
  uint8_t    fp_prefix[FP_PREFIX_LEN];

  // Use an anonymous union here so we can fit two TypeSpecs in one cache line.
  union {
    struct {
      // Use parallel arrays here for denser packing (of the arrays).
      FieldMeta* metas;
      TypeSpec** specs;
    } tstruct;
    struct {
      TypeSpec *subtype1;
      TypeSpec *subtype2;
    } tcontainer;
  };

  // Static initialization of unions isn't really possible,
  // so take the plunge and use constructors.
  // Hopefully they'll be evaluated at compile time.

  TypeSpec(TType ttype) : ttype(ttype) {
    std::memset(fp_prefix, 0, FP_PREFIX_LEN);
  }

  TypeSpec(TType ttype,
           const uint8_t* fingerprint,
           FieldMeta* metas,
           TypeSpec** specs) :
    ttype(ttype)
  {
    std::memcpy(fp_prefix, fingerprint, FP_PREFIX_LEN);
    tstruct.metas = metas;
    tstruct.specs = specs;
  }

  TypeSpec(TType ttype, TypeSpec* subtype1, TypeSpec* subtype2) :
    ttype(ttype)
  {
    std::memset(fp_prefix, 0, FP_PREFIX_LEN);
    tcontainer.subtype1 = subtype1;
    tcontainer.subtype2 = subtype2;
  }

};

}}}} // facebook::thrift::reflection::local

#endif // #ifndef _THRIFT_TREFLECTIONLOCAL_H_
