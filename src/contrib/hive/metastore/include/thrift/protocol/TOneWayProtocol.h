// Copyright (c) 2006- Facebook
// Distributed under the Thrift Software License
//
// See accompanying file LICENSE or visit the Thrift site at:
// http://developers.facebook.com/thrift/

#ifndef _THRIFT_PROTOCOL_TONEWAYPROTOCOL_H_
#define _THRIFT_PROTOCOL_TONEWAYPROTOCOL_H_ 1

#include "TProtocol.h"

namespace facebook { namespace thrift { namespace protocol { 

/**
 * Abstract class for implementing a protocol that can only be written,
 * not read.
 *
 * @author David Reiss <dreiss@facebook.com>
 */
class TWriteOnlyProtocol : public TProtocol {
 public:
  /**
   * @param subclass_name  The name of the concrete subclass.
   */
  TWriteOnlyProtocol(boost::shared_ptr<TTransport> trans,
                     const std::string& subclass_name)
    : TProtocol(trans)
    , subclass_(subclass_name)
  {}

  // All writing functions remain abstract.

  /**
   * Reading functions all throw an exception.
   */

  uint32_t readMessageBegin(std::string& name,
                            TMessageType& messageType,
                            int32_t& seqid) {
    throw TProtocolException(TProtocolException::NOT_IMPLEMENTED,
        subclass_ + " does not support reading (yet).");
  }

  uint32_t readMessageEnd() {
    throw TProtocolException(TProtocolException::NOT_IMPLEMENTED,
        subclass_ + " does not support reading (yet).");
  }

  uint32_t readStructBegin(std::string& name) {
    throw TProtocolException(TProtocolException::NOT_IMPLEMENTED,
        subclass_ + " does not support reading (yet).");
  }

  uint32_t readStructEnd() {
    throw TProtocolException(TProtocolException::NOT_IMPLEMENTED,
        subclass_ + " does not support reading (yet).");
  }

  uint32_t readFieldBegin(std::string& name,
                          TType& fieldType,
                          int16_t& fieldId) {
    throw TProtocolException(TProtocolException::NOT_IMPLEMENTED,
        subclass_ + " does not support reading (yet).");
  }
  
  uint32_t readFieldEnd() {
    throw TProtocolException(TProtocolException::NOT_IMPLEMENTED,
        subclass_ + " does not support reading (yet).");
  }
 
  uint32_t readMapBegin(TType& keyType,
                        TType& valType,
                        uint32_t& size) {
    throw TProtocolException(TProtocolException::NOT_IMPLEMENTED,
        subclass_ + " does not support reading (yet).");
  }

  uint32_t readMapEnd() {
    throw TProtocolException(TProtocolException::NOT_IMPLEMENTED,
        subclass_ + " does not support reading (yet).");
  }

  uint32_t readListBegin(TType& elemType,
                         uint32_t& size) {
    throw TProtocolException(TProtocolException::NOT_IMPLEMENTED,
        subclass_ + " does not support reading (yet).");
  }
  
  uint32_t readListEnd() {
    throw TProtocolException(TProtocolException::NOT_IMPLEMENTED,
        subclass_ + " does not support reading (yet).");
  }

  uint32_t readSetBegin(TType& elemType,
                        uint32_t& size) {
    throw TProtocolException(TProtocolException::NOT_IMPLEMENTED,
        subclass_ + " does not support reading (yet).");
  }

  uint32_t readSetEnd() {
    throw TProtocolException(TProtocolException::NOT_IMPLEMENTED,
        subclass_ + " does not support reading (yet).");
  }

  uint32_t readBool(bool& value) {
    throw TProtocolException(TProtocolException::NOT_IMPLEMENTED,
        subclass_ + " does not support reading (yet).");
  }

  uint32_t readByte(int8_t& byte) {
    throw TProtocolException(TProtocolException::NOT_IMPLEMENTED,
        subclass_ + " does not support reading (yet).");
  }

  uint32_t readI16(int16_t& i16) {
    throw TProtocolException(TProtocolException::NOT_IMPLEMENTED,
        subclass_ + " does not support reading (yet).");
  }

  uint32_t readI32(int32_t& i32) {
    throw TProtocolException(TProtocolException::NOT_IMPLEMENTED,
        subclass_ + " does not support reading (yet).");
  }

  uint32_t readI64(int64_t& i64) {
    throw TProtocolException(TProtocolException::NOT_IMPLEMENTED,
        subclass_ + " does not support reading (yet).");
  }

  uint32_t readDouble(double& dub) {
    throw TProtocolException(TProtocolException::NOT_IMPLEMENTED,
        subclass_ + " does not support reading (yet).");
  }

  uint32_t readString(std::string& str) {
    throw TProtocolException(TProtocolException::NOT_IMPLEMENTED,
        subclass_ + " does not support reading (yet).");
  }


 private:
  std::string subclass_;
};

}}} // facebook::thrift::protocol

#endif // #ifndef _THRIFT_PROTOCOL_TBINARYPROTOCOL_H_
