// Copyright (c) 2006- Facebook
// Distributed under the Thrift Software License
//
// See accompanying file LICENSE or visit the Thrift site at:
// http://developers.facebook.com/thrift/

#ifndef _THRIFT_PROTOCOL_TPROTOCOL_H_
#define _THRIFT_PROTOCOL_TPROTOCOL_H_ 1

#include <transport/TTransport.h>
#include <protocol/TProtocolException.h>

#include <boost/shared_ptr.hpp>

#include <netinet/in.h>
#include <sys/types.h>
#include <string>
#include <map>

namespace facebook { namespace thrift { namespace protocol { 

using facebook::thrift::transport::TTransport;

#ifdef HAVE_ENDIAN_H
#include <endian.h>
#endif

#ifndef __BYTE_ORDER
# if defined(BYTE_ORDER) && defined(LITTLE_ENDIAN) && defined(BIG_ENDIAN)
#  define __BYTE_ORDER BYTE_ORDER
#  define __LITTLE_ENDIAN LITTLE_ENDIAN
#  define __BIG_ENDIAN BIG_ENDIAN
# else
#  error "Cannot determine endianness"
# endif
#endif

#if __BYTE_ORDER == __BIG_ENDIAN
# define ntohll(n) (n)
# define htonll(n) (n)
#elif __BYTE_ORDER == __LITTLE_ENDIAN
# if defined(__GNUC__) && defined(__GLIBC__)
#  include <byteswap.h>
#  define ntohll(n) bswap_64(n)
#  define htonll(n) bswap_64(n)
# else /* GNUC & GLIBC */
#  define ntohll(n) ( (((unsigned long long)ntohl(n)) << 32) + ntohl(n >> 32) )
#  define htonll(n) ( (((unsigned long long)htonl(n)) << 32) + htonl(n >> 32) )
# endif /* GNUC & GLIBC */
#else /* __BYTE_ORDER */
# error "Can't define htonll or ntohll!"
#endif

/**
 * Enumerated definition of the types that the Thrift protocol supports.
 * Take special note of the T_END type which is used specifically to mark
 * the end of a sequence of fields.
 */
enum TType {
  T_STOP       = 0,
  T_VOID       = 1,
  T_BOOL       = 2,
  T_BYTE       = 3,
  T_I08        = 3,
  T_I16        = 6,
  T_I32        = 8,
  T_U64        = 9,
  T_I64        = 10,
  T_DOUBLE     = 4,
  T_STRING     = 11,
  T_UTF7       = 11,
  T_STRUCT     = 12,
  T_MAP        = 13,
  T_SET        = 14,
  T_LIST       = 15,
  T_UTF8       = 16,
  T_UTF16      = 17
};

/**
 * Enumerated definition of the message types that the Thrift protocol
 * supports.
 */
enum TMessageType {
  T_CALL       = 1,
  T_REPLY      = 2,
  T_EXCEPTION  = 3
};

/**
 * Abstract class for a thrift protocol driver. These are all the methods that
 * a protocol must implement. Essentially, there must be some way of reading
 * and writing all the base types, plus a mechanism for writing out structs
 * with indexed fields.
 *
 * TProtocol objects should not be shared across multiple encoding contexts,
 * as they may need to maintain internal state in some protocols (i.e. XML).
 * Note that is is acceptable for the TProtocol module to do its own internal
 * buffered reads/writes to the underlying TTransport where appropriate (i.e.
 * when parsing an input XML stream, reading should be batched rather than
 * looking ahead character by character for a close tag).
 *
 * @author Mark Slee <mcslee@facebook.com>
 */
class TProtocol {
 public:
  virtual ~TProtocol() {}

  /**
   * Writing functions.
   */

  virtual uint32_t writeMessageBegin(const std::string& name,
                                     const TMessageType messageType,
                                     const int32_t seqid) = 0;

  virtual uint32_t writeMessageEnd() = 0;


  virtual uint32_t writeStructBegin(const std::string& name) = 0;
  
  virtual uint32_t writeStructEnd() = 0;
  
  virtual uint32_t writeFieldBegin(const std::string& name,
                                   const TType fieldType,
                                   const int16_t fieldId) = 0;

  virtual uint32_t writeFieldEnd() = 0;

  virtual uint32_t writeFieldStop() = 0;
                                      
  virtual uint32_t writeMapBegin(const TType keyType,
                                 const TType valType,
                                 const uint32_t size) = 0;

  virtual uint32_t writeMapEnd() = 0;
  
  virtual uint32_t writeListBegin(const TType elemType,
                                  const uint32_t size) = 0;

  virtual uint32_t writeListEnd() = 0;

  virtual uint32_t writeSetBegin(const TType elemType,
                                 const uint32_t size) = 0;

  virtual uint32_t writeSetEnd() = 0;

  virtual uint32_t writeBool(const bool value) = 0;

  virtual uint32_t writeByte(const int8_t byte) = 0;

  virtual uint32_t writeI16(const int16_t i16) = 0;

  virtual uint32_t writeI32(const int32_t i32) = 0;

  virtual uint32_t writeI64(const int64_t i64) = 0;

  virtual uint32_t writeDouble(const double dub) = 0;

  virtual uint32_t writeString(const std::string& str) = 0;

  /**
   * Reading functions
   */

  virtual uint32_t readMessageBegin(std::string& name,
                                    TMessageType& messageType,
                                    int32_t& seqid) = 0;
  
  virtual uint32_t readMessageEnd() = 0;

  virtual uint32_t readStructBegin(std::string& name) = 0;

  virtual uint32_t readStructEnd() = 0;

  virtual uint32_t readFieldBegin(std::string& name,
                                  TType& fieldType,
                                  int16_t& fieldId) = 0;
  
  virtual uint32_t readFieldEnd() = 0;
 
  virtual uint32_t readMapBegin(TType& keyType,
                                TType& valType,
                                uint32_t& size) = 0;

  virtual uint32_t readMapEnd() = 0;

  virtual uint32_t readListBegin(TType& elemType,
                                 uint32_t& size) = 0;

  virtual uint32_t readListEnd() = 0;

  virtual uint32_t readSetBegin(TType& elemType,
                                uint32_t& size) = 0;

  virtual uint32_t readSetEnd() = 0;

  virtual uint32_t readBool(bool& value) = 0;

  virtual uint32_t readByte(int8_t& byte) = 0;

  virtual uint32_t readI16(int16_t& i16) = 0;

  virtual uint32_t readI32(int32_t& i32) = 0;

  virtual uint32_t readI64(int64_t& i64) = 0;

  virtual uint32_t readDouble(double& dub) = 0;

  virtual uint32_t readString(std::string& str) = 0;

  /**
   * Method to arbitrarily skip over data.
   */
  uint32_t skip(TType type) {
    switch (type) {
    case T_BOOL:
      {
        bool boolv;
        return readBool(boolv);
      }
    case T_BYTE:
      {
        int8_t bytev;
        return readByte(bytev);
      }
    case T_I16:
      {
        int16_t i16;
        return readI16(i16);
      }
    case T_I32:
      {
        int32_t i32;
        return readI32(i32);
      }
    case T_I64:
      {
        int64_t i64;
        return readI64(i64);
      }
    case T_DOUBLE:
      {
        double dub;
        return readDouble(dub);
      }
    case T_STRING:
      {
        std::string str;
        return readString(str);
      }
    case T_STRUCT:
      {
        uint32_t result = 0;
        std::string name;
        int16_t fid;
        TType ftype;
        result += readStructBegin(name);
        while (true) {
          result += readFieldBegin(name, ftype, fid);
          if (ftype == T_STOP) {
            break;
          }
          result += skip(ftype);
          result += readFieldEnd();
        }
        result += readStructEnd();
        return result;
      }
    case T_MAP:
      {
        uint32_t result = 0;
        TType keyType;
        TType valType;
        uint32_t i, size;
        result += readMapBegin(keyType, valType, size);
        for (i = 0; i < size; i++) {
          result += skip(keyType);
          result += skip(valType);
        }
        result += readMapEnd();
        return result;
      }
    case T_SET:
      {
        uint32_t result = 0;
        TType elemType;
        uint32_t i, size;
        result += readSetBegin(elemType, size);
        for (i = 0; i < size; i++) {
          result += skip(elemType);
        }
        result += readSetEnd();
        return result;
      }
    case T_LIST:
      {
        uint32_t result = 0;
        TType elemType;
        uint32_t i, size;
        result += readListBegin(elemType, size);
        for (i = 0; i < size; i++) {
          result += skip(elemType);
        }
        result += readListEnd();
        return result;
      }
    default:
      return 0;
    }
  }

  inline boost::shared_ptr<TTransport> getTransport() {
    return ptrans_;
  }

  // TODO: remove these two calls, they are for backwards
  // compatibility
  inline boost::shared_ptr<TTransport> getInputTransport() {
    return ptrans_;
  }
  inline boost::shared_ptr<TTransport> getOutputTransport() {
    return ptrans_;
  }

 protected:
  TProtocol(boost::shared_ptr<TTransport> ptrans):
    ptrans_(ptrans) {
    trans_ = ptrans.get();
  }
    
  boost::shared_ptr<TTransport> ptrans_;
  TTransport* trans_;

 private:
  TProtocol() {}
};

/**
 * Constructs input and output protocol objects given transports.
 */
class TProtocolFactory {
 public:
  TProtocolFactory() {}

  virtual ~TProtocolFactory() {}

  virtual boost::shared_ptr<TProtocol> getProtocol(boost::shared_ptr<TTransport> trans) = 0;
};

}}} // facebook::thrift::protocol

#endif // #define _THRIFT_PROTOCOL_TPROTOCOL_H_ 1
