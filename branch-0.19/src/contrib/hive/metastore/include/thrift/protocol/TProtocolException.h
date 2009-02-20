// Copyright (c) 2006- Facebook
// Distributed under the Thrift Software License
//
// See accompanying file LICENSE or visit the Thrift site at:
// http://developers.facebook.com/thrift/

#ifndef _THRIFT_PROTOCOL_TPROTOCOLEXCEPTION_H_
#define _THRIFT_PROTOCOL_TPROTOCOLEXCEPTION_H_ 1

#include <boost/lexical_cast.hpp>
#include <string>

namespace facebook { namespace thrift { namespace protocol { 

/**
 * Class to encapsulate all the possible types of protocol errors that may
 * occur in various protocol systems. This provides a sort of generic
 * wrapper around the shitty UNIX E_ error codes that lets a common code
 * base of error handling to be used for various types of protocols, i.e.
 * pipes etc.
 *
 * @author Mark Slee <mcslee@facebook.com>
 */
class TProtocolException : public facebook::thrift::TException {
 public:

  /**
   * Error codes for the various types of exceptions.
   */
  enum TProtocolExceptionType {
    UNKNOWN = 0,
    INVALID_DATA = 1,
    NEGATIVE_SIZE = 2,
    SIZE_LIMIT = 3,
    BAD_VERSION = 4,
    NOT_IMPLEMENTED = 5,
  };

  TProtocolException() :
    facebook::thrift::TException(),
    type_(UNKNOWN) {}

  TProtocolException(TProtocolExceptionType type) :
    facebook::thrift::TException(), 
    type_(type) {}

  TProtocolException(const std::string& message) :
    facebook::thrift::TException(message),
    type_(UNKNOWN) {}

  TProtocolException(TProtocolExceptionType type, const std::string& message) :
    facebook::thrift::TException(message),
    type_(type) {}

  virtual ~TProtocolException() throw() {}

  /**
   * Returns an error code that provides information about the type of error
   * that has occurred.
   *
   * @return Error code
   */
  TProtocolExceptionType getType() {
    return type_;
  }

  virtual const char* what() const throw() {
    if (message_.empty()) {
      switch (type_) {
        case UNKNOWN         : return "TProtocolException: Unknown protocol exception";
        case INVALID_DATA    : return "TProtocolException: Invalid data";
        case NEGATIVE_SIZE   : return "TProtocolException: Negative size";
        case SIZE_LIMIT      : return "TProtocolException: Exceeded size limit";
        case BAD_VERSION     : return "TProtocolException: Invalid version";
        case NOT_IMPLEMENTED : return "TProtocolException: Not implemented";
        default              : return "TProtocolException: (Invalid exception type)";
      }
    } else {
      return message_.c_str();
    }
  }

 protected:
  /** 
   * Error code
   */
  TProtocolExceptionType type_;
 
};

}}} // facebook::thrift::protocol

#endif // #ifndef _THRIFT_PROTOCOL_TPROTOCOLEXCEPTION_H_
