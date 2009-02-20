// Copyright (c) 2006- Facebook
// Distributed under the Thrift Software License
//
// See accompanying file LICENSE or visit the Thrift site at:
// http://developers.facebook.com/thrift/

#ifndef _THRIFT_TRANSPORT_TTRANSPORTEXCEPTION_H_
#define _THRIFT_TRANSPORT_TTRANSPORTEXCEPTION_H_ 1

#include <boost/lexical_cast.hpp>
#include <string>
#include <Thrift.h>

namespace facebook { namespace thrift { namespace transport { 

/**
 * Class to encapsulate all the possible types of transport errors that may
 * occur in various transport systems. This provides a sort of generic
 * wrapper around the shitty UNIX E_ error codes that lets a common code
 * base of error handling to be used for various types of transports, i.e.
 * pipes etc.
 *
 * @author Mark Slee <mcslee@facebook.com>
 */
class TTransportException : public facebook::thrift::TException {
 public:
  /**
   * Error codes for the various types of exceptions.
   */
  enum TTransportExceptionType {
    UNKNOWN = 0,
    NOT_OPEN = 1,
    ALREADY_OPEN = 2,
    TIMED_OUT = 3,
    END_OF_FILE = 4,
    INTERRUPTED = 5,
    BAD_ARGS = 6,
    CORRUPTED_DATA = 7,
    INTERNAL_ERROR = 8,
  };
  
  TTransportException() :
    facebook::thrift::TException(),
    type_(UNKNOWN) {}

  TTransportException(TTransportExceptionType type) :
    facebook::thrift::TException(), 
    type_(type) {}

  TTransportException(const std::string& message) :
    facebook::thrift::TException(message),
    type_(UNKNOWN) {}

  TTransportException(TTransportExceptionType type, const std::string& message) :
    facebook::thrift::TException(message),
    type_(type) {}

  TTransportException(TTransportExceptionType type,
                      const std::string& message,
                      int errno_copy) :
    facebook::thrift::TException(message + ": " + strerror_s(errno_copy)),
    type_(type) {}

  virtual ~TTransportException() throw() {}

  /**
   * Returns an error code that provides information about the type of error
   * that has occurred.
   *
   * @return Error code
   */
  TTransportExceptionType getType() const throw() {
    return type_;
  }

  virtual const char* what() const throw() {
    if (message_.empty()) {
      switch (type_) {
        case UNKNOWN        : return "TTransportException: Unknown transport exception";
        case NOT_OPEN       : return "TTransportException: Transport not open";
        case ALREADY_OPEN   : return "TTransportException: Transport already open";
        case TIMED_OUT      : return "TTransportException: Timed out";
        case END_OF_FILE    : return "TTransportException: End of file";
        case INTERRUPTED    : return "TTransportException: Interrupted";
        case BAD_ARGS       : return "TTransportException: Invalid arguments";
        case CORRUPTED_DATA : return "TTransportException: Corrupted Data";
        case INTERNAL_ERROR : return "TTransportException: Internal error";
        default             : return "TTransportException: (Invalid exception type)";
      }
    } else {
      return message_.c_str();
    }
  }
 
 protected:
  /** Just like strerror_r but returns a C++ string object. */
  std::string strerror_s(int errno_copy);

  /** Error code */
  TTransportExceptionType type_;

};

}}} // facebook::thrift::transport

#endif // #ifndef _THRIFT_TRANSPORT_TTRANSPORTEXCEPTION_H_
