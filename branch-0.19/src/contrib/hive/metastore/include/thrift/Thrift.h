// Copyright (c) 2006- Facebook
// Distributed under the Thrift Software License
//
// See accompanying file LICENSE or visit the Thrift site at:
// http://developers.facebook.com/thrift/

#ifndef _THRIFT_THRIFT_H_
#define _THRIFT_THRIFT_H_ 1

#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#include <netinet/in.h>
#ifdef HAVE_INTTYPES_H
#include <inttypes.h>
#endif
#include <string>
#include <map>
#include <list>
#include <set>
#include <vector>
#include <exception>

#include "TLogging.h"

namespace facebook { namespace thrift {

class TOutput {
 public:
  TOutput() : f_(&perrorTimeWrapper) {}

  inline void setOutputFunction(void (*function)(const char *)){
    f_ = function;
  }

  inline void operator()(const char *message){
    f_(message);
  }

  inline static void perrorTimeWrapper(const char* msg) {
    time_t now;
    char dbgtime[25];
    time(&now);
    ctime_r(&now, dbgtime);
    dbgtime[24] = 0;
    fprintf(stderr, "%s ", dbgtime);
    perror(msg);
  }
 private:
  void (*f_)(const char *);
};

extern TOutput GlobalOutput;

namespace protocol {
  class TProtocol;
}

class TException : public std::exception {
 public:
  TException() {}

  TException(const std::string& message) :
    message_(message) {}

  virtual ~TException() throw() {}

  virtual const char* what() const throw() {
    if (message_.empty()) {
      return "Default TException.";
    } else {
      return message_.c_str();
    }
  }

 protected:
  std::string message_;

};

class TApplicationException : public TException {
 public:

  /**
   * Error codes for the various types of exceptions.
   */
  enum TApplicationExceptionType {
    UNKNOWN = 0,
    UNKNOWN_METHOD = 1,
    INVALID_MESSAGE_TYPE = 2,
    WRONG_METHOD_NAME = 3,
    BAD_SEQUENCE_ID = 4,
    MISSING_RESULT = 5,
  };

  TApplicationException() :
    TException(),
    type_(UNKNOWN) {}

  TApplicationException(TApplicationExceptionType type) :
    TException(), 
    type_(type) {}

  TApplicationException(const std::string& message) :
    TException(message),
    type_(UNKNOWN) {}

  TApplicationException(TApplicationExceptionType type,
                        const std::string& message) :
    TException(message),
    type_(type) {}

  virtual ~TApplicationException() throw() {}

  /**
   * Returns an error code that provides information about the type of error
   * that has occurred.
   *
   * @return Error code
   */
  TApplicationExceptionType getType() {
    return type_;
  }

  virtual const char* what() const throw() {
    if (message_.empty()) {
      switch (type_) {
        case UNKNOWN              : return "TApplicationException: Unknown application exception";
        case UNKNOWN_METHOD       : return "TApplicationException: Unknown method";
        case INVALID_MESSAGE_TYPE : return "TApplicationException: Invalid message type";
        case WRONG_METHOD_NAME    : return "TApplicationException: Wrong method name";
        case BAD_SEQUENCE_ID      : return "TApplicationException: Bad sequence identifier";
        case MISSING_RESULT       : return "TApplicationException: Missing result";
        default                   : return "TApplicationException: (Invalid exception type)";
      };
    } else {
      return message_.c_str();
    }
  }

  uint32_t read(protocol::TProtocol* iprot);
  uint32_t write(protocol::TProtocol* oprot) const;

 protected:
  /**
   * Error code
   */
  TApplicationExceptionType type_;

};


// Forward declare this structure used by TDenseProtocol
namespace reflection { namespace local {
struct TypeSpec;
}}


}} // facebook::thrift

#endif // #ifndef _THRIFT_THRIFT_H_
