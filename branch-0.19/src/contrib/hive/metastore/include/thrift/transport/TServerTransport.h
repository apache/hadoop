// Copyright (c) 2006- Facebook
// Distributed under the Thrift Software License
//
// See accompanying file LICENSE or visit the Thrift site at:
// http://developers.facebook.com/thrift/

#ifndef _THRIFT_TRANSPORT_TSERVERTRANSPORT_H_
#define _THRIFT_TRANSPORT_TSERVERTRANSPORT_H_ 1

#include "TTransport.h"
#include "TTransportException.h"
#include <boost/shared_ptr.hpp>

namespace facebook { namespace thrift { namespace transport { 

/**
 * Server transport framework. A server needs to have some facility for
 * creating base transports to read/write from.
 *
 * @author Mark Slee <mcslee@facebook.com>
 */
class TServerTransport {
 public:
  virtual ~TServerTransport() {}

  /**
   * Starts the server transport listening for new connections. Prior to this
   * call most transports will not return anything when accept is called.
   *
   * @throws TTransportException if we were unable to listen
   */
  virtual void listen() {}

  /**
   * Gets a new dynamically allocated transport object and passes it to the
   * caller. Note that it is the explicit duty of the caller to free the
   * allocated object. The returned TTransport object must always be in the
   * opened state. NULL should never be returned, instead an Exception should
   * always be thrown.
   *
   * @return A new TTransport object
   * @throws TTransportException if there is an error
   */
  boost::shared_ptr<TTransport> accept() {
    boost::shared_ptr<TTransport> result = acceptImpl();
    if (result == NULL) {
      throw TTransportException("accept() may not return NULL");
    }
    return result;
  }

  /**
   * For "smart" TServerTransport implementations that work in a multi
   * threaded context this can be used to break out of an accept() call.
   * It is expected that the transport will throw a TTransportException
   * with the interrupted error code.
   */
  virtual void interrupt() {}

  /**
   * Closes this transport such that future calls to accept will do nothing.
   */
  virtual void close() = 0;

 protected:
  TServerTransport() {}

  /**
   * Subclasses should implement this function for accept.
   *
   * @return A newly allocated TTransport object
   * @throw TTransportException If an error occurs
   */
  virtual boost::shared_ptr<TTransport> acceptImpl() = 0;

};

}}} // facebook::thrift::transport

#endif // #ifndef _THRIFT_TRANSPORT_TSERVERTRANSPORT_H_
