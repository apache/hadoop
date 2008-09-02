// Copyright (c) 2006- Facebook
// Distributed under the Thrift Software License
//
// See accompanying file LICENSE or visit the Thrift site at:
// http://developers.facebook.com/thrift/

#ifndef _THRIFT_TRANSPORT_TTRANSPORT_H_
#define _THRIFT_TRANSPORT_TTRANSPORT_H_ 1

#include <Thrift.h>
#include <boost/shared_ptr.hpp>
#include <transport/TTransportException.h>
#include <string>

namespace facebook { namespace thrift { namespace transport { 

/**
 * Generic interface for a method of transporting data. A TTransport may be
 * capable of either reading or writing, but not necessarily both.
 *
 * @author Mark Slee <mcslee@facebook.com>
 */
class TTransport {
 public:
  /**
   * Virtual deconstructor.
   */
  virtual ~TTransport() {}

  /**
   * Whether this transport is open.
   */
  virtual bool isOpen() {
    return false;
  }

  /**
   * Tests whether there is more data to read or if the remote side is
   * still open. By default this is true whenever the transport is open,
   * but implementations should add logic to test for this condition where
   * possible (i.e. on a socket).
   * This is used by a server to check if it should listen for another
   * request.
   */
  virtual bool peek() {
    return isOpen();
  }

  /**
   * Opens the transport for communications.
   *
   * @return bool Whether the transport was successfully opened
   * @throws TTransportException if opening failed
   */
  virtual void open() {
    throw TTransportException(TTransportException::NOT_OPEN, "Cannot open base TTransport.");
  }

  /**
   * Closes the transport.
   */
  virtual void close() {
    throw TTransportException(TTransportException::NOT_OPEN, "Cannot close base TTransport.");
  }

  /**
   * Attempt to read up to the specified number of bytes into the string.
   *
   * @param buf  Reference to the location to write the data
   * @param len  How many bytes to read
   * @return How many bytes were actually read
   * @throws TTransportException If an error occurs
   */
  virtual uint32_t read(uint8_t* buf, uint32_t len) {
    throw TTransportException(TTransportException::NOT_OPEN, "Base TTransport cannot read.");
  }

  /**
   * Reads the given amount of data in its entirety no matter what.
   *
   * @param s     Reference to location for read data
   * @param len   How many bytes to read
   * @return How many bytes read, which must be equal to size
   * @throws TTransportException If insufficient data was read
   */
  virtual uint32_t readAll(uint8_t* buf, uint32_t len) {
    uint32_t have = 0;
    uint32_t get = 0;
    
    while (have < len) {
      get = read(buf+have, len-have);
      if (get <= 0) {
        throw TTransportException("No more data to read.");
      }
      have += get;
    }
    
    return have;
  }

  /**
   * Called when read is completed. 
   * This can be over-ridden to perform a transport-specific action
   * e.g. logging the request to a file
   *
   */
  virtual void readEnd() {
    // default behaviour is to do nothing
    return;
  }

  /**
   * Writes the string in its entirety to the buffer.
   *
   * @param buf  The data to write out
   * @throws TTransportException if an error occurs
   */
  virtual void write(const uint8_t* buf, uint32_t len) {
    throw TTransportException(TTransportException::NOT_OPEN, "Base TTransport cannot write.");
  }

  /**
   * Called when write is completed. 
   * This can be over-ridden to perform a transport-specific action
   * at the end of a request.
   *
   */
  virtual void writeEnd() {
    // default behaviour is to do nothing
    return;
  }

  /**
   * Flushes any pending data to be written. Typically used with buffered
   * transport mechanisms.
   *
   * @throws TTransportException if an error occurs
   */
  virtual void flush() {}

  /**
   * Attempts to copy len bytes from the transport into buf.  Does not consume
   * the bytes read (i.e.: a later read will return the same data).  This
   * method is meant to support protocols that need to read variable-length
   * fields.  They can attempt to borrow the maximum amount of data that they
   * will need, then consume (see next method) what they actually use.  Some
   * transports will not support this method and others will fail occasionally,
   * so protocols must be prepared to use read if borrow fails.
   *
   * @oaram buf  The buffer to store the data
   * @param len  How much data to borrow
   * @return true if the requested data has been borrowed, false otherwise
   * @throws TTransportException if an error occurs
   */
  virtual bool borrow(uint8_t* buf, uint32_t len) {
    return false;
  }

  /**
   * Remove len bytes from the transport.  This should always follow a borrow
   * of at least len bytes, and should always succeed.
   * TODO(dreiss): Is there any transport that could borrow but fail to
   * consume, or that would require a buffer to dump the consumed data?
   *
   * @param len  How many bytes to consume
   * @throws TTransportException If an error occurs
   */
  virtual void consume(uint32_t len) {
    throw TTransportException(TTransportException::NOT_OPEN, "Base TTransport cannot consume.");
  }

 protected:
  /**
   * Simple constructor.
   */
  TTransport() {}
};

/**
 * Generic factory class to make an input and output transport out of a
 * source transport. Commonly used inside servers to make input and output
 * streams out of raw clients.
 *
 * @author Mark Slee <mcslee@facebook.com>
 */
class TTransportFactory {
 public:
  TTransportFactory() {}

  virtual ~TTransportFactory() {}

  /**
   * Default implementation does nothing, just returns the transport given.
   */
  virtual boost::shared_ptr<TTransport> getTransport(boost::shared_ptr<TTransport> trans) {
    return trans;
  }

};

}}} // facebook::thrift::transport

#endif // #ifndef _THRIFT_TRANSPORT_TTRANSPORT_H_
