// Copyright (c) 2006- Facebook
// Distributed under the Thrift Software License
//
// See accompanying file LICENSE or visit the Thrift site at:
// http://developers.facebook.com/thrift/

#ifndef _THRIFT_TRANSPORT_TZLIBTRANSPORT_H_
#define _THRIFT_TRANSPORT_TZLIBTRANSPORT_H_ 1

#include <boost/lexical_cast.hpp>
#include <transport/TTransport.h>

struct z_stream_s;

namespace facebook { namespace thrift { namespace transport { 

class TZlibTransportException : public TTransportException {
 public:
  TZlibTransportException(int status, const char* msg) :
    TTransportException(TTransportException::INTERNAL_ERROR,
                        errorMessage(status, msg)),
    zlib_status_(status),
    zlib_msg_(msg == NULL ? "(null)" : msg) {}

  virtual ~TZlibTransportException() throw() {}

  int getZlibStatus() { return zlib_status_; }
  std::string getZlibMessage() { return zlib_msg_; }

  static std::string errorMessage(int status, const char* msg) {
    std::string rv = "zlib error: ";
    if (msg) {
      rv += msg;
    } else {
      rv += "(no message)";
    }
    rv += " (status = ";
    rv += boost::lexical_cast<std::string>(status);
    rv += ")";
    return rv;
  }

  int zlib_status_;
  std::string zlib_msg_;
};

/**
 * This transport uses zlib's compressed format on the "far" side.
 *
 * There are two kinds of TZlibTransport objects:
 * - Standalone objects are used to encode self-contained chunks of data
 *   (like structures).  They include checksums.
 * - Non-standalone transports are used for RPC.  They are not implemented yet.
 *
 * TODO(dreiss): Don't do an extra copy of the compressed data if
 *               the underlying transport is TBuffered or TMemory.
 *
 * @author David Reiss <dreiss@facebook.com>
 */
class TZlibTransport : public TTransport {
 public:

  /**
   * @param transport    The transport to read compressed data from
   *                     and write compressed data to.
   * @param use_for_rpc  True if this object will be used for RPC,
   *                     false if this is a standalone object.
   * @param urbuf_size   Uncompressed buffer size for reading.
   * @param crbuf_size   Compressed buffer size for reading.
   * @param uwbuf_size   Uncompressed buffer size for writing.
   * @param cwbuf_size   Compressed buffer size for writing.
   *
   * TODO(dreiss): Write a constructor that isn't a pain.
   */
  TZlibTransport(boost::shared_ptr<TTransport> transport,
                 bool use_for_rpc,
                 int urbuf_size = DEFAULT_URBUF_SIZE,
                 int crbuf_size = DEFAULT_CRBUF_SIZE,
                 int uwbuf_size = DEFAULT_UWBUF_SIZE,
                 int cwbuf_size = DEFAULT_CWBUF_SIZE) :
    transport_(transport),
    standalone_(!use_for_rpc),
    urpos_(0),
    uwpos_(0),
    input_ended_(false),
    output_flushed_(false),
    urbuf_size_(urbuf_size),
    crbuf_size_(crbuf_size),
    uwbuf_size_(uwbuf_size),
    cwbuf_size_(cwbuf_size),
    urbuf_(NULL),
    crbuf_(NULL),
    uwbuf_(NULL),
    cwbuf_(NULL),
    rstream_(NULL),
    wstream_(NULL)
  {

    if (!standalone_) {
      throw TTransportException(
          TTransportException::BAD_ARGS,
          "TZLibTransport has not been tested for RPC.");
    }

    if (uwbuf_size_ < MIN_DIRECT_DEFLATE_SIZE) {
      // Have to copy this into a local because of a linking issue.
      int minimum = MIN_DIRECT_DEFLATE_SIZE;
      throw TTransportException(
          TTransportException::BAD_ARGS,
          "TZLibTransport: uncompressed write buffer must be at least"
          + boost::lexical_cast<std::string>(minimum) + ".");
    }

    try {
      urbuf_ = new uint8_t[urbuf_size];
      crbuf_ = new uint8_t[crbuf_size];
      uwbuf_ = new uint8_t[uwbuf_size];
      cwbuf_ = new uint8_t[cwbuf_size];

      // Don't call this outside of the constructor.
      initZlib();

    } catch (...) {
      delete[] urbuf_;
      delete[] crbuf_;
      delete[] uwbuf_;
      delete[] cwbuf_;
      throw;
    }
  }

  // Don't call this outside of the constructor.
  void initZlib();

  ~TZlibTransport();

  bool isOpen();
  
  void open() {
    transport_->open();
  }

  void close() {
    transport_->close();
  }

  uint32_t read(uint8_t* buf, uint32_t len);
  
  void write(const uint8_t* buf, uint32_t len);

  void flush();

  bool borrow(uint8_t* buf, uint32_t len);

  void consume(uint32_t len);

  void verifyChecksum();

   /**
    * TODO(someone_smart): Choose smart defaults.
    */
  static const int DEFAULT_URBUF_SIZE = 128;
  static const int DEFAULT_CRBUF_SIZE = 1024;
  static const int DEFAULT_UWBUF_SIZE = 128;
  static const int DEFAULT_CWBUF_SIZE = 1024;

 protected:

  inline void checkZlibRv(int status, const char* msg);
  inline void checkZlibRvNothrow(int status, const char* msg);
  inline int readAvail();
  void flushToZlib(const uint8_t* buf, int len, bool finish = false);

  // Writes smaller than this are buffered up.
  // Larger (or equal) writes are dumped straight to zlib.
  static const int MIN_DIRECT_DEFLATE_SIZE = 32;

  boost::shared_ptr<TTransport> transport_;
  bool standalone_;

  int urpos_;
  int uwpos_;

  /// True iff zlib has reached the end of a stream.
  /// This is only ever true in standalone protcol objects.
  bool input_ended_;
  /// True iff we have flushed the output stream.
  /// This is only ever true in standalone protcol objects.
  bool output_flushed_;

  int urbuf_size_;
  int crbuf_size_;
  int uwbuf_size_;
  int cwbuf_size_;

  uint8_t* urbuf_;
  uint8_t* crbuf_;
  uint8_t* uwbuf_;
  uint8_t* cwbuf_;

  struct z_stream_s* rstream_;
  struct z_stream_s* wstream_;
};

}}} // facebook::thrift::transport

#endif // #ifndef _THRIFT_TRANSPORT_TZLIBTRANSPORT_H_
