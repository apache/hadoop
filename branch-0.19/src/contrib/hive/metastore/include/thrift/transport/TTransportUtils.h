// Copyright (c) 2006- Facebook
// Distributed under the Thrift Software License
//
// See accompanying file LICENSE or visit the Thrift site at:
// http://developers.facebook.com/thrift/

#ifndef _THRIFT_TRANSPORT_TTRANSPORTUTILS_H_
#define _THRIFT_TRANSPORT_TTRANSPORTUTILS_H_ 1

#include <string>
#include <algorithm>
#include <transport/TTransport.h>
#include <transport/TFileTransport.h>

namespace facebook { namespace thrift { namespace transport {

/**
 * The null transport is a dummy transport that doesn't actually do anything.
 * It's sort of an analogy to /dev/null, you can never read anything from it
 * and it will let you write anything you want to it, though it won't actually
 * go anywhere.
 *
 * @author Mark Slee <mcslee@facebook.com>
 */
class TNullTransport : public TTransport {
 public:
  TNullTransport() {}

  ~TNullTransport() {}

  bool isOpen() {
    return true;
  }

  void open() {}

  void write(const uint8_t* buf, uint32_t len) {
    return;
  }

};


/**
 * Buffered transport. For reads it will read more data than is requested
 * and will serve future data out of a local buffer. For writes, data is
 * stored to an in memory buffer before being written out.
 *
 * @author Mark Slee <mcslee@facebook.com>
 */
class TBufferedTransport : public TTransport {
 public:
  TBufferedTransport(boost::shared_ptr<TTransport> transport) :
    transport_(transport),
    rBufSize_(512), rPos_(0), rLen_(0),
    wBufSize_(512), wLen_(0) {
    rBuf_ = new uint8_t[rBufSize_];
    wBuf_ = new uint8_t[wBufSize_];
  }

  TBufferedTransport(boost::shared_ptr<TTransport> transport, uint32_t sz) :
    transport_(transport),
    rBufSize_(sz), rPos_(0), rLen_(0),
    wBufSize_(sz), wLen_(0) {
    rBuf_ = new uint8_t[rBufSize_];
    wBuf_ = new uint8_t[wBufSize_];
  }

  TBufferedTransport(boost::shared_ptr<TTransport> transport, uint32_t rsz, uint32_t wsz) :
    transport_(transport),
    rBufSize_(rsz), rPos_(0), rLen_(0),
    wBufSize_(wsz), wLen_(0) {
    rBuf_ = new uint8_t[rBufSize_];
    wBuf_ = new uint8_t[wBufSize_];
  }

  ~TBufferedTransport() {
    delete [] rBuf_;
    delete [] wBuf_;
  }

  bool isOpen() {
    return transport_->isOpen();
  }

  bool peek() {
    if (rPos_ >= rLen_) {
      rLen_ = transport_->read(rBuf_, rBufSize_);
      rPos_ = 0;
    }
    return (rLen_ > rPos_);
  }

  void open() {
    transport_->open();
  }

  void close() {
    flush();
    transport_->close();
  }

  uint32_t read(uint8_t* buf, uint32_t len);

  void write(const uint8_t* buf, uint32_t len);

  void flush();

  bool borrow(uint8_t* buf, uint32_t len);

  void consume(uint32_t len);

  boost::shared_ptr<TTransport> getUnderlyingTransport() {
    return transport_;
  }

 protected:
  boost::shared_ptr<TTransport> transport_;
  uint8_t* rBuf_;
  uint32_t rBufSize_;
  uint32_t rPos_;
  uint32_t rLen_;

  uint8_t* wBuf_;
  uint32_t wBufSize_;
  uint32_t wLen_;
};

/**
 * Wraps a transport into a buffered one.
 *
 * @author Mark Slee <mcslee@facebook.com>
 */
class TBufferedTransportFactory : public TTransportFactory {
 public:
  TBufferedTransportFactory() {}

  virtual ~TBufferedTransportFactory() {}

  /**
   * Wraps the transport into a buffered one.
   */
  virtual boost::shared_ptr<TTransport> getTransport(boost::shared_ptr<TTransport> trans) {
    return boost::shared_ptr<TTransport>(new TBufferedTransport(trans));
  }

};

/**
 * Framed transport. All writes go into an in-memory buffer until flush is
 * called, at which point the transport writes the length of the entire
 * binary chunk followed by the data payload. This allows the receiver on the
 * other end to always do fixed-length reads.
 *
 * @author Mark Slee <mcslee@facebook.com>
 */
class TFramedTransport : public TTransport {
 public:
  TFramedTransport(boost::shared_ptr<TTransport> transport) :
    transport_(transport),
    rPos_(0),
    rLen_(0),
    read_(true),
    wBufSize_(512),
    wLen_(0),
    write_(true) {
    rBuf_ = NULL;
    wBuf_ = new uint8_t[wBufSize_];
  }

  TFramedTransport(boost::shared_ptr<TTransport> transport, uint32_t sz) :
    transport_(transport),
    rPos_(0),
    rLen_(0),
    read_(true),
    wBufSize_(sz),
    wLen_(0),
    write_(true) {
    rBuf_ = NULL;
    wBuf_ = new uint8_t[wBufSize_];
  }

  ~TFramedTransport() {
    if (rBuf_ != NULL) {
      delete [] rBuf_;
    }
    if (wBuf_ != NULL) {
      delete [] wBuf_;
    }
  }

  void setRead(bool read) {
    read_ = read;
  }

  void setWrite(bool write) {
    write_ = write;
  }

  void open() {
    transport_->open();
  }

  bool isOpen() {
    return transport_->isOpen();
  }

  bool peek() {
    if (rPos_ < rLen_) {
      return true;
    }
    return transport_->peek();
  }

  void close() {
    if (wLen_ > 0) {
      flush();
    }
    transport_->close();
  }

  uint32_t read(uint8_t* buf, uint32_t len);

  void write(const uint8_t* buf, uint32_t len);

  void flush();

  bool borrow(uint8_t* buf, uint32_t len);

  void consume(uint32_t len);

  boost::shared_ptr<TTransport> getUnderlyingTransport() {
    return transport_;
  }

 protected:
  boost::shared_ptr<TTransport> transport_;
  uint8_t* rBuf_;
  uint32_t rPos_;
  uint32_t rLen_;
  bool read_;

  uint8_t* wBuf_;
  uint32_t wBufSize_;
  uint32_t wLen_;
  bool write_;

  /**
   * Reads a frame of input from the underlying stream.
   */
  void readFrame();
};

/**
 * Wraps a transport into a framed one.
 *
 * @author Dave Simpson <dave@powerset.com>
 */
class TFramedTransportFactory : public TTransportFactory {
 public:
  TFramedTransportFactory() {}

  virtual ~TFramedTransportFactory() {}

  /**
   * Wraps the transport into a framed one.
   */
  virtual boost::shared_ptr<TTransport> getTransport(boost::shared_ptr<TTransport> trans) {
    return boost::shared_ptr<TTransport>(new TFramedTransport(trans));
  }

};


/**
 * A memory buffer is a tranpsort that simply reads from and writes to an
 * in memory buffer. Anytime you call write on it, the data is simply placed
 * into a buffer, and anytime you call read, data is read from that buffer.
 *
 * The buffers are allocated using C constructs malloc,realloc, and the size
 * doubles as necessary.
 *
 * @author Mark Slee <mcslee@facebook.com>
 */
class TMemoryBuffer : public TTransport {
 private:

  // Common initialization done by all constructors.
  void initCommon(uint8_t* buf, uint32_t size, bool owner, uint32_t wPos) {
    if (buf == NULL && size != 0) {
      assert(owner);
      buf = (uint8_t*)malloc(size);
      if (buf == NULL) {
        throw TTransportException("Out of memory");
      }
    }

    buffer_ = buf;
    bufferSize_ = size;
    owner_ = owner;
    wPos_ = wPos;
    rPos_ = 0;
  }

 public:
  static const uint32_t defaultSize = 1024;

  TMemoryBuffer() {
    initCommon(NULL, defaultSize, true, 0);
  }

  TMemoryBuffer(uint32_t sz) {
    initCommon(NULL, sz, true, 0);
  }

  // transferOwnership should be true if you want TMemoryBuffer to call free(buf).
  TMemoryBuffer(uint8_t* buf, int sz, bool transferOwnership = false) {
    initCommon(buf, sz, transferOwnership, sz);
  }

  // copy should be true if you want TMemoryBuffer to make a copy of the string.
  // If you set copy to false, the string must not be destroyed before you are
  // done with the TMemoryBuffer.
  TMemoryBuffer(const std::string& str, bool copy = false) {
    if (copy) {
      initCommon(NULL, str.length(), true, 0);
      this->write(reinterpret_cast<const uint8_t*>(str.data()), str.length());
    } else {
      // This first argument should be equivalent to the following:
      // const_cast<uint8_t*>(reinterpret_cast<const uint8_t*>(str.data()))
      initCommon((uint8_t*)str.data(), str.length(), false, str.length());
    }
  }

  ~TMemoryBuffer() {
    if (owner_) {
      free(buffer_);
      buffer_ = NULL;
    }
  }

  bool isOpen() {
    return true;
  }

  bool peek() {
    return (rPos_ < wPos_);
  }

  void open() {}

  void close() {}

  void getBuffer(uint8_t** bufPtr, uint32_t* sz) {
    *bufPtr = buffer_;
    *sz = wPos_;
  }

  std::string getBufferAsString() {
    if (buffer_ == NULL) {
      return "";
    }
    return std::string((char*)buffer_, (std::string::size_type)wPos_);
  }

  void appendBufferToString(std::string& str) {
    if (buffer_ == NULL) {
      return;
    }
    str.append((char*)buffer_, wPos_);
  }

  void resetBuffer() {
    wPos_ = 0;
    rPos_ = 0;
  }

  // transferOwnership should be true if you want TMemoryBuffer to call free(buf).
  void resetBuffer(uint8_t* buf, uint32_t sz, bool transferOwnership = false) {
    if (owner_) {
      free(buffer_);
    }
    owner_ = transferOwnership;
    buffer_ = buf;
    bufferSize_ = sz;
    wPos_ = sz;
    rPos_ = 0;
  }

  // See the constructor that takes a string.
  void resetFromString(const std::string& str, bool copy = false) {
    if (copy) {
      uint8_t* buf = (uint8_t*)malloc(str.length());
      if (buf == NULL) {
        throw TTransportException("Out of memory");
      }
      std::copy(str.begin(), str.end(), buf);
      resetBuffer(buf, str.length(), true);
    } else {
      // See the above comment about const_cast.
      resetBuffer((uint8_t*)str.data(), str.length(), false);
    }
  }

  uint32_t read(uint8_t* buf, uint32_t len);

  std::string readAsString(uint32_t len) {
    std::string str;
    (void)readAppendToString(str, len);
    return str;
  }

  uint32_t readAppendToString(std::string& str, uint32_t len);

  void readEnd() {
    if (rPos_ == wPos_) {
      resetBuffer();
    }
  }

  void write(const uint8_t* buf, uint32_t len);

  uint32_t available() {
    return wPos_ - rPos_;
  }

  bool borrow(uint8_t* buf, uint32_t len);

  void consume(uint32_t len);

 private:
  // Data buffer
  uint8_t* buffer_;

  // Allocated buffer size
  uint32_t bufferSize_;

  // Where the write is at
  uint32_t wPos_;

  // Where the reader is at
  uint32_t rPos_;

  // Is this object the owner of the buffer?
  bool owner_;

};

/**
 * TPipedTransport. This transport allows piping of a request from one
 * transport to another either when readEnd() or writeEnd(). The typical
 * use case for this is to log a request or a reply to disk.
 * The underlying buffer expands to a keep a copy of the entire
 * request/response.
 *
 * @author Aditya Agarwal <aditya@facebook.com>
 */
class TPipedTransport : virtual public TTransport {
 public:
  TPipedTransport(boost::shared_ptr<TTransport> srcTrans,
                  boost::shared_ptr<TTransport> dstTrans) :
    srcTrans_(srcTrans),
    dstTrans_(dstTrans),
    rBufSize_(512), rPos_(0), rLen_(0),
    wBufSize_(512), wLen_(0) {

    // default is to to pipe the request when readEnd() is called
    pipeOnRead_ = true;
    pipeOnWrite_ = false;

    rBuf_ = (uint8_t*) malloc(sizeof(uint8_t) * rBufSize_);
    wBuf_ = (uint8_t*) malloc(sizeof(uint8_t) * wBufSize_);
  }

  TPipedTransport(boost::shared_ptr<TTransport> srcTrans,
                  boost::shared_ptr<TTransport> dstTrans,
                  uint32_t sz) :
    srcTrans_(srcTrans),
    dstTrans_(dstTrans),
    rBufSize_(512), rPos_(0), rLen_(0),
    wBufSize_(sz), wLen_(0) {

    rBuf_ = (uint8_t*) malloc(sizeof(uint8_t) * rBufSize_);
    wBuf_ = (uint8_t*) malloc(sizeof(uint8_t) * wBufSize_);
  }

  ~TPipedTransport() {
    free(rBuf_);
    free(wBuf_);
  }

  bool isOpen() {
    return srcTrans_->isOpen();
  }

  bool peek() {
    if (rPos_ >= rLen_) {
      // Double the size of the underlying buffer if it is full
      if (rLen_ == rBufSize_) {
        rBufSize_ *=2;
        rBuf_ = (uint8_t *)realloc(rBuf_, sizeof(uint8_t) * rBufSize_);
      }

      // try to fill up the buffer
      rLen_ += srcTrans_->read(rBuf_+rPos_, rBufSize_ - rPos_);
    }
    return (rLen_ > rPos_);
  }


  void open() {
    srcTrans_->open();
  }

  void close() {
    srcTrans_->close();
  }

  void setPipeOnRead(bool pipeVal) {
    pipeOnRead_ = pipeVal;
  }

  void setPipeOnWrite(bool pipeVal) {
    pipeOnWrite_ = pipeVal;
  }

  uint32_t read(uint8_t* buf, uint32_t len);

  void readEnd() {

    if (pipeOnRead_) {
      dstTrans_->write(rBuf_, rLen_);
      dstTrans_->flush();
    }

    srcTrans_->readEnd();

    // reset state
    rLen_ = 0;
    rPos_ = 0;
  }

  void write(const uint8_t* buf, uint32_t len);

  void writeEnd() {
    if (pipeOnWrite_) {
      dstTrans_->write(wBuf_, wLen_);
      dstTrans_->flush();
    }
  }

  void flush();

  boost::shared_ptr<TTransport> getTargetTransport() {
    return dstTrans_;
  }

 protected:
  boost::shared_ptr<TTransport> srcTrans_;
  boost::shared_ptr<TTransport> dstTrans_;

  uint8_t* rBuf_;
  uint32_t rBufSize_;
  uint32_t rPos_;
  uint32_t rLen_;

  uint8_t* wBuf_;
  uint32_t wBufSize_;
  uint32_t wLen_;

  bool pipeOnRead_;
  bool pipeOnWrite_;
};


/**
 * Wraps a transport into a pipedTransport instance.
 *
 * @author Aditya Agarwal <aditya@facebook.com>
 */
class TPipedTransportFactory : public TTransportFactory {
 public:
  TPipedTransportFactory() {}
  TPipedTransportFactory(boost::shared_ptr<TTransport> dstTrans) {
    initializeTargetTransport(dstTrans);
  }
  virtual ~TPipedTransportFactory() {}

  /**
   * Wraps the base transport into a piped transport.
   */
  virtual boost::shared_ptr<TTransport> getTransport(boost::shared_ptr<TTransport> srcTrans) {
    return boost::shared_ptr<TTransport>(new TPipedTransport(srcTrans, dstTrans_));
  }

  virtual void initializeTargetTransport(boost::shared_ptr<TTransport> dstTrans) {
    if (dstTrans_.get() == NULL) {
      dstTrans_ = dstTrans;
    } else {
      throw TException("Target transport already initialized");
    }
  }

 protected:
  boost::shared_ptr<TTransport> dstTrans_;
};

/**
 * TPipedFileTransport. This is just like a TTransport, except that
 * it is a templatized class, so that clients who rely on a specific
 * TTransport can still access the original transport.
 *
 * @author James Wang <jwang@facebook.com>
 */
class TPipedFileReaderTransport : public TPipedTransport,
                                  public TFileReaderTransport {
 public:
  TPipedFileReaderTransport(boost::shared_ptr<TFileReaderTransport> srcTrans, boost::shared_ptr<TTransport> dstTrans);

  ~TPipedFileReaderTransport();

  // TTransport functions
  bool isOpen();
  bool peek();
  void open();
  void close();
  uint32_t read(uint8_t* buf, uint32_t len);
  uint32_t readAll(uint8_t* buf, uint32_t len);
  void readEnd();
  void write(const uint8_t* buf, uint32_t len);
  void writeEnd();
  void flush();

  // TFileReaderTransport functions
  int32_t getReadTimeout();
  void setReadTimeout(int32_t readTimeout);
  uint32_t getNumChunks();
  uint32_t getCurChunk();
  void seekToChunk(int32_t chunk);
  void seekToEnd();

 protected:
  // shouldn't be used
  TPipedFileReaderTransport();
  boost::shared_ptr<TFileReaderTransport> srcTrans_;
};

/**
 * Creates a TPipedFileReaderTransport from a filepath and a destination transport
 *
 * @author James Wang <jwang@facebook.com>
 */
class TPipedFileReaderTransportFactory : public TPipedTransportFactory {
 public:
  TPipedFileReaderTransportFactory() {}
  TPipedFileReaderTransportFactory(boost::shared_ptr<TTransport> dstTrans)
    : TPipedTransportFactory(dstTrans)
  {}
  virtual ~TPipedFileReaderTransportFactory() {}

  boost::shared_ptr<TTransport> getTransport(boost::shared_ptr<TTransport> srcTrans) {
    boost::shared_ptr<TFileReaderTransport> pFileReaderTransport = boost::dynamic_pointer_cast<TFileReaderTransport>(srcTrans);
    if (pFileReaderTransport.get() != NULL) {
      return getFileReaderTransport(pFileReaderTransport);
    } else {
      return boost::shared_ptr<TTransport>();
    }
  }

  boost::shared_ptr<TFileReaderTransport> getFileReaderTransport(boost::shared_ptr<TFileReaderTransport> srcTrans) {
    return boost::shared_ptr<TFileReaderTransport>(new TPipedFileReaderTransport(srcTrans, dstTrans_));
  }
};

}}} // facebook::thrift::transport

#endif // #ifndef _THRIFT_TRANSPORT_TTRANSPORTUTILS_H_
