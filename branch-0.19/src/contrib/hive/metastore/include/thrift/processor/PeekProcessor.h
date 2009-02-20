// Copyright (c) 2006- Facebook
// Distributed under the Thrift Software License
//
// See accompanying file LICENSE or visit the Thrift site at:
// http://developers.facebook.com/thrift/

#ifndef PEEKPROCESSOR_H
#define PEEKPROCESSOR_H

#include <string>
#include <TProcessor.h>
#include <transport/TTransport.h>
#include <transport/TTransportUtils.h>
#include <boost/shared_ptr.hpp>

namespace facebook { namespace thrift { namespace processor { 

/*
 * Class for peeking at the raw data that is being processed by another processor
 * and gives the derived class a chance to change behavior accordingly
 *
 * @author James Wang <jwang@facebook.com>
 */
class PeekProcessor : public facebook::thrift::TProcessor {
  
 public:
  PeekProcessor();
  virtual ~PeekProcessor();

  // Input here: actualProcessor  - the underlying processor
  //             protocolFactory  - the protocol factory used to wrap the memory buffer
  //             transportFactory - this TPipedTransportFactory is used to wrap the source transport
  //                                via a call to getPipedTransport
  void initialize(boost::shared_ptr<facebook::thrift::TProcessor> actualProcessor,
                  boost::shared_ptr<facebook::thrift::protocol::TProtocolFactory> protocolFactory,
                  boost::shared_ptr<facebook::thrift::transport::TPipedTransportFactory> transportFactory);

  boost::shared_ptr<facebook::thrift::transport::TTransport> getPipedTransport(boost::shared_ptr<facebook::thrift::transport::TTransport> in);

  void setTargetTransport(boost::shared_ptr<facebook::thrift::transport::TTransport> targetTransport);

  virtual bool process(boost::shared_ptr<facebook::thrift::protocol::TProtocol> in, 
                       boost::shared_ptr<facebook::thrift::protocol::TProtocol> out);

  // The following three functions can be overloaded by child classes to
  // achieve desired peeking behavior
  virtual void peekName(const std::string& fname);
  virtual void peekBuffer(uint8_t* buffer, uint32_t size);
  virtual void peek(boost::shared_ptr<facebook::thrift::protocol::TProtocol> in, 
                    facebook::thrift::protocol::TType ftype,
                    int16_t fid);
  virtual void peekEnd();

 private:
  boost::shared_ptr<facebook::thrift::TProcessor> actualProcessor_;
  boost::shared_ptr<facebook::thrift::protocol::TProtocol> pipedProtocol_;
  boost::shared_ptr<facebook::thrift::transport::TPipedTransportFactory> transportFactory_;
  boost::shared_ptr<facebook::thrift::transport::TMemoryBuffer> memoryBuffer_;
  boost::shared_ptr<facebook::thrift::transport::TTransport> targetTransport_;
};

}}} // facebook::thrift::processor

#endif
