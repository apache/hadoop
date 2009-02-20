// Copyright (c) 2006- Facebook
// Distributed under the Thrift Software License
//
// See accompanying file LICENSE or visit the Thrift site at:
// http://developers.facebook.com/thrift/

#ifndef _THRIFT_SERVER_TSERVER_H_
#define _THRIFT_SERVER_TSERVER_H_ 1

#include <TProcessor.h>
#include <transport/TServerTransport.h>
#include <protocol/TBinaryProtocol.h>
#include <concurrency/Thread.h>

#include <boost/shared_ptr.hpp>

namespace facebook { namespace thrift { namespace server {

using facebook::thrift::TProcessor;
using facebook::thrift::protocol::TBinaryProtocolFactory;
using facebook::thrift::protocol::TProtocol;
using facebook::thrift::protocol::TProtocolFactory;
using facebook::thrift::transport::TServerTransport;
using facebook::thrift::transport::TTransport;
using facebook::thrift::transport::TTransportFactory;

/**
 * Virtual interface class that can handle events from the server core. To
 * use this you should subclass it and implement the methods that you care
 * about. Your subclass can also store local data that you may care about,
 * such as additional "arguments" to these methods (stored in the object
 * instance's state).
 */
class TServerEventHandler {
 public:

  virtual ~TServerEventHandler() {}

  /**
   * Called before the server begins.
   */
  virtual void preServe() {}

  /**
   * Called when a new client has connected and is about to being processing.
   */
  virtual void clientBegin(boost::shared_ptr<TProtocol> input,
                           boost::shared_ptr<TProtocol> output) {}

  /**
   * Called when a client has finished making requests.
   */
  virtual void clientEnd(boost::shared_ptr<TProtocol> input,
                         boost::shared_ptr<TProtocol> output) {}

 protected:

  /**
   * Prevent direct instantiation.
   */
  TServerEventHandler() {}

};

/**
 * Thrift server.
 *
 * @author Mark Slee <mcslee@facebook.com>
 */
class TServer : public concurrency::Runnable {
 public:

  virtual ~TServer() {}

  virtual void serve() = 0;

  virtual void stop() {}

  // Allows running the server as a Runnable thread
  virtual void run() {
    serve();
  }

  boost::shared_ptr<TProcessor> getProcessor() {
    return processor_;
  }

  boost::shared_ptr<TServerTransport> getServerTransport() {
    return serverTransport_;
  }

  boost::shared_ptr<TTransportFactory> getInputTransportFactory() {
    return inputTransportFactory_;
  }

  boost::shared_ptr<TTransportFactory> getOutputTransportFactory() {
    return outputTransportFactory_;
  }

  boost::shared_ptr<TProtocolFactory> getInputProtocolFactory() {
    return inputProtocolFactory_;
  }

  boost::shared_ptr<TProtocolFactory> getOutputProtocolFactory() {
    return outputProtocolFactory_;
  }

  boost::shared_ptr<TServerEventHandler> getEventHandler() {
    return eventHandler_;
  }

protected:
  TServer(boost::shared_ptr<TProcessor> processor):
    processor_(processor) {
    setInputTransportFactory(boost::shared_ptr<TTransportFactory>(new TTransportFactory()));
    setOutputTransportFactory(boost::shared_ptr<TTransportFactory>(new TTransportFactory()));
    setInputProtocolFactory(boost::shared_ptr<TProtocolFactory>(new TBinaryProtocolFactory()));
    setOutputProtocolFactory(boost::shared_ptr<TProtocolFactory>(new TBinaryProtocolFactory()));
  }

  TServer(boost::shared_ptr<TProcessor> processor,
          boost::shared_ptr<TServerTransport> serverTransport):
    processor_(processor),
    serverTransport_(serverTransport) {
    setInputTransportFactory(boost::shared_ptr<TTransportFactory>(new TTransportFactory()));
    setOutputTransportFactory(boost::shared_ptr<TTransportFactory>(new TTransportFactory()));
    setInputProtocolFactory(boost::shared_ptr<TProtocolFactory>(new TBinaryProtocolFactory()));
    setOutputProtocolFactory(boost::shared_ptr<TProtocolFactory>(new TBinaryProtocolFactory()));
  }

  TServer(boost::shared_ptr<TProcessor> processor,
          boost::shared_ptr<TServerTransport> serverTransport,
          boost::shared_ptr<TTransportFactory> transportFactory,
          boost::shared_ptr<TProtocolFactory> protocolFactory):
    processor_(processor),
    serverTransport_(serverTransport),
    inputTransportFactory_(transportFactory),
    outputTransportFactory_(transportFactory),
    inputProtocolFactory_(protocolFactory),
    outputProtocolFactory_(protocolFactory) {}

  TServer(boost::shared_ptr<TProcessor> processor,
          boost::shared_ptr<TServerTransport> serverTransport,
          boost::shared_ptr<TTransportFactory> inputTransportFactory,
          boost::shared_ptr<TTransportFactory> outputTransportFactory,
          boost::shared_ptr<TProtocolFactory> inputProtocolFactory,
          boost::shared_ptr<TProtocolFactory> outputProtocolFactory):
    processor_(processor),
    serverTransport_(serverTransport),
    inputTransportFactory_(inputTransportFactory),
    outputTransportFactory_(outputTransportFactory),
    inputProtocolFactory_(inputProtocolFactory),
    outputProtocolFactory_(outputProtocolFactory) {}


  // Class variables
  boost::shared_ptr<TProcessor> processor_;
  boost::shared_ptr<TServerTransport> serverTransport_;

  boost::shared_ptr<TTransportFactory> inputTransportFactory_;
  boost::shared_ptr<TTransportFactory> outputTransportFactory_;

  boost::shared_ptr<TProtocolFactory> inputProtocolFactory_;
  boost::shared_ptr<TProtocolFactory> outputProtocolFactory_;

  boost::shared_ptr<TServerEventHandler> eventHandler_;

public:
  void setInputTransportFactory(boost::shared_ptr<TTransportFactory> inputTransportFactory) {
    inputTransportFactory_ = inputTransportFactory;
  }

  void setOutputTransportFactory(boost::shared_ptr<TTransportFactory> outputTransportFactory) {
    outputTransportFactory_ = outputTransportFactory;
  }

  void setInputProtocolFactory(boost::shared_ptr<TProtocolFactory> inputProtocolFactory) {
    inputProtocolFactory_ = inputProtocolFactory;
  }

  void setOutputProtocolFactory(boost::shared_ptr<TProtocolFactory> outputProtocolFactory) {
    outputProtocolFactory_ = outputProtocolFactory;
  }

  void setServerEventHandler(boost::shared_ptr<TServerEventHandler> eventHandler) {
    eventHandler_ = eventHandler;
  }

};

}}} // facebook::thrift::server

#endif // #ifndef _THRIFT_SERVER_TSERVER_H_
