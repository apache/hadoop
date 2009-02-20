// Copyright (c) 2006- Facebook
// Distributed under the Thrift Software License
//
// See accompanying file LICENSE or visit the Thrift site at:
// http://developers.facebook.com/thrift/

#ifndef _THRIFT_SERVER_TSIMPLESERVER_H_
#define _THRIFT_SERVER_TSIMPLESERVER_H_ 1

#include "server/TServer.h"
#include "transport/TServerTransport.h"

namespace facebook { namespace thrift { namespace server {

/**
 * This is the most basic simple server. It is single-threaded and runs a
 * continuous loop of accepting a single connection, processing requests on
 * that connection until it closes, and then repeating. It is a good example
 * of how to extend the TServer interface.
 *
 * @author Mark Slee <mcslee@facebook.com>
 */
class TSimpleServer : public TServer {
 public:
  TSimpleServer(boost::shared_ptr<TProcessor> processor,
                boost::shared_ptr<TServerTransport> serverTransport,
                boost::shared_ptr<TTransportFactory> transportFactory,
                boost::shared_ptr<TProtocolFactory> protocolFactory) :
    TServer(processor, serverTransport, transportFactory, protocolFactory),
    stop_(false) {}

  TSimpleServer(boost::shared_ptr<TProcessor> processor,
                boost::shared_ptr<TServerTransport> serverTransport,
                boost::shared_ptr<TTransportFactory> inputTransportFactory,
                boost::shared_ptr<TTransportFactory> outputTransportFactory,
                boost::shared_ptr<TProtocolFactory> inputProtocolFactory,
                boost::shared_ptr<TProtocolFactory> outputProtocolFactory):
    TServer(processor, serverTransport,
            inputTransportFactory, outputTransportFactory,
            inputProtocolFactory, outputProtocolFactory),
    stop_(false) {}

  ~TSimpleServer() {}

  void serve();

  void stop() {
    stop_ = true;
  }

 protected:
  bool stop_;

};

}}} // facebook::thrift::server

#endif // #ifndef _THRIFT_SERVER_TSIMPLESERVER_H_
