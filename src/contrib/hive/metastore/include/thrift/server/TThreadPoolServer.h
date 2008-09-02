// Copyright (c) 2006- Facebook
// Distributed under the Thrift Software License
//
// See accompanying file LICENSE or visit the Thrift site at:
// http://developers.facebook.com/thrift/

#ifndef _THRIFT_SERVER_TTHREADPOOLSERVER_H_
#define _THRIFT_SERVER_TTHREADPOOLSERVER_H_ 1

#include <concurrency/ThreadManager.h>
#include <server/TServer.h>
#include <transport/TServerTransport.h>

#include <boost/shared_ptr.hpp>

namespace facebook { namespace thrift { namespace server {

using facebook::thrift::concurrency::ThreadManager;
using facebook::thrift::protocol::TProtocolFactory;
using facebook::thrift::transport::TServerTransport;
using facebook::thrift::transport::TTransportFactory;

class TThreadPoolServer : public TServer {
 public:
  class Task;

  TThreadPoolServer(boost::shared_ptr<TProcessor> processor,
                    boost::shared_ptr<TServerTransport> serverTransport,
                    boost::shared_ptr<TTransportFactory> transportFactory,
                    boost::shared_ptr<TProtocolFactory> protocolFactory,
                    boost::shared_ptr<ThreadManager> threadManager);

  TThreadPoolServer(boost::shared_ptr<TProcessor> processor,
                    boost::shared_ptr<TServerTransport> serverTransport,
                    boost::shared_ptr<TTransportFactory> inputTransportFactory,
                    boost::shared_ptr<TTransportFactory> outputTransportFactory,
                    boost::shared_ptr<TProtocolFactory> inputProtocolFactory,
                    boost::shared_ptr<TProtocolFactory> outputProtocolFactory,
                    boost::shared_ptr<ThreadManager> threadManager);

  virtual ~TThreadPoolServer();

  virtual void serve();

  virtual int64_t getTimeout() const;

  virtual void setTimeout(int64_t value);

  virtual void stop() {
    stop_ = true;
    serverTransport_->interrupt();
  }

 protected:

  boost::shared_ptr<ThreadManager> threadManager_;

  volatile bool stop_;

  volatile int64_t timeout_;

};

}}} // facebook::thrift::server

#endif // #ifndef _THRIFT_SERVER_TTHREADPOOLSERVER_H_
