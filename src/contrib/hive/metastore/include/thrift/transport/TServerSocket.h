// Copyright (c) 2006- Facebook
// Distributed under the Thrift Software License
//
// See accompanying file LICENSE or visit the Thrift site at:
// http://developers.facebook.com/thrift/

#ifndef _THRIFT_TRANSPORT_TSERVERSOCKET_H_
#define _THRIFT_TRANSPORT_TSERVERSOCKET_H_ 1

#include "TServerTransport.h"
#include <boost/shared_ptr.hpp>

namespace facebook { namespace thrift { namespace transport { 

class TSocket;

/**
 * Server socket implementation of TServerTransport. Wrapper around a unix
 * socket listen and accept calls.
 *
 * @author Mark Slee <mcslee@facebook.com>
 */
class TServerSocket : public TServerTransport {
 public:
  TServerSocket(int port);
  TServerSocket(int port, int sendTimeout, int recvTimeout);

  ~TServerSocket();

  void setSendTimeout(int sendTimeout);
  void setRecvTimeout(int recvTimeout);

  void setRetryLimit(int retryLimit);
  void setRetryDelay(int retryDelay);

  void listen();
  void close();

  void interrupt();

 protected:
  boost::shared_ptr<TTransport> acceptImpl();

 private:
  int port_;
  int serverSocket_;
  int acceptBacklog_;
  int sendTimeout_;
  int recvTimeout_;
  int retryLimit_;
  int retryDelay_;

  int intSock1_;
  int intSock2_;
};

}}} // facebook::thrift::transport

#endif // #ifndef _THRIFT_TRANSPORT_TSERVERSOCKET_H_
