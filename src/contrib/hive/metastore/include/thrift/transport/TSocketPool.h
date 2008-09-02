// Copyright (c) 2006- Facebook
// Distributed under the Thrift Software License
//
// See accompanying file LICENSE or visit the Thrift site at:
// http://developers.facebook.com/thrift/

#ifndef _THRIFT_TRANSPORT_TSOCKETPOOL_H_
#define _THRIFT_TRANSPORT_TSOCKETPOOL_H_ 1

#include <vector>
#include "TSocket.h"

namespace facebook { namespace thrift { namespace transport { 

/**
 * TCP Socket implementation of the TTransport interface.
 *
 * @author Mark Slee <mcslee@facebook.com>
 */
class TSocketPool : public TSocket {

 public:
   /**
    * Socket pool constructor
    *
    * @param hosts list of host names
    * @param ports list of port names
    */
   TSocketPool(const std::vector<std::string> &hosts,
               const std::vector<int> &ports);

   /**
    * Socket pool constructor
    *
    * @param servers list of pairs of host name and port
    */
   TSocketPool(const std::vector<std::pair<std::string, int> > servers);

   /**
    * Socket pool constructor
    *
    * @param host single host
    * @param port single port
    */
   TSocketPool(const std::string& host, int port);

   /**
    * Destroyes the socket object, closing it if necessary.
    */
   virtual ~TSocketPool();

   /**
    * Add a server to the pool
    */
   void addServer(const std::string& host, int port);

   /**
    * Sets how many times to keep retrying a host in the connect function.
    */
   void setNumRetries(int numRetries);

   /**
    * Sets how long to wait until retrying a host if it was marked down
    */
   void setRetryInterval(int retryInterval);

   /**
    * Sets how many times to keep retrying a host before marking it as down.
    */
   void setMaxConsecutiveFailures(int maxConsecutiveFailures);

   /**
    * Turns randomization in connect order on or off.
    */
   void setRandomize(bool randomize);

   /**
    * Whether to always try the last server.
    */
   void setAlwaysTryLast(bool alwaysTryLast);

   /**
    * Creates and opens the UNIX socket.
    */
   void open();

 protected:

   /** List of servers to connect to */
   std::vector<std::pair<std::string, int> > servers_;

   /** How many times to retry each host in connect */
   int numRetries_;

   /** Retry interval in seconds, how long to not try a host if it has been
    * marked as down.
    */
   int retryInterval_;

   /** Max consecutive failures before marking a host down. */
   int maxConsecutiveFailures_;

   /** Try hosts in order? or Randomized? */
   bool randomize_;

   /** Always try last host, even if marked down? */
   bool alwaysTryLast_;
};

}}} // facebook::thrift::transport

#endif // #ifndef _THRIFT_TRANSPORT_TSOCKETPOOL_H_

