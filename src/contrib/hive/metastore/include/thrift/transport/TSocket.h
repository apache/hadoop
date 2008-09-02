// Copyright (c) 2006- Facebook
// Distributed under the Thrift Software License
//
// See accompanying file LICENSE or visit the Thrift site at:
// http://developers.facebook.com/thrift/

#ifndef _THRIFT_TRANSPORT_TSOCKET_H_
#define _THRIFT_TRANSPORT_TSOCKET_H_ 1

#include <string>
#include <sys/time.h>

#include "TTransport.h"
#include "TServerSocket.h"

namespace facebook { namespace thrift { namespace transport {

/**
 * TCP Socket implementation of the TTransport interface.
 *
 * @author Mark Slee <mcslee@facebook.com>
 * @author Aditya Agarwal <aditya@facebook.com>
 */
class TSocket : public TTransport {
  /**
   * We allow the TServerSocket acceptImpl() method to access the private
   * members of a socket so that it can access the TSocket(int socket)
   * constructor which creates a socket object from the raw UNIX socket
   * handle.
   */
  friend class TServerSocket;

 public:
  /**
   * Constructs a new socket. Note that this does NOT actually connect the
   * socket.
   *
   */
  TSocket();

  /**
   * Constructs a new socket. Note that this does NOT actually connect the
   * socket.
   *
   * @param host An IP address or hostname to connect to
   * @param port The port to connect on
   */
  TSocket(std::string host, int port);

  /**
   * Destroyes the socket object, closing it if necessary.
   */
  virtual ~TSocket();

  /**
   * Whether the socket is alive.
   *
   * @return Is the socket alive?
   */
  bool isOpen();

  /**
   * Calls select on the socket to see if there is more data available.
   */
  bool peek();

  /**
   * Creates and opens the UNIX socket.
   *
   * @throws TTransportException If the socket could not connect
   */
  virtual void open();

  /**
   * Shuts down communications on the socket.
   */
  void close();

  /**
   * Reads from the underlying socket.
   */
  uint32_t read(uint8_t* buf, uint32_t len);

  /**
   * Writes to the underlying socket.
   */
  void write(const uint8_t* buf, uint32_t len);

  /**
   * Get the host that the socket is connected to
   *
   * @return string host identifier
   */
  std::string getHost();

  /**
   * Get the port that the socket is connected to
   *
   * @return int port number
   */
  int getPort();

  /**
   * Set the host that socket will connect to
   *
   * @param host host identifier
   */
  void setHost(std::string host);

  /**
   * Set the port that socket will connect to
   *
   * @param port port number
   */
  void setPort(int port);

  /**
   * Controls whether the linger option is set on the socket.
   *
   * @param on      Whether SO_LINGER is on
   * @param linger  If linger is active, the number of seconds to linger for
   */
  void setLinger(bool on, int linger);

  /**
   * Whether to enable/disable Nagle's algorithm.
   *
   * @param noDelay Whether or not to disable the algorithm.
   * @return
   */
  void setNoDelay(bool noDelay);

  /**
   * Set the connect timeout
   */
  void setConnTimeout(int ms);

  /**
   * Set the receive timeout
   */
  void setRecvTimeout(int ms);

  /**
   * Set the send timeout
   */
  void setSendTimeout(int ms);

  /**
   * Set the max number of recv retries in case of an EAGAIN
   * error
   */
  void setMaxRecvRetries(int maxRecvRetries);

  /**
   * Get socket information formated as a string <Host: x Port: x>
   */
  std::string getSocketInfo();

  /**
   * Returns the DNS name of the host to which the socket is connected
   */
  std::string getPeerHost();

  /**
   * Returns the address of the host to which the socket is connected
   */
  std::string getPeerAddress();

  /**
   * Returns the port of the host to which the socket is connected
   **/
  int getPeerPort();


 protected:
  /**
   * Constructor to create socket from raw UNIX handle. Never called directly
   * but used by the TServerSocket class.
   */
  TSocket(int socket);

  /** connect, called by open */
  void openConnection(struct addrinfo *res);

  /** Host to connect to */
  std::string host_;

  /** Peer hostname */
  std::string peerHost_;

  /** Peer address */
  std::string peerAddress_;

  /** Peer port */
  int peerPort_;

  /** Port number to connect on */
  int port_;

  /** Underlying UNIX socket handle */
  int socket_;

  /** Connect timeout in ms */
  int connTimeout_;

  /** Send timeout in ms */
  int sendTimeout_;

  /** Recv timeout in ms */
  int recvTimeout_;

  /** Linger on */
  bool lingerOn_;

  /** Linger val */
  int lingerVal_;

  /** Nodelay */
  bool noDelay_;

  /** Recv EGAIN retries */
  int maxRecvRetries_;

  /** Recv timeout timeval */
  struct timeval recvTimeval_;

};

}}} // facebook::thrift::transport

#endif // #ifndef _THRIFT_TRANSPORT_TSOCKET_H_

