package org.apache.hadoop.net;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.UnknownHostException;

import javax.net.SocketFactory;

/**
 * Specialized SocketFactory to create sockets with a SOCKS proxy
 */
public class StandardSocketFactory extends SocketFactory {

  /**
   * Default empty constructor (for use with the reflection API).
   */
  public StandardSocketFactory() {
  }

  /* @inheritDoc */
  @Override
  public Socket createSocket() throws IOException {
    return new Socket();
  }

  /* @inheritDoc */
  @Override
  public Socket createSocket(InetAddress addr, int port) throws IOException {

    Socket socket = createSocket();
    socket.connect(new InetSocketAddress(addr, port));
    return socket;
  }

  /* @inheritDoc */
  @Override
  public Socket createSocket(InetAddress addr, int port,
      InetAddress localHostAddr, int localPort) throws IOException {

    Socket socket = createSocket();
    socket.bind(new InetSocketAddress(localHostAddr, localPort));
    socket.connect(new InetSocketAddress(addr, port));
    return socket;
  }

  /* @inheritDoc */
  @Override
  public Socket createSocket(String host, int port) throws IOException,
      UnknownHostException {

    Socket socket = createSocket();
    socket.connect(new InetSocketAddress(host, port));
    return socket;
  }

  /* @inheritDoc */
  @Override
  public Socket createSocket(String host, int port,
      InetAddress localHostAddr, int localPort) throws IOException,
      UnknownHostException {

    Socket socket = createSocket();
    socket.bind(new InetSocketAddress(localHostAddr, localPort));
    socket.connect(new InetSocketAddress(host, port));
    return socket;
  }

  /* @inheritDoc */
  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (!(obj instanceof StandardSocketFactory))
      return false;
    return true;
  }

  /* @inheritDoc */
  @Override
  public int hashCode() {
    // Dummy hash code (to make find bugs happy)
    return 47;
  } 
  
}
