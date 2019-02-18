/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdfs.net;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.channels.ReadableByteChannel;

import org.apache.hadoop.net.SocketInputStream;
import org.apache.hadoop.net.SocketOutputStream;
import org.apache.hadoop.net.unix.DomainSocket;
import org.apache.http.client.utils.URIBuilder;

/**
 * Represents a peer that we communicate with by using non-blocking I/O
 * on a Socket.
 */
public class NioInetPeer implements Peer {
  private final Socket socket;

  /**
   * An InputStream which simulates blocking I/O with timeouts using NIO.
   */
  private final SocketInputStream in;

  /**
   * An OutputStream which simulates blocking I/O with timeouts using NIO.
   */
  private final SocketOutputStream out;

  private final boolean isLocal;

  private final URI localURI;
  private final URI remoteURI;

  public NioInetPeer(Socket socket) throws IOException {
    this.socket = socket;
    this.in = new SocketInputStream(socket.getChannel(), 0);
    this.out = new SocketOutputStream(socket.getChannel(), 0);
    this.isLocal = socket.getInetAddress().equals(socket.getLocalAddress());
    this.localURI = getURI(socket.getLocalAddress().getHostAddress(),
        socket.getLocalPort());
    this.remoteURI =
        getURI(socket.getInetAddress().getHostAddress(), socket.getPort());
  }

  @Override
  public ReadableByteChannel getInputStreamChannel() {
    return in;
  }

  @Override
  public void setReadTimeout(int timeoutMs) throws IOException {
    in.setTimeout(timeoutMs);
  }

  @Override
  public int getReceiveBufferSize() throws IOException {
    return socket.getReceiveBufferSize();
  }

  @Override
  public boolean getTcpNoDelay() throws IOException {
    return socket.getTcpNoDelay();
  }

  @Override
  public void setWriteTimeout(int timeoutMs) throws IOException {
    out.setTimeout(timeoutMs);
  }

  @Override
  public boolean isClosed() {
    return socket.isClosed();
  }

  @Override
  public void close() throws IOException {
    // We always close the outermost streams-- in this case, 'in' and 'out'
    // Closing either one of these will also close the Socket.
    try {
      in.close();
    } finally {
      out.close();
    }
  }

  @Override
  public String getRemoteAddressString() {
    return socket.getRemoteSocketAddress().toString();
  }

  @Override
  public String getLocalAddressString() {
    return socket.getLocalSocketAddress().toString();
  }

  @Override
  public URI getRemoteURI() {
    return this.remoteURI;
  }

  @Override
  public URI getLocalURI() {
    return this.localURI;
  }

  @Override
  public InputStream getInputStream() throws IOException {
    return in;
  }

  @Override
  public OutputStream getOutputStream() throws IOException {
    return out;
  }

  @Override
  public boolean isLocal() {
    return isLocal;
  }

  @Override
  public DomainSocket getDomainSocket() {
    return null;
  }

  @Override
  public boolean hasSecureChannel() {
    return false;
  }

  @Override
  public String toString() {
    return "NioInetPeer [isLocal=" + isLocal + ", localURI=" + localURI
        + ", remoteURI=" + remoteURI + "]";
  }

  /**
   * Given a host name and port, create a DN URI. Turn checked exception into
   * runtime. Exception should never happen because the inputs are captures from
   * an exiting socket and not parsed from an external source.
   *
   * @param host Host for URI
   * @param port Port for URI
   * @return A URI
   */
  private URI getURI(final String host, final int port) {
    try {
      return new URIBuilder().setScheme("hdfs+dn")
      .setHost(host)
      .setPort(port).build();
    } catch (URISyntaxException e) {
      throw new IllegalArgumentException(e);
    }
  }
}
