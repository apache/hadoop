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
import java.nio.channels.ReadableByteChannel;

import org.apache.hadoop.net.unix.DomainSocket;
import org.apache.hadoop.classification.InterfaceAudience;

/**
 * Represents a peer that we communicate with by using blocking I/O
 * on a UNIX domain socket.
 */
@InterfaceAudience.Private
public class DomainPeer implements Peer {
  private final DomainSocket socket;
  private final OutputStream out;
  private final InputStream in;
  private final ReadableByteChannel channel;

  public DomainPeer(DomainSocket socket) {
    this.socket = socket;
    this.out = socket.getOutputStream();
    this.in = socket.getInputStream();
    this.channel = socket.getChannel();
  }

  @Override
  public ReadableByteChannel getInputStreamChannel() {
    return channel;
  }

  @Override
  public void setReadTimeout(int timeoutMs) throws IOException {
    socket.setAttribute(DomainSocket.RECEIVE_TIMEOUT, timeoutMs);
  }

  @Override
  public int getReceiveBufferSize() throws IOException {
    return socket.getAttribute(DomainSocket.RECEIVE_BUFFER_SIZE);
  }

  @Override
  public boolean getTcpNoDelay() throws IOException {
    /* No TCP, no TCP_NODELAY. */
    return false;
  }

  @Override
  public void setWriteTimeout(int timeoutMs) throws IOException {
    socket.setAttribute(DomainSocket.SEND_TIMEOUT, timeoutMs);
  }

  @Override
  public boolean isClosed() {
    return !socket.isOpen();
  }

  @Override
  public void close() throws IOException {
    socket.close();
  }

  @Override
  public String getRemoteAddressString() {
    return "unix:" + socket.getPath();
  }

  @Override
  public String getLocalAddressString() {
    return "<local>";
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
    /* UNIX domain sockets can only be used for local communication. */
    return true;
  }

  @Override
  public String toString() {
    return "DomainPeer(" + getRemoteAddressString() + ")";
  }

  @Override
  public DomainSocket getDomainSocket() {
    return socket;
  }

  @Override
  public boolean hasSecureChannel() {
    //
    // Communication over domain sockets is assumed to be secure, since it
    // doesn't pass over any network.  We also carefully control the privileges
    // that can be used on the domain socket inode and its parent directories.
    // See #{java.org.apache.hadoop.net.unix.DomainSocket#validateSocketPathSecurity0}
    // for details.
    //
    // So unless you are running as root or the hdfs superuser, you cannot
    // launch a man-in-the-middle attach on UNIX domain socket traffic.
    //
    return true;
  }
}
