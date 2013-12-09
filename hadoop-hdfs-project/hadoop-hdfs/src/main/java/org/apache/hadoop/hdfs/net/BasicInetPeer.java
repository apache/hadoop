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
import java.nio.channels.ReadableByteChannel;

import org.apache.hadoop.net.unix.DomainSocket;

/**
 * Represents a peer that we communicate with by using a basic Socket
 * that has no associated Channel.
 *
 */
class BasicInetPeer implements Peer {
  private final Socket socket;
  private final OutputStream out;
  private final InputStream in;
  private final boolean isLocal;

  public BasicInetPeer(Socket socket) throws IOException {
    this.socket = socket;
    this.out = socket.getOutputStream();
    this.in = socket.getInputStream();
    this.isLocal = socket.getInetAddress().equals(socket.getLocalAddress());
  }

  @Override
  public ReadableByteChannel getInputStreamChannel() {
    /*
     * This Socket has no channel, so there's nothing to return here.
     */
    return null;
  }

  @Override
  public void setReadTimeout(int timeoutMs) throws IOException {
    socket.setSoTimeout(timeoutMs);
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
  public void setWriteTimeout(int timeoutMs) {
   /* 
    * We can't implement write timeouts. :(
    * 
    * Java provides no facility to set a blocking write timeout on a Socket.
    * You can simulate a blocking write with a timeout by using
    * non-blocking I/O.  However, we can't use nio here, because this Socket
    * doesn't have an associated Channel.
    * 
    * See http://bugs.sun.com/bugdatabase/view_bug.do?bug_id=4031100 for
    * more details.
    */
  }

  @Override
  public boolean isClosed() {
    return socket.isClosed();
  }

  @Override
  public void close() throws IOException {
    socket.close();
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
  public String toString() {
    return "BasicInetPeer(" + socket.toString() + ")";
  }

  @Override
  public DomainSocket getDomainSocket() {
    return null;
  }

  @Override
  public boolean hasSecureChannel() {
    return false;
  }
}
