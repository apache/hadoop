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
import java.net.SocketTimeoutException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.net.unix.DomainSocket;

@InterfaceAudience.Private
public class DomainPeerServer implements PeerServer {
  static final Log LOG = LogFactory.getLog(DomainPeerServer.class);
  private final DomainSocket sock;

  DomainPeerServer(DomainSocket sock) {
    this.sock = sock;
  }

  public DomainPeerServer(String path, int port) 
      throws IOException {
    this(DomainSocket.bindAndListen(DomainSocket.getEffectivePath(path, port)));
  }
  
  public String getBindPath() {
    return sock.getPath();
  }

  @Override
  public void setReceiveBufferSize(int size) throws IOException {
    sock.setAttribute(DomainSocket.RECEIVE_BUFFER_SIZE, size);
  }

  @Override
  public Peer accept() throws IOException, SocketTimeoutException {
    DomainSocket connSock = sock.accept();
    Peer peer = null;
    boolean success = false;
    try {
      peer = new DomainPeer(connSock);
      success = true;
      return peer;
    } finally {
      if (!success) {
        if (peer != null) peer.close();
        connSock.close();
      }
    }
  }

  @Override
  public String getListeningString() {
    return "unix:" + sock.getPath();
  }
  
  @Override
  public void close() throws IOException {
    try {
      sock.close();
    } catch (IOException e) {
      LOG.error("error closing DomainPeerServer: ", e);
    }
  }

  @Override
  public String toString() {
    return "DomainPeerServer(" + getListeningString() + ")";
  }
}
