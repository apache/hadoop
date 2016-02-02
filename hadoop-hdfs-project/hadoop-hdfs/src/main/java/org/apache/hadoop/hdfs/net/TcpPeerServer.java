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
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.SocketTimeoutException;
import java.nio.channels.ServerSocketChannel;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hdfs.DFSUtilClient;
import org.apache.hadoop.hdfs.server.datanode.SecureDataNodeStarter.SecureResources;
import org.apache.hadoop.ipc.Server;

@InterfaceAudience.Private
public class TcpPeerServer implements PeerServer {
  static final Log LOG = LogFactory.getLog(TcpPeerServer.class);

  private final ServerSocket serverSocket;

  /**
   * Create a non-secure TcpPeerServer.
   *
   * @param socketWriteTimeout    The Socket write timeout in ms.
   * @param bindAddr              The address to bind to.
   * @param backlogLength         The length of the tcp accept backlog
   * @throws IOException
   */
  public TcpPeerServer(int socketWriteTimeout,
                       InetSocketAddress bindAddr,
                       int backlogLength) throws IOException {
    this.serverSocket = (socketWriteTimeout > 0) ?
          ServerSocketChannel.open().socket() : new ServerSocket();
    Server.bind(serverSocket, bindAddr, backlogLength);
  }

  /**
   * Create a secure TcpPeerServer.
   *
   * @param secureResources   Security resources.
   */
  public TcpPeerServer(SecureResources secureResources) {
    this.serverSocket = secureResources.getStreamingSocket();
  }
  
  /**
   * @return     the IP address which this TcpPeerServer is listening on.
   */
  public InetSocketAddress getStreamingAddr() {
    return new InetSocketAddress(
        serverSocket.getInetAddress().getHostAddress(),
        serverSocket.getLocalPort());
  }

  @Override
  public void setReceiveBufferSize(int size) throws IOException {
    this.serverSocket.setReceiveBufferSize(size);
  }

  @Override
  public int getReceiveBufferSize() throws IOException {
    return this.serverSocket.getReceiveBufferSize();
  }

  @Override
  public Peer accept() throws IOException, SocketTimeoutException {
    Peer peer = DFSUtilClient.peerFromSocket(serverSocket.accept());
    return peer;
  }

  @Override
  public String getListeningString() {
    return serverSocket.getLocalSocketAddress().toString();
  }
  
  @Override
  public void close() throws IOException {
    try {
      serverSocket.close();
    } catch(IOException e) {
      LOG.error("error closing TcpPeerServer: ", e);
    }
  }

  @Override
  public String toString() {
    return "TcpPeerServer(" + getListeningString() + ")";
  }
}
