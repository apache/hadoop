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

import java.io.Closeable;
import org.apache.hadoop.classification.InterfaceAudience;
import java.io.IOException;
import java.net.SocketTimeoutException;

@InterfaceAudience.Private
public interface PeerServer extends Closeable {
  /**
   * Set the receive buffer size of the PeerServer.
   * 
   * @param size     The receive buffer size.
   */
  public void setReceiveBufferSize(int size) throws IOException;

  /**
   * Listens for a connection to be made to this server and accepts 
   * it. The method blocks until a connection is made.
   *
   * @exception IOException  if an I/O error occurs when waiting for a
   *               connection.
   * @exception SecurityException  if a security manager exists and its  
   *             <code>checkAccept</code> method doesn't allow the operation.
   * @exception SocketTimeoutException if a timeout was previously set and
   *             the timeout has been reached.
   */
  public Peer accept() throws IOException, SocketTimeoutException;

  /**
   * @return                 A string representation of the address we're
   *                         listening on.
   */
  public String getListeningString();

  /**
   * Free the resources associated with this peer server.
   * This normally includes sockets, etc.
   *
   * @throws IOException     If there is an error closing the PeerServer
   */
  public void close() throws IOException;
}
