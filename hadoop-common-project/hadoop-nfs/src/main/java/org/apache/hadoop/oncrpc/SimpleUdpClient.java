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
package org.apache.hadoop.oncrpc;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.util.Arrays;

/**
 * A simple UDP based RPC client which just sends one request to a server.
 */
public class SimpleUdpClient {
  
  protected final String host;
  protected final int port;
  protected final XDR request;
  protected final boolean oneShot;
  protected final DatagramSocket clientSocket;
  private int udpTimeoutMillis;

  public SimpleUdpClient(String host, int port, XDR request,
      DatagramSocket clientSocket) {
    this(host, port, request, true, clientSocket, 500);
  }

  public SimpleUdpClient(String host, int port, XDR request, Boolean oneShot,
      DatagramSocket clientSocket) {
    this(host, port, request, oneShot, clientSocket, 500);
  }

  public SimpleUdpClient(String host, int port, XDR request, Boolean oneShot,
                         DatagramSocket clientSocket, int udpTimeoutMillis) {
    this.host = host;
    this.port = port;
    this.request = request;
    this.oneShot = oneShot;
    this.clientSocket = clientSocket;
    this.udpTimeoutMillis = udpTimeoutMillis;
  }

  public void run() throws IOException {
    InetAddress IPAddress = InetAddress.getByName(host);
    byte[] sendData = request.getBytes();
    byte[] receiveData = new byte[65535];
    // Use the provided socket if there is one, else just make a new one.
    DatagramSocket socket = this.clientSocket == null ?
        new DatagramSocket() : this.clientSocket;

    try {
      DatagramPacket sendPacket = new DatagramPacket(sendData, sendData.length,
          IPAddress, port);
      socket.send(sendPacket);
      socket.setSoTimeout(udpTimeoutMillis);
      DatagramPacket receivePacket = new DatagramPacket(receiveData,
          receiveData.length);
      socket.receive(receivePacket);
  
      // Check reply status
      XDR xdr = new XDR(Arrays.copyOfRange(receiveData, 0,
          receivePacket.getLength()));
      RpcReply reply = RpcReply.read(xdr);
      if (reply.getState() != RpcReply.ReplyState.MSG_ACCEPTED) {
        throw new IOException("Request failed: " + reply.getState());
      }
    } finally {
      // If the client socket was passed in to this UDP client, it's on the
      // caller of this UDP client to close that socket.
      if (this.clientSocket == null) {
        socket.close();
      }
    }
  }
}
