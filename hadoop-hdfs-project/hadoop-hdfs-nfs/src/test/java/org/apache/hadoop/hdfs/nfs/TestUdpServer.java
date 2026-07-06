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

package org.apache.hadoop.hdfs.nfs;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.UnknownHostException;

import org.apache.hadoop.nfs.nfs3.Nfs3Constant;
import org.apache.hadoop.oncrpc.RpcCall;
import org.apache.hadoop.oncrpc.XDR;
import org.apache.hadoop.oncrpc.security.CredentialsNone;
import org.apache.hadoop.oncrpc.security.VerifierNone;

// TODO: convert this to Junit
public class TestUdpServer {
  static void testRequest(XDR request, XDR request2) {
    try {
      DatagramSocket clientSocket = new DatagramSocket();
      InetAddress IPAddress = InetAddress.getByName("localhost");
      byte[] sendData = request.getBytes();
      byte[] receiveData = new byte[65535];

      DatagramPacket sendPacket = new DatagramPacket(sendData, sendData.length,
          IPAddress, Nfs3Constant.SUN_RPCBIND);
      clientSocket.send(sendPacket);
      DatagramPacket receivePacket = new DatagramPacket(receiveData,
          receiveData.length);
      clientSocket.receive(receivePacket);
      clientSocket.close();

    } catch (UnknownHostException e) {
      System.err.println("Don't know about host: localhost.");
      System.exit(1);
    } catch (IOException e) {
      System.err.println("Couldn't get I/O for "
          + "the connection to: localhost.");
      System.exit(1);
    }
  }
 
  public static void main(String[] args) throws InterruptedException {
    Thread t1 = new Runtest1();
    // TODO: cleanup
    //Thread t2 = new Runtest2();
    t1.start();
    //t2.start();
    t1.join();
    //t2.join();
    //testDump();
  }
  
  static class Runtest1 extends Thread {
    @Override
    public void run() {
      testGetportMount();
    }
  }
  
  static class Runtest2 extends Thread {
    @Override
    public void run() {
      testDump();
    }
  }
  
  static void createPortmapXDRheader(XDR xdr_out, int procedure) {
    // Make this a method
    RpcCall.getInstance(0, 100000, 2, procedure, new CredentialsNone(),
        new VerifierNone()).write(xdr_out);
  }
 
  static void testGetportMount() {
    XDR xdr_out = new XDR();
    createPortmapXDRheader(xdr_out, 3);
    xdr_out.writeInt(100005);
    xdr_out.writeInt(1);
    xdr_out.writeInt(6);
    xdr_out.writeInt(0);

    XDR request2 = new XDR();
    createPortmapXDRheader(xdr_out, 3);
    request2.writeInt(100005);
    request2.writeInt(1);
    request2.writeInt(6);
    request2.writeInt(0);

    testRequest(xdr_out, request2);
  }
  
  static void testGetport() {
    XDR xdr_out = new XDR();

    createPortmapXDRheader(xdr_out, 3);

    xdr_out.writeInt(100003);
    xdr_out.writeInt(3);
    xdr_out.writeInt(6);
    xdr_out.writeInt(0);

    XDR request2 = new XDR();

    createPortmapXDRheader(xdr_out, 3);
    request2.writeInt(100003);
    request2.writeInt(3);
    request2.writeInt(6);
    request2.writeInt(0);

    testRequest(xdr_out, request2);
  }
  
  static void testDump() {
    XDR xdr_out = new XDR();
    createPortmapXDRheader(xdr_out, 4);
    testRequest(xdr_out, xdr_out);
  }
}