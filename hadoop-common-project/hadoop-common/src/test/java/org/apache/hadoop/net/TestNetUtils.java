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
package org.apache.hadoop.net;

import org.junit.Test;

import static org.junit.Assert.*;

import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.Socket;
import java.net.ConnectException;
import java.net.SocketException;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.Enumeration;

import org.apache.hadoop.conf.Configuration;

public class TestNetUtils {

  /**
   * Test that we can't accidentally connect back to the connecting socket due
   * to a quirk in the TCP spec.
   *
   * This is a regression test for HADOOP-6722.
   */
  @Test
  public void testAvoidLoopbackTcpSockets() throws Exception {
    Configuration conf = new Configuration();

    Socket socket = NetUtils.getDefaultSocketFactory(conf)
      .createSocket();
    socket.bind(new InetSocketAddress("127.0.0.1", 0));
    System.err.println("local address: " + socket.getLocalAddress());
    System.err.println("local port: " + socket.getLocalPort());
    try {
      NetUtils.connect(socket,
        new InetSocketAddress(socket.getLocalAddress(), socket.getLocalPort()),
        20000);
      socket.close();
      fail("Should not have connected");
    } catch (ConnectException ce) {
      System.err.println("Got exception: " + ce);
      assertTrue(ce.getMessage().contains("resulted in a loopback"));
    } catch (SocketException se) {
      // Some TCP stacks will actually throw their own Invalid argument exception
      // here. This is also OK.
      assertTrue(se.getMessage().contains("Invalid argument"));
    }
  }
  
  /**
   * Test for {
   * @throws UnknownHostException @link NetUtils#getLocalInetAddress(String)
   * @throws SocketException 
   */
  @Test
  public void testGetLocalInetAddress() throws Exception {
    assertNotNull(NetUtils.getLocalInetAddress("127.0.0.1"));
    assertNull(NetUtils.getLocalInetAddress("invalid-address-for-test"));
    assertNull(NetUtils.getLocalInetAddress(null));
  }

  @Test(expected=UnknownHostException.class)
  public void testVerifyHostnamesException() throws UnknownHostException {
    String[] names = {"valid.host.com", "1.com", "invalid host here"};
    NetUtils.verifyHostnames(names);
  }  

  @Test
  public void testVerifyHostnamesNoException() {
    String[] names = {"valid.host.com", "1.com"};
    try {
      NetUtils.verifyHostnames(names);
    } catch (UnknownHostException e) {
      fail("NetUtils.verifyHostnames threw unexpected UnknownHostException");
    }
  }
  
  /** 
   * Test for {@link NetUtils#isLocalAddress(java.net.InetAddress)}
   */
  @Test
  public void testIsLocalAddress() throws Exception {
    // Test - local host is local address
    assertTrue(NetUtils.isLocalAddress(InetAddress.getLocalHost()));
    
    // Test - all addresses bound network interface is local address
    Enumeration<NetworkInterface> interfaces = NetworkInterface
        .getNetworkInterfaces();
    if (interfaces != null) { // Iterate through all network interfaces
      while (interfaces.hasMoreElements()) {
        NetworkInterface i = interfaces.nextElement();
        Enumeration<InetAddress> addrs = i.getInetAddresses();
        if (addrs == null) {
          continue;
        }
        // Iterate through all the addresses of a network interface
        while (addrs.hasMoreElements()) {
          InetAddress addr = addrs.nextElement();
          assertTrue(NetUtils.isLocalAddress(addr));
        }
      }
    }
    assertFalse(NetUtils.isLocalAddress(InetAddress.getByName("8.8.8.8")));
  }
}
