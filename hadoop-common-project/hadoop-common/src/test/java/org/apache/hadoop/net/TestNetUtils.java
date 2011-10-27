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

import junit.framework.AssertionFailedError;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Test;

import static org.junit.Assert.*;

import java.io.IOException;
import java.net.BindException;
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

  private static final Log LOG = LogFactory.getLog(TestNetUtils.class);
  private static final int DEST_PORT = 4040;
  private static final String DEST_PORT_NAME = Integer.toString(DEST_PORT);
  private static final int LOCAL_PORT = 8080;
  private static final String LOCAL_PORT_NAME = Integer.toString(LOCAL_PORT);

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

  @Test
  public void testWrapConnectException() throws Throwable {
    IOException e = new ConnectException("failed");
    IOException wrapped = verifyExceptionClass(e, ConnectException.class);
    assertInException(wrapped, "failed");
    assertWikified(wrapped);
    assertInException(wrapped, "localhost");
    assertRemoteDetailsIncluded(wrapped);
    assertInException(wrapped, "/ConnectionRefused");
  }

  @Test
  public void testWrapBindException() throws Throwable {
    IOException e = new BindException("failed");
    IOException wrapped = verifyExceptionClass(e, BindException.class);
    assertInException(wrapped, "failed");
    assertLocalDetailsIncluded(wrapped);
    assertNotInException(wrapped, DEST_PORT_NAME);
    assertInException(wrapped, "/BindException");
  }

  @Test
  public void testWrapUnknownHostException() throws Throwable {
    IOException e = new UnknownHostException("failed");
    IOException wrapped = verifyExceptionClass(e, UnknownHostException.class);
    assertInException(wrapped, "failed");
    assertWikified(wrapped);
    assertInException(wrapped, "localhost");
    assertRemoteDetailsIncluded(wrapped);
    assertInException(wrapped, "/UnknownHost");
  }
  
  @Test
  public void testCreateSocketAddress() throws Throwable {
    InetSocketAddress addr = NetUtils.createSocketAddr(
        "127.0.0.1:12345", 1000, "myconfig");
    assertEquals("127.0.0.1", addr.getAddress().getHostAddress());
    assertEquals(12345, addr.getPort());
    
    addr = NetUtils.createSocketAddr(
        "127.0.0.1", 1000, "myconfig");
    assertEquals("127.0.0.1", addr.getAddress().getHostAddress());
    assertEquals(1000, addr.getPort());

    try {
      addr = NetUtils.createSocketAddr(
          "127.0.0.1:blahblah", 1000, "myconfig");
      fail("Should have failed to parse bad port");
    } catch (IllegalArgumentException iae) {
      assertInException(iae, "myconfig");
    }
  }

  private void assertRemoteDetailsIncluded(IOException wrapped)
      throws Throwable {
    assertInException(wrapped, "desthost");
    assertInException(wrapped, DEST_PORT_NAME);
  }

  private void assertLocalDetailsIncluded(IOException wrapped)
      throws Throwable {
    assertInException(wrapped, "localhost");
    assertInException(wrapped, LOCAL_PORT_NAME);
  }

  private void assertWikified(Exception e) throws Throwable {
    assertInException(e, NetUtils.HADOOP_WIKI);
  }

  private void assertInException(Exception e, String text) throws Throwable {
    String message = extractExceptionMessage(e);
    if (!(message.contains(text))) {
      throw new AssertionFailedError("Wrong text in message "
                                         + "\"" + message + "\""
                                         + " expected \"" + text + "\"")
          .initCause(e);
    }
  }

  private String extractExceptionMessage(Exception e) throws Throwable {
    assertNotNull("Null Exception", e);
    String message = e.getMessage();
    if (message == null) {
      throw new AssertionFailedError("Empty text in exception " + e)
          .initCause(e);
    }
    return message;
  }

  private void assertNotInException(Exception e, String text)
      throws Throwable{
    String message = extractExceptionMessage(e);
    if (message.contains(text)) {
      throw new AssertionFailedError("Wrong text in message "
                                         + "\"" + message + "\""
                                         + " did not expect \"" + text + "\"")
          .initCause(e);
    }
  }

  private IOException verifyExceptionClass(IOException e,
                                           Class expectedClass)
      throws Throwable {
    assertNotNull("Null Exception", e);
    IOException wrapped =
        NetUtils.wrapException("desthost", DEST_PORT,
                               "localhost", LOCAL_PORT,
                               e);
    LOG.info(wrapped.toString(), wrapped);
    if(!(wrapped.getClass().equals(expectedClass))) {
      throw new AssertionFailedError("Wrong exception class; expected "
                                         + expectedClass
                                         + " got " + wrapped.getClass() + ": " + wrapped).initCause(wrapped);
    }
    return wrapped;
  }
}
