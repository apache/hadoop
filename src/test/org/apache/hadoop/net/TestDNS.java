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

import junit.framework.TestCase;

import javax.naming.NamingException;
import java.net.UnknownHostException;
import java.net.InetAddress;
import java.net.Inet6Address;
import java.net.Inet4Address;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.Enumeration;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * These tests print things out. This is not normally approved of in testing,
 * where it is the job of the test cases to determine success/failure. The
 * printing of values is to help diagnose root causes of any failures.
 */
public class TestDNS extends TestCase {

  private static final Log LOG = LogFactory.getLog(TestDNS.class);

  private static final String DEFAULT = "default";

  /**
   * Constructs a test case with the given name.
   *
   * @param name test name
   */
  public TestDNS(String name) {
    super(name);
  }

  public void testGetLocalHost() throws Exception {
    String hostname = DNS.getDefaultHost(DEFAULT);
    assertNotNull(hostname);
  }

  public void testGetLocalHostIsFast() throws Exception {
    long t0 = System.currentTimeMillis();
    String hostname = DNS.getDefaultHost(DEFAULT);
    assertNotNull(hostname);
    String hostname2 = DNS.getDefaultHost(DEFAULT);
    long t2 = System.currentTimeMillis();
    String hostname3 = DNS.getDefaultHost(DEFAULT);
    long t3 = System.currentTimeMillis();
    assertEquals(hostname3, hostname2);
    assertEquals(hostname2, hostname);
    long interval2 = t3 - t2;
    assertTrue(
            "It is taking to long to determine the local host -caching is not working",
            interval2 < 20000);
  }

  public void testLocalHostHasAnAddress() throws Exception {
    InetAddress addr = getLocalIPAddr();
    assertNotNull(addr);
    LOG.info("Default address: " + addr.getHostAddress());
    if(addr.isLoopbackAddress()) {
      LOG.warn("This is a loopback address, and not accessible remotely");
    }
    if (addr.isLinkLocalAddress()) {
      LOG.warn("This is a link local address, "
              + "and not accessible beyond the local network");
    }

    String canonical = addr.getCanonicalHostName();
    LOG.info("Canonical Hostname of default address: " + canonical);
    if(canonical.indexOf('.')<0) {
      LOG.warn("The canonical hostname is not fully qualified: "+canonical);
    }
  }

  private InetAddress getLocalIPAddr() throws UnknownHostException {
    String hostname = DNS.getDefaultHost(DEFAULT);
    InetAddress localhost = InetAddress.getByName(hostname);
    return localhost;
  }

  public void testLocalHostHasAName() throws Throwable {
    String hostname = DNS.getDefaultHost(DEFAULT);
    assertNotNull(hostname);
    LOG.info("Default hostname: " + hostname);
  }

  public void testLocalHostHasAtLeastOneName() throws Throwable {
    String hostname = DNS.getDefaultHost(DEFAULT);
    InetAddress[] inetAddresses = InetAddress.getAllByName(hostname);
    int count = 0;
    for (InetAddress address : inetAddresses) {
      LOG.info("Address " + count + ": " + address);
      count++;
    }
    assertTrue("No addresses enumerated", count > 0);

  }

  public void testNullInterface() throws Exception {
    try {
      String host = DNS.getDefaultHost(null);
      fail("Expected a NullPointerException, got " + host);
    } catch (NullPointerException e) {
      //this is expected
    }
  }

  public void testIPsOfUnknownInterface() throws Exception {
    String[] ips = DNS.getIPs("name-of-an-unknown-interface");
    assertNotNull(ips);
    assertTrue(ips.length > 0);
  }

  public void testRDNS() throws Exception {
    InetAddress localhost = getLocalIPAddr();
    try {
      String s = DNS.reverseDns(localhost, null);
      LOG.info("Local RDNS hostname is " + s);
    } catch (NamingException e) {
      LOG.info("Unable to determine hostname of " + localhost
              + " through Reverse DNS", e);
    }
  }

  /**
   * Here for diagnostics; if this is seen to fail
   *
   * @throws Exception for any problems
   */
  public void testLocalhostResolves() throws Exception {
    InetAddress localhost = InetAddress.getByName("localhost");
    assertNotNull("localhost is null", localhost);
    LOG.info("Localhost IPAddr is " + localhost.toString());
  }

  public NetworkInterface getFirstInterface() throws SocketException {
    Enumeration<NetworkInterface> interfaces = NetworkInterface
            .getNetworkInterfaces();
    if (interfaces.hasMoreElements()) {
      return interfaces.nextElement();
    } else {
      return null;
    }
  }

  public void testGetDefaultHostFromFirstInterface() throws Throwable {
    String hostname = DNS.getDefaultHost(getFirstInterface().toString(), DEFAULT);
    assertNotNull("hostname is null", hostname);
  }

  public void testGetDefaultHostFromAllInterfaces() throws Throwable {
    Enumeration<NetworkInterface> interfaces = NetworkInterface
            .getNetworkInterfaces();
    while(interfaces.hasMoreElements()) {
      NetworkInterface nic = interfaces.nextElement();
      String hostname = DNS
              .getDefaultHost(nic.toString(), DEFAULT);
      assertNotNull("hostname is null", hostname);
    }
  }

  public void testGetDefaultHostFromEth0() throws Throwable {
    String hostname = DNS
            .getDefaultHost(getFirstInterface().toString(), DEFAULT);
    assertNotNull("hostname is null", hostname);
  }

  public void testReverseDNSIPv4() throws Throwable {
    InetAddress ipAddr = InetAddress.getByName("140.211.11.9");
    assertTrue(ipAddr instanceof Inet4Address);
    String hostname = DNS.reverseDns(ipAddr, null);
    assertNotNull(hostname);
  }

  public void testReverseDNSIPv6() throws Throwable {
    InetAddress ipAddr = InetAddress.getByName("fe80::250:56ff:fec0:8");
    assertTrue(ipAddr instanceof Inet6Address);
    try {
      String hostname = DNS.reverseDns(ipAddr, null);
      fail("Expected an exception, got "+hostname);
    } catch (IllegalArgumentException e) {
      //we expect this
    }

  }


}
