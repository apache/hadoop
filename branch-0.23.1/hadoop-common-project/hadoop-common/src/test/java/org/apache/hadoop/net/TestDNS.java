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

import java.net.UnknownHostException;
import java.net.InetAddress;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import javax.naming.NameNotFoundException;

/**
 *
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

  /**
   * Test that asking for the default hostname works
   * @throws Exception if hostname lookups fail   */
  public void testGetLocalHost() throws Exception {
    String hostname = DNS.getDefaultHost(DEFAULT);
    assertNotNull(hostname);
  }

  /**
   * Test that repeated calls to getting the local host are fairly fast, and
   * hence that caching is being used
   * @throws Exception if hostname lookups fail
   */
  public void testGetLocalHostIsFast() throws Exception {
    String hostname = DNS.getDefaultHost(DEFAULT);
    assertNotNull(hostname);
    long t1 = System.currentTimeMillis();
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

  /**
   * Test that our local IP address is not null
   * @throws Exception if something went wrong
   */
  public void testLocalHostHasAnAddress() throws Exception {
    assertNotNull(getLocalIPAddr());
  }

  private InetAddress getLocalIPAddr() throws UnknownHostException {
    String hostname = DNS.getDefaultHost(DEFAULT);
    InetAddress localhost = InetAddress.getByName(hostname);
    return localhost;
  }

  /**
   * Test that passing a null pointer is as the interface
   * fails with a NullPointerException
   * @throws Exception if something went wrong
   */
  public void testNullInterface() throws Exception {
    try {
      String host = DNS.getDefaultHost(null);
      fail("Expected a NullPointerException, got " + host);
    } catch (NullPointerException expected) {
      //this is expected
    }
  }

  /**
   * Get the IP addresses of an unknown interface, expect to get something
   * back
   * @throws Exception if something went wrong
   */
  public void testIPsOfUnknownInterface() throws Exception {
    String[] ips = DNS.getIPs("name-of-an-unknown-interface");
    assertNotNull(ips);
    assertTrue(ips.length > 0);
  }

  /**
   * TestCase: get our local address and reverse look it up
   * @throws Exception if that fails
   */
  public void testRDNS() throws Exception {
    InetAddress localhost = getLocalIPAddr();
    try {
      String s = DNS.reverseDns(localhost, null);
      LOG.info("Local revers DNS hostname is " + s);
    } catch (NameNotFoundException e) {
      if (!localhost.isLinkLocalAddress() || localhost.isLoopbackAddress()) {
        //these addresses probably won't work with rDNS anyway, unless someone
        //has unusual entries in their DNS server mapping 1.0.0.127 to localhost
        LOG.info("Reverse DNS failing as due to incomplete networking", e);
        LOG.info("Address is " + localhost
                + " Loopback=" + localhost.isLoopbackAddress()
                + " Linklocal=" + localhost.isLinkLocalAddress());
      }

    }
  }

  /**
   * Test that the name "localhost" resolves to something.
   *
   * If this fails, your machine's network is in a mess, go edit /etc/hosts
   * @throws Exception for any problems
   */
  public void testLocalhostResolves() throws Exception {
    InetAddress localhost = InetAddress.getByName("localhost");
    assertNotNull("localhost is null", localhost);
    LOG.info("Localhost IPAddr is " + localhost.toString());
  }
}
