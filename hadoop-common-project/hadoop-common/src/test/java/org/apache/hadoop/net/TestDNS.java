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

import java.net.UnknownHostException;
import java.net.InetAddress;

import javax.naming.NameNotFoundException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.util.Time;

import org.junit.Test;
import static org.junit.Assert.*;

/**
 * Test host name and IP resolution and caching.
 */
public class TestDNS {

  private static final Log LOG = LogFactory.getLog(TestDNS.class);
  private static final String DEFAULT = "default";

  /**
   * Test that asking for the default hostname works
   * @throws Exception if hostname lookups fail
   */
  @Test
  public void testGetLocalHost() throws Exception {
    String hostname = DNS.getDefaultHost(DEFAULT);
    assertNotNull(hostname);
  }

  /**
   * Test that repeated calls to getting the local host are fairly fast, and
   * hence that caching is being used
   * @throws Exception if hostname lookups fail
   */
  @Test
  public void testGetLocalHostIsFast() throws Exception {
    String hostname1 = DNS.getDefaultHost(DEFAULT);
    assertNotNull(hostname1);
    String hostname2 = DNS.getDefaultHost(DEFAULT);
    long t1 = Time.now();
    String hostname3 = DNS.getDefaultHost(DEFAULT);
    long t2 = Time.now();
    assertEquals(hostname3, hostname2);
    assertEquals(hostname2, hostname1);
    long interval = t2 - t1;
    assertTrue(
        "Took too long to determine local host - caching is not working",
        interval < 20000);
  }

  /**
   * Test that our local IP address is not null
   * @throws Exception if something went wrong
   */
  @Test
  public void testLocalHostHasAnAddress() throws Exception {
    assertNotNull(getLocalIPAddr());
  }

  private InetAddress getLocalIPAddr() throws UnknownHostException {
    String hostname = DNS.getDefaultHost(DEFAULT);
    InetAddress localhost = InetAddress.getByName(hostname);
    return localhost;
  }

  /**
   * Test null interface name
   */
  @Test
  public void testNullInterface() throws Exception {
    try {
      String host = DNS.getDefaultHost(null);
      fail("Expected a NullPointerException, got " + host);
    } catch (NullPointerException npe) {
      // Expected
    }
    try {
      String ip = DNS.getDefaultIP(null);
      fail("Expected a NullPointerException, got " + ip);
    } catch (NullPointerException npe) {
      // Expected
    }
  }

  /**
   * Get the IP addresses of an unknown interface
   */
  @Test
  public void testIPsOfUnknownInterface() throws Exception {
    try {
      DNS.getIPs("name-of-an-unknown-interface");
      fail("Got an IP for a bogus interface");
    } catch (UnknownHostException e) {
      assertEquals("No such interface name-of-an-unknown-interface",
          e.getMessage());
    }
  }

  /**
   * Test the "default" IP addresses is the local IP addr
   */
  @Test
  public void testGetIPWithDefault() throws Exception {
    String[] ips = DNS.getIPs(DEFAULT);
    assertEquals("Should only return 1 default IP", 1, ips.length);
    assertEquals(getLocalIPAddr().getHostAddress(), ips[0].toString());
    String ip = DNS.getDefaultIP(DEFAULT);
    assertEquals(ip, ips[0].toString());
  }

  /**
   * TestCase: get our local address and reverse look it up
   */
  @Test
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
   */
  @Test
  public void testLocalhostResolves() throws Exception {
    InetAddress localhost = InetAddress.getByName("localhost");
    assertNotNull("localhost is null", localhost);
    LOG.info("Localhost IPAddr is " + localhost.toString());
  }
}
