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

import java.net.NetworkInterface;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.net.InetAddress;

import javax.naming.CommunicationException;
import javax.naming.NameNotFoundException;
import javax.naming.ServiceUnavailableException;

import org.apache.hadoop.util.Time;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hadoop.test.PlatformAssumptions.assumeNotWindows;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Test host name and IP resolution and caching.
 */
public class TestDNS {

  private static final Logger LOG = LoggerFactory.getLogger(TestDNS.class);
  private static final String DEFAULT = "default";

  // This is not a legal hostname (starts with a hyphen). It will never
  // be returned on any test machine.
  private static final String DUMMY_HOSTNAME = "-DUMMY_HOSTNAME";
  private static final String INVALID_DNS_SERVER = "0.0.0.0";

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
    assertTrue(interval < 20000,
        "Took too long to determine local host - caching is not working");
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
    String host = DNS.getDefaultHost(null);  // should work.
    assertThat(host).isEqualTo(DNS.getDefaultHost(DEFAULT));
    try {
      String ip = DNS.getDefaultIP(null);
      fail("Expected a NullPointerException, got " + ip);
    } catch (NullPointerException npe) {
      // Expected
    }
  }

  /**
   * Test that 'null' DNS server gives the same result as if no DNS
   * server was passed.
   */
  @Test
  public void testNullDnsServer() throws Exception {
    String host = DNS.getDefaultHost(getLoopbackInterface(), null);
    assertThat(host)
        .isEqualTo(DNS.getDefaultHost(getLoopbackInterface()));
  }

  /**
   * Test that "default" DNS server gives the same result as if no DNS
   * server was passed.
   */
  @Test
  public void testDefaultDnsServer() throws Exception {
    String host = DNS.getDefaultHost(getLoopbackInterface(), DEFAULT);
    assertThat(host)
        .isEqualTo(DNS.getDefaultHost(getLoopbackInterface()));
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
    assertEquals(1, ips.length, "Should only return 1 default IP");
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
      LOG.info("Local reverse DNS hostname is " + s);
    } catch (NameNotFoundException | CommunicationException | ServiceUnavailableException e) {
      if (!localhost.isLinkLocalAddress() || localhost.isLoopbackAddress()) {
        //these addresses probably won't work with rDNS anyway, unless someone
        //has unusual entries in their DNS server mapping 1.0.0.127 to localhost
        LOG.info("Reverse DNS failing as due to incomplete networking", e);
        LOG.info("Address is " + localhost
                + " Loopback=" + localhost.isLoopbackAddress()
                + " Linklocal=" + localhost.isLinkLocalAddress());
      }
      assumeTrue(false, e.getMessage());
    }
  }

  /**
   * Test that when using an invalid DNS server with hosts file fallback,
   * we are able to get the hostname from the hosts file.
   *
   * This test may fail on some misconfigured test machines that don't have
   * an entry for "localhost" in their hosts file. This entry is correctly
   * configured out of the box on common Linux distributions and OS X.
   *
   * Windows refuses to resolve 127.0.0.1 to "localhost" despite the presence of
   * this entry in the hosts file.  We skip the test on Windows to avoid
   * reporting a spurious failure.
   *
   * @throws Exception
   */
  @Test
  @Timeout(value = 60)
  public void testLookupWithHostsFallback() throws Exception {
    assumeNotWindows();
    final String oldHostname = DNS.getCachedHostname();
    try {
      DNS.setCachedHostname(DUMMY_HOSTNAME);
      String hostname = DNS.getDefaultHost(
          getLoopbackInterface(), INVALID_DNS_SERVER, true);

      // Expect to get back something other than the cached host name.
      assertThat(hostname).isNotEqualTo(DUMMY_HOSTNAME);
    } finally {
      // Restore DNS#cachedHostname for subsequent tests.
      DNS.setCachedHostname(oldHostname);
    }
  }

  /**
   * Test that when using an invalid DNS server without hosts file
   * fallback, we get back the cached host name.
   *
   * @throws Exception
   */
  @Test
  @Timeout(value = 60)
  public void testLookupWithoutHostsFallback() throws Exception {
    final String oldHostname = DNS.getCachedHostname();
    try {
      DNS.setCachedHostname(DUMMY_HOSTNAME);
      String hostname = DNS.getDefaultHost(
          getLoopbackInterface(), INVALID_DNS_SERVER, false);

      // Expect to get back the cached host name since there was no hosts
      // file lookup.
      assertThat(hostname).isEqualTo(DUMMY_HOSTNAME);
    } finally {
      // Restore DNS#cachedHostname for subsequent tests.
      DNS.setCachedHostname(oldHostname);
    }
  }

  private String getLoopbackInterface() throws SocketException {
    return NetworkInterface.getByInetAddress(
        InetAddress.getLoopbackAddress()).getName();
  }

  /**
   * Test that the name "localhost" resolves to something.
   *
   * If this fails, your machine's network is in a mess, go edit /etc/hosts
   */
  @Test
  public void testLocalhostResolves() throws Exception {
    InetAddress localhost = InetAddress.getByName("localhost");
    assertNotNull(localhost, "localhost is null");
    LOG.info("Localhost IPAddr is " + localhost.toString());
  }
}
