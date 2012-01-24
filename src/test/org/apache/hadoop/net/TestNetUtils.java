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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.net.ConnectException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketException;
import java.net.URI;
import java.net.UnknownHostException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.NetUtilsTestResolver;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

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
    socket.bind(new InetSocketAddress("localhost", 0));
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
      // Some TCP stacks will actually throw their own Invalid argument
      // exception here. This is also OK.
      assertTrue(se.getMessage().contains("Invalid argument"));
    }
  }

  static NetUtilsTestResolver resolver;
  static Configuration config;
  
  @BeforeClass
  public static void setupResolver() {
    resolver = NetUtilsTestResolver.install();
  }
  
  @Before
  public void resetResolver() {
    resolver.reset();
    config = new Configuration();
  }

  // getByExactName
  
  private void verifyGetByExactNameSearch(String host, String ... searches) {
    assertNull(resolver.getByExactName(host));
    assertBetterArrayEquals(searches, resolver.getHostSearches());
  }

  @Test
  public void testResolverGetByExactNameUnqualified() {
    verifyGetByExactNameSearch("unknown", "unknown.");
  }

  @Test
  public void testResolverGetByExactNameUnqualifiedWithDomain() {
    verifyGetByExactNameSearch("unknown.domain", "unknown.domain.");
  }

  @Test
  public void testResolverGetByExactNameQualified() {
    verifyGetByExactNameSearch("unknown.", "unknown.");
  }
  
  @Test
  public void testResolverGetByExactNameQualifiedWithDomain() {
    verifyGetByExactNameSearch("unknown.domain.", "unknown.domain.");
  }

  // getByNameWithSearch
  
  private void verifyGetByNameWithSearch(String host, String ... searches) {
    assertNull(resolver.getByNameWithSearch(host));
    assertBetterArrayEquals(searches, resolver.getHostSearches());
  }

  @Test
  public void testResolverGetByNameWithSearchUnqualified() {
    String host = "unknown";
    verifyGetByNameWithSearch(host, host+".a.b.", host+".b.", host+".c.");
  }

  @Test
  public void testResolverGetByNameWithSearchUnqualifiedWithDomain() {
    String host = "unknown.domain";
    verifyGetByNameWithSearch(host, host+".a.b.", host+".b.", host+".c.");
  }

  @Test
  public void testResolverGetByNameWithSearchQualified() {
    String host = "unknown.";
    verifyGetByNameWithSearch(host, host);
  }

  @Test
  public void testResolverGetByNameWithSearchQualifiedWithDomain() {
    String host = "unknown.domain.";
    verifyGetByNameWithSearch(host, host);
  }

  // getByName

  private void verifyGetByName(String host, String ... searches) {
    InetAddress addr = null;
    try {
      addr = resolver.getByName(host);
    } catch (UnknownHostException e) {} // ignore
    assertNull(addr);
    assertBetterArrayEquals(searches, resolver.getHostSearches());
  }
  
  @Test
  public void testResolverGetByNameQualified() {
    String host = "unknown.";
    verifyGetByName(host, host);
  }

  @Test
  public void testResolverGetByNameQualifiedWithDomain() {
    verifyGetByName("unknown.domain.", "unknown.domain.");
  }

  @Test
  public void testResolverGetByNameUnqualified() {
    String host = "unknown";
    verifyGetByName(host, host+".a.b.", host+".b.", host+".c.", host+".");
  }

  @Test
  public void testResolverGetByNameUnqualifiedWithDomain() {
    String host = "unknown.domain";
    verifyGetByName(host, host+".", host+".a.b.", host+".b.", host+".c.");
  }
  
  // resolving of hosts

  private InetAddress verifyResolve(String host, String ... searches) {
    InetAddress addr = null;
    try {
      addr = resolver.getByName(host);
    } catch (UnknownHostException e) {} // ignore
    assertNotNull(addr);
    assertBetterArrayEquals(searches, resolver.getHostSearches());
    return addr;
  }

  private void
  verifyInetAddress(InetAddress addr, String host, String ip) {
    assertNotNull(addr);
    assertEquals(host, addr.getHostName());
    assertEquals(ip, addr.getHostAddress());
  }
  
  @Test
  public void testResolverUnqualified() {
    String host = "host";
    InetAddress addr = verifyResolve(host, host+".a.b.");
    verifyInetAddress(addr, "host.a.b", "1.1.1.1");
  }

  @Test
  public void testResolverUnqualifiedWithDomain() {
    String host = "host.a";
    InetAddress addr = verifyResolve(host, host+".", host+".a.b.", host+".b.");
    verifyInetAddress(addr, "host.a.b", "1.1.1.1");
  }

  @Test
  public void testResolverUnqualifedFull() {
    String host = "host.a.b";
    InetAddress addr = verifyResolve(host, host+".");
    verifyInetAddress(addr, host, "1.1.1.1");
  }
  
  @Test
  public void testResolverQualifed() {
    String host = "host.a.b.";
    InetAddress addr = verifyResolve(host, host);
    verifyInetAddress(addr, host, "1.1.1.1");
  }
  
  // localhost
  
  @Test
  public void testResolverLoopback() {
    String host = "Localhost";
    InetAddress addr = verifyResolve(host); // no lookup should occur
    verifyInetAddress(addr, "Localhost", "127.0.0.1");
  }

  @Test
  public void testResolverIP() {
    String host = "1.1.1.1";
    InetAddress addr = verifyResolve(host); // no lookup should occur for ips
    verifyInetAddress(addr, host, host);
  }

  //
    
  @Test
  public void testCanonicalUriWithPort() {
    URI uri;

    uri = NetUtils.getCanonicalUri(URI.create("scheme://host:123"), 456);
    assertEquals("scheme://host.a.b:123", uri.toString());

    uri = NetUtils.getCanonicalUri(URI.create("scheme://host:123/"), 456);
    assertEquals("scheme://host.a.b:123/", uri.toString());

    uri = NetUtils.getCanonicalUri(URI.create("scheme://host:123/path"), 456);
    assertEquals("scheme://host.a.b:123/path", uri.toString());

    uri = NetUtils.getCanonicalUri(URI.create("scheme://host:123/path?q#frag"), 456);
    assertEquals("scheme://host.a.b:123/path?q#frag", uri.toString());
  }

  @Test
  public void testCanonicalUriWithDefaultPort() {
    URI uri;
    
    uri = NetUtils.getCanonicalUri(URI.create("scheme://host"), 123);
    assertEquals("scheme://host.a.b:123", uri.toString());

    uri = NetUtils.getCanonicalUri(URI.create("scheme://host/"), 123);
    assertEquals("scheme://host.a.b:123/", uri.toString());

    uri = NetUtils.getCanonicalUri(URI.create("scheme://host/path"), 123);
    assertEquals("scheme://host.a.b:123/path", uri.toString());

    uri = NetUtils.getCanonicalUri(URI.create("scheme://host/path?q#frag"), 123);
    assertEquals("scheme://host.a.b:123/path?q#frag", uri.toString());
  }

  @Test
  public void testCanonicalUriWithPath() {
    URI uri;

    uri = NetUtils.getCanonicalUri(URI.create("path"), 2);
    assertEquals("path", uri.toString());

    uri = NetUtils.getCanonicalUri(URI.create("/path"), 2);
    assertEquals("/path", uri.toString());
  }

  @Test
  public void testCanonicalUriWithNoAuthority() {
    URI uri;

    uri = NetUtils.getCanonicalUri(URI.create("scheme:/"), 2);
    assertEquals("scheme:/", uri.toString());

    uri = NetUtils.getCanonicalUri(URI.create("scheme:/path"), 2);
    assertEquals("scheme:/path", uri.toString());

    uri = NetUtils.getCanonicalUri(URI.create("scheme:///"), 2);
    assertEquals("scheme:///", uri.toString());

    uri = NetUtils.getCanonicalUri(URI.create("scheme:///path"), 2);
    assertEquals("scheme:///path", uri.toString());
  }

  @Test
  public void testCanonicalUriWithNoHost() {
    URI uri = NetUtils.getCanonicalUri(URI.create("scheme://:123/path"), 2);
    assertEquals("scheme://:123/path", uri.toString());
  }

  @Test
  public void testCanonicalUriWithNoPortNoDefaultPort() {
    URI uri = NetUtils.getCanonicalUri(URI.create("scheme://host/path"), -1);
    assertEquals("scheme://host.a.b/path", uri.toString());
  }
  
  private <T> void assertBetterArrayEquals(T[] expect, T[]got) {
    String expectStr = StringUtils.join(expect, ", ");
    String gotStr = StringUtils.join(got, ", ");
    assertEquals(expectStr, gotStr);
  }
}