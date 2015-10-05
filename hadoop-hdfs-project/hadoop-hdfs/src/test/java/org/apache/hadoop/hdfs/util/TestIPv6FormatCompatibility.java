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
package org.apache.hadoop.hdfs.util;

import com.google.common.net.InetAddresses;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hdfs.net.Peer;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.protocol.datatransfer.sasl.DataTransferSaslUtil;
import org.apache.hadoop.net.unix.DomainSocket;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.channels.ReadableByteChannel;

import static org.junit.Assert.*;

/**
 * This is a very basic, very fast test to test IPv6 parsing issues as we find them.
 * It does NOT depend on having a working IPv6 stack and should succeed even if run
 * with "-Djava.net.preferIPv4Stack=true"
 */
public class TestIPv6FormatCompatibility {
  private final String IPV6_LOOPBACK_LONG_STRING = "0:0:0:0:0:0:0:1";
  private final String IPV6_SAMPLE_ADDRESS = "2a03:2880:2130:cf05:face:b00c:0:1";
  private final String IPV6_LOOPBACK_SHORT_STRING = "::1";
  private final String IPV4_LOOPBACK_WITH_PORT = "127.0.0.1:10";
  private final String IPV6_LOOPBACK_WITH_PORT = "["+IPV6_LOOPBACK_LONG_STRING+"]:10";
  private final String IPV6_SAMPLE_WITH_PORT = "[" + IPV6_SAMPLE_ADDRESS + "]:10";
  private final InetAddress IPV6LOOPBACK = InetAddresses.forString(IPV6_LOOPBACK_LONG_STRING);
  private final InetAddress IPV4LOOPBACK = Inet4Address.getLoopbackAddress();
  private final InetAddress IPV6SAMPLE = InetAddresses.forString(IPV6_SAMPLE_ADDRESS);
  private final String IPV4_LOOPBACK_STRING = IPV4LOOPBACK.getHostAddress();

  private static final Log LOG = LogFactory.getLog(TestIPv6FormatCompatibility.class);

  // HDFS-8078 : note that we're expecting URI-style (see Javadoc for java.net.URI or rfc2732)
  @Test
  public void testDatanodeIDXferAddressAddsBrackets() {
    DatanodeID ipv4localhost =
        new DatanodeID(IPV4_LOOPBACK_STRING, "localhost", "no-uuid", 10, 20, 30, 40);
    DatanodeID ipv6localhost =
        new DatanodeID(IPV6_LOOPBACK_LONG_STRING, "localhost", "no-uuid", 10, 20, 30, 40);
    DatanodeID ipv6sample =
        new DatanodeID(IPV6_SAMPLE_ADDRESS, "ipv6.example.com", "no-uuid", 10, 20, 30, 40);
    assertEquals("IPv6 should have brackets added",
        IPV6_LOOPBACK_WITH_PORT, ipv6localhost.getXferAddr(false));
    assertEquals("IPv6 should have brackets added",
        IPV6_SAMPLE_WITH_PORT, ipv6sample.getXferAddr(false));
    assertEquals("IPv4 should not have brackets added",
        IPV4_LOOPBACK_WITH_PORT, ipv4localhost.getXferAddr(false));
  }

  // HDFS-8078
  @Test
  public void testDatanodeIDXferAddressShouldNormalizeIPv6() {
    DatanodeID ipv6short =
        new DatanodeID(IPV6_LOOPBACK_SHORT_STRING, "localhost", "no-uuid", 10, 20, 30, 40);
    assertEquals("IPv6 should be normalized and not abbreviated",
        IPV6_LOOPBACK_WITH_PORT, ipv6short.getXferAddr(false));
  }

  // HDFS-8078 : note that in some cases we're parsing the results of java.net.SocketAddress.toString() \
  // which doesn't product the URI-style results, and we're splitting this rather than producing the
  // combined string to be consumed.
  @Test
  public void testGetPeerShouldFindFullIPAddress() {
    Peer ipv6SamplePeer = new MockInetPeer(IPV6SAMPLE, false);
    Peer ipv4loopback = new MockInetPeer(IPV4LOOPBACK, false);
    Peer ipv6loopback = new MockInetPeer(IPV6LOOPBACK, false);
    assertNotNull(DataTransferSaslUtil.getPeerAddress(ipv6SamplePeer));
    assertNotNull(DataTransferSaslUtil.getPeerAddress(ipv6loopback));
    assertNotNull(DataTransferSaslUtil.getPeerAddress(ipv4loopback));
  }

  // HDFS-8078 : It looks like in some cases this could also produce URI-style
  // results, so we test both.
  @Test
  public void testGetPeerAccept() {
    Peer ipv6loopbackAsURI = new MockInetPeer(IPV6LOOPBACK, true);
    assertEquals("getPeer should still with URI-style [bracket]", IPV6_LOOPBACK_LONG_STRING, DataTransferSaslUtil.getPeerAddress(ipv6loopbackAsURI).getHostAddress());
  }

  /**
   * Mocks a Peer purely to test DataTransferSaslUtil,getPeerAddress() which takes a Peer
   * and consumers getRemoteAddressString(). All other functionality missing.
   */
  private class MockInetPeer implements Peer {
    SocketAddress sa;
    boolean asURI;

    public MockInetPeer(InetAddress addr, boolean asURI) {
      sa = new InetSocketAddress(addr, 50010);
      this.asURI = asURI;
    }

    @Override
    public ReadableByteChannel getInputStreamChannel() {
      return null;
    }

    @Override
    public void setReadTimeout(int timeoutMs) throws IOException {
    }

    @Override
    public int getReceiveBufferSize() throws IOException {
      return 0;
    }

    @Override
    public boolean getTcpNoDelay() throws IOException {
      return false;
    }

    @Override
    public void setWriteTimeout(int timeoutMs) throws IOException {

    }

    @Override
    public boolean isClosed() {
      return false;
    }

    @Override
    public void close() throws IOException {
    }

    @Override
    public String getRemoteAddressString() {
      return sa.toString();
    }

    @Override
    public String getLocalAddressString() {
      return null;
    }

    @Override
    public InputStream getInputStream() throws IOException {
      return null;
    }

    @Override
    public OutputStream getOutputStream() throws IOException {
      return null;
    }

    @Override
    public boolean isLocal() {
      return false;
    }

    @Override
    public DomainSocket getDomainSocket() {
      return null;
    }

    @Override
    public boolean hasSecureChannel() {
      return false;
    }
  }
}