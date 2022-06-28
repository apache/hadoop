/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.client;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.retry.FailoverProxyProvider;
import org.apache.hadoop.yarn.api.ApplicationClientProtocol;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.resourcemanager.HATestUtil;
import org.junit.Before;
import org.junit.Test;

import java.io.Closeable;
import java.io.IOException;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;
import java.net.InetSocketAddress;
import org.mockito.ArgumentMatcher;

import static org.apache.hadoop.yarn.conf.YarnConfiguration.RM_ADDRESS;
import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link ConfiguredRMFailoverProxyProvider} and
 * {@link AutoRefreshRMFailoverProxyProvider}.
 */
public class TestRMFailoverProxyProvider {

  // Default port of yarn RM
  private static final int RM1_PORT = 8032;
  private static final int RM2_PORT = 8031;
  private static final int RM3_PORT = 8033;

  private Configuration conf;

  private class TestProxy extends Proxy implements Closeable {
    protected TestProxy(InvocationHandler h) {
      super(h);
    }

    @Override
    public void close() throws IOException {
    }
  }

  @Before
  public void setUp() throws IOException, YarnException {
    conf = new YarnConfiguration();
    conf.setClass(YarnConfiguration.CLIENT_FAILOVER_NO_HA_PROXY_PROVIDER,
        ConfiguredRMFailoverProxyProvider.class, RMFailoverProxyProvider.class);
    conf.setBoolean(YarnConfiguration.RM_HA_ENABLED, true);
  }

  /**
   * Test that the {@link ConfiguredRMFailoverProxyProvider}
   * will loop through its set of proxies when
   * and {@link ConfiguredRMFailoverProxyProvider#performFailover(Object)}
   * gets called.
   */
  @Test
  public void testFailoverChange() throws Exception {
    //Adjusting the YARN Conf
    conf.set(YarnConfiguration.RM_HA_IDS, "rm0, rm1");

    // Create two proxies and mock a RMProxy
    Proxy mockProxy2 = new TestProxy((proxy, method, args) -> null);
    Proxy mockProxy1 = new TestProxy((proxy, method, args) -> null);

    Class protocol = ApplicationClientProtocol.class;
    RMProxy mockRMProxy = mock(RMProxy.class);
    ConfiguredRMFailoverProxyProvider<RMProxy> fpp =
        new ConfiguredRMFailoverProxyProvider<RMProxy>();

    // generate two address with different ports.
    // Default port of yarn RM
    InetSocketAddress mockAdd1 = new InetSocketAddress(RM1_PORT);
    InetSocketAddress mockAdd2 = new InetSocketAddress(RM2_PORT);

    // Mock RMProxy methods
    when(mockRMProxy.getRMAddress(any(YarnConfiguration.class),
        any(Class.class))).thenReturn(mockAdd1);
    when(mockRMProxy.getProxy(any(YarnConfiguration.class),
        any(Class.class), eq(mockAdd1))).thenReturn(mockProxy1);

    // Initialize failover proxy provider and get proxy from it.
    fpp.init(conf, mockRMProxy, protocol);
    FailoverProxyProvider.ProxyInfo<RMProxy> actualProxy1 = fpp.getProxy();
    assertEquals(
        "ConfiguredRMFailoverProxyProvider doesn't generate " +
        "expected proxy",
        mockProxy1, actualProxy1.proxy);

    // Invoke fpp.getProxy() multiple times and
    // validate the returned proxy is always mockProxy1
    actualProxy1 = fpp.getProxy();
    assertEquals(
        "ConfiguredRMFailoverProxyProvider doesn't generate " +
        "expected proxy",
        mockProxy1, actualProxy1.proxy);
    actualProxy1 = fpp.getProxy();
    assertEquals(
        "ConfiguredRMFailoverProxyProvider doesn't generate " +
        "expected proxy",
        mockProxy1, actualProxy1.proxy);

    // verify that mockRMProxy.getProxy() is invoked once only.
    verify(mockRMProxy, times(1))
        .getProxy(any(YarnConfiguration.class), any(Class.class),
        eq(mockAdd1));

    // Mock RMProxy methods to generate different proxy
    // based on different IP address.
    when(mockRMProxy.getRMAddress(
        any(YarnConfiguration.class),
        any(Class.class))).thenReturn(mockAdd2);
    when(mockRMProxy.getProxy(
        any(YarnConfiguration.class),
        any(Class.class), eq(mockAdd2))).thenReturn(mockProxy2);

    // Perform Failover and get proxy again from failover proxy provider
    fpp.performFailover(actualProxy1.proxy);
    FailoverProxyProvider.ProxyInfo <RMProxy> actualProxy2 = fpp.getProxy();
    assertEquals("ConfiguredRMFailoverProxyProvider " +
        "doesn't generate expected proxy after failover",
        mockProxy2, actualProxy2.proxy);

    // check the proxy is different with the one we created before.
    assertNotEquals("ConfiguredRMFailoverProxyProvider " +
        "shouldn't generate same proxy after failover",
        actualProxy1.proxy, actualProxy2.proxy);

    // verify that mockRMProxy.getProxy() has been one with each address
    verify(mockRMProxy, times(1))
        .getProxy(any(YarnConfiguration.class), any(Class.class),
        eq(mockAdd1));
    verify(mockRMProxy, times(1))
        .getProxy(any(YarnConfiguration.class), any(Class.class),
        eq(mockAdd2));

    // Mock RMProxy methods to generate the first proxy again
    when(mockRMProxy.getRMAddress(
        any(YarnConfiguration.class),
        any(Class.class))).thenReturn(mockAdd1);
    when(mockRMProxy.getProxy(
        any(YarnConfiguration.class),
        any(Class.class), eq(mockAdd1))).thenReturn(mockProxy1);

    // Perform Failover and get proxy again from failover proxy provider
    fpp.performFailover(actualProxy2.proxy);
    FailoverProxyProvider.ProxyInfo <RMProxy> actualProxy3 = fpp.getProxy();

    // check the proxy is the same as the one we created before.
    assertEquals("ConfiguredRMFailoverProxyProvider " +
        "doesn't generate expected proxy after failover",
        mockProxy1, actualProxy3.proxy);

    // verify that mockRMProxy.getProxy() has still only been invoked twice
    verify(mockRMProxy, times(1))
        .getProxy(any(YarnConfiguration.class), any(Class.class),
        eq(mockAdd1));
    verify(mockRMProxy, times(1))
        .getProxy(any(YarnConfiguration.class), any(Class.class),
        eq(mockAdd2));
  }

  /**
   * Test that the {@link AutoRefreshRMFailoverProxyProvider}
   * will loop through its set of proxies when
   * and {@link AutoRefreshRMFailoverProxyProvider#performFailover(Object)}
   * gets called.
   */
  @Test
  public void testAutoRefreshFailoverChange() throws Exception {
    conf.setClass(YarnConfiguration.CLIENT_FAILOVER_NO_HA_PROXY_PROVIDER,
        AutoRefreshRMFailoverProxyProvider.class,
        RMFailoverProxyProvider.class);

    //Adjusting the YARN Conf
    conf.setBoolean(YarnConfiguration.RM_HA_ENABLED, true);
    conf.set(YarnConfiguration.RM_HA_IDS, "rm0, rm1");

    // Create three proxies and mock a RMProxy
    Proxy mockProxy1 = new TestProxy((proxy, method, args) -> null);
    Proxy mockProxy2 = new TestProxy((proxy, method, args) -> null);
    Proxy mockProxy3 = new TestProxy((proxy, method, args) -> null);
    Class protocol = ApplicationClientProtocol.class;
    RMProxy mockRMProxy = mock(RMProxy.class);
    AutoRefreshRMFailoverProxyProvider<RMProxy> fpp =
        new AutoRefreshRMFailoverProxyProvider<RMProxy>();

    // generate three address with different ports.
    InetSocketAddress mockAdd1 = new InetSocketAddress(RM1_PORT);
    InetSocketAddress mockAdd2 = new InetSocketAddress(RM2_PORT);
    InetSocketAddress mockAdd3 = new InetSocketAddress(RM3_PORT);


    // Mock RMProxy methods
    when(mockRMProxy.getRMAddress(any(YarnConfiguration.class),
        any(Class.class))).thenReturn(mockAdd1);
    when(mockRMProxy.getProxy(any(YarnConfiguration.class),
        any(Class.class), eq(mockAdd1))).thenReturn(mockProxy1);

    // Initialize failover proxy provider and get proxy from it.
    fpp.init(conf, mockRMProxy, protocol);
    FailoverProxyProvider.ProxyInfo <RMProxy> actualProxy1 = fpp.getProxy();
    assertEquals(
        "AutoRefreshRMFailoverProxyProvider doesn't generate " +
        "expected proxy",
        mockProxy1, actualProxy1.proxy);

    // Invoke fpp.getProxy() multiple times and
    // validate the returned proxy is always mockProxy1
    actualProxy1 = fpp.getProxy();
    assertEquals(
        "AutoRefreshRMFailoverProxyProvider doesn't generate " +
        "expected proxy",
        mockProxy1, actualProxy1.proxy);
    actualProxy1 = fpp.getProxy();
    assertEquals(
        "AutoRefreshRMFailoverProxyProvider doesn't generate " +
        "expected proxy",
        mockProxy1, actualProxy1.proxy);

    // verify that mockRMProxy.getProxy() is invoked once only.
    verify(mockRMProxy, times(1))
        .getProxy(any(YarnConfiguration.class), any(Class.class),
        eq(mockAdd1));

    // Mock RMProxy methods to generate different proxy
    // based on different IP address.
    when(mockRMProxy.getRMAddress(
        any(YarnConfiguration.class),
        any(Class.class))).thenReturn(mockAdd2);
    when(mockRMProxy.getProxy(
        any(YarnConfiguration.class),
        any(Class.class), eq(mockAdd2))).thenReturn(mockProxy2);

    // Perform Failover and get proxy again from failover proxy provider
    fpp.performFailover(actualProxy1.proxy);
    FailoverProxyProvider.ProxyInfo <RMProxy> actualProxy2 = fpp.getProxy();
    assertEquals("AutoRefreshRMFailoverProxyProvider " +
        "doesn't generate expected proxy after failover",
        mockProxy2, actualProxy2.proxy);

    // check the proxy is different with the one we created before.
    assertNotEquals("AutoRefreshRMFailoverProxyProvider " +
        "shouldn't generate same proxy after failover",
        actualProxy1.proxy, actualProxy2.proxy);

    // verify that mockRMProxy.getProxy() has been one with each address
    verify(mockRMProxy, times(1))
        .getProxy(any(YarnConfiguration.class), any(Class.class),
        eq(mockAdd1));
    verify(mockRMProxy, times(1))
        .getProxy(any(YarnConfiguration.class), any(Class.class),
        eq(mockAdd2));

    // Mock RMProxy methods to generate a different address
    when(mockRMProxy.getRMAddress(
        any(YarnConfiguration.class),
        any(Class.class))).thenReturn(mockAdd3);
    when(mockRMProxy.getProxy(
        any(YarnConfiguration.class),
        any(Class.class), eq(mockAdd3))).thenReturn(mockProxy1);

    // Perform Failover and get proxy again from failover proxy provider
    fpp.performFailover(actualProxy2.proxy);
    FailoverProxyProvider.ProxyInfo <RMProxy> actualProxy3 = fpp.getProxy();

    // check the proxy is the same as the one we created before.
    assertEquals("ConfiguredRMFailoverProxyProvider " +
        "doesn't generate expected proxy after failover",
        mockProxy1, actualProxy3.proxy);

    // verify that mockRMProxy.getProxy() is still only been invoked thrice
    verify(mockRMProxy, times(1))
        .getProxy(any(YarnConfiguration.class), any(Class.class),
        eq(mockAdd1));
    verify(mockRMProxy, times(1))
        .getProxy(any(YarnConfiguration.class), any(Class.class),
        eq(mockAdd2));
    verify(mockRMProxy, times(1))
        .getProxy(any(YarnConfiguration.class), any(Class.class),
        eq(mockAdd3));
  }

  private class HAIdMatcher implements ArgumentMatcher<YarnConfiguration> {

    private final String rmId;

    HAIdMatcher(String id) {
      this.rmId = id;
    }

    @Override
    public boolean matches(YarnConfiguration conf) {
      if (conf == null || conf.get(YarnConfiguration.RM_HA_ID) == null ||
          !conf.get(YarnConfiguration.RM_HA_ID).equals(this.rmId)) {
        return false;
      }
      return true;
    }
  }

  @Test
  public void testInitialRMSequenceShuffleRMEnable() throws Exception {
    testInitialRMSequence(true);
  }

  @Test
  public void testInitialRMSequenceShuffleRMDisable() throws Exception {
    testInitialRMSequence(false);
  }

  public void testInitialRMSequence(boolean shuffled) throws Exception {
    conf.set(YarnConfiguration.RM_HA_IDS, "rm1,rm2,rm3");
    conf.setBoolean(YarnConfiguration.RM_HA_ENABLED, true);
    conf.setBoolean(YarnConfiguration.CLIENT_FAILOVER_SHUFFLE_RM_KEY, shuffled);
    conf.setClass(YarnConfiguration.CLIENT_FAILOVER_PROXY_PROVIDER,
        ConfiguredRMFailoverProxyProvider.class, FailoverProxyProvider.class);

    // Set RM address for HA
    HATestUtil.setConfForRM("rm1", RM_ADDRESS, "localhost:" + RM1_PORT, conf);
    HATestUtil.setConfForRM("rm2", RM_ADDRESS, "localhost:" + RM2_PORT, conf);
    HATestUtil.setConfForRM("rm3", RM_ADDRESS, "localhost:" + RM3_PORT, conf);

    // Create two proxies and mock a RMProxy
    Proxy mockProxy1 = new TestProxy((proxy, method, args) -> null);
    Proxy mockProxy2 = new TestProxy((proxy, method, args) -> null);
    Proxy mockProxy3 = new TestProxy((proxy, method, args) -> null);

    // Create mock address
    InetSocketAddress mockAdd1 = new InetSocketAddress("localhost", RM1_PORT);
    InetSocketAddress mockAdd2 = new InetSocketAddress("localhost", RM2_PORT);
    InetSocketAddress mockAdd3 = new InetSocketAddress("localhost", RM3_PORT);

    // Mock RMProxy methods
    RMProxy<Proxy> mockRMProxy = mock(RMProxy.class);
    when(mockRMProxy.getRMAddress(argThat(new HAIdMatcher("rm1")),
        any(Class.class))).thenReturn(mockAdd1);
    when(mockRMProxy.getRMAddress(argThat(new HAIdMatcher("rm2")),
        any(Class.class))).thenReturn(mockAdd2);
    when(mockRMProxy.getRMAddress(argThat(new HAIdMatcher("rm3")),
        any(Class.class))).thenReturn(mockAdd3);

    when(mockRMProxy.getProxy(any(YarnConfiguration.class),
        any(Class.class), eq(mockAdd1))).thenReturn(mockProxy1);
    when(mockRMProxy.getProxy(any(YarnConfiguration.class),
        any(Class.class), eq(mockAdd2))).thenReturn(mockProxy2);
    when(mockRMProxy.getProxy(any(YarnConfiguration.class),
        any(Class.class), eq(mockAdd3))).thenReturn(mockProxy3);

    int matchAdd1 = 0;
    int matchAdd2 = 0;
    int matchAdd3 = 0;
    int unMatched = 0;

    Class protocol = ApplicationClientProtocol.class;
    ConfiguredRMFailoverProxyProvider<Proxy> fpp =
        new ConfiguredRMFailoverProxyProvider<>();

    for (int i = 0; i < 20; i++) {
      // Initialize failover proxy provider and get proxy from it.
      fpp.init(conf, mockRMProxy, protocol);
      FailoverProxyProvider.ProxyInfo<Proxy> actualProxy = fpp.getProxy();
      if (actualProxy.proxy.equals(mockProxy1)) {
        matchAdd1++;
      } else if (actualProxy.proxy.equals(mockProxy2)) {
        matchAdd2++;
      } else if (actualProxy.proxy.equals(mockProxy3)) {
        matchAdd3++;
      } else {
        unMatched++;
      }
    }
    if (shuffled) {
      assertTrue(matchAdd1 > 0);
      assertTrue(matchAdd2 > 0);
      assertTrue(matchAdd3 > 0);
      assertEquals(0, unMatched);
    } else {
      assertEquals(20, matchAdd1);
      assertEquals(0, matchAdd2);
      assertEquals(0, matchAdd3);
      assertEquals(0, unMatched);
    }
  }
}

