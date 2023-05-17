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

package org.apache.hadoop.yarn.client;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.retry.FailoverProxyProvider;
import org.apache.hadoop.net.MockDomainNameResolver;
import org.apache.hadoop.yarn.api.ApplicationClientProtocol;
import org.apache.hadoop.yarn.conf.HAUtil;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;

public class TestConfiguredRMFailoverProxyProvider {
  private final static List<String> HOST_LIST = Arrays.asList(
      "host01",
      "host02",
      "host03",
      "host04"
  );
  private final static String SCHEDULER_HOST_ADDRESS = "host11";
  private final static List<byte[]> IP_LIST = Arrays.asList(
      new byte[] {10, 0, 0, 1},
      new byte[] {10, 0, 0, 2},
      new byte[] {10, 0, 0, 3},
      new byte[] {10, 0, 0, 4}
  );
  private final static String MULTI_A_RM_ID = "rm";
  private final static String MULTI_A_RM_ADDRESS = "rm.com";
  private final static String MULTI_A_RM_SCHEDULER_ADDRESS = "rmscheduler.com";
  private final static long REFRESH_TIME_INTERVAL = 1000; // unit ms

  public Configuration getDNSConfig() {
    Configuration dnsConf = new Configuration();
    dnsConf.set(YarnConfiguration.RESOLVE_RM_ADDRESS_KEY, MockDomainNameResolver.class.getName());
    dnsConf.set(YarnConfiguration.RM_HA_IDS, MULTI_A_RM_ID);
    dnsConf.set(HAUtil.addSuffix(YarnConfiguration.RM_ADDRESS, MULTI_A_RM_ID),
        MULTI_A_RM_ADDRESS + ":" + YarnConfiguration.DEFAULT_RM_PORT);
    dnsConf.set(HAUtil.addSuffix(YarnConfiguration.RM_ADMIN_ADDRESS, MULTI_A_RM_ID),
        MULTI_A_RM_ADDRESS + ":" + YarnConfiguration.DEFAULT_RM_ADMIN_PORT);
    dnsConf.set(HAUtil.addSuffix(YarnConfiguration.RM_SCHEDULER_ADDRESS, MULTI_A_RM_ID),
        MULTI_A_RM_SCHEDULER_ADDRESS + ":" + YarnConfiguration.DEFAULT_RM_SCHEDULER_PORT);
    dnsConf.setBoolean(YarnConfiguration.RESOLVE_RM_ADDRESS_NEEDED_KEY, true);
    dnsConf.setBoolean(YarnConfiguration.RESOLVE_RM_ADDRESS_TO_FQDN, true);
    return dnsConf;
  }

  /*
   * Override the MockDomainNameResolver address mapping in HAUtil
   * Source of mapping is from HOST_LIST and IP_LIST except for the exludeIdx one
   */
  private void overrideDNSMapping(int excludeIdx) throws UnknownHostException {
    // Mapping of domain names and IP addresses
    Map<String, InetAddress[]> addressMap = new HashMap<>();
    // Mapping from IP addresses to fqdns
    Map<InetAddress, String> ptrMap = new HashMap<>();
    int idx = 0;
    InetAddress[] addresses = new InetAddress[HOST_LIST.size() - 1];
    InetAddress[] schedulerAddresses = new InetAddress[1];
    addressMap.put(MULTI_A_RM_ADDRESS, addresses);
    addressMap.put(MULTI_A_RM_SCHEDULER_ADDRESS, schedulerAddresses);
    // for addresses
    for (int i = 0; i < HOST_LIST.size(); i++) {
      if (i == excludeIdx) {
        continue;
      }
      InetAddress address = InetAddress.getByAddress(IP_LIST.get(i));
      String host = HOST_LIST.get(i);
      addresses[idx++] = address;
      ptrMap.put(address, host);
    }
    // for schedulerAddress
    InetAddress schedulerAddress = InetAddress.getByAddress(new byte[] {10, 1, 1, 1});
    schedulerAddresses[0] = schedulerAddress;
    ptrMap.put(schedulerAddress, SCHEDULER_HOST_ADDRESS);
    ((MockDomainNameResolver)HAUtil.getDnr()).setAddressMap(addressMap);
    ((MockDomainNameResolver)HAUtil.getDnr()).setPtrMap(ptrMap);
  }

  @SuppressWarnings("unchecked") // mock generics
  private void initProxyProvider(
      Configuration conf,
      ConfiguredRMFailoverProxyProvider<ApplicationClientProtocol> proxyProvider) {
    RMProxy rm1Mock = mock(RMProxy.class);
    doNothing().when(rm1Mock).checkAllowedProtocols(ApplicationClientProtocol.class);
    proxyProvider.init(conf, rm1Mock, ApplicationClientProtocol.class);
  }

  @Test(expected = IllegalStateException.class)
  public void testInitWithNoInstances() {
    Configuration conf = new Configuration();
    ConfiguredRMFailoverProxyProvider<ApplicationClientProtocol> proxyProviderWithNoInstances =
        new ConfiguredRMFailoverProxyProvider<ApplicationClientProtocol>();
    initProxyProvider(conf, proxyProviderWithNoInstances);
  }

  @Test
  public void testGetProxy() {
    Configuration conf = new Configuration();
    conf.set(YarnConfiguration.RM_HA_IDS, "rm1,rm2,rm3,rm4");
    ConfiguredRMFailoverProxyProvider<ApplicationClientProtocol> proxyProvider =
        new ConfiguredRMFailoverProxyProvider<ApplicationClientProtocol>();
    initProxyProvider(conf, proxyProvider);
    assertEquals(proxyProvider.rmServiceIds[0], "rm1");
    FailoverProxyProvider.ProxyInfo<ApplicationClientProtocol> current =
        proxyProvider.getProxy();
    assertEquals(current.proxyInfo, "rm1");
  }

  @Test
  public void testInitWithMultiARecordRM() throws UnknownHostException {
    Configuration conf = getDNSConfig();
    ConfiguredRMFailoverProxyProvider<ApplicationClientProtocol> proxyProviderWithmultiA =
        new ConfiguredRMFailoverProxyProvider<ApplicationClientProtocol>();
    HAUtil.setDnrByConfiguration(conf);
    overrideDNSMapping(1);
    initProxyProvider(conf, proxyProviderWithmultiA);
    assertEquals(HOST_LIST.size() - 1, proxyProviderWithmultiA.rmServiceIds.length);
    for (String rmId : proxyProviderWithmultiA.rmServiceIds) {
      assertTrue(rmId.startsWith("rm_resolved_"));
    }
  }

  @Test
  public void testResolveDifferentMultiARecordRM() throws UnknownHostException {
    Configuration conf = getDNSConfig();
    ConfiguredRMFailoverProxyProvider<ApplicationClientProtocol> proxyProviderWithmultiA =
        new ConfiguredRMFailoverProxyProvider<ApplicationClientProtocol>();
    HAUtil.setDnrByConfiguration(conf);
    overrideDNSMapping(1);
    initProxyProvider(conf, proxyProviderWithmultiA);
    List<String> rmAddresses = getRMAddresses(proxyProviderWithmultiA.conf,
        proxyProviderWithmultiA.rmServiceIds,
        YarnConfiguration.RM_ADDRESS);
    List<String> rmAdminAddresses = getRMAddresses(proxyProviderWithmultiA.conf,
        proxyProviderWithmultiA.rmServiceIds,
        YarnConfiguration.RM_ADMIN_ADDRESS);
    List<String> rmSchedulerAddresses = getRMAddresses(proxyProviderWithmultiA.conf,
        proxyProviderWithmultiA.rmServiceIds,
        YarnConfiguration.RM_SCHEDULER_ADDRESS);
    assertEquals(HOST_LIST.size() - 1, rmAddresses.size());
    assertEquals(HOST_LIST.size() - 1, rmAdminAddresses.size());
    assertEquals(1, rmSchedulerAddresses.size());
    assertEquals(SCHEDULER_HOST_ADDRESS + ":8030", rmSchedulerAddresses.get(0));
  }

  @Test
  public void testRefreshWithMultiARecordRM() throws UnknownHostException, InterruptedException {
    Configuration conf = getDNSConfig();
    conf.setLong(YarnConfiguration.RM_ID_REFRESH_INTERVAL, REFRESH_TIME_INTERVAL);
    ConfiguredRMFailoverProxyProvider<ApplicationClientProtocol> proxyProviderWithmultiA =
        new ConfiguredRMFailoverProxyProvider<ApplicationClientProtocol>();
    HAUtil.setDnrByConfiguration(conf);
    overrideDNSMapping(0);
    initProxyProvider(conf, proxyProviderWithmultiA);
    InetSocketAddress oldActiveRMAddress =
        conf.getSocketAddr(HAUtil.addSuffix(YarnConfiguration.RM_ADDRESS, HAUtil.getRMHAId(conf)),
            YarnConfiguration.DEFAULT_RM_ADDRESS,
            YarnConfiguration.DEFAULT_RM_PORT);
    List<String> oldRMAddresses = getRMAddresses(
        conf, proxyProviderWithmultiA.rmServiceIds, YarnConfiguration.RM_ADDRESS);
    for (String address : oldRMAddresses) {
      assertTrue(!address.equals(HOST_LIST.get(0)));
    }
    // refresh
    overrideDNSMapping(2);
    Thread.sleep(REFRESH_TIME_INTERVAL * 2);
    InetSocketAddress newActiveRMAddress =
        conf.getSocketAddr(HAUtil.addSuffix(YarnConfiguration.RM_ADDRESS, HAUtil.getRMHAId(conf)),
            YarnConfiguration.DEFAULT_RM_ADDRESS,
            YarnConfiguration.DEFAULT_RM_PORT);
    List<String> newRMAddresses = getRMAddresses(
        conf, proxyProviderWithmultiA.rmServiceIds, YarnConfiguration.RM_ADDRESS);
    assertEquals(HOST_LIST.size() - 1, newRMAddresses.size());
    // active RM should remain same, even without failover
    assertEquals(oldActiveRMAddress, newActiveRMAddress);
    for (String address : newRMAddresses) {
      assertTrue(!address.equals(HOST_LIST.get(2)));
    }
  }

  private List<String> getRMAddresses(Configuration conf, String[] rmServiceIds,
      String configKey) {
    List<String> addresses = new ArrayList<>();
    for (String rmId : rmServiceIds) {
      String address = conf.get(HAUtil.addSuffix(configKey, rmId));
      if (address != null) {
        addresses.add(address);
      }
    }
    return addresses;
  }
}
