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

import org.apache.hadoop.io.retry.FailoverProxyProvider;
import org.apache.hadoop.yarn.api.ApplicationClientProtocol;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerServiceProtos;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerServiceProtos.ActiveRMInfoProto;

import org.junit.Assert;
import org.junit.Test;

public class TestRMFailoverProxyProvider {
  @Test
  public void testConfiguredRMFailoverProxyProvider() {
    YarnConfiguration conf = new YarnConfiguration();
    conf.setBoolean(YarnConfiguration.RM_HA_ENABLED, true);
    conf.set(YarnConfiguration.RM_HA_IDS, "rm0,rm1");
    conf.set("yarn.resourcemanager.address.rm0", YarnConfiguration.DEFAULT_RM_ADDRESS);
    conf.set("yarn.resourcemanager.address.rm1", YarnConfiguration.DEFAULT_RM_ADDRESS);

    ConfiguredRMFailoverProxyProvider provider = new ConfiguredRMFailoverProxyProvider();
    provider.init(conf, ClientRMProxy.INSTANCE, ApplicationClientProtocol.class);
    FailoverProxyProvider.ProxyInfo proxy = provider.getProxy();
    Assert.assertEquals("rm0", proxy.proxyInfo);

    provider.performFailover(proxy);
    proxy = provider.getProxy();
    Assert.assertEquals("rm1", proxy.proxyInfo);

    provider.performFailover(proxy);
    proxy = provider.getProxy();
    Assert.assertEquals("rm0", proxy.proxyInfo);
  }

  @Test
  public void testZKConfiguredRMFailoverProxyProvider() throws Exception {
    YarnConfiguration conf = new YarnConfiguration();
    conf.setBoolean(YarnConfiguration.RM_HA_ENABLED, true);
    conf.set(YarnConfiguration.RM_HA_IDS, "rm0,rm1");
    conf.set("yarn.resourcemanager.address.rm0", YarnConfiguration.DEFAULT_RM_ADDRESS);
    conf.set("yarn.resourcemanager.address.rm1", YarnConfiguration.DEFAULT_RM_ADDRESS);

    ZkConfiguredFailoverProxyProvider provider = new ZkConfiguredFailoverProxyProvider() {
      @Override
      protected ActiveRMInfoProto getActiveRMInfoProto() {
        return ActiveRMInfoProto.newBuilder().setClusterId("TestRMFailoverProxyProvider")
            .setRmId("zk")
            .setRmAddr(YarnConfiguration.DEFAULT_RM_ADDRESS)
            .build();
      }
    };

    provider.init(conf,ClientRMProxy.INSTANCE,ApplicationClientProtocol.class) ;
    FailoverProxyProvider.ProxyInfo proxy = provider.getProxy();
    Assert.assertEquals("rm0", proxy.proxyInfo);

    provider.performFailover(proxy);
    proxy = provider.getProxy();
    Assert.assertEquals("rm1", proxy.proxyInfo);

    provider.performFailover(proxy);
    proxy = provider.getProxy();
    Assert.assertEquals("zk", proxy.proxyInfo);

    provider.performFailover(proxy);
    proxy = provider.getProxy();
    Assert.assertEquals("rm0", proxy.proxyInfo);
  }
}
