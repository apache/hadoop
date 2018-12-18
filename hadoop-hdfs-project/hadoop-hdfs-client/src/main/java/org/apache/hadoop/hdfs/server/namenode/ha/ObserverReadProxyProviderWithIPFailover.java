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
package org.apache.hadoop.hdfs.server.namenode.ha;

import java.net.InetSocketAddress;
import java.net.URI;

import java.util.Collections;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.hdfs.HAUtilClient;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Extends {@link ObserverReadProxyProvider} to support NameNode IP failover.
 *
 * For Observer reads a client needs to know physical addresses of all
 * NameNodes, so that it could switch between active and observer nodes
 * for write and read requests.
 *
 * Traditional {@link IPFailoverProxyProvider} works with a virtual
 * address of the NameNode. If active NameNode fails the virtual address
 * is assigned to the standby NameNode, and IPFailoverProxyProvider, which
 * keeps talking to the same virtual address is in fact now connects to
 * the new physical server.
 *
 * To combine these behaviors ObserverReadProxyProviderWithIPFailover
 * should both
 * <ol>
 * <li> Maintain all physical addresses of NameNodes in order to allow
 * observer reads, and
 * <li> Should rely on the virtual address of the NameNode in order to
 * perform failover by assuming that the virtual address always points
 * to the active NameNode.
 * </ol>
 *
 * An example of a configuration to leverage
 * ObserverReadProxyProviderWithIPFailover
 * should include the following values:
 * <pre>{@code
 * fs.defaultFS = hdfs://mycluster
 * dfs.nameservices = mycluster
 * dfs.ha.namenodes.mycluster = ha1,ha2
 * dfs.namenode.rpc-address.mycluster.ha1 = nn01-ha1.com:8020
 * dfs.namenode.rpc-address.mycluster.ha2 = nn01-ha2.com:8020
 * dfs.client.failover.ipfailover.virtual-address.mycluster =
 *     hdfs://nn01.com:8020
 * dfs.client.failover.proxy.provider.mycluster =
 *     org.apache...ObserverReadProxyProviderWithIPFailover
 * }</pre>
 * Here {@code nn01.com:8020} is the virtual address of the active NameNode,
 * while {@code nn01-ha1.com:8020} and {@code nn01-ha2.com:8020}
 * are the physically addresses the two NameNodes.
 *
 * With this configuration, client will use
 * ObserverReadProxyProviderWithIPFailover, which creates proxies for both
 * nn01-ha1 and nn01-ha2, used for read/write RPC calls, but for the failover,
 * it relies on the virtual address nn01.com
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class ObserverReadProxyProviderWithIPFailover<T extends ClientProtocol>
    extends ObserverReadProxyProvider<T> {
  private static final Logger LOG = LoggerFactory.getLogger(
      ObserverReadProxyProviderWithIPFailover.class);

  private static final String IPFAILOVER_CONFIG_PREFIX =
      HdfsClientConfigKeys.Failover.PREFIX + "ipfailover.virtual-address";

  /**
   * By default ObserverReadProxyProviderWithIPFailover
   * uses {@link IPFailoverProxyProvider} for failover.
   */
  public ObserverReadProxyProviderWithIPFailover(
      Configuration conf, URI uri, Class<T> xface, HAProxyFactory<T> factory) {
    this(conf, uri, xface, factory,
        new IPFailoverProxyProvider<>(conf,
            getFailoverVirtualIP(conf, uri.getHost()), xface, factory));
  }

  @Override
  public boolean useLogicalURI() {
    return true;
  }

  public ObserverReadProxyProviderWithIPFailover(
      Configuration conf, URI uri, Class<T> xface, HAProxyFactory<T> factory,
      AbstractNNFailoverProxyProvider<T> failoverProxy) {
    super(conf, uri, xface, factory, failoverProxy);
    cloneDelegationTokenForVirtualIP(conf, uri);
  }

  /**
   * Clone delegation token for the virtual IP. Specifically
   * clone the dt that corresponds to the name service uri,
   * to the configured corresponding virtual IP.
   *
   * @param conf configuration
   * @param haURI the ha uri, a name service id in this case.
   */
  private void cloneDelegationTokenForVirtualIP(
      Configuration conf, URI haURI) {
    URI virtualIPURI = getFailoverVirtualIP(conf, haURI.getHost());
    InetSocketAddress vipAddress = new InetSocketAddress(
        virtualIPURI.getHost(), virtualIPURI.getPort());
    HAUtilClient.cloneDelegationTokenForLogicalUri(
        ugi, haURI, Collections.singleton(vipAddress));
  }

  private static URI getFailoverVirtualIP(
      Configuration conf, String nameServiceID) {
    String configKey = IPFAILOVER_CONFIG_PREFIX + "." + nameServiceID;
    String virtualIP = conf.get(configKey);
    LOG.info("Name service ID {} will use virtual IP {} for failover",
        nameServiceID, virtualIP);
    if (virtualIP == null) {
      throw new IllegalArgumentException("Virtual IP for failover not found,"
          + "misconfigured " + configKey + "?");
    }
    return URI.create(virtualIP);
  }
}
