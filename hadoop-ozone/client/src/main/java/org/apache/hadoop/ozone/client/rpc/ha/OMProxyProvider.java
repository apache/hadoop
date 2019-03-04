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

package org.apache.hadoop.ozone.client.rpc.ha;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.Client;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.ozone.OmUtils;
import org.apache.hadoop.ozone.om.protocolPB
    .OzoneManagerProtocolClientSideTranslatorPB;
import org.apache.hadoop.ozone.om.protocolPB.OzoneManagerProtocolPB;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.ratis.protocol.ClientId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_ADDRESS_KEY;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_SERVICE_IDS_KEY;

/**
 * A failover proxy provider implementation which allows clients to configure
 * multiple OMs to connect to. In case of OM failover, client can try
 * connecting to another OM node from the list of proxies.
 */
public class OMProxyProvider implements Closeable {

  public static final Logger LOG =
      LoggerFactory.getLogger(OMProxyProvider.class);

  private List<OMProxyInfo> omProxies;

  private int currentProxyIndex = 0;

  private final Configuration conf;
  private final long omVersion;
  private final UserGroupInformation ugi;
  private ClientId clientId = ClientId.randomId();

  public OMProxyProvider(Configuration configuration,
      UserGroupInformation ugi) {
    this.conf = configuration;
    this.omVersion = RPC.getProtocolVersion(OzoneManagerProtocolPB.class);
    this.ugi = ugi;
    loadOMClientConfigs(conf);
  }

  private void loadOMClientConfigs(Configuration config) {
    this.omProxies = new ArrayList<>();

    Collection<String> omServiceIds = config.getTrimmedStringCollection(
        OZONE_OM_SERVICE_IDS_KEY);

    if (omServiceIds.size() > 1) {
      throw new IllegalArgumentException("Multi-OM Services is not supported." +
          " Please configure only one OM Service ID in " +
          OZONE_OM_SERVICE_IDS_KEY);
    }

    for (String serviceId : OmUtils.emptyAsSingletonNull(omServiceIds)) {
      Collection<String> omNodeIds = OmUtils.getOMNodeIds(config, serviceId);

      for (String nodeId : OmUtils.emptyAsSingletonNull(omNodeIds)) {

        String rpcAddrKey = OmUtils.addKeySuffixes(OZONE_OM_ADDRESS_KEY,
            serviceId, nodeId);
        String rpcAddrStr = OmUtils.getOmRpcAddress(config, rpcAddrKey);
        if (rpcAddrStr == null) {
          continue;
        }

        InetSocketAddress addr = NetUtils.createSocketAddr(rpcAddrStr);

        // Add the OM client proxy info to list of proxies
        if (addr != null) {
          OMProxyInfo omProxyInfo = new OMProxyInfo(addr);
          omProxies.add(omProxyInfo);
        } else {
          LOG.error("Failed to create OM proxy at address {}", rpcAddrStr);
        }
      }
    }

    if (omProxies.isEmpty()) {
      throw new IllegalArgumentException("Could not find any configured " +
          "addresses for OM. Please configure the system with "
          + OZONE_OM_ADDRESS_KEY);
    }
  }

  private OzoneManagerProtocolClientSideTranslatorPB getOMClient(
      InetSocketAddress omAddress) throws IOException {
    return new OzoneManagerProtocolClientSideTranslatorPB(
        RPC.getProxy(OzoneManagerProtocolPB.class, omVersion, omAddress, ugi,
            conf, NetUtils.getDefaultSocketFactory(conf),
            Client.getRpcTimeout(conf)), clientId.toString());
  }

  /**
   * Get the proxy object which should be used until the next failover event
   * occurs. RPC proxy object is intialized lazily.
   * @return the OM proxy object to invoke methods upon
   */
  public synchronized OzoneManagerProtocolClientSideTranslatorPB getProxy() {
    OMProxyInfo currentOMProxyInfo = omProxies.get(currentProxyIndex);
    return createOMClientIfNeeded(currentOMProxyInfo);
  }

  private OzoneManagerProtocolClientSideTranslatorPB createOMClientIfNeeded(
      OMProxyInfo proxyInfo) {
    if (proxyInfo.getOMProxy() == null) {
      try {
        proxyInfo.setOMProxy(getOMClient(proxyInfo.getAddress()));
      } catch (IOException ioe) {
        LOG.error("{} Failed to create RPC proxy to OM at {}",
            this.getClass().getSimpleName(), proxyInfo.getAddress(), ioe);
        throw new RuntimeException(ioe);
      }
    }
    return proxyInfo.getOMProxy();
  }

  /**
   * Called whenever an error warrants failing over. It is determined by the
   * retry policy.
   */
  public void performFailover() {
    incrementProxyIndex();
  }

  synchronized void incrementProxyIndex() {
    currentProxyIndex = (currentProxyIndex + 1) % omProxies.size();
  }

  /**
   * Close all the proxy objects which have been opened over the lifetime of
   * the proxy provider.
   */
  @Override
  public synchronized void close() throws IOException {
    for (OMProxyInfo proxy : omProxies) {
      OzoneManagerProtocolClientSideTranslatorPB omProxy = proxy.getOMProxy();
      if (omProxy != null) {
        RPC.stopProxy(omProxy);
      }
    }
  }

  @VisibleForTesting
  public List<OMProxyInfo> getOMProxies() {
    return omProxies;
  }
}
