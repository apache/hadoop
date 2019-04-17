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

package org.apache.hadoop.ozone.om.ha;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.retry.FailoverProxyProvider;
import org.apache.hadoop.ipc.Client;
import org.apache.hadoop.ipc.ProtobufRpcEngine;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.ozone.OmUtils;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.om.protocolPB.OzoneManagerProtocolPB;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_ADDRESS_KEY;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_SERVICE_IDS_KEY;

/**
 * A failover proxy provider implementation which allows clients to configure
 * multiple OMs to connect to. In case of OM failover, client can try
 * connecting to another OM node from the list of proxies.
 */
public class OMFailoverProxyProvider implements
    FailoverProxyProvider<OzoneManagerProtocolPB>, Closeable {

  public static final Logger LOG =
      LoggerFactory.getLogger(OMFailoverProxyProvider.class);

  // Map of OMNodeID to its proxy
  private Map<String, OMProxyInfo> omProxies;
  private List<String> omNodeIDList;

  private String currentProxyOMNodeId;
  private int currentProxyIndex;

  private final Configuration conf;
  private final long omVersion;
  private final UserGroupInformation ugi;

  public OMFailoverProxyProvider(OzoneConfiguration configuration,
      UserGroupInformation ugi) throws IOException {
    this.conf = configuration;
    this.omVersion = RPC.getProtocolVersion(OzoneManagerProtocolPB.class);
    this.ugi = ugi;
    loadOMClientConfigs(conf);

    currentProxyIndex = 0;
    currentProxyOMNodeId = omNodeIDList.get(currentProxyIndex);
  }

  /**
   * Class to store proxy information.
   */
  public class OMProxyInfo
      extends FailoverProxyProvider.ProxyInfo<OzoneManagerProtocolPB> {
    private InetSocketAddress address;
    private Text dtService;

    OMProxyInfo(OzoneManagerProtocolPB proxy, String proxyInfoStr,
        Text dtService,
        InetSocketAddress addr) {
      super(proxy, proxyInfoStr);
      this.address = addr;
      this.dtService = dtService;
    }

    public InetSocketAddress getAddress() {
      return address;
    }

    public Text getDelegationTokenService() {
      return dtService;
    }
  }

  private void loadOMClientConfigs(Configuration config) throws IOException {
    this.omProxies = new HashMap<>();
    this.omNodeIDList = new ArrayList<>();

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
          Text dtService = SecurityUtil.buildTokenService(addr);
          StringBuilder proxyInfo = new StringBuilder()
              .append(nodeId).append("(")
              .append(NetUtils.getHostPortString(addr)).append(")");
          OMProxyInfo omProxyInfo = new OMProxyInfo(null,
              proxyInfo.toString(), dtService, addr);

          // For a non-HA OM setup, nodeId might be null. If so, we assign it
          // a dummy value
          if (nodeId == null) {
            nodeId = OzoneConsts.OM_NODE_ID_DUMMY;
          }
          omProxies.put(nodeId, omProxyInfo);
          omNodeIDList.add(nodeId);

        } else {
          LOG.error("Failed to create OM proxy for {} at address {}",
              nodeId, rpcAddrStr);
        }
      }
    }

    if (omProxies.isEmpty()) {
      throw new IllegalArgumentException("Could not find any configured " +
          "addresses for OM. Please configure the system with "
          + OZONE_OM_ADDRESS_KEY);
    }
  }

  @VisibleForTesting
  public synchronized String getCurrentProxyOMNodeId() {
    return currentProxyOMNodeId;
  }

  private OzoneManagerProtocolPB createOMProxy(InetSocketAddress omAddress)
      throws IOException {
    RPC.setProtocolEngine(conf, OzoneManagerProtocolPB.class,
        ProtobufRpcEngine.class);
    return RPC.getProxy(OzoneManagerProtocolPB.class, omVersion, omAddress, ugi,
        conf, NetUtils.getDefaultSocketFactory(conf),
        Client.getRpcTimeout(conf));
  }

  /**
   * Get the proxy object which should be used until the next failover event
   * occurs. RPC proxy object is intialized lazily.
   * @return the OM proxy object to invoke methods upon
   */
  @Override
  public synchronized OMProxyInfo getProxy() {
    OMProxyInfo currentOMProxyInfo = omProxies.get(currentProxyOMNodeId);
    createOMProxyIfNeeded(currentOMProxyInfo);
    return currentOMProxyInfo;
  }

  /**
   * Creates OM proxy object if it does not already exist.
   */
  private OMProxyInfo createOMProxyIfNeeded(OMProxyInfo proxyInfo) {
    if (proxyInfo.proxy == null) {
      try {
        proxyInfo.proxy = createOMProxy(proxyInfo.address);
      } catch (IOException ioe) {
        LOG.error("{} Failed to create RPC proxy to OM at {}",
            this.getClass().getSimpleName(), proxyInfo.address, ioe);
        throw new RuntimeException(ioe);
      }
    }
    return proxyInfo;
  }

  /**
   * Called whenever an error warrants failing over. It is determined by the
   * retry policy.
   */
  @Override
  public void performFailover(OzoneManagerProtocolPB currentProxy) {
    int newProxyIndex = incrementProxyIndex();
    LOG.debug("Failing over OM proxy to index: {}, nodeId: {}",
        newProxyIndex, omNodeIDList.get(newProxyIndex));
  }

  /**
   * Update the proxy index to the next proxy in the list.
   * @return the new proxy index
   */
  private synchronized int incrementProxyIndex() {
    currentProxyIndex = (currentProxyIndex + 1) % omProxies.size();
    currentProxyOMNodeId = omNodeIDList.get(currentProxyIndex);
    return currentProxyIndex;
  }

  @Override
  public Class<OzoneManagerProtocolPB> getInterface() {
    return OzoneManagerProtocolPB.class;
  }

  /**
   * Performs failover if the leaderOMNodeId returned through OMReponse does
   * not match the current leaderOMNodeId cached by the proxy provider.
   */
  public void performFailoverIfRequired(String newLeaderOMNodeId) {
    if (newLeaderOMNodeId == null) {
      LOG.debug("No suggested leader nodeId. Performing failover to next peer" +
          " node");
      performFailover(null);
    } else {
      if (updateLeaderOMNodeId(newLeaderOMNodeId)) {
        LOG.debug("Failing over OM proxy to nodeId: {}", newLeaderOMNodeId);
      }
    }
  }

  /**
   * Failover to the OM proxy specified by the new leader OMNodeId.
   * @param newLeaderOMNodeId OMNodeId to failover to.
   * @return true if failover is successful, false otherwise.
   */
  synchronized boolean updateLeaderOMNodeId(String newLeaderOMNodeId) {
    if (!currentProxyOMNodeId.equals(newLeaderOMNodeId)) {
      if (omProxies.containsKey(newLeaderOMNodeId)) {
        currentProxyOMNodeId = newLeaderOMNodeId;
        currentProxyIndex = omNodeIDList.indexOf(currentProxyOMNodeId);
        return true;
      }
    }
    return false;
  }

  /**
   * Close all the proxy objects which have been opened over the lifetime of
   * the proxy provider.
   */
  @Override
  public synchronized void close() throws IOException {
    for (OMProxyInfo proxy : omProxies.values()) {
      OzoneManagerProtocolPB omProxy = proxy.proxy;
      if (omProxy != null) {
        RPC.stopProxy(omProxy);
      }
    }
  }

  @VisibleForTesting
  public List<OMProxyInfo> getOMProxies() {
    return new ArrayList<>(omProxies.values());
  }
}

