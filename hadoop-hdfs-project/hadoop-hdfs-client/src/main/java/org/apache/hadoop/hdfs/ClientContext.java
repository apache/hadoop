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
package org.apache.hadoop.hdfs;

import static org.apache.hadoop.fs.CommonConfigurationKeys.FS_CLIENT_TOPOLOGY_RESOLUTION_ENABLED;
import static org.apache.hadoop.fs.CommonConfigurationKeys.FS_CLIENT_TOPOLOGY_RESOLUTION_ENABLED_DEFAULT;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.hdfs.client.impl.DfsClientConf;
import org.apache.hadoop.hdfs.client.impl.DfsClientConf.ShortCircuitConf;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.shortcircuit.DomainSocketFactory;
import org.apache.hadoop.hdfs.shortcircuit.ShortCircuitCache;
import org.apache.hadoop.hdfs.util.ByteArrayManager;
import org.apache.hadoop.net.DNSToSwitchMapping;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.net.NetworkTopology;
import org.apache.hadoop.net.NodeBase;
import org.apache.hadoop.net.ScriptBasedMapping;
import org.apache.hadoop.util.ReflectionUtils;

import com.google.common.annotations.VisibleForTesting;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ClientContext contains context information for a client.
 *
 * This allows us to share caches such as the socket cache across
 * DFSClient instances.
 */
@InterfaceAudience.Private
public class ClientContext {
  private static final Logger LOG = LoggerFactory.getLogger(
      ClientContext.class);

  /**
   * Global map of context names to caches contexts.
   */
  private final static HashMap<String, ClientContext> CACHES = new HashMap<>();

  /**
   * Name of context.
   */
  private final String name;

  /**
   * String representation of the configuration.
   */
  private final String confString;

  /**
   * Caches short-circuit file descriptors, mmap regions.
   */
  private final ShortCircuitCache shortCircuitCache;

  /**
   * Caches TCP and UNIX domain sockets for reuse.
   */
  private final PeerCache peerCache;

  /**
   * Stores information about socket paths.
   */
  private final DomainSocketFactory domainSocketFactory;

  /**
   * Caches key Providers for the DFSClient
   */
  private final KeyProviderCache keyProviderCache;
  /**
   * True if we should use the legacy BlockReaderLocal.
   */
  private final boolean useLegacyBlockReaderLocal;

  /**
   * True if the legacy BlockReaderLocal is disabled.
   *
   * The legacy block reader local gets disabled completely whenever there is an
   * error or miscommunication.  The new block reader local code handles this
   * case more gracefully inside DomainSocketFactory.
   */
  private volatile boolean disableLegacyBlockReaderLocal = false;

  /** Creating byte[] for {@link DFSOutputStream}. */
  private final ByteArrayManager byteArrayManager;

  /**
   * Whether or not we complained about a DFSClient fetching a CacheContext that
   * didn't match its config values yet.
   */
  private boolean printedConfWarning = false;

  private NodeBase clientNode;
  private boolean topologyResolutionEnabled;

  private ClientContext(String name, DfsClientConf conf,
      Configuration config) {
    final ShortCircuitConf scConf = conf.getShortCircuitConf();

    this.name = name;
    this.confString = scConf.confAsString();
    this.shortCircuitCache = ShortCircuitCache.fromConf(scConf);
    this.peerCache = new PeerCache(scConf.getSocketCacheCapacity(),
        scConf.getSocketCacheExpiry());
    this.keyProviderCache = new KeyProviderCache(
        scConf.getKeyProviderCacheExpiryMs());
    this.useLegacyBlockReaderLocal = scConf.isUseLegacyBlockReaderLocal();
    this.domainSocketFactory = new DomainSocketFactory(scConf);

    this.byteArrayManager = ByteArrayManager.newInstance(
        conf.getWriteByteArrayManagerConf());
    initTopologyResolution(config);
  }

  private void initTopologyResolution(Configuration config) {
    topologyResolutionEnabled = config.getBoolean(
        FS_CLIENT_TOPOLOGY_RESOLUTION_ENABLED,
        FS_CLIENT_TOPOLOGY_RESOLUTION_ENABLED_DEFAULT);
    if (!topologyResolutionEnabled) {
      return;
    }
    DNSToSwitchMapping dnsToSwitchMapping = ReflectionUtils.newInstance(
        config.getClass(
            CommonConfigurationKeys.NET_TOPOLOGY_NODE_SWITCH_MAPPING_IMPL_KEY,
            ScriptBasedMapping.class, DNSToSwitchMapping.class), config);
    String clientHostName = NetUtils.getLocalHostname();
    List<String> nodes = new ArrayList<>();
    nodes.add(clientHostName);
    List<String> resolvedHosts = dnsToSwitchMapping.resolve(nodes);
    if (resolvedHosts != null && !resolvedHosts.isEmpty() &&
        !resolvedHosts.get(0).equals(NetworkTopology.DEFAULT_RACK)) {
      // The client machine is able to resolve its own network location.
      this.clientNode = new NodeBase(clientHostName, resolvedHosts.get(0));
    }
  }

  public static ClientContext get(String name, DfsClientConf conf,
      Configuration config) {
    ClientContext context;
    synchronized(ClientContext.class) {
      context = CACHES.get(name);
      if (context == null) {
        context = new ClientContext(name, conf, config);
        CACHES.put(name, context);
      } else {
        context.printConfWarningIfNeeded(conf);
      }
    }
    return context;
  }

  public static ClientContext get(String name, Configuration config) {
    return get(name, new DfsClientConf(config), config);
  }

  /**
   * Get a client context, from a Configuration object.
   *
   * This method is less efficient than the version which takes a DFSClient#Conf
   * object, and should be mostly used by tests.
   */
  @VisibleForTesting
  public static ClientContext getFromConf(Configuration conf) {
    return get(conf.get(HdfsClientConfigKeys.DFS_CLIENT_CONTEXT,
        HdfsClientConfigKeys.DFS_CLIENT_CONTEXT_DEFAULT), conf);
  }

  private void printConfWarningIfNeeded(DfsClientConf conf) {
    String existing = this.getConfString();
    String requested = conf.getShortCircuitConf().confAsString();
    if (!existing.equals(requested)) {
      if (!printedConfWarning) {
        printedConfWarning = true;
        LOG.warn("Existing client context '" + name + "' does not match " +
            "requested configuration.  Existing: " + existing +
            ", Requested: " + requested);
      }
    }
  }

  public String getConfString() {
    return confString;
  }

  public ShortCircuitCache getShortCircuitCache() {
    return shortCircuitCache;
  }

  public PeerCache getPeerCache() {
    return peerCache;
  }

  public KeyProviderCache getKeyProviderCache() {
    return keyProviderCache;
  }

  public boolean getUseLegacyBlockReaderLocal() {
    return useLegacyBlockReaderLocal;
  }

  public boolean getDisableLegacyBlockReaderLocal() {
    return disableLegacyBlockReaderLocal;
  }

  public void setDisableLegacyBlockReaderLocal() {
    disableLegacyBlockReaderLocal = true;
  }

  public DomainSocketFactory getDomainSocketFactory() {
    return domainSocketFactory;
  }

  public ByteArrayManager getByteArrayManager() {
    return byteArrayManager;
  }

  public int getNetworkDistance(DatanodeInfo datanodeInfo) throws IOException {
    // If applications disable the feature or the client machine can't
    // resolve its network location, clientNode will be set to null.
    if (clientNode == null) {
      return DFSUtilClient.isLocalAddress(NetUtils.
          createSocketAddr(datanodeInfo.getXferAddr())) ? 0 :
          Integer.MAX_VALUE;
    }
    NodeBase node = new NodeBase(datanodeInfo.getHostName(),
        datanodeInfo.getNetworkLocation());
    return NetworkTopology.getDistanceByPath(clientNode, node);
  }
}
