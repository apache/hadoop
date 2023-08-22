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
package org.apache.hadoop.hdfs.server.blockmanagement;

import static org.apache.hadoop.hdfs.server.protocol.DatanodeProtocol.DNA_ERASURE_CODING_RECONSTRUCTION;
import static org.apache.hadoop.util.Time.monotonicNow;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.util.Preconditions;

import org.apache.hadoop.thirdparty.com.google.common.collect.ImmutableList;
import org.apache.hadoop.thirdparty.com.google.common.net.InetAddresses;

import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.net.DFSNetworkTopology;
import org.apache.hadoop.hdfs.protocol.*;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo.DatanodeInfoBuilder;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.DatanodeReportType;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenIdentifier;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeDescriptor.BlockTargetPair;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeDescriptor.CachedBlocksList;
import org.apache.hadoop.hdfs.server.common.Util;
import org.apache.hadoop.hdfs.server.namenode.CachedBlock;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.namenode.Namesystem;
import org.apache.hadoop.hdfs.server.protocol.*;
import org.apache.hadoop.hdfs.server.protocol.BlockECReconstructionCommand.BlockECReconstructionInfo;
import org.apache.hadoop.hdfs.server.protocol.BlockRecoveryCommand.RecoveringBlock;
import org.apache.hadoop.hdfs.server.protocol.BlockRecoveryCommand.RecoveringStripedBlock;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.net.*;
import org.apache.hadoop.net.NetworkTopology.InvalidTopologyException;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.Daemon;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.Sets;
import org.apache.hadoop.util.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/**
 * Manage datanodes, include decommission and other activities.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class DatanodeManager {
  static final Logger LOG = LoggerFactory.getLogger(DatanodeManager.class);

  private final Namesystem namesystem;
  private final BlockManager blockManager;
  private final DatanodeAdminManager datanodeAdminManager;
  private final HeartbeatManager heartbeatManager;
  private final FSClusterStats fsClusterStats;

  private volatile long heartbeatIntervalSeconds;
  private volatile int heartbeatRecheckInterval;
  /**
   * Stores the datanode -> block map.  
   * <p>
   * Done by storing a set of {@link DatanodeDescriptor} objects, sorted by 
   * storage id. In order to keep the storage map consistent it tracks 
   * all storages ever registered with the namenode.
   * A descriptor corresponding to a specific storage id can be
   * <ul> 
   * <li>added to the map if it is a new storage id;</li>
   * <li>updated with a new datanode started as a replacement for the old one 
   * with the same storage id; and </li>
   * <li>removed if and only if an existing datanode is restarted to serve a
   * different storage id.</li>
   * </ul> <br> 
   * <p>
   * Mapping: StorageID -> DatanodeDescriptor
   */
  private final Map<String, DatanodeDescriptor> datanodeMap
      = new HashMap<>();

  /** Cluster network topology. */
  private final NetworkTopology networktopology;

  /** Host names to datanode descriptors mapping. */
  private final Host2NodesMap host2DatanodeMap = new Host2NodesMap();

  private final DNSToSwitchMapping dnsToSwitchMapping;
  private final boolean rejectUnresolvedTopologyDN;

  private final int defaultXferPort;
  
  private final int defaultInfoPort;

  private final int defaultInfoSecurePort;

  private final int defaultIpcPort;

  /** Read include/exclude files. */
  private HostConfigManager hostConfigManager;

  /** The period to wait for datanode heartbeat.*/
  private long heartbeatExpireInterval;
  /** Ask Datanode only up to this many blocks to delete. */
  private volatile int blockInvalidateLimit;

  /** The interval for judging stale DataNodes for read/write */
  private final long staleInterval;
  
  /** Whether or not to avoid using stale DataNodes for reading */
  private final boolean avoidStaleDataNodesForRead;

  /** Whether or not to avoid using slow DataNodes for reading. */
  private volatile boolean avoidSlowDataNodesForRead;

  /** Whether or not to consider lad for reading. */
  private final boolean readConsiderLoad;

  /** Whether or not to consider storageType for reading. */
  private final boolean readConsiderStorageType;

  /**
   * Whether or not to avoid using stale DataNodes for writing.
   * Note that, even if this is configured, the policy may be
   * temporarily disabled when a high percentage of the nodes
   * are marked as stale.
   */
  private final boolean avoidStaleDataNodesForWrite;

  /**
   * When the ratio of stale datanodes reaches this number, stop avoiding 
   * writing to stale datanodes, i.e., continue using stale nodes for writing.
   */
  private final float ratioUseStaleDataNodesForWrite;

  /** The number of stale DataNodes */
  private volatile int numStaleNodes;

  /** The number of stale storages */
  private volatile int numStaleStorages;

  /**
   * Number of blocks to check for each postponedMisreplicatedBlocks iteration
   */
  private final long blocksPerPostponedMisreplicatedBlocksRescan;

  /**
   * Whether or not this cluster has ever consisted of more than 1 rack,
   * according to the NetworkTopology.
   */
  private boolean hasClusterEverBeenMultiRack = false;

  private final boolean checkIpHostnameInRegistration;
  /**
   * Whether we should tell datanodes what to cache in replies to
   * heartbeat messages.
   */
  private boolean shouldSendCachingCommands = false;

  /**
   * The number of datanodes for each software version. This list should change
   * during rolling upgrades.
   * Software version -> Number of datanodes with this version
   */
  private final HashMap<String, Integer> datanodesSoftwareVersions =
    new HashMap<>(4, 0.75f);

  /**
   *  True if we should process latency metrics from individual DN disks.
   */
  private final boolean dataNodeDiskStatsEnabled;

  /**
   * If we use DfsNetworkTopology to choose nodes for placing replicas.
   */
  private final boolean useDfsNetworkTopology;

  private static final String IP_PORT_SEPARATOR = ":";

  @Nullable
  private SlowPeerTracker slowPeerTracker;
  private static Set<String> slowNodesUuidSet = Sets.newConcurrentHashSet();
  private Daemon slowPeerCollectorDaemon;
  private final long slowPeerCollectionInterval;
  private volatile int maxSlowPeerReportNodes;

  @Nullable
  private final SlowDiskTracker slowDiskTracker;
  
  /**
   * The minimum time between resending caching directives to Datanodes,
   * in milliseconds.
   *
   * Note that when a rescan happens, we will send the new directives
   * as soon as possible.  This timeout only applies to resending 
   * directives that we've already sent.
   */
  private final long timeBetweenResendingCachingDirectivesMs;

  DatanodeManager(final BlockManager blockManager, final Namesystem namesystem,
      final Configuration conf) throws IOException {
    this.namesystem = namesystem;
    this.blockManager = blockManager;

    this.useDfsNetworkTopology = conf.getBoolean(
        DFSConfigKeys.DFS_USE_DFS_NETWORK_TOPOLOGY_KEY,
        DFSConfigKeys.DFS_USE_DFS_NETWORK_TOPOLOGY_DEFAULT);
    if (useDfsNetworkTopology) {
      networktopology = DFSNetworkTopology.getInstance(conf);
    } else {
      networktopology = NetworkTopology.getInstance(conf);
    }
    this.heartbeatManager = new HeartbeatManager(namesystem,
        blockManager, conf);
    this.datanodeAdminManager = new DatanodeAdminManager(namesystem,
        blockManager, heartbeatManager);
    this.fsClusterStats = newFSClusterStats();
    this.dataNodeDiskStatsEnabled = Util.isDiskStatsEnabled(conf.getInt(
        DFSConfigKeys.DFS_DATANODE_FILEIO_PROFILING_SAMPLING_PERCENTAGE_KEY,
        DFSConfigKeys.
            DFS_DATANODE_FILEIO_PROFILING_SAMPLING_PERCENTAGE_DEFAULT));
    final Timer timer = new Timer();
    final boolean dataNodePeerStatsEnabledVal =
        conf.getBoolean(DFSConfigKeys.DFS_DATANODE_PEER_STATS_ENABLED_KEY,
            DFSConfigKeys.DFS_DATANODE_PEER_STATS_ENABLED_DEFAULT);
    this.maxSlowPeerReportNodes = conf.getInt(
        DFSConfigKeys.DFS_NAMENODE_MAX_SLOWPEER_COLLECT_NODES_KEY,
        DFSConfigKeys.DFS_NAMENODE_MAX_SLOWPEER_COLLECT_NODES_DEFAULT);
    this.slowPeerCollectionInterval = conf.getTimeDuration(
        DFSConfigKeys.DFS_NAMENODE_SLOWPEER_COLLECT_INTERVAL_KEY,
        DFSConfigKeys.DFS_NAMENODE_SLOWPEER_COLLECT_INTERVAL_DEFAULT,
        TimeUnit.MILLISECONDS);
    initSlowPeerTracker(conf, timer, dataNodePeerStatsEnabledVal);
    this.slowDiskTracker = dataNodeDiskStatsEnabled ?
        new SlowDiskTracker(conf, timer) : null;
    this.defaultXferPort = NetUtils.createSocketAddr(
          conf.getTrimmed(DFSConfigKeys.DFS_DATANODE_ADDRESS_KEY,
              DFSConfigKeys.DFS_DATANODE_ADDRESS_DEFAULT)).getPort();
    this.defaultInfoPort = NetUtils.createSocketAddr(
          conf.getTrimmed(DFSConfigKeys.DFS_DATANODE_HTTP_ADDRESS_KEY,
              DFSConfigKeys.DFS_DATANODE_HTTP_ADDRESS_DEFAULT)).getPort();
    this.defaultInfoSecurePort = NetUtils.createSocketAddr(
        conf.getTrimmed(DFSConfigKeys.DFS_DATANODE_HTTPS_ADDRESS_KEY,
            DFSConfigKeys.DFS_DATANODE_HTTPS_ADDRESS_DEFAULT)).getPort();
    this.defaultIpcPort = NetUtils.createSocketAddr(
          conf.getTrimmed(DFSConfigKeys.DFS_DATANODE_IPC_ADDRESS_KEY,
              DFSConfigKeys.DFS_DATANODE_IPC_ADDRESS_DEFAULT)).getPort();
    this.hostConfigManager = ReflectionUtils.newInstance(
        conf.getClass(DFSConfigKeys.DFS_NAMENODE_HOSTS_PROVIDER_CLASSNAME_KEY,
            HostFileManager.class, HostConfigManager.class), conf);
    try {
      this.hostConfigManager.refresh();
    } catch (IOException e) {
      LOG.error("error reading hosts files: ", e);
    }
    this.dnsToSwitchMapping = ReflectionUtils.newInstance(
        conf.getClass(DFSConfigKeys.NET_TOPOLOGY_NODE_SWITCH_MAPPING_IMPL_KEY, 
            ScriptBasedMapping.class, DNSToSwitchMapping.class), conf);
    this.rejectUnresolvedTopologyDN = conf.getBoolean(
        DFSConfigKeys.DFS_REJECT_UNRESOLVED_DN_TOPOLOGY_MAPPING_KEY,
        DFSConfigKeys.DFS_REJECT_UNRESOLVED_DN_TOPOLOGY_MAPPING_DEFAULT);
    
    // If the dns to switch mapping supports cache, resolve network
    // locations of those hosts in the include list and store the mapping
    // in the cache; so future calls to resolve will be fast.
    if (dnsToSwitchMapping instanceof CachedDNSToSwitchMapping) {
      final ArrayList<String> locations = new ArrayList<>();
      for (InetSocketAddress addr : hostConfigManager.getIncludes()) {
        locations.add(addr.getAddress().getHostAddress());
      }
      dnsToSwitchMapping.resolve(locations);
    }
    heartbeatIntervalSeconds = conf.getTimeDuration(
        DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_KEY,
        DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_DEFAULT, TimeUnit.SECONDS);
    heartbeatRecheckInterval = conf.getInt(
        DFSConfigKeys.DFS_NAMENODE_HEARTBEAT_RECHECK_INTERVAL_KEY, 
        DFSConfigKeys.DFS_NAMENODE_HEARTBEAT_RECHECK_INTERVAL_DEFAULT); // 5 minutes
    this.heartbeatExpireInterval = 2 * heartbeatRecheckInterval
        + 10 * 1000 * heartbeatIntervalSeconds;
    final int configuredBlockInvalidateLimit = conf.getInt(
        DFSConfigKeys.DFS_BLOCK_INVALIDATE_LIMIT_KEY,
        DFSConfigKeys.DFS_BLOCK_INVALIDATE_LIMIT_DEFAULT);
    // Block invalidate limit also has some dependency on heartbeat interval.
    // Check setBlockInvalidateLimit().
    setBlockInvalidateLimit(configuredBlockInvalidateLimit);
    this.checkIpHostnameInRegistration = conf.getBoolean(
        DFSConfigKeys.DFS_NAMENODE_DATANODE_REGISTRATION_IP_HOSTNAME_CHECK_KEY,
        DFSConfigKeys.DFS_NAMENODE_DATANODE_REGISTRATION_IP_HOSTNAME_CHECK_DEFAULT);
    LOG.info(DFSConfigKeys.DFS_NAMENODE_DATANODE_REGISTRATION_IP_HOSTNAME_CHECK_KEY
        + "=" + checkIpHostnameInRegistration);
    this.avoidStaleDataNodesForRead = conf.getBoolean(
        DFSConfigKeys.DFS_NAMENODE_AVOID_STALE_DATANODE_FOR_READ_KEY,
        DFSConfigKeys.DFS_NAMENODE_AVOID_STALE_DATANODE_FOR_READ_DEFAULT);
    this.avoidSlowDataNodesForRead = conf.getBoolean(
        DFSConfigKeys.DFS_NAMENODE_AVOID_SLOW_DATANODE_FOR_READ_KEY,
        DFSConfigKeys.DFS_NAMENODE_AVOID_SLOW_DATANODE_FOR_READ_DEFAULT);
    this.readConsiderLoad = conf.getBoolean(
        DFSConfigKeys.DFS_NAMENODE_READ_CONSIDERLOAD_KEY,
        DFSConfigKeys.DFS_NAMENODE_READ_CONSIDERLOAD_DEFAULT);
    this.readConsiderStorageType = conf.getBoolean(
        DFSConfigKeys.DFS_NAMENODE_READ_CONSIDERSTORAGETYPE_KEY,
        DFSConfigKeys.DFS_NAMENODE_READ_CONSIDERSTORAGETYPE_DEFAULT);
    if (readConsiderLoad && readConsiderStorageType) {
      LOG.warn(
          "{} and {} are incompatible and only one can be enabled. "
              + "Both are currently enabled. {} will be ignored.",
          DFSConfigKeys.DFS_NAMENODE_READ_CONSIDERLOAD_KEY,
          DFSConfigKeys.DFS_NAMENODE_READ_CONSIDERSTORAGETYPE_KEY,
          DFSConfigKeys.DFS_NAMENODE_READ_CONSIDERSTORAGETYPE_KEY);
    }
    this.avoidStaleDataNodesForWrite = conf.getBoolean(
        DFSConfigKeys.DFS_NAMENODE_AVOID_STALE_DATANODE_FOR_WRITE_KEY,
        DFSConfigKeys.DFS_NAMENODE_AVOID_STALE_DATANODE_FOR_WRITE_DEFAULT);
    this.staleInterval = getStaleIntervalFromConf(conf, heartbeatExpireInterval);
    this.ratioUseStaleDataNodesForWrite = conf.getFloat(
        DFSConfigKeys.DFS_NAMENODE_USE_STALE_DATANODE_FOR_WRITE_RATIO_KEY,
        DFSConfigKeys.DFS_NAMENODE_USE_STALE_DATANODE_FOR_WRITE_RATIO_DEFAULT);
    Preconditions.checkArgument(
        (ratioUseStaleDataNodesForWrite > 0 && 
            ratioUseStaleDataNodesForWrite <= 1.0f),
        DFSConfigKeys.DFS_NAMENODE_USE_STALE_DATANODE_FOR_WRITE_RATIO_KEY +
        " = '" + ratioUseStaleDataNodesForWrite + "' is invalid. " +
        "It should be a positive non-zero float value, not greater than 1.0f.");
    this.timeBetweenResendingCachingDirectivesMs = conf.getLong(
        DFSConfigKeys.DFS_NAMENODE_PATH_BASED_CACHE_RETRY_INTERVAL_MS,
        DFSConfigKeys.DFS_NAMENODE_PATH_BASED_CACHE_RETRY_INTERVAL_MS_DEFAULT);
    this.blocksPerPostponedMisreplicatedBlocksRescan = conf.getLong(
        DFSConfigKeys.DFS_NAMENODE_BLOCKS_PER_POSTPONEDBLOCKS_RESCAN_KEY,
        DFSConfigKeys.DFS_NAMENODE_BLOCKS_PER_POSTPONEDBLOCKS_RESCAN_KEY_DEFAULT);
  }

  /**
   * Determines whether slow peer tracker should be enabled. If dataNodePeerStatsEnabledVal is
   * true, slow peer tracker is initialized.
   *
   * @param conf The configuration to use while initializing slowPeerTracker.
   * @param timer Timer object for slowPeerTracker.
   * @param dataNodePeerStatsEnabled To determine whether slow peer tracking should be enabled.
   */
  public void initSlowPeerTracker(Configuration conf, Timer timer,
      boolean dataNodePeerStatsEnabled) {
    this.slowPeerTracker = dataNodePeerStatsEnabled ?
        new SlowPeerTracker(conf, timer) :
        new SlowPeerDisabledTracker(conf, timer);
    if (slowPeerTracker.isSlowPeerTrackerEnabled()) {
      startSlowPeerCollector();
    } else {
      stopSlowPeerCollector();
    }
  }

  private void startSlowPeerCollector() {
    if (slowPeerCollectorDaemon != null) {
      LOG.warn("Slow peers collection thread has been started.");
      return;
    }
    slowPeerCollectorDaemon = new Daemon(new Runnable() {
      @Override
      public void run() {
        while (true) {
          try {
            slowNodesUuidSet = getSlowPeersUuidSet();
          } catch (Exception e) {
            LOG.error("Failed to collect slow peers", e);
          }

          try {
            Thread.sleep(slowPeerCollectionInterval);
          } catch (InterruptedException e) {
            LOG.error("Slow peers collection thread interrupted", e);
            return;
          }
        }
      }
    });
    slowPeerCollectorDaemon.start();
    LOG.info("Slow peers collection thread start.");
  }

  public void stopSlowPeerCollector() {
    LOG.info("Slow peers collection thread shutdown");
    if (slowPeerCollectorDaemon == null) {
      return;
    }
    slowPeerCollectorDaemon.interrupt();
    try {
      slowPeerCollectorDaemon.join();
    } catch (InterruptedException e) {
      LOG.error("Slow peers collection thread did not shutdown", e);
    } finally {
      slowPeerCollectorDaemon = null;
    }
  }

  private static long getStaleIntervalFromConf(Configuration conf,
      long heartbeatExpireInterval) {
    long staleInterval = conf.getLong(
        DFSConfigKeys.DFS_NAMENODE_STALE_DATANODE_INTERVAL_KEY, 
        DFSConfigKeys.DFS_NAMENODE_STALE_DATANODE_INTERVAL_DEFAULT);
    Preconditions.checkArgument(staleInterval > 0,
        DFSConfigKeys.DFS_NAMENODE_STALE_DATANODE_INTERVAL_KEY +
        " = '" + staleInterval + "' is invalid. " +
        "It should be a positive non-zero value.");
    
    final long heartbeatIntervalSeconds = conf.getTimeDuration(
        DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_KEY,
        DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_DEFAULT, TimeUnit.SECONDS);
    // The stale interval value cannot be smaller than 
    // 3 times of heartbeat interval 
    final long minStaleInterval = conf.getInt(
        DFSConfigKeys.DFS_NAMENODE_STALE_DATANODE_MINIMUM_INTERVAL_KEY,
        DFSConfigKeys.DFS_NAMENODE_STALE_DATANODE_MINIMUM_INTERVAL_DEFAULT)
        * heartbeatIntervalSeconds * 1000;
    if (staleInterval < minStaleInterval) {
      LOG.warn("The given interval for marking stale datanode = "
          + staleInterval + ", which is less than "
          + DFSConfigKeys.DFS_NAMENODE_STALE_DATANODE_MINIMUM_INTERVAL_DEFAULT
          + " heartbeat intervals. This may cause too frequent changes of " 
          + "stale states of DataNodes since a heartbeat msg may be missing " 
          + "due to temporary short-term failures. Reset stale interval to " 
          + minStaleInterval + ".");
      staleInterval = minStaleInterval;
    }
    if (staleInterval > heartbeatExpireInterval) {
      LOG.warn("The given interval for marking stale datanode = "
          + staleInterval + ", which is larger than heartbeat expire interval "
          + heartbeatExpireInterval + ".");
    }
    return staleInterval;
  }
  
  void activate(final Configuration conf) {
    datanodeAdminManager.activate(conf);
    heartbeatManager.activate();
  }

  void close() {
    datanodeAdminManager.close();
    heartbeatManager.close();
    stopSlowPeerCollector();
  }

  /** @return the network topology. */
  public NetworkTopology getNetworkTopology() {
    return networktopology;
  }

  /** @return the heartbeat manager. */
  HeartbeatManager getHeartbeatManager() {
    return heartbeatManager;
  }

  @VisibleForTesting
  public DatanodeAdminManager getDatanodeAdminManager() {
    return datanodeAdminManager;
  }

  public HostConfigManager getHostConfigManager() {
    return hostConfigManager;
  }

  @VisibleForTesting
  public void setHeartbeatExpireInterval(long expiryMs) {
    this.heartbeatExpireInterval = expiryMs;
  }

  @VisibleForTesting
  public FSClusterStats getFSClusterStats() {
    return fsClusterStats;
  }

  @VisibleForTesting
  public int getBlockInvalidateLimit() {
    return blockInvalidateLimit;
  }

  /** @return the datanode statistics. */
  public DatanodeStatistics getDatanodeStatistics() {
    return heartbeatManager;
  }

  private boolean isInactive(DatanodeInfo datanode) {
    return datanode.isDecommissioned() ||
        datanode.isDecommissionInProgress() ||
        datanode.isEnteringMaintenance() ||
        (avoidStaleDataNodesForRead && datanode.isStale(staleInterval));
  }

  private boolean isSlowNode(String dnUuid) {
    return avoidSlowDataNodesForRead && slowNodesUuidSet.contains(dnUuid);
  }

  public void setAvoidSlowDataNodesForReadEnabled(boolean enable) {
    this.avoidSlowDataNodesForRead = enable;
  }

  @VisibleForTesting
  public boolean getEnableAvoidSlowDataNodesForRead() {
    return this.avoidSlowDataNodesForRead;
  }

  public void setMaxSlowpeerCollectNodes(int maxNodes) {
    this.maxSlowPeerReportNodes = maxNodes;
  }

  @VisibleForTesting
  public int getMaxSlowpeerCollectNodes() {
    return this.maxSlowPeerReportNodes;
  }

  /**
   * Sort the non-striped located blocks by the distance to the target host.
   *
   * For striped blocks, it will only move decommissioned/decommissioning/stale/slow
   * nodes to the bottom. For example, assume we have storage list:
   * d0, d1, d2, d3, d4, d5, d6, d7, d8, d9
   * mapping to block indices:
   * 0, 1, 2, 3, 4, 5, 6, 7, 8, 2
   *
   * Here the internal block b2 is duplicated, locating in d2 and d9. If d2 is
   * a decommissioning node then should switch d2 and d9 in the storage list.
   * After sorting locations, will update corresponding block indices
   * and block tokens.
   */
  public void sortLocatedBlocks(final String targetHost,
      final List<LocatedBlock> locatedBlocks) {
    Comparator<DatanodeInfo> comparator =
        avoidStaleDataNodesForRead || avoidSlowDataNodesForRead ?
        new DFSUtil.StaleAndSlowComparator(
            avoidStaleDataNodesForRead, staleInterval,
            avoidSlowDataNodesForRead, slowNodesUuidSet) :
        new DFSUtil.ServiceComparator();
    // sort located block
    for (LocatedBlock lb : locatedBlocks) {
      if (lb.isStriped()) {
        sortLocatedStripedBlock(lb, comparator);
      } else {
        sortLocatedBlock(lb, targetHost, comparator);
      }
    }
  }

  /**
   * Move decommissioned/decommissioning/entering_maintenance/stale/slow
   * datanodes to the bottom. After sorting it will
   * update block indices and block tokens respectively.
   *
   * @param lb located striped block
   * @param comparator dn comparator
   */
  private void sortLocatedStripedBlock(final LocatedBlock lb,
      Comparator<DatanodeInfo> comparator) {
    DatanodeInfo[] di = lb.getLocations();
    HashMap<DatanodeInfo, Byte> locToIndex = new HashMap<>();
    HashMap<DatanodeInfo, Token<BlockTokenIdentifier>> locToToken =
        new HashMap<>();
    LocatedStripedBlock lsb = (LocatedStripedBlock) lb;
    for (int i = 0; i < di.length; i++) {
      locToIndex.put(di[i], lsb.getBlockIndices()[i]);
      locToToken.put(di[i], lsb.getBlockTokens()[i]);
    }
    // Arrange the order of datanodes as follows:
    // live(in-service) -> stale -> entering_maintenance -> decommissioning -> decommissioned
    Arrays.sort(di, comparator);

    // must update cache since we modified locations array
    lb.updateCachedStorageInfo();

    // must update block indices and block tokens respectively
    for (int i = 0; i < di.length; i++) {
      lsb.getBlockIndices()[i] = locToIndex.get(di[i]);
      lsb.getBlockTokens()[i] = locToToken.get(di[i]);
    }
  }

  /**
   * Move decommissioned/decommissioning/entering_maintenance/stale/slow
   * datanodes to the bottom. Also, sort nodes by network
   * distance.
   *
   * @param lb located block
   * @param targetHost target host
   * @param comparator dn comparator
   */
  private void sortLocatedBlock(final LocatedBlock lb, String targetHost,
      Comparator<DatanodeInfo> comparator) {
    // As it is possible for the separation of node manager and datanode, 
    // here we should get node but not datanode only .
    boolean nonDatanodeReader = false;
    Node client = getDatanodeByHost(targetHost);
    if (client == null) {
      nonDatanodeReader = true;
      List<String> hosts = new ArrayList<>(1);
      hosts.add(targetHost);
      List<String> resolvedHosts = dnsToSwitchMapping.resolve(hosts);
      if (resolvedHosts != null && !resolvedHosts.isEmpty()) {
        String rName = resolvedHosts.get(0);
        if (rName != null) {
          client = new NodeBase(rName + NodeBase.PATH_SEPARATOR_STR +
            targetHost);
        }
      } else {
        LOG.error("Node Resolution failed. Please make sure that rack " +
          "awareness scripts are functional.");
      }
    }

    DatanodeInfoWithStorage[] di = lb.getLocations();
    // Arrange the order of datanodes as follows:
    // live(in-service) -> stale -> entering_maintenance -> decommissioning -> decommissioned
    Arrays.sort(di, comparator);

    // Sort nodes by network distance only for located blocks
    int lastActiveIndex = di.length - 1;
    while (lastActiveIndex > 0 && (
        isSlowNode(di[lastActiveIndex].getDatanodeUuid()) ||
            isInactive(di[lastActiveIndex]))) {
      --lastActiveIndex;
    }
    int activeLen = lastActiveIndex + 1;
    if(nonDatanodeReader) {
      networktopology.sortByDistanceUsingNetworkLocation(client,
          lb.getLocations(), activeLen, createSecondaryNodeSorter());
    } else {
      networktopology.sortByDistance(client, lb.getLocations(), activeLen,
          createSecondaryNodeSorter());
    }
    // move PROVIDED storage to the end to prefer local replicas.
    lb.moveProvidedToEnd(activeLen);
    // must update cache since we modified locations array
    lb.updateCachedStorageInfo();
  }

  private Consumer<List<DatanodeInfoWithStorage>> createSecondaryNodeSorter() {
    Consumer<List<DatanodeInfoWithStorage>> secondarySort = null;
    if (readConsiderStorageType) {
      Comparator<DatanodeInfoWithStorage> comp =
          Comparator.comparing(DatanodeInfoWithStorage::getStorageType);
      secondarySort = list -> Collections.sort(list, comp);
    }
    if (readConsiderLoad) {
      Comparator<DatanodeInfoWithStorage> comp =
          Comparator.comparingInt(DatanodeInfo::getXceiverCount);
      secondarySort = list -> Collections.sort(list, comp);
    }
    return secondarySort;
  }

  /** @return the datanode descriptor for the host. */
  public DatanodeDescriptor getDatanodeByHost(final String host) {
    return host2DatanodeMap.getDatanodeByHost(host);
  }

  /** @return the datanode descriptor for the host. */
  public DatanodeDescriptor getDatanodeByXferAddr(String host, int xferPort) {
    return host2DatanodeMap.getDatanodeByXferAddr(host, xferPort);
  }

  /** @return the datanode descriptors for all nodes. */
  public Set<DatanodeDescriptor> getDatanodes() {
    final Set<DatanodeDescriptor> datanodes;
    synchronized (this) {
      datanodes = new HashSet<>(datanodeMap.values());
    }
    return datanodes;
  }

  /** @return the Host2NodesMap */
  public Host2NodesMap getHost2DatanodeMap() {
    return this.host2DatanodeMap;
  }

  /**
   * Given datanode address or host name, returns the DatanodeDescriptor for the
   * same, or if it doesn't find the datanode, it looks for a machine local and
   * then rack local datanode, if a rack local datanode is not possible either,
   * it returns the DatanodeDescriptor of any random node in the cluster.
   *
   * @param address hostaddress:transfer address
   * @return the best match for the given datanode
   */
  DatanodeDescriptor getDatanodeDescriptor(String address) {
    DatanodeID dnId = parseDNFromHostsEntry(address);
    String host = dnId.getIpAddr();
    int xferPort = dnId.getXferPort();
    DatanodeDescriptor node = getDatanodeByXferAddr(host, xferPort);
    if (node == null) {
      node = getDatanodeByHost(host);
    }
    if (node == null) {
      String networkLocation = 
          resolveNetworkLocationWithFallBackToDefaultLocation(dnId);

      // If the current cluster doesn't contain the node, fallback to
      // something machine local and then rack local.
      List<Node> rackNodes = getNetworkTopology()
                                   .getDatanodesInRack(networkLocation);
      if (rackNodes != null) {
        // Try something machine local.
        for (Node rackNode : rackNodes) {
          if (((DatanodeDescriptor) rackNode).getIpAddr().equals(host)) {
            node = (DatanodeDescriptor) rackNode;
            break;
          }
        }

        // Try something rack local.
        if (node == null && !rackNodes.isEmpty()) {
          node = (DatanodeDescriptor) (rackNodes
              .get(ThreadLocalRandom.current().nextInt(rackNodes.size())));
        }
      }

      // If we can't even choose rack local, just choose any node in the
      // cluster.
      if (node == null) {
        node = (DatanodeDescriptor)getNetworkTopology()
                                   .chooseRandom(NodeBase.ROOT);
      }
    }
    return node;
  }


  /** Get a datanode descriptor given corresponding DatanodeUUID */
  public DatanodeDescriptor getDatanode(final String datanodeUuid) {
    if (datanodeUuid == null) {
      return null;
    }
    synchronized (this) {
      return datanodeMap.get(datanodeUuid);
    }
  }

  /**
   * Get data node by datanode ID.
   * 
   * @param nodeID datanode ID
   * @return DatanodeDescriptor or null if the node is not found.
   * @throws UnregisteredNodeException
   */
  public DatanodeDescriptor getDatanode(DatanodeID nodeID)
      throws UnregisteredNodeException {
    final DatanodeDescriptor node = getDatanode(nodeID.getDatanodeUuid());
    if (node == null) 
      return null;
    if (!node.getXferAddr().equals(nodeID.getXferAddr())) {
      final UnregisteredNodeException e = new UnregisteredNodeException(
          nodeID, node);
      NameNode.stateChangeLog.error("BLOCK* NameSystem.getDatanode: "
                                    + e.getLocalizedMessage());
      throw e;
    }
    return node;
  }

  public DatanodeStorageInfo[] getDatanodeStorageInfos(
      DatanodeID[] datanodeID, String[] storageIDs,
      String format, Object... args) throws UnregisteredNodeException {
    storageIDs = storageIDs == null ? new String[0] : storageIDs;
    if (datanodeID.length != storageIDs.length) {
      // Error for pre-2.0.0-alpha clients.
      final String err = (storageIDs.length == 0?
          "Missing storageIDs: It is likely that the HDFS client,"
          + " who made this call, is running in an older version of Hadoop"
          + "(pre-2.0.0-alpha)  which does not support storageIDs."
          : "Length mismatched: storageIDs.length=" + storageIDs.length + " != "
          ) + " datanodeID.length=" + datanodeID.length;
      throw new HadoopIllegalArgumentException(
          err + ", "+ String.format(format, args));
    }
    if (datanodeID.length == 0) {
      return null;
    }
    final DatanodeStorageInfo[] storages = new DatanodeStorageInfo[datanodeID.length];
    for(int i = 0; i < datanodeID.length; i++) {
      if (datanodeID[i].equals(DatanodeID.EMPTY_DATANODE_ID)) {
        storages[i] = null;
        continue;
      }
      final DatanodeDescriptor dd = getDatanode(datanodeID[i]);
      if (dd != null) {
        storages[i] = dd.getStorageInfo(storageIDs[i]);
      }
    }
    return storages;
  }

  /** Prints information about all datanodes. */
  void datanodeDump(final PrintWriter out) {
    final Map<String,DatanodeDescriptor> sortedDatanodeMap;
    synchronized (this) {
      sortedDatanodeMap = new TreeMap<>(datanodeMap);
    }
    out.println("Metasave: Number of datanodes: " + sortedDatanodeMap.size());
    for (DatanodeDescriptor node : sortedDatanodeMap.values()) {
      out.println(node.dumpDatanode());
    }
  }

  /**
   * Remove a datanode descriptor.
   * @param nodeInfo datanode descriptor.
   */
  private void removeDatanode(DatanodeDescriptor nodeInfo) {
    removeDatanode(nodeInfo, true);
  }

  /**
   * Remove a datanode descriptor.
   * @param nodeInfo datanode descriptor.
   */
  private void removeDatanode(DatanodeDescriptor nodeInfo,
      boolean removeBlocksFromBlocksMap) {
    assert namesystem.hasWriteLock();
    heartbeatManager.removeDatanode(nodeInfo);
    if (removeBlocksFromBlocksMap) {
      blockManager.removeBlocksAssociatedTo(nodeInfo);
    }
    networktopology.remove(nodeInfo);
    decrementVersionCount(nodeInfo.getSoftwareVersion());
    blockManager.getBlockReportLeaseManager().unregister(nodeInfo);

    LOG.debug("remove datanode {}.", nodeInfo);
    blockManager.checkSafeMode();
  }

  /**
   * Remove a datanode
   * @throws UnregisteredNodeException 
   */
  public void removeDatanode(final DatanodeID node)
      throws UnregisteredNodeException {
    namesystem.writeLock();
    try {
      final DatanodeDescriptor descriptor = getDatanode(node);
      if (descriptor != null) {
        removeDatanode(descriptor, true);
      } else {
        NameNode.stateChangeLog.warn("BLOCK* removeDatanode: "
                                     + node + " does not exist");
      }
    } finally {
      namesystem.writeUnlock("removeDatanode");
    }
  }

  /** Remove a dead datanode. */
  void removeDeadDatanode(final DatanodeID nodeID,
      boolean removeBlocksFromBlockMap) {
    DatanodeDescriptor d;
    try {
      d = getDatanode(nodeID);
    } catch(IOException e) {
      d = null;
    }
    if (d != null && isDatanodeDead(d)) {
      NameNode.stateChangeLog.info(
          "BLOCK* removeDeadDatanode: lost heartbeat from " + d
              + ", removeBlocksFromBlockMap " + removeBlocksFromBlockMap);
      removeDatanode(d, removeBlocksFromBlockMap);
    }
  }

  /** Is the datanode dead? */
  boolean isDatanodeDead(DatanodeDescriptor node) {
    return (node.getLastUpdateMonotonic() <
            (monotonicNow() - heartbeatExpireInterval));
  }

  /** Add a datanode. */
  void addDatanode(final DatanodeDescriptor node) {
    // To keep host2DatanodeMap consistent with datanodeMap,
    // remove  from host2DatanodeMap the datanodeDescriptor removed
    // from datanodeMap before adding node to host2DatanodeMap.
    synchronized(this) {
      host2DatanodeMap.remove(datanodeMap.put(node.getDatanodeUuid(), node));
    }

    networktopology.add(node); // may throw InvalidTopologyException
    host2DatanodeMap.add(node);
    checkIfClusterIsNowMultiRack(node);
    resolveUpgradeDomain(node);

    if (LOG.isDebugEnabled()) {
      LOG.debug("{}.addDatanode: node {} is added to datanodeMap.",
          getClass().getSimpleName(), node);
    }
  }

  /** Physically remove node from datanodeMap. */
  private void wipeDatanode(final DatanodeID node) {
    final String key = node.getDatanodeUuid();
    synchronized (this) {
      host2DatanodeMap.remove(datanodeMap.remove(key));
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("{}.wipeDatanode({}): storage {} is removed from datanodeMap.",
          getClass().getSimpleName(), node, key);
    }
  }

  private void incrementVersionCount(String version) {
    if (version == null) {
      return;
    }
    synchronized(this) {
      Integer count = this.datanodesSoftwareVersions.get(version);
      count = count == null ? 1 : count + 1;
      this.datanodesSoftwareVersions.put(version, count);
    }
  }

  private void decrementVersionCount(String version) {
    if (version == null) {
      return;
    }
    synchronized(this) {
      Integer count = this.datanodesSoftwareVersions.get(version);
      if(count != null) {
        if(count > 1) {
          this.datanodesSoftwareVersions.put(version, count-1);
        } else {
          this.datanodesSoftwareVersions.remove(version);
        }
      }
    }
  }

  /**
   * Will return true for all Datanodes which have a non-null software
   * version and are considered alive (by {@link DatanodeDescriptor#isAlive()}),
   * indicating the node has not yet been removed. Use {@code isAlive}
   * rather than {@link DatanodeManager#isDatanodeDead(DatanodeDescriptor)}
   * to ensure that the version is decremented even if the datanode
   * hasn't issued a heartbeat recently.
   *
   * @param node The datanode in question
   * @return True iff its version count should be decremented
   */
  private boolean shouldCountVersion(DatanodeDescriptor node) {
    return node.getSoftwareVersion() != null && node.isAlive();
  }

  private void countSoftwareVersions() {
    synchronized(this) {
      datanodesSoftwareVersions.clear();
      for(DatanodeDescriptor dn: datanodeMap.values()) {
        if (shouldCountVersion(dn)) {
          Integer num = datanodesSoftwareVersions.get(dn.getSoftwareVersion());
          num = num == null ? 1 : num+1;
          datanodesSoftwareVersions.put(dn.getSoftwareVersion(), num);
        }
      }
    }
  }

  public HashMap<String, Integer> getDatanodesSoftwareVersions() {
    synchronized(this) {
      return new HashMap<> (this.datanodesSoftwareVersions);
    }
  }

  void resolveUpgradeDomain(DatanodeDescriptor node) {
    String upgradeDomain = hostConfigManager.getUpgradeDomain(node);
    if (upgradeDomain != null && upgradeDomain.length() > 0) {
      node.setUpgradeDomain(upgradeDomain);
    }
  }

  /**
   *  Resolve a node's network location. If the DNS to switch mapping fails 
   *  then this method guarantees default rack location. 
   *  @param node to resolve to network location
   *  @return network location path
   */
  private String resolveNetworkLocationWithFallBackToDefaultLocation (
      DatanodeID node) {
    String networkLocation;
    try {
      networkLocation = resolveNetworkLocation(node);
    } catch (UnresolvedTopologyException e) {
      LOG.error("Unresolved topology mapping. Using " +
          NetworkTopology.DEFAULT_RACK + " for host " + node.getHostName());
      networkLocation = NetworkTopology.DEFAULT_RACK;
    }
    return networkLocation;
  }
  
  /**
   * Resolve a node's network location. If the DNS to switch mapping fails, 
   * then this method throws UnresolvedTopologyException. 
   * @param node to resolve to network location
   * @return network location path.
   * @throws UnresolvedTopologyException if the DNS to switch mapping fails 
   *    to resolve network location.
   */
  private String resolveNetworkLocation (DatanodeID node) 
      throws UnresolvedTopologyException {
    List<String> names = new ArrayList<>(1);
    if (dnsToSwitchMapping instanceof CachedDNSToSwitchMapping) {
      names.add(node.getIpAddr());
    } else {
      names.add(node.getHostName());
    }
    
    List<String> rName = resolveNetworkLocation(names);
    String networkLocation;
    if (rName == null) {
      LOG.error("The resolve call returned null!");
        throw new UnresolvedTopologyException(
            "Unresolved topology mapping for host " + node.getHostName());
    } else {
      networkLocation = rName.get(0);
    }
    return networkLocation;
  }

  /**
   * Resolve network locations for specified hosts
   *
   * @return Network locations if available, Else returns null
   */
  public List<String> resolveNetworkLocation(List<String> names) {
    // resolve its network location
    return dnsToSwitchMapping.resolve(names);
  }

  /**
   * Resolve a node's dependencies in the network. If the DNS to switch 
   * mapping fails then this method returns empty list of dependencies 
   * @param node to get dependencies for
   * @return List of dependent host names
   */
  private List<String> getNetworkDependenciesWithDefault(DatanodeInfo node) {
    List<String> dependencies;
    try {
      dependencies = getNetworkDependencies(node);
    } catch (UnresolvedTopologyException e) {
      LOG.error("Unresolved dependency mapping for host " + 
          node.getHostName() +". Continuing with an empty dependency list");
      dependencies = Collections.emptyList();
    }
    return dependencies;
  }
  
  /**
   * Resolves a node's dependencies in the network. If the DNS to switch 
   * mapping fails to get dependencies, then this method throws 
   * UnresolvedTopologyException. 
   * @param node to get dependencies for
   * @return List of dependent host names 
   * @throws UnresolvedTopologyException if the DNS to switch mapping fails
   */
  private List<String> getNetworkDependencies(DatanodeInfo node)
      throws UnresolvedTopologyException {
    List<String> dependencies = Collections.emptyList();

    if (dnsToSwitchMapping instanceof DNSToSwitchMappingWithDependency) {
      //Get dependencies
      dependencies = 
          ((DNSToSwitchMappingWithDependency)dnsToSwitchMapping).getDependency(
              node.getHostName());
      if(dependencies == null) {
        LOG.error("The dependency call returned null for host " + 
            node.getHostName());
        throw new UnresolvedTopologyException("The dependency call returned " + 
            "null for host " + node.getHostName());
      }
    }

    return dependencies;
  }

  /**
   * Remove decommissioned datanode from the the list of live or dead nodes.
   * This is used to not to display a decommissioned datanode to the operators.
   * @param nodeList , array list of live or dead nodes.
   */
  private static void removeDecomNodeFromList(
      final List<DatanodeDescriptor> nodeList) {
    for (Iterator<DatanodeDescriptor> it = nodeList.iterator(); it.hasNext();) {
      DatanodeDescriptor node = it.next();
      if (node.isDecommissioned()) {
        it.remove();
      }
    }
  }

  /**
   * Decommission the node if it is in the host exclude list.
   *
   * @param nodeReg datanode
   */
  void startAdminOperationIfNecessary(DatanodeDescriptor nodeReg) {
    long maintenanceExpireTimeInMS =
        hostConfigManager.getMaintenanceExpirationTimeInMS(nodeReg);
    // If the registered node is in exclude list, then decommission it
    if (getHostConfigManager().isExcluded(nodeReg)) {
      datanodeAdminManager.startDecommission(nodeReg);
    } else if (nodeReg.maintenanceNotExpired(maintenanceExpireTimeInMS)) {
      datanodeAdminManager.startMaintenance(nodeReg, maintenanceExpireTimeInMS);
    }
  }

  /**
   * Register the given datanode with the namenode. NB: the given
   * registration is mutated and given back to the datanode.
   *
   * @param nodeReg the datanode registration
   * @throws DisallowedDatanodeException if the registration request is
   *    denied because the datanode does not match includes/excludes
   * @throws UnresolvedTopologyException if the registration request is 
   *    denied because resolving datanode network location fails.
   */
  public void registerDatanode(DatanodeRegistration nodeReg)
      throws DisallowedDatanodeException, UnresolvedTopologyException {
    InetAddress dnAddress = Server.getRemoteIp();
    if (dnAddress != null) {
      // Mostly called inside an RPC, update ip and peer hostname
      String hostname = dnAddress.getHostName();
      String ip = dnAddress.getHostAddress();
      if (checkIpHostnameInRegistration && !isNameResolved(dnAddress)) {
        // Reject registration of unresolved datanode to prevent performance
        // impact of repetitive DNS lookups later.
        final String message = "hostname cannot be resolved (ip="
            + ip + ", hostname=" + hostname + ")";
        LOG.warn("Unresolved datanode registration: " + message);
        throw new DisallowedDatanodeException(nodeReg, message);
      }
      // update node registration with the ip and hostname from rpc request
      nodeReg.setIpAddr(ip);
      nodeReg.setPeerHostName(hostname);
    }
    
    try {
      nodeReg.setExportedKeys(blockManager.getBlockKeys());
  
      // Checks if the node is not on the hosts list.  If it is not, then
      // it will be disallowed from registering. 
      if (!hostConfigManager.isIncluded(nodeReg)) {
        throw new DisallowedDatanodeException(nodeReg);
      }
        
      NameNode.stateChangeLog.info("BLOCK* registerDatanode: from "
          + nodeReg + " storage " + nodeReg.getDatanodeUuid());
  
      DatanodeDescriptor nodeS = getDatanode(nodeReg.getDatanodeUuid());
      DatanodeDescriptor nodeN = host2DatanodeMap.getDatanodeByXferAddr(
          nodeReg.getIpAddr(), nodeReg.getXferPort());
        
      if (nodeN != null && nodeN != nodeS) {
        NameNode.LOG.info("BLOCK* registerDatanode: " + nodeN);
        // nodeN previously served a different data storage, 
        // which is not served by anybody anymore.
        removeDatanode(nodeN);
        // physically remove node from datanodeMap
        wipeDatanode(nodeN);
        nodeN = null;
      }
  
      boolean updateHost2DatanodeMap = false;
      if (nodeS != null) {
        if (nodeN == nodeS) {
          // The same datanode has been just restarted to serve the same data 
          // storage. We do not need to remove old data blocks, the delta will
          // be calculated on the next block report from the datanode
          NameNode.stateChangeLog.debug("BLOCK* registerDatanode: node restarted.");
        } else {
          // nodeS is found
          /* The registering datanode is a replacement node for the existing 
            data storage, which from now on will be served by a new node.
            If this message repeats, both nodes might have same storageID 
            by (insanely rare) random chance. User needs to restart one of the
            nodes with its data cleared (or user can just remove the StorageID
            value in "VERSION" file under the data directory of the datanode,
            but this is might not work if VERSION file format has changed 
         */
          // Check if nodeS's host information is same as nodeReg's, if not,
          // it needs to update host2DatanodeMap accordringly.
          updateHost2DatanodeMap = !nodeS.getXferAddr().equals(nodeReg.getXferAddr());

          NameNode.stateChangeLog.info("BLOCK* registerDatanode: " + nodeS
              + " is replaced by " + nodeReg + " with the same storageID "
              + nodeReg.getDatanodeUuid());
        }
        
        boolean success = false;
        try {
          // update cluster map
          getNetworkTopology().remove(nodeS);

          // Update Host2DatanodeMap
          if (updateHost2DatanodeMap) {
            getHost2DatanodeMap().remove(nodeS);
          }
          if(shouldCountVersion(nodeS)) {
            decrementVersionCount(nodeS.getSoftwareVersion());
          }
          nodeS.updateRegInfo(nodeReg);

          nodeS.setSoftwareVersion(nodeReg.getSoftwareVersion());
          nodeS.setDisallowed(false); // Node is in the include list

          // resolve network location
          if(this.rejectUnresolvedTopologyDN) {
            nodeS.setNetworkLocation(resolveNetworkLocation(nodeS));
            nodeS.setDependentHostNames(getNetworkDependencies(nodeS));
          } else {
            nodeS.setNetworkLocation(
                resolveNetworkLocationWithFallBackToDefaultLocation(nodeS));
            nodeS.setDependentHostNames(
                getNetworkDependenciesWithDefault(nodeS));
          }

          if (updateHost2DatanodeMap) {
            getHost2DatanodeMap().add(nodeS);
          }

          getNetworkTopology().add(nodeS);
          resolveUpgradeDomain(nodeS);

          // also treat the registration message as a heartbeat
          heartbeatManager.register(nodeS);
          incrementVersionCount(nodeS.getSoftwareVersion());
          startAdminOperationIfNecessary(nodeS);
          success = true;
        } finally {
          if (!success) {
            removeDatanode(nodeS);
            wipeDatanode(nodeS);
            countSoftwareVersions();
          }
        }
        return;
      }

      DatanodeDescriptor nodeDescr 
        = new DatanodeDescriptor(nodeReg, NetworkTopology.DEFAULT_RACK);
      boolean success = false;
      try {
        // resolve network location
        if(this.rejectUnresolvedTopologyDN) {
          nodeDescr.setNetworkLocation(resolveNetworkLocation(nodeDescr));
          nodeDescr.setDependentHostNames(getNetworkDependencies(nodeDescr));
        } else {
          nodeDescr.setNetworkLocation(
              resolveNetworkLocationWithFallBackToDefaultLocation(nodeDescr));
          nodeDescr.setDependentHostNames(
              getNetworkDependenciesWithDefault(nodeDescr));
        }
        nodeDescr.setSoftwareVersion(nodeReg.getSoftwareVersion());
        resolveUpgradeDomain(nodeDescr);

        // register new datanode
        addDatanode(nodeDescr);
        blockManager.getBlockReportLeaseManager().register(nodeDescr);
        // also treat the registration message as a heartbeat
        // no need to update its timestamp
        // because its is done when the descriptor is created
        heartbeatManager.addDatanode(nodeDescr);
        heartbeatManager.updateDnStat(nodeDescr);
        incrementVersionCount(nodeReg.getSoftwareVersion());
        startAdminOperationIfNecessary(nodeDescr);
        success = true;
      } finally {
        if (!success) {
          removeDatanode(nodeDescr);
          wipeDatanode(nodeDescr);
          countSoftwareVersions();
        }
      }
    } catch (InvalidTopologyException e) {
      // If the network location is invalid, clear the cached mappings
      // so that we have a chance to re-add this DataNode with the
      // correct network location later.
      List<String> invalidNodeNames = new ArrayList<>(3);
      // clear cache for nodes in IP or Hostname
      invalidNodeNames.add(nodeReg.getIpAddr());
      invalidNodeNames.add(nodeReg.getHostName());
      invalidNodeNames.add(nodeReg.getPeerHostName());
      dnsToSwitchMapping.reloadCachedMappings(invalidNodeNames);
      throw e;
    }
  }

  /**
   * Rereads conf to get hosts and exclude list file names.
   * Rereads the files to update the hosts and exclude lists.  It
   * checks if any of the hosts have changed states:
   */
  public void refreshNodes(final Configuration conf) throws IOException {
    refreshHostsReader(conf);
    namesystem.writeLock();
    try {
      refreshDatanodes();
      countSoftwareVersions();
    } finally {
      namesystem.writeUnlock("refreshNodes");
    }
  }

  /** Reread include/exclude files. */
  private void refreshHostsReader(Configuration conf) throws IOException {
    // Reread the conf to get dfs.hosts and dfs.hosts.exclude filenames.
    // Update the file names and refresh internal includes and excludes list.
    if (conf == null) {
      conf = new HdfsConfiguration();
    }
    this.hostConfigManager.setConf(conf);
    this.hostConfigManager.refresh();
  }
  
  /**
   * Reload datanode membership and the desired admin operations from
   * host files. If a node isn't allowed, hostConfigManager.isIncluded returns
   * false and the node can't be used.
   * If a node is allowed and the desired admin operation is defined,
   * it will transition to the desired admin state.
   * If a node is allowed and upgrade domain is defined,
   * the upgrade domain will be set on the node.
   * To use maintenance mode or upgrade domain, set
   * DFS_NAMENODE_HOSTS_PROVIDER_CLASSNAME_KEY to
   * CombinedHostFileManager.class.
   */
  private void refreshDatanodes() {
    final Map<String, DatanodeDescriptor> copy;
    synchronized (this) {
      copy = new HashMap<>(datanodeMap);
    }
    for (DatanodeDescriptor node : copy.values()) {
      // Check if not include.
      if (!hostConfigManager.isIncluded(node)) {
        node.setDisallowed(true);
      } else {
        long maintenanceExpireTimeInMS =
            hostConfigManager.getMaintenanceExpirationTimeInMS(node);
        if (node.maintenanceNotExpired(maintenanceExpireTimeInMS)) {
          datanodeAdminManager.startMaintenance(
              node, maintenanceExpireTimeInMS);
        } else if (hostConfigManager.isExcluded(node)) {
          datanodeAdminManager.startDecommission(node);
        } else {
          datanodeAdminManager.stopMaintenance(node);
          datanodeAdminManager.stopDecommission(node);
        }
      }
      node.setUpgradeDomain(hostConfigManager.getUpgradeDomain(node));
    }
  }

  /** @return the number of live datanodes. */
  public int getNumLiveDataNodes() {
    int numLive = 0;
    synchronized (this) {
      for(DatanodeDescriptor dn : datanodeMap.values()) {
        if (!isDatanodeDead(dn) ) {
          numLive++;
        }
      }
    }
    return numLive;
  }

  /** @return the number of dead datanodes. */
  public int getNumDeadDataNodes() {
    return getDatanodeListForReport(DatanodeReportType.DEAD).size();
  }

  /** @return the number of datanodes. */
  public int getNumOfDataNodes() {
    synchronized (this) {
      return datanodeMap.size();
    }
  }

  /** @return list of datanodes where decommissioning is in progress. */
  public List<DatanodeDescriptor> getDecommissioningNodes() {
    // There is no need to take namesystem reader lock as
    // getDatanodeListForReport will synchronize on datanodeMap
    // A decommissioning DN may be "alive" or "dead".
    return getDatanodeListForReport(DatanodeReportType.DECOMMISSIONING);
  }

  /** @return list of datanodes that are entering maintenance. */
  public List<DatanodeDescriptor> getEnteringMaintenanceNodes() {
    return getDatanodeListForReport(DatanodeReportType.ENTERING_MAINTENANCE);
  }

  /* Getter and Setter for stale DataNodes related attributes */

  /**
   * Whether stale datanodes should be avoided as targets on the write path.
   * The result of this function may change if the number of stale datanodes
   * eclipses a configurable threshold.
   * 
   * @return whether stale datanodes should be avoided on the write path
   */
  public boolean shouldAvoidStaleDataNodesForWrite() {
    // If # stale exceeds maximum staleness ratio, disable stale
    // datanode avoidance on the write path
    return avoidStaleDataNodesForWrite && numStaleNodes > 0 &&
        (numStaleNodes <= heartbeatManager.getLiveDatanodeCount()
            * ratioUseStaleDataNodesForWrite);
  }

  public long getBlocksPerPostponedMisreplicatedBlocksRescan() {
    return blocksPerPostponedMisreplicatedBlocksRescan;
  }

  /**
   * @return The time interval used to mark DataNodes as stale.
   */
  long getStaleInterval() {
    return staleInterval;
  }

  public long getHeartbeatInterval() {
    return this.heartbeatIntervalSeconds;
  }

  public long getHeartbeatRecheckInterval() {
    return this.heartbeatRecheckInterval;
  }

  /**
   * Set the number of current stale DataNodes. The HeartbeatManager got this
   * number based on DataNodes' heartbeats.
   * 
   * @param numStaleNodes
   *          The number of stale DataNodes to be set.
   */
  void setNumStaleNodes(int numStaleNodes) {
    this.numStaleNodes = numStaleNodes;
  }
  
  /**
   * @return Return the current number of stale DataNodes (detected by
   * HeartbeatManager). 
   */
  public int getNumStaleNodes() {
    return this.numStaleNodes;
  }

  /**
   * Get the number of content stale storages.
   */
  public int getNumStaleStorages() {
    return numStaleStorages;
  }

  /**
   * Set the number of content stale storages.
   *
   * @param numStaleStorages The number of content stale storages.
   */
  void setNumStaleStorages(int numStaleStorages) {
    this.numStaleStorages = numStaleStorages;
  }

  /** Fetch live and dead datanodes. */
  public void fetchDatanodes(final List<DatanodeDescriptor> live, 
      final List<DatanodeDescriptor> dead, final boolean removeDecommissionNode) {
    if (live == null && dead == null) {
      throw new HadoopIllegalArgumentException("Both live and dead lists are null");
    }

    // There is no need to take namesystem reader lock as
    // getDatanodeListForReport will synchronize on datanodeMap
    final List<DatanodeDescriptor> results =
        getDatanodeListForReport(DatanodeReportType.ALL);
    for(DatanodeDescriptor node : results) {
      if (isDatanodeDead(node)) {
        if (dead != null) {
          dead.add(node);
        }
      } else {
        if (live != null) {
          live.add(node);
        }
      }
    }
    
    if (removeDecommissionNode) {
      if (live != null) {
        removeDecomNodeFromList(live);
      }
      if (dead != null) {
        removeDecomNodeFromList(dead);
      }
    }
  }

  /**
   * Check if the cluster now consists of multiple racks. If it does, and this
   * is the first time it's consisted of multiple racks, then process blocks
   * that may now be misreplicated.
   * 
   * @param node DN which caused cluster to become multi-rack. Used for logging.
   */
  @VisibleForTesting
  void checkIfClusterIsNowMultiRack(DatanodeDescriptor node) {
    if (!hasClusterEverBeenMultiRack && networktopology.getNumOfRacks() > 1) {
      String message = "DN " + node + " joining cluster has expanded a formerly " +
          "single-rack cluster to be multi-rack. ";
      if (blockManager.isPopulatingReplQueues()) {
        message += "Re-checking all blocks for replication, since they should " +
            "now be replicated cross-rack";
        LOG.info(message);
      } else {
        if (LOG.isDebugEnabled()) {
          message += "Not checking for mis-replicated blocks because this NN "
              + "is not yet processing repl queues.";
          LOG.debug(message);
        }
      }
      hasClusterEverBeenMultiRack = true;
      if (blockManager.isPopulatingReplQueues()) {
        blockManager.processMisReplicatedBlocks();
      }
    }
  }

  /**
   * Parse a DatanodeID from a hosts file entry
   * @param hostLine of form [hostname|ip][:port]?
   * @return DatanodeID constructed from the given string
   */
  private DatanodeID parseDNFromHostsEntry(String hostLine) {
    DatanodeID dnId;
    String hostStr;
    int port;
    int idx = hostLine.indexOf(':');

    if (-1 == idx) {
      hostStr = hostLine;
      port = DFSConfigKeys.DFS_DATANODE_DEFAULT_PORT;
    } else {
      hostStr = hostLine.substring(0, idx);
      port = Integer.parseInt(hostLine.substring(idx+1));
    }

    if (InetAddresses.isInetAddress(hostStr)) {
      // The IP:port is sufficient for listing in a report
      dnId = new DatanodeID(hostStr, "", "", port,
          DFSConfigKeys.DFS_DATANODE_HTTP_DEFAULT_PORT,
          DFSConfigKeys.DFS_DATANODE_HTTPS_DEFAULT_PORT,
          DFSConfigKeys.DFS_DATANODE_IPC_DEFAULT_PORT);
    } else {
      String ipAddr = "";
      try {
        ipAddr = InetAddress.getByName(hostStr).getHostAddress();
      } catch (UnknownHostException e) {
        LOG.warn("Invalid hostname " + hostStr + " in hosts file");
      }
      dnId = new DatanodeID(ipAddr, hostStr, "", port,
          DFSConfigKeys.DFS_DATANODE_HTTP_DEFAULT_PORT,
          DFSConfigKeys.DFS_DATANODE_HTTPS_DEFAULT_PORT,
          DFSConfigKeys.DFS_DATANODE_IPC_DEFAULT_PORT);
    }
    return dnId;
  }

  /** For generating datanode reports */
  public List<DatanodeDescriptor> getDatanodeListForReport(
      final DatanodeReportType type) {
    final boolean listLiveNodes =
        type == DatanodeReportType.ALL ||
        type == DatanodeReportType.LIVE;
    final boolean listDeadNodes =
        type == DatanodeReportType.ALL ||
        type == DatanodeReportType.DEAD;
    final boolean listDecommissioningNodes =
        type == DatanodeReportType.ALL ||
        type == DatanodeReportType.DECOMMISSIONING;
    final boolean listEnteringMaintenanceNodes =
        type == DatanodeReportType.ALL ||
        type == DatanodeReportType.ENTERING_MAINTENANCE;
    final boolean listInMaintenanceNodes =
        type == DatanodeReportType.ALL ||
        type == DatanodeReportType.IN_MAINTENANCE;

    ArrayList<DatanodeDescriptor> nodes;
    final HostSet foundNodes = new HostSet();
    final Iterable<InetSocketAddress> includedNodes =
        hostConfigManager.getIncludes();

    synchronized(this) {
      nodes = new ArrayList<>(datanodeMap.size());
      for (DatanodeDescriptor dn : datanodeMap.values()) {
        final boolean isDead = isDatanodeDead(dn);
        final boolean isDecommissioning = dn.isDecommissionInProgress();
        final boolean isEnteringMaintenance = dn.isEnteringMaintenance();
        final boolean isInMaintenance = dn.isInMaintenance();

        if (((listLiveNodes && !isDead) ||
            (listDeadNodes && isDead) ||
            (listDecommissioningNodes && isDecommissioning) ||
            (listEnteringMaintenanceNodes && isEnteringMaintenance) ||
            (listInMaintenanceNodes && isInMaintenance)) &&
            hostConfigManager.isIncluded(dn)) {
          nodes.add(dn);
        }

        foundNodes.add(dn.getResolvedAddress());
      }
    }
    Collections.sort(nodes);

    if (listDeadNodes) {
      for (InetSocketAddress addr : includedNodes) {
        if (foundNodes.matchedBy(addr)) {
          continue;
        }
        // The remaining nodes are ones that are referenced by the hosts
        // files but that we do not know about, ie that we have never
        // head from. Eg. an entry that is no longer part of the cluster
        // or a bogus entry was given in the hosts files
        //
        // If the host file entry specified the xferPort, we use that.
        // Otherwise, we guess that it is the default xfer port.
        // We can't ask the DataNode what it had configured, because it's
        // dead.
        DatanodeDescriptor dn = new DatanodeDescriptor(new DatanodeID(addr
                .getAddress().getHostAddress(), addr.getHostName(), "",
                addr.getPort() == 0 ? defaultXferPort : addr.getPort(),
                defaultInfoPort, defaultInfoSecurePort, defaultIpcPort));
        setDatanodeDead(dn);
        if (hostConfigManager.isExcluded(dn)) {
          dn.setDecommissioned();
        }
        nodes.add(dn);
      }
    }

    if (LOG.isDebugEnabled()) {
      LOG.debug("getDatanodeListForReport with includedNodes = {}, excludedNodes = {}"
              + ", foundNodes = {}, nodes = {}.", hostConfigManager.getIncludes(),
          hostConfigManager.getExcludes(), foundNodes, nodes);
    }
    return nodes;
  }

  public List<DatanodeDescriptor> getAllSlowDataNodes() {
    if (slowPeerTracker == null) {
      LOG.debug("{} is disabled. Try enabling it first to capture slow peer outliers.",
          DFSConfigKeys.DFS_DATANODE_PEER_STATS_ENABLED_KEY);
      return ImmutableList.of();
    }
    List<String> slowNodes = slowPeerTracker.getSlowNodes(getNumOfDataNodes());
    return getDnDescriptorsFromIpAddr(slowNodes);
  }

  /**
   * Checks if name resolution was successful for the given address.  If IP
   * address and host name are the same, then it means name resolution has
   * failed.  As a special case, local addresses are also considered
   * acceptable.  This is particularly important on Windows, where 127.0.0.1 does
   * not resolve to "localhost".
   *
   * @param address InetAddress to check
   * @return boolean true if name resolution successful or address is local
   */
  private static boolean isNameResolved(InetAddress address) {
    String hostname = address.getHostName();
    String ip = address.getHostAddress();
    return !hostname.equals(ip) || NetUtils.isLocalAddress(address);
  }
  
  private void setDatanodeDead(DatanodeDescriptor node) {
    node.setLastUpdate(0);
    node.setLastUpdateMonotonic(0);
  }

  private BlockRecoveryCommand getBlockRecoveryCommand(String blockPoolId,
      DatanodeDescriptor nodeinfo) throws IOException {
    BlockInfo[] blocks = nodeinfo.getLeaseRecoveryCommand(Integer.MAX_VALUE);
    if (blocks == null) {
      return null;
    }
    BlockRecoveryCommand brCommand = new BlockRecoveryCommand(blocks.length);
    for (BlockInfo b : blocks) {
      BlockUnderConstructionFeature uc = b.getUnderConstructionFeature();
      if(uc == null) {
        throw new IOException("Recovery block " + b +
            " where it is not under construction.");
      }
      final DatanodeStorageInfo[] storages = uc.getExpectedStorageLocations();
      // Skip stale nodes during recovery
      final List<DatanodeStorageInfo> recoveryLocations =
          new ArrayList<>(storages.length);
      final List<Integer> storageIdx = new ArrayList<>(storages.length);
      for (int i = 0; i < storages.length; ++i) {
        if (!storages[i].getDatanodeDescriptor().isStale(staleInterval)) {
          recoveryLocations.add(storages[i]);
          storageIdx.add(i);
        }
      }
      // If we are performing a truncate recovery than set recovery fields
      // to old block.
      boolean truncateRecovery = uc.getTruncateBlock() != null;
      boolean copyOnTruncateRecovery = truncateRecovery &&
          uc.getTruncateBlock().getBlockId() != b.getBlockId();
      ExtendedBlock primaryBlock = (copyOnTruncateRecovery) ?
          new ExtendedBlock(blockPoolId, uc.getTruncateBlock()) :
          new ExtendedBlock(blockPoolId, b);
      // If we only get 1 replica after eliminating stale nodes, choose all
      // replicas for recovery and let the primary data node handle failures.
      DatanodeInfo[] recoveryInfos;
      if (recoveryLocations.size() > 1) {
        if (recoveryLocations.size() != storages.length) {
          LOG.info("Skipped stale nodes for recovery : "
              + (storages.length - recoveryLocations.size()));
        }
        recoveryInfos = DatanodeStorageInfo.toDatanodeInfos(recoveryLocations);
      } else {
        // If too many replicas are stale, then choose all replicas to
        // participate in block recovery.
        recoveryInfos = DatanodeStorageInfo.toDatanodeInfos(storages);
      }
      RecoveringBlock rBlock;
      if (truncateRecovery) {
        Block recoveryBlock = (copyOnTruncateRecovery) ? b : uc.getTruncateBlock();
        rBlock = new RecoveringBlock(primaryBlock, recoveryInfos, recoveryBlock);
      } else {
        rBlock = new RecoveringBlock(primaryBlock, recoveryInfos,
            uc.getBlockRecoveryId());
        if (b.isStriped()) {
          rBlock = new RecoveringStripedBlock(rBlock,
              uc.getBlockIndicesForSpecifiedStorages(storageIdx),
              ((BlockInfoStriped) b).getErasureCodingPolicy());
        }
      }
      brCommand.add(rBlock);
    }
    return brCommand;
  }

  private void addCacheCommands(String blockPoolId, DatanodeDescriptor nodeinfo,
      List<DatanodeCommand> cmds) {
    boolean sendingCachingCommands = false;
    final long nowMs = monotonicNow();
    if (shouldSendCachingCommands &&
        ((nowMs - nodeinfo.getLastCachingDirectiveSentTimeMs()) >=
            timeBetweenResendingCachingDirectivesMs)) {
      DatanodeCommand pendingCacheCommand = getCacheCommand(
          nodeinfo.getPendingCached(), DatanodeProtocol.DNA_CACHE,
          blockPoolId);
      if (pendingCacheCommand != null) {
        cmds.add(pendingCacheCommand);
        sendingCachingCommands = true;
      }
      DatanodeCommand pendingUncacheCommand = getCacheCommand(
          nodeinfo.getPendingUncached(), DatanodeProtocol.DNA_UNCACHE,
          blockPoolId);
      if (pendingUncacheCommand != null) {
        cmds.add(pendingUncacheCommand);
        sendingCachingCommands = true;
      }
      if (sendingCachingCommands) {
        nodeinfo.setLastCachingDirectiveSentTimeMs(nowMs);
      }
    }
  }

  /** Handle heartbeat from datanodes. */
  public DatanodeCommand[] handleHeartbeat(DatanodeRegistration nodeReg,
      StorageReport[] reports, final String blockPoolId,
      long cacheCapacity, long cacheUsed, int xceiverCount,
      int xmitsInProgress, int failedVolumes,
      VolumeFailureSummary volumeFailureSummary,
      @Nonnull SlowPeerReports slowPeers,
      @Nonnull SlowDiskReports slowDisks) throws IOException {
    final DatanodeDescriptor nodeinfo;
    try {
      nodeinfo = getDatanode(nodeReg);
    } catch (UnregisteredNodeException e) {
      return new DatanodeCommand[]{RegisterCommand.REGISTER};
    }

    // Check if this datanode should actually be shutdown instead.
    if (nodeinfo != null && nodeinfo.isDisallowed()) {
      setDatanodeDead(nodeinfo);
      throw new DisallowedDatanodeException(nodeinfo);
    }

    if (nodeinfo == null || !nodeinfo.isRegistered()) {
      return new DatanodeCommand[]{RegisterCommand.REGISTER};
    }
    heartbeatManager.updateHeartbeat(nodeinfo, reports, cacheCapacity,
        cacheUsed, xceiverCount, failedVolumes, volumeFailureSummary);

    // If we are in safemode, do not send back any recovery / replication
    // requests. Don't even drain the existing queue of work.
    if (namesystem.isInSafeMode()) {
      return new DatanodeCommand[0];
    }

    // block recovery command
    final BlockRecoveryCommand brCommand = getBlockRecoveryCommand(blockPoolId,
        nodeinfo);
    if (brCommand != null) {
      return new DatanodeCommand[]{brCommand};
    }

    final List<DatanodeCommand> cmds = new ArrayList<>();
    // Allocate _approximately_ maxTransfers pending tasks to DataNode.
    // NN chooses pending tasks based on the ratio between the lengths of
    // replication and erasure-coded block queues.
    int replicationBlocks = nodeinfo.getNumberOfReplicateBlocks();
    int ecBlocksToBeReplicated = nodeinfo.getNumberOfECBlocksToBeReplicated();
    int ecBlocksToBeErasureCoded = nodeinfo.getNumberOfBlocksToBeErasureCoded();
    int totalBlocks = replicationBlocks + ecBlocksToBeReplicated + ecBlocksToBeErasureCoded;
    if (totalBlocks > 0) {
      int maxTransfers = blockManager.getMaxReplicationStreams() - xmitsInProgress;
      int maxECReplicatedTransfers;
      if (nodeinfo.isDecommissionInProgress()) {
        maxECReplicatedTransfers = blockManager.getReplicationStreamsHardLimit()
            - xmitsInProgress;
      } else {
        maxECReplicatedTransfers = maxTransfers;
      }
      int numReplicationTasks = (int) Math.ceil(
          (double) (replicationBlocks * maxTransfers) / totalBlocks);
      int numEcReplicatedTasks = (int) Math.ceil(
              (double) (ecBlocksToBeReplicated * maxECReplicatedTransfers) / totalBlocks);
      int numECReconstructedTasks = (int) Math.ceil(
          (double) (ecBlocksToBeErasureCoded * maxTransfers) / totalBlocks);
      LOG.debug("Pending replication tasks: {} ec to be replicated tasks: {} " +
                      "ec reconstruction tasks: {}.",
          numReplicationTasks, numEcReplicatedTasks, numECReconstructedTasks);
      // check pending replication tasks
      List<BlockTargetPair> pendingReplicationList = nodeinfo.getReplicationCommand(
          numReplicationTasks);
      List<BlockTargetPair> pendingECReplicatedList = nodeinfo.getECReplicatedCommand(
              numEcReplicatedTasks);
      List<BlockTargetPair> pendingList = new ArrayList<BlockTargetPair>();
      if(pendingReplicationList != null && !pendingReplicationList.isEmpty()) {
        pendingList.addAll(pendingReplicationList);
      }
      if(pendingECReplicatedList != null && !pendingECReplicatedList.isEmpty()) {
        pendingList.addAll(pendingECReplicatedList);
      }
      if (!pendingList.isEmpty()) {
        // If the block is deleted, the block size will become
        // BlockCommand.NO_ACK (LONG.MAX_VALUE) . This kind of block we don't
        // need
        // to send for replication or reconstruction
        Iterator<BlockTargetPair> iterator = pendingList.iterator();
        while (iterator.hasNext()) {
          BlockTargetPair cmd = iterator.next();
          if (cmd.block != null
              && cmd.block.getNumBytes() == BlockCommand.NO_ACK) {
            // block deleted
            DatanodeStorageInfo.decrementBlocksScheduled(cmd.targets);
            iterator.remove();
          }
        }
        if (!pendingList.isEmpty()) {
          cmds.add(new BlockCommand(DatanodeProtocol.DNA_TRANSFER, blockPoolId,
              pendingList));
        }
      }
      // check pending erasure coding tasks
      List<BlockECReconstructionInfo> pendingECList = nodeinfo
          .getErasureCodeCommand(numECReconstructedTasks);
      if (pendingECList != null && !pendingECList.isEmpty()) {
        cmds.add(new BlockECReconstructionCommand(
            DNA_ERASURE_CODING_RECONSTRUCTION, pendingECList));
      }
    }

    // check block invalidation
    Block[] blks = nodeinfo.getInvalidateBlocks(blockInvalidateLimit);
    if (blks != null) {
      cmds.add(new BlockCommand(DatanodeProtocol.DNA_INVALIDATE, blockPoolId,
          blks));
    }
    // cache commands
    addCacheCommands(blockPoolId, nodeinfo, cmds);
    // key update command
    blockManager.addKeyUpdateCommand(cmds, nodeinfo);

    // check for balancer bandwidth update
    if (nodeinfo.getBalancerBandwidth() > 0) {
      cmds.add(new BalancerBandwidthCommand(nodeinfo.getBalancerBandwidth()));
      // set back to 0 to indicate that datanode has been sent the new value
      nodeinfo.setBalancerBandwidth(0);
    }

    Preconditions.checkNotNull(slowPeerTracker, "slowPeerTracker should not be un-assigned");

    if (slowPeerTracker.isSlowPeerTrackerEnabled()) {
      final Map<String, OutlierMetrics> slowPeersMap = slowPeers.getSlowPeers();
      if (!slowPeersMap.isEmpty()) {
        LOG.debug("DataNode {} reported slow peers: {}.", nodeReg, slowPeersMap);
        for (Map.Entry<String, OutlierMetrics> slowNodeEntry : slowPeersMap.entrySet()) {
          slowPeerTracker.addReport(slowNodeEntry.getKey(), nodeReg.getIpcAddr(false),
              slowNodeEntry.getValue());
        }
      }
    }

    if (slowDiskTracker != null) {
      if (!slowDisks.getSlowDisks().isEmpty()) {
        LOG.debug("DataNode {} reported slow disks: {}.", nodeReg, slowDisks.getSlowDisks());
        slowDiskTracker.addSlowDiskReport(nodeReg.getIpcAddr(false), slowDisks);
      }
      slowDiskTracker.checkAndUpdateReportIfNecessary();
    }

    if (!cmds.isEmpty()) {
      return cmds.toArray(new DatanodeCommand[cmds.size()]);
    }

    return new DatanodeCommand[0];
  }

  /**
   * Handles a lifeline message sent by a DataNode.
   *
   * @param nodeReg registration info for DataNode sending the lifeline
   * @param reports storage reports from DataNode
   * @param cacheCapacity cache capacity at DataNode
   * @param cacheUsed cache used at DataNode
   * @param xceiverCount estimated count of transfer threads running at DataNode
   * @param failedVolumes count of failed volumes at DataNode
   * @param volumeFailureSummary info on failed volumes at DataNode
   * @throws IOException if there is an error
   */
  public void handleLifeline(DatanodeRegistration nodeReg,
      StorageReport[] reports, long cacheCapacity,
      long cacheUsed, int xceiverCount, int failedVolumes,
      VolumeFailureSummary volumeFailureSummary) throws IOException {
    LOG.debug("Received handleLifeline from nodeReg = {}.", nodeReg);
    DatanodeDescriptor nodeinfo = getDatanode(nodeReg);
    if (nodeinfo == null || !nodeinfo.isRegistered()) {
      // This can happen if the lifeline message comes when DataNode is either
      // not registered at all or its marked dead at NameNode and expectes
      // re-registration. Ignore lifeline messages without registration.
      // Lifeline message handling can't send commands back to the DataNode to
      // tell it to register, so simply exit.
      return;
    }
    if (nodeinfo.isDisallowed()) {
      // This is highly unlikely, because heartbeat handling is much more
      // frequent and likely would have already sent the disallowed error.
      // Lifeline messages are not intended to send any kind of control response
      // back to the DataNode, so simply exit.
      return;
    }
    heartbeatManager.updateLifeline(nodeinfo, reports, cacheCapacity, cacheUsed,
        xceiverCount, failedVolumes, volumeFailureSummary);
  }

  /**
   * Convert a CachedBlockList into a DatanodeCommand with a list of blocks.
   *
   * @param list       The {@link CachedBlocksList}.  This function 
   *                   clears the list.
   * @param action     The action to perform in the command.
   * @param poolId     The block pool id.
   * @return           A DatanodeCommand to be sent back to the DN, or null if
   *                   there is nothing to be done.
   */
  private DatanodeCommand getCacheCommand(CachedBlocksList list, int action,
      String poolId) {
    int length = list.size();
    if (length == 0) {
      return null;
    }
    // Read the existing cache commands.
    long[] blockIds = new long[length];
    int i = 0;
    for (CachedBlock cachedBlock : list) {
      blockIds[i++] = cachedBlock.getBlockId();
    }
    return new BlockIdCommand(action, poolId, blockIds);
  }

  /**
   * Tell all datanodes to use a new, non-persistent bandwidth value for
   * dfs.datanode.balance.bandwidthPerSec.
   *
   * A system administrator can tune the balancer bandwidth parameter
   * (dfs.datanode.balance.bandwidthPerSec) dynamically by calling
   * "dfsadmin -setBalanacerBandwidth newbandwidth", at which point the
   * following 'bandwidth' variable gets updated with the new value for each
   * node. Once the heartbeat command is issued to update the value on the
   * specified datanode, this value will be set back to 0.
   *
   * @param bandwidth Blanacer bandwidth in bytes per second for all datanodes.
   * @throws IOException
   */
  public void setBalancerBandwidth(long bandwidth) throws IOException {
    synchronized(this) {
      for (DatanodeDescriptor nodeInfo : datanodeMap.values()) {
        nodeInfo.setBalancerBandwidth(bandwidth);
      }
    }
  }
  
  public void markAllDatanodesStale() {
    LOG.info("Marking all datanodes as stale");
    synchronized (this) {
      for (DatanodeDescriptor dn : datanodeMap.values()) {
        for(DatanodeStorageInfo storage : dn.getStorageInfos()) {
          storage.markStaleAfterFailover();
        }
      }
    }
  }

  /**
   * Clear any actions that are queued up to be sent to the DNs
   * on their next heartbeats. This includes block invalidations,
   * recoveries, and replication requests.
   */
  public void clearPendingQueues() {
    synchronized (this) {
      for (DatanodeDescriptor dn : datanodeMap.values()) {
        dn.clearBlockQueues();
      }
    }
  }

  /**
   * Reset the lastCachingDirectiveSentTimeMs field of all the DataNodes we
   * know about.
   */
  public void resetLastCachingDirectiveSentTime() {
    synchronized (this) {
      for (DatanodeDescriptor dn : datanodeMap.values()) {
        dn.setLastCachingDirectiveSentTimeMs(0L);
      }
    }
  }

  @Override
  public String toString() {
    return getClass().getSimpleName() + ": " + host2DatanodeMap;
  }

  public void clearPendingCachingCommands() {
    synchronized (this) {
      for (DatanodeDescriptor dn : datanodeMap.values()) {
        dn.getPendingCached().clear();
        dn.getPendingUncached().clear();
      }
    }
  }

  public void setShouldSendCachingCommands(boolean shouldSendCachingCommands) {
    this.shouldSendCachingCommands = shouldSendCachingCommands;
  }

  FSClusterStats newFSClusterStats() {
    return new FSClusterStats() {
      @Override
      public int getTotalLoad() {
        return heartbeatManager.getXceiverCount();
      }

      @Override
      public boolean isAvoidingStaleDataNodesForWrite() {
        return shouldAvoidStaleDataNodesForWrite();
      }

      @Override
      public int getNumDatanodesInService() {
        return heartbeatManager.getNumDatanodesInService();
      }

      @Override
      public double getInServiceXceiverAverage() {
        double avgLoad = 0;
        final int nodes = getNumDatanodesInService();
        if (nodes != 0) {
          final int xceivers = heartbeatManager
            .getInServiceXceiverCount();
          avgLoad = (double)xceivers/nodes;
        }
        return avgLoad;
      }

      @Override
      public double getInServiceXceiverAverageForVolume() {
        double avgLoad = 0;
        final int volumes = heartbeatManager.getInServiceAvailableVolumeCount();
        if (volumes > 0) {
          final long xceivers = heartbeatManager.getInServiceXceiverCount();
          avgLoad = (double)xceivers/volumes;
        }
        return avgLoad;
      }

      @Override
      public Map<StorageType, StorageTypeStats> getStorageTypeStats() {
        return heartbeatManager.getStorageTypeStats();
      }
    };
  }

  public void setHeartbeatInterval(long intervalSeconds) {
    setHeartbeatInterval(intervalSeconds,
        this.heartbeatRecheckInterval);
  }

  public void setHeartbeatRecheckInterval(int recheckInterval) {
    setHeartbeatInterval(this.heartbeatIntervalSeconds,
        recheckInterval);
  }

  /**
   * Set parameters derived from heartbeat interval.
   */
  private void setHeartbeatInterval(long intervalSeconds,
      int recheckInterval) {
    this.heartbeatIntervalSeconds = intervalSeconds;
    this.heartbeatRecheckInterval = recheckInterval;
    this.heartbeatExpireInterval = 2L * recheckInterval + 10 * 1000
        * intervalSeconds;
    this.blockInvalidateLimit = getBlockInvalidateLimit(blockInvalidateLimit);
  }

  private int getBlockInvalidateLimitFromHBInterval() {
    return 20 * (int) heartbeatIntervalSeconds;
  }

  private int getBlockInvalidateLimit(int configuredBlockInvalidateLimit) {
    return Math.max(getBlockInvalidateLimitFromHBInterval(), configuredBlockInvalidateLimit);
  }

  public void setBlockInvalidateLimit(int configuredBlockInvalidateLimit) {
    final int countedBlockInvalidateLimit = getBlockInvalidateLimitFromHBInterval();
    // Effected block invalidate limit is the bigger value between
    // value configured in hdfs-site.xml, and 20 * HB interval.
    this.blockInvalidateLimit = getBlockInvalidateLimit(configuredBlockInvalidateLimit);
    LOG.info("{} : configured={}, counted={}, effected={}",
        DFSConfigKeys.DFS_BLOCK_INVALIDATE_LIMIT_KEY, configuredBlockInvalidateLimit,
        countedBlockInvalidateLimit, this.blockInvalidateLimit);
  }

  /**
   * Retrieve information about slow peers as a JSON.
   * Returns null if we are not tracking slow peers.
   * @return
   */
  public String getSlowPeersReport() {
    Preconditions.checkNotNull(slowPeerTracker, "slowPeerTracker should not be un-assigned");
    return slowPeerTracker.getJson();
  }

  /**
   * Returns all tracking slow peers.
   * @return
   */
  public Set<String> getSlowPeersUuidSet() {
    Set<String> slowPeersUuidSet = Sets.newConcurrentHashSet();
    List<String> slowNodes;
    Preconditions.checkNotNull(slowPeerTracker, "slowPeerTracker should not be un-assigned");
    slowNodes = slowPeerTracker.getSlowNodes(maxSlowPeerReportNodes);
    List<DatanodeDescriptor> datanodeDescriptors = getDnDescriptorsFromIpAddr(slowNodes);
    datanodeDescriptors.forEach(
        datanodeDescriptor -> slowPeersUuidSet.add(datanodeDescriptor.getDatanodeUuid()));
    return slowPeersUuidSet;
  }

  private List<DatanodeDescriptor> getDnDescriptorsFromIpAddr(List<String> nodes) {
    List<DatanodeDescriptor> datanodeDescriptors = new ArrayList<>();
    for (String node : nodes) {
      if (StringUtils.isBlank(node) || !node.contains(IP_PORT_SEPARATOR)) {
        continue;
      }
      String ipAddr = node.split(IP_PORT_SEPARATOR)[0];
      DatanodeDescriptor datanodeByHost =
          host2DatanodeMap.getDatanodeByHost(ipAddr);
      if (datanodeByHost != null) {
        datanodeDescriptors.add(datanodeByHost);
      }
    }
    return datanodeDescriptors;
  }

  /**
   * Returns all tracking slow datanodes uuids.
   * @return
   */
  public static Set<String> getSlowNodesUuidSet() {
    return slowNodesUuidSet;
  }

  /**
   * Use only for testing.
   */
  @VisibleForTesting
  public SlowPeerTracker getSlowPeerTracker() {
    return slowPeerTracker;
  }

  /**
   * Use only for testing.
   */
  @VisibleForTesting
  public SlowDiskTracker getSlowDiskTracker() {
    return slowDiskTracker;
  }

  @VisibleForTesting
  public void addSlowPeers(String dnUuid) {
    slowNodesUuidSet.add(dnUuid);
  }

  /**
   * Retrieve information about slow disks as a JSON.
   * Returns null if we are not tracking slow disks.
   * @return
   */
  public String getSlowDisksReport() {
    return slowDiskTracker != null ?
        slowDiskTracker.getSlowDiskReportAsJsonString() : null;
  }

  /**
   * Generates datanode reports for the given report type.
   *
   * @param type
   *          type of the datanode report
   * @return array of DatanodeStorageReports
   */
  public DatanodeStorageReport[] getDatanodeStorageReport(
      DatanodeReportType type) {
    final List<DatanodeDescriptor> datanodes = getDatanodeListForReport(type);

    DatanodeStorageReport[] reports = new DatanodeStorageReport[datanodes
        .size()];
    for (int i = 0; i < reports.length; i++) {
      final DatanodeDescriptor d = datanodes.get(i);
      reports[i] = new DatanodeStorageReport(
          new DatanodeInfoBuilder().setFrom(d).setNumBlocks(d.numBlocks()).build(),
          d.getStorageReports());
    }
    return reports;
  }

  @VisibleForTesting
  public Map<String, DatanodeDescriptor> getDatanodeMap() {
    return datanodeMap;
  }

  public void setMaxSlowPeersToReport(int maxSlowPeersToReport) {
    Preconditions.checkNotNull(slowPeerTracker, "slowPeerTracker should not be un-assigned");
    slowPeerTracker.setMaxSlowPeersToReport(maxSlowPeersToReport);
  }

  @VisibleForTesting
  public boolean isSlowPeerCollectorInitialized() {
    return slowPeerCollectorDaemon == null;
  }
}
