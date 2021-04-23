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
package org.apache.hadoop.hdfs.server.federation.metrics;

import static org.apache.hadoop.util.Time.now;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.management.NotCompliantMBeanException;
import javax.management.ObjectName;
import javax.management.StandardMBean;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ha.HAServiceProtocol.HAServiceState;
import org.apache.hadoop.hdfs.DFSUtilClient;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.DatanodeReportType;
import org.apache.hadoop.hdfs.protocol.RollingUpgradeInfo;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.NamenodeRole;
import org.apache.hadoop.hdfs.server.federation.resolver.FederationNamespaceInfo;
import org.apache.hadoop.hdfs.server.federation.router.RBFConfigKeys;
import org.apache.hadoop.hdfs.server.federation.router.Router;
import org.apache.hadoop.hdfs.server.federation.router.RouterRpcServer;
import org.apache.hadoop.hdfs.server.federation.router.SubClusterTimeoutException;
import org.apache.hadoop.hdfs.server.federation.store.MembershipStore;
import org.apache.hadoop.hdfs.server.federation.store.StateStoreService;
import org.apache.hadoop.hdfs.server.federation.store.protocol.GetNamespaceInfoRequest;
import org.apache.hadoop.hdfs.server.federation.store.protocol.GetNamespaceInfoResponse;
import org.apache.hadoop.hdfs.server.namenode.NameNodeMXBean;
import org.apache.hadoop.hdfs.server.namenode.NameNodeStatusMXBean;
import org.apache.hadoop.hdfs.server.namenode.metrics.FSNamesystemMBean;
import org.apache.hadoop.ipc.StandbyException;
import org.apache.hadoop.metrics2.util.MBeans;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.util.VersionInfo;
import org.eclipse.jetty.util.ajax.JSON;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.thirdparty.com.google.common.cache.CacheBuilder;
import org.apache.hadoop.thirdparty.com.google.common.cache.CacheLoader;
import org.apache.hadoop.thirdparty.com.google.common.cache.LoadingCache;

/**
 * Expose the Namenode metrics as the Router was one.
 */
public class NamenodeBeanMetrics
    implements FSNamesystemMBean, NameNodeMXBean, NameNodeStatusMXBean {

  private static final Logger LOG =
      LoggerFactory.getLogger(NamenodeBeanMetrics.class);

  /** Instance of the Router being monitored. */
  private final Router router;

  /** FSNamesystem bean. */
  private ObjectName fsBeanName;
  /** FSNamesystemState bean. */
  private ObjectName fsStateBeanName;
  /** NameNodeInfo bean. */
  private ObjectName nnInfoBeanName;
  /** NameNodeStatus bean. */
  private ObjectName nnStatusBeanName;

  /** Timeout to get the DN report. */
  private final long dnReportTimeOut;
  /** DN type -> full DN report in JSON. */
  private final LoadingCache<DatanodeReportType, String> dnCache;


  public NamenodeBeanMetrics(Router router) {
    this.router = router;

    try {
      // TODO this needs to be done with the Metrics from FSNamesystem
      StandardMBean bean = new StandardMBean(this, FSNamesystemMBean.class);
      this.fsBeanName = MBeans.register("NameNode", "FSNamesystem", bean);
      LOG.info("Registered FSNamesystem MBean: {}", this.fsBeanName);
    } catch (NotCompliantMBeanException e) {
      throw new RuntimeException("Bad FSNamesystem MBean setup", e);
    }

    try {
      StandardMBean bean = new StandardMBean(this, FSNamesystemMBean.class);
      this.fsStateBeanName =
          MBeans.register("NameNode", "FSNamesystemState", bean);
      LOG.info("Registered FSNamesystemState MBean: {}", this.fsStateBeanName);
    } catch (NotCompliantMBeanException e) {
      throw new RuntimeException("Bad FSNamesystemState MBean setup", e);
    }

    try {
      StandardMBean bean = new StandardMBean(this, NameNodeMXBean.class);
      this.nnInfoBeanName = MBeans.register("NameNode", "NameNodeInfo", bean);
      LOG.info("Registered NameNodeInfo MBean: {}", this.nnInfoBeanName);
    } catch (NotCompliantMBeanException e) {
      throw new RuntimeException("Bad NameNodeInfo MBean setup", e);
    }

    try {
      StandardMBean bean = new StandardMBean(this, NameNodeStatusMXBean.class);
      this.nnStatusBeanName =
          MBeans.register("NameNode", "NameNodeStatus", bean);
      LOG.info("Registered NameNodeStatus MBean: {}", this.nnStatusBeanName);
    } catch (NotCompliantMBeanException e) {
      throw new RuntimeException("Bad NameNodeStatus MBean setup", e);
    }

    // Initialize the cache for the DN reports
    Configuration conf = router.getConfig();
    this.dnReportTimeOut = conf.getTimeDuration(
        RBFConfigKeys.DN_REPORT_TIME_OUT,
        RBFConfigKeys.DN_REPORT_TIME_OUT_MS_DEFAULT, TimeUnit.MILLISECONDS);
    long dnCacheExpire = conf.getTimeDuration(
        RBFConfigKeys.DN_REPORT_CACHE_EXPIRE,
        RBFConfigKeys.DN_REPORT_CACHE_EXPIRE_MS_DEFAULT, TimeUnit.MILLISECONDS);
    this.dnCache = CacheBuilder.newBuilder()
        .expireAfterWrite(dnCacheExpire, TimeUnit.MILLISECONDS)
        .build(
            new CacheLoader<DatanodeReportType, String>() {
              @Override
              public String load(DatanodeReportType type) throws Exception {
                return getNodesImpl(type);
              }
            });
  }

  /**
   * De-register the JMX interfaces.
   */
  public void close() {
    if (fsStateBeanName != null) {
      MBeans.unregister(fsStateBeanName);
      fsStateBeanName = null;
    }
    if (nnInfoBeanName != null) {
      MBeans.unregister(nnInfoBeanName);
      nnInfoBeanName = null;
    }
    // Remove the NameNode status bean
    if (nnStatusBeanName != null) {
      MBeans.unregister(nnStatusBeanName);
      nnStatusBeanName = null;
    }
  }

  private RBFMetrics getRBFMetrics() throws IOException {
    RBFMetrics metrics = getRouter().getMetrics();
    if (metrics == null) {
      throw new IOException("Federated metrics is not initialized");
    }
    return metrics;
  }

  /////////////////////////////////////////////////////////
  // NameNodeMXBean
  /////////////////////////////////////////////////////////

  @Override
  public String getVersion() {
    return VersionInfo.getVersion() + ", r" + VersionInfo.getRevision();
  }

  @Override
  public String getSoftwareVersion() {
    return VersionInfo.getVersion();
  }

  @Override
  public long getUsed() {
    try {
      return getRBFMetrics().getUsedCapacity();
    } catch (IOException e) {
      LOG.debug("Failed to get the used capacity", e.getMessage());
    }
    return 0;
  }

  @Override
  public long getFree() {
    try {
      return getRBFMetrics().getRemainingCapacity();
    } catch (IOException e) {
      LOG.debug("Failed to get remaining capacity", e.getMessage());
    }
    return 0;
  }

  @Override
  public long getTotal() {
    try {
      return getRBFMetrics().getTotalCapacity();
    } catch (IOException e) {
      LOG.debug("Failed to Get total capacity", e.getMessage());
    }
    return 0;
  }

  @Override
  public long getProvidedCapacity() {
    try {
      return getRBFMetrics().getProvidedSpace();
    } catch (IOException e) {
      LOG.debug("Failed to get provided capacity", e.getMessage());
    }
    return 0;
  }

  @Override
  public String getSafemode() {
    try {
      return getRBFMetrics().getSafemode();
    } catch (IOException e) {
      return "Failed to get safemode status. Please check router"
          + "log for more detail.";
    }
  }

  @Override
  public boolean isUpgradeFinalized() {
    // We assume the upgrade is always finalized in a federated biew
    return true;
  }

  @Override
  public RollingUpgradeInfo.Bean getRollingUpgradeStatus() {
    return null;
  }

  @Override
  public long getNonDfsUsedSpace() {
    return 0;
  }

  @Override
  public float getPercentUsed() {
    return DFSUtilClient.getPercentUsed(getCapacityUsed(), getCapacityTotal());
  }

  @Override
  public float getPercentRemaining() {
    return DFSUtilClient.getPercentUsed(
        getCapacityRemaining(), getCapacityTotal());
  }

  @Override
  public long getCacheUsed() {
    return 0;
  }

  @Override
  public long getCacheCapacity() {
    return 0;
  }

  @Override
  public long getBlockPoolUsedSpace() {
    return 0;
  }

  @Override
  public float getPercentBlockPoolUsed() {
    return 0;
  }

  @Override
  public long getTotalBlocks() {
    try {
      return getRBFMetrics().getNumBlocks();
    } catch (IOException e) {
      LOG.debug("Failed to get number of blocks", e.getMessage());
    }
    return 0;
  }

  @Override
  public long getNumberOfMissingBlocks() {
    try {
      return getRBFMetrics().getNumOfMissingBlocks();
    } catch (IOException e) {
      LOG.debug("Failed to get number of missing blocks", e.getMessage());
    }
    return 0;
  }

  @Override
  @Deprecated
  public long getPendingReplicationBlocks() {
    try {
      return getRBFMetrics().getNumOfBlocksPendingReplication();
    } catch (IOException e) {
      LOG.debug("Failed to get number of blocks pending replica",
          e.getMessage());
    }
    return 0;
  }

  @Override
  public long getPendingReconstructionBlocks() {
    try {
      return getRBFMetrics().getNumOfBlocksPendingReplication();
    } catch (IOException e) {
      LOG.debug("Failed to get number of blocks pending replica",
          e.getMessage());
    }
    return 0;
  }

  @Override
  @Deprecated
  public long getUnderReplicatedBlocks() {
    try {
      return getRBFMetrics().getNumOfBlocksUnderReplicated();
    } catch (IOException e) {
      LOG.debug("Failed to get number of blocks under replicated",
          e.getMessage());
    }
    return 0;
  }

  @Override
  public long getLowRedundancyBlocks() {
    try {
      return getRBFMetrics().getNumOfBlocksUnderReplicated();
    } catch (IOException e) {
      LOG.debug("Failed to get number of blocks under replicated",
          e.getMessage());
    }
    return 0;
  }

  @Override
  public long getPendingDeletionBlocks() {
    try {
      return getRBFMetrics().getNumOfBlocksPendingDeletion();
    } catch (IOException e) {
      LOG.debug("Failed to get number of blocks pending deletion",
          e.getMessage());
    }
    return 0;
  }

  @Override
  public long getScheduledReplicationBlocks() {
    return -1;
  }

  @Override
  public long getNumberOfMissingBlocksWithReplicationFactorOne() {
    return 0;
  }

  @Override
  public long getHighestPriorityLowRedundancyReplicatedBlocks() {
    return 0;
  }

  @Override
  public long getHighestPriorityLowRedundancyECBlocks() {
    return 0;
  }

  @Override
  public String getCorruptFiles() {
    return "N/A";
  }

  @Override
  public int getCorruptFilesCount() {
    return 0;
  }

  @Override
  public int getThreads() {
    return ManagementFactory.getThreadMXBean().getThreadCount();
  }

  @Override
  public String getLiveNodes() {
    return this.getNodes(DatanodeReportType.LIVE);
  }

  @Override
  public String getDeadNodes() {
    return this.getNodes(DatanodeReportType.DEAD);
  }

  @Override
  public String getDecomNodes() {
    return this.getNodes(DatanodeReportType.DECOMMISSIONING);
  }

  /**
   * Get all the nodes in the federation from a particular type. Getting this
   * information is expensive and we use a cache.
   * @param type Type of the datanodes to check.
   * @return JSON with the nodes.
   */
  private String getNodes(final DatanodeReportType type) {
    try {
      return this.dnCache.get(type);
    } catch (ExecutionException e) {
      LOG.error("Cannot get the DN storage report for {}", type, e);
    }
    // If we cannot get the report, return empty JSON
    return "{}";
  }

  /**
   * Get all the nodes in the federation from a particular type.
   * @param type Type of the datanodes to check.
   * @return JSON with the nodes.
   */
  private String getNodesImpl(final DatanodeReportType type) {
    final Map<String, Map<String, Object>> info = new HashMap<>();
    try {
      RouterRpcServer rpcServer = this.router.getRpcServer();
      DatanodeInfo[] datanodes =
          rpcServer.getDatanodeReport(type, false, dnReportTimeOut);
      for (DatanodeInfo node : datanodes) {
        Map<String, Object> innerinfo = new HashMap<>();
        innerinfo.put("infoAddr", node.getInfoAddr());
        innerinfo.put("infoSecureAddr", node.getInfoSecureAddr());
        innerinfo.put("xferaddr", node.getXferAddr());
        innerinfo.put("location", node.getNetworkLocation());
        innerinfo.put("lastContact", getLastContact(node));
        innerinfo.put("usedSpace", node.getDfsUsed());
        innerinfo.put("adminState", node.getAdminState().toString());
        innerinfo.put("nonDfsUsedSpace", node.getNonDfsUsed());
        innerinfo.put("capacity", node.getCapacity());
        innerinfo.put("numBlocks", -1); // node.numBlocks()
        innerinfo.put("version", (node.getSoftwareVersion() == null ?
                        "UNKNOWN" : node.getSoftwareVersion()));
        innerinfo.put("used", node.getDfsUsed());
        innerinfo.put("remaining", node.getRemaining());
        innerinfo.put("blockScheduled", -1); // node.getBlocksScheduled()
        innerinfo.put("blockPoolUsed", node.getBlockPoolUsed());
        innerinfo.put("blockPoolUsedPercent", node.getBlockPoolUsedPercent());
        innerinfo.put("volfails", -1); // node.getVolumeFailures()
        info.put(node.getHostName() + ":" + node.getXferPort(),
            Collections.unmodifiableMap(innerinfo));
      }
    } catch (StandbyException e) {
      LOG.error("Cannot get {} nodes, Router in safe mode", type);
    } catch (SubClusterTimeoutException e) {
      LOG.error("Cannot get {} nodes, subclusters timed out responding", type);
    } catch (IOException e) {
      LOG.error("Cannot get " + type + " nodes", e);
    }
    return JSON.toString(info);
  }

  @Override
  public String getClusterId() {
    try {
      return getNamespaceInfo(FederationNamespaceInfo::getClusterId).toString();
    } catch (IOException e) {
      LOG.error("Cannot fetch cluster ID metrics {}", e.getMessage());
      return "";
    }
  }

  @Override
  public String getBlockPoolId() {
    try {
      return
          getNamespaceInfo(FederationNamespaceInfo::getBlockPoolId).toString();
    } catch (IOException e) {
      LOG.error("Cannot fetch block pool ID metrics {}", e.getMessage());
      return "";
    }
  }

  /**
   * Build a set of unique values found in all namespaces.
   *
   * @param f Method reference of the appropriate FederationNamespaceInfo
   *          getter function
   * @return Set of unique string values found in all discovered namespaces.
   * @throws IOException if the query could not be executed.
   */
  private Collection<String> getNamespaceInfo(
      Function<FederationNamespaceInfo, String> f) throws IOException {
    StateStoreService stateStore = router.getStateStore();
    MembershipStore membershipStore =
        stateStore.getRegisteredRecordStore(MembershipStore.class);

    GetNamespaceInfoRequest request = GetNamespaceInfoRequest.newInstance();
    GetNamespaceInfoResponse response =
        membershipStore.getNamespaceInfo(request);
    return response.getNamespaceInfo().stream()
      .map(f)
      .collect(Collectors.toSet());
  }

  @Override
  public String getNameDirStatuses() {
    return "N/A";
  }

  @Override
  public String getNodeUsage() {
    return "N/A";
  }

  @Override
  public String getNameJournalStatus() {
    return "N/A";
  }

  @Override
  public String getJournalTransactionInfo() {
    return "N/A";
  }

  @Override
  public long getNNStartedTimeInMillis() {
    try {
      return getRouter().getStartTime();
    } catch (IOException e) {
      LOG.debug("Failed to get the router startup time", e.getMessage());
    }
    return 0;
  }

  @Override
  public String getCompileInfo() {
    return VersionInfo.getDate() + " by " + VersionInfo.getUser() +
        " from " + VersionInfo.getBranch();
  }

  @Override
  public int getDistinctVersionCount() {
    return 0;
  }

  @Override
  public Map<String, Integer> getDistinctVersions() {
    return null;
  }

  /////////////////////////////////////////////////////////
  // FSNamesystemMBean
  /////////////////////////////////////////////////////////

  @Override
  public String getFSState() {
    // We assume is not in safe mode
    return "Operational";
  }

  @Override
  public long getBlocksTotal() {
    return this.getTotalBlocks();
  }

  @Override
  public long getCapacityTotal() {
    return this.getTotal();
  }

  @Override
  public long getCapacityRemaining() {
    return this.getFree();
  }

  @Override
  public long getCapacityUsed() {
    return this.getUsed();
  }

  @Override
  public long getProvidedCapacityTotal() {
    return getProvidedCapacity();
  }

  @Override
  public long getFilesTotal() {
    try {
      return getRBFMetrics().getNumFiles();
    } catch (IOException e) {
      LOG.debug("Failed to get number of files", e.getMessage());
    }
    return 0;
  }

  @Override
  public int getTotalLoad() {
    return -1;
  }

  @Override
  public int getNumLiveDataNodes() {
    try {
      return getRBFMetrics().getNumLiveNodes();
    } catch (IOException e) {
      LOG.debug("Failed to get number of live nodes", e.getMessage());
    }
    return 0;
  }

  @Override
  public int getNumDeadDataNodes() {
    try {
      return getRBFMetrics().getNumDeadNodes();
    } catch (IOException e) {
      LOG.debug("Failed to get number of dead nodes", e.getMessage());
    }
    return 0;
  }

  @Override
  public int getNumStaleDataNodes() {
    try {
      return getRBFMetrics().getNumStaleNodes();
    } catch (IOException e) {
      LOG.debug("Failed to get number of stale nodes", e.getMessage());
    }
    return 0;
  }

  @Override
  public int getNumDecomLiveDataNodes() {
    try {
      return getRBFMetrics().getNumDecomLiveNodes();
    } catch (IOException e) {
      LOG.debug("Failed to get the number of live decommissioned datanodes",
          e.getMessage());
    }
    return 0;
  }

  @Override
  public int getNumDecomDeadDataNodes() {
    try {
      return getRBFMetrics().getNumDecomDeadNodes();
    } catch (IOException e) {
      LOG.debug("Failed to get the number of dead decommissioned datanodes",
          e.getMessage());
    }
    return 0;
  }

  @Override
  public int getNumDecommissioningDataNodes() {
    try {
      return getRBFMetrics().getNumDecommissioningNodes();
    } catch (IOException e) {
      LOG.debug("Failed to get number of decommissioning nodes",
          e.getMessage());
    }
    return 0;
  }

  @Override
  public int getNumInMaintenanceLiveDataNodes() {
    try {
      return getRBFMetrics().getNumInMaintenanceLiveDataNodes();
    } catch (IOException e) {
      LOG.debug("Failed to get number of live in maintenance nodes",
          e.getMessage());
    }
    return 0;
  }

  @Override
  public int getNumInMaintenanceDeadDataNodes() {
    try {
      return getRBFMetrics().getNumInMaintenanceDeadDataNodes();
    } catch (IOException e) {
      LOG.debug("Failed to get number of dead in maintenance nodes",
          e.getMessage());
    }
    return 0;
  }

  @Override
  public int getNumEnteringMaintenanceDataNodes() {
    try {
      return getRBFMetrics().getNumEnteringMaintenanceDataNodes();
    } catch (IOException e) {
      LOG.debug("Failed to get number of entering maintenance nodes",
          e.getMessage());
    }
    return 0;
  }

  @Override
  public int getNumInServiceLiveDataNodes() {
    return 0;
  }

  @Override
  public int getVolumeFailuresTotal() {
    return 0;
  }

  @Override
  public long getEstimatedCapacityLostTotal() {
    return 0;
  }

  @Override
  public String getSnapshotStats() {
    return null;
  }

  @Override
  public long getMaxObjects() {
    return 0;
  }

  @Override
  public long getBlockDeletionStartTime() {
    return -1;
  }

  @Override
  public int getNumStaleStorages() {
    return -1;
  }

  @Override
  public String getTopUserOpCounts() {
    return "N/A";
  }

  @Override
  public int getFsLockQueueLength() {
    return 0;
  }

  @Override
  public long getTotalSyncCount() {
    return 0;
  }

  @Override
  public String getTotalSyncTimes() {
    return "";
  }

  private long getLastContact(DatanodeInfo node) {
    return (now() - node.getLastUpdate()) / 1000;
  }

  /////////////////////////////////////////////////////////
  // NameNodeStatusMXBean
  /////////////////////////////////////////////////////////

  @Override
  public String getNNRole() {
    return NamenodeRole.NAMENODE.toString();
  }

  @Override
  public String getState() {
    return HAServiceState.ACTIVE.toString();
  }

  @Override
  public String getHostAndPort() {
    return NetUtils.getHostPortString(router.getRpcServerAddress());
  }

  @Override
  public boolean isSecurityEnabled() {
    try {
      return getRBFMetrics().isSecurityEnabled();
    } catch (IOException e) {
      LOG.debug("Failed to get security status.",
          e.getMessage());
    }
    return false;
  }

  @Override
  public long getLastHATransitionTime() {
    return 0;
  }

  @Override
  public long getBytesWithFutureGenerationStamps() {
    return 0;
  }

  @Override
  public String getSlowPeersReport() {
    return "N/A";
  }

  @Override
  public String getSlowDisksReport() {
    return "N/A";
  }

  @Override
  public long getNumberOfSnapshottableDirs() {
    return 0;
  }

  @Override
  public String getEnteringMaintenanceNodes() {
    return "{}";
  }

  @Override
  public String getNameDirSize() {
    return "N/A";
  }

  @Override
  public int getNumEncryptionZones() {
    return 0;
  }

  @Override
  public String getVerifyECWithTopologyResult() {
    return null;
  }

  @Override
  public long getCurrentTokensCount() {
    return 0;
  }

  private Router getRouter() throws IOException {
    if (this.router == null) {
      throw new IOException("Router is not initialized");
    }
    return this.router;
  }
}
