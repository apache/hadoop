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

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

/**
 * Expose the Namenode metrics as the Router was one.
 */
public class NamenodeBeanMetrics
    implements FSNamesystemMBean, NameNodeMXBean, NameNodeStatusMXBean {

  private static final Logger LOG =
      LoggerFactory.getLogger(NamenodeBeanMetrics.class);

  /** Prevent holding the page from loading too long. */
  private static final String DN_REPORT_TIME_OUT =
      RBFConfigKeys.FEDERATION_ROUTER_PREFIX + "dn-report.time-out";
  /** We only wait for 1 second. */
  private static final long DN_REPORT_TIME_OUT_DEFAULT =
      TimeUnit.SECONDS.toMillis(1);

  /** Time to cache the DN information. */
  public static final String DN_REPORT_CACHE_EXPIRE =
      RBFConfigKeys.FEDERATION_ROUTER_PREFIX + "dn-report.cache-expire";
  /** We cache the DN information for 10 seconds by default. */
  public static final long DN_REPORT_CACHE_EXPIRE_DEFAULT =
      TimeUnit.SECONDS.toMillis(10);


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
        DN_REPORT_TIME_OUT, DN_REPORT_TIME_OUT_DEFAULT, TimeUnit.MILLISECONDS);
    long dnCacheExpire = conf.getTimeDuration(
        DN_REPORT_CACHE_EXPIRE,
        DN_REPORT_CACHE_EXPIRE_DEFAULT, TimeUnit.MILLISECONDS);
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

  private FederationMetrics getFederationMetrics() {
    return this.router.getMetrics();
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
    return getFederationMetrics().getUsedCapacity();
  }

  @Override
  public long getFree() {
    return getFederationMetrics().getRemainingCapacity();
  }

  @Override
  public long getTotal() {
    return getFederationMetrics().getTotalCapacity();
  }

  @Override
  public long getProvidedCapacity() {
    return getFederationMetrics().getProvidedSpace();
  }

  @Override
  public String getSafemode() {
    // We assume that the global federated view is never in safe mode
    return "";
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
    return getFederationMetrics().getNumBlocks();
  }

  @Override
  public long getNumberOfMissingBlocks() {
    return getFederationMetrics().getNumOfMissingBlocks();
  }

  @Override
  @Deprecated
  public long getPendingReplicationBlocks() {
    return getFederationMetrics().getNumOfBlocksPendingReplication();
  }

  @Override
  public long getPendingReconstructionBlocks() {
    return getFederationMetrics().getNumOfBlocksPendingReplication();
  }

  @Override
  @Deprecated
  public long getUnderReplicatedBlocks() {
    return getFederationMetrics().getNumOfBlocksUnderReplicated();
  }

  @Override
  public long getLowRedundancyBlocks() {
    return getFederationMetrics().getNumOfBlocksUnderReplicated();
  }

  @Override
  public long getPendingDeletionBlocks() {
    return getFederationMetrics().getNumOfBlocksPendingDeletion();
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
    return this.router.getStartTime();
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
    return getFederationMetrics().getNumFiles();
  }

  @Override
  public int getTotalLoad() {
    return -1;
  }

  @Override
  public int getNumLiveDataNodes() {
    return this.router.getMetrics().getNumLiveNodes();
  }

  @Override
  public int getNumDeadDataNodes() {
    return this.router.getMetrics().getNumDeadNodes();
  }

  @Override
  public int getNumStaleDataNodes() {
    return -1;
  }

  @Override
  public int getNumDecomLiveDataNodes() {
    return this.router.getMetrics().getNumDecomLiveNodes();
  }

  @Override
  public int getNumDecomDeadDataNodes() {
    return this.router.getMetrics().getNumDecomDeadNodes();
  }

  @Override
  public int getNumDecommissioningDataNodes() {
    return this.router.getMetrics().getNumDecommissioningNodes();
  }

  @Override
  public int getNumInMaintenanceLiveDataNodes() {
    return 0;
  }

  @Override
  public int getNumInMaintenanceDeadDataNodes() {
    return 0;
  }

  @Override
  public int getNumEnteringMaintenanceDataNodes() {
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
    return "N/A";
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
}
