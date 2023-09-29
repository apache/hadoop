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
package org.apache.hadoop.hdfs.server.federation.router;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.MetricsRegistry;
import org.apache.hadoop.metrics2.lib.MutableCounterLong;

import java.lang.reflect.Method;

import static org.apache.hadoop.metrics2.impl.MsInfo.ProcessName;
import static org.apache.hadoop.metrics2.impl.MsInfo.SessionId;

/**
 * This class is for maintaining the various Router Client activity statistics
 * and publishing them through the metrics interfaces.
 */
@Metrics(name="RouterClientActivity", about="Router metrics", context="dfs")
public class RouterClientMetrics {

  private final MetricsRegistry registry = new MetricsRegistry("router");

  @Metric private MutableCounterLong getBlockLocationsOps;
  @Metric private MutableCounterLong getServerDefaultsOps;
  @Metric private MutableCounterLong createOps;
  @Metric private MutableCounterLong appendOps;
  @Metric private MutableCounterLong recoverLeaseOps;
  @Metric private MutableCounterLong setReplicationOps;
  @Metric private MutableCounterLong setStoragePolicyOps;
  @Metric private MutableCounterLong getStoragePoliciesOps;
  @Metric private MutableCounterLong setPermissionOps;
  @Metric private MutableCounterLong setOwnerOps;
  @Metric private MutableCounterLong addBlockOps;
  @Metric private MutableCounterLong getAdditionalDatanodeOps;
  @Metric private MutableCounterLong abandonBlockOps;
  @Metric private MutableCounterLong completeOps;
  @Metric private MutableCounterLong updateBlockForPipelineOps;
  @Metric private MutableCounterLong updatePipelineOps;
  @Metric private MutableCounterLong getPreferredBlockSizeOps;
  @Metric private MutableCounterLong renameOps;
  @Metric private MutableCounterLong rename2Ops;
  @Metric private MutableCounterLong concatOps;
  @Metric private MutableCounterLong truncateOps;
  @Metric private MutableCounterLong deleteOps;
  @Metric private MutableCounterLong mkdirsOps;
  @Metric private MutableCounterLong renewLeaseOps;
  @Metric private MutableCounterLong getListingOps;
  @Metric private MutableCounterLong getBatchedListingOps;
  @Metric private MutableCounterLong getFileInfoOps;
  @Metric private MutableCounterLong isFileClosedOps;
  @Metric private MutableCounterLong getFileLinkInfoOps;
  @Metric private MutableCounterLong getLocatedFileInfoOps;
  @Metric private MutableCounterLong getStatsOps;
  @Metric private MutableCounterLong getDatanodeReportOps;
  @Metric private MutableCounterLong getDatanodeStorageReportOps;
  @Metric private MutableCounterLong setSafeModeOps;
  @Metric private MutableCounterLong restoreFailedStorageOps;
  @Metric private MutableCounterLong saveNamespaceOps;
  @Metric private MutableCounterLong rollEditsOps;
  @Metric private MutableCounterLong refreshNodesOps;
  @Metric private MutableCounterLong finalizeUpgradeOps;
  @Metric private MutableCounterLong upgradeStatusOps;
  @Metric private MutableCounterLong rollingUpgradeOps;
  @Metric private MutableCounterLong metaSaveOps;
  @Metric private MutableCounterLong listCorruptFileBlocksOps;
  @Metric private MutableCounterLong setBalancerBandwidthOps;
  @Metric private MutableCounterLong getContentSummaryOps;
  @Metric private MutableCounterLong fsyncOps;
  @Metric private MutableCounterLong setTimesOps;
  @Metric private MutableCounterLong createSymlinkOps;
  @Metric private MutableCounterLong getLinkTargetOps;
  @Metric private MutableCounterLong allowSnapshotOps;
  @Metric private MutableCounterLong disallowSnapshotOps;
  @Metric private MutableCounterLong renameSnapshotOps;
  @Metric private MutableCounterLong getSnapshottableDirListingOps;
  @Metric private MutableCounterLong getSnapshotListingOps;
  @Metric private MutableCounterLong getSnapshotDiffReportOps;
  @Metric private MutableCounterLong getSnapshotDiffReportListingOps;
  @Metric private MutableCounterLong addCacheDirectiveOps;
  @Metric private MutableCounterLong modifyCacheDirectiveOps;
  @Metric private MutableCounterLong removeCacheDirectiveOps;
  @Metric private MutableCounterLong listCacheDirectivesOps;
  @Metric private MutableCounterLong addCachePoolOps;
  @Metric private MutableCounterLong modifyCachePoolOps;
  @Metric private MutableCounterLong removeCachePoolOps;
  @Metric private MutableCounterLong listCachePoolsOps;
  @Metric private MutableCounterLong modifyAclEntriesOps;
  @Metric private MutableCounterLong removeAclEntriesOps;
  @Metric private MutableCounterLong removeDefaultAclOps;
  @Metric private MutableCounterLong removeAclOps;
  @Metric private MutableCounterLong setAclOps;
  @Metric private MutableCounterLong getAclStatusOps;
  @Metric private MutableCounterLong createEncryptionZoneOps;
  @Metric private MutableCounterLong getEZForPathOps;
  @Metric private MutableCounterLong listEncryptionZonesOps;
  @Metric private MutableCounterLong reencryptEncryptionZoneOps;
  @Metric private MutableCounterLong listReencryptionStatusOps;
  @Metric private MutableCounterLong setXAttrOps;
  @Metric private MutableCounterLong getXAttrsOps;
  @Metric private MutableCounterLong listXAttrsOps;
  @Metric private MutableCounterLong removeXAttrsOps;
  @Metric private MutableCounterLong checkAccessOps;
  @Metric private MutableCounterLong getCurrentEditLogTxidOps;
  @Metric private MutableCounterLong getEditsFromTxidOps;
  @Metric private MutableCounterLong getDataEncryptionKeyOps;
  @Metric private MutableCounterLong createSnapshotOps;
  @Metric private MutableCounterLong deleteSnapshotOps;
  @Metric private MutableCounterLong setQuotaOps;
  @Metric private MutableCounterLong getQuotaUsageOps;
  @Metric private MutableCounterLong reportBadBlocksOps;
  @Metric private MutableCounterLong unsetStoragePolicyOps;
  @Metric private MutableCounterLong getStoragePolicyOps;
  @Metric private MutableCounterLong getErasureCodingPoliciesOps;
  @Metric private MutableCounterLong getErasureCodingCodecsOps;
  @Metric private MutableCounterLong addErasureCodingPoliciesOps;
  @Metric private MutableCounterLong removeErasureCodingPolicyOps;
  @Metric private MutableCounterLong disableErasureCodingPolicyOps;
  @Metric private MutableCounterLong enableErasureCodingPolicyOps;
  @Metric private MutableCounterLong getErasureCodingPolicyOps;
  @Metric private MutableCounterLong setErasureCodingPolicyOps;
  @Metric private MutableCounterLong unsetErasureCodingPolicyOps;
  @Metric private MutableCounterLong getECTopologyResultForPoliciesOps;
  @Metric private MutableCounterLong getECBlockGroupStatsOps;
  @Metric private MutableCounterLong getReplicatedBlockStatsOps;
  @Metric private MutableCounterLong listOpenFilesOps;
  @Metric private MutableCounterLong msyncOps;
  @Metric private MutableCounterLong satisfyStoragePolicyOps;
  @Metric private MutableCounterLong getHAServiceStateOps;
  @Metric private MutableCounterLong otherOps;
  @Metric private MutableCounterLong getSlowDatanodeReportOps;

  // private ConcurrentOps
  @Metric private MutableCounterLong concurrentSetReplicationOps;
  @Metric private MutableCounterLong concurrentSetPermissionOps;
  @Metric private MutableCounterLong concurrentSetOwnerOps;
  @Metric private MutableCounterLong concurrentRenameOps;
  @Metric private MutableCounterLong concurrentRename2Ops;
  @Metric private MutableCounterLong concurrentDeleteOps;
  @Metric private MutableCounterLong concurrentMkdirsOps;
  @Metric private MutableCounterLong concurrentRenewLeaseOps;
  @Metric private MutableCounterLong concurrentGetListingOps;
  @Metric private MutableCounterLong concurrentGetFileInfoOps;
  @Metric private MutableCounterLong concurrentGetStatsOps;
  @Metric private MutableCounterLong concurrentGetDatanodeReportOps;
  @Metric private MutableCounterLong concurrentSetSafeModeOps;
  @Metric private MutableCounterLong concurrentRestoreFailedStorageOps;
  @Metric private MutableCounterLong concurrentSaveNamespaceOps;
  @Metric private MutableCounterLong concurrentRollEditsOps;
  @Metric private MutableCounterLong concurrentRefreshNodesOps;
  @Metric private MutableCounterLong concurrentFinalizeUpgradeOps;
  @Metric private MutableCounterLong concurrentRollingUpgradeOps;
  @Metric private MutableCounterLong concurrentMetaSaveOps;
  @Metric private MutableCounterLong concurrentListCorruptFileBlocksOps;
  @Metric private MutableCounterLong concurrentSetBalancerBandwidthOps;
  @Metric private MutableCounterLong concurrentGetContentSummaryOps;
  @Metric private MutableCounterLong concurrentModifyAclEntriesOps;
  @Metric private MutableCounterLong concurrentRemoveAclEntriesOps;
  @Metric private MutableCounterLong concurrentRemoveDefaultAclOps;
  @Metric private MutableCounterLong concurrentRemoveAclOps;
  @Metric private MutableCounterLong concurrentSetAclOps;
  @Metric private MutableCounterLong concurrentSetXAttrOps;
  @Metric private MutableCounterLong concurrentRemoveXAttrOps;
  @Metric private MutableCounterLong concurrentGetCurrentEditLogTxidOps;
  @Metric private MutableCounterLong concurrentGetReplicatedBlockStatsOps;
  @Metric private MutableCounterLong concurrentSetQuotaOps;
  @Metric private MutableCounterLong concurrentGetQuotaUsageOps;
  @Metric private MutableCounterLong concurrentOtherOps;
  @Metric private MutableCounterLong concurrentGetSlowDatanodeReportOps;

  RouterClientMetrics(String processName, String sessionId) {
    registry.tag(ProcessName, processName).tag(SessionId, sessionId);
  }

  public static RouterClientMetrics create(Configuration conf) {
    String sessionId = conf.get(DFSConfigKeys.DFS_METRICS_SESSION_ID_KEY);
    String processName = "Router";
    MetricsSystem ms = DefaultMetricsSystem.instance();

    return ms.register(new RouterClientMetrics(processName, sessionId));
  }

  public void shutdown() {
    DefaultMetricsSystem.shutdown();
  }

  /**
   * Increase the metrics based on the method being invoked.
   * @param method method being invoked
   */
  public void incInvokedMethod(Method method) {
    switch (method.getName()) {
    case "getBlockLocations":
      getBlockLocationsOps.incr();
      break;
    case "getServerDefaults":
      getServerDefaultsOps.incr();
      break;
    case "create":
      createOps.incr();
      break;
    case "append":
      appendOps.incr();
      break;
    case "recoverLease":
      recoverLeaseOps.incr();
      break;
    case "setReplication":
      setReplicationOps.incr();
      break;
    case "setStoragePolicy":
      setStoragePolicyOps.incr();
      break;
    case "getStoragePolicies":
      getStoragePoliciesOps.incr();
      break;
    case "setPermission":
      setPermissionOps.incr();
      break;
    case "setOwner":
      setOwnerOps.incr();
      break;
    case "addBlock":
      addBlockOps.incr();
      break;
    case "getAdditionalDatanode":
      getAdditionalDatanodeOps.incr();
      break;
    case "abandonBlock":
      abandonBlockOps.incr();
      break;
    case "complete":
      completeOps.incr();
      break;
    case "updateBlockForPipeline":
      updateBlockForPipelineOps.incr();
      break;
    case "updatePipeline":
      updatePipelineOps.incr();
      break;
    case "getPreferredBlockSize":
      getPreferredBlockSizeOps.incr();
      break;
    case "rename":
      renameOps.incr();
      break;
    case "rename2":
      rename2Ops.incr();
      break;
    case "concat":
      concatOps.incr();
      break;
    case "truncate":
      truncateOps.incr();
      break;
    case "delete":
      deleteOps.incr();
      break;
    case "mkdirs":
      mkdirsOps.incr();
      break;
    case "renewLease":
      renewLeaseOps.incr();
      break;
    case "getListing":
      getListingOps.incr();
      break;
    case "getBatchedListing":
      getBatchedListingOps.incr();
      break;
    case "getFileInfo":
      getFileInfoOps.incr();
      break;
    case "isFileClosed":
      isFileClosedOps.incr();
      break;
    case "getFileLinkInfo":
      getFileLinkInfoOps.incr();
      break;
    case "getLocatedFileInfo":
      getLocatedFileInfoOps.incr();
      break;
    case "getStats":
      getStatsOps.incr();
      break;
    case "getDatanodeReport":
      getDatanodeReportOps.incr();
      break;
    case "getDatanodeStorageReport":
      getDatanodeStorageReportOps.incr();
      break;
    case "setSafeMode":
      setSafeModeOps.incr();
      break;
    case "restoreFailedStorage":
      restoreFailedStorageOps.incr();
      break;
    case "saveNamespace":
      saveNamespaceOps.incr();
      break;
    case "rollEdits":
      rollEditsOps.incr();
      break;
    case "refreshNodes":
      refreshNodesOps.incr();
      break;
    case "finalizeUpgrade":
      finalizeUpgradeOps.incr();
      break;
    case "upgradeStatus":
      upgradeStatusOps.incr();
      break;
    case "rollingUpgrade":
      rollingUpgradeOps.incr();
      break;
    case "metaSave":
      metaSaveOps.incr();
      break;
    case "listCorruptFileBlocks":
      listCorruptFileBlocksOps.incr();
      break;
    case "setBalancerBandwidth":
      setBalancerBandwidthOps.incr();
      break;
    case "getContentSummary":
      getContentSummaryOps.incr();
      break;
    case "fsync":
      fsyncOps.incr();
      break;
    case "setTimes":
      setTimesOps.incr();
      break;
    case "createSymlink":
      createSymlinkOps.incr();
      break;
    case "getLinkTarget":
      getLinkTargetOps.incr();
      break;
    case "allowSnapshot":
      allowSnapshotOps.incr();
      break;
    case "disallowSnapshot":
      disallowSnapshotOps.incr();
      break;
    case "renameSnapshot":
      renameSnapshotOps.incr();
      break;
    case "getSnapshottableDirListing":
      getSnapshottableDirListingOps.incr();
      break;
    case "getSnapshotListing":
      getSnapshotListingOps.incr();
      break;
    case "getSnapshotDiffReport":
      getSnapshotDiffReportOps.incr();
      break;
    case "getSnapshotDiffReportListing":
      getSnapshotDiffReportListingOps.incr();
      break;
    case "addCacheDirective":
      addCacheDirectiveOps.incr();
      break;
    case "modifyCacheDirective":
      modifyCacheDirectiveOps.incr();
      break;
    case "removeCacheDirective":
      removeCacheDirectiveOps.incr();
      break;
    case "listCacheDirectives":
      listCacheDirectivesOps.incr();
      break;
    case "addCachePool":
      addCachePoolOps.incr();
      break;
    case "modifyCachePool":
      modifyCachePoolOps.incr();
      break;
    case "removeCachePool":
      removeCachePoolOps.incr();
      break;
    case "listCachePools":
      listCachePoolsOps.incr();
      break;
    case "modifyAclEntries":
      modifyAclEntriesOps.incr();
      break;
    case "removeAclEntries":
      removeAclEntriesOps.incr();
      break;
    case "removeDefaultAcl":
      removeDefaultAclOps.incr();
      break;
    case "removeAcl":
      removeAclOps.incr();
      break;
    case "setAcl":
      setAclOps.incr();
      break;
    case "getAclStatus":
      getAclStatusOps.incr();
      break;
    case "createEncryptionZone":
      createEncryptionZoneOps.incr();
      break;
    case "getEZForPath":
      getEZForPathOps.incr();
      break;
    case "listEncryptionZones":
      listEncryptionZonesOps.incr();
      break;
    case "reencryptEncryptionZone":
      reencryptEncryptionZoneOps.incr();
      break;
    case "listReencryptionStatus":
      listReencryptionStatusOps.incr();
      break;
    case "setXAttr":
      setXAttrOps.incr();
      break;
    case "getXAttrs":
      getXAttrsOps.incr();
      break;
    case "listXAttrs":
      listXAttrsOps.incr();
      break;
    case "removeXAttr":
      removeXAttrsOps.incr();
      break;
    case "checkAccess":
      checkAccessOps.incr();
      break;
    case "getCurrentEditLogTxid":
      getCurrentEditLogTxidOps.incr();
      break;
    case "getEditsFromTxid":
      getEditsFromTxidOps.incr();
      break;
    case "getDataEncryptionKey":
      getDataEncryptionKeyOps.incr();
      break;
    case "createSnapshot":
      createSnapshotOps.incr();
      break;
    case "deleteSnapshot":
      deleteSnapshotOps.incr();
      break;
    case "setQuota":
      setQuotaOps.incr();
      break;
    case "getQuotaUsage":
      getQuotaUsageOps.incr();
      break;
    case "reportBadBlocks":
      reportBadBlocksOps.incr();
      break;
    case "unsetStoragePolicy":
      unsetStoragePolicyOps.incr();
      break;
    case "getStoragePolicy":
      getStoragePolicyOps.incr();
      break;
    case "getErasureCodingPolicies":
      getErasureCodingPoliciesOps.incr();
      break;
    case "getErasureCodingCodecs":
      getErasureCodingCodecsOps.incr();
      break;
    case "addErasureCodingPolicies":
      addErasureCodingPoliciesOps.incr();
      break;
    case "removeErasureCodingPolicy":
      removeErasureCodingPolicyOps.incr();
      break;
    case "disableErasureCodingPolicy":
      disableErasureCodingPolicyOps.incr();
      break;
    case "enableErasureCodingPolicy":
      enableErasureCodingPolicyOps.incr();
      break;
    case "getErasureCodingPolicy":
      getErasureCodingPolicyOps.incr();
      break;
    case "setErasureCodingPolicy":
      setErasureCodingPolicyOps.incr();
      break;
    case "unsetErasureCodingPolicy":
      unsetErasureCodingPolicyOps.incr();
      break;
    case "getECTopologyResultForPolicies":
      getECTopologyResultForPoliciesOps.incr();
      break;
    case "getECBlockGroupStats":
      getECBlockGroupStatsOps.incr();
      break;
    case "getReplicatedBlockStats":
      getReplicatedBlockStatsOps.incr();
      break;
    case "listOpenFiles":
      listOpenFilesOps.incr();
      break;
    case "msync":
      msyncOps.incr();
      break;
    case "satisfyStoragePolicy":
      satisfyStoragePolicyOps.incr();
      break;
    case "getHAServiceState":
      getHAServiceStateOps.incr();
      break;
    case "getSlowDatanodeReport":
      getSlowDatanodeReportOps.incr();
      break;
    default:
      otherOps.incr();
    }
  }

  /**
   * Increase the concurrent metrics based on the method being invoked.
   * @param method concurrently invoked method
   */
  public void incInvokedConcurrent(Method method){
    switch (method.getName()) {
    case "setReplication":
      concurrentSetReplicationOps.incr();
      break;
    case "setPermission":
      concurrentSetPermissionOps.incr();
      break;
    case "setOwner":
      concurrentSetOwnerOps.incr();
      break;
    case "rename":
      concurrentRenameOps.incr();
      break;
    case "rename2":
      concurrentRename2Ops.incr();
      break;
    case "delete":
      concurrentDeleteOps.incr();
      break;
    case "mkdirs":
      concurrentMkdirsOps.incr();
      break;
    case "renewLease":
      concurrentRenewLeaseOps.incr();
      break;
    case "getListing":
      concurrentGetListingOps.incr();
      break;
    case "getFileInfo":
      concurrentGetFileInfoOps.incr();
      break;
    case "getStats":
      concurrentGetStatsOps.incr();
      break;
    case "getDatanodeReport":
      concurrentGetDatanodeReportOps.incr();
      break;
    case "setSafeMode":
      concurrentSetSafeModeOps.incr();
      break;
    case "restoreFailedStorage":
      concurrentRestoreFailedStorageOps.incr();
      break;
    case "saveNamespace":
      concurrentSaveNamespaceOps.incr();
      break;
    case "rollEdits":
      concurrentRollEditsOps.incr();
      break;
    case "refreshNodes":
      concurrentRefreshNodesOps.incr();
      break;
    case "finalizeUpgrade":
      concurrentFinalizeUpgradeOps.incr();
      break;
    case "rollingUpgrade":
      concurrentRollingUpgradeOps.incr();
      break;
    case "metaSave":
      concurrentMetaSaveOps.incr();
      break;
    case "listCorruptFileBlocks":
      concurrentListCorruptFileBlocksOps.incr();
      break;
    case "setBalancerBandwidth":
      concurrentSetBalancerBandwidthOps.incr();
      break;
    case "getContentSummary":
      concurrentGetContentSummaryOps.incr();
      break;
    case "modifyAclEntries":
      concurrentModifyAclEntriesOps.incr();
      break;
    case "removeAclEntries":
      concurrentRemoveAclEntriesOps.incr();
      break;
    case "removeDefaultAcl":
      concurrentRemoveDefaultAclOps.incr();
      break;
    case "removeAcl":
      concurrentRemoveAclOps.incr();
      break;
    case "setAcl":
      concurrentSetAclOps.incr();
      break;
    case "setXAttr":
      concurrentSetXAttrOps.incr();
      break;
    case "removeXAttr":
      concurrentRemoveXAttrOps.incr();
      break;
    case "getCurrentEditLogTxid":
      concurrentGetCurrentEditLogTxidOps.incr();
      break;
    case "getReplicatedBlockStats":
      concurrentGetReplicatedBlockStatsOps.incr();
      break;
    case "setQuota":
      concurrentSetQuotaOps.incr();
      break;
    case "getQuotaUsage":
      concurrentGetQuotaUsageOps.incr();
      break;
    case "getSlowDatanodeReport":
      concurrentGetSlowDatanodeReportOps.incr();
      break;
    default :
      concurrentOtherOps.incr();
    }
  }
}
