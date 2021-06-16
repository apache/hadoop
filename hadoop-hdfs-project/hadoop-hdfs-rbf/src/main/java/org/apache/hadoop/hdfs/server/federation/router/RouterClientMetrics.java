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
import org.apache.hadoop.metrics2.source.JvmMetrics;

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

  @Metric MutableCounterLong getBlockLocationsOps;
  @Metric MutableCounterLong getServerDefaultsOps;
  @Metric MutableCounterLong createOps;
  @Metric MutableCounterLong appendOps;
  @Metric MutableCounterLong recoverLeaseOps;
  @Metric MutableCounterLong setReplicationOps;
  @Metric MutableCounterLong setStoragePolicyOps;
  @Metric MutableCounterLong getStoragePoliciesOps;
  @Metric MutableCounterLong setPermissionOps;
  @Metric MutableCounterLong setOwnerOps;
  @Metric MutableCounterLong addBlockOps;
  @Metric MutableCounterLong getAdditionalDatanodeOps;
  @Metric MutableCounterLong abandonBlockOps;
  @Metric MutableCounterLong completeOps;
  @Metric MutableCounterLong updateBlockForPipelineOps;
  @Metric MutableCounterLong updatePipelineOps;
  @Metric MutableCounterLong getPreferredBlockSizeOps;
  @Metric MutableCounterLong renameOps;
  @Metric MutableCounterLong rename2Ops;
  @Metric MutableCounterLong concatOps;
  @Metric MutableCounterLong truncateOps;
  @Metric MutableCounterLong deleteOps;
  @Metric MutableCounterLong mkdirsOps;
  @Metric MutableCounterLong renewLeaseOps;
  @Metric MutableCounterLong getListingOps;
  @Metric MutableCounterLong getBatchedListingOps;
  @Metric MutableCounterLong getFileInfoOps;
  @Metric MutableCounterLong isFileClosedOps;
  @Metric MutableCounterLong getFileLinkInfoOps;
  @Metric MutableCounterLong getLocatedFileInfoOps;
  @Metric MutableCounterLong getStatsOps;
  @Metric MutableCounterLong getDatanodeReportOps;
  @Metric MutableCounterLong getDatanodeStorageReportOps;
  @Metric MutableCounterLong setSafeModeOps;
  @Metric MutableCounterLong restoreFailedStorageOps;
  @Metric MutableCounterLong saveNamespaceOps;
  @Metric MutableCounterLong rollEditsOps;
  @Metric MutableCounterLong refreshNodesOps;
  @Metric MutableCounterLong finalizeUpgradeOps;
  @Metric MutableCounterLong upgradeStatusOps;
  @Metric MutableCounterLong rollingUpgradeOps;
  @Metric MutableCounterLong metaSaveOps;
  @Metric MutableCounterLong listCorruptFileBlocksOps;
  @Metric MutableCounterLong setBalancerBandwidthOps;
  @Metric MutableCounterLong getContentSummaryOps;
  @Metric MutableCounterLong fsyncOps;
  @Metric MutableCounterLong setTimesOps;
  @Metric MutableCounterLong createSymlinkOps;
  @Metric MutableCounterLong getLinkTargetOps;
  @Metric MutableCounterLong allowSnapshotOps;
  @Metric MutableCounterLong disallowSnapshotOps;
  @Metric MutableCounterLong renameSnapshotOps;
  @Metric MutableCounterLong getSnapshottableDirListingOps;
  @Metric MutableCounterLong getSnapshotListingOps;
  @Metric MutableCounterLong getSnapshotDiffReportOps;
  @Metric MutableCounterLong getSnapshotDiffReportListingOps;
  @Metric MutableCounterLong addCacheDirectiveOps;
  @Metric MutableCounterLong modifyCacheDirectiveOps;
  @Metric MutableCounterLong removeCacheDirectiveOps;
  @Metric MutableCounterLong listCacheDirectivesOps;
  @Metric MutableCounterLong addCachePoolOps;
  @Metric MutableCounterLong modifyCachePoolOps;
  @Metric MutableCounterLong removeCachePoolOps;
  @Metric MutableCounterLong listCachePoolsOps;
  @Metric MutableCounterLong modifyAclEntriesOps;
  @Metric MutableCounterLong removeAclEntriesOps;
  @Metric MutableCounterLong removeDefaultAclOps;
  @Metric MutableCounterLong removeAclOps;
  @Metric MutableCounterLong setAclOps;
  @Metric MutableCounterLong getAclStatusOps;
  @Metric MutableCounterLong createEncryptionZoneOps;
  @Metric MutableCounterLong getEZForPathOps;
  @Metric MutableCounterLong listEncryptionZonesOps;
  @Metric MutableCounterLong reencryptEncryptionZoneOps;
  @Metric MutableCounterLong listReencryptionStatusOps;
  @Metric MutableCounterLong setXAttrOps;
  @Metric MutableCounterLong getXAttrsOps;
  @Metric MutableCounterLong listXAttrsOps;
  @Metric MutableCounterLong removeXAttrsOps;
  @Metric MutableCounterLong checkAccessOps;
  @Metric MutableCounterLong getCurrentEditLogTxidOps;
  @Metric MutableCounterLong getEditsFromTxidOps;
  @Metric MutableCounterLong getDataEncryptionKeyOps;
  @Metric MutableCounterLong createSnapshotOps;
  @Metric MutableCounterLong deleteSnapshotOps;
  @Metric MutableCounterLong setQuotaOps;
  @Metric MutableCounterLong getQuotaUsageOps;
  @Metric MutableCounterLong reportBadBlocksOps;
  @Metric MutableCounterLong unsetStoragePolicyOps;
  @Metric MutableCounterLong getStoragePolicyOps;
  @Metric MutableCounterLong getErasureCodingPoliciesOps;
  @Metric MutableCounterLong getErasureCodingCodecsOps;
  @Metric MutableCounterLong addErasureCodingPoliciesOps;
  @Metric MutableCounterLong removeErasureCodingPolicyOps;
  @Metric MutableCounterLong disableErasureCodingPolicyOps;
  @Metric MutableCounterLong enableErasureCodingPolicyOps;
  @Metric MutableCounterLong getErasureCodingPolicyOps;
  @Metric MutableCounterLong setErasureCodingPolicyOps;
  @Metric MutableCounterLong unsetErasureCodingPolicyOps;
  @Metric MutableCounterLong getECTopologyResultForPoliciesOps;
  @Metric MutableCounterLong getECBlockGroupStatsOps;
  @Metric MutableCounterLong getReplicatedBlockStatsOps;
  @Metric MutableCounterLong listOpenFilesOps;
  @Metric MutableCounterLong msyncOps;
  @Metric MutableCounterLong satisfyStoragePolicyOps;
  @Metric MutableCounterLong getHAServiceStateOps;
  @Metric MutableCounterLong otherOps;

  // ConcurrentOps
  @Metric MutableCounterLong concurrentSetReplicationOps;
  @Metric MutableCounterLong concurrentSetPermissionOps;
  @Metric MutableCounterLong concurrentSetOwnerOps;
  @Metric MutableCounterLong concurrentRenameOps;
  @Metric MutableCounterLong concurrentRename2Ops;
  @Metric MutableCounterLong concurrentDeleteOps;
  @Metric MutableCounterLong concurrentMkdirsOps;
  @Metric MutableCounterLong concurrentRenewLeaseOps;
  @Metric MutableCounterLong concurrentGetListingOps;
  @Metric MutableCounterLong concurrentGetFileInfoOps;
  @Metric MutableCounterLong concurrentGetStatsOps;
  @Metric MutableCounterLong concurrentGetDatanodeReportOps;
  @Metric MutableCounterLong concurrentSetSafeModeOps;
  @Metric MutableCounterLong concurrentRestoreFailedStorageOps;
  @Metric MutableCounterLong concurrentSaveNamespaceOps;
  @Metric MutableCounterLong concurrentRollEditsOps;
  @Metric MutableCounterLong concurrentRefreshNodesOps;
  @Metric MutableCounterLong concurrentFinalizeUpgradeOps;
  @Metric MutableCounterLong concurrentRollingUpgradeOps;
  @Metric MutableCounterLong concurrentMetaSaveOps;
  @Metric MutableCounterLong concurrentListCorruptFileBlocksOps;
  @Metric MutableCounterLong concurrentSetBalancerBandwidthOps;
  @Metric MutableCounterLong concurrentGetContentSummaryOps;
  @Metric MutableCounterLong concurrentModifyAclEntriesOps;
  @Metric MutableCounterLong concurrentRemoveAclEntriesOps;
  @Metric MutableCounterLong concurrentRemoveDefaultAclOps;
  @Metric MutableCounterLong concurrentRemoveAclOps;
  @Metric MutableCounterLong concurrentSetAclOps;
  @Metric MutableCounterLong concurrentSetXAttrOps;
  @Metric MutableCounterLong concurrentRemoveXAttrOps;
  @Metric MutableCounterLong concurrentGetCurrentEditLogTxidOps;
  @Metric MutableCounterLong concurrentGetReplicatedBlockStatsOps;
  @Metric MutableCounterLong concurrentSetQuotaOps;
  @Metric MutableCounterLong concurrentGetQuotaUsageOps;
  @Metric MutableCounterLong concurrentOtherOps;

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
      default :
        concurrentOtherOps.incr();
    }
  }
}
