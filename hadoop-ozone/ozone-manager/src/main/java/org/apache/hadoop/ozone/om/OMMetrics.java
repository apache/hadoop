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
package org.apache.hadoop.ozone.om;

import com.google.common.annotations.VisibleForTesting;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.MutableCounterLong;
import org.apache.hadoop.metrics2.lib.MutableGaugeLong;

/**
 * This class is for maintaining Ozone Manager statistics.
 */
@InterfaceAudience.Private
@Metrics(about="Ozone Manager Metrics", context="dfs")
public class OMMetrics {
  private static final String SOURCE_NAME =
      OMMetrics.class.getSimpleName();

  // OM request type op metrics
  private @Metric MutableCounterLong numVolumeOps;
  private @Metric MutableCounterLong numBucketOps;
  private @Metric MutableCounterLong numKeyOps;
  private @Metric MutableCounterLong numFSOps;

  // OM op metrics
  private @Metric MutableCounterLong numVolumeCreates;
  private @Metric MutableCounterLong numVolumeUpdates;
  private @Metric MutableCounterLong numVolumeInfos;
  private @Metric MutableCounterLong numVolumeCheckAccesses;
  private @Metric MutableCounterLong numBucketCreates;
  private @Metric MutableCounterLong numVolumeDeletes;
  private @Metric MutableCounterLong numBucketInfos;
  private @Metric MutableCounterLong numBucketUpdates;
  private @Metric MutableCounterLong numBucketDeletes;
  private @Metric MutableCounterLong numKeyAllocate;
  private @Metric MutableCounterLong numKeyLookup;
  private @Metric MutableCounterLong numKeyRenames;
  private @Metric MutableCounterLong numKeyDeletes;
  private @Metric MutableCounterLong numBucketLists;
  private @Metric MutableCounterLong numKeyLists;
  private @Metric MutableCounterLong numVolumeLists;
  private @Metric MutableCounterLong numKeyCommits;
  private @Metric MutableCounterLong numAllocateBlockCalls;
  private @Metric MutableCounterLong numAddAllocateBlockCalls;
  private @Metric MutableCounterLong numGetServiceLists;
  private @Metric MutableCounterLong numListS3Buckets;
  private @Metric MutableCounterLong numInitiateMultipartUploads;
  private @Metric MutableCounterLong numCompleteMultipartUploads;

  private @Metric MutableCounterLong numGetFileStatus;
  private @Metric MutableCounterLong numCreateDirectory;
  private @Metric MutableCounterLong numCreateFile;
  private @Metric MutableCounterLong numLookupFile;

  // Failure Metrics
  private @Metric MutableCounterLong numVolumeCreateFails;
  private @Metric MutableCounterLong numVolumeUpdateFails;
  private @Metric MutableCounterLong numVolumeInfoFails;
  private @Metric MutableCounterLong numVolumeDeleteFails;
  private @Metric MutableCounterLong numBucketCreateFails;
  private @Metric MutableCounterLong numVolumeCheckAccessFails;
  private @Metric MutableCounterLong numBucketInfoFails;
  private @Metric MutableCounterLong numBucketUpdateFails;
  private @Metric MutableCounterLong numBucketDeleteFails;
  private @Metric MutableCounterLong numKeyAllocateFails;
  private @Metric MutableCounterLong numKeyLookupFails;
  private @Metric MutableCounterLong numKeyRenameFails;
  private @Metric MutableCounterLong numKeyDeleteFails;
  private @Metric MutableCounterLong numBucketListFails;
  private @Metric MutableCounterLong numKeyListFails;
  private @Metric MutableCounterLong numVolumeListFails;
  private @Metric MutableCounterLong numKeyCommitFails;
  private @Metric MutableCounterLong numBlockAllocateCallFails;
  private @Metric MutableCounterLong numAddAllocateBlockCallFails;
  private @Metric MutableCounterLong numGetServiceListFails;
  private @Metric MutableCounterLong numListS3BucketsFails;
  private @Metric MutableCounterLong numInitiateMultipartUploadFails;
  private @Metric MutableCounterLong numCommitMultipartUploadParts;
  private @Metric MutableCounterLong getNumCommitMultipartUploadPartFails;
  private @Metric MutableCounterLong numCompleteMultipartUploadFails;
  private @Metric MutableCounterLong numAbortMultipartUploads;
  private @Metric MutableCounterLong numAbortMultipartUploadFails;
  private @Metric MutableCounterLong numListMultipartUploadParts;
  private @Metric MutableCounterLong numListMultipartUploadPartFails;

  private @Metric MutableCounterLong numGetFileStatusFails;
  private @Metric MutableCounterLong numCreateDirectoryFails;
  private @Metric MutableCounterLong numCreateFileFails;
  private @Metric MutableCounterLong numLookupFileFails;

  // Metrics for total number of volumes, buckets and keys

  private @Metric MutableCounterLong numVolumes;
  private @Metric MutableCounterLong numBuckets;

  //TODO: This metric is an estimate and it may be inaccurate on restart if the
  // OM process was not shutdown cleanly. Key creations/deletions in the last
  // few minutes before restart may not be included in this count.
  private @Metric MutableCounterLong numKeys;

  // Metrics to track checkpointing statistics from last run.
  private @Metric MutableGaugeLong lastCheckpointCreationTimeTaken;
  private @Metric MutableGaugeLong lastCheckpointTarOperationTimeTaken;
  private @Metric MutableGaugeLong lastCheckpointStreamingTimeTaken;

  public OMMetrics() {
  }

  public static OMMetrics create() {
    MetricsSystem ms = DefaultMetricsSystem.instance();
    return ms.register(SOURCE_NAME,
        "Ozone Manager Metrics",
        new OMMetrics());
  }

  public void incNumVolumes() {
    numVolumes.incr();
  }

  public void decNumVolumes() {
    numVolumes.incr(-1);
  }

  public void incNumBuckets() {
    numBuckets.incr();
  }

  public void decNumBuckets() {
    numBuckets.incr(-1);
  }

  public void incNumKeys() {
    numKeys.incr();
  }

  public void decNumKeys() {
    numKeys.incr(-1);
  }

  public void setNumVolumes(long val) {
    this.numVolumes.incr(val);
  }

  public void setNumBuckets(long val) {
    this.numBuckets.incr(val);
  }

  public void setNumKeys(long val) {
    this.numKeys.incr(val);
  }

  public long getNumVolumes() {
    return numVolumes.value();
  }

  public long getNumBuckets() {
    return numBuckets.value();
  }

  public long getNumKeys() {
    return numKeys.value();
  }


  public void incNumVolumeCreates() {
    numVolumeOps.incr();
    numVolumeCreates.incr();
  }

  public void incNumVolumeUpdates() {
    numVolumeOps.incr();
    numVolumeUpdates.incr();
  }

  public void incNumVolumeInfos() {
    numVolumeOps.incr();
    numVolumeInfos.incr();
  }

  public void incNumVolumeDeletes() {
    numVolumeOps.incr();
    numVolumeDeletes.incr();
  }

  public void incNumVolumeCheckAccesses() {
    numVolumeOps.incr();
    numVolumeCheckAccesses.incr();
  }

  public void incNumBucketCreates() {
    numBucketOps.incr();
    numBucketCreates.incr();
  }

  public void incNumBucketInfos() {
    numBucketOps.incr();
    numBucketInfos.incr();
  }

  public void incNumBucketUpdates() {
    numBucketOps.incr();
    numBucketUpdates.incr();
  }

  public void incNumBucketDeletes() {
    numBucketOps.incr();
    numBucketDeletes.incr();
  }

  public void incNumBucketLists() {
    numBucketOps.incr();
    numBucketLists.incr();
  }

  public void incNumKeyLists() {
    numKeyOps.incr();
    numKeyLists.incr();
  }

  public void incNumVolumeLists() {
    numVolumeOps.incr();
    numVolumeLists.incr();
  }

  public void incNumListS3Buckets() {
    numBucketOps.incr();
    numListS3Buckets.incr();
  }

  public void incNumListS3BucketsFails() {
    numBucketOps.incr();
    numListS3BucketsFails.incr();
  }

  public void incNumInitiateMultipartUploads() {
    numKeyOps.incr();
    numInitiateMultipartUploads.incr();
  }

  public void incNumInitiateMultipartUploadFails() {
    numInitiateMultipartUploadFails.incr();
  }

  public void incNumCommitMultipartUploadParts() {
    numKeyOps.incr();
    numCommitMultipartUploadParts.incr();
  }

  public void incNumCommitMultipartUploadPartFails() {
    numInitiateMultipartUploadFails.incr();
  }

  public void incNumCompleteMultipartUploads() {
    numKeyOps.incr();
    numCompleteMultipartUploads.incr();
  }

  public void incNumCompleteMultipartUploadFails() {
    numCompleteMultipartUploadFails.incr();
  }

  public void incNumAbortMultipartUploads() {
    numKeyOps.incr();
    numAbortMultipartUploads.incr();
  }

  public void incNumAbortMultipartUploadFails() {
    numAbortMultipartUploadFails.incr();
  }

  public void incNumListMultipartUploadParts() {
    numKeyOps.incr();
    numListMultipartUploadParts.incr();
  }

  public void incNumGetFileStatus() {
    numKeyOps.incr();
    numFSOps.incr();
    numGetFileStatus.incr();
  }

  public void incNumGetFileStatusFails() {
    numGetFileStatusFails.incr();
  }

  public void incNumCreateDirectory() {
    numKeyOps.incr();
    numFSOps.incr();
    numCreateDirectory.incr();
  }

  public void incNumCreateDirectoryFails() {
    numCreateDirectoryFails.incr();
  }

  public void incNumCreateFile() {
    numKeyOps.incr();
    numFSOps.incr();
    numCreateFile.incr();
  }

  public void incNumCreateFileFails() {
    numCreateFileFails.incr();
  }

  public void incNumLookupFile() {
    numKeyOps.incr();
    numFSOps.incr();
    numLookupFile.incr();
  }

  public void incNumLookupFileFails() {
    numLookupFileFails.incr();
  }

  public void incNumListMultipartUploadPartFails() {
    numListMultipartUploadPartFails.incr();
  }

  public void incNumGetServiceLists() {
    numGetServiceLists.incr();
  }

  public void incNumVolumeCreateFails() {
    numVolumeCreateFails.incr();
  }

  public void incNumVolumeUpdateFails() {
    numVolumeUpdateFails.incr();
  }

  public void incNumVolumeInfoFails() {
    numVolumeInfoFails.incr();
  }

  public void incNumVolumeDeleteFails() {
    numVolumeDeleteFails.incr();
  }

  public void incNumVolumeCheckAccessFails() {
    numVolumeCheckAccessFails.incr();
  }

  public void incNumBucketCreateFails() {
    numBucketCreateFails.incr();
  }

  public void incNumBucketInfoFails() {
    numBucketInfoFails.incr();
  }

  public void incNumBucketUpdateFails() {
    numBucketUpdateFails.incr();
  }

  public void incNumBucketDeleteFails() {
    numBucketDeleteFails.incr();
  }

  public void incNumKeyAllocates() {
    numKeyOps.incr();
    numKeyAllocate.incr();
  }

  public void incNumKeyAllocateFails() {
    numKeyAllocateFails.incr();
  }

  public void incNumKeyLookups() {
    numKeyOps.incr();
    numKeyLookup.incr();
  }

  public void incNumKeyLookupFails() {
    numKeyLookupFails.incr();
  }

  public void incNumKeyRenames() {
    numKeyOps.incr();
    numKeyRenames.incr();
  }

  public void incNumKeyRenameFails() {
    numKeyOps.incr();
    numKeyRenameFails.incr();
  }

  public void incNumKeyDeleteFails() {
    numKeyDeleteFails.incr();
  }

  public void incNumKeyDeletes() {
    numKeyOps.incr();
    numKeyDeletes.incr();
  }

  public void incNumKeyCommits() {
    numKeyOps.incr();
    numKeyCommits.incr();
  }

  public void incNumKeyCommitFails() {
    numKeyCommitFails.incr();
  }

  public void incNumBlockAllocateCalls() {
    numAllocateBlockCalls.incr();
  }

  public void incNumBlockAllocateCallFails() {
    numBlockAllocateCallFails.incr();
  }

  public void incNumAddAllocateBlockCalls() {
    numAddAllocateBlockCalls.incr();
  }

  public void incNumAddAllocateBlockFails() {
    numAddAllocateBlockCallFails.incr();
  }

  public void incNumBucketListFails() {
    numBucketListFails.incr();
  }

  public void incNumKeyListFails() {
    numKeyListFails.incr();
  }

  public void incNumVolumeListFails() {
    numVolumeListFails.incr();
  }

  public void incNumGetServiceListFails() {
    numGetServiceListFails.incr();
  }

  public void setLastCheckpointCreationTimeTaken(long val) {
    this.lastCheckpointCreationTimeTaken.set(val);
  }

  public void setLastCheckpointTarOperationTimeTaken(long val) {
    this.lastCheckpointTarOperationTimeTaken.set(val);
  }

  public void setLastCheckpointStreamingTimeTaken(long val) {
    this.lastCheckpointStreamingTimeTaken.set(val);
  }

  @VisibleForTesting
  public long getNumVolumeCreates() {
    return numVolumeCreates.value();
  }

  @VisibleForTesting
  public long getNumVolumeUpdates() {
    return numVolumeUpdates.value();
  }

  @VisibleForTesting
  public long getNumVolumeInfos() {
    return numVolumeInfos.value();
  }

  @VisibleForTesting
  public long getNumVolumeDeletes() {
    return numVolumeDeletes.value();
  }

  @VisibleForTesting
  public long getNumVolumeCheckAccesses() {
    return numVolumeCheckAccesses.value();
  }

  @VisibleForTesting
  public long getNumBucketCreates() {
    return numBucketCreates.value();
  }

  @VisibleForTesting
  public long getNumBucketInfos() {
    return numBucketInfos.value();
  }

  @VisibleForTesting
  public long getNumBucketUpdates() {
    return numBucketUpdates.value();
  }

  @VisibleForTesting
  public long getNumBucketDeletes() {
    return numBucketDeletes.value();
  }

  @VisibleForTesting
  public long getNumBucketLists() {
    return numBucketLists.value();
  }

  @VisibleForTesting
  public long getNumVolumeLists() {
    return numVolumeLists.value();
  }

  @VisibleForTesting
  public long getNumKeyLists() {
    return numKeyLists.value();
  }

  @VisibleForTesting
  public long getNumGetServiceLists() {
    return numGetServiceLists.value();
  }

  @VisibleForTesting
  public long getNumVolumeCreateFails() {
    return numVolumeCreateFails.value();
  }

  @VisibleForTesting
  public long getNumVolumeUpdateFails() {
    return numVolumeUpdateFails.value();
  }

  @VisibleForTesting
  public long getNumVolumeInfoFails() {
    return numVolumeInfoFails.value();
  }

  @VisibleForTesting
  public long getNumVolumeDeleteFails() {
    return numVolumeDeleteFails.value();
  }

  @VisibleForTesting
  public long getNumVolumeCheckAccessFails() {
    return numVolumeCheckAccessFails.value();
  }

  @VisibleForTesting
  public long getNumBucketCreateFails() {
    return numBucketCreateFails.value();
  }

  @VisibleForTesting
  public long getNumBucketInfoFails() {
    return numBucketInfoFails.value();
  }

  @VisibleForTesting
  public long getNumBucketUpdateFails() {
    return numBucketUpdateFails.value();
  }

  @VisibleForTesting
  public long getNumBucketDeleteFails() {
    return numBucketDeleteFails.value();
  }

  @VisibleForTesting
  public long getNumKeyAllocates() {
    return numKeyAllocate.value();
  }

  @VisibleForTesting
  public long getNumKeyAllocateFails() {
    return numKeyAllocateFails.value();
  }

  @VisibleForTesting
  public long getNumKeyLookups() {
    return numKeyLookup.value();
  }

  @VisibleForTesting
  public long getNumKeyLookupFails() {
    return numKeyLookupFails.value();
  }

  @VisibleForTesting
  public long getNumKeyRenames() {
    return numKeyRenames.value();
  }

  @VisibleForTesting
  public long getNumKeyRenameFails() {
    return numKeyRenameFails.value();
  }

  @VisibleForTesting
  public long getNumKeyDeletes() {
    return numKeyDeletes.value();
  }

  @VisibleForTesting
  public long getNumKeyDeletesFails() {
    return numKeyDeleteFails.value();
  }

  @VisibleForTesting
  public long getNumBucketListFails() {
    return numBucketListFails.value();
  }

  @VisibleForTesting
  public long getNumKeyListFails() {
    return numKeyListFails.value();
  }


  @VisibleForTesting
  public long getNumFSOps() {
    return numFSOps.value();
  }

  @VisibleForTesting
  public long getNumGetFileStatus() {
    return numGetFileStatus.value();
  }

  @VisibleForTesting
  public long getNumVolumeListFails() {
    return numVolumeListFails.value();
  }

  @VisibleForTesting
  public long getNumKeyCommits() {
    return numKeyCommits.value();
  }

  @VisibleForTesting
  public long getNumKeyCommitFails() {
    return numKeyCommitFails.value();
  }

  @VisibleForTesting
  public long getNumBlockAllocates() {
    return numAllocateBlockCalls.value();
  }

  @VisibleForTesting
  public long getNumBlockAllocateFails() {
    return numBlockAllocateCallFails.value();
  }

  @VisibleForTesting
  public long getNumGetServiceListFails() {
    return numGetServiceListFails.value();
  }

  @VisibleForTesting
  public long getNumListS3Buckets() {
    return numListS3Buckets.value();
  }

  @VisibleForTesting
  public long getNumListS3BucketsFails() {
    return numListS3BucketsFails.value();
  }

  public long getNumInitiateMultipartUploads() {
    return numInitiateMultipartUploads.value();
  }

  public long getNumInitiateMultipartUploadFails() {
    return numInitiateMultipartUploadFails.value();
  }

  public long getNumAbortMultipartUploads() {
    return numAbortMultipartUploads.value();
  }

  public long getNumAbortMultipartUploadFails() {
    return numAbortMultipartUploadFails.value();
  }

  @VisibleForTesting
  public long getLastCheckpointCreationTimeTaken() {
    return lastCheckpointCreationTimeTaken.value();
  }

  @VisibleForTesting
  public long getLastCheckpointTarOperationTimeTaken() {
    return lastCheckpointTarOperationTimeTaken.value();
  }

  @VisibleForTesting
  public long getLastCheckpointStreamingTimeTaken() {
    return lastCheckpointStreamingTimeTaken.value();
  }

  public void unRegister() {
    MetricsSystem ms = DefaultMetricsSystem.instance();
    ms.unregisterSource(SOURCE_NAME);
  }
}
