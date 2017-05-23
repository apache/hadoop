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

package org.apache.hadoop.cblock.jscsiHelper;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.MutableCounterLong;
import org.apache.hadoop.metrics2.lib.MutableRate;

/**
 * This class is for maintaining  the various Cblock Target statistics
 * and publishing them through the metrics interfaces.
 * This also registers the JMX MBean for RPC.
 *
 * This class maintains stats like cache hit and miss ratio
 * as well as the latency time of read and write ops.
 */
public class CBlockTargetMetrics {
  // IOPS based Metrics
  @Metric private MutableCounterLong numReadOps;
  @Metric private MutableCounterLong numWriteOps;
  @Metric private MutableCounterLong numReadCacheHits;
  @Metric private MutableCounterLong numReadCacheMiss;
  @Metric private MutableCounterLong numDirectBlockWrites;

  // Cblock internal Metrics
  @Metric private MutableCounterLong numDirtyLogBlockRead;
  @Metric private MutableCounterLong numBytesDirtyLogRead;
  @Metric private MutableCounterLong numBytesDirtyLogWritten;
  @Metric private MutableCounterLong numBlockBufferFlushCompleted;
  @Metric private MutableCounterLong numBlockBufferFlushTriggered;
  @Metric private MutableCounterLong numBlockBufferUpdates;
  @Metric private MutableCounterLong numRetryLogBlockRead;
  @Metric private MutableCounterLong numBytesRetryLogRead;

  // Failure Metrics
  @Metric private MutableCounterLong numReadLostBlocks;
  @Metric private MutableCounterLong numFailedReadBlocks;
  @Metric private MutableCounterLong numWriteIOExceptionRetryBlocks;
  @Metric private MutableCounterLong numWriteGenericExceptionRetryBlocks;
  @Metric private MutableCounterLong numFailedDirectBlockWrites;
  @Metric private MutableCounterLong numIllegalDirtyLogFiles;
  @Metric private MutableCounterLong numFailedDirtyLogFileDeletes;
  @Metric private MutableCounterLong numFailedBlockBufferFlushes;
  @Metric private MutableCounterLong numInterruptedBufferWaits;
  @Metric private MutableCounterLong numFailedRetryLogFileWrites;
  @Metric private MutableCounterLong numWriteMaxRetryBlocks;
  @Metric private MutableCounterLong numFailedReleaseLevelDB;

  // Latency based Metrics
  @Metric private MutableRate dbReadLatency;
  @Metric private MutableRate containerReadLatency;
  @Metric private MutableRate dbWriteLatency;
  @Metric private MutableRate containerWriteLatency;
  @Metric private MutableRate blockBufferFlushLatency;
  @Metric private MutableRate directBlockWriteLatency;

  public CBlockTargetMetrics() {
  }

  public static CBlockTargetMetrics create() {
    MetricsSystem ms = DefaultMetricsSystem.instance();
    return ms.register("CBlockTargetMetrics",
        "CBlock Target Metrics",
        new CBlockTargetMetrics());
  }

  public void incNumReadOps() {
    numReadOps.incr();
  }

  public void incNumWriteOps() {
    numWriteOps.incr();
  }

  public void incNumReadCacheHits() {
    numReadCacheHits.incr();
  }

  public void incNumReadCacheMiss() {
    numReadCacheMiss.incr();
  }

  public void incNumReadLostBlocks() {
    numReadLostBlocks.incr();
  }

  public void incNumDirectBlockWrites() {
    numDirectBlockWrites.incr();
  }

  public void incNumWriteIOExceptionRetryBlocks() {
    numWriteIOExceptionRetryBlocks.incr();
  }

  public void incNumWriteGenericExceptionRetryBlocks() {
    numWriteGenericExceptionRetryBlocks.incr();
  }

  public void incNumFailedDirectBlockWrites() {
    numFailedDirectBlockWrites.incr();
  }

  public void incNumFailedReadBlocks() {
    numFailedReadBlocks.incr();
  }

  public void incNumBlockBufferFlushCompleted() {
    numBlockBufferFlushCompleted.incr();
  }

  public void incNumBlockBufferFlushTriggered() {
    numBlockBufferFlushTriggered.incr();
  }

  public void incNumDirtyLogBlockRead() {
    numDirtyLogBlockRead.incr();
  }

  public void incNumBytesDirtyLogRead(int bytes) {
    numBytesDirtyLogRead.incr(bytes);
  }

  public void incNumBlockBufferUpdates() {
    numBlockBufferUpdates.incr();
  }

  public void incNumRetryLogBlockRead() {
    numRetryLogBlockRead.incr();
  }

  public void incNumBytesRetryLogRead(int bytes) {
    numBytesRetryLogRead.incr(bytes);
  }

  public void incNumBytesDirtyLogWritten(int bytes) {
    numBytesDirtyLogWritten.incr(bytes);
  }

  public void incNumFailedBlockBufferFlushes() {
    numFailedBlockBufferFlushes.incr();
  }

  public void incNumInterruptedBufferWaits() {
    numInterruptedBufferWaits.incr();
  }

  public void incNumIllegalDirtyLogFiles() {
    numIllegalDirtyLogFiles.incr();
  }

  public void incNumFailedDirtyLogFileDeletes() {
    numFailedDirtyLogFileDeletes.incr();
  }

  public void incNumFailedRetryLogFileWrites() {
    numFailedRetryLogFileWrites.incr();
  }

  public void incNumWriteMaxRetryBlocks() {
    numWriteMaxRetryBlocks.incr();
  }

  public void incNumFailedReleaseLevelDB() {
    numFailedReleaseLevelDB.incr();
  }

  public void updateDBReadLatency(long latency) {
    dbReadLatency.add(latency);
  }

  public void updateContainerReadLatency(long latency) {
    containerReadLatency.add(latency);
  }

  public void updateDBWriteLatency(long latency) {
    dbWriteLatency.add(latency);
  }

  public void updateContainerWriteLatency(long latency) {
    containerWriteLatency.add(latency);
  }

  public void updateDirectBlockWriteLatency(long latency) {
    directBlockWriteLatency.add(latency);
  }

  public void updateBlockBufferFlushLatency(long latency) {
    blockBufferFlushLatency.add(latency);
  }

  @VisibleForTesting
  public long getNumReadOps() {
    return numReadOps.value();
  }

  @VisibleForTesting
  public long getNumWriteOps() {
    return numWriteOps.value();
  }

  @VisibleForTesting
  public long getNumReadCacheHits() {
    return numReadCacheHits.value();
  }

  @VisibleForTesting
  public long getNumReadCacheMiss() {
    return numReadCacheMiss.value();
  }

  @VisibleForTesting
  public long getNumReadLostBlocks() {
    return numReadLostBlocks.value();
  }

  @VisibleForTesting
  public long getNumDirectBlockWrites() {
    return numDirectBlockWrites.value();
  }

  @VisibleForTesting
  public long getNumFailedDirectBlockWrites() {
    return numFailedDirectBlockWrites.value();
  }

  @VisibleForTesting
  public long getNumFailedReadBlocks() {
    return numFailedReadBlocks.value();
  }

  @VisibleForTesting
  public long getNumWriteIOExceptionRetryBlocks() {
    return numWriteIOExceptionRetryBlocks.value();
  }

  @VisibleForTesting
  public long getNumWriteGenericExceptionRetryBlocks() {
    return numWriteGenericExceptionRetryBlocks.value();
  }

  @VisibleForTesting
  public long getNumBlockBufferFlushCompleted() {
    return numBlockBufferFlushCompleted.value();
  }

  @VisibleForTesting
  public long getNumBlockBufferFlushTriggered() {
    return numBlockBufferFlushTriggered.value();
  }

  @VisibleForTesting
  public long getNumDirtyLogBlockRead() {
    return numDirtyLogBlockRead.value();
  }

  @VisibleForTesting
  public long getNumBytesDirtyLogReads() {
    return numBytesDirtyLogRead.value();
  }

  @VisibleForTesting
  public long getNumBlockBufferUpdates() {
    return numBlockBufferUpdates.value();
  }

  @VisibleForTesting
  public long getNumRetryLogBlockRead() {
    return numRetryLogBlockRead.value();
  }

  @VisibleForTesting
  public long getNumBytesRetryLogReads() {
    return numBytesRetryLogRead.value();
  }

  @VisibleForTesting
  public long getNumBytesDirtyLogWritten() {
    return numBytesDirtyLogWritten.value();
  }

  @VisibleForTesting
  public long getNumFailedBlockBufferFlushes() {
    return numFailedBlockBufferFlushes.value();
  }

  @VisibleForTesting
  public long getNumInterruptedBufferWaits() {
    return numInterruptedBufferWaits.value();
  }

  @VisibleForTesting
  public long getNumIllegalDirtyLogFiles() {
    return numIllegalDirtyLogFiles.value();
  }

  @VisibleForTesting
  public long getNumFailedDirtyLogFileDeletes() {
    return numFailedDirtyLogFileDeletes.value();
  }

  @VisibleForTesting
  public long getNumFailedRetryLogFileWrites() {
    return numFailedRetryLogFileWrites.value();
  }

  @VisibleForTesting
  public long getNumWriteMaxRetryBlocks() {
    return numWriteMaxRetryBlocks.value();
  }

  @VisibleForTesting
  public long getNumFailedReleaseLevelDB() {
    return numFailedReleaseLevelDB.value();
  }
}
