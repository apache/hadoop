/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hdfs.server.datanode;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.DataNodeVolumeMetrics;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsVolumeSpi;
import org.apache.hadoop.util.Time;

import javax.annotation.Nullable;

/**
 * {@link FileIoEvents} that profiles the performance of the metadata and data
 * related operations on datanode volumes.
 */
@InterfaceAudience.Private
class ProfilingFileIoEvents extends FileIoEvents {

  @Override
  public long beforeMetadataOp(@Nullable FsVolumeSpi volume,
      FileIoProvider.OPERATION op) {
    DataNodeVolumeMetrics metrics = getVolumeMetrics(volume);
    if (metrics != null) {
      return Time.monotonicNow();
    }
    return 0;
  }

  @Override
  public void afterMetadataOp(@Nullable FsVolumeSpi volume,
      FileIoProvider.OPERATION op, long begin) {
    DataNodeVolumeMetrics metrics = getVolumeMetrics(volume);
    if (metrics != null) {
      metrics.addMetadastaOperationLatency(Time.monotonicNow() - begin);
    }
  }

  @Override
  public long beforeFileIo(@Nullable FsVolumeSpi volume,
      FileIoProvider.OPERATION op, long len) {
    DataNodeVolumeMetrics metrics = getVolumeMetrics(volume);
    if (metrics != null) {
      return Time.monotonicNow();
    }
    return 0;
  }

  @Override
  public void afterFileIo(@Nullable FsVolumeSpi volume,
      FileIoProvider.OPERATION op, long begin, long len) {
    DataNodeVolumeMetrics metrics = getVolumeMetrics(volume);
    if (metrics != null) {
      long latency = Time.monotonicNow() - begin;
      metrics.addDataFileIoLatency(latency);
      switch (op) {
      case SYNC:
        metrics.addSyncIoLatency(latency);
        break;
      case FLUSH:
        metrics.addFlushIoLatency(latency);
        break;
      case READ:
        metrics.addReadIoLatency(latency);
        break;
      case WRITE:
        metrics.addWriteIoLatency(latency);
        break;
      default:
      }
    }
  }

  @Override
  public void onFailure(@Nullable FsVolumeSpi volume,
      FileIoProvider.OPERATION op, Exception e, long begin) {
    DataNodeVolumeMetrics metrics = getVolumeMetrics(volume);
    if (metrics != null) {
      metrics.addFileIoError(Time.monotonicNow() - begin);
    }
  }

  @Nullable
  @Override
  public String getStatistics() {
    return null;
  }

  private DataNodeVolumeMetrics getVolumeMetrics(final FsVolumeSpi volume) {
    if (volume != null) {
      return volume.getMetrics();
    }
    return null;
  }
}