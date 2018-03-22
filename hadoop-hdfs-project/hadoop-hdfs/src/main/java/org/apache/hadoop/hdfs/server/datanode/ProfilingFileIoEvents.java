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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.server.common.Util;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.DataNodeVolumeMetrics;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsVolumeSpi;
import org.apache.hadoop.util.Time;

import javax.annotation.Nullable;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Profiles the performance of the metadata and data related operations on
 * datanode volumes.
 */
@InterfaceAudience.Private
class ProfilingFileIoEvents {
  static final Log LOG = LogFactory.getLog(ProfilingFileIoEvents.class);

  private final boolean isEnabled;
  private final int sampleRangeMax;

  public ProfilingFileIoEvents(@Nullable Configuration conf) {
    if (conf != null) {
      int fileIOSamplingPercentage = conf.getInt(
          DFSConfigKeys.DFS_DATANODE_FILEIO_PROFILING_SAMPLING_PERCENTAGE_KEY,
          DFSConfigKeys
              .DFS_DATANODE_FILEIO_PROFILING_SAMPLING_PERCENTAGE_DEFAULT);
      isEnabled = Util.isDiskStatsEnabled(fileIOSamplingPercentage);
      if (fileIOSamplingPercentage > 100) {
        LOG.warn(DFSConfigKeys
            .DFS_DATANODE_FILEIO_PROFILING_SAMPLING_PERCENTAGE_KEY +
            " value cannot be more than 100. Setting value to 100");
        fileIOSamplingPercentage = 100;
      }
      sampleRangeMax = (int) ((double) fileIOSamplingPercentage / 100 *
          Integer.MAX_VALUE);
    } else {
      isEnabled = false;
      sampleRangeMax = 0;
    }
  }

  public long beforeMetadataOp(@Nullable FsVolumeSpi volume,
      FileIoProvider.OPERATION op) {
    if (isEnabled) {
      DataNodeVolumeMetrics metrics = getVolumeMetrics(volume);
      if (metrics != null) {
        return Time.monotonicNow();
      }
    }
    return 0;
  }

  public void afterMetadataOp(@Nullable FsVolumeSpi volume,
      FileIoProvider.OPERATION op, long begin) {
    if (isEnabled) {
      DataNodeVolumeMetrics metrics = getVolumeMetrics(volume);
      if (metrics != null) {
        metrics.addMetadastaOperationLatency(Time.monotonicNow() - begin);
      }
    }
  }

  public long beforeFileIo(@Nullable FsVolumeSpi volume,
      FileIoProvider.OPERATION op, long len) {
    if (isEnabled && ThreadLocalRandom.current().nextInt() < sampleRangeMax) {
      DataNodeVolumeMetrics metrics = getVolumeMetrics(volume);
      if (metrics != null) {
        return Time.monotonicNow();
      }
    }
    return 0;
  }

  public void afterFileIo(@Nullable FsVolumeSpi volume,
      FileIoProvider.OPERATION op, long begin, long len) {
    if (isEnabled && begin != 0) {
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
  }

  public void onFailure(@Nullable FsVolumeSpi volume, long begin) {
    if (isEnabled) {
      DataNodeVolumeMetrics metrics = getVolumeMetrics(volume);
      if (metrics != null) {
        metrics.addFileIoError(Time.monotonicNow() - begin);
      }
    }
  }

  private DataNodeVolumeMetrics getVolumeMetrics(final FsVolumeSpi volume) {
    if (isEnabled) {
      if (volume != null) {
        return volume.getMetrics();
      }
    }
    return null;
  }
}
