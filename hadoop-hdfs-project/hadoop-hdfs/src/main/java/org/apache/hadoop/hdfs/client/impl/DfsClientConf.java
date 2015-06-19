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
package org.apache.hadoop.hdfs.client.impl;

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_BLOCK_SIZE_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_BLOCK_SIZE_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_BYTES_PER_CHECKSUM_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_BYTES_PER_CHECKSUM_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_CLIENT_CACHED_CONN_RETRY_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_CLIENT_CACHED_CONN_RETRY_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_CLIENT_DATANODE_RESTART_TIMEOUT_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_CLIENT_DATANODE_RESTART_TIMEOUT_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_CLIENT_MAX_BLOCK_ACQUIRE_FAILURES_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_CLIENT_MAX_BLOCK_ACQUIRE_FAILURES_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_CLIENT_SOCKET_CACHE_CAPACITY_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_CLIENT_SOCKET_CACHE_CAPACITY_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_CLIENT_SOCKET_CACHE_EXPIRY_MSEC_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_CLIENT_SOCKET_CACHE_EXPIRY_MSEC_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_CLIENT_SOCKET_TIMEOUT_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_CLIENT_USE_DN_HOSTNAME;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_CLIENT_USE_DN_HOSTNAME_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_SOCKET_WRITE_TIMEOUT_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_REPLICATION_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_REPLICATION_KEY;

import com.google.common.base.Preconditions;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.Options.ChecksumOpt;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.BlockReaderFactory;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.util.ByteArrayManager;
import org.apache.hadoop.ipc.Client;
import org.apache.hadoop.util.DataChecksum;

import com.google.common.annotations.VisibleForTesting;

/**
 * DFSClient configuration 
 */
public class DfsClientConf {

  private final int hdfsTimeout;    // timeout value for a DFS operation.

  private final int maxFailoverAttempts;
  private final int maxRetryAttempts;
  private final int failoverSleepBaseMillis;
  private final int failoverSleepMaxMillis;
  private final int maxBlockAcquireFailures;
  private final int datanodeSocketWriteTimeout;
  private final int ioBufferSize;
  private final ChecksumOpt defaultChecksumOpt;
  private final int writePacketSize;
  private final int writeMaxPackets;
  private final ByteArrayManager.Conf writeByteArrayManagerConf;
  private final int socketTimeout;
  private final long excludedNodesCacheExpiry;
  /** Wait time window (in msec) if BlockMissingException is caught */
  private final int timeWindow;
  private final int numCachedConnRetry;
  private final int numBlockWriteRetry;
  private final int numBlockWriteLocateFollowingRetry;
  private final int blockWriteLocateFollowingInitialDelayMs;
  private final long defaultBlockSize;
  private final long prefetchSize;
  private final short defaultReplication;
  private final String taskId;
  private final FsPermission uMask;
  private final boolean connectToDnViaHostname;
  private final boolean hdfsBlocksMetadataEnabled;
  private final int fileBlockStorageLocationsNumThreads;
  private final int fileBlockStorageLocationsTimeoutMs;
  private final int retryTimesForGetLastBlockLength;
  private final int retryIntervalForGetLastBlockLength;
  private final long datanodeRestartTimeout;
  private final long slowIoWarningThresholdMs;

  private final ShortCircuitConf shortCircuitConf;
  
  private final long hedgedReadThresholdMillis;
  private final int hedgedReadThreadpoolSize;

  private final int stripedReadThreadpoolSize;


  public DfsClientConf(Configuration conf) {
    // The hdfsTimeout is currently the same as the ipc timeout 
    hdfsTimeout = Client.getTimeout(conf);

    maxRetryAttempts = conf.getInt(
        HdfsClientConfigKeys.Retry.MAX_ATTEMPTS_KEY,
        HdfsClientConfigKeys.Retry.MAX_ATTEMPTS_DEFAULT);
    timeWindow = conf.getInt(
        HdfsClientConfigKeys.Retry.WINDOW_BASE_KEY,
        HdfsClientConfigKeys.Retry.WINDOW_BASE_DEFAULT);
    retryTimesForGetLastBlockLength = conf.getInt(
        HdfsClientConfigKeys.Retry.TIMES_GET_LAST_BLOCK_LENGTH_KEY,
        HdfsClientConfigKeys.Retry.TIMES_GET_LAST_BLOCK_LENGTH_DEFAULT);
    retryIntervalForGetLastBlockLength = conf.getInt(
        HdfsClientConfigKeys.Retry.INTERVAL_GET_LAST_BLOCK_LENGTH_KEY,
        HdfsClientConfigKeys.Retry.INTERVAL_GET_LAST_BLOCK_LENGTH_DEFAULT);

    maxFailoverAttempts = conf.getInt(
        HdfsClientConfigKeys.Failover.MAX_ATTEMPTS_KEY,
        HdfsClientConfigKeys.Failover.MAX_ATTEMPTS_DEFAULT);
    failoverSleepBaseMillis = conf.getInt(
        HdfsClientConfigKeys.Failover.SLEEPTIME_BASE_KEY,
        HdfsClientConfigKeys.Failover.SLEEPTIME_BASE_DEFAULT);
    failoverSleepMaxMillis = conf.getInt(
        HdfsClientConfigKeys.Failover.SLEEPTIME_MAX_KEY,
        HdfsClientConfigKeys.Failover.SLEEPTIME_MAX_DEFAULT);

    maxBlockAcquireFailures = conf.getInt(
        DFS_CLIENT_MAX_BLOCK_ACQUIRE_FAILURES_KEY,
        DFS_CLIENT_MAX_BLOCK_ACQUIRE_FAILURES_DEFAULT);
    datanodeSocketWriteTimeout = conf.getInt(DFS_DATANODE_SOCKET_WRITE_TIMEOUT_KEY,
        HdfsServerConstants.WRITE_TIMEOUT);
    ioBufferSize = conf.getInt(
        CommonConfigurationKeysPublic.IO_FILE_BUFFER_SIZE_KEY,
        CommonConfigurationKeysPublic.IO_FILE_BUFFER_SIZE_DEFAULT);
    defaultChecksumOpt = getChecksumOptFromConf(conf);
    socketTimeout = conf.getInt(DFS_CLIENT_SOCKET_TIMEOUT_KEY,
        HdfsServerConstants.READ_TIMEOUT);
    /** dfs.write.packet.size is an internal config variable */
    writePacketSize = conf.getInt(
        DFSConfigKeys.DFS_CLIENT_WRITE_PACKET_SIZE_KEY,
        DFSConfigKeys.DFS_CLIENT_WRITE_PACKET_SIZE_DEFAULT);
    writeMaxPackets = conf.getInt(
        HdfsClientConfigKeys.Write.MAX_PACKETS_IN_FLIGHT_KEY,
        HdfsClientConfigKeys.Write.MAX_PACKETS_IN_FLIGHT_DEFAULT);
    
    final boolean byteArrayManagerEnabled = conf.getBoolean(
        HdfsClientConfigKeys.Write.ByteArrayManager.ENABLED_KEY,
        HdfsClientConfigKeys.Write.ByteArrayManager.ENABLED_DEFAULT);
    if (!byteArrayManagerEnabled) {
      writeByteArrayManagerConf = null;
    } else {
      final int countThreshold = conf.getInt(
          HdfsClientConfigKeys.Write.ByteArrayManager.COUNT_THRESHOLD_KEY,
          HdfsClientConfigKeys.Write.ByteArrayManager.COUNT_THRESHOLD_DEFAULT);
      final int countLimit = conf.getInt(
          HdfsClientConfigKeys.Write.ByteArrayManager.COUNT_LIMIT_KEY,
          HdfsClientConfigKeys.Write.ByteArrayManager.COUNT_LIMIT_DEFAULT);
      final long countResetTimePeriodMs = conf.getLong(
          HdfsClientConfigKeys.Write.ByteArrayManager.COUNT_RESET_TIME_PERIOD_MS_KEY,
          HdfsClientConfigKeys.Write.ByteArrayManager.COUNT_RESET_TIME_PERIOD_MS_DEFAULT);
      writeByteArrayManagerConf = new ByteArrayManager.Conf(
          countThreshold, countLimit, countResetTimePeriodMs); 
    }
    
    defaultBlockSize = conf.getLongBytes(DFS_BLOCK_SIZE_KEY,
        DFS_BLOCK_SIZE_DEFAULT);
    defaultReplication = (short) conf.getInt(
        DFS_REPLICATION_KEY, DFS_REPLICATION_DEFAULT);
    taskId = conf.get("mapreduce.task.attempt.id", "NONMAPREDUCE");
    excludedNodesCacheExpiry = conf.getLong(
        HdfsClientConfigKeys.Write.EXCLUDE_NODES_CACHE_EXPIRY_INTERVAL_KEY,
        HdfsClientConfigKeys.Write.EXCLUDE_NODES_CACHE_EXPIRY_INTERVAL_DEFAULT);
    prefetchSize = conf.getLong(HdfsClientConfigKeys.Read.PREFETCH_SIZE_KEY,
        10 * defaultBlockSize);
    numCachedConnRetry = conf.getInt(DFS_CLIENT_CACHED_CONN_RETRY_KEY,
        DFS_CLIENT_CACHED_CONN_RETRY_DEFAULT);
    numBlockWriteRetry = conf.getInt(
        HdfsClientConfigKeys.BlockWrite.RETRIES_KEY,
        HdfsClientConfigKeys.BlockWrite.RETRIES_DEFAULT);
    numBlockWriteLocateFollowingRetry = conf.getInt(
        HdfsClientConfigKeys.BlockWrite.LOCATEFOLLOWINGBLOCK_RETRIES_KEY,
        HdfsClientConfigKeys.BlockWrite.LOCATEFOLLOWINGBLOCK_RETRIES_DEFAULT);
    blockWriteLocateFollowingInitialDelayMs = conf.getInt(
        HdfsClientConfigKeys.BlockWrite.LOCATEFOLLOWINGBLOCK_INITIAL_DELAY_MS_KEY,
        HdfsClientConfigKeys.BlockWrite.LOCATEFOLLOWINGBLOCK_INITIAL_DELAY_MS_DEFAULT);
    uMask = FsPermission.getUMask(conf);
    connectToDnViaHostname = conf.getBoolean(DFS_CLIENT_USE_DN_HOSTNAME,
        DFS_CLIENT_USE_DN_HOSTNAME_DEFAULT);
    hdfsBlocksMetadataEnabled = conf.getBoolean(
        DFSConfigKeys.DFS_HDFS_BLOCKS_METADATA_ENABLED,
        DFSConfigKeys.DFS_HDFS_BLOCKS_METADATA_ENABLED_DEFAULT);
    fileBlockStorageLocationsNumThreads = conf.getInt(
        DFSConfigKeys.DFS_CLIENT_FILE_BLOCK_STORAGE_LOCATIONS_NUM_THREADS,
        DFSConfigKeys.DFS_CLIENT_FILE_BLOCK_STORAGE_LOCATIONS_NUM_THREADS_DEFAULT);
    fileBlockStorageLocationsTimeoutMs = conf.getInt(
        DFSConfigKeys.DFS_CLIENT_FILE_BLOCK_STORAGE_LOCATIONS_TIMEOUT_MS,
        DFSConfigKeys.DFS_CLIENT_FILE_BLOCK_STORAGE_LOCATIONS_TIMEOUT_MS_DEFAULT);

    datanodeRestartTimeout = conf.getLong(
        DFS_CLIENT_DATANODE_RESTART_TIMEOUT_KEY,
        DFS_CLIENT_DATANODE_RESTART_TIMEOUT_DEFAULT) * 1000;
    slowIoWarningThresholdMs = conf.getLong(
        DFSConfigKeys.DFS_CLIENT_SLOW_IO_WARNING_THRESHOLD_KEY,
        DFSConfigKeys.DFS_CLIENT_SLOW_IO_WARNING_THRESHOLD_DEFAULT);
    
    shortCircuitConf = new ShortCircuitConf(conf);

    hedgedReadThresholdMillis = conf.getLong(
        HdfsClientConfigKeys.HedgedRead.THRESHOLD_MILLIS_KEY,
        HdfsClientConfigKeys.HedgedRead.THRESHOLD_MILLIS_DEFAULT);
    hedgedReadThreadpoolSize = conf.getInt(
        HdfsClientConfigKeys.HedgedRead.THREADPOOL_SIZE_KEY,
        HdfsClientConfigKeys.HedgedRead.THREADPOOL_SIZE_DEFAULT);

    stripedReadThreadpoolSize = conf.getInt(
        HdfsClientConfigKeys.StripedRead.THREADPOOL_SIZE_KEY,
        HdfsClientConfigKeys.StripedRead.THREADPOOL_SIZE_DEFAULT);
    Preconditions.checkArgument(stripedReadThreadpoolSize > 0, "The value of " +
        HdfsClientConfigKeys.StripedRead.THREADPOOL_SIZE_KEY +
        " must be greater than 0.");
  }

  private DataChecksum.Type getChecksumType(Configuration conf) {
    final String checksum = conf.get(
        DFSConfigKeys.DFS_CHECKSUM_TYPE_KEY,
        DFSConfigKeys.DFS_CHECKSUM_TYPE_DEFAULT);
    try {
      return DataChecksum.Type.valueOf(checksum);
    } catch(IllegalArgumentException iae) {
      DFSClient.LOG.warn("Bad checksum type: " + checksum + ". Using default "
          + DFSConfigKeys.DFS_CHECKSUM_TYPE_DEFAULT);
      return DataChecksum.Type.valueOf(
          DFSConfigKeys.DFS_CHECKSUM_TYPE_DEFAULT); 
    }
  }

  // Construct a checksum option from conf
  private ChecksumOpt getChecksumOptFromConf(Configuration conf) {
    DataChecksum.Type type = getChecksumType(conf);
    int bytesPerChecksum = conf.getInt(DFS_BYTES_PER_CHECKSUM_KEY,
        DFS_BYTES_PER_CHECKSUM_DEFAULT);
    return new ChecksumOpt(type, bytesPerChecksum);
  }

  /** create a DataChecksum with the given option. */
  public DataChecksum createChecksum(ChecksumOpt userOpt) {
    // Fill in any missing field with the default.
    ChecksumOpt opt = ChecksumOpt.processChecksumOpt(
        defaultChecksumOpt, userOpt);
    DataChecksum dataChecksum = DataChecksum.newDataChecksum(
        opt.getChecksumType(),
        opt.getBytesPerChecksum());
    if (dataChecksum == null) {
      throw new HadoopIllegalArgumentException("Invalid checksum type: userOpt="
          + userOpt + ", default=" + defaultChecksumOpt
          + ", effective=null");
    }
    return dataChecksum;
  }

  @VisibleForTesting
  public int getBlockWriteLocateFollowingInitialDelayMs() {
    return blockWriteLocateFollowingInitialDelayMs;
  }

  /**
   * @return the hdfsTimeout
   */
  public int getHdfsTimeout() {
    return hdfsTimeout;
  }

  /**
   * @return the maxFailoverAttempts
   */
  public int getMaxFailoverAttempts() {
    return maxFailoverAttempts;
  }

  /**
   * @return the maxRetryAttempts
   */
  public int getMaxRetryAttempts() {
    return maxRetryAttempts;
  }

  /**
   * @return the failoverSleepBaseMillis
   */
  public int getFailoverSleepBaseMillis() {
    return failoverSleepBaseMillis;
  }

  /**
   * @return the failoverSleepMaxMillis
   */
  public int getFailoverSleepMaxMillis() {
    return failoverSleepMaxMillis;
  }

  /**
   * @return the maxBlockAcquireFailures
   */
  public int getMaxBlockAcquireFailures() {
    return maxBlockAcquireFailures;
  }

  /**
   * @return the datanodeSocketWriteTimeout
   */
  public int getDatanodeSocketWriteTimeout() {
    return datanodeSocketWriteTimeout;
  }

  /**
   * @return the ioBufferSize
   */
  public int getIoBufferSize() {
    return ioBufferSize;
  }

  /**
   * @return the defaultChecksumOpt
   */
  public ChecksumOpt getDefaultChecksumOpt() {
    return defaultChecksumOpt;
  }

  /**
   * @return the writePacketSize
   */
  public int getWritePacketSize() {
    return writePacketSize;
  }

  /**
   * @return the writeMaxPackets
   */
  public int getWriteMaxPackets() {
    return writeMaxPackets;
  }

  /**
   * @return the writeByteArrayManagerConf
   */
  public ByteArrayManager.Conf getWriteByteArrayManagerConf() {
    return writeByteArrayManagerConf;
  }

  /**
   * @return the socketTimeout
   */
  public int getSocketTimeout() {
    return socketTimeout;
  }

  /**
   * @return the excludedNodesCacheExpiry
   */
  public long getExcludedNodesCacheExpiry() {
    return excludedNodesCacheExpiry;
  }

  /**
   * @return the timeWindow
   */
  public int getTimeWindow() {
    return timeWindow;
  }

  /**
   * @return the numCachedConnRetry
   */
  public int getNumCachedConnRetry() {
    return numCachedConnRetry;
  }

  /**
   * @return the numBlockWriteRetry
   */
  public int getNumBlockWriteRetry() {
    return numBlockWriteRetry;
  }

  /**
   * @return the numBlockWriteLocateFollowingRetry
   */
  public int getNumBlockWriteLocateFollowingRetry() {
    return numBlockWriteLocateFollowingRetry;
  }

  /**
   * @return the defaultBlockSize
   */
  public long getDefaultBlockSize() {
    return defaultBlockSize;
  }

  /**
   * @return the prefetchSize
   */
  public long getPrefetchSize() {
    return prefetchSize;
  }

  /**
   * @return the defaultReplication
   */
  public short getDefaultReplication() {
    return defaultReplication;
  }

  /**
   * @return the taskId
   */
  public String getTaskId() {
    return taskId;
  }

  /**
   * @return the uMask
   */
  public FsPermission getUMask() {
    return uMask;
  }

  /**
   * @return the connectToDnViaHostname
   */
  public boolean isConnectToDnViaHostname() {
    return connectToDnViaHostname;
  }

  /**
   * @return the hdfsBlocksMetadataEnabled
   */
  public boolean isHdfsBlocksMetadataEnabled() {
    return hdfsBlocksMetadataEnabled;
  }

  /**
   * @return the fileBlockStorageLocationsNumThreads
   */
  public int getFileBlockStorageLocationsNumThreads() {
    return fileBlockStorageLocationsNumThreads;
  }

  /**
   * @return the getFileBlockStorageLocationsTimeoutMs
   */
  public int getFileBlockStorageLocationsTimeoutMs() {
    return fileBlockStorageLocationsTimeoutMs;
  }

  /**
   * @return the retryTimesForGetLastBlockLength
   */
  public int getRetryTimesForGetLastBlockLength() {
    return retryTimesForGetLastBlockLength;
  }

  /**
   * @return the retryIntervalForGetLastBlockLength
   */
  public int getRetryIntervalForGetLastBlockLength() {
    return retryIntervalForGetLastBlockLength;
  }

  /**
   * @return the datanodeRestartTimeout
   */
  public long getDatanodeRestartTimeout() {
    return datanodeRestartTimeout;
  }

  /**
   * @return the slowIoWarningThresholdMs
   */
  public long getSlowIoWarningThresholdMs() {
    return slowIoWarningThresholdMs;
  }

  /**
   * @return the hedgedReadThresholdMillis
   */
  public long getHedgedReadThresholdMillis() {
    return hedgedReadThresholdMillis;
  }

  /**
   * @return the hedgedReadThreadpoolSize
   */
  public int getHedgedReadThreadpoolSize() {
    return hedgedReadThreadpoolSize;
  }

  /**
   * @return the stripedReadThreadpoolSize
   */
  public int getStripedReadThreadpoolSize() {
    return stripedReadThreadpoolSize;
  }

  /**
   * @return the shortCircuitConf
   */
  public ShortCircuitConf getShortCircuitConf() {
    return shortCircuitConf;
  }

  public static class ShortCircuitConf {
    private static final Log LOG = LogFactory.getLog(ShortCircuitConf.class);

    private final int socketCacheCapacity;
    private final long socketCacheExpiry;

    private final boolean useLegacyBlockReader;
    private final boolean useLegacyBlockReaderLocal;
    private final String domainSocketPath;
    private final boolean skipShortCircuitChecksums;

    private final int shortCircuitBufferSize;
    private final boolean shortCircuitLocalReads;
    private final boolean domainSocketDataTraffic;
    private final int shortCircuitStreamsCacheSize;
    private final long shortCircuitStreamsCacheExpiryMs; 
    private final int shortCircuitSharedMemoryWatcherInterruptCheckMs;
    
    private final boolean shortCircuitMmapEnabled;
    private final int shortCircuitMmapCacheSize;
    private final long shortCircuitMmapCacheExpiryMs;
    private final long shortCircuitMmapCacheRetryTimeout;
    private final long shortCircuitCacheStaleThresholdMs;

    private final long keyProviderCacheExpiryMs;

    @VisibleForTesting
    public BlockReaderFactory.FailureInjector brfFailureInjector =
        new BlockReaderFactory.FailureInjector();

    public ShortCircuitConf(Configuration conf) {
      socketCacheCapacity = conf.getInt(
          DFS_CLIENT_SOCKET_CACHE_CAPACITY_KEY,
          DFS_CLIENT_SOCKET_CACHE_CAPACITY_DEFAULT);
      socketCacheExpiry = conf.getLong(
          DFS_CLIENT_SOCKET_CACHE_EXPIRY_MSEC_KEY,
          DFS_CLIENT_SOCKET_CACHE_EXPIRY_MSEC_DEFAULT);

      useLegacyBlockReader = conf.getBoolean(
          DFSConfigKeys.DFS_CLIENT_USE_LEGACY_BLOCKREADER,
          DFSConfigKeys.DFS_CLIENT_USE_LEGACY_BLOCKREADER_DEFAULT);
      useLegacyBlockReaderLocal = conf.getBoolean(
          DFSConfigKeys.DFS_CLIENT_USE_LEGACY_BLOCKREADERLOCAL,
          DFSConfigKeys.DFS_CLIENT_USE_LEGACY_BLOCKREADERLOCAL_DEFAULT);
      shortCircuitLocalReads = conf.getBoolean(
          HdfsClientConfigKeys.Read.ShortCircuit.KEY,
          HdfsClientConfigKeys.Read.ShortCircuit.DEFAULT);
      domainSocketDataTraffic = conf.getBoolean(
          DFSConfigKeys.DFS_CLIENT_DOMAIN_SOCKET_DATA_TRAFFIC,
          DFSConfigKeys.DFS_CLIENT_DOMAIN_SOCKET_DATA_TRAFFIC_DEFAULT);
      domainSocketPath = conf.getTrimmed(
          DFSConfigKeys.DFS_DOMAIN_SOCKET_PATH_KEY,
          DFSConfigKeys.DFS_DOMAIN_SOCKET_PATH_DEFAULT);

      if (LOG.isDebugEnabled()) {
        LOG.debug(DFSConfigKeys.DFS_CLIENT_USE_LEGACY_BLOCKREADERLOCAL
            + " = " + useLegacyBlockReaderLocal);
        LOG.debug(HdfsClientConfigKeys.Read.ShortCircuit.KEY
            + " = " + shortCircuitLocalReads);
        LOG.debug(DFSConfigKeys.DFS_CLIENT_DOMAIN_SOCKET_DATA_TRAFFIC
            + " = " + domainSocketDataTraffic);
        LOG.debug(DFSConfigKeys.DFS_DOMAIN_SOCKET_PATH_KEY
            + " = " + domainSocketPath);
      }

      skipShortCircuitChecksums = conf.getBoolean(
          HdfsClientConfigKeys.Read.ShortCircuit.SKIP_CHECKSUM_KEY,
          HdfsClientConfigKeys.Read.ShortCircuit.SKIP_CHECKSUM_DEFAULT);
      shortCircuitBufferSize = conf.getInt(
          HdfsClientConfigKeys.Read.ShortCircuit.BUFFER_SIZE_KEY,
          HdfsClientConfigKeys.Read.ShortCircuit.BUFFER_SIZE_DEFAULT);
      shortCircuitStreamsCacheSize = conf.getInt(
          HdfsClientConfigKeys.Read.ShortCircuit.STREAMS_CACHE_SIZE_KEY,
          HdfsClientConfigKeys.Read.ShortCircuit.STREAMS_CACHE_SIZE_DEFAULT);
      shortCircuitStreamsCacheExpiryMs = conf.getLong(
          HdfsClientConfigKeys.Read.ShortCircuit.STREAMS_CACHE_EXPIRY_MS_KEY,
          HdfsClientConfigKeys.Read.ShortCircuit.STREAMS_CACHE_EXPIRY_MS_DEFAULT);
      shortCircuitMmapEnabled = conf.getBoolean(
          HdfsClientConfigKeys.Mmap.ENABLED_KEY,
          HdfsClientConfigKeys.Mmap.ENABLED_DEFAULT);
      shortCircuitMmapCacheSize = conf.getInt(
          HdfsClientConfigKeys.Mmap.CACHE_SIZE_KEY,
          HdfsClientConfigKeys.Mmap.CACHE_SIZE_DEFAULT);
      shortCircuitMmapCacheExpiryMs = conf.getLong(
          HdfsClientConfigKeys.Mmap.CACHE_TIMEOUT_MS_KEY,
          HdfsClientConfigKeys.Mmap.CACHE_TIMEOUT_MS_DEFAULT);
      shortCircuitMmapCacheRetryTimeout = conf.getLong(
          HdfsClientConfigKeys.Mmap.RETRY_TIMEOUT_MS_KEY,
          HdfsClientConfigKeys.Mmap.RETRY_TIMEOUT_MS_DEFAULT);
      shortCircuitCacheStaleThresholdMs = conf.getLong(
          HdfsClientConfigKeys.ShortCircuit.REPLICA_STALE_THRESHOLD_MS_KEY,
          HdfsClientConfigKeys.ShortCircuit.REPLICA_STALE_THRESHOLD_MS_DEFAULT);
      shortCircuitSharedMemoryWatcherInterruptCheckMs = conf.getInt(
          DFSConfigKeys.DFS_SHORT_CIRCUIT_SHARED_MEMORY_WATCHER_INTERRUPT_CHECK_MS,
          DFSConfigKeys.DFS_SHORT_CIRCUIT_SHARED_MEMORY_WATCHER_INTERRUPT_CHECK_MS_DEFAULT);

      keyProviderCacheExpiryMs = conf.getLong(
          DFSConfigKeys.DFS_CLIENT_KEY_PROVIDER_CACHE_EXPIRY_MS,
          DFSConfigKeys.DFS_CLIENT_KEY_PROVIDER_CACHE_EXPIRY_DEFAULT);
    }

    /**
     * @return the socketCacheCapacity
     */
    public int getSocketCacheCapacity() {
      return socketCacheCapacity;
    }

    /**
     * @return the socketCacheExpiry
     */
    public long getSocketCacheExpiry() {
      return socketCacheExpiry;
    }

    public boolean isUseLegacyBlockReaderLocal() {
      return useLegacyBlockReaderLocal;
    }

    public String getDomainSocketPath() {
      return domainSocketPath;
    }

    public boolean isShortCircuitLocalReads() {
      return shortCircuitLocalReads;
    }

    public boolean isDomainSocketDataTraffic() {
      return domainSocketDataTraffic;
    }
    /**
     * @return the useLegacyBlockReader
     */
    public boolean isUseLegacyBlockReader() {
      return useLegacyBlockReader;
    }

    /**
     * @return the skipShortCircuitChecksums
     */
    public boolean isSkipShortCircuitChecksums() {
      return skipShortCircuitChecksums;
    }

    /**
     * @return the shortCircuitBufferSize
     */
    public int getShortCircuitBufferSize() {
      return shortCircuitBufferSize;
    }

    /**
     * @return the shortCircuitStreamsCacheSize
     */
    public int getShortCircuitStreamsCacheSize() {
      return shortCircuitStreamsCacheSize;
    }

    /**
     * @return the shortCircuitStreamsCacheExpiryMs
     */
    public long getShortCircuitStreamsCacheExpiryMs() {
      return shortCircuitStreamsCacheExpiryMs;
    }

    /**
     * @return the shortCircuitSharedMemoryWatcherInterruptCheckMs
     */
    public int getShortCircuitSharedMemoryWatcherInterruptCheckMs() {
      return shortCircuitSharedMemoryWatcherInterruptCheckMs;
    }

    /**
     * @return the shortCircuitMmapEnabled
     */
    public boolean isShortCircuitMmapEnabled() {
      return shortCircuitMmapEnabled;
    }

    /**
     * @return the shortCircuitMmapCacheSize
     */
    public int getShortCircuitMmapCacheSize() {
      return shortCircuitMmapCacheSize;
    }

    /**
     * @return the shortCircuitMmapCacheExpiryMs
     */
    public long getShortCircuitMmapCacheExpiryMs() {
      return shortCircuitMmapCacheExpiryMs;
    }

    /**
     * @return the shortCircuitMmapCacheRetryTimeout
     */
    public long getShortCircuitMmapCacheRetryTimeout() {
      return shortCircuitMmapCacheRetryTimeout;
    }

    /**
     * @return the shortCircuitCacheStaleThresholdMs
     */
    public long getShortCircuitCacheStaleThresholdMs() {
      return shortCircuitCacheStaleThresholdMs;
    }

    /**
     * @return the keyProviderCacheExpiryMs
     */
    public long getKeyProviderCacheExpiryMs() {
      return keyProviderCacheExpiryMs;
    }

    public String confAsString() {
      StringBuilder builder = new StringBuilder();
      builder.append("shortCircuitStreamsCacheSize = ").
        append(shortCircuitStreamsCacheSize).
        append(", shortCircuitStreamsCacheExpiryMs = ").
        append(shortCircuitStreamsCacheExpiryMs).
        append(", shortCircuitMmapCacheSize = ").
        append(shortCircuitMmapCacheSize).
        append(", shortCircuitMmapCacheExpiryMs = ").
        append(shortCircuitMmapCacheExpiryMs).
        append(", shortCircuitMmapCacheRetryTimeout = ").
        append(shortCircuitMmapCacheRetryTimeout).
        append(", shortCircuitCacheStaleThresholdMs = ").
        append(shortCircuitCacheStaleThresholdMs).
        append(", socketCacheCapacity = ").
        append(socketCacheCapacity).
        append(", socketCacheExpiry = ").
        append(socketCacheExpiry).
        append(", shortCircuitLocalReads = ").
        append(shortCircuitLocalReads).
        append(", useLegacyBlockReaderLocal = ").
        append(useLegacyBlockReaderLocal).
        append(", domainSocketDataTraffic = ").
        append(domainSocketDataTraffic).
        append(", shortCircuitSharedMemoryWatcherInterruptCheckMs = ").
        append(shortCircuitSharedMemoryWatcherInterruptCheckMs).
        append(", keyProviderCacheExpiryMs = ").
        append(keyProviderCacheExpiryMs);

      return builder.toString();
    }
  }
}
