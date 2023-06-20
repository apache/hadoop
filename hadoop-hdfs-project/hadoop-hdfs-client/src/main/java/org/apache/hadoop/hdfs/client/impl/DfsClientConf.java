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

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.util.Preconditions;
import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.Options.ChecksumCombineMode;
import org.apache.hadoop.fs.Options.ChecksumOpt;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.ReplicaAccessorBuilder;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.util.ByteArrayManager;
import org.apache.hadoop.ipc.Client;
import org.apache.hadoop.util.DataChecksum;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.apache.hadoop.hdfs.client.HdfsClientConfigKeys.BlockWrite;
import static org.apache.hadoop.hdfs.client.HdfsClientConfigKeys.DFS_BLOCK_SIZE_DEFAULT;
import static org.apache.hadoop.hdfs.client.HdfsClientConfigKeys.DFS_BLOCK_SIZE_KEY;
import static org.apache.hadoop.hdfs.client.HdfsClientConfigKeys.DFS_BYTES_PER_CHECKSUM_DEFAULT;
import static org.apache.hadoop.hdfs.client.HdfsClientConfigKeys.DFS_BYTES_PER_CHECKSUM_KEY;
import static org.apache.hadoop.hdfs.client.HdfsClientConfigKeys.DFS_CHECKSUM_COMBINE_MODE_DEFAULT;
import static org.apache.hadoop.hdfs.client.HdfsClientConfigKeys.DFS_CHECKSUM_COMBINE_MODE_KEY;
import static org.apache.hadoop.hdfs.client.HdfsClientConfigKeys.DFS_CHECKSUM_EC_SOCKET_TIMEOUT_DEFAULT;
import static org.apache.hadoop.hdfs.client.HdfsClientConfigKeys.DFS_CHECKSUM_EC_SOCKET_TIMEOUT_KEY;
import static org.apache.hadoop.hdfs.client.HdfsClientConfigKeys.DFS_CHECKSUM_TYPE_DEFAULT;
import static org.apache.hadoop.hdfs.client.HdfsClientConfigKeys.DFS_CHECKSUM_TYPE_KEY;
import static org.apache.hadoop.hdfs.client.HdfsClientConfigKeys.DFS_CLIENT_CACHED_CONN_RETRY_DEFAULT;
import static org.apache.hadoop.hdfs.client.HdfsClientConfigKeys.DFS_CLIENT_CACHED_CONN_RETRY_KEY;
import static org.apache.hadoop.hdfs.client.HdfsClientConfigKeys.DFS_CLIENT_DATANODE_RESTART_TIMEOUT_DEFAULT;
import static org.apache.hadoop.hdfs.client.HdfsClientConfigKeys.DFS_CLIENT_DATANODE_RESTART_TIMEOUT_KEY;
import static org.apache.hadoop.hdfs.client.HdfsClientConfigKeys.DFS_CLIENT_DEAD_NODE_DETECTION_ENABLED_DEFAULT;
import static org.apache.hadoop.hdfs.client.HdfsClientConfigKeys.DFS_CLIENT_DEAD_NODE_DETECTION_ENABLED_KEY;
import static org.apache.hadoop.hdfs.client.HdfsClientConfigKeys.DFS_CLIENT_DOMAIN_SOCKET_DATA_TRAFFIC;
import static org.apache.hadoop.hdfs.client.HdfsClientConfigKeys.DFS_CLIENT_DOMAIN_SOCKET_DATA_TRAFFIC_DEFAULT;
import static org.apache.hadoop.hdfs.client.HdfsClientConfigKeys.DFS_CLIENT_KEY_PROVIDER_CACHE_EXPIRY_DEFAULT;
import static org.apache.hadoop.hdfs.client.HdfsClientConfigKeys.DFS_CLIENT_KEY_PROVIDER_CACHE_EXPIRY_MS;
import static org.apache.hadoop.hdfs.client.HdfsClientConfigKeys.DFS_CLIENT_MARK_SLOWNODE_AS_BADNODE_THRESHOLD_DEFAULT;
import static org.apache.hadoop.hdfs.client.HdfsClientConfigKeys.DFS_CLIENT_MARK_SLOWNODE_AS_BADNODE_THRESHOLD_KEY;
import static org.apache.hadoop.hdfs.client.HdfsClientConfigKeys.DFS_CLIENT_MAX_BLOCK_ACQUIRE_FAILURES_DEFAULT;
import static org.apache.hadoop.hdfs.client.HdfsClientConfigKeys.DFS_CLIENT_MAX_BLOCK_ACQUIRE_FAILURES_KEY;
import static org.apache.hadoop.hdfs.client.HdfsClientConfigKeys.DFS_CLIENT_READ_USE_CACHE_PRIORITY;
import static org.apache.hadoop.hdfs.client.HdfsClientConfigKeys.DFS_CLIENT_READ_USE_CACHE_PRIORITY_DEFAULT;
import static org.apache.hadoop.hdfs.client.HdfsClientConfigKeys.DFS_CLIENT_SLOW_IO_WARNING_THRESHOLD_DEFAULT;
import static org.apache.hadoop.hdfs.client.HdfsClientConfigKeys.DFS_CLIENT_SLOW_IO_WARNING_THRESHOLD_KEY;
import static org.apache.hadoop.hdfs.client.HdfsClientConfigKeys.DFS_CLIENT_SOCKET_CACHE_CAPACITY_DEFAULT;
import static org.apache.hadoop.hdfs.client.HdfsClientConfigKeys.DFS_CLIENT_SOCKET_CACHE_CAPACITY_KEY;
import static org.apache.hadoop.hdfs.client.HdfsClientConfigKeys.DFS_CLIENT_SOCKET_CACHE_EXPIRY_MSEC_DEFAULT;
import static org.apache.hadoop.hdfs.client.HdfsClientConfigKeys.DFS_CLIENT_SOCKET_CACHE_EXPIRY_MSEC_KEY;
import static org.apache.hadoop.hdfs.client.HdfsClientConfigKeys.DFS_CLIENT_SOCKET_SEND_BUFFER_SIZE_DEFAULT;
import static org.apache.hadoop.hdfs.client.HdfsClientConfigKeys.DFS_CLIENT_SOCKET_SEND_BUFFER_SIZE_KEY;
import static org.apache.hadoop.hdfs.client.HdfsClientConfigKeys.DFS_CLIENT_SOCKET_TIMEOUT_KEY;
import static org.apache.hadoop.hdfs.client.HdfsClientConfigKeys.DFS_CLIENT_USE_DN_HOSTNAME;
import static org.apache.hadoop.hdfs.client.HdfsClientConfigKeys.DFS_CLIENT_USE_DN_HOSTNAME_DEFAULT;
import static org.apache.hadoop.hdfs.client.HdfsClientConfigKeys.DFS_CLIENT_USE_LEGACY_BLOCKREADERLOCAL;
import static org.apache.hadoop.hdfs.client.HdfsClientConfigKeys.DFS_CLIENT_USE_LEGACY_BLOCKREADERLOCAL_DEFAULT;
import static org.apache.hadoop.hdfs.client.HdfsClientConfigKeys.DFS_CLIENT_WRITE_PACKET_SIZE_DEFAULT;
import static org.apache.hadoop.hdfs.client.HdfsClientConfigKeys.DFS_CLIENT_WRITE_PACKET_SIZE_KEY;
import static org.apache.hadoop.hdfs.client.HdfsClientConfigKeys.DFS_DATANODE_SOCKET_WRITE_TIMEOUT_KEY;
import static org.apache.hadoop.hdfs.client.HdfsClientConfigKeys.DFS_DATA_TRANSFER_CLIENT_TCPNODELAY_DEFAULT;
import static org.apache.hadoop.hdfs.client.HdfsClientConfigKeys.DFS_DATA_TRANSFER_CLIENT_TCPNODELAY_KEY;
import static org.apache.hadoop.hdfs.client.HdfsClientConfigKeys.DFS_DOMAIN_SOCKET_DISABLE_INTERVAL_SECOND_DEFAULT;
import static org.apache.hadoop.hdfs.client.HdfsClientConfigKeys.DFS_DOMAIN_SOCKET_DISABLE_INTERVAL_SECOND_KEY;
import static org.apache.hadoop.hdfs.client.HdfsClientConfigKeys.DFS_DOMAIN_SOCKET_PATH_DEFAULT;
import static org.apache.hadoop.hdfs.client.HdfsClientConfigKeys.DFS_DOMAIN_SOCKET_PATH_KEY;
import static org.apache.hadoop.hdfs.client.HdfsClientConfigKeys.DFS_REPLICATION_DEFAULT;
import static org.apache.hadoop.hdfs.client.HdfsClientConfigKeys.DFS_REPLICATION_KEY;
import static org.apache.hadoop.hdfs.client.HdfsClientConfigKeys.DFS_SHORT_CIRCUIT_SHARED_MEMORY_WATCHER_INTERRUPT_CHECK_MS;
import static org.apache.hadoop.hdfs.client.HdfsClientConfigKeys.DFS_SHORT_CIRCUIT_SHARED_MEMORY_WATCHER_INTERRUPT_CHECK_MS_DEFAULT;
import static org.apache.hadoop.hdfs.client.HdfsClientConfigKeys.Failover;
import static org.apache.hadoop.hdfs.client.HdfsClientConfigKeys.HedgedRead;
import static org.apache.hadoop.hdfs.client.HdfsClientConfigKeys.Mmap;
import static org.apache.hadoop.hdfs.client.HdfsClientConfigKeys.Read;
import static org.apache.hadoop.hdfs.client.HdfsClientConfigKeys.Retry;
import static org.apache.hadoop.hdfs.client.HdfsClientConfigKeys.ShortCircuit;
import static org.apache.hadoop.hdfs.client.HdfsClientConfigKeys.Write;

/**
 * DFSClient configuration.
 */
public class DfsClientConf {
  private static final Logger LOG = LoggerFactory.getLogger(DfsClientConf
                                                                .class);

  private final int hdfsTimeout;    // timeout value for a DFS operation.

  private final int maxFailoverAttempts;
  private final int maxRetryAttempts;
  private final int maxPipelineRecoveryRetries;
  private final int failoverSleepBaseMillis;
  private final int failoverSleepMaxMillis;
  private final int maxBlockAcquireFailures;
  private final int datanodeSocketWriteTimeout;
  private final int ioBufferSize;
  private final ChecksumOpt defaultChecksumOpt;
  private final ChecksumCombineMode checksumCombineMode;
  private final int checksumEcSocketTimeout;
  private final int writePacketSize;
  private final int writeMaxPackets;
  private final ByteArrayManager.Conf writeByteArrayManagerConf;
  private final int socketTimeout;
  private final int socketSendBufferSize;
  private final long excludedNodesCacheExpiry;
  /** Wait time window (in msec) if BlockMissingException is caught. */
  private final int timeWindow;
  private final int numCachedConnRetry;
  private final int numBlockWriteRetry;
  private final int numBlockWriteLocateFollowingRetry;
  private final int blockWriteLocateFollowingInitialDelayMs;
  private final int blockWriteLocateFollowingMaxDelayMs;
  private final long defaultBlockSize;
  private final long prefetchSize;
  private final boolean uriCacheEnabled;
  private final short defaultReplication;
  private final String taskId;
  private final FsPermission uMask;
  private final boolean connectToDnViaHostname;
  private final int retryTimesForGetLastBlockLength;
  private final int retryIntervalForGetLastBlockLength;
  private final long datanodeRestartTimeout;
  private final long slowIoWarningThresholdMs;
  private final int markSlowNodeAsBadNodeThreshold;

  /** wait time window before refreshing blocklocation for inputstream. */
  private final long refreshReadBlockLocationsMS;
  private final boolean refreshReadBlockLocationsAutomatically;

  private final ShortCircuitConf shortCircuitConf;
  private final int clientShortCircuitNum;

  private final long hedgedReadThresholdMillis;
  private final int hedgedReadThreadpoolSize;
  private final List<Class<? extends ReplicaAccessorBuilder>>
      replicaAccessorBuilderClasses;

  private final int stripedReadThreadpoolSize;

  private final boolean dataTransferTcpNoDelay;

  private final boolean readUseCachePriority;

  private final boolean deadNodeDetectionEnabled;
  private final long leaseHardLimitPeriod;

  public DfsClientConf(Configuration conf) {
    // The hdfsTimeout is currently the same as the ipc timeout
    hdfsTimeout = Client.getRpcTimeout(conf);

    maxRetryAttempts = conf.getInt(
        Retry.MAX_ATTEMPTS_KEY,
        Retry.MAX_ATTEMPTS_DEFAULT);
    timeWindow = conf.getInt(
        Retry.WINDOW_BASE_KEY,
        Retry.WINDOW_BASE_DEFAULT);
    retryTimesForGetLastBlockLength = conf.getInt(
        Retry.TIMES_GET_LAST_BLOCK_LENGTH_KEY,
        Retry.TIMES_GET_LAST_BLOCK_LENGTH_DEFAULT);
    retryIntervalForGetLastBlockLength = conf.getInt(
        Retry.INTERVAL_GET_LAST_BLOCK_LENGTH_KEY,
        Retry.INTERVAL_GET_LAST_BLOCK_LENGTH_DEFAULT);

    maxFailoverAttempts = conf.getInt(
        Failover.MAX_ATTEMPTS_KEY,
        Failover.MAX_ATTEMPTS_DEFAULT);
    failoverSleepBaseMillis = conf.getInt(
        Failover.SLEEPTIME_BASE_KEY,
        Failover.SLEEPTIME_BASE_DEFAULT);
    failoverSleepMaxMillis = conf.getInt(
        Failover.SLEEPTIME_MAX_KEY,
        Failover.SLEEPTIME_MAX_DEFAULT);

    maxBlockAcquireFailures = conf.getInt(
        DFS_CLIENT_MAX_BLOCK_ACQUIRE_FAILURES_KEY,
        DFS_CLIENT_MAX_BLOCK_ACQUIRE_FAILURES_DEFAULT);
    datanodeSocketWriteTimeout = conf.getInt(
        DFS_DATANODE_SOCKET_WRITE_TIMEOUT_KEY,
        HdfsConstants.WRITE_TIMEOUT);
    ioBufferSize = conf.getInt(
        CommonConfigurationKeysPublic.IO_FILE_BUFFER_SIZE_KEY,
        CommonConfigurationKeysPublic.IO_FILE_BUFFER_SIZE_DEFAULT);
    defaultChecksumOpt = getChecksumOptFromConf(conf);
    checksumCombineMode = getChecksumCombineModeFromConf(conf);
    checksumEcSocketTimeout = conf.getInt(DFS_CHECKSUM_EC_SOCKET_TIMEOUT_KEY,
      DFS_CHECKSUM_EC_SOCKET_TIMEOUT_DEFAULT);
    dataTransferTcpNoDelay = conf.getBoolean(
        DFS_DATA_TRANSFER_CLIENT_TCPNODELAY_KEY,
        DFS_DATA_TRANSFER_CLIENT_TCPNODELAY_DEFAULT);
    socketTimeout = conf.getInt(DFS_CLIENT_SOCKET_TIMEOUT_KEY,
        HdfsConstants.READ_TIMEOUT);
    socketSendBufferSize = conf.getInt(DFS_CLIENT_SOCKET_SEND_BUFFER_SIZE_KEY,
        DFS_CLIENT_SOCKET_SEND_BUFFER_SIZE_DEFAULT);
    /** dfs.write.packet.size is an internal config variable */
    writePacketSize = conf.getInt(
        DFS_CLIENT_WRITE_PACKET_SIZE_KEY,
        DFS_CLIENT_WRITE_PACKET_SIZE_DEFAULT);
    writeMaxPackets = conf.getInt(
        Write.MAX_PACKETS_IN_FLIGHT_KEY,
        Write.MAX_PACKETS_IN_FLIGHT_DEFAULT);

    writeByteArrayManagerConf = loadWriteByteArrayManagerConf(conf);

    defaultBlockSize = conf.getLongBytes(DFS_BLOCK_SIZE_KEY,
        DFS_BLOCK_SIZE_DEFAULT);
    defaultReplication = (short) conf.getInt(
        DFS_REPLICATION_KEY, DFS_REPLICATION_DEFAULT);
    taskId = conf.get("mapreduce.task.attempt.id", "NONMAPREDUCE");
    excludedNodesCacheExpiry = conf.getLong(
        Write.EXCLUDE_NODES_CACHE_EXPIRY_INTERVAL_KEY,
        Write.EXCLUDE_NODES_CACHE_EXPIRY_INTERVAL_DEFAULT);
    prefetchSize = conf.getLong(Read.PREFETCH_SIZE_KEY,
        10 * defaultBlockSize);

    uriCacheEnabled = conf.getBoolean(Read.URI_CACHE_KEY,
        Read.URI_CACHE_DEFAULT);

    numCachedConnRetry = conf.getInt(DFS_CLIENT_CACHED_CONN_RETRY_KEY,
        DFS_CLIENT_CACHED_CONN_RETRY_DEFAULT);
    numBlockWriteRetry = conf.getInt(
        BlockWrite.RETRIES_KEY,
        BlockWrite.RETRIES_DEFAULT);
    numBlockWriteLocateFollowingRetry = conf.getInt(
        BlockWrite.LOCATEFOLLOWINGBLOCK_RETRIES_KEY,
        BlockWrite.LOCATEFOLLOWINGBLOCK_RETRIES_DEFAULT);
    blockWriteLocateFollowingInitialDelayMs = conf.getInt(
        BlockWrite.LOCATEFOLLOWINGBLOCK_INITIAL_DELAY_MS_KEY,
        BlockWrite.LOCATEFOLLOWINGBLOCK_INITIAL_DELAY_MS_DEFAULT);
    blockWriteLocateFollowingMaxDelayMs = conf.getInt(
        BlockWrite.LOCATEFOLLOWINGBLOCK_MAX_DELAY_MS_KEY,
        BlockWrite.LOCATEFOLLOWINGBLOCK_MAX_DELAY_MS_DEFAULT);
    uMask = FsPermission.getUMask(conf);
    connectToDnViaHostname = conf.getBoolean(DFS_CLIENT_USE_DN_HOSTNAME,
        DFS_CLIENT_USE_DN_HOSTNAME_DEFAULT);

    datanodeRestartTimeout = conf.getTimeDuration(
        DFS_CLIENT_DATANODE_RESTART_TIMEOUT_KEY,
        DFS_CLIENT_DATANODE_RESTART_TIMEOUT_DEFAULT,
        TimeUnit.SECONDS, TimeUnit.MILLISECONDS);
    slowIoWarningThresholdMs = conf.getLong(
        DFS_CLIENT_SLOW_IO_WARNING_THRESHOLD_KEY,
        DFS_CLIENT_SLOW_IO_WARNING_THRESHOLD_DEFAULT);
    readUseCachePriority = conf.getBoolean(DFS_CLIENT_READ_USE_CACHE_PRIORITY,
        DFS_CLIENT_READ_USE_CACHE_PRIORITY_DEFAULT);
    markSlowNodeAsBadNodeThreshold = conf.getInt(
        DFS_CLIENT_MARK_SLOWNODE_AS_BADNODE_THRESHOLD_KEY,
        DFS_CLIENT_MARK_SLOWNODE_AS_BADNODE_THRESHOLD_DEFAULT);

    refreshReadBlockLocationsMS = conf.getLong(
        HdfsClientConfigKeys.DFS_CLIENT_REFRESH_READ_BLOCK_LOCATIONS_MS_KEY,
        HdfsClientConfigKeys.
            DFS_CLIENT_REFRESH_READ_BLOCK_LOCATIONS_MS_DEFAULT);

    refreshReadBlockLocationsAutomatically = conf.getBoolean(
        HdfsClientConfigKeys.DFS_CLIENT_REFRESH_READ_BLOCK_LOCATIONS_AUTOMATICALLY_KEY,
        HdfsClientConfigKeys.DFS_CLIENT_REFRESH_READ_BLOCK_LOCATIONS_AUTOMATICALLY_DEFAULT);

    hedgedReadThresholdMillis = conf.getLong(
        HedgedRead.THRESHOLD_MILLIS_KEY,
        HedgedRead.THRESHOLD_MILLIS_DEFAULT);
    hedgedReadThreadpoolSize = conf.getInt(
        HdfsClientConfigKeys.HedgedRead.THREADPOOL_SIZE_KEY,
        HdfsClientConfigKeys.HedgedRead.THREADPOOL_SIZE_DEFAULT);

    deadNodeDetectionEnabled =
        conf.getBoolean(DFS_CLIENT_DEAD_NODE_DETECTION_ENABLED_KEY,
            DFS_CLIENT_DEAD_NODE_DETECTION_ENABLED_DEFAULT);

    stripedReadThreadpoolSize = conf.getInt(
        HdfsClientConfigKeys.StripedRead.THREADPOOL_SIZE_KEY,
        HdfsClientConfigKeys.StripedRead.THREADPOOL_SIZE_DEFAULT);
    Preconditions.checkArgument(stripedReadThreadpoolSize > 0, "The value of " +
        HdfsClientConfigKeys.StripedRead.THREADPOOL_SIZE_KEY +
        " must be greater than 0.");
    replicaAccessorBuilderClasses = loadReplicaAccessorBuilderClasses(conf);

    leaseHardLimitPeriod =
        conf.getLong(HdfsClientConfigKeys.DFS_LEASE_HARDLIMIT_KEY,
            HdfsClientConfigKeys.DFS_LEASE_HARDLIMIT_DEFAULT) * 1000;

    shortCircuitConf = new ShortCircuitConf(conf);
    clientShortCircuitNum = conf.getInt(
            HdfsClientConfigKeys.DFS_CLIENT_SHORT_CIRCUIT_NUM,
            HdfsClientConfigKeys.DFS_CLIENT_SHORT_CIRCUIT_NUM_DEFAULT);
    Preconditions.checkArgument(clientShortCircuitNum >= 1,
            HdfsClientConfigKeys.DFS_CLIENT_SHORT_CIRCUIT_NUM +
                    "can't be less then 1.");
    Preconditions.checkArgument(clientShortCircuitNum <= 5,
            HdfsClientConfigKeys.DFS_CLIENT_SHORT_CIRCUIT_NUM +
                    "can't be more then 5.");
    maxPipelineRecoveryRetries = conf.getInt(
        HdfsClientConfigKeys.DFS_CLIENT_PIPELINE_RECOVERY_MAX_RETRIES,
        HdfsClientConfigKeys.DFS_CLIENT_PIPELINE_RECOVERY_MAX_RETRIES_DEFAULT
    );
  }

  private ByteArrayManager.Conf loadWriteByteArrayManagerConf(
      Configuration conf) {
    final boolean byteArrayManagerEnabled = conf.getBoolean(
        Write.ByteArrayManager.ENABLED_KEY,
        Write.ByteArrayManager.ENABLED_DEFAULT);
    if (!byteArrayManagerEnabled) {
      return null;
    }
    final int countThreshold = conf.getInt(
        Write.ByteArrayManager.COUNT_THRESHOLD_KEY,
        Write.ByteArrayManager.COUNT_THRESHOLD_DEFAULT);
    final int countLimit = conf.getInt(
        Write.ByteArrayManager.COUNT_LIMIT_KEY,
        Write.ByteArrayManager.COUNT_LIMIT_DEFAULT);
    final long countResetTimePeriodMs = conf.getLong(
        Write.ByteArrayManager.COUNT_RESET_TIME_PERIOD_MS_KEY,
        Write.ByteArrayManager.COUNT_RESET_TIME_PERIOD_MS_DEFAULT);
    return new ByteArrayManager.Conf(
        countThreshold, countLimit, countResetTimePeriodMs);
  }

  @SuppressWarnings("unchecked")
  private List<Class<? extends ReplicaAccessorBuilder>>
      loadReplicaAccessorBuilderClasses(Configuration conf) {
    String[] classNames = conf.getTrimmedStrings(
        HdfsClientConfigKeys.REPLICA_ACCESSOR_BUILDER_CLASSES_KEY);
    if (classNames.length == 0) {
      return Collections.emptyList();
    }
    ArrayList<Class<? extends ReplicaAccessorBuilder>> classes =
        new ArrayList<>();
    ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
    for (String className: classNames) {
      try {
        Class<? extends ReplicaAccessorBuilder> cls =
            (Class<? extends ReplicaAccessorBuilder>)
                classLoader.loadClass(className);
        classes.add(cls);
      } catch (Throwable t) {
        LOG.warn("Unable to load {}", className, t);
      }
    }
    return classes;
  }

  private static DataChecksum.Type getChecksumType(Configuration conf) {
    final String checksum = conf.get(
        DFS_CHECKSUM_TYPE_KEY,
        DFS_CHECKSUM_TYPE_DEFAULT);
    try {
      return DataChecksum.Type.valueOf(checksum);
    } catch(IllegalArgumentException iae) {
      LOG.warn("Bad checksum type: {}. Using default {}", checksum,
               DFS_CHECKSUM_TYPE_DEFAULT);
      return DataChecksum.Type.valueOf(
          DFS_CHECKSUM_TYPE_DEFAULT);
    }
  }

  private static ChecksumCombineMode getChecksumCombineModeFromConf(
      Configuration conf) {
    final String mode = conf.get(
        DFS_CHECKSUM_COMBINE_MODE_KEY,
        DFS_CHECKSUM_COMBINE_MODE_DEFAULT);
    try {
      return ChecksumCombineMode.valueOf(mode);
    } catch(IllegalArgumentException iae) {
      LOG.warn("Bad checksum combine mode: {}. Using default {}", mode,
               DFS_CHECKSUM_COMBINE_MODE_DEFAULT);
      return ChecksumCombineMode.valueOf(
          DFS_CHECKSUM_COMBINE_MODE_DEFAULT);
    }
  }

  // Construct a checksum option from conf
  public static ChecksumOpt getChecksumOptFromConf(Configuration conf) {
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

  public int getBlockWriteLocateFollowingMaxDelayMs() {
    return blockWriteLocateFollowingMaxDelayMs;
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
   * @return the checksumCombineMode
   */
  public ChecksumCombineMode getChecksumCombineMode() {
    return checksumCombineMode;
  }

  /**
   * @return the checksumEcSocketTimeout
   */
  public int getChecksumEcSocketTimeout() {
    return checksumEcSocketTimeout;
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
   * @return whether TCP_NODELAY should be set on client sockets
   */
  public boolean getDataTransferTcpNoDelay() {
    return dataTransferTcpNoDelay;
  }

  /**
   * @return the socketTimeout
   */
  public int getSocketTimeout() {
    return socketTimeout;
  }

  /**
   * @return the socketSendBufferSize
   */
  public int getSocketSendBufferSize() {
    return socketSendBufferSize;
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
   * @return the uriCacheEnable
   */
  public boolean isUriCacheEnabled() {
    return uriCacheEnabled;
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
   * @return the continuous slowNode replies received to mark slowNode as badNode
   */
  public int getMarkSlowNodeAsBadNodeThreshold() {
    return markSlowNodeAsBadNodeThreshold;
  }

  /*
   * @return the clientShortCircuitNum
   */
  public int getClientShortCircuitNum() {
    return clientShortCircuitNum;
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
   * @return the deadNodeDetectionEnabled
   */
  public boolean isDeadNodeDetectionEnabled() {
    return deadNodeDetectionEnabled;
  }

  /**
   * @return the leaseHardLimitPeriod
   */
  public long getleaseHardLimitPeriod() {
    return leaseHardLimitPeriod;
  }

  /**
   * @return the readUseCachePriority
   */
  public boolean isReadUseCachePriority() {
    return readUseCachePriority;
  }

  /**
   * @return the replicaAccessorBuilderClasses
   */
  public List<Class<? extends ReplicaAccessorBuilder>>
        getReplicaAccessorBuilderClasses() {
    return replicaAccessorBuilderClasses;
  }

  public boolean isLocatedBlocksRefresherEnabled() {
    return refreshReadBlockLocationsMS > 0;
  }

  public long getLocatedBlocksRefresherInterval() {
    return refreshReadBlockLocationsMS;
  }

  public boolean isRefreshReadBlockLocationsAutomatically() {
    return refreshReadBlockLocationsAutomatically;
  }

  /**
   * @return the shortCircuitConf
   */
  public ShortCircuitConf getShortCircuitConf() {
    return shortCircuitConf;
  }

  /**
   *@return the maxPipelineRecoveryRetries
   */
  public int getMaxPipelineRecoveryRetries() {
    return maxPipelineRecoveryRetries;
  }

  /**
   * Configuration for short-circuit reads.
   */
  public static class ShortCircuitConf {
    private static final Logger LOG = DfsClientConf.LOG;

    private final int socketCacheCapacity;
    private final long socketCacheExpiry;

    private final boolean useLegacyBlockReaderLocal;
    private final String domainSocketPath;
    private final boolean skipShortCircuitChecksums;

    private final int shortCircuitBufferSize;
    private final boolean shortCircuitLocalReads;
    private final boolean domainSocketDataTraffic;
    private final int shortCircuitStreamsCacheSize;
    private final long shortCircuitStreamsCacheExpiryMs;
    private final int shortCircuitSharedMemoryWatcherInterruptCheckMs;

    // Short Circuit Read Metrics
    private final boolean scrMetricsEnabled;
    private final int scrMetricsSamplingPercentage;

    private final boolean shortCircuitMmapEnabled;
    private final int shortCircuitMmapCacheSize;
    private final long shortCircuitMmapCacheExpiryMs;
    private final long shortCircuitMmapCacheRetryTimeout;
    private final long shortCircuitCacheStaleThresholdMs;
    private final long domainSocketDisableIntervalSeconds;

    private final long keyProviderCacheExpiryMs;

    public ShortCircuitConf(Configuration conf) {
      socketCacheCapacity = conf.getInt(
          DFS_CLIENT_SOCKET_CACHE_CAPACITY_KEY,
          DFS_CLIENT_SOCKET_CACHE_CAPACITY_DEFAULT);
      socketCacheExpiry = conf.getLong(
          DFS_CLIENT_SOCKET_CACHE_EXPIRY_MSEC_KEY,
          DFS_CLIENT_SOCKET_CACHE_EXPIRY_MSEC_DEFAULT);

      useLegacyBlockReaderLocal = conf.getBoolean(
          DFS_CLIENT_USE_LEGACY_BLOCKREADERLOCAL,
          DFS_CLIENT_USE_LEGACY_BLOCKREADERLOCAL_DEFAULT);
      shortCircuitLocalReads = conf.getBoolean(
          Read.ShortCircuit.KEY,
          Read.ShortCircuit.DEFAULT);
      int scrSamplingPercentage = conf.getInt(
          Read.ShortCircuit.METRICS_SAMPLING_PERCENTAGE_KEY,
          Read.ShortCircuit.METRICS_SAMPLING_PERCENTAGE_DEFAULT);
      if (scrSamplingPercentage <= 0) {
        scrMetricsSamplingPercentage = 0;
        scrMetricsEnabled = false;
      } else if (scrSamplingPercentage > 100) {
        scrMetricsSamplingPercentage = 100;
        scrMetricsEnabled = true;
      } else {
        scrMetricsSamplingPercentage = scrSamplingPercentage;
        scrMetricsEnabled = true;
      }

      domainSocketDataTraffic = conf.getBoolean(
          DFS_CLIENT_DOMAIN_SOCKET_DATA_TRAFFIC,
          DFS_CLIENT_DOMAIN_SOCKET_DATA_TRAFFIC_DEFAULT);
      domainSocketPath = conf.getTrimmed(
          DFS_DOMAIN_SOCKET_PATH_KEY,
          DFS_DOMAIN_SOCKET_PATH_DEFAULT);

      LOG.debug(DFS_CLIENT_USE_LEGACY_BLOCKREADERLOCAL
                    + " = {}", useLegacyBlockReaderLocal);
      LOG.debug(Read.ShortCircuit.KEY
                    + " = {}", shortCircuitLocalReads);
      LOG.debug(DFS_CLIENT_DOMAIN_SOCKET_DATA_TRAFFIC
                    + " = {}", domainSocketDataTraffic);
      LOG.debug(DFS_DOMAIN_SOCKET_PATH_KEY
                    + " = {}", domainSocketPath);

      skipShortCircuitChecksums = conf.getBoolean(
          Read.ShortCircuit.SKIP_CHECKSUM_KEY,
          Read.ShortCircuit.SKIP_CHECKSUM_DEFAULT);
      shortCircuitBufferSize = conf.getInt(
          Read.ShortCircuit.BUFFER_SIZE_KEY,
          Read.ShortCircuit.BUFFER_SIZE_DEFAULT);
      shortCircuitStreamsCacheSize = conf.getInt(
          Read.ShortCircuit.STREAMS_CACHE_SIZE_KEY,
          Read.ShortCircuit.STREAMS_CACHE_SIZE_DEFAULT);
      shortCircuitStreamsCacheExpiryMs = conf.getLong(
          Read.ShortCircuit.STREAMS_CACHE_EXPIRY_MS_KEY,
          Read.ShortCircuit.STREAMS_CACHE_EXPIRY_MS_DEFAULT);
      shortCircuitMmapEnabled = conf.getBoolean(
          Mmap.ENABLED_KEY,
          Mmap.ENABLED_DEFAULT);
      shortCircuitMmapCacheSize = conf.getInt(
          Mmap.CACHE_SIZE_KEY,
          Mmap.CACHE_SIZE_DEFAULT);
      shortCircuitMmapCacheExpiryMs = conf.getLong(
          Mmap.CACHE_TIMEOUT_MS_KEY,
          Mmap.CACHE_TIMEOUT_MS_DEFAULT);
      shortCircuitMmapCacheRetryTimeout = conf.getLong(
          Mmap.RETRY_TIMEOUT_MS_KEY,
          Mmap.RETRY_TIMEOUT_MS_DEFAULT);
      shortCircuitCacheStaleThresholdMs = conf.getLong(
          ShortCircuit.REPLICA_STALE_THRESHOLD_MS_KEY,
          ShortCircuit.REPLICA_STALE_THRESHOLD_MS_DEFAULT);
      shortCircuitSharedMemoryWatcherInterruptCheckMs = conf.getInt(
          DFS_SHORT_CIRCUIT_SHARED_MEMORY_WATCHER_INTERRUPT_CHECK_MS,
          DFS_SHORT_CIRCUIT_SHARED_MEMORY_WATCHER_INTERRUPT_CHECK_MS_DEFAULT);
      domainSocketDisableIntervalSeconds = conf.getLong(
          DFS_DOMAIN_SOCKET_DISABLE_INTERVAL_SECOND_KEY,
          DFS_DOMAIN_SOCKET_DISABLE_INTERVAL_SECOND_DEFAULT);
      Preconditions.checkArgument(domainSocketDisableIntervalSeconds >= 0,
          DFS_DOMAIN_SOCKET_DISABLE_INTERVAL_SECOND_KEY + "can't be negative.");

      keyProviderCacheExpiryMs = conf.getLong(
          DFS_CLIENT_KEY_PROVIDER_CACHE_EXPIRY_MS,
          DFS_CLIENT_KEY_PROVIDER_CACHE_EXPIRY_DEFAULT);
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

    public boolean isScrMetricsEnabled() {
      return scrMetricsEnabled;
    }

    public int getScrMetricsSamplingPercentage() {
      return scrMetricsSamplingPercentage;
    }

    public boolean isDomainSocketDataTraffic() {
      return domainSocketDataTraffic;
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
     * @return the domainSocketDisableIntervalSeconds
     */
    public long getDomainSocketDisableIntervalSeconds() {
      return domainSocketDisableIntervalSeconds;
    }

    /**
     * @return the keyProviderCacheExpiryMs
     */
    public long getKeyProviderCacheExpiryMs() {
      return keyProviderCacheExpiryMs;
    }

    public String confAsString() {

      return "shortCircuitStreamsCacheSize = "
          + shortCircuitStreamsCacheSize
          + ", shortCircuitStreamsCacheExpiryMs = "
          + shortCircuitStreamsCacheExpiryMs
          + ", shortCircuitMmapCacheSize = "
          + shortCircuitMmapCacheSize
          + ", shortCircuitMmapCacheExpiryMs = "
          + shortCircuitMmapCacheExpiryMs
          + ", shortCircuitMmapCacheRetryTimeout = "
          + shortCircuitMmapCacheRetryTimeout
          + ", shortCircuitCacheStaleThresholdMs = "
          + shortCircuitCacheStaleThresholdMs
          + ", socketCacheCapacity = "
          + socketCacheCapacity
          + ", socketCacheExpiry = "
          + socketCacheExpiry
          + ", shortCircuitLocalReads = "
          + shortCircuitLocalReads
          + ", useLegacyBlockReaderLocal = "
          + useLegacyBlockReaderLocal
          + ", domainSocketDataTraffic = "
          + domainSocketDataTraffic
          + ", shortCircuitSharedMemoryWatcherInterruptCheckMs = "
          + shortCircuitSharedMemoryWatcherInterruptCheckMs
          + ", keyProviderCacheExpiryMs = "
          + keyProviderCacheExpiryMs
          + ", domainSocketDisableIntervalSeconds = "
          + domainSocketDisableIntervalSeconds;
    }
  }
}
