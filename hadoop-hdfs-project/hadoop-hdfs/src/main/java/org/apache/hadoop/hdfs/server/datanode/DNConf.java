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
package org.apache.hadoop.hdfs.server.datanode;

import org.apache.hadoop.classification.InterfaceAudience;

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_BLOCKREPORT_INITIAL_DELAY_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_BLOCKREPORT_INITIAL_DELAY_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_BLOCKREPORT_INTERVAL_MSEC_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_BLOCKREPORT_INTERVAL_MSEC_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_BLOCKREPORT_SPLIT_THRESHOLD_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_BLOCKREPORT_SPLIT_THRESHOLD_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_CACHEREPORT_INTERVAL_MSEC_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_CACHEREPORT_INTERVAL_MSEC_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_CLIENT_SOCKET_TIMEOUT_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_CLIENT_WRITE_PACKET_SIZE_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_CLIENT_WRITE_PACKET_SIZE_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_MAX_LOCKED_MEMORY_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_MAX_LOCKED_MEMORY_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_SOCKET_WRITE_TIMEOUT_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_SYNCONCLOSE_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_SYNCONCLOSE_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_TRANSFERTO_ALLOWED_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_TRANSFERTO_ALLOWED_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_XCEIVER_STOP_TIMEOUT_MILLIS_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_XCEIVER_STOP_TIMEOUT_MILLIS_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_MIN_SUPPORTED_NAMENODE_VERSION_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_MIN_SUPPORTED_NAMENODE_VERSION_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_ENCRYPT_DATA_TRANSFER_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_ENCRYPT_DATA_TRANSFER_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATA_ENCRYPTION_ALGORITHM_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_RESTART_REPLICA_EXPIRY_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_RESTART_REPLICA_EXPIRY_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.IGNORE_SECURE_PORTS_FOR_TESTING_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.IGNORE_SECURE_PORTS_FOR_TESTING_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_BP_READY_TIMEOUT_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_BP_READY_TIMEOUT_DEFAULT;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.protocol.datatransfer.TrustedChannelResolver;
import org.apache.hadoop.hdfs.protocol.datatransfer.sasl.DataTransferSaslUtil;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.security.SaslPropertiesResolver;

/**
 * Simple class encapsulating all of the configuration that the DataNode
 * loads at startup time.
 */
@InterfaceAudience.Private
public class DNConf {
  final Configuration conf;
  final int socketTimeout;
  final int socketWriteTimeout;
  final int socketKeepaliveTimeout;
  
  final boolean transferToAllowed;
  final boolean dropCacheBehindWrites;
  final boolean syncBehindWrites;
  final boolean syncBehindWritesInBackground;
  final boolean dropCacheBehindReads;
  final boolean syncOnClose;
  final boolean encryptDataTransfer;
  final boolean connectToDnViaHostname;

  final long readaheadLength;
  final long heartBeatInterval;
  final long blockReportInterval;
  final long blockReportSplitThreshold;
  final long deleteReportInterval;
  final long initialBlockReportDelay;
  final long cacheReportInterval;
  final long dfsclientSlowIoWarningThresholdMs;
  final long datanodeSlowIoWarningThresholdMs;
  final int writePacketSize;
  
  final String minimumNameNodeVersion;
  final String encryptionAlgorithm;
  final SaslPropertiesResolver saslPropsResolver;
  final TrustedChannelResolver trustedChannelResolver;
  private final boolean ignoreSecurePortsForTesting;
  
  final long xceiverStopTimeout;
  final long restartReplicaExpiry;

  final long maxLockedMemory;

  private final long bpReadyTimeout;

  public DNConf(Configuration conf) {
    this.conf = conf;
    socketTimeout = conf.getInt(DFS_CLIENT_SOCKET_TIMEOUT_KEY,
        HdfsServerConstants.READ_TIMEOUT);
    socketWriteTimeout = conf.getInt(DFS_DATANODE_SOCKET_WRITE_TIMEOUT_KEY,
        HdfsServerConstants.WRITE_TIMEOUT);
    socketKeepaliveTimeout = conf.getInt(
        DFSConfigKeys.DFS_DATANODE_SOCKET_REUSE_KEEPALIVE_KEY,
        DFSConfigKeys.DFS_DATANODE_SOCKET_REUSE_KEEPALIVE_DEFAULT);
    
    /* Based on results on different platforms, we might need set the default 
     * to false on some of them. */
    transferToAllowed = conf.getBoolean(
        DFS_DATANODE_TRANSFERTO_ALLOWED_KEY,
        DFS_DATANODE_TRANSFERTO_ALLOWED_DEFAULT);

    writePacketSize = conf.getInt(DFS_CLIENT_WRITE_PACKET_SIZE_KEY, 
        DFS_CLIENT_WRITE_PACKET_SIZE_DEFAULT);
    
    readaheadLength = conf.getLong(
        DFSConfigKeys.DFS_DATANODE_READAHEAD_BYTES_KEY,
        DFSConfigKeys.DFS_DATANODE_READAHEAD_BYTES_DEFAULT);
    dropCacheBehindWrites = conf.getBoolean(
        DFSConfigKeys.DFS_DATANODE_DROP_CACHE_BEHIND_WRITES_KEY,
        DFSConfigKeys.DFS_DATANODE_DROP_CACHE_BEHIND_WRITES_DEFAULT);
    syncBehindWrites = conf.getBoolean(
        DFSConfigKeys.DFS_DATANODE_SYNC_BEHIND_WRITES_KEY,
        DFSConfigKeys.DFS_DATANODE_SYNC_BEHIND_WRITES_DEFAULT);
    syncBehindWritesInBackground = conf.getBoolean(
        DFSConfigKeys.DFS_DATANODE_SYNC_BEHIND_WRITES_IN_BACKGROUND_KEY,
        DFSConfigKeys.DFS_DATANODE_SYNC_BEHIND_WRITES_IN_BACKGROUND_DEFAULT);
    dropCacheBehindReads = conf.getBoolean(
        DFSConfigKeys.DFS_DATANODE_DROP_CACHE_BEHIND_READS_KEY,
        DFSConfigKeys.DFS_DATANODE_DROP_CACHE_BEHIND_READS_DEFAULT);
    connectToDnViaHostname = conf.getBoolean(
        DFSConfigKeys.DFS_DATANODE_USE_DN_HOSTNAME,
        DFSConfigKeys.DFS_DATANODE_USE_DN_HOSTNAME_DEFAULT);
    this.blockReportInterval = conf.getLong(DFS_BLOCKREPORT_INTERVAL_MSEC_KEY,
        DFS_BLOCKREPORT_INTERVAL_MSEC_DEFAULT);
    this.blockReportSplitThreshold = conf.getLong(DFS_BLOCKREPORT_SPLIT_THRESHOLD_KEY,
                                            DFS_BLOCKREPORT_SPLIT_THRESHOLD_DEFAULT);
    this.cacheReportInterval = conf.getLong(DFS_CACHEREPORT_INTERVAL_MSEC_KEY,
        DFS_CACHEREPORT_INTERVAL_MSEC_DEFAULT);

    this.dfsclientSlowIoWarningThresholdMs = conf.getLong(
        DFSConfigKeys.DFS_CLIENT_SLOW_IO_WARNING_THRESHOLD_KEY,
        DFSConfigKeys.DFS_CLIENT_SLOW_IO_WARNING_THRESHOLD_DEFAULT);
    this.datanodeSlowIoWarningThresholdMs = conf.getLong(
        DFSConfigKeys.DFS_DATANODE_SLOW_IO_WARNING_THRESHOLD_KEY,
        DFSConfigKeys.DFS_DATANODE_SLOW_IO_WARNING_THRESHOLD_DEFAULT);

    long initBRDelay = conf.getLong(
        DFS_BLOCKREPORT_INITIAL_DELAY_KEY,
        DFS_BLOCKREPORT_INITIAL_DELAY_DEFAULT) * 1000L;
    if (initBRDelay >= blockReportInterval) {
      initBRDelay = 0;
      DataNode.LOG.info("dfs.blockreport.initialDelay is greater than " +
          "dfs.blockreport.intervalMsec." + " Setting initial delay to 0 msec:");
    }
    initialBlockReportDelay = initBRDelay;
    
    heartBeatInterval = conf.getLong(DFS_HEARTBEAT_INTERVAL_KEY,
        DFS_HEARTBEAT_INTERVAL_DEFAULT) * 1000L;
    
    this.deleteReportInterval = 100 * heartBeatInterval;
    // do we need to sync block file contents to disk when blockfile is closed?
    this.syncOnClose = conf.getBoolean(DFS_DATANODE_SYNCONCLOSE_KEY, 
        DFS_DATANODE_SYNCONCLOSE_DEFAULT);

    this.minimumNameNodeVersion = conf.get(DFS_DATANODE_MIN_SUPPORTED_NAMENODE_VERSION_KEY,
        DFS_DATANODE_MIN_SUPPORTED_NAMENODE_VERSION_DEFAULT);
    
    this.encryptDataTransfer = conf.getBoolean(DFS_ENCRYPT_DATA_TRANSFER_KEY,
        DFS_ENCRYPT_DATA_TRANSFER_DEFAULT);
    this.encryptionAlgorithm = conf.get(DFS_DATA_ENCRYPTION_ALGORITHM_KEY);
    this.trustedChannelResolver = TrustedChannelResolver.getInstance(conf);
    this.saslPropsResolver = DataTransferSaslUtil.getSaslPropertiesResolver(
      conf);
    this.ignoreSecurePortsForTesting = conf.getBoolean(
        IGNORE_SECURE_PORTS_FOR_TESTING_KEY,
        IGNORE_SECURE_PORTS_FOR_TESTING_DEFAULT);
    
    this.xceiverStopTimeout = conf.getLong(
        DFS_DATANODE_XCEIVER_STOP_TIMEOUT_MILLIS_KEY,
        DFS_DATANODE_XCEIVER_STOP_TIMEOUT_MILLIS_DEFAULT);

    this.maxLockedMemory = conf.getLong(
        DFS_DATANODE_MAX_LOCKED_MEMORY_KEY,
        DFS_DATANODE_MAX_LOCKED_MEMORY_DEFAULT);

    this.restartReplicaExpiry = conf.getLong(
        DFS_DATANODE_RESTART_REPLICA_EXPIRY_KEY,
        DFS_DATANODE_RESTART_REPLICA_EXPIRY_DEFAULT) * 1000L;

    this.bpReadyTimeout = conf.getLong(
        DFS_DATANODE_BP_READY_TIMEOUT_KEY,
        DFS_DATANODE_BP_READY_TIMEOUT_DEFAULT);
  }

  // We get minimumNameNodeVersion via a method so it can be mocked out in tests.
  String getMinimumNameNodeVersion() {
    return this.minimumNameNodeVersion;
  }
  
  /**
   * Returns the configuration.
   * 
   * @return Configuration the configuration
   */
  public Configuration getConf() {
    return conf;
  }

  /**
   * Returns true if encryption enabled for DataTransferProtocol.
   *
   * @return boolean true if encryption enabled for DataTransferProtocol
   */
  public boolean getEncryptDataTransfer() {
    return encryptDataTransfer;
  }

  /**
   * Returns encryption algorithm configured for DataTransferProtocol, or null
   * if not configured.
   *
   * @return encryption algorithm configured for DataTransferProtocol
   */
  public String getEncryptionAlgorithm() {
    return encryptionAlgorithm;
  }

  public long getXceiverStopTimeout() {
    return xceiverStopTimeout;
  }

  public long getMaxLockedMemory() {
    return maxLockedMemory;
  }

  /**
   * Returns the SaslPropertiesResolver configured for use with
   * DataTransferProtocol, or null if not configured.
   *
   * @return SaslPropertiesResolver configured for use with DataTransferProtocol
   */
  public SaslPropertiesResolver getSaslPropsResolver() {
    return saslPropsResolver;
  }

  /**
   * Returns the TrustedChannelResolver configured for use with
   * DataTransferProtocol, or null if not configured.
   *
   * @return TrustedChannelResolver configured for use with DataTransferProtocol
   */
  public TrustedChannelResolver getTrustedChannelResolver() {
    return trustedChannelResolver;
  }

  /**
   * Returns true if configuration is set to skip checking for proper
   * port configuration in a secured cluster.  This is only intended for use in
   * dev testing.
   *
   * @return true if configured to skip checking secured port configuration
   */
  public boolean getIgnoreSecurePortsForTesting() {
    return ignoreSecurePortsForTesting;
  }

  public long getBpReadyTimeout() {
    return bpReadyTimeout;
  }
}
