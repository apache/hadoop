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

package org.apache.hadoop.hdfs.server.datanode.metrics;

/**
 * 
 * This is the JMX  interface for the runtime statistics for the data node.
 * Many of the statistics are sampled and averaged on an interval 
 * which can be specified in the config file.
 * <p>
 * For the statistics that are sampled and averaged, one must specify 
 * a metrics context that does periodic update calls. Most do.
 * The default Null metrics context however does NOT. So if you aren't
 * using any other metrics context then you can turn on the viewing and averaging
 * of sampled metrics by  specifying the following two lines
 *  in the hadoop-meterics.properties file:
 *  <pre>
 *        dfs.class=org.apache.hadoop.metrics.spi.NullContextWithUpdateThread
 *        dfs.period=10
 *  </pre>
 *<p>
 * Note that the metrics are collected regardless of the context used.
 * The context with the update thread is used to average the data periodically.
 * <p>
 * Name Node Status info is reported in another MBean
 * @see org.apache.hadoop.hdfs.server.datanode.metrics.FSDatasetMBean
 *
 */
public interface DataNodeStatisticsMBean {
  
  /**
   *   Number of bytes written in the last interval
   * @return number of bytes written
   */
  long getBytesWritten();
  
  /**
   *   Number of bytes read in the last interval
   * @return number of bytes read
   */
  long getBytesRead();
  
  /**
   *   Number of blocks written in the last interval
   * @return number of blocks written
   */
  int getBlocksWritten(); 
  
  /**
   *   Number of blocks read in the last interval
   * @return number of blocks read
   */
  int getBlocksRead(); 
  
  /**
   *   Number of blocks replicated in the last interval
   * @return number of blocks replicated
   */
  int getBlocksReplicated();
  
  /**
   *   Number of blocks removed in the last interval
   * @return number of blocks removed
   */
  int getBlocksRemoved();
  
  /**
   *   Number of blocks verified in the last interval
   * @return number of blocks verified
   */
  int getBlocksVerified();
  
  /**
   *   Number of block verification failures in the last interval
   * @return number of block verification failures
   */
  int getBlockVerificationFailures();
  
  /**
   * Number of reads from local clients in the last interval
   * @return number of reads from local clients
   */
  int getReadsFromLocalClient();
  
  
  /**
   * Number of reads from remote clients in the last interval
   * @return number of reads from remote clients
   */
  int getReadsFromRemoteClient();
  
  
  /**
   * Number of writes from local clients in the last interval
   * @return number of writes from local clients
   */
  int getWritesFromLocalClient();
  
  
  /**
   * Number of writes from remote clients in the last interval
   * @return number of writes from remote clients
   */
  int getWritesFromRemoteClient();

  /**
   * Number of ReadBlock Operation in last interval
   * @return number of operations
   */
  int getReadBlockOpNum();  

  /**
   * Average time for ReadBlock Operation in last interval
   * @return time in msec
   */
  long getReadBlockOpAverageTime();
  
  /**
   *   The Minimum ReadBlock Operation Time since reset was called
   * @return time in msec
   */
  long getReadBlockOpMinTime();
  
  /**
   *   The Maximum ReadBlock Operation Time since reset was called
   * @return time in msec
   */
  long getReadBlockOpMaxTime();
  
  /**
   * Number of WriteBlock Operation in last interval
   * @return number of operations
   */
  int getWriteBlockOpNum();

  /**
   * Average time for WriteBlock Operation in last interval
   * @return time in msec
   */
  long getWriteBlockOpAverageTime();
  
  /**
   *   The Minimum WriteBlock Operation Time since reset was called
   * @return time in msec
   */
  long getWriteBlockOpMinTime();
  
  /**
   *   The Maximum WriteBlock Operation Time since reset was called
   * @return time in msec
   */
  long getWriteBlockOpMaxTime(); 
  
  /**
   * Number of ReadMetadata Operation in last interval
   * @return number of operations
   */
  int getReadMetadataOpNum(); 

  /**
   * Average time for ReadMetadata Operation in last interval
   * @return time in msec
   */
  long getReadMetadataOpAverageTime();
  
  /**
   *   The Minimum ReadMetadata Operation Time since reset was called
   * @return time in msec
   */
  long getReadMetadataOpMinTime();
  
  /**
   *   The Maximum ReadMetadata Operation Time since reset was called
   * @return time in msec
   */
  long getReadMetadataOpMaxTime();
  
  /**
   * Number of block BlockChecksum in last interval
   * @return number of operations
   */
  int getBlockChecksumOpNum(); 

  /**
   * Average time for BlockChecksum Operation in last interval
   * @return time in msec
   */
  long getBlockChecksumOpAverageTime();
  
  /**
   *   The Minimum BlockChecksum Operation Time since reset was called
   * @return time in msec
   */
  long getBlockChecksumOpMinTime();
  
  /**
   *   The Maximum BlockChecksum Operation Time since reset was called
   * @return time in msec
   */
  long getBlockChecksumOpMaxTime();
  
  /**
   * Number of CopyBlock Operation in last interval
   * @return number of operations
   */
  int getCopyBlockOpNum();

  /**
   * Average time for CopyBlock Operation in last interval
   * @return time in msec
   */
  long getCopyBlockOpAverageTime();
  
  /**
   *   The Minimum CopyBlock Operation Time since reset was called
   * @return time in msec
   */
  long getCopyBlockOpMinTime();
  
  /**
   *   The Maximum CopyBlock Operation Time since reset was called
   * @return time in msec
   */
  long getCopyBlockOpMaxTime();

  /**
   * Number of ReplaceBlock Operation in last interval
   * @return number of operations
   */
  int getReplaceBlockOpNum();
  

  /**
   * Average time for ReplaceBlock Operation in last interval
   * @return time in msec
   */
  long getReplaceBlockOpAverageTime();
  
  /**
   *   The Minimum ReplaceBlock Operation Time since reset was called
   * @return time in msec
   */
  long getReplaceBlockOpMinTime();
  
  /**
   *   The Maximum ReplaceBlock Operation Time since reset was called
   * @return time in msec
   */
  long getReplaceBlockOpMaxTime();
  
  /**
   * Number of Block Reports sent in last interval
   * @return number of operations
   */
  int getBlockReportsNum();

  /**
   * Average time for Block Reports Operation in last interval
   * @return time in msec
   */
  long getBlockReportsAverageTime();
  
  /**
   *   The Minimum Block Reports Operation Time since reset was called
   * @return time in msec
   */
  long getBlockReportsMinTime();
  
  /**
   *   The Maximum Block Reports Operation Time since reset was called
   * @return time in msec
   */
  long getBlockReportsMaxTime();

  /**
   * Number of Heartbeat Operation in last interval
   * @return number of operations
   */
  int getHeartbeatsNum();

  /**
   * Average time for Heartbeat Operation in last interval
   * @return time in msec
   */
  long getHeartbeatsAverageTime();
  
  /**
   *   The Minimum Heartbeat Operation Time since reset was called
   * @return time in msec
   */
  long getHeartbeatsMinTime();
  
  /**
   *   The Maximum Heartbeat Operation Time since reset was called
   * @return time in msec
   */
  long getHeartbeatsMaxTime();
  
  
  /**
   * Reset all min max times
   */
  public void resetAllMinMax();
}
