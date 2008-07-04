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

package org.apache.hadoop.hdfs.server.namenode.metrics;

/**
 * 
 * This is the JMX management interface for getting runtime statistics of
 * the name node.
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
 * Name Node Status info is report in another MBean
 * @see org.apache.hadoop.hdfs.server.namenode.metrics.FSNamesystemMBean
 *
 */
public interface NameNodeStatisticsMBean {
  
  /**
   * The time spent in the Safemode at startup
   * @return time in msec
   */
  int getSafemodeTime();
  
  /**
   * Time spent loading the FS Image at startup
   * @return time in msec
   */
  int getFSImageLoadTime();
  
  /**
   * Number of Journal Transactions in the last interval
   * @return number of operations
   */
  int getJournalTransactionNum();
  
  /**
   * Average time for Journal transactions in last interval
   * @return time in msec
   */
  long getJournalTransactionAverageTime();
  
  /**
   * The Minimum Journal Transaction Time since reset was called
   * @return time in msec
   */
  long getJournalTransactionMinTime();
  
  /**
   *  The Maximum Journal Transaction Time since reset was called
   * @return time in msec
   */
  long getJournalTransactionMaxTime();
  
  /**
   *  Number of block Reports processed in the last interval
   * @return number of operations
   */
  int getBlockReportNum();
  
  /**
   * Average time for Block Report Processing in last interval
   * @return time in msec
   */
  long getBlockReportAverageTime();
  
  /**
   *  The Minimum Block Report Processing Time since reset was called
   * @return time in msec
   */
  long getBlockReportMinTime();
  
  /**
   *  The Maximum Block Report Processing Time since reset was called
   * @return time in msec
   */
  long getBlockReportMaxTime();
  
  /**
   *  Number of Journal Syncs in the last interval
   * @return number of operations
   */
  int getJournalSyncNum();
  
  /**
   * Average time for Journal Sync in last interval
   * @return time in msec
   */
  long getJournalSyncAverageTime();
  
  /**
   *  The Minimum Journal Sync Time since reset was called
   * @return time in msec
   */
  long getJournalSyncMinTime();
  
  /**
   *   The Maximum Journal Sync Time since reset was called
   * @return time in msec
   */
  long getJournalSyncMaxTime();
  
  /**
   * Reset all min max times
   */
  void resetAllMinMax();
  
  /**
   *  Number of files created in the last interval
   * @return  number of operations
   */
  int getNumFilesCreated();
  
  /**
   * Number of
   * {@link org.apache.hadoop.hdfs.server.namenode.NameNode#getBlockLocations(String,long,long)}
   * @return  number of operations
   */
  int getNumGetBlockLocations();

  /**
   *   Number of files renamed in the last interval
   * @return number of operations
   */
  int getNumFilesRenamed();
  
  /**
   *   Number of files listed in the last interval
   * @return number of operations
   * @deprecated Use getNumGetListingOps() instead
   */
  @Deprecated
  int getNumFilesListed();

  /**
   *   Number of files listed in the last interval
   * @return number of operations
   */
  int getNumGetListingOps();

  /**
   *   Number of file creation operations in the last interval
   * @return number of file creation operations
   */
  int getNumCreateFileOps();

  /**
   *   Number of file deletion operations in the last interval
   * @return number of file deletion operations
   */
  int getNumDeleteFileOps();

  /**
   *   Number of add block operations in the last interval
   * @return number of add block operations
   */
  int getNumAddBlockOps();
}
