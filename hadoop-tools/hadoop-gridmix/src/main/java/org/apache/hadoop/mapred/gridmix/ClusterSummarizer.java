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
package org.apache.hadoop.mapred.gridmix;

import org.apache.commons.lang3.time.FastDateFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapred.gridmix.Statistics.ClusterStats;
import org.apache.hadoop.mapreduce.server.jobtracker.JTConfig;

/**
 * Summarizes the Hadoop cluster used in this {@link Gridmix} run. 
 * Statistics that are reported are
 * <ul>
 *   <li>Total number of active trackers in the cluster</li>
 *   <li>Total number of blacklisted trackers in the cluster</li>
 *   <li>Max map task capacity of the cluster</li>
 *   <li>Max reduce task capacity of the cluster</li>
 * </ul>
 * 
 * Apart from these statistics, {@link JobTracker} and {@link FileSystem} 
 * addresses are also recorded in the summary.
 */
class ClusterSummarizer implements StatListener<ClusterStats> {
  static final Logger LOG = LoggerFactory.getLogger(ClusterSummarizer.class);
  
  private int numBlacklistedTrackers;
  private int numActiveTrackers;
  private int maxMapTasks;
  private int maxReduceTasks;
  private String jobTrackerInfo = Summarizer.NA;
  private String namenodeInfo = Summarizer.NA;
  
  @Override
  @SuppressWarnings("deprecation")
  public void update(ClusterStats item) {
    try {
      numBlacklistedTrackers = item.getStatus().getBlacklistedTrackers();
      numActiveTrackers = item.getStatus().getTaskTrackers();
      maxMapTasks = item.getStatus().getMaxMapTasks();
      maxReduceTasks = item.getStatus().getMaxReduceTasks();
    } catch (Exception e) {
      long time = System.currentTimeMillis();
      LOG.info("Error in processing cluster status at " 
               + FastDateFormat.getInstance().format(time));
    }
  }
  
  /**
   * Summarizes the cluster used for this {@link Gridmix} run.
   */
  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder();
    builder.append("Cluster Summary:-");
    builder.append("\nJobTracker: ").append(getJobTrackerInfo());
    builder.append("\nFileSystem: ").append(getNamenodeInfo());
    builder.append("\nNumber of blacklisted trackers: ")
           .append(getNumBlacklistedTrackers());
    builder.append("\nNumber of active trackers: ")
           .append(getNumActiveTrackers());
    builder.append("\nMax map task capacity: ")
           .append(getMaxMapTasks());
    builder.append("\nMax reduce task capacity: ").append(getMaxReduceTasks());
    builder.append("\n\n");
    return builder.toString();
  }
  
  void start(Configuration conf) {
    jobTrackerInfo = conf.get(JTConfig.JT_IPC_ADDRESS);
    namenodeInfo = conf.get(CommonConfigurationKeys.FS_DEFAULT_NAME_KEY);
  }
  
  // Getters
  protected int getNumBlacklistedTrackers() {
    return numBlacklistedTrackers;
  }
  
  protected int getNumActiveTrackers() {
    return numActiveTrackers;
  }
  
  protected int getMaxMapTasks() {
    return maxMapTasks;
  }
  
  protected int getMaxReduceTasks() {
    return maxReduceTasks;
  }
  
  protected String getJobTrackerInfo() {
    return jobTrackerInfo;
  }
  
  protected String getNamenodeInfo() {
    return namenodeInfo;
  }
}
