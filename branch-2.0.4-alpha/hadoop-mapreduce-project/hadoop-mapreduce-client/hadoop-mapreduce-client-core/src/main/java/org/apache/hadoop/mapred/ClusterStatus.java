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

package org.apache.hadoop.mapred;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapreduce.ClusterMetrics;
import org.apache.hadoop.mapreduce.TaskTrackerInfo;
import org.apache.hadoop.mapreduce.Cluster.JobTrackerStatus;
import org.apache.hadoop.util.StringInterner;

/**
 * Status information on the current state of the Map-Reduce cluster.
 * 
 * <p><code>ClusterStatus</code> provides clients with information such as:
 * <ol>
 *   <li>
 *   Size of the cluster. 
 *   </li>
 *   <li>
 *   Name of the trackers. 
 *   </li>
 *   <li>
 *   Task capacity of the cluster. 
 *   </li>
 *   <li>
 *   The number of currently running map & reduce tasks.
 *   </li>
 *   <li>
 *   State of the <code>JobTracker</code>.
 *   </li>
 *   <li>
 *   Details regarding black listed trackers.
 *   </li>
 * </ol></p>
 * 
 * <p>Clients can query for the latest <code>ClusterStatus</code>, via 
 * {@link JobClient#getClusterStatus()}.</p>
 * 
 * @see JobClient
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class ClusterStatus implements Writable {
  /**
   * Class which encapsulates information about a blacklisted tasktracker.
   *  
   * The information includes the tasktracker's name and reasons for
   * getting blacklisted. The toString method of the class will print
   * the information in a whitespace separated fashion to enable parsing.
   */
  public static class BlackListInfo implements Writable {

    private String trackerName;

    private String reasonForBlackListing;
    
    private String blackListReport;
    
    BlackListInfo() {
    }
    

    /**
     * Gets the blacklisted tasktracker's name.
     * 
     * @return tracker's name.
     */
    public String getTrackerName() {
      return trackerName;
    }

    /**
     * Gets the reason for which the tasktracker was blacklisted.
     * 
     * @return reason which tracker was blacklisted
     */
    public String getReasonForBlackListing() {
      return reasonForBlackListing;
    }

    /**
     * Sets the blacklisted tasktracker's name.
     * 
     * @param trackerName of the tracker.
     */
    void setTrackerName(String trackerName) {
      this.trackerName = trackerName;
    }

    /**
     * Sets the reason for which the tasktracker was blacklisted.
     * 
     * @param reasonForBlackListing
     */
    void setReasonForBlackListing(String reasonForBlackListing) {
      this.reasonForBlackListing = reasonForBlackListing;
    }

    /**
     * Gets a descriptive report about why the tasktracker was blacklisted.
     * 
     * @return report describing why the tasktracker was blacklisted.
     */
    public String getBlackListReport() {
      return blackListReport;
    }

    /**
     * Sets a descriptive report about why the tasktracker was blacklisted.
     * @param blackListReport report describing why the tasktracker 
     *                        was blacklisted.
     */
    void setBlackListReport(String blackListReport) {
      this.blackListReport = blackListReport;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      trackerName = StringInterner.weakIntern(Text.readString(in));
      reasonForBlackListing = StringInterner.weakIntern(Text.readString(in));
      blackListReport = StringInterner.weakIntern(Text.readString(in));
    }

    @Override
    public void write(DataOutput out) throws IOException {
      Text.writeString(out, trackerName);
      Text.writeString(out, reasonForBlackListing);
      Text.writeString(out, blackListReport);
    }

    @Override
    /**
     * Print information related to the blacklisted tasktracker in a
     * whitespace separated fashion.
     * 
     * The method changes any newlines in the report describing why
     * the tasktracker was blacklisted to a ':' for enabling better
     * parsing.
     */
    public String toString() {
      StringBuilder sb = new StringBuilder();
      sb.append(trackerName);
      sb.append("\t");
      sb.append(reasonForBlackListing);
      sb.append("\t");
      sb.append(blackListReport.replace("\n", ":"));
      return sb.toString();
    }
    
  }
  
  public static final int UNINITIALIZED_MEMORY_VALUE = -1;
  
  private int numActiveTrackers;
  private Collection<String> activeTrackers = new ArrayList<String>();
  private int numBlacklistedTrackers;
  private int numExcludedNodes;
  private long ttExpiryInterval;
  private int map_tasks;
  private int reduce_tasks;
  private int max_map_tasks;
  private int max_reduce_tasks;
  private JobTrackerStatus status;
  private Collection<BlackListInfo> blacklistedTrackersInfo =
    new ArrayList<BlackListInfo>();

  ClusterStatus() {}
  
  /**
   * Construct a new cluster status.
   * 
   * @param trackers no. of tasktrackers in the cluster
   * @param blacklists no of blacklisted task trackers in the cluster
   * @param ttExpiryInterval the tasktracker expiry interval
   * @param maps no. of currently running map-tasks in the cluster
   * @param reduces no. of currently running reduce-tasks in the cluster
   * @param maxMaps the maximum no. of map tasks in the cluster
   * @param maxReduces the maximum no. of reduce tasks in the cluster
   * @param status the {@link JobTrackerStatus} of the <code>JobTracker</code>
   */
  ClusterStatus(int trackers, int blacklists, long ttExpiryInterval, 
                int maps, int reduces,
                int maxMaps, int maxReduces, JobTrackerStatus status) {
    this(trackers, blacklists, ttExpiryInterval, maps, reduces, maxMaps, 
         maxReduces, status, 0);
  }

  /**
   * Construct a new cluster status.
   * 
   * @param trackers no. of tasktrackers in the cluster
   * @param blacklists no of blacklisted task trackers in the cluster
   * @param ttExpiryInterval the tasktracker expiry interval
   * @param maps no. of currently running map-tasks in the cluster
   * @param reduces no. of currently running reduce-tasks in the cluster
   * @param maxMaps the maximum no. of map tasks in the cluster
   * @param maxReduces the maximum no. of reduce tasks in the cluster
   * @param status the {@link JobTrackerStatus} of the <code>JobTracker</code>
   * @param numDecommissionedNodes number of decommission trackers
   */
  ClusterStatus(int trackers, int blacklists, long ttExpiryInterval, 
                int maps, int reduces, int maxMaps, int maxReduces, 
                JobTrackerStatus status, int numDecommissionedNodes) {
    numActiveTrackers = trackers;
    numBlacklistedTrackers = blacklists;
    this.numExcludedNodes = numDecommissionedNodes;
    this.ttExpiryInterval = ttExpiryInterval;
    map_tasks = maps;
    reduce_tasks = reduces;
    max_map_tasks = maxMaps;
    max_reduce_tasks = maxReduces;
    this.status = status;
  }

  /**
   * Construct a new cluster status.
   * 
   * @param activeTrackers active tasktrackers in the cluster
   * @param blacklistedTrackers blacklisted tasktrackers in the cluster
   * @param ttExpiryInterval the tasktracker expiry interval
   * @param maps no. of currently running map-tasks in the cluster
   * @param reduces no. of currently running reduce-tasks in the cluster
   * @param maxMaps the maximum no. of map tasks in the cluster
   * @param maxReduces the maximum no. of reduce tasks in the cluster
   * @param status the {@link JobTrackerStatus} of the <code>JobTracker</code>
   */
  ClusterStatus(Collection<String> activeTrackers, 
      Collection<BlackListInfo> blacklistedTrackers,
      long ttExpiryInterval,
      int maps, int reduces, int maxMaps, int maxReduces, 
      JobTrackerStatus status) {
    this(activeTrackers, blacklistedTrackers, ttExpiryInterval, maps, reduces, 
         maxMaps, maxReduces, status, 0);
  }


  /**
   * Construct a new cluster status.
   * 
   * @param activeTrackers active tasktrackers in the cluster
   * @param blackListedTrackerInfo blacklisted tasktrackers information 
   * in the cluster
   * @param ttExpiryInterval the tasktracker expiry interval
   * @param maps no. of currently running map-tasks in the cluster
   * @param reduces no. of currently running reduce-tasks in the cluster
   * @param maxMaps the maximum no. of map tasks in the cluster
   * @param maxReduces the maximum no. of reduce tasks in the cluster
   * @param status the {@link JobTrackerStatus} of the <code>JobTracker</code>
   * @param numDecommissionNodes number of decommission trackers
   */
  
  ClusterStatus(Collection<String> activeTrackers,
      Collection<BlackListInfo> blackListedTrackerInfo, long ttExpiryInterval,
      int maps, int reduces, int maxMaps, int maxReduces,
      JobTrackerStatus status, int numDecommissionNodes) {
    this(activeTrackers.size(), blackListedTrackerInfo.size(),
        ttExpiryInterval, maps, reduces, maxMaps, maxReduces, status,
        numDecommissionNodes);
    this.activeTrackers = activeTrackers;
    this.blacklistedTrackersInfo = blackListedTrackerInfo;
  }

  /**
   * Get the number of task trackers in the cluster.
   * 
   * @return the number of task trackers in the cluster.
   */
  public int getTaskTrackers() {
    return numActiveTrackers;
  }
  
  /**
   * Get the names of task trackers in the cluster.
   * 
   * @return the active task trackers in the cluster.
   */
  public Collection<String> getActiveTrackerNames() {
    return activeTrackers;
  }

  /**
   * Get the names of task trackers in the cluster.
   * 
   * @return the blacklisted task trackers in the cluster.
   */
  public Collection<String> getBlacklistedTrackerNames() {
    ArrayList<String> blacklistedTrackers = new ArrayList<String>();
    for(BlackListInfo bi : blacklistedTrackersInfo) {
      blacklistedTrackers.add(bi.getTrackerName());
    }
    return blacklistedTrackers;
  }
  
  /**
   * Get the number of blacklisted task trackers in the cluster.
   * 
   * @return the number of blacklisted task trackers in the cluster.
   */
  public int getBlacklistedTrackers() {
    return numBlacklistedTrackers;
  }
  
  /**
   * Get the number of excluded hosts in the cluster.
   * @return the number of excluded hosts in the cluster.
   */
  public int getNumExcludedNodes() {
    return numExcludedNodes;
  }
  
  /**
   * Get the tasktracker expiry interval for the cluster
   * @return the expiry interval in msec
   */
  public long getTTExpiryInterval() {
    return ttExpiryInterval;
  }
  
  /**
   * Get the number of currently running map tasks in the cluster.
   * 
   * @return the number of currently running map tasks in the cluster.
   */
  public int getMapTasks() {
    return map_tasks;
  }
  
  /**
   * Get the number of currently running reduce tasks in the cluster.
   * 
   * @return the number of currently running reduce tasks in the cluster.
   */
  public int getReduceTasks() {
    return reduce_tasks;
  }
  
  /**
   * Get the maximum capacity for running map tasks in the cluster.
   * 
   * @return the maximum capacity for running map tasks in the cluster.
   */
  public int getMaxMapTasks() {
    return max_map_tasks;
  }

  /**
   * Get the maximum capacity for running reduce tasks in the cluster.
   * 
   * @return the maximum capacity for running reduce tasks in the cluster.
   */
  public int getMaxReduceTasks() {
    return max_reduce_tasks;
  }
  
  /**
   * Get the JobTracker's status.
   * 
   * @return {@link JobTrackerStatus} of the JobTracker
   */
  public JobTrackerStatus getJobTrackerStatus() {
    return status;
  }
  
  /**
   * Returns UNINITIALIZED_MEMORY_VALUE (-1)
   */
  @Deprecated
  public long getMaxMemory() {
    return UNINITIALIZED_MEMORY_VALUE;
  }
  
  /**
   * Returns UNINITIALIZED_MEMORY_VALUE (-1)
   */
  @Deprecated
  public long getUsedMemory() {
    return UNINITIALIZED_MEMORY_VALUE;
  }

  /**
   * Gets the list of blacklisted trackers along with reasons for blacklisting.
   * 
   * @return the collection of {@link BlackListInfo} objects. 
   * 
   */
  public Collection<BlackListInfo> getBlackListedTrackersInfo() {
    return blacklistedTrackersInfo;
  }

  public void write(DataOutput out) throws IOException {
    if (activeTrackers.size() == 0) {
      out.writeInt(numActiveTrackers);
      out.writeInt(0);
    } else {
      out.writeInt(activeTrackers.size());
      out.writeInt(activeTrackers.size());
      for (String tracker : activeTrackers) {
        Text.writeString(out, tracker);
      }
    }
    if (blacklistedTrackersInfo.size() == 0) {
      out.writeInt(numBlacklistedTrackers);
      out.writeInt(blacklistedTrackersInfo.size());
    } else {
      out.writeInt(blacklistedTrackersInfo.size());
      out.writeInt(blacklistedTrackersInfo.size());
      for (BlackListInfo tracker : blacklistedTrackersInfo) {
        tracker.write(out);
      }
    }
    out.writeInt(numExcludedNodes);
    out.writeLong(ttExpiryInterval);
    out.writeInt(map_tasks);
    out.writeInt(reduce_tasks);
    out.writeInt(max_map_tasks);
    out.writeInt(max_reduce_tasks);
    WritableUtils.writeEnum(out, status);
  }

  public void readFields(DataInput in) throws IOException {
    numActiveTrackers = in.readInt();
    int numTrackerNames = in.readInt();
    if (numTrackerNames > 0) {
      for (int i = 0; i < numTrackerNames; i++) {
        String name = StringInterner.weakIntern(Text.readString(in));
        activeTrackers.add(name);
      }
    }
    numBlacklistedTrackers = in.readInt();
    int blackListTrackerInfoSize = in.readInt();
    if(blackListTrackerInfoSize > 0) {
      for (int i = 0; i < blackListTrackerInfoSize; i++) {
        BlackListInfo info = new BlackListInfo();
        info.readFields(in);
        blacklistedTrackersInfo.add(info);
      }
    }
    numExcludedNodes = in.readInt();
    ttExpiryInterval = in.readLong();
    map_tasks = in.readInt();
    reduce_tasks = in.readInt();
    max_map_tasks = in.readInt();
    max_reduce_tasks = in.readInt();
    status = WritableUtils.readEnum(in, JobTrackerStatus.class);
  }
}
