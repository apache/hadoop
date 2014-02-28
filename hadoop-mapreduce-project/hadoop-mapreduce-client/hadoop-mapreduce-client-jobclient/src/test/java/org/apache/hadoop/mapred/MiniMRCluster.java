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

import java.io.IOException;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.UserGroupInformation;

/**
 * This class is an MR2 replacement for older MR1 MiniMRCluster, that was used
 * by tests prior to MR2. This replacement class uses the new MiniMRYarnCluster
 * in MR2 but provides the same old MR1 interface, so tests can be migrated from
 * MR1 to MR2 with minimal changes.
 *
 * Due to major differences between MR1 and MR2, a number of methods are either
 * unimplemented/unsupported or were re-implemented to provide wrappers around
 * MR2 functionality.
 *
 * @deprecated Use {@link org.apache.hadoop.mapred.MiniMRClientClusterFactory}
 * instead
 */
@Deprecated
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class MiniMRCluster {
  private static final Log LOG = LogFactory.getLog(MiniMRCluster.class);

  private MiniMRClientCluster mrClientCluster;

  public String getTaskTrackerLocalDir(int taskTracker) {
    throw new UnsupportedOperationException();
  }

  public String[] getTaskTrackerLocalDirs(int taskTracker) {
    throw new UnsupportedOperationException();
  }

  class JobTrackerRunner {
    // Mock class
  }

  class TaskTrackerRunner {
    // Mock class
  }

  public JobTrackerRunner getJobTrackerRunner() {
    throw new UnsupportedOperationException();
  }

  TaskTrackerRunner getTaskTrackerRunner(int id) {
    throw new UnsupportedOperationException();
  }

  public int getNumTaskTrackers() {
    throw new UnsupportedOperationException();
  }

  public void setInlineCleanupThreads() {
    throw new UnsupportedOperationException();
  }

  public void waitUntilIdle() {
    throw new UnsupportedOperationException();
  }

  private void waitTaskTrackers() {
    throw new UnsupportedOperationException();
  }

  public int getJobTrackerPort() {
    throw new UnsupportedOperationException();
  }

  public JobConf createJobConf() {
    JobConf jobConf = null;
    try {
      jobConf = new JobConf(mrClientCluster.getConfig());
    } catch (IOException e) {
      LOG.error(e);
    }
    return jobConf;
  }

  public JobConf createJobConf(JobConf conf) {
    JobConf jobConf = null;
    try {
      jobConf = new JobConf(mrClientCluster.getConfig());
    } catch (IOException e) {
      LOG.error(e);
    }
    return jobConf;
  }

  static JobConf configureJobConf(JobConf conf, String namenode,
      int jobTrackerPort, int jobTrackerInfoPort, UserGroupInformation ugi) {
    throw new UnsupportedOperationException();
  }

  public MiniMRCluster(int numTaskTrackers, String namenode, int numDir,
      String[] racks, String[] hosts) throws IOException {
    this(0, 0, numTaskTrackers, namenode, numDir, racks, hosts);
  }

  public MiniMRCluster(int numTaskTrackers, String namenode, int numDir,
      String[] racks, String[] hosts, JobConf conf) throws IOException {
    this(0, 0, numTaskTrackers, namenode, numDir, racks, hosts, null, conf);
  }

  public MiniMRCluster(int numTaskTrackers, String namenode, int numDir)
      throws IOException {
    this(0, 0, numTaskTrackers, namenode, numDir);
  }

  public MiniMRCluster(int jobTrackerPort, int taskTrackerPort,
      int numTaskTrackers, String namenode, int numDir) throws IOException {
    this(jobTrackerPort, taskTrackerPort, numTaskTrackers, namenode, numDir,
        null);
  }

  public MiniMRCluster(int jobTrackerPort, int taskTrackerPort,
      int numTaskTrackers, String namenode, int numDir, String[] racks)
      throws IOException {
    this(jobTrackerPort, taskTrackerPort, numTaskTrackers, namenode, numDir,
        racks, null);
  }

  public MiniMRCluster(int jobTrackerPort, int taskTrackerPort,
      int numTaskTrackers, String namenode, int numDir, String[] racks,
      String[] hosts) throws IOException {
    this(jobTrackerPort, taskTrackerPort, numTaskTrackers, namenode, numDir,
        racks, hosts, null);
  }

  public MiniMRCluster(int jobTrackerPort, int taskTrackerPort,
      int numTaskTrackers, String namenode, int numDir, String[] racks,
      String[] hosts, UserGroupInformation ugi) throws IOException {
    this(jobTrackerPort, taskTrackerPort, numTaskTrackers, namenode, numDir,
        racks, hosts, ugi, null);
  }

  public MiniMRCluster(int jobTrackerPort, int taskTrackerPort,
      int numTaskTrackers, String namenode, int numDir, String[] racks,
      String[] hosts, UserGroupInformation ugi, JobConf conf)
      throws IOException {
    this(jobTrackerPort, taskTrackerPort, numTaskTrackers, namenode, numDir,
        racks, hosts, ugi, conf, 0);
  }

  public MiniMRCluster(int jobTrackerPort, int taskTrackerPort,
      int numTaskTrackers, String namenode, int numDir, String[] racks,
      String[] hosts, UserGroupInformation ugi, JobConf conf,
      int numTrackerToExclude) throws IOException {
    this(jobTrackerPort, taskTrackerPort, numTaskTrackers, namenode, numDir,
        racks, hosts, ugi, conf, numTrackerToExclude, new Clock());
  }

  public MiniMRCluster(int jobTrackerPort, int taskTrackerPort,
      int numTaskTrackers, String namenode, int numDir, String[] racks,
      String[] hosts, UserGroupInformation ugi, JobConf conf,
      int numTrackerToExclude, Clock clock) throws IOException {
    if (conf == null) conf = new JobConf();
    FileSystem.setDefaultUri(conf, namenode);
    String identifier = this.getClass().getSimpleName() + "_"
        + Integer.toString(new Random().nextInt(Integer.MAX_VALUE));
    mrClientCluster = MiniMRClientClusterFactory.create(this.getClass(),
        identifier, numTaskTrackers, conf);
  }

  public UserGroupInformation getUgi() {
    throw new UnsupportedOperationException();
  }

  public TaskCompletionEvent[] getTaskCompletionEvents(JobID id, int from,
      int max) throws IOException {
    throw new UnsupportedOperationException();
  }

  public void setJobPriority(JobID jobId, JobPriority priority)
      throws AccessControlException, IOException {
    throw new UnsupportedOperationException();
  }

  public JobPriority getJobPriority(JobID jobId) {
    throw new UnsupportedOperationException();
  }

  public long getJobFinishTime(JobID jobId) {
    throw new UnsupportedOperationException();
  }

  public void initializeJob(JobID jobId) throws IOException {
    throw new UnsupportedOperationException();
  }

  public MapTaskCompletionEventsUpdate getMapTaskCompletionEventsUpdates(
      int index, JobID jobId, int max) throws IOException {
    throw new UnsupportedOperationException();
  }

  public JobConf getJobTrackerConf() {
    JobConf jobConf = null;
    try {
      jobConf = new JobConf(mrClientCluster.getConfig());
    } catch (IOException e) {
      LOG.error(e);
    }
    return jobConf;
  }

  public int getFaultCount(String hostName) {
    throw new UnsupportedOperationException();
  }

  public void startJobTracker() {
    // Do nothing
  }

  public void startJobTracker(boolean wait) {
    // Do nothing
  }

  public void stopJobTracker() {
    // Do nothing
  }

  public void stopTaskTracker(int id) {
    // Do nothing
  }

  public void startTaskTracker(String host, String rack, int idx, int numDir)
      throws IOException {
    // Do nothing
  }

  void addTaskTracker(TaskTrackerRunner taskTracker) {
    throw new UnsupportedOperationException();
  }

  int getTaskTrackerID(String trackerName) {
    throw new UnsupportedOperationException();
  }

  public void shutdown() {
    try {
      mrClientCluster.stop();
    } catch (IOException e) {
      LOG.error(e);
    }
  }

}
