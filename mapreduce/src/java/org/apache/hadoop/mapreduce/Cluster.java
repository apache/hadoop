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

package org.apache.hadoop.mapreduce;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobTracker;
import org.apache.hadoop.mapred.LocalJobRunner;
import org.apache.hadoop.mapreduce.jobhistory.JobHistory;
import org.apache.hadoop.mapreduce.protocol.ClientProtocol;
import org.apache.hadoop.mapreduce.server.jobtracker.State;
import org.apache.hadoop.mapreduce.util.ConfigUtil;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.UnixUserGroupInformation;

/**
 * Provides a way to access information about the map/reduce cluster.
 */
public class Cluster {
  private ClientProtocol client;
  private UnixUserGroupInformation ugi;
  private Configuration conf;
  private FileSystem fs = null;
  private Path sysDir = null;
  private Path jobHistoryDir = null;

  static {
    ConfigUtil.loadResources();
  }
  
  public Cluster(Configuration conf) throws IOException {
    this.conf = conf;
    this.ugi = Job.getUGI(conf);
    client = createClient(conf);
  }

  public Cluster(InetSocketAddress jobTrackAddr, Configuration conf) 
      throws IOException {
    this.conf = conf;
    this.ugi = Job.getUGI(conf);
    client = createRPCProxy(jobTrackAddr, conf);
  }

  private ClientProtocol createRPCProxy(InetSocketAddress addr,
      Configuration conf) throws IOException {
    return (ClientProtocol) RPC.getProxy(ClientProtocol.class,
      ClientProtocol.versionID, addr, ugi, conf,
      NetUtils.getSocketFactory(conf, ClientProtocol.class));
  }

  private ClientProtocol createClient(Configuration conf) throws IOException {
    ClientProtocol client;
    String tracker = conf.get("mapred.job.tracker", "local");
    if ("local".equals(tracker)) {
      client = new LocalJobRunner(conf);
    } else {
      client = createRPCProxy(JobTracker.getAddress(conf), conf);
    }
    return client;
  }
  
  ClientProtocol getClient() {
    return client;
  }
  
  Configuration getConf() {
    return conf;
  }
  
  /**
   * Close the <code>Cluster</code>.
   */
  public synchronized void close() throws IOException {
    if (!(client instanceof LocalJobRunner)) {
      RPC.stopProxy(client);
    }
  }

  private Job[] getJobs(JobStatus[] stats) throws IOException {
    List<Job> jobs = new ArrayList<Job>();
    for (JobStatus stat : stats) {
      jobs.add(new Job(this, stat, new JobConf(stat.getJobFile())));
    }
    return jobs.toArray(new Job[0]);
  }

  /**
   * Get the file system where job-specific files are stored
   * 
   * @return object of FileSystem
   * @throws IOException
   * @throws InterruptedException
   */
  public synchronized FileSystem getFileSystem() 
      throws IOException, InterruptedException {
    if (this.fs == null) {
      Path sysDir = new Path(client.getSystemDir());
      this.fs = sysDir.getFileSystem(getConf());
    }
    return fs;
  }

  /**
   * Get job corresponding to jobid.
   * 
   * @param jobId
   * @return object of {@link Job}
   * @throws IOException
   * @throws InterruptedException
   */
  public Job getJob(JobID jobId) throws IOException, InterruptedException {
    JobStatus status = client.getJobStatus(jobId);
    if (status != null) {
      return new Job(this, status, new JobConf(status.getJobFile()));
    }
    return null;
  }
  
  /**
   * Get all the queues in cluster.
   * 
   * @return array of {@link QueueInfo}
   * @throws IOException
   * @throws InterruptedException
   */
  public QueueInfo[] getQueues() throws IOException, InterruptedException {
    return client.getQueues();
  }
  
  /**
   * Get queue information for the specified name.
   * 
   * @param name queuename
   * @return object of {@link QueueInfo}
   * @throws IOException
   * @throws InterruptedException
   */
  public QueueInfo getQueue(String name) 
      throws IOException, InterruptedException {
    return client.getQueue(name);
  }
  
  /**
   * Get current cluster status.
   * 
   * @return object of {@link ClusterMetrics}
   * @throws IOException
   * @throws InterruptedException
   */
  public ClusterMetrics getClusterStatus() throws IOException, InterruptedException {
    return client.getClusterMetrics();
  }
  
  /**
   * Get all active trackers in the cluster.
   * 
   * @return array of {@link TaskTrackerInfo}
   * @throws IOException
   * @throws InterruptedException
   */
  public TaskTrackerInfo[] getActiveTaskTrackers() 
      throws IOException, InterruptedException  {
    return client.getActiveTrackers();
  }
  
  /**
   * Get blacklisted trackers.
   * 
   * @return array of {@link TaskTrackerInfo}
   * @throws IOException
   * @throws InterruptedException
   */
  public TaskTrackerInfo[] getBlackListedTaskTrackers() 
      throws IOException, InterruptedException  {
    return client.getBlacklistedTrackers();
  }
  
  /**
   * Get all the jobs in cluster.
   * 
   * @return array of {@link Job}
   * @throws IOException
   * @throws InterruptedException
   */
  public Job[] getAllJobs() throws IOException, InterruptedException {
    return getJobs(client.getAllJobs());
  }
  
  /**
   * Grab the jobtracker system directory path where 
   * job-specific files will  be placed.
   * 
   * @return the system directory where job-specific files are to be placed.
   */
  public Path getSystemDir() throws IOException, InterruptedException {
    if (sysDir == null) {
      sysDir = new Path(client.getSystemDir());
    }
    return sysDir;
  }

  /**
   * Get the job history file path for a given job id. The job history file at 
   * this path may or may not be existing depending on the job completion state.
   * The file is present only for the completed jobs.
   * @param jobId the JobID of the job submitted by the current user.
   * @return the file path of the job history file
   * @throws IOException
   * @throws InterruptedException
   */
  public String getJobHistoryUrl(JobID jobId) throws IOException, 
    InterruptedException {
    if (jobHistoryDir == null) {
      jobHistoryDir = new Path(client.getJobHistoryDir());
    }
    return JobHistory.getJobHistoryFile(jobHistoryDir, jobId, 
        ugi.getUserName()).toString();
  }

  /**
   * Gets the Queue ACLs for current user
   * @return array of QueueAclsInfo object for current user.
   * @throws IOException
   */
  public QueueAclsInfo[] getQueueAclsForCurrentUser() 
      throws IOException, InterruptedException  {
    return client.getQueueAclsForCurrentUser();
  }

  /**
   * Gets the root level queues.
   * @return array of JobQueueInfo object.
   * @throws IOException
   */
  public QueueInfo[] getRootQueues() throws IOException, InterruptedException {
    return client.getRootQueues();
  }
  
  /**
   * Returns immediate children of queueName.
   * @param queueName
   * @return array of JobQueueInfo which are children of queueName
   * @throws IOException
   */
  public QueueInfo[] getChildQueues(String queueName) 
      throws IOException, InterruptedException {
    return client.getChildQueues(queueName);
  }

  /**
   * Get JobTracker's state
   * 
   * @return {@link State} of the JobTracker
   * @throws IOException
   * @throws InterruptedException
   */
  public State getJobTrackerState() throws IOException, InterruptedException {
    return client.getJobTrackerState();
  }
  
  /**
   * Get the tasktracker expiry interval for the cluster
   * @return the expiry interval in msec
   */
  public long getTaskTrackerExpiryInterval() throws IOException,
      InterruptedException {
    return client.getTaskTrackerExpiryInterval();
  }

}
