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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.List;
import java.util.ServiceLoader;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.protocol.ClientProtocol;
import org.apache.hadoop.mapreduce.protocol.ClientProtocolProvider;
import org.apache.hadoop.mapreduce.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.mapreduce.util.ConfigUtil;
import org.apache.hadoop.mapreduce.v2.LogParams;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.SecretManager.InvalidToken;
import org.apache.hadoop.security.token.Token;

/**
 * Provides a way to access information about the map/reduce cluster.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class Cluster {
  
  @InterfaceStability.Evolving
  public static enum JobTrackerStatus {INITIALIZING, RUNNING};
  
  private ClientProtocolProvider clientProtocolProvider;
  private ClientProtocol client;
  private UserGroupInformation ugi;
  private Configuration conf;
  private FileSystem fs = null;
  private Path sysDir = null;
  private Path stagingAreaDir = null;
  private Path jobHistoryDir = null;
  private static final Log LOG = LogFactory.getLog(Cluster.class);

  private static ServiceLoader<ClientProtocolProvider> frameworkLoader =
      ServiceLoader.load(ClientProtocolProvider.class);
  private volatile List<ClientProtocolProvider> providerList = null;

  private void initProviderList() {
    if (providerList == null) {
      synchronized (frameworkLoader) {
        if (providerList == null) {
          List<ClientProtocolProvider> localProviderList =
              new ArrayList<ClientProtocolProvider>();
          for (ClientProtocolProvider provider : frameworkLoader) {
            localProviderList.add(provider);
          }
          providerList = localProviderList;
        }
      }
    }
  }

  static {
    ConfigUtil.loadResources();
  }
  
  public Cluster(Configuration conf) throws IOException {
    this(null, conf);
  }

  public Cluster(InetSocketAddress jobTrackAddr, Configuration conf) 
      throws IOException {
    this.conf = conf;
    this.ugi = UserGroupInformation.getCurrentUser();
    initialize(jobTrackAddr, conf);
  }
  
  private void initialize(InetSocketAddress jobTrackAddr, Configuration conf)
      throws IOException {

    initProviderList();
    final IOException initEx = new IOException(
        "Cannot initialize Cluster. Please check your configuration for "
            + MRConfig.FRAMEWORK_NAME
            + " and the correspond server addresses.");
    for (ClientProtocolProvider provider : providerList) {
      LOG.debug("Trying ClientProtocolProvider : "
          + provider.getClass().getName());
      ClientProtocol clientProtocol = null;
      try {
        if (jobTrackAddr == null) {
          clientProtocol = provider.create(conf);
        } else {
          clientProtocol = provider.create(jobTrackAddr, conf);
        }

        if (clientProtocol != null) {
          clientProtocolProvider = provider;
          client = clientProtocol;
          LOG.debug("Picked " + provider.getClass().getName()
              + " as the ClientProtocolProvider");
          break;
        } else {
          LOG.debug("Cannot pick " + provider.getClass().getName()
              + " as the ClientProtocolProvider - returned null protocol");
        }
      } catch (Exception e) {
        final String errMsg = "Failed to use " + provider.getClass().getName()
            + " due to error: ";
        initEx.addSuppressed(new IOException(errMsg, e));
        LOG.info(errMsg, e);
      }
    }

    if (null == clientProtocolProvider || null == client) {
      throw initEx;
    }
  }

  ClientProtocol getClient() {
    return client;
  }
  
  Configuration getConf() {
    return conf;
  }
  
  /**
   * Close the <code>Cluster</code>.
   * @throws IOException
   */
  public synchronized void close() throws IOException {
    clientProtocolProvider.close(client);
  }

  private Job[] getJobs(JobStatus[] stats) throws IOException {
    List<Job> jobs = new ArrayList<Job>();
    for (JobStatus stat : stats) {
      jobs.add(Job.getInstance(this, stat, new JobConf(stat.getJobFile())));
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
      try {
        this.fs = ugi.doAs(new PrivilegedExceptionAction<FileSystem>() {
          public FileSystem run() throws IOException, InterruptedException {
            final Path sysDir = new Path(client.getSystemDir());
            return sysDir.getFileSystem(getConf());
          }
        });
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
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
      final JobConf conf = new JobConf();
      final Path jobPath = new Path(client.getFilesystemName(),
          status.getJobFile());
      final FileSystem fs = FileSystem.get(jobPath.toUri(), getConf());
      try {
        conf.addResource(fs.open(jobPath), jobPath.toString());
      } catch (FileNotFoundException fnf) {
        if (LOG.isWarnEnabled()) {
          LOG.warn("Job conf missing on cluster", fnf);
        }
      }
      return Job.getInstance(this, status, conf);
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
   * Get log parameters for the specified jobID or taskAttemptID
   * @param jobID the job id.
   * @param taskAttemptID the task attempt id. Optional.
   * @return the LogParams
   * @throws IOException
   * @throws InterruptedException
   */
  public LogParams getLogParams(JobID jobID, TaskAttemptID taskAttemptID)
      throws IOException, InterruptedException {
    return client.getLogFileParams(jobID, taskAttemptID);
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
   * @deprecated Use {@link #getAllJobStatuses()} instead.
   */
  @Deprecated
  public Job[] getAllJobs() throws IOException, InterruptedException {
    return getJobs(client.getAllJobs());
  }

  /**
   * Get job status for all jobs in the cluster.
   * @return job status for all jobs in cluster
   * @throws IOException
   * @throws InterruptedException
   */
  public JobStatus[] getAllJobStatuses() throws IOException, InterruptedException {
    return client.getAllJobs();
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
   * Grab the jobtracker's view of the staging directory path where 
   * job-specific files will  be placed.
   * 
   * @return the staging directory where job-specific files are to be placed.
   */
  public Path getStagingAreaDir() throws IOException, InterruptedException {
    if (stagingAreaDir == null) {
      stagingAreaDir = new Path(client.getStagingAreaDir());
    }
    return stagingAreaDir;
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
    return new Path(jobHistoryDir, jobId.toString() + "_"
                    + ugi.getShortUserName()).toString();
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
   * Get the JobTracker's status.
   * 
   * @return {@link JobTrackerStatus} of the JobTracker
   * @throws IOException
   * @throws InterruptedException
   */
  public JobTrackerStatus getJobTrackerStatus() throws IOException,
      InterruptedException {
    return client.getJobTrackerStatus();
  }
  
  /**
   * Get the tasktracker expiry interval for the cluster
   * @return the expiry interval in msec
   */
  public long getTaskTrackerExpiryInterval() throws IOException,
      InterruptedException {
    return client.getTaskTrackerExpiryInterval();
  }

  /**
   * Get a delegation token for the user from the JobTracker.
   * @param renewer the user who can renew the token
   * @return the new token
   * @throws IOException
   */
  public Token<DelegationTokenIdentifier> 
      getDelegationToken(Text renewer) throws IOException, InterruptedException{
    // client has already set the service
    return client.getDelegationToken(renewer);
  }

  /**
   * Renew a delegation token
   * @param token the token to renew
   * @return the new expiration time
   * @throws InvalidToken
   * @throws IOException
   * @deprecated Use {@link Token#renew} instead
   */
  public long renewDelegationToken(Token<DelegationTokenIdentifier> token
                                   ) throws InvalidToken, IOException,
                                            InterruptedException {
    return token.renew(getConf());
  }

  /**
   * Cancel a delegation token from the JobTracker
   * @param token the token to cancel
   * @throws IOException
   * @deprecated Use {@link Token#cancel} instead
   */
  public void cancelDelegationToken(Token<DelegationTokenIdentifier> token
                                    ) throws IOException,
                                             InterruptedException {
    token.cancel(getConf());
  }

}
