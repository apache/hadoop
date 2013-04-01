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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.ClusterMetrics;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.JobStatus;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.QueueAclsInfo;
import org.apache.hadoop.mapreduce.QueueInfo;
import org.apache.hadoop.mapreduce.TaskTrackerInfo;
import org.apache.hadoop.mapreduce.TypeConverter;
import org.apache.hadoop.mapreduce.v2.util.MRApps;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.yarn.api.ClientRMProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationResponse;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.YarnClusterMetrics;
import org.apache.hadoop.yarn.client.YarnClientImpl;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.ProtoUtils;

public class ResourceMgrDelegate extends YarnClientImpl {
  private static final Log LOG = LogFactory.getLog(ResourceMgrDelegate.class);
      
  private YarnConfiguration conf;
  private GetNewApplicationResponse application;
  private ApplicationId applicationId;

  /**
   * Delegate responsible for communicating with the Resource Manager's {@link ClientRMProtocol}.
   * @param conf the configuration object.
   */
  public ResourceMgrDelegate(YarnConfiguration conf) {
    super();
    this.conf = conf;
    init(conf);
    start();
  }

  public TaskTrackerInfo[] getActiveTrackers() throws IOException,
      InterruptedException {
    return TypeConverter.fromYarnNodes(super.getNodeReports());
  }

  public JobStatus[] getAllJobs() throws IOException, InterruptedException {
    return TypeConverter.fromYarnApps(super.getApplicationList(), this.conf);
  }

  public TaskTrackerInfo[] getBlacklistedTrackers() throws IOException,
      InterruptedException {
    // TODO: Implement getBlacklistedTrackers
    LOG.warn("getBlacklistedTrackers - Not implemented yet");
    return new TaskTrackerInfo[0];
  }

  public ClusterMetrics getClusterMetrics() throws IOException,
      InterruptedException {
    YarnClusterMetrics metrics = super.getYarnClusterMetrics();
    ClusterMetrics oldMetrics = new ClusterMetrics(1, 1, 1, 1, 1, 1, 
        metrics.getNumNodeManagers() * 10, metrics.getNumNodeManagers() * 2, 1,
        metrics.getNumNodeManagers(), 0, 0);
    return oldMetrics;
  }

  @SuppressWarnings("rawtypes")
  public Token getDelegationToken(Text renewer) throws IOException,
      InterruptedException {
    return ProtoUtils.convertFromProtoFormat(
      super.getRMDelegationToken(renewer), rmAddress);
  }

  public String getFilesystemName() throws IOException, InterruptedException {
    return FileSystem.get(conf).getUri().toString();
  }

  public JobID getNewJobID() throws IOException, InterruptedException {
    this.application = super.getNewApplication();
    this.applicationId = this.application.getApplicationId();
    return TypeConverter.fromYarn(applicationId);
  }

  public QueueInfo getQueue(String queueName) throws IOException,
  InterruptedException {
    org.apache.hadoop.yarn.api.records.QueueInfo queueInfo =
        super.getQueueInfo(queueName);
    return (queueInfo == null) ? null : TypeConverter.fromYarn(queueInfo, conf);
  }

  public QueueAclsInfo[] getQueueAclsForCurrentUser() throws IOException,
      InterruptedException {
    return TypeConverter.fromYarnQueueUserAclsInfo(super
      .getQueueAclsInfo());
  }

  public QueueInfo[] getQueues() throws IOException, InterruptedException {
    return TypeConverter.fromYarnQueueInfo(super.getAllQueues(), this.conf);
  }

  public QueueInfo[] getRootQueues() throws IOException, InterruptedException {
    return TypeConverter.fromYarnQueueInfo(super.getRootQueueInfos(), this.conf);
  }

  public QueueInfo[] getChildQueues(String parent) throws IOException,
      InterruptedException {
    return TypeConverter.fromYarnQueueInfo(super.getChildQueueInfos(parent),
      this.conf);
  }

  public String getStagingAreaDir() throws IOException, InterruptedException {
//    Path path = new Path(MRJobConstants.JOB_SUBMIT_DIR);
    String user = 
      UserGroupInformation.getCurrentUser().getShortUserName();
    Path path = MRApps.getStagingAreaDir(conf, user);
    LOG.debug("getStagingAreaDir: dir=" + path);
    return path.toString();
  }


  public String getSystemDir() throws IOException, InterruptedException {
    Path sysDir = new Path(MRJobConfig.JOB_SUBMIT_DIR);
    //FileContext.getFileContext(conf).delete(sysDir, true);
    return sysDir.toString();
  }
  

  public long getTaskTrackerExpiryInterval() throws IOException,
      InterruptedException {
    return 0;
  }
  
  public void setJobPriority(JobID arg0, String arg1) throws IOException,
      InterruptedException {
    return;
  }


  public long getProtocolVersion(String arg0, long arg1) throws IOException {
    return 0;
  }

  public ApplicationId getApplicationId() {
    return applicationId;
  }
}
