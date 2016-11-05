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
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.conf.Configuration;
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
import org.apache.hadoop.yarn.api.ApplicationClientProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewReservationResponse;
import org.apache.hadoop.yarn.api.protocolrecords.ReservationDeleteRequest;
import org.apache.hadoop.yarn.api.protocolrecords.ReservationDeleteResponse;
import org.apache.hadoop.yarn.api.protocolrecords.ReservationListRequest;
import org.apache.hadoop.yarn.api.protocolrecords.ReservationListResponse;
import org.apache.hadoop.yarn.api.protocolrecords.ReservationSubmissionRequest;
import org.apache.hadoop.yarn.api.protocolrecords.ReservationSubmissionResponse;
import org.apache.hadoop.yarn.api.protocolrecords.ReservationUpdateRequest;
import org.apache.hadoop.yarn.api.protocolrecords.ReservationUpdateResponse;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptReport;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerReport;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.NodeLabel;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.NodeState;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.QueueUserACLInfo;
import org.apache.hadoop.yarn.api.records.SignalContainerCommand;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.api.records.YarnClusterMetrics;
import org.apache.hadoop.yarn.client.ClientRMProxy;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.YarnClientApplication;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.security.AMRMTokenIdentifier;
import org.apache.hadoop.yarn.util.ConverterUtils;

import com.google.common.annotations.VisibleForTesting;

public class ResourceMgrDelegate extends YarnClient {
  private static final Log LOG = LogFactory.getLog(ResourceMgrDelegate.class);
      
  private YarnConfiguration conf;
  private ApplicationSubmissionContext application;
  private ApplicationId applicationId;
  @Private
  @VisibleForTesting
  protected YarnClient client;
  private Text rmDTService;

  /**
   * Delegate responsible for communicating with the Resource Manager's
   * {@link ApplicationClientProtocol}.
   * @param conf the configuration object.
   */
  public ResourceMgrDelegate(YarnConfiguration conf) {
    super(ResourceMgrDelegate.class.getName());
    this.conf = conf;
    this.client = YarnClient.createYarnClient();
    init(conf);
    start();
  }

  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    client.init(conf);
    super.serviceInit(conf);
  }

  @Override
  protected void serviceStart() throws Exception {
    client.start();
    super.serviceStart();
  }

  @Override
  protected void serviceStop() throws Exception {
    client.stop();
    super.serviceStop();
  }

  public TaskTrackerInfo[] getActiveTrackers() throws IOException,
      InterruptedException {
    try {
      return TypeConverter.fromYarnNodes(
          client.getNodeReports(NodeState.RUNNING));
    } catch (YarnException e) {
      throw new IOException(e);
    }
  }

  public JobStatus[] getAllJobs() throws IOException, InterruptedException {
    try {
      Set<String> appTypes = new HashSet<String>(1);
      appTypes.add(MRJobConfig.MR_APPLICATION_TYPE);
      EnumSet<YarnApplicationState> appStates =
          EnumSet.noneOf(YarnApplicationState.class);
      return TypeConverter.fromYarnApps(
          client.getApplications(appTypes, appStates), this.conf);
    } catch (YarnException e) {
      throw new IOException(e);
    }
  }

  public TaskTrackerInfo[] getBlacklistedTrackers() throws IOException,
      InterruptedException {
    // TODO: Implement getBlacklistedTrackers
    LOG.warn("getBlacklistedTrackers - Not implemented yet");
    return new TaskTrackerInfo[0];
  }

  public ClusterMetrics getClusterMetrics() throws IOException,
      InterruptedException {
    try {
      YarnClusterMetrics metrics = client.getYarnClusterMetrics();
      ClusterMetrics oldMetrics =
          new ClusterMetrics(1, 1, 1, 1, 1, 1,
              metrics.getNumNodeManagers() * 10,
              metrics.getNumNodeManagers() * 2, 1,
              metrics.getNumNodeManagers(), 0, 0);
      return oldMetrics;
    } catch (YarnException e) {
      throw new IOException(e);
    }
  }

  public Text getRMDelegationTokenService() {
    if (rmDTService == null) {
      rmDTService = ClientRMProxy.getRMDelegationTokenService(conf);
    }
    return rmDTService;
  }
  
  @SuppressWarnings("rawtypes")
  public Token getDelegationToken(Text renewer) throws IOException,
      InterruptedException {
    try {
      return ConverterUtils.convertFromYarn(
          client.getRMDelegationToken(renewer), getRMDelegationTokenService());
    } catch (YarnException e) {
      throw new IOException(e);
    }
  }

  public String getFilesystemName() throws IOException, InterruptedException {
    return FileSystem.get(conf).getUri().toString();
  }

  public JobID getNewJobID() throws IOException, InterruptedException {
    try {
      this.application = client.createApplication().getApplicationSubmissionContext();
      this.applicationId = this.application.getApplicationId();
      return TypeConverter.fromYarn(applicationId);
    } catch (YarnException e) {
      throw new IOException(e);
    }
  }

  public QueueInfo getQueue(String queueName) throws IOException,
  InterruptedException {
    try {
      org.apache.hadoop.yarn.api.records.QueueInfo queueInfo =
          client.getQueueInfo(queueName);
      return (queueInfo == null) ? null : TypeConverter.fromYarn(queueInfo,
          conf);
    } catch (YarnException e) {
      throw new IOException(e);
    }
  }

  public QueueAclsInfo[] getQueueAclsForCurrentUser() throws IOException,
      InterruptedException {
    try {
      return TypeConverter.fromYarnQueueUserAclsInfo(client
        .getQueueAclsInfo());
    } catch (YarnException e) {
      throw new IOException(e);
    }
  }

  public QueueInfo[] getQueues() throws IOException, InterruptedException {
    try {
      return TypeConverter.fromYarnQueueInfo(client.getAllQueues(), this.conf);
    } catch (YarnException e) {
      throw new IOException(e);
    }
  }

  public QueueInfo[] getRootQueues() throws IOException, InterruptedException {
    try {
      return TypeConverter.fromYarnQueueInfo(client.getRootQueueInfos(),
          this.conf);
    } catch (YarnException e) {
      throw new IOException(e);
    }
  }

  public QueueInfo[] getChildQueues(String parent) throws IOException,
      InterruptedException {
    try {
      return TypeConverter.fromYarnQueueInfo(client.getChildQueueInfos(parent),
        this.conf);
    } catch (YarnException e) {
      throw new IOException(e);
    }
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

  @Override
  public YarnClientApplication createApplication() throws
      YarnException, IOException {
    return client.createApplication();
  }

  @Override
  public ApplicationId
      submitApplication(ApplicationSubmissionContext appContext)
          throws YarnException, IOException {
    return client.submitApplication(appContext);
  }

  @Override
  public void failApplicationAttempt(ApplicationAttemptId attemptId)
      throws YarnException, IOException {
    client.failApplicationAttempt(attemptId);
  }

  @Override
  public void killApplication(ApplicationId applicationId)
      throws YarnException, IOException {
    client.killApplication(applicationId);
  }

  @Override
  public ApplicationReport getApplicationReport(ApplicationId appId)
      throws YarnException, IOException {
    return client.getApplicationReport(appId);
  }

  @Override
  public Token<AMRMTokenIdentifier> getAMRMToken(ApplicationId appId) 
    throws YarnException, IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<ApplicationReport> getApplications() throws YarnException,
      IOException {
    return client.getApplications();
  }

  @Override
  public List<ApplicationReport> getApplications(Set<String> applicationTypes)
      throws YarnException,
      IOException {
    return client.getApplications(applicationTypes);
  }

  @Override
  public List<ApplicationReport> getApplications(
      EnumSet<YarnApplicationState> applicationStates) throws YarnException,
      IOException {
    return client.getApplications(applicationStates);
  }

  @Override
  public List<ApplicationReport> getApplications(
      Set<String> applicationTypes,
      EnumSet<YarnApplicationState> applicationStates)
      throws YarnException, IOException {
    return client.getApplications(applicationTypes, applicationStates);
  }

  @Override
  public List<ApplicationReport> getApplications(
      Set<String> applicationTypes,
      EnumSet<YarnApplicationState> applicationStates,
      Set<String> applicationTags)
      throws YarnException, IOException {
    return client.getApplications(
        applicationTypes, applicationStates, applicationTags);
  }

  @Override
  public List<ApplicationReport> getApplications(Set<String> queues,
      Set<String> users, Set<String> applicationTypes,
      EnumSet<YarnApplicationState> applicationStates) throws YarnException,
      IOException {
    return client.getApplications(queues, users, applicationTypes,
      applicationStates);
  }

  @Override
  public YarnClusterMetrics getYarnClusterMetrics() throws YarnException,
      IOException {
    return client.getYarnClusterMetrics();
  }

  @Override
  public List<NodeReport> getNodeReports(NodeState... states)
      throws YarnException, IOException {
    return client.getNodeReports(states);
  }

  @Override
  public org.apache.hadoop.yarn.api.records.Token getRMDelegationToken(
      Text renewer) throws YarnException, IOException {
    return client.getRMDelegationToken(renewer);
  }

  @Override
  public org.apache.hadoop.yarn.api.records.QueueInfo getQueueInfo(
      String queueName) throws YarnException, IOException {
    return client.getQueueInfo(queueName);
  }

  @Override
  public List<org.apache.hadoop.yarn.api.records.QueueInfo> getAllQueues()
      throws YarnException, IOException {
    return client.getAllQueues();
  }

  @Override
  public List<org.apache.hadoop.yarn.api.records.QueueInfo> getRootQueueInfos()
      throws YarnException, IOException {
    return client.getRootQueueInfos();
  }

  @Override
  public List<org.apache.hadoop.yarn.api.records.QueueInfo> getChildQueueInfos(
      String parent) throws YarnException, IOException {
    return client.getChildQueueInfos(parent);
  }

  @Override
  public List<QueueUserACLInfo> getQueueAclsInfo() throws YarnException,
      IOException {
    return client.getQueueAclsInfo();
  }

  @Override
  public ApplicationAttemptReport getApplicationAttemptReport(
      ApplicationAttemptId appAttemptId) throws YarnException, IOException {
    return client.getApplicationAttemptReport(appAttemptId);
  }

  @Override
  public List<ApplicationAttemptReport> getApplicationAttempts(
      ApplicationId appId) throws YarnException, IOException {
    return client.getApplicationAttempts(appId);
  }

  @Override
  public ContainerReport getContainerReport(ContainerId containerId)
      throws YarnException, IOException {
    return client.getContainerReport(containerId);
  }

  @Override
  public List<ContainerReport> getContainers(
      ApplicationAttemptId applicationAttemptId) throws YarnException,
      IOException {
    return client.getContainers(applicationAttemptId);
  }

  @Override
  public void moveApplicationAcrossQueues(ApplicationId appId, String queue)
      throws YarnException, IOException {
    client.moveApplicationAcrossQueues(appId, queue);
  }

  @Override
  public GetNewReservationResponse createReservation() throws YarnException,
      IOException {
    return client.createReservation();
  }

  @Override
  public ReservationSubmissionResponse submitReservation(
      ReservationSubmissionRequest request) throws YarnException, IOException {
    return client.submitReservation(request);
  }

  @Override
  public ReservationUpdateResponse updateReservation(
      ReservationUpdateRequest request) throws YarnException, IOException {
    return client.updateReservation(request);
  }

  @Override
  public ReservationDeleteResponse deleteReservation(
      ReservationDeleteRequest request) throws YarnException, IOException {
    return client.deleteReservation(request);
  }

  @Override
  public ReservationListResponse listReservations(
          ReservationListRequest request) throws YarnException, IOException {
    return client.listReservations(request);
  }
  @Override
  public Map<NodeId, Set<NodeLabel>> getNodeToLabels() throws YarnException,
      IOException {
    return client.getNodeToLabels();
  }

  @Override
  public Map<NodeLabel, Set<NodeId>> getLabelsToNodes() throws YarnException,
      IOException {
    return client.getLabelsToNodes();
  }

  @Override
  public Map<NodeLabel, Set<NodeId>> getLabelsToNodes(Set<String> labels)
      throws YarnException, IOException {
    return client.getLabelsToNodes(labels);
  }

  @Override
  public List<NodeLabel> getClusterNodeLabels()
      throws YarnException, IOException {
    return client.getClusterNodeLabels();
  }

  @Override
  public Priority updateApplicationPriority(ApplicationId applicationId,
      Priority priority) throws YarnException, IOException {
    return client.updateApplicationPriority(applicationId, priority);
  }

  @Override
  public void signalToContainer(ContainerId containerId,
      SignalContainerCommand command)
      throws YarnException, IOException {
    client.signalToContainer(containerId, command);
  }

  @Override
  public void killApplication(ApplicationId appId, String diagnostics)
      throws YarnException, IOException {
    client.killApplication(appId, diagnostics);
  }
}
