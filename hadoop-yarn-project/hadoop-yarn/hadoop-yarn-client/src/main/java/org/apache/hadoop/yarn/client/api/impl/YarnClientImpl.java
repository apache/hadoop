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

package org.apache.hadoop.yarn.client.api.impl;

import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Future;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataInputByteBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authentication.server.KerberosAuthenticationHandler;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.yarn.api.ApplicationClientProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.FailApplicationAttemptRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetAllResourceProfilesRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetAllResourceTypeInfoRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationAttemptReportRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationAttemptReportResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationAttemptsRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationAttemptsResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationReportRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationReportResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationsRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationsResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetAttributesToNodesRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetClusterMetricsRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetClusterMetricsResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetClusterNodeAttributesRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetClusterNodeLabelsRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetClusterNodesRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetClusterNodesResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetContainerReportRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetContainerReportResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetContainersRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetContainersResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetDelegationTokenRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetDelegationTokenResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetLabelsToNodesRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewReservationRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewReservationResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetNodesToAttributesRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetNodesToLabelsRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetQueueInfoRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetQueueUserAclsInfoRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetResourceProfileRequest;
import org.apache.hadoop.yarn.api.protocolrecords.KillApplicationRequest;
import org.apache.hadoop.yarn.api.protocolrecords.KillApplicationResponse;
import org.apache.hadoop.yarn.api.protocolrecords.MoveApplicationAcrossQueuesRequest;
import org.apache.hadoop.yarn.api.protocolrecords.ReservationDeleteRequest;
import org.apache.hadoop.yarn.api.protocolrecords.ReservationDeleteResponse;
import org.apache.hadoop.yarn.api.protocolrecords.ReservationListRequest;
import org.apache.hadoop.yarn.api.protocolrecords.ReservationListResponse;
import org.apache.hadoop.yarn.api.protocolrecords.ReservationSubmissionRequest;
import org.apache.hadoop.yarn.api.protocolrecords.ReservationSubmissionResponse;
import org.apache.hadoop.yarn.api.protocolrecords.ReservationUpdateRequest;
import org.apache.hadoop.yarn.api.protocolrecords.ReservationUpdateResponse;
import org.apache.hadoop.yarn.api.protocolrecords.SignalContainerRequest;
import org.apache.hadoop.yarn.api.protocolrecords.SubmitApplicationRequest;
import org.apache.hadoop.yarn.api.protocolrecords.UpdateApplicationPriorityRequest;
import org.apache.hadoop.yarn.api.protocolrecords.UpdateApplicationTimeoutsRequest;
import org.apache.hadoop.yarn.api.protocolrecords.UpdateApplicationTimeoutsResponse;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptReport;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.ContainerReport;
import org.apache.hadoop.yarn.api.records.NodeAttribute;
import org.apache.hadoop.yarn.api.records.NodeAttributeKey;
import org.apache.hadoop.yarn.api.records.NodeAttributeInfo;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.NodeLabel;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.NodeState;
import org.apache.hadoop.yarn.api.records.NodeToAttributeValue;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.QueueInfo;
import org.apache.hadoop.yarn.api.records.QueueUserACLInfo;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceTypeInfo;
import org.apache.hadoop.yarn.api.records.ShellContainerCommand;
import org.apache.hadoop.yarn.api.records.SignalContainerCommand;
import org.apache.hadoop.yarn.api.records.Token;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.api.records.YarnClusterMetrics;
import org.apache.hadoop.yarn.client.ClientRMProxy;
import org.apache.hadoop.yarn.client.api.AHSClient;
import org.apache.hadoop.yarn.client.api.ContainerShellWebSocket;
import org.apache.hadoop.yarn.client.api.TimelineClient;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.YarnClientApplication;
import org.apache.hadoop.yarn.client.util.YarnClientUtils;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.ApplicationIdNotProvidedException;
import org.apache.hadoop.yarn.exceptions.ApplicationNotFoundException;
import org.apache.hadoop.yarn.exceptions.ContainerNotFoundException;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.logaggregation.filecontroller.LogAggregationFileController;
import org.apache.hadoop.yarn.logaggregation.filecontroller.LogAggregationFileControllerFactory;
import org.apache.hadoop.yarn.security.AMRMTokenIdentifier;
import org.apache.hadoop.yarn.security.client.TimelineDelegationTokenIdentifier;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;
import org.apache.hadoop.yarn.util.resource.ResourceUtils;
import org.apache.hadoop.yarn.util.timeline.TimelineUtils;
import org.eclipse.jetty.websocket.api.Session;
import org.eclipse.jetty.websocket.api.WebSocketException;
import org.eclipse.jetty.websocket.client.ClientUpgradeRequest;
import org.eclipse.jetty.websocket.client.WebSocketClient;

import org.apache.hadoop.classification.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Private
@Unstable
public class YarnClientImpl extends YarnClient {

  private static final Logger LOG = LoggerFactory
          .getLogger(YarnClientImpl.class);

  protected ApplicationClientProtocol rmClient;
  protected long submitPollIntervalMillis;
  private long asyncApiPollIntervalMillis;
  private long asyncApiPollTimeoutMillis;
  protected AHSClient historyClient;
  private AHSClient ahsV2Client;
  private boolean historyServiceEnabled;
  protected volatile TimelineClient timelineClient;
  @VisibleForTesting
  Text timelineService;
  @VisibleForTesting
  String timelineDTRenewer;
  private boolean timelineV1ServiceEnabled;
  protected boolean timelineServiceBestEffort;
  private boolean loadResourceTypesFromServer;

  private boolean timelineV2ServiceEnabled;

  private static final String ROOT = "root";

  public YarnClientImpl() {
    super(YarnClientImpl.class.getName());
  }

  @SuppressWarnings("deprecation")
  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    asyncApiPollIntervalMillis =
        conf.getLong(YarnConfiguration.YARN_CLIENT_APPLICATION_CLIENT_PROTOCOL_POLL_INTERVAL_MS,
          YarnConfiguration.DEFAULT_YARN_CLIENT_APPLICATION_CLIENT_PROTOCOL_POLL_INTERVAL_MS);
    asyncApiPollTimeoutMillis =
        conf.getLong(YarnConfiguration.YARN_CLIENT_APPLICATION_CLIENT_PROTOCOL_POLL_TIMEOUT_MS,
            YarnConfiguration.DEFAULT_YARN_CLIENT_APPLICATION_CLIENT_PROTOCOL_POLL_TIMEOUT_MS);
    submitPollIntervalMillis = asyncApiPollIntervalMillis;
    if (conf.get(YarnConfiguration.YARN_CLIENT_APP_SUBMISSION_POLL_INTERVAL_MS)
        != null) {
      submitPollIntervalMillis = conf.getLong(
        YarnConfiguration.YARN_CLIENT_APP_SUBMISSION_POLL_INTERVAL_MS,
        YarnConfiguration.DEFAULT_YARN_CLIENT_APPLICATION_CLIENT_PROTOCOL_POLL_INTERVAL_MS);
    }

    if (YarnConfiguration.timelineServiceV1Enabled(conf)) {
      timelineV1ServiceEnabled = true;
      timelineDTRenewer = getTimelineDelegationTokenRenewer(conf);
      timelineService = TimelineUtils.buildTimelineTokenService(conf);
    }

    if (YarnConfiguration.timelineServiceV2Enabled(conf)) {
      timelineV2ServiceEnabled = true;
    }

    // The AHSClientService is enabled by default when we start the
    // TimelineServer which means we are able to get history information
    // for applications/applicationAttempts/containers by using ahsClient
    // when the TimelineServer is running.
    if (timelineV1ServiceEnabled || conf.getBoolean(
        YarnConfiguration.APPLICATION_HISTORY_ENABLED,
        YarnConfiguration.DEFAULT_APPLICATION_HISTORY_ENABLED)) {
      historyServiceEnabled = true;
      historyClient = AHSClient.createAHSClient();
      historyClient.init(conf);
    }

    if (timelineV2ServiceEnabled) {
      ahsV2Client = AHSClient.createAHSv2Client();
      ahsV2Client.init(conf);
    }

    timelineServiceBestEffort = conf.getBoolean(
        YarnConfiguration.TIMELINE_SERVICE_CLIENT_BEST_EFFORT,
        YarnConfiguration.DEFAULT_TIMELINE_SERVICE_CLIENT_BEST_EFFORT);

    loadResourceTypesFromServer = conf.getBoolean(
        YarnConfiguration.YARN_CLIENT_LOAD_RESOURCETYPES_FROM_SERVER,
        YarnConfiguration.DEFAULT_YARN_CLIENT_LOAD_RESOURCETYPES_FROM_SERVER);

    super.serviceInit(conf);
  }

  TimelineClient createTimelineClient() throws IOException, YarnException {
    return TimelineClient.createTimelineClient();
  }

  @Override
  protected void serviceStart() throws Exception {
    try {
      rmClient = ClientRMProxy.createRMProxy(getConfig(),
          ApplicationClientProtocol.class);
      if (historyServiceEnabled) {
        historyClient.start();
      }
      if (timelineV2ServiceEnabled) {
        ahsV2Client.start();
      }
    } catch (IOException e) {
      throw new YarnRuntimeException(e);
    }

    // Reinitialize local resource types cache from list of resources pulled from
    // RM.
    if (loadResourceTypesFromServer) {
      ResourceUtils.reinitializeResources(getResourceTypeInfo());
    }

    super.serviceStart();
  }

  @Override
  protected void serviceStop() throws Exception {
    if (this.rmClient != null) {
      RPC.stopProxy(this.rmClient);
    }
    if (historyServiceEnabled) {
      historyClient.stop();
    }
    if (timelineV2ServiceEnabled) {
      ahsV2Client.stop();
    }
    if (timelineClient != null) {
      timelineClient.stop();
    }
    super.serviceStop();
  }

  private GetNewApplicationResponse getNewApplication()
      throws YarnException, IOException {
    GetNewApplicationRequest request =
        Records.newRecord(GetNewApplicationRequest.class);
    return rmClient.getNewApplication(request);
  }

  @Override
  public YarnClientApplication createApplication()
      throws YarnException, IOException {
    ApplicationSubmissionContext context = Records.newRecord
        (ApplicationSubmissionContext.class);
    GetNewApplicationResponse newApp = getNewApplication();
    ApplicationId appId = newApp.getApplicationId();
    context.setApplicationId(appId);
    return new YarnClientApplication(newApp, context);
  }

  @Override
  public ApplicationId
      submitApplication(ApplicationSubmissionContext appContext)
          throws YarnException, IOException {
    ApplicationId applicationId = appContext.getApplicationId();
    if (applicationId == null) {
      throw new ApplicationIdNotProvidedException(
          "ApplicationId is not provided in ApplicationSubmissionContext");
    }
    SubmitApplicationRequest request =
        Records.newRecord(SubmitApplicationRequest.class);
    request.setApplicationSubmissionContext(appContext);

    // Automatically add the timeline DT into the CLC
    // Only when the security and the timeline service are both enabled
    if (isSecurityEnabled() && timelineV1ServiceEnabled &&
            getConfig().get(YarnConfiguration.TIMELINE_HTTP_AUTH_TYPE)
                    .equals(KerberosAuthenticationHandler.TYPE)) {
      addTimelineDelegationToken(appContext.getAMContainerSpec());
    }

    // Automatically add the DT for Log Aggregation path
    // This is useful when a separate storage is used for log aggregation
    try {
      if (isSecurityEnabled()) {
        addLogAggregationDelegationToken(appContext.getAMContainerSpec());
      }
    } catch (Exception e) {
      LOG.warn("Failed to obtain delegation token for Log Aggregation Path", e);
    }

    //TODO: YARN-1763:Handle RM failovers during the submitApplication call.
    rmClient.submitApplication(request);

    int pollCount = 0;
    long startTime = System.currentTimeMillis();
    EnumSet<YarnApplicationState> waitingStates = 
                                 EnumSet.of(YarnApplicationState.NEW,
                                 YarnApplicationState.NEW_SAVING,
                                 YarnApplicationState.SUBMITTED);
    EnumSet<YarnApplicationState> failToSubmitStates = 
                                  EnumSet.of(YarnApplicationState.FAILED,
                                  YarnApplicationState.KILLED);		
    while (true) {
      try {
        ApplicationReport appReport = getApplicationReport(applicationId);
        YarnApplicationState state = appReport.getYarnApplicationState();
        if (!waitingStates.contains(state)) {
          if(failToSubmitStates.contains(state)) {
            throw new YarnException("Failed to submit " + applicationId + 
                " to YARN : " + appReport.getDiagnostics());
          }
          LOG.info("Submitted application {}", applicationId);
          break;
        }

        long elapsedMillis = System.currentTimeMillis() - startTime;
        if (enforceAsyncAPITimeout() &&
            elapsedMillis >= asyncApiPollTimeoutMillis) {
          throw new YarnException("Timed out while waiting for application " +
              applicationId + " to be submitted successfully");
        }

        // Notify the client through the log every 10 poll, in case the client
        // is blocked here too long.
        if (++pollCount % 10 == 0) {
          LOG.info("Application submission is not finished, " +
                          "submitted application {} is still in {}",
                  applicationId,
                  state);
        }
        try {
          Thread.sleep(submitPollIntervalMillis);
        } catch (InterruptedException ie) {
          String msg = "Interrupted while waiting for application "
              + applicationId + " to be successfully submitted.";
          LOG.error(msg);
          throw new YarnException(msg, ie);
        }
      } catch (ApplicationNotFoundException ex) {
        // FailOver or RM restart happens before RMStateStore saves
        // ApplicationState
        LOG.info("Re-submit application {} with the" +
                " same ApplicationSubmissionContext", applicationId);
        rmClient.submitApplication(request);
      }
    }

    return applicationId;
  }

  private void addLogAggregationDelegationToken(
      ContainerLaunchContext clc) throws YarnException, IOException {
    Credentials credentials = new Credentials();
    DataInputByteBuffer dibb = new DataInputByteBuffer();
    ByteBuffer tokens = clc.getTokens();
    if (tokens != null) {
      dibb.reset(tokens);
      credentials.readTokenStorageStream(dibb);
      tokens.rewind();
    }

    Configuration conf = getConfig();
    String masterPrincipal = YarnClientUtils.getRmPrincipal(conf);
    if (StringUtils.isEmpty(masterPrincipal)) {
      throw new IOException(
          "Can't get Master Kerberos principal for use as renewer");
    }
    LOG.debug("Delegation Token Renewer: {}", masterPrincipal);

    LogAggregationFileControllerFactory factory =
        new LogAggregationFileControllerFactory(conf);
    LogAggregationFileController fileController =
        factory.getFileControllerForWrite();
    Path remoteRootLogDir = fileController.getRemoteRootLogDir();
    FileSystem fs = remoteRootLogDir.getFileSystem(conf);

    final org.apache.hadoop.security.token.Token<?>[] finalTokens =
        fs.addDelegationTokens(masterPrincipal, credentials);
    if (finalTokens != null) {
      for (org.apache.hadoop.security.token.Token<?> token : finalTokens) {
        LOG.info("Added delegation token for log aggregation path {}; {}", remoteRootLogDir, token);
      }
    }

    DataOutputBuffer dob = new DataOutputBuffer();
    credentials.writeTokenStorageToStream(dob);
    tokens = ByteBuffer.wrap(dob.getData(), 0, dob.getLength());
    clc.setTokens(tokens);
  }

  private void addTimelineDelegationToken(
      ContainerLaunchContext clc) throws YarnException, IOException {
    Credentials credentials = new Credentials();
    DataInputByteBuffer dibb = new DataInputByteBuffer();
    ByteBuffer tokens = clc.getTokens();
    if (tokens != null) {
      dibb.reset(tokens);
      credentials.readTokenStorageStream(dibb);
      tokens.rewind();
    }
    // If the timeline delegation token is already in the CLC, no need to add
    // one more
    for (org.apache.hadoop.security.token.Token<? extends TokenIdentifier> token : credentials
        .getAllTokens()) {
      if (token.getKind().equals(TimelineDelegationTokenIdentifier.KIND_NAME)) {
        return;
      }
    }
    org.apache.hadoop.security.token.Token<TimelineDelegationTokenIdentifier>
        timelineDelegationToken = getTimelineDelegationToken();
    if (timelineDelegationToken == null) {
      return;
    }
    credentials.addToken(timelineService, timelineDelegationToken);
    LOG.debug("Add timeline delegation token into credentials: {}",
        timelineDelegationToken);
    DataOutputBuffer dob = new DataOutputBuffer();
    credentials.writeTokenStorageToStream(dob);
    tokens = ByteBuffer.wrap(dob.getData(), 0, dob.getLength());
    clc.setTokens(tokens);
  }

  @VisibleForTesting
  org.apache.hadoop.security.token.Token<TimelineDelegationTokenIdentifier>
      getTimelineDelegationToken() throws IOException, YarnException {
    try {
      // Only reachable when both security and timeline service are enabled.
      if (timelineClient == null) {
        synchronized (this) {
          if (timelineClient == null) {
            TimelineClient tlClient = createTimelineClient();
            tlClient.init(getConfig());
            tlClient.start();
            // Assign value to timeline client variable only
            // when it is fully initiated. In order to avoid
            // other threads to see partially initialized object.
            this.timelineClient = tlClient;
          }
        }
      }
      return timelineClient.getDelegationToken(timelineDTRenewer);
    } catch (Exception e) {
      if (timelineServiceBestEffort) {
        LOG.warn("Failed to get delegation token from the timeline server: {}", e.getMessage());
        return null;
      }
      throw new IOException(e);
    } catch (NoClassDefFoundError e) {
      NoClassDefFoundError wrappedError = new NoClassDefFoundError(
          e.getMessage() + ". It appears that the timeline client "
              + "failed to initiate because an incompatible dependency "
              + "in classpath. If timeline service is optional to this "
              + "client, try to work around by setting "
              + YarnConfiguration.TIMELINE_SERVICE_ENABLED
              + " to false in client configuration.");
      wrappedError.setStackTrace(e.getStackTrace());
      throw wrappedError;
    }
  }

  private static String getTimelineDelegationTokenRenewer(Configuration conf)
      throws IOException, YarnException  {
    // Parse the RM daemon user if it exists in the config
    String rmPrincipal = conf.get(YarnConfiguration.RM_PRINCIPAL);
    String renewer = null;
    if (rmPrincipal != null && rmPrincipal.length() > 0) {
      String rmHost = conf.getSocketAddr(
          YarnConfiguration.RM_ADDRESS,
          YarnConfiguration.DEFAULT_RM_ADDRESS,
          YarnConfiguration.DEFAULT_RM_PORT).getHostName();
      renewer = SecurityUtil.getServerPrincipal(rmPrincipal, rmHost);
    }
    return renewer;
  }

  @Private
  @VisibleForTesting
  protected boolean isSecurityEnabled() {
    return UserGroupInformation.isSecurityEnabled();
  }

  @Override
  public void failApplicationAttempt(ApplicationAttemptId attemptId)
      throws YarnException, IOException {
    LOG.info("Failing application attempt {}.", attemptId);
    FailApplicationAttemptRequest request =
        Records.newRecord(FailApplicationAttemptRequest.class);
    request.setApplicationAttemptId(attemptId);
    rmClient.failApplicationAttempt(request);
  }

  @Override
  public void killApplication(ApplicationId applicationId)
      throws YarnException, IOException {
    killApplication(applicationId, null);
  }

  @Override
  public void killApplication(ApplicationId applicationId, String diagnostics)
      throws YarnException, IOException {

    KillApplicationRequest request =
        Records.newRecord(KillApplicationRequest.class);
    request.setApplicationId(applicationId);

    if (diagnostics != null) {
      request.setDiagnostics(diagnostics);
    }

    try {
      int pollCount = 0;
      long startTime = System.currentTimeMillis();

      while (true) {
        KillApplicationResponse response =
            rmClient.forceKillApplication(request);
        if (response.getIsKillCompleted()) {
          LOG.info("Killed application {}", applicationId);
          break;
        }

        long elapsedMillis = System.currentTimeMillis() - startTime;
        if (enforceAsyncAPITimeout()
            && elapsedMillis >= this.asyncApiPollTimeoutMillis) {
          throw new YarnException("Timed out while waiting for application "
              + applicationId + " to be killed.");
        }

        if (++pollCount % 10 == 0) {
          LOG.info(
              "Waiting for application {} to be killed.", applicationId);
        }
        Thread.sleep(asyncApiPollIntervalMillis);
      }
    } catch (InterruptedException e) {
      String msg = "Interrupted while waiting for application "
          + applicationId + " to be killed.";
      LOG.error(msg);
      throw new YarnException(msg, e);
    }
  }

  @VisibleForTesting
  boolean enforceAsyncAPITimeout() {
    return asyncApiPollTimeoutMillis >= 0;
  }

  @Override
  public ApplicationReport getApplicationReport(ApplicationId appId)
      throws YarnException, IOException {
    GetApplicationReportResponse response = null;
    try {
      GetApplicationReportRequest request = Records
          .newRecord(GetApplicationReportRequest.class);
      request.setApplicationId(appId);
      response = rmClient.getApplicationReport(request);
    } catch (ApplicationNotFoundException e) {
      if (timelineV2ServiceEnabled) {
        try {
          return ahsV2Client.getApplicationReport(appId);
        } catch (Exception ex) {
          LOG.warn("Failed to fetch application report from "
              + "ATS v2", ex);
        }
      }
      if (!historyServiceEnabled) {
        // Just throw it as usual if historyService is not enabled.
        throw e;
      }
      return historyClient.getApplicationReport(appId);
    }
    return response.getApplicationReport();
  }

  public org.apache.hadoop.security.token.Token<AMRMTokenIdentifier>
      getAMRMToken(ApplicationId appId) throws YarnException, IOException {
    Token token = getApplicationReport(appId).getAMRMToken();
    org.apache.hadoop.security.token.Token<AMRMTokenIdentifier> amrmToken =
        null;
    if (token != null) {
      amrmToken = ConverterUtils.convertFromYarn(token, (Text) null);
    }
    return amrmToken;
  }

  @Override
  public List<ApplicationReport> getApplications() throws YarnException,
      IOException {
    return getApplications(null, null);
  }

  @Override
  public List<ApplicationReport> getApplications(Set<String> applicationTypes)
      throws YarnException,
      IOException {
    return getApplications(applicationTypes, null);
  }

  @Override
  public List<ApplicationReport> getApplications(
      EnumSet<YarnApplicationState> applicationStates)
      throws YarnException, IOException {
    return getApplications(null, applicationStates);
  }

  @Override
  public List<ApplicationReport> getApplications(Set<String> applicationTypes,
      EnumSet<YarnApplicationState> applicationStates) throws YarnException,
      IOException {
    GetApplicationsRequest request =
        GetApplicationsRequest.newInstance(applicationTypes, applicationStates);
    GetApplicationsResponse response = rmClient.getApplications(request);
    return response.getApplicationList();
  }

  @Override
  public List<ApplicationReport> getApplications(Set<String> applicationTypes,
      EnumSet<YarnApplicationState> applicationStates,
      Set<String> applicationTags) throws YarnException, IOException {
    GetApplicationsRequest request =
        GetApplicationsRequest.newInstance(applicationTypes, applicationStates);
    request.setApplicationTags(applicationTags);
    GetApplicationsResponse response = rmClient.getApplications(request);
    return response.getApplicationList();
  }

  @Override
  public List<ApplicationReport> getApplications(Set<String> queues,
      Set<String> users, Set<String> applicationTypes,
      EnumSet<YarnApplicationState> applicationStates) throws YarnException,
      IOException {
    GetApplicationsRequest request =
        GetApplicationsRequest.newInstance(applicationTypes, applicationStates);
    request.setQueues(queues);
    request.setUsers(users);
    GetApplicationsResponse response = rmClient.getApplications(request);
    return response.getApplicationList();
  }

  @Override
  public List<ApplicationReport> getApplications(
      GetApplicationsRequest request) throws YarnException, IOException {
    GetApplicationsResponse response = rmClient.getApplications(request);
    return response.getApplicationList();
  }

  @Override
  public YarnClusterMetrics getYarnClusterMetrics() throws YarnException,
      IOException {
    GetClusterMetricsRequest request =
        Records.newRecord(GetClusterMetricsRequest.class);
    GetClusterMetricsResponse response = rmClient.getClusterMetrics(request);
    return response.getClusterMetrics();
  }

  @Override
  public List<NodeReport> getNodeReports(NodeState... states) throws YarnException,
      IOException {
    EnumSet<NodeState> statesSet = (states.length == 0) ?
        EnumSet.allOf(NodeState.class) : EnumSet.noneOf(NodeState.class);
    for (NodeState state : states) {
      statesSet.add(state);
    }
    GetClusterNodesRequest request = GetClusterNodesRequest
        .newInstance(statesSet);
    GetClusterNodesResponse response = rmClient.getClusterNodes(request);
    return response.getNodeReports();
  }

  @Override
  public Token getRMDelegationToken(Text renewer)
      throws YarnException, IOException {
    /* get the token from RM */
    GetDelegationTokenRequest rmDTRequest =
        Records.newRecord(GetDelegationTokenRequest.class);
    rmDTRequest.setRenewer(renewer.toString());
    GetDelegationTokenResponse response =
        rmClient.getDelegationToken(rmDTRequest);
    return response.getRMDelegationToken();
  }


  private GetQueueInfoRequest
      getQueueInfoRequest(String queueName, boolean includeApplications,
          boolean includeChildQueues, boolean recursive) {
    GetQueueInfoRequest request = Records.newRecord(GetQueueInfoRequest.class);
    request.setQueueName(queueName);
    request.setIncludeApplications(includeApplications);
    request.setIncludeChildQueues(includeChildQueues);
    request.setRecursive(recursive);
    return request;
  }

  @Override
  public QueueInfo getQueueInfo(String queueName) throws YarnException,
      IOException {
    GetQueueInfoRequest request =
        getQueueInfoRequest(queueName, true, false, false);
    Records.newRecord(GetQueueInfoRequest.class);
    return rmClient.getQueueInfo(request).getQueueInfo();
  }

  @Override
  public List<QueueUserACLInfo> getQueueAclsInfo() throws YarnException,
      IOException {
    GetQueueUserAclsInfoRequest request =
        Records.newRecord(GetQueueUserAclsInfoRequest.class);
    return rmClient.getQueueUserAcls(request).getUserAclsInfoList();
  }

  @Override
  public List<QueueInfo> getAllQueues() throws YarnException,
      IOException {
    List<QueueInfo> queues = new ArrayList<QueueInfo>();

    QueueInfo rootQueue =
        rmClient.getQueueInfo(getQueueInfoRequest(ROOT, false, true, true))
          .getQueueInfo();
    getChildQueues(rootQueue, queues, true);
    return queues;
  }

  @Override
  public List<QueueInfo> getRootQueueInfos() throws YarnException,
      IOException {
    List<QueueInfo> queues = new ArrayList<QueueInfo>();

    QueueInfo rootQueue =
        rmClient.getQueueInfo(getQueueInfoRequest(ROOT, false, true, true))
          .getQueueInfo();
    getChildQueues(rootQueue, queues, false);
    return queues;
  }

  @Override
  public List<QueueInfo> getChildQueueInfos(String parent)
      throws YarnException, IOException {
    List<QueueInfo> queues = new ArrayList<QueueInfo>();

    QueueInfo parentQueue =
        rmClient.getQueueInfo(getQueueInfoRequest(parent, false, true, false))
          .getQueueInfo();
    getChildQueues(parentQueue, queues, true);
    return queues;
  }

  private void getChildQueues(QueueInfo parent, List<QueueInfo> queues,
      boolean recursive) {
    List<QueueInfo> childQueues = parent.getChildQueues();

    for (QueueInfo child : childQueues) {
      queues.add(child);
      if (recursive) {
        getChildQueues(child, queues, recursive);
      }
    }
  }

  @Private
  @VisibleForTesting
  public void setRMClient(ApplicationClientProtocol rmClient) {
    this.rmClient = rmClient;
  }

  @Override
  public ApplicationAttemptReport getApplicationAttemptReport(
      ApplicationAttemptId appAttemptId) throws YarnException, IOException {
    try {
      GetApplicationAttemptReportRequest request = Records
          .newRecord(GetApplicationAttemptReportRequest.class);
      request.setApplicationAttemptId(appAttemptId);
      GetApplicationAttemptReportResponse response = rmClient
          .getApplicationAttemptReport(request);
      return response.getApplicationAttemptReport();
    } catch (YarnException e) {

      // Even if history-service is enabled, treat all exceptions still the same
      // except the following
      if (e.getClass() != ApplicationNotFoundException.class) {
        throw e;
      }
      if (timelineV2ServiceEnabled) {
        try {
          return ahsV2Client.getApplicationAttemptReport(appAttemptId);
        } catch (Exception ex) {
          LOG.warn("Failed to fetch application attempt report from "
              + "ATS v2", ex);
        }
      }
      if (!historyServiceEnabled) {
        // Just throw it as usual if historyService is not enabled.
        throw e;
      }
      return historyClient.getApplicationAttemptReport(appAttemptId);
    }
  }

  @Override
  public List<ApplicationAttemptReport> getApplicationAttempts(
      ApplicationId appId) throws YarnException, IOException {
    try {
      GetApplicationAttemptsRequest request = Records
          .newRecord(GetApplicationAttemptsRequest.class);
      request.setApplicationId(appId);
      GetApplicationAttemptsResponse response = rmClient
          .getApplicationAttempts(request);
      return response.getApplicationAttemptList();
    } catch (YarnException e) {
      // Even if history-service is enabled, treat all exceptions still the same
      // except the following
      if (e.getClass() != ApplicationNotFoundException.class) {
        throw e;
      }
      if (timelineV2ServiceEnabled) {
        try {
          return ahsV2Client.getApplicationAttempts(appId);
        } catch (Exception ex) {
          LOG.warn("Failed to fetch application attempts from "
              + "ATS v2", ex);
        }
      }
      if (!historyServiceEnabled) {
        // Just throw it as usual if historyService is not enabled.
        throw e;
      }
      return historyClient.getApplicationAttempts(appId);
    }
  }

  @Override
  public ContainerReport getContainerReport(ContainerId containerId)
      throws YarnException, IOException {
    try {
      GetContainerReportRequest request = Records
          .newRecord(GetContainerReportRequest.class);
      request.setContainerId(containerId);
      GetContainerReportResponse response = rmClient
          .getContainerReport(request);
      return response.getContainerReport();
    } catch (YarnException e) {
      // Even if history-service is enabled, treat all exceptions still the same
      // except the following
      if (e.getClass() != ApplicationNotFoundException.class
          && e.getClass() != ContainerNotFoundException.class) {
        throw e;
      }
      if (timelineV2ServiceEnabled) {
        try {
          return ahsV2Client.getContainerReport(containerId);
        } catch (Exception ex) {
          LOG.warn("Failed to fetch container report from "
              + "ATS v2", ex);
        }
      }
      if (!historyServiceEnabled) {
        // Just throw it as usual if historyService is not enabled.
        throw e;
      }
      return historyClient.getContainerReport(containerId);
    }
  }

  @Override
  public List<ContainerReport> getContainers(
      ApplicationAttemptId applicationAttemptId) throws YarnException,
      IOException {
    List<ContainerReport> containersForAttempt =
        new ArrayList<ContainerReport>();
    boolean appNotFoundInRM = false;
    try {
      GetContainersRequest request =
          Records.newRecord(GetContainersRequest.class);
      request.setApplicationAttemptId(applicationAttemptId);
      GetContainersResponse response = rmClient.getContainers(request);
      containersForAttempt.addAll(response.getContainerList());
    } catch (YarnException e) {
      // Even if history-service is enabled, treat all exceptions still the same
      // except the following
      if (e.getClass() != ApplicationNotFoundException.class) {
        throw e;
      }
      if (!historyServiceEnabled && !timelineV2ServiceEnabled) {
        // if both history server and ATSv2 are not enabled throw exception.
        throw e;
      }
      appNotFoundInRM = true;
    }
    // Check with AHS even if found in RM because to capture info of finished
    // containers also
    List<ContainerReport> containersListFromAHS = null;
    try {
      containersListFromAHS =
          getContainerReportFromHistory(applicationAttemptId);
    } catch (IOException | YarnException e) {
      if (appNotFoundInRM) {
        throw e;
      }
    }
    if (null != containersListFromAHS && containersListFromAHS.size() > 0) {
      // remove duplicates
      Set<ContainerId> containerIdsToBeKeptFromAHS =
          new HashSet<ContainerId>();
      Iterator<ContainerReport> tmpItr = containersListFromAHS.iterator();
      while (tmpItr.hasNext()) {
        containerIdsToBeKeptFromAHS.add(tmpItr.next().getContainerId());
      }

      Iterator<ContainerReport> rmContainers =
          containersForAttempt.iterator();
      while (rmContainers.hasNext()) {
        ContainerReport tmp = rmContainers.next();
        containerIdsToBeKeptFromAHS.remove(tmp.getContainerId());
        // Remove containers from AHS as container from RM will have latest
        // information
      }

      if (containerIdsToBeKeptFromAHS.size() > 0
          && containersListFromAHS.size() != containerIdsToBeKeptFromAHS
              .size()) {
        Iterator<ContainerReport> containersFromHS =
            containersListFromAHS.iterator();
        while (containersFromHS.hasNext()) {
          ContainerReport containerReport = containersFromHS.next();
          if (containerIdsToBeKeptFromAHS.contains(containerReport
              .getContainerId())) {
            containersForAttempt.add(containerReport);
          }
        }
      } else if (containersListFromAHS.size() == containerIdsToBeKeptFromAHS
          .size()) {
        containersForAttempt.addAll(containersListFromAHS);
      }
    }
    return containersForAttempt;
  }

  private List<ContainerReport> getContainerReportFromHistory(
      ApplicationAttemptId applicationAttemptId)
      throws IOException, YarnException {
    List<ContainerReport> containersListFromAHS = null;
    if (timelineV2ServiceEnabled) {
      try {
        containersListFromAHS = ahsV2Client.getContainers(applicationAttemptId);
      } catch (Exception e) {
        LOG.warn("Got an error while fetching container report from ATSv2", e);
        if (historyServiceEnabled) {
          containersListFromAHS = historyClient.getContainers(
              applicationAttemptId);
        } else {
          throw e;
        }
      }
    } else if (historyServiceEnabled) {
      containersListFromAHS = historyClient.getContainers(applicationAttemptId);
    }
    return containersListFromAHS;
  }

  @Override
  public void moveApplicationAcrossQueues(ApplicationId appId,
      String queue) throws YarnException, IOException {
    MoveApplicationAcrossQueuesRequest request =
        MoveApplicationAcrossQueuesRequest.newInstance(appId, queue);
    rmClient.moveApplicationAcrossQueues(request);
  }

  @Override
  public GetNewReservationResponse createReservation() throws YarnException,
      IOException {
    GetNewReservationRequest request =
        Records.newRecord(GetNewReservationRequest.class);
    return rmClient.getNewReservation(request);
  }

  @Override
  public ReservationSubmissionResponse submitReservation(
      ReservationSubmissionRequest request) throws YarnException, IOException {
    return rmClient.submitReservation(request);
  }

  @Override
  public ReservationUpdateResponse updateReservation(
      ReservationUpdateRequest request) throws YarnException, IOException {
    return rmClient.updateReservation(request);
  }

  @Override
  public ReservationDeleteResponse deleteReservation(
      ReservationDeleteRequest request) throws YarnException, IOException {
    return rmClient.deleteReservation(request);
  }

  @Override
  public ReservationListResponse listReservations(
          ReservationListRequest request) throws YarnException, IOException {
    return rmClient.listReservations(request);
  }

  @Override
  public Map<NodeId, Set<String>> getNodeToLabels() throws YarnException,
      IOException {
    return rmClient.getNodeToLabels(GetNodesToLabelsRequest.newInstance())
        .getNodeToLabels();
  }

  @Override
  public Map<String, Set<NodeId>> getLabelsToNodes() throws YarnException,
      IOException {
    return rmClient.getLabelsToNodes(GetLabelsToNodesRequest.newInstance())
        .getLabelsToNodes();
  }

  @Override
  public Map<String, Set<NodeId>> getLabelsToNodes(Set<String> labels)
      throws YarnException, IOException {
    return rmClient.getLabelsToNodes(
        GetLabelsToNodesRequest.newInstance(labels)).getLabelsToNodes();
  }

  @Override
  public List<NodeLabel> getClusterNodeLabels() throws YarnException, IOException {
    return rmClient.getClusterNodeLabels(
        GetClusterNodeLabelsRequest.newInstance()).getNodeLabelList();
  }

  @Override
  public Priority updateApplicationPriority(ApplicationId applicationId,
      Priority priority) throws YarnException, IOException {
    UpdateApplicationPriorityRequest request =
        UpdateApplicationPriorityRequest.newInstance(applicationId, priority);
    return rmClient.updateApplicationPriority(request).getApplicationPriority();
  }

  @Override
  public void signalToContainer(ContainerId containerId,
      SignalContainerCommand command)
          throws YarnException, IOException {
    LOG.info("Signalling container {} with command {}", containerId, command);
    SignalContainerRequest request =
        SignalContainerRequest.newInstance(containerId, command);
    rmClient.signalToContainer(request);
  }

  @Override
  public UpdateApplicationTimeoutsResponse updateApplicationTimeouts(
      UpdateApplicationTimeoutsRequest request)
      throws YarnException, IOException {
    return rmClient.updateApplicationTimeouts(request);
  }

  @Override
  public Map<String, Resource> getResourceProfiles()
      throws YarnException, IOException {
    GetAllResourceProfilesRequest request =
        GetAllResourceProfilesRequest.newInstance();
    return rmClient.getResourceProfiles(request).getResourceProfiles();
  }

  @Override
  public Resource getResourceProfile(String profile)
      throws YarnException, IOException {
    GetResourceProfileRequest request = GetResourceProfileRequest
        .newInstance(profile);
    return rmClient.getResourceProfile(request).getResource();
  }

  @Override
  public List<ResourceTypeInfo> getResourceTypeInfo()
      throws YarnException, IOException {
    GetAllResourceTypeInfoRequest request =
        GetAllResourceTypeInfoRequest.newInstance();
    return rmClient.getResourceTypeInfo(request).getResourceTypeInfo();
  }

  @Override
  public Set<NodeAttributeInfo> getClusterAttributes()
      throws YarnException, IOException {
    GetClusterNodeAttributesRequest request =
        GetClusterNodeAttributesRequest.newInstance();
    return rmClient.getClusterNodeAttributes(request).getNodeAttributes();
  }

  @Override
  public Map<NodeAttributeKey, List<NodeToAttributeValue>> getAttributesToNodes(
      Set<NodeAttributeKey> attributes) throws YarnException, IOException {
    GetAttributesToNodesRequest request =
        GetAttributesToNodesRequest.newInstance(attributes);
    return rmClient.getAttributesToNodes(request).getAttributesToNodes();
  }

  @Override
  public Map<String, Set<NodeAttribute>> getNodeToAttributes(
      Set<String> hostNames) throws YarnException, IOException {
    GetNodesToAttributesRequest request =
        GetNodesToAttributesRequest.newInstance(hostNames);
    return rmClient.getNodesToAttributes(request).getNodeToAttributes();
  }

  @Override
  public void shellToContainer(ContainerId containerId,
      ShellContainerCommand command) throws IOException {
    try {
      GetContainerReportRequest request = Records
          .newRecord(GetContainerReportRequest.class);
      request.setContainerId(containerId);
      GetContainerReportResponse response = rmClient
          .getContainerReport(request);
      URI nodeHttpAddress = new URI(response.getContainerReport()
          .getNodeHttpAddress());
      String host = nodeHttpAddress.getHost();
      int port = nodeHttpAddress.getPort();
      String scheme = nodeHttpAddress.getScheme();
      String protocol = "ws://";
      if (scheme.equals("https")) {
        protocol = "wss://";
      }
      WebSocketClient client = new WebSocketClient();
      URI uri = URI.create(protocol + host + ":" + port + "/container/" +
          containerId + "/" + command);
      if (!UserGroupInformation.isSecurityEnabled()) {
        uri = URI.create(protocol + host + ":" + port + "/container/" +
            containerId + "/" + command + "?user.name=" +
            System.getProperty("user.name"));
      }
      try {
        client.start();
        // The socket that receives events
        ContainerShellWebSocket socket = new ContainerShellWebSocket();
        ClientUpgradeRequest upgradeRequest = new ClientUpgradeRequest();
        if (UserGroupInformation.isSecurityEnabled()) {
          String challenge = YarnClientUtils.generateToken(host);
          upgradeRequest.setHeader("Authorization", "Negotiate " + challenge);
        }
        // Attempt Connect
        Future<Session> fut = client.connect(socket, uri, upgradeRequest);
        Session session = fut.get();
        if (session.isOpen()) {
          socket.run();
        }
      } finally {
        client.stop();
      }
    } catch (WebSocketException e) {
      LOG.debug("Websocket exception: {}", e.getMessage());
    } catch (Throwable t) {
      LOG.error("Fail to shell to container: {}", t.getMessage());
    }
  }
}
