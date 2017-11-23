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

package org.apache.hadoop.yarn.server.resourcemanager;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.security.AccessControlException;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.cli.UnrecognizedOptionException;
import org.apache.commons.lang.math.LongRange;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.ipc.CallerContext;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.UserGroupInformation.AuthenticationMethod;
import org.apache.hadoop.security.authorize.PolicyProvider;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.api.ApplicationClientProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.ApplicationsRequestScope;
import org.apache.hadoop.yarn.api.protocolrecords.CancelDelegationTokenRequest;
import org.apache.hadoop.yarn.api.protocolrecords.CancelDelegationTokenResponse;
import org.apache.hadoop.yarn.api.protocolrecords.FailApplicationAttemptRequest;
import org.apache.hadoop.yarn.api.protocolrecords.FailApplicationAttemptResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationAttemptReportRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationAttemptReportResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationAttemptsRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationAttemptsResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationReportRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationReportResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationsRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationsResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetClusterMetricsRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetClusterMetricsResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetClusterNodeLabelsRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetClusterNodeLabelsResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetClusterNodesRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetClusterNodesResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetContainerReportRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetContainerReportResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetContainersRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetContainersResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetDelegationTokenRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetDelegationTokenResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetLabelsToNodesRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetLabelsToNodesResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewReservationRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewReservationResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetNodesToLabelsRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetNodesToLabelsResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetQueueInfoRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetQueueInfoResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetQueueUserAclsInfoRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetQueueUserAclsInfoResponse;
import org.apache.hadoop.yarn.api.protocolrecords.KillApplicationRequest;
import org.apache.hadoop.yarn.api.protocolrecords.KillApplicationResponse;
import org.apache.hadoop.yarn.api.protocolrecords.MoveApplicationAcrossQueuesRequest;
import org.apache.hadoop.yarn.api.protocolrecords.MoveApplicationAcrossQueuesResponse;
import org.apache.hadoop.yarn.api.protocolrecords.RenewDelegationTokenRequest;
import org.apache.hadoop.yarn.api.protocolrecords.RenewDelegationTokenResponse;
import org.apache.hadoop.yarn.api.protocolrecords.ReservationDeleteRequest;
import org.apache.hadoop.yarn.api.protocolrecords.ReservationDeleteResponse;
import org.apache.hadoop.yarn.api.protocolrecords.ReservationListRequest;
import org.apache.hadoop.yarn.api.protocolrecords.ReservationListResponse;
import org.apache.hadoop.yarn.api.protocolrecords.ReservationSubmissionRequest;
import org.apache.hadoop.yarn.api.protocolrecords.ReservationSubmissionResponse;
import org.apache.hadoop.yarn.api.protocolrecords.ReservationUpdateRequest;
import org.apache.hadoop.yarn.api.protocolrecords.ReservationUpdateResponse;
import org.apache.hadoop.yarn.api.protocolrecords.SignalContainerRequest;
import org.apache.hadoop.yarn.api.protocolrecords.SignalContainerResponse;
import org.apache.hadoop.yarn.api.protocolrecords.SubmitApplicationRequest;
import org.apache.hadoop.yarn.api.protocolrecords.SubmitApplicationResponse;
import org.apache.hadoop.yarn.api.protocolrecords.UpdateApplicationPriorityRequest;
import org.apache.hadoop.yarn.api.protocolrecords.UpdateApplicationPriorityResponse;
import org.apache.hadoop.yarn.api.protocolrecords.UpdateApplicationTimeoutsRequest;
import org.apache.hadoop.yarn.api.protocolrecords.UpdateApplicationTimeoutsResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetAllResourceTypeInfoRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetAllResourceTypeInfoResponse;
import org.apache.hadoop.yarn.api.records.ApplicationAccessType;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptReport;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ApplicationTimeoutType;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerReport;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.NodeState;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.QueueACL;
import org.apache.hadoop.yarn.api.records.QueueInfo;
import org.apache.hadoop.yarn.api.records.ReservationACL;
import org.apache.hadoop.yarn.api.records.ReservationAllocationState;
import org.apache.hadoop.yarn.api.records.ReservationDefinition;
import org.apache.hadoop.yarn.api.records.ReservationId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.api.records.YarnClusterMetrics;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.ApplicationAttemptNotFoundException;
import org.apache.hadoop.yarn.exceptions.ApplicationNotFoundException;
import org.apache.hadoop.yarn.exceptions.ContainerNotFoundException;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.ipc.RPCUtil;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.yarn.security.client.RMDelegationTokenIdentifier;
import org.apache.hadoop.yarn.server.resourcemanager.RMAuditLogger.AuditConstants;
import org.apache.hadoop.yarn.server.resourcemanager.RMAuditLogger.Keys;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.Plan;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.ReservationAllocation;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.ReservationInputValidator;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.ReservationInterval;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.ReservationSystem;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.ReservationSystemUtil;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.exceptions.PlanningException;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppKillByClientEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppState;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptEventType;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNodeSignalContainerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerAppReport;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerNodeReport;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.YarnScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.security.QueueACLsManager;
import org.apache.hadoop.yarn.server.resourcemanager.security.RMDelegationTokenSecretManager;
import org.apache.hadoop.yarn.server.resourcemanager.security.ReservationsACLsManager;
import org.apache.hadoop.yarn.server.resourcemanager.security.authorize.RMPolicyProvider;
import org.apache.hadoop.yarn.server.security.ApplicationACLsManager;
import org.apache.hadoop.yarn.server.utils.BuilderUtils;
import org.apache.hadoop.yarn.util.Clock;
import org.apache.hadoop.yarn.util.Records;
import org.apache.hadoop.yarn.util.UTCClock;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.yarn.util.resource.ResourceUtils;
import org.apache.hadoop.yarn.util.timeline.TimelineUtils;


/**
 * The client interface to the Resource Manager. This module handles all the rpc
 * interfaces to the resource manager from the client.
 */
public class ClientRMService extends AbstractService implements
    ApplicationClientProtocol {
  private static final ArrayList<ApplicationReport> EMPTY_APPS_REPORT = new ArrayList<ApplicationReport>();

  private static final Log LOG = LogFactory.getLog(ClientRMService.class);

  final private AtomicInteger applicationCounter = new AtomicInteger(0);
  final private YarnScheduler scheduler;
  final private RMContext rmContext;
  private final RMAppManager rmAppManager;

  private Server server;
  protected RMDelegationTokenSecretManager rmDTSecretManager;

  private final RecordFactory recordFactory = RecordFactoryProvider.getRecordFactory(null);
  private InetSocketAddress clientBindAddress;

  private final ApplicationACLsManager applicationsACLsManager;
  private final QueueACLsManager queueACLsManager;

  // For Reservation APIs
  private Clock clock;
  private ReservationSystem reservationSystem;
  private ReservationInputValidator rValidator;

  private boolean displayPerUserApps = false;

  private static final EnumSet<RMAppState> ACTIVE_APP_STATES = EnumSet.of(
      RMAppState.ACCEPTED, RMAppState.RUNNING);

  public ClientRMService(RMContext rmContext, YarnScheduler scheduler,
      RMAppManager rmAppManager, ApplicationACLsManager applicationACLsManager,
      QueueACLsManager queueACLsManager,
      RMDelegationTokenSecretManager rmDTSecretManager) {
    this(rmContext, scheduler, rmAppManager, applicationACLsManager,
        queueACLsManager, rmDTSecretManager, new UTCClock());
  }

  public ClientRMService(RMContext rmContext, YarnScheduler scheduler,
      RMAppManager rmAppManager, ApplicationACLsManager applicationACLsManager,
      QueueACLsManager queueACLsManager,
      RMDelegationTokenSecretManager rmDTSecretManager, Clock clock) {
    super(ClientRMService.class.getName());
    this.scheduler = scheduler;
    this.rmContext = rmContext;
    this.rmAppManager = rmAppManager;
    this.applicationsACLsManager = applicationACLsManager;
    this.queueACLsManager = queueACLsManager;
    this.rmDTSecretManager = rmDTSecretManager;
    this.reservationSystem = rmContext.getReservationSystem();
    this.clock = clock;
    this.rValidator = new ReservationInputValidator(clock);
  }

  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    clientBindAddress = getBindAddress(conf);
    super.serviceInit(conf);
  }

  @Override
  protected void serviceStart() throws Exception {
    Configuration conf = getConfig();
    YarnRPC rpc = YarnRPC.create(conf);
    this.server =   
      rpc.getServer(ApplicationClientProtocol.class, this,
            clientBindAddress,
            conf, this.rmDTSecretManager,
            conf.getInt(YarnConfiguration.RM_CLIENT_THREAD_COUNT, 
                YarnConfiguration.DEFAULT_RM_CLIENT_THREAD_COUNT));
    
    // Enable service authorization?
    if (conf.getBoolean(
        CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHORIZATION, 
        false)) {
      InputStream inputStream =
          this.rmContext.getConfigurationProvider()
              .getConfigurationInputStream(conf,
                  YarnConfiguration.HADOOP_POLICY_CONFIGURATION_FILE);
      if (inputStream != null) {
        conf.addResource(inputStream);
      }
      refreshServiceAcls(conf, RMPolicyProvider.getInstance());
    }

    this.displayPerUserApps  = conf.getBoolean(
        YarnConfiguration.DISPLAY_APPS_FOR_LOGGED_IN_USER,
        YarnConfiguration.DEFAULT_DISPLAY_APPS_FOR_LOGGED_IN_USER);

    this.server.start();
    clientBindAddress = conf.updateConnectAddr(YarnConfiguration.RM_BIND_HOST,
                                               YarnConfiguration.RM_ADDRESS,
                                               YarnConfiguration.DEFAULT_RM_ADDRESS,
                                               server.getListenerAddress());
    super.serviceStart();
  }

  @Override
  protected void serviceStop() throws Exception {
    if (this.server != null) {
        this.server.stop();
    }
    super.serviceStop();
  }

  InetSocketAddress getBindAddress(Configuration conf) {
    return conf.getSocketAddr(
            YarnConfiguration.RM_BIND_HOST,
            YarnConfiguration.RM_ADDRESS,
            YarnConfiguration.DEFAULT_RM_ADDRESS,
            YarnConfiguration.DEFAULT_RM_PORT);
  }

  @Private
  public InetSocketAddress getBindAddress() {
    return clientBindAddress;
  }

  /**
   * check if the calling user has the access to application information.
   * @param callerUGI the user information who submit the request
   * @param owner the user of the application
   * @param operationPerformed the type of operation defined in
   *        {@link ApplicationAccessType}
   * @param application submitted application
   * @return access is permitted or not
   */
  private boolean checkAccess(UserGroupInformation callerUGI, String owner,
      ApplicationAccessType operationPerformed, RMApp application) {
    return applicationsACLsManager
        .checkAccess(callerUGI, operationPerformed, owner,
            application.getApplicationId()) || queueACLsManager
        .checkAccess(callerUGI, QueueACL.ADMINISTER_QUEUE, application,
            Server.getRemoteAddress(), null);
  }

  ApplicationId getNewApplicationId() {
    ApplicationId applicationId = org.apache.hadoop.yarn.server.utils.BuilderUtils
        .newApplicationId(recordFactory, ResourceManager.getClusterTimeStamp(),
            applicationCounter.incrementAndGet());
    LOG.info("Allocated new applicationId: " + applicationId.getId());
    return applicationId;
  }

  @Override
  public GetNewApplicationResponse getNewApplication(
      GetNewApplicationRequest request) throws YarnException {
    GetNewApplicationResponse response = recordFactory
        .newRecordInstance(GetNewApplicationResponse.class);
    response.setApplicationId(getNewApplicationId());
    // Pick up min/max resource from scheduler...
    response.setMaximumResourceCapability(scheduler
        .getMaximumResourceCapability());       
    
    return response;
  }
  
  /**
   * It gives response which includes application report if the application
   * present otherwise throws ApplicationNotFoundException.
   */
  @Override
  public GetApplicationReportResponse getApplicationReport(
      GetApplicationReportRequest request) throws YarnException {
    ApplicationId applicationId = request.getApplicationId();
    if (applicationId == null) {
      throw new ApplicationNotFoundException("Invalid application id: null");
    }

    UserGroupInformation callerUGI;
    try {
      callerUGI = UserGroupInformation.getCurrentUser();
    } catch (IOException ie) {
      LOG.info("Error getting UGI ", ie);
      throw RPCUtil.getRemoteException(ie);
    }

    RMApp application = this.rmContext.getRMApps().get(applicationId);
    if (application == null) {
      // If the RM doesn't have the application, throw
      // ApplicationNotFoundException and let client to handle.
      throw new ApplicationNotFoundException("Application with id '"
          + applicationId + "' doesn't exist in RM. Please check "
          + "that the job submission was successful.");
    }

    boolean allowAccess = checkAccess(callerUGI, application.getUser(),
        ApplicationAccessType.VIEW_APP, application);
    ApplicationReport report =
        application.createAndGetApplicationReport(callerUGI.getUserName(),
            allowAccess);

    GetApplicationReportResponse response = recordFactory
        .newRecordInstance(GetApplicationReportResponse.class);
    response.setApplicationReport(report);
    return response;
  }

  @Override
  public GetApplicationAttemptReportResponse getApplicationAttemptReport(
      GetApplicationAttemptReportRequest request) throws YarnException,
      IOException {
    ApplicationId applicationId
        = request.getApplicationAttemptId().getApplicationId();
    ApplicationAttemptId appAttemptId = request.getApplicationAttemptId();
    UserGroupInformation callerUGI = getCallerUgi(applicationId,
        AuditConstants.GET_APP_ATTEMPT_REPORT);
    RMApp application = verifyUserAccessForRMApp(applicationId, callerUGI,
        AuditConstants.GET_APP_ATTEMPT_REPORT, ApplicationAccessType.VIEW_APP,
        false);

    boolean allowAccess = checkAccess(callerUGI, application.getUser(),
        ApplicationAccessType.VIEW_APP, application);
    GetApplicationAttemptReportResponse response = null;
    if (allowAccess) {
      RMAppAttempt appAttempt = application.getAppAttempts().get(appAttemptId);
      if (appAttempt == null) {
        throw new ApplicationAttemptNotFoundException(
            "ApplicationAttempt with id '" + appAttemptId +
            "' doesn't exist in RM.");
      }
      ApplicationAttemptReport attemptReport = appAttempt
          .createApplicationAttemptReport();
      response = GetApplicationAttemptReportResponse.newInstance(attemptReport);
    }else{
      throw new YarnException("User " + callerUGI.getShortUserName()
          + " does not have privilege to see this attempt " + appAttemptId);
    }
    return response;
  }
  
  @Override
  public GetApplicationAttemptsResponse getApplicationAttempts(
      GetApplicationAttemptsRequest request) throws YarnException, IOException {
    ApplicationId appId = request.getApplicationId();
    UserGroupInformation callerUGI = getCallerUgi(appId,
        AuditConstants.GET_APP_ATTEMPTS);
    RMApp application = verifyUserAccessForRMApp(appId, callerUGI,
        AuditConstants.GET_APP_ATTEMPTS, ApplicationAccessType.VIEW_APP,
        false);
    boolean allowAccess = checkAccess(callerUGI, application.getUser(),
        ApplicationAccessType.VIEW_APP, application);
    GetApplicationAttemptsResponse response = null;
    if (allowAccess) {
      Map<ApplicationAttemptId, RMAppAttempt> attempts = application
          .getAppAttempts();
      List<ApplicationAttemptReport> listAttempts = 
        new ArrayList<ApplicationAttemptReport>();
      Iterator<Map.Entry<ApplicationAttemptId, RMAppAttempt>> iter = attempts
          .entrySet().iterator();
      while (iter.hasNext()) {
        listAttempts.add(iter.next().getValue()
            .createApplicationAttemptReport());
      }
      response = GetApplicationAttemptsResponse.newInstance(listAttempts);
    } else {
      throw new YarnException("User " + callerUGI.getShortUserName()
          + " does not have privilege to see this application " + appId);
    }
    return response;
  }
  
  /*
   * (non-Javadoc)
   * 
   * we're going to fix the issue of showing non-running containers of the
   * running application in YARN-1794
   */
  @Override
  public GetContainerReportResponse getContainerReport(
      GetContainerReportRequest request) throws YarnException, IOException {
    ContainerId containerId = request.getContainerId();
    ApplicationAttemptId appAttemptId = containerId.getApplicationAttemptId();
    ApplicationId appId = appAttemptId.getApplicationId();
    UserGroupInformation callerUGI = getCallerUgi(appId,
        AuditConstants.GET_CONTAINER_REPORT);
    RMApp application = verifyUserAccessForRMApp(appId, callerUGI,
        AuditConstants.GET_CONTAINER_REPORT, ApplicationAccessType.VIEW_APP,
        false);
    boolean allowAccess = checkAccess(callerUGI, application.getUser(),
        ApplicationAccessType.VIEW_APP, application);
    GetContainerReportResponse response = null;
    if (allowAccess) {
      RMAppAttempt appAttempt = application.getAppAttempts().get(appAttemptId);
      if (appAttempt == null) {
        throw new ApplicationAttemptNotFoundException(
            "ApplicationAttempt with id '" + appAttemptId +
            "' doesn't exist in RM.");
      }
      RMContainer rmContainer = this.rmContext.getScheduler().getRMContainer(
          containerId);
      if (rmContainer == null) {
        throw new ContainerNotFoundException("Container with id '" + containerId
            + "' doesn't exist in RM.");
      }
      response = GetContainerReportResponse.newInstance(rmContainer
          .createContainerReport());
    } else {
      throw new YarnException("User " + callerUGI.getShortUserName()
          + " does not have privilege to see this application " + appId);
    }
    return response;
  }
  
  /*
   * (non-Javadoc)
   * 
   * we're going to fix the issue of showing non-running containers of the
   * running application in YARN-1794"
   */
  @Override
  public GetContainersResponse getContainers(GetContainersRequest request)
      throws YarnException, IOException {
    ApplicationAttemptId appAttemptId = request.getApplicationAttemptId();
    ApplicationId appId = appAttemptId.getApplicationId();
    UserGroupInformation callerUGI = getCallerUgi(appId,
        AuditConstants.GET_CONTAINERS);
    RMApp application = verifyUserAccessForRMApp(appId, callerUGI,
        AuditConstants.GET_CONTAINERS, ApplicationAccessType.VIEW_APP, false);
    boolean allowAccess = checkAccess(callerUGI, application.getUser(),
        ApplicationAccessType.VIEW_APP, application);
    GetContainersResponse response = null;
    if (allowAccess) {
      RMAppAttempt appAttempt = application.getAppAttempts().get(appAttemptId);
      if (appAttempt == null) {
        throw new ApplicationAttemptNotFoundException(
            "ApplicationAttempt with id '" + appAttemptId +
            "' doesn't exist in RM.");
      }
      Collection<RMContainer> rmContainers = Collections.emptyList();
      SchedulerAppReport schedulerAppReport =
          this.rmContext.getScheduler().getSchedulerAppInfo(appAttemptId);
      if (schedulerAppReport != null) {
        rmContainers = schedulerAppReport.getLiveContainers();
      }
      List<ContainerReport> listContainers = new ArrayList<ContainerReport>();
      for (RMContainer rmContainer : rmContainers) {
        listContainers.add(rmContainer.createContainerReport());
      }
      response = GetContainersResponse.newInstance(listContainers);
    } else {
      throw new YarnException("User " + callerUGI.getShortUserName()
          + " does not have privilege to see this application " + appId);
    }
    return response;
  }

  @Override
  public SubmitApplicationResponse submitApplication(
      SubmitApplicationRequest request) throws YarnException, IOException {
    ApplicationSubmissionContext submissionContext = request
        .getApplicationSubmissionContext();
    ApplicationId applicationId = submissionContext.getApplicationId();
    CallerContext callerContext = CallerContext.getCurrent();

    // ApplicationSubmissionContext needs to be validated for safety - only
    // those fields that are independent of the RM's configuration will be
    // checked here, those that are dependent on RM configuration are validated
    // in RMAppManager.

    String user = null;
    try {
      // Safety
      user = UserGroupInformation.getCurrentUser().getShortUserName();
    } catch (IOException ie) {
      LOG.warn("Unable to get the current user.", ie);
      RMAuditLogger.logFailure(user, AuditConstants.SUBMIT_APP_REQUEST,
          ie.getMessage(), "ClientRMService",
          "Exception in submitting application", applicationId, callerContext);
      throw RPCUtil.getRemoteException(ie);
    }

    if (YarnConfiguration.timelineServiceV2Enabled(getConfig())) {
      // Sanity check for flow run
      String value = null;
      try {
        for (String tag : submissionContext.getApplicationTags()) {
          if (tag.startsWith(TimelineUtils.FLOW_RUN_ID_TAG_PREFIX + ":") ||
              tag.startsWith(
                  TimelineUtils.FLOW_RUN_ID_TAG_PREFIX.toLowerCase() + ":")) {
            value = tag.substring(TimelineUtils.FLOW_RUN_ID_TAG_PREFIX.length()
                + 1);
            // In order to check the number format
            Long.valueOf(value);
          }
        }
      } catch (NumberFormatException e) {
        LOG.warn("Invalid to flow run: " + value +
            ". Flow run should be a long integer", e);
        RMAuditLogger.logFailure(user, AuditConstants.SUBMIT_APP_REQUEST,
            e.getMessage(), "ClientRMService",
            "Exception in submitting application", applicationId);
        throw RPCUtil.getRemoteException(e);
      }
    }

    // Check whether app has already been put into rmContext,
    // If it is, simply return the response
    if (rmContext.getRMApps().get(applicationId) != null) {
      LOG.info("This is an earlier submitted application: " + applicationId);
      return SubmitApplicationResponse.newInstance();
    }

    ByteBuffer tokenConf =
        submissionContext.getAMContainerSpec().getTokensConf();
    if (tokenConf != null) {
      int maxSize = getConfig()
          .getInt(YarnConfiguration.RM_DELEGATION_TOKEN_MAX_CONF_SIZE,
              YarnConfiguration.DEFAULT_RM_DELEGATION_TOKEN_MAX_CONF_SIZE_BYTES);
      LOG.info("Using app provided configurations for delegation token renewal,"
          + " total size = " + tokenConf.capacity());
      if (tokenConf.capacity() > maxSize) {
        throw new YarnException(
            "Exceed " + YarnConfiguration.RM_DELEGATION_TOKEN_MAX_CONF_SIZE
                + " = " + maxSize + " bytes, current conf size = "
                + tokenConf.capacity() + " bytes.");
      }
    }
    if (submissionContext.getQueue() == null) {
      submissionContext.setQueue(YarnConfiguration.DEFAULT_QUEUE_NAME);
    }
    if (submissionContext.getApplicationName() == null) {
      submissionContext.setApplicationName(
          YarnConfiguration.DEFAULT_APPLICATION_NAME);
    }
    if (submissionContext.getApplicationType() == null) {
      submissionContext
        .setApplicationType(YarnConfiguration.DEFAULT_APPLICATION_TYPE);
    } else {
      if (submissionContext.getApplicationType().length() > YarnConfiguration.APPLICATION_TYPE_LENGTH) {
        submissionContext.setApplicationType(submissionContext
          .getApplicationType().substring(0,
            YarnConfiguration.APPLICATION_TYPE_LENGTH));
      }
    }

    ReservationId reservationId = request.getApplicationSubmissionContext()
            .getReservationID();

    checkReservationACLs(submissionContext.getQueue(), AuditConstants
            .SUBMIT_RESERVATION_REQUEST, reservationId);

    try {
      // call RMAppManager to submit application directly
      rmAppManager.submitApplication(submissionContext,
          System.currentTimeMillis(), user);

      LOG.info("Application with id " + applicationId.getId() + 
          " submitted by user " + user);
      RMAuditLogger.logSuccess(user, AuditConstants.SUBMIT_APP_REQUEST,
          "ClientRMService", applicationId, callerContext);
    } catch (YarnException e) {
      LOG.info("Exception in submitting " + applicationId, e);
      RMAuditLogger.logFailure(user, AuditConstants.SUBMIT_APP_REQUEST,
          e.getMessage(), "ClientRMService",
          "Exception in submitting application", applicationId, callerContext);
      throw e;
    }

    return recordFactory
        .newRecordInstance(SubmitApplicationResponse.class);
  }

  @SuppressWarnings("unchecked")
  @Override
  public FailApplicationAttemptResponse failApplicationAttempt(
      FailApplicationAttemptRequest request) throws YarnException {

    ApplicationAttemptId attemptId = request.getApplicationAttemptId();
    ApplicationId applicationId = attemptId.getApplicationId();

    UserGroupInformation callerUGI = getCallerUgi(applicationId,
        AuditConstants.FAIL_ATTEMPT_REQUEST);
    RMApp application = verifyUserAccessForRMApp(applicationId, callerUGI,
        AuditConstants.FAIL_ATTEMPT_REQUEST, ApplicationAccessType.MODIFY_APP,
        true);

    RMAppAttempt appAttempt = application.getAppAttempts().get(attemptId);
    if (appAttempt == null) {
      throw new ApplicationAttemptNotFoundException(
          "ApplicationAttempt with id '" + attemptId + "' doesn't exist in RM.");
    }

    FailApplicationAttemptResponse response =
        recordFactory.newRecordInstance(FailApplicationAttemptResponse.class);

    if (application.isAppInCompletedStates()) {
      RMAuditLogger.logSuccess(callerUGI.getShortUserName(),
          AuditConstants.FAIL_ATTEMPT_REQUEST, "ClientRMService",
          applicationId);
      return response;
    }

    this.rmContext.getDispatcher().getEventHandler().handle(
        new RMAppAttemptEvent(attemptId, RMAppAttemptEventType.FAIL,
        "Attempt failed by user."));

    RMAuditLogger.logSuccess(callerUGI.getShortUserName(),
        AuditConstants.FAIL_ATTEMPT_REQUEST, "ClientRMService", applicationId,
        attemptId);

    return response;
  }

  @SuppressWarnings("unchecked")
  @Override
  public KillApplicationResponse forceKillApplication(
      KillApplicationRequest request) throws YarnException {

    ApplicationId applicationId = request.getApplicationId();
    CallerContext callerContext = CallerContext.getCurrent();

    UserGroupInformation callerUGI;
    try {
      callerUGI = UserGroupInformation.getCurrentUser();
    } catch (IOException ie) {
      LOG.info("Error getting UGI ", ie);
      RMAuditLogger.logFailure("UNKNOWN", AuditConstants.KILL_APP_REQUEST,
              "UNKNOWN", "ClientRMService", "Error getting UGI",
              applicationId, callerContext);
      throw RPCUtil.getRemoteException(ie);
    }
    RMApp application = this.rmContext.getRMApps().get(applicationId);
    if (application == null) {
      RMAuditLogger.logFailure(callerUGI.getUserName(),
          AuditConstants.KILL_APP_REQUEST, "UNKNOWN", "ClientRMService",
          "Trying to kill an absent application", applicationId, callerContext);
      throw new ApplicationNotFoundException("Trying to kill an absent"
          + " application " + applicationId);
    }

    if (!checkAccess(callerUGI, application.getUser(),
        ApplicationAccessType.MODIFY_APP, application)) {
      RMAuditLogger.logFailure(callerUGI.getShortUserName(),
          AuditConstants.KILL_APP_REQUEST,
          "User doesn't have permissions to "
              + ApplicationAccessType.MODIFY_APP.toString(), "ClientRMService",
          AuditConstants.UNAUTHORIZED_USER, applicationId, callerContext);
      throw RPCUtil.getRemoteException(new AccessControlException("User "
          + callerUGI.getShortUserName() + " cannot perform operation "
          + ApplicationAccessType.MODIFY_APP.name() + " on " + applicationId));
    }

    if (application.isAppFinalStateStored()) {
      return KillApplicationResponse.newInstance(true);
    }

    StringBuilder message = new StringBuilder();
    message.append("Application ").append(applicationId)
        .append(" was killed by user ").append(callerUGI.getShortUserName());

    InetAddress remoteAddress = Server.getRemoteIp();
    if (null != remoteAddress) {
      message.append(" at ").append(remoteAddress.getHostAddress());
    }

    String diagnostics = org.apache.commons.lang.StringUtils
        .trimToNull(request.getDiagnostics());
    if (diagnostics != null) {
      message.append(" with diagnostic message: ");
      message.append(diagnostics);
    }

    this.rmContext.getDispatcher().getEventHandler()
        .handle(new RMAppKillByClientEvent(applicationId, message.toString(),
            callerUGI, remoteAddress));

    // For Unmanaged AMs, return true so they don't retry
    return KillApplicationResponse.newInstance(
        application.getApplicationSubmissionContext().getUnmanagedAM());
  }

  @Override
  public GetClusterMetricsResponse getClusterMetrics(
      GetClusterMetricsRequest request) throws YarnException {
    GetClusterMetricsResponse response = recordFactory
        .newRecordInstance(GetClusterMetricsResponse.class);
    YarnClusterMetrics ymetrics = recordFactory
        .newRecordInstance(YarnClusterMetrics.class);
    ymetrics.setNumNodeManagers(this.rmContext.getRMNodes().size());
    ClusterMetrics clusterMetrics = ClusterMetrics.getMetrics();
    ymetrics.setNumDecommissionedNodeManagers(clusterMetrics
      .getNumDecommisionedNMs());
    ymetrics.setNumActiveNodeManagers(clusterMetrics.getNumActiveNMs());
    ymetrics.setNumLostNodeManagers(clusterMetrics.getNumLostNMs());
    ymetrics.setNumUnhealthyNodeManagers(clusterMetrics.getUnhealthyNMs());
    ymetrics.setNumRebootedNodeManagers(clusterMetrics.getNumRebootedNMs());
    response.setClusterMetrics(ymetrics);
    return response;
  }

  /**
   * Get applications matching the {@link GetApplicationsRequest}. If
   * caseSensitive is set to false, applicationTypes in
   * GetApplicationRequest are expected to be in all-lowercase
   */
  @Override
  public GetApplicationsResponse getApplications(GetApplicationsRequest request)
      throws YarnException {
    UserGroupInformation callerUGI;
    try {
      callerUGI = UserGroupInformation.getCurrentUser();
    } catch (IOException ie) {
      LOG.info("Error getting UGI ", ie);
      throw RPCUtil.getRemoteException(ie);
    }

    Set<String> applicationTypes = getLowerCasedAppTypes(request);
    EnumSet<YarnApplicationState> applicationStates =
        request.getApplicationStates();
    Set<String> users = request.getUsers();
    Set<String> queues = request.getQueues();
    Set<String> tags = request.getApplicationTags();
    long limit = request.getLimit();
    LongRange start = request.getStartRange();
    LongRange finish = request.getFinishRange();
    ApplicationsRequestScope scope = request.getScope();

    final Map<ApplicationId, RMApp> apps = rmContext.getRMApps();
    Iterator<RMApp> appsIter;
    // If the query filters by queues, we can avoid considering apps outside
    // of those queues by asking the scheduler for the apps in those queues.
    if (queues != null && !queues.isEmpty()) {
      // Construct an iterator over apps in given queues
      // Collect list of lists to avoid copying all apps
      final List<List<ApplicationAttemptId>> queueAppLists =
          new ArrayList<List<ApplicationAttemptId>>();
      for (String queue : queues) {
        List<ApplicationAttemptId> appsInQueue = scheduler.getAppsInQueue(queue);
        if (appsInQueue != null && !appsInQueue.isEmpty()) {
          queueAppLists.add(appsInQueue);
        }
      }
      appsIter = new Iterator<RMApp>() {
        Iterator<List<ApplicationAttemptId>> appListIter = queueAppLists.iterator();
        Iterator<ApplicationAttemptId> schedAppsIter;

        @Override
        public boolean hasNext() {
          // Because queueAppLists has no empty lists, hasNext is whether the
          // current list hasNext or whether there are any remaining lists
          return (schedAppsIter != null && schedAppsIter.hasNext())
              || appListIter.hasNext();
        }
        @Override
        public RMApp next() {
          if (schedAppsIter == null || !schedAppsIter.hasNext()) {
            schedAppsIter = appListIter.next().iterator();
          }
          return apps.get(schedAppsIter.next().getApplicationId());
        }
        @Override
        public void remove() {
          throw new UnsupportedOperationException("Remove not supported");
        }
      };
    } else {
      appsIter = apps.values().iterator();
    }
    
    List<ApplicationReport> reports = new ArrayList<ApplicationReport>();
    while (appsIter.hasNext() && reports.size() < limit) {
      RMApp application = appsIter.next();

      // Check if current application falls under the specified scope
      if (scope == ApplicationsRequestScope.OWN &&
          !callerUGI.getUserName().equals(application.getUser())) {
        continue;
      }

      if (applicationTypes != null && !applicationTypes.isEmpty()) {
        String appTypeToMatch =
            StringUtils.toLowerCase(application.getApplicationType());
        if (!applicationTypes.contains(appTypeToMatch)) {
          continue;
        }
      }

      if (applicationStates != null && !applicationStates.isEmpty()) {
        if (!applicationStates.contains(application
            .createApplicationState())) {
          continue;
        }
      }

      if (users != null && !users.isEmpty() &&
          !users.contains(application.getUser())) {
        continue;
      }

      if (start != null && !start.containsLong(application.getStartTime())) {
        continue;
      }

      if (finish != null && !finish.containsLong(application.getFinishTime())) {
        continue;
      }

      if (tags != null && !tags.isEmpty()) {
        Set<String> appTags = application.getApplicationTags();
        if (appTags == null || appTags.isEmpty()) {
          continue;
        }
        boolean match = false;
        for (String tag : tags) {
          if (appTags.contains(tag)) {
            match = true;
            break;
          }
        }
        if (!match) {
          continue;
        }
      }

      // checkAccess can grab the scheduler lock so call it last
      boolean allowAccess = checkAccess(callerUGI, application.getUser(),
          ApplicationAccessType.VIEW_APP, application);
      if (scope == ApplicationsRequestScope.VIEWABLE && !allowAccess) {
        continue;
      }

      // Given RM is configured to display apps per user, skip apps to which
      // this caller doesn't have access to view.
      if (displayPerUserApps && !allowAccess) {
        continue;
      }

      reports.add(application.createAndGetApplicationReport(
          callerUGI.getUserName(), allowAccess));
    }

    RMAuditLogger.logSuccess(callerUGI.getUserName(),
        AuditConstants.GET_APPLICATIONS_REQUEST, "ClientRMService");
    GetApplicationsResponse response =
      recordFactory.newRecordInstance(GetApplicationsResponse.class);
    response.setApplicationList(reports);
    return response;
  }

  private Set<String> getLowerCasedAppTypes(GetApplicationsRequest request) {
    Set<String> applicationTypes = new HashSet<>();
    if (request.getApplicationTypes() != null && !request.getApplicationTypes()
        .isEmpty()) {
      for (String type : request.getApplicationTypes()) {
        applicationTypes.add(StringUtils.toLowerCase(type));
      }
    }
    return applicationTypes;
  }

  @Override
  public GetClusterNodesResponse getClusterNodes(GetClusterNodesRequest request)
      throws YarnException {
    GetClusterNodesResponse response = 
      recordFactory.newRecordInstance(GetClusterNodesResponse.class);
    EnumSet<NodeState> nodeStates = request.getNodeStates();
    if (nodeStates == null || nodeStates.isEmpty()) {
      nodeStates = EnumSet.allOf(NodeState.class);
    }
    Collection<RMNode> nodes = RMServerUtils.queryRMNodes(rmContext,
        nodeStates);
    
    List<NodeReport> nodeReports = new ArrayList<NodeReport>(nodes.size());
    for (RMNode nodeInfo : nodes) {
      nodeReports.add(createNodeReports(nodeInfo));
    }
    response.setNodeReports(nodeReports);
    return response;
  }

  @Override
  public GetQueueInfoResponse getQueueInfo(GetQueueInfoRequest request)
      throws YarnException {
    UserGroupInformation callerUGI;
    try {
      callerUGI = UserGroupInformation.getCurrentUser();
    } catch (IOException ie) {
      LOG.info("Error getting UGI ", ie);
      throw RPCUtil.getRemoteException(ie);
    }

    GetQueueInfoResponse response =
      recordFactory.newRecordInstance(GetQueueInfoResponse.class);
    RMAuditLogger.ArgsBuilder arguments = new RMAuditLogger.ArgsBuilder()
        .append(Keys.QUEUENAME, request.getQueueName())
        .append(Keys.INCLUDEAPPS,
            String.valueOf(request.getIncludeApplications()))
        .append(Keys.INCLUDECHILDQUEUES,
            String.valueOf(request.getIncludeChildQueues()))
        .append(Keys.RECURSIVE, String.valueOf(request.getRecursive()));
    try {
      QueueInfo queueInfo = 
        scheduler.getQueueInfo(request.getQueueName(),  
            request.getIncludeChildQueues(), 
            request.getRecursive());
      List<ApplicationReport> appReports = EMPTY_APPS_REPORT;
      if (request.getIncludeApplications()) {
        List<ApplicationAttemptId> apps =
            scheduler.getAppsInQueue(request.getQueueName());
        appReports = new ArrayList<ApplicationReport>(apps.size());
        for (ApplicationAttemptId app : apps) {
          RMApp rmApp = rmContext.getRMApps().get(app.getApplicationId());
          if (rmApp != null) {
            // Check if user is allowed access to this app
            if (!checkAccess(callerUGI, rmApp.getUser(),
                ApplicationAccessType.VIEW_APP, rmApp)) {
              continue;
            }
            appReports.add(
                rmApp.createAndGetApplicationReport(
                    callerUGI.getUserName(), true));
          }          
        }
      }
      queueInfo.setApplications(appReports);
      response.setQueueInfo(queueInfo);
      RMAuditLogger.logSuccess(callerUGI.getUserName(),
          AuditConstants.GET_QUEUE_INFO_REQUEST,
          "ClientRMService", arguments);
    } catch (IOException ioe) {
      LOG.info("Failed to getQueueInfo for " + request.getQueueName(), ioe);
      RMAuditLogger.logFailure(callerUGI.getUserName(),
          AuditConstants.GET_QUEUE_INFO_REQUEST, "UNKNOWN", "ClientRMService",
          ioe.getMessage(), arguments);
    }
    
    return response;
  }

  private NodeReport createNodeReports(RMNode rmNode) {
    SchedulerNodeReport schedulerNodeReport = 
        scheduler.getNodeReport(rmNode.getNodeID());
    Resource used = BuilderUtils.newResource(0, 0);
    int numContainers = 0;
    if (schedulerNodeReport != null) {
      used = schedulerNodeReport.getUsedResource();
      numContainers = schedulerNodeReport.getNumContainers();
    } 

    NodeReport report =
        BuilderUtils.newNodeReport(rmNode.getNodeID(), rmNode.getState(),
            rmNode.getHttpAddress(), rmNode.getRackName(), used,
            rmNode.getTotalCapability(), numContainers,
            rmNode.getHealthReport(), rmNode.getLastHealthReportTime(),
            rmNode.getNodeLabels(), rmNode.getAggregatedContainersUtilization(),
            rmNode.getNodeUtilization(), rmNode.getDecommissioningTimeout(),
            null);

    return report;
  }

  @Override
  public GetQueueUserAclsInfoResponse getQueueUserAcls(
      GetQueueUserAclsInfoRequest request) throws YarnException {
    GetQueueUserAclsInfoResponse response = 
      recordFactory.newRecordInstance(GetQueueUserAclsInfoResponse.class);
    response.setUserAclsInfoList(scheduler.getQueueUserAclInfo());
    return response;
  }


  @Override
  public GetDelegationTokenResponse getDelegationToken(
      GetDelegationTokenRequest request) throws YarnException {
    try {

      // Verify that the connection is kerberos authenticated
      if (!isAllowedDelegationTokenOp()) {
        throw new IOException(
          "Delegation Token can be issued only with kerberos authentication");
      }

      GetDelegationTokenResponse response =
          recordFactory.newRecordInstance(GetDelegationTokenResponse.class);
      UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
      Text owner = new Text(ugi.getUserName());
      Text realUser = null;
      if (ugi.getRealUser() != null) {
        realUser = new Text(ugi.getRealUser().getUserName());
      }
      RMDelegationTokenIdentifier tokenIdentifier =
          new RMDelegationTokenIdentifier(owner, new Text(request.getRenewer()), 
              realUser);
      Token<RMDelegationTokenIdentifier> realRMDToken =
          new Token<RMDelegationTokenIdentifier>(tokenIdentifier,
              this.rmDTSecretManager);
      response.setRMDelegationToken(
          BuilderUtils.newDelegationToken(
              realRMDToken.getIdentifier(),
              realRMDToken.getKind().toString(),
              realRMDToken.getPassword(),
              realRMDToken.getService().toString()
              ));
      return response;
    } catch(IOException io) {
      throw RPCUtil.getRemoteException(io);
    }
  }

  @Override
  public RenewDelegationTokenResponse renewDelegationToken(
      RenewDelegationTokenRequest request) throws YarnException {
    try {
      if (!isAllowedDelegationTokenOp()) {
        throw new IOException(
            "Delegation Token can be renewed only with kerberos authentication");
      }
      
      org.apache.hadoop.yarn.api.records.Token protoToken = request.getDelegationToken();
      Token<RMDelegationTokenIdentifier> token = new Token<RMDelegationTokenIdentifier>(
          protoToken.getIdentifier().array(), protoToken.getPassword().array(),
          new Text(protoToken.getKind()), new Text(protoToken.getService()));

      String user = getRenewerForToken(token);
      long nextExpTime = rmDTSecretManager.renewToken(token, user);
      RenewDelegationTokenResponse renewResponse = Records
          .newRecord(RenewDelegationTokenResponse.class);
      renewResponse.setNextExpirationTime(nextExpTime);
      return renewResponse;
    } catch (IOException e) {
      throw RPCUtil.getRemoteException(e);
    }
  }

  @Override
  public CancelDelegationTokenResponse cancelDelegationToken(
      CancelDelegationTokenRequest request) throws YarnException {
    try {
      if (!isAllowedDelegationTokenOp()) {
        throw new IOException(
            "Delegation Token can be cancelled only with kerberos authentication");
      }
      org.apache.hadoop.yarn.api.records.Token protoToken = request.getDelegationToken();
      Token<RMDelegationTokenIdentifier> token = new Token<RMDelegationTokenIdentifier>(
          protoToken.getIdentifier().array(), protoToken.getPassword().array(),
          new Text(protoToken.getKind()), new Text(protoToken.getService()));

      String user = UserGroupInformation.getCurrentUser().getUserName();
      rmDTSecretManager.cancelToken(token, user);
      return Records.newRecord(CancelDelegationTokenResponse.class);
    } catch (IOException e) {
      throw RPCUtil.getRemoteException(e);
    }
  }
  
  @SuppressWarnings("unchecked")
  @Override
  public MoveApplicationAcrossQueuesResponse moveApplicationAcrossQueues(
      MoveApplicationAcrossQueuesRequest request) throws YarnException {
    ApplicationId applicationId = request.getApplicationId();

    UserGroupInformation callerUGI = getCallerUgi(applicationId,
        AuditConstants.MOVE_APP_REQUEST);
    RMApp application = verifyUserAccessForRMApp(applicationId, callerUGI,
        AuditConstants.MOVE_APP_REQUEST, ApplicationAccessType.MODIFY_APP,
        true);

    String targetQueue = request.getTargetQueue();
    if (!accessToTargetQueueAllowed(callerUGI, application, targetQueue)) {
      RMAuditLogger.logFailure(callerUGI.getShortUserName(),
          AuditConstants.MOVE_APP_REQUEST, "Target queue doesn't exist or user"
              + " doesn't have permissions to submit to target queue: "
              + targetQueue, "ClientRMService",
          AuditConstants.UNAUTHORIZED_USER, applicationId);
      throw RPCUtil.getRemoteException(new AccessControlException("User "
          + callerUGI.getShortUserName() + " cannot submit applications to"
          + " target queue or the target queue doesn't exist: "
          + targetQueue + " while moving " + applicationId));
    }

    // Moves only allowed when app is in a state that means it is tracked by
    // the scheduler. Introducing SUBMITTED state also to this list as there
    // could be a corner scenario that app may not be in Scheduler in SUBMITTED
    // state.
    if (!ACTIVE_APP_STATES.contains(application.getState())) {
      String msg = "App in " + application.getState() +
          " state cannot be moved.";
      RMAuditLogger.logFailure(callerUGI.getShortUserName(),
          AuditConstants.MOVE_APP_REQUEST, "UNKNOWN", "ClientRMService", msg);
      throw new YarnException(msg);
    }

    try {
      this.rmAppManager.moveApplicationAcrossQueue(
          application.getApplicationId(),
          request.getTargetQueue());
    } catch (YarnException ex) {
      RMAuditLogger.logFailure(callerUGI.getShortUserName(),
          AuditConstants.MOVE_APP_REQUEST, "UNKNOWN", "ClientRMService",
          ex.getMessage());
      throw ex;
    }

    RMAuditLogger.logSuccess(callerUGI.getShortUserName(), 
        AuditConstants.MOVE_APP_REQUEST, "ClientRMService" , applicationId);
    return recordFactory
        .newRecordInstance(MoveApplicationAcrossQueuesResponse.class);
  }

  /**
   * Check if the submission of an application to the target queue is allowed.
   * @param callerUGI the caller UGI
   * @param application the application to move
   * @param targetQueue the queue to move the application to
   * @return true if submission is allowed, false otherwise
   */
  private boolean accessToTargetQueueAllowed(UserGroupInformation callerUGI,
      RMApp application, String targetQueue) {
    return
        queueACLsManager.checkAccess(callerUGI,
            QueueACL.SUBMIT_APPLICATIONS, application,
            Server.getRemoteAddress(), null, targetQueue) ||
        queueACLsManager.checkAccess(callerUGI,
            QueueACL.ADMINISTER_QUEUE, application,
            Server.getRemoteAddress(), null, targetQueue);
  }

  private String getRenewerForToken(Token<RMDelegationTokenIdentifier> token)
      throws IOException {
    UserGroupInformation user = UserGroupInformation.getCurrentUser();
    UserGroupInformation loginUser = UserGroupInformation.getLoginUser();
    // we can always renew our own tokens
    return loginUser.getUserName().equals(user.getUserName())
        ? token.decodeIdentifier().getRenewer().toString()
        : user.getShortUserName();
  }

  void refreshServiceAcls(Configuration configuration, 
      PolicyProvider policyProvider) {
    this.server.refreshServiceAclWithLoadedConfiguration(configuration,
        policyProvider);
  }

  private boolean isAllowedDelegationTokenOp() throws IOException {
    if (UserGroupInformation.isSecurityEnabled()) {
      return EnumSet.of(AuthenticationMethod.KERBEROS,
                        AuthenticationMethod.KERBEROS_SSL,
                        AuthenticationMethod.CERTIFICATE)
          .contains(UserGroupInformation.getCurrentUser()
                  .getRealAuthenticationMethod());
    } else {
      return true;
    }
  }

  @VisibleForTesting
  public Server getServer() {
    return this.server;
  }

  @Override
  public GetNewReservationResponse getNewReservation(
      GetNewReservationRequest request) throws YarnException, IOException {
    checkReservationSystem();
    GetNewReservationResponse response =
        recordFactory.newRecordInstance(GetNewReservationResponse.class);

    ReservationId reservationId = reservationSystem.getNewReservationId();
    response.setReservationId(reservationId);
    // Create a new Reservation Id
    return response;
  }

  @Override
  public ReservationSubmissionResponse submitReservation(
      ReservationSubmissionRequest request) throws YarnException, IOException {
    // Check if reservation system is enabled
    checkReservationSystem();
    ReservationSubmissionResponse response =
        recordFactory.newRecordInstance(ReservationSubmissionResponse.class);
    ReservationId reservationId = request.getReservationId();
    // Validate the input
    Plan plan =
        rValidator.validateReservationSubmissionRequest(reservationSystem,
            request, reservationId);

    ReservationAllocation allocation = plan.getReservationById(reservationId);

    if (allocation != null) {
      boolean isNewDefinition = !allocation.getReservationDefinition().equals(
          request.getReservationDefinition());
      if (isNewDefinition) {
        String message = "Reservation allocation already exists with the " +
            "reservation id " + reservationId.toString() + ", but a different" +
            " reservation definition was provided. Please try again with a " +
            "new reservation id, or consider updating the reservation instead.";
        throw RPCUtil.getRemoteException(message);
      } else {
        return response;
      }
    }

    // Check ACLs
    String queueName = request.getQueue();
    String user =
        checkReservationACLs(queueName,
            AuditConstants.SUBMIT_RESERVATION_REQUEST, null);
    try {
      // Try to place the reservation using the agent
      boolean result =
          plan.getReservationAgent().createReservation(reservationId, user,
              plan, request.getReservationDefinition());
      if (result) {
        // add the reservation id to valid ones maintained by reservation
        // system
        reservationSystem.setQueueForReservation(reservationId, queueName);
        // create the reservation synchronously if required
        refreshScheduler(queueName, request.getReservationDefinition(),
            reservationId.toString());
        // return the reservation id
      }
    } catch (PlanningException e) {
      RMAuditLogger.logFailure(user, AuditConstants.SUBMIT_RESERVATION_REQUEST,
          e.getMessage(), "ClientRMService",
          "Unable to create the reservation: " + reservationId);
      throw RPCUtil.getRemoteException(e);
    }
    RMAuditLogger.logSuccess(user, AuditConstants.SUBMIT_RESERVATION_REQUEST,
        "ClientRMService: " + reservationId);
    return response;
  }

  @Override
  public ReservationUpdateResponse updateReservation(
      ReservationUpdateRequest request) throws YarnException, IOException {
    // Check if reservation system is enabled
    checkReservationSystem();
    ReservationUpdateResponse response =
        recordFactory.newRecordInstance(ReservationUpdateResponse.class);
    // Validate the input
    Plan plan =
        rValidator.validateReservationUpdateRequest(reservationSystem, request);
    ReservationId reservationId = request.getReservationId();
    String queueName = reservationSystem.getQueueForReservation(reservationId);
    // Check ACLs
    String user =
        checkReservationACLs(queueName,
            AuditConstants.UPDATE_RESERVATION_REQUEST, reservationId);
    // Try to update the reservation using default agent
    try {
      boolean result =
          plan.getReservationAgent().updateReservation(reservationId, user,
              plan, request.getReservationDefinition());
      if (!result) {
        String errMsg = "Unable to update reservation: " + reservationId;
        RMAuditLogger.logFailure(user,
            AuditConstants.UPDATE_RESERVATION_REQUEST, errMsg,
            "ClientRMService", errMsg);
        throw RPCUtil.getRemoteException(errMsg);
      }
    } catch (PlanningException e) {
      RMAuditLogger.logFailure(user, AuditConstants.UPDATE_RESERVATION_REQUEST,
          e.getMessage(), "ClientRMService",
          "Unable to update the reservation: " + reservationId);
      throw RPCUtil.getRemoteException(e);
    }
    RMAuditLogger.logSuccess(user, AuditConstants.UPDATE_RESERVATION_REQUEST,
        "ClientRMService: " + reservationId);
    return response;
  }

  @Override
  public ReservationDeleteResponse deleteReservation(
      ReservationDeleteRequest request) throws YarnException, IOException {
    // Check if reservation system is enabled
    checkReservationSystem();
    ReservationDeleteResponse response =
        recordFactory.newRecordInstance(ReservationDeleteResponse.class);
    // Validate the input
    Plan plan =
        rValidator.validateReservationDeleteRequest(reservationSystem, request);
    ReservationId reservationId = request.getReservationId();
    String queueName = reservationSystem.getQueueForReservation(reservationId);
    // Check ACLs
    String user =
        checkReservationACLs(queueName,
            AuditConstants.DELETE_RESERVATION_REQUEST, reservationId);
    // Try to update the reservation using default agent
    try {
      boolean result =
          plan.getReservationAgent().deleteReservation(reservationId, user,
              plan);
      if (!result) {
        String errMsg = "Could not delete reservation: " + reservationId;
        RMAuditLogger.logFailure(user,
            AuditConstants.DELETE_RESERVATION_REQUEST, errMsg,
            "ClientRMService", errMsg);
        throw RPCUtil.getRemoteException(errMsg);
      }
    } catch (PlanningException e) {
      RMAuditLogger.logFailure(user, AuditConstants.DELETE_RESERVATION_REQUEST,
          e.getMessage(), "ClientRMService",
          "Unable to delete the reservation: " + reservationId);
      throw RPCUtil.getRemoteException(e);
    }
    RMAuditLogger.logSuccess(user, AuditConstants.DELETE_RESERVATION_REQUEST,
        "ClientRMService: " + reservationId);
    return response;
  }

  @Override
  public ReservationListResponse listReservations(
        ReservationListRequest requestInfo) throws YarnException, IOException {
    // Check if reservation system is enabled
    checkReservationSystem();
    ReservationListResponse response =
            recordFactory.newRecordInstance(ReservationListResponse.class);

    Plan plan = rValidator.validateReservationListRequest(
            reservationSystem, requestInfo);
    boolean includeResourceAllocations = requestInfo
            .getIncludeResourceAllocations();

    ReservationId reservationId = null;
    if (requestInfo.getReservationId() != null && !requestInfo
            .getReservationId().isEmpty()) {
      reservationId = ReservationId.parseReservationId(
            requestInfo.getReservationId());
    }

    checkReservationACLs(requestInfo.getQueue(),
            AuditConstants.LIST_RESERVATION_REQUEST, reservationId);

    long startTime = Math.max(requestInfo.getStartTime(), 0);
    long endTime = requestInfo.getEndTime() <= -1? Long.MAX_VALUE : requestInfo
            .getEndTime();

    Set<ReservationAllocation> reservations;

    reservations = plan.getReservations(reservationId, new ReservationInterval(
            startTime, endTime));

    List<ReservationAllocationState> info =
            ReservationSystemUtil.convertAllocationsToReservationInfo(
                    reservations, includeResourceAllocations);

    response.setReservationAllocationState(info);
    return response;
  }

  @Override
  public GetNodesToLabelsResponse getNodeToLabels(
      GetNodesToLabelsRequest request) throws YarnException, IOException {
    RMNodeLabelsManager labelsMgr = rmContext.getNodeLabelManager();
    return GetNodesToLabelsResponse.newInstance(labelsMgr.getNodeLabels());
  }

  @Override
  public GetLabelsToNodesResponse getLabelsToNodes(
      GetLabelsToNodesRequest request) throws YarnException, IOException {
    RMNodeLabelsManager labelsMgr = rmContext.getNodeLabelManager();
    if (request.getNodeLabels() == null || request.getNodeLabels().isEmpty()) {
      return GetLabelsToNodesResponse.newInstance(labelsMgr.getLabelsToNodes());
    } else {
      return GetLabelsToNodesResponse.newInstance(
          labelsMgr.getLabelsToNodes(request.getNodeLabels()));
    }
  }

  @Override
  public GetClusterNodeLabelsResponse getClusterNodeLabels(
      GetClusterNodeLabelsRequest request) throws YarnException, IOException {
    RMNodeLabelsManager labelsMgr = rmContext.getNodeLabelManager();
    return GetClusterNodeLabelsResponse.newInstance(
            labelsMgr.getClusterNodeLabels());
  }

  private void checkReservationSystem()
      throws YarnException {
    // Check if reservation is enabled
    if (reservationSystem == null) {
      throw RPCUtil.getRemoteException("Reservation is not enabled."
          + " Please enable & try again");
    }
  }

  private void refreshScheduler(String planName,
      ReservationDefinition contract, String reservationId) {
    if ((contract.getArrival() - clock.getTime()) < reservationSystem
        .getPlanFollowerTimeStep()) {
      LOG.debug(MessageFormat
          .format(
              "Reservation {0} is within threshold so attempting to create synchronously.",
              reservationId));
      reservationSystem.synchronizePlan(planName, true);
      LOG.info(MessageFormat.format("Created reservation {0} synchronously.",
          reservationId));
    }
  }

  private String checkReservationACLs(String queueName, String auditConstant,
                                      ReservationId reservationId)
      throws YarnException, IOException {
    UserGroupInformation callerUGI;
    try {
      callerUGI = UserGroupInformation.getCurrentUser();
    } catch (IOException ie) {
      RMAuditLogger.logFailure("UNKNOWN", auditConstant, queueName,
          "ClientRMService", "Error getting UGI");
      throw RPCUtil.getRemoteException(ie);
    }

    if (reservationSystem == null) {
      return callerUGI.getShortUserName();
    }

    ReservationsACLsManager manager = reservationSystem
            .getReservationsACLsManager();
    ReservationACL reservationACL = getReservationACLFromAuditConstant(
            auditConstant);

    if (manager == null) {
      return callerUGI.getShortUserName();
    }

    String reservationCreatorName = "";
    ReservationAllocation reservation;
    // Get the user associated with the reservation.
    Plan plan = reservationSystem.getPlan(queueName);
    if (reservationId != null && plan != null) {
      reservation = plan.getReservationById(reservationId);
      if (reservation != null) {
        reservationCreatorName = reservation.getUser();
      }
    }

    // If the reservation to be altered or listed belongs to the current user,
    // access will be given.
    if (reservationCreatorName != null && !reservationCreatorName.isEmpty()
           && reservationCreatorName.equals(callerUGI.getUserName())) {
      return callerUGI.getShortUserName();
    }

    // Check if the user has access to the specific ACL
    if (manager.checkAccess(callerUGI, reservationACL, queueName)) {
      return callerUGI.getShortUserName();
    }

    // If the user has Administer ACL then access is granted
    if (manager.checkAccess(callerUGI, ReservationACL
            .ADMINISTER_RESERVATIONS, queueName)) {
      return callerUGI.getShortUserName();
    }

    handleNoAccess(callerUGI.getShortUserName(), queueName, auditConstant,
            reservationACL.toString(), reservationACL.name());
    throw new IllegalStateException();
  }

  private ReservationACL getReservationACLFromAuditConstant(
          String auditConstant) throws YarnException{
    if (auditConstant.equals(AuditConstants.SUBMIT_RESERVATION_REQUEST)) {
      return ReservationACL.SUBMIT_RESERVATIONS;
    } else if (auditConstant.equals(AuditConstants.LIST_RESERVATION_REQUEST)) {
      return ReservationACL.LIST_RESERVATIONS;
    } else if (auditConstant.equals(AuditConstants.DELETE_RESERVATION_REQUEST)
          || auditConstant.equals(AuditConstants.UPDATE_RESERVATION_REQUEST)) {
      return ReservationACL.ADMINISTER_RESERVATIONS;
    } else {
      String error = "Audit Constant " + auditConstant + " is not recognized.";
      LOG.error(error);
      throw RPCUtil.getRemoteException(new UnrecognizedOptionException(error));
    }
  }

  private void handleNoAccess(String name, String queue, String auditConstant,
          String acl, String op) throws YarnException {
    RMAuditLogger.logFailure(
            name,
            auditConstant,
            "User doesn't have permissions to " + acl, "ClientRMService",
            auditConstant);
    throw RPCUtil.getRemoteException(new AccessControlException("User "
            + name + " cannot perform operation " + op + " on queue " + queue));
  }

  @Override
  public UpdateApplicationPriorityResponse updateApplicationPriority(
      UpdateApplicationPriorityRequest request) throws YarnException,
      IOException {

    ApplicationId applicationId = request.getApplicationId();
    Priority newAppPriority = request.getApplicationPriority();

    UserGroupInformation callerUGI =
        getCallerUgi(applicationId, AuditConstants.UPDATE_APP_PRIORITY);
    RMApp application = verifyUserAccessForRMApp(applicationId, callerUGI,
        AuditConstants.UPDATE_APP_PRIORITY, ApplicationAccessType.MODIFY_APP,
        true);

    UpdateApplicationPriorityResponse response = recordFactory
        .newRecordInstance(UpdateApplicationPriorityResponse.class);
    // Update priority only when app is tracked by the scheduler
    if (!ACTIVE_APP_STATES.contains(application.getState())) {
      if (application.isAppInCompletedStates()) {
        // If Application is in any of the final states, change priority
        // can be skipped rather throwing exception.
        RMAuditLogger.logSuccess(callerUGI.getShortUserName(),
            AuditConstants.UPDATE_APP_PRIORITY, "ClientRMService",
            applicationId);
        response.setApplicationPriority(application
            .getApplicationPriority());
        return response;
      }
      String msg = "Application in " + application.getState()
          + " state cannot update priority.";
      RMAuditLogger
          .logFailure(callerUGI.getShortUserName(),
              AuditConstants.UPDATE_APP_PRIORITY, "UNKNOWN", "ClientRMService",
              msg);
      throw new YarnException(msg);
    }

    try {
      rmAppManager.updateApplicationPriority(callerUGI,
          application.getApplicationId(),
          newAppPriority);
    } catch (YarnException ex) {
      RMAuditLogger.logFailure(callerUGI.getShortUserName(),
          AuditConstants.UPDATE_APP_PRIORITY, "UNKNOWN", "ClientRMService",
          ex.getMessage());
      throw ex;
    }

    RMAuditLogger.logSuccess(callerUGI.getShortUserName(),
        AuditConstants.UPDATE_APP_PRIORITY, "ClientRMService", applicationId);
    response.setApplicationPriority(application.getApplicationPriority());
    return response;
  }

  /**
   * Send a signal to a container.
   *
   * After the request passes some sanity check, it will be delivered
   * to RMNodeImpl so that the next NM heartbeat will pick up the signal request
   * @param request request to signal a container
   * @return the response of sending signal request
   * @throws YarnException rpc related exception
   * @throws IOException fail to obtain user group information
   */
  @SuppressWarnings("unchecked")
  @Override
  public SignalContainerResponse signalToContainer(
      SignalContainerRequest request) throws YarnException, IOException {
    ContainerId containerId = request.getContainerId();

    UserGroupInformation callerUGI;
    try {
      callerUGI = UserGroupInformation.getCurrentUser();
    } catch (IOException ie) {
      LOG.info("Error getting UGI ", ie);
      throw RPCUtil.getRemoteException(ie);
    }

    ApplicationId applicationId = containerId.getApplicationAttemptId().
        getApplicationId();
    RMApp application = this.rmContext.getRMApps().get(applicationId);
    if (application == null) {
      RMAuditLogger.logFailure(callerUGI.getUserName(),
          AuditConstants.SIGNAL_CONTAINER, "UNKNOWN", "ClientRMService",
          "Trying to signal an absent container", applicationId, containerId, null);
      throw RPCUtil
          .getRemoteException("Trying to signal an absent container "
              + containerId);
    }

    if (!checkAccess(callerUGI, application.getUser(),
        ApplicationAccessType.MODIFY_APP, application)) {
      RMAuditLogger.logFailure(callerUGI.getShortUserName(),
          AuditConstants.SIGNAL_CONTAINER, "User doesn't have permissions to "
              + ApplicationAccessType.MODIFY_APP.toString(), "ClientRMService",
          AuditConstants.UNAUTHORIZED_USER, applicationId);
      throw RPCUtil.getRemoteException(new AccessControlException("User "
          + callerUGI.getShortUserName() + " cannot perform operation "
          + ApplicationAccessType.MODIFY_APP.name() + " on " + applicationId));
    }

    RMContainer container = scheduler.getRMContainer(containerId);
    if (container != null) {
      this.rmContext.getDispatcher().getEventHandler().handle(
          new RMNodeSignalContainerEvent(container.getContainer().getNodeId(),
              request));
      RMAuditLogger.logSuccess(callerUGI.getShortUserName(),
          AuditConstants.SIGNAL_CONTAINER, "ClientRMService", applicationId,
          containerId, null);
    } else {
      RMAuditLogger.logFailure(callerUGI.getUserName(),
          AuditConstants.SIGNAL_CONTAINER, "UNKNOWN", "ClientRMService",
          "Trying to signal an absent container", applicationId, containerId, null);
      throw RPCUtil
          .getRemoteException("Trying to signal an absent container "
              + containerId);
    }

    return recordFactory
        .newRecordInstance(SignalContainerResponse.class);
  }

  @Override
  public UpdateApplicationTimeoutsResponse updateApplicationTimeouts(
      UpdateApplicationTimeoutsRequest request)
      throws YarnException, IOException {

    ApplicationId applicationId = request.getApplicationId();
    Map<ApplicationTimeoutType, String> applicationTimeouts =
        request.getApplicationTimeouts();

    UserGroupInformation callerUGI =
        getCallerUgi(applicationId, AuditConstants.UPDATE_APP_TIMEOUTS);
    RMApp application = verifyUserAccessForRMApp(applicationId, callerUGI,
        AuditConstants.UPDATE_APP_TIMEOUTS, ApplicationAccessType.MODIFY_APP,
        true);

    if (applicationTimeouts.isEmpty()) {
      String message =
          "At least one ApplicationTimeoutType should be configured"
              + " for updating timeouts.";
      RMAuditLogger.logFailure(callerUGI.getShortUserName(),
          AuditConstants.UPDATE_APP_TIMEOUTS, "UNKNOWN", "ClientRMService",
          message, applicationId);
      throw RPCUtil.getRemoteException(message);
    }

    UpdateApplicationTimeoutsResponse response = recordFactory
        .newRecordInstance(UpdateApplicationTimeoutsResponse.class);

    RMAppState state = application.getState();
    if (!EnumSet
        .of(RMAppState.SUBMITTED, RMAppState.ACCEPTED, RMAppState.RUNNING)
        .contains(state)) {
      if (application.isAppInCompletedStates()) {
        // If Application is in any of the final states, update timeout
        // can be skipped rather throwing exception.
        RMAuditLogger.logSuccess(callerUGI.getShortUserName(),
            AuditConstants.UPDATE_APP_TIMEOUTS, "ClientRMService",
            applicationId);
        response.setApplicationTimeouts(applicationTimeouts);
        return response;
      }
      String msg =
          "Application is in " + state + " state can not update timeout.";
      RMAuditLogger.logFailure(callerUGI.getShortUserName(),
          AuditConstants.UPDATE_APP_TIMEOUTS, "UNKNOWN", "ClientRMService",
          msg);
      throw RPCUtil.getRemoteException(msg);
    }

    try {
      applicationTimeouts = rmAppManager.updateApplicationTimeout(application,
          applicationTimeouts);
    } catch (YarnException ex) {
      RMAuditLogger.logFailure(callerUGI.getShortUserName(),
          AuditConstants.UPDATE_APP_TIMEOUTS, "UNKNOWN", "ClientRMService",
          ex.getMessage());
      throw ex;
    }

    RMAuditLogger.logSuccess(callerUGI.getShortUserName(),
        AuditConstants.UPDATE_APP_TIMEOUTS, "ClientRMService", applicationId);
    response.setApplicationTimeouts(applicationTimeouts);
    return response;
  }

  private UserGroupInformation getCallerUgi(ApplicationId applicationId,
      String operation) throws YarnException {
    UserGroupInformation callerUGI;
    try {
      callerUGI = UserGroupInformation.getCurrentUser();
    } catch (IOException ie) {
      LOG.info("Error getting UGI ", ie);
      RMAuditLogger.logFailure("UNKNOWN", operation, "UNKNOWN",
          "ClientRMService", "Error getting UGI", applicationId);
      throw RPCUtil.getRemoteException(ie);
    }
    return callerUGI;
  }

  private RMApp verifyUserAccessForRMApp(ApplicationId applicationId,
      UserGroupInformation callerUGI, String operation,
      ApplicationAccessType accessType,
      boolean needCheckAccess) throws YarnException {
    RMApp application = this.rmContext.getRMApps().get(applicationId);
    if (application == null) {
      RMAuditLogger.logFailure(callerUGI.getUserName(), operation, "UNKNOWN",
          "ClientRMService",
          "Trying to " + operation + " of an absent application",
          applicationId);
        // If the RM doesn't have the application, throw
        // ApplicationNotFoundException and let client to handle.
      throw new ApplicationNotFoundException("Application with id '"
              + applicationId + "' doesn't exist in RM. "
              + "Please check that the job "
              + "submission was successful.");
    }

    if (needCheckAccess) {
      if (!checkAccess(callerUGI, application.getUser(),
              accessType, application)) {
        RMAuditLogger.logFailure(callerUGI.getShortUserName(), operation,
                "User doesn't have permissions to "
                        + accessType.toString(),
                "ClientRMService", AuditConstants.UNAUTHORIZED_USER,
                applicationId);
        throw RPCUtil.getRemoteException(new AccessControlException("User "
                + callerUGI.getShortUserName() + " cannot perform operation "
                + accessType.name() + " on " + applicationId));
      }
    }
    return application;
  }

  @VisibleForTesting
  public void setDisplayPerUserApps(boolean displayPerUserApps) {
    this.displayPerUserApps = displayPerUserApps;
  }

  @Override
  public GetAllResourceTypeInfoResponse getResourceTypeInfo(
      GetAllResourceTypeInfoRequest request) throws YarnException, IOException {
    GetAllResourceTypeInfoResponse response =
        GetAllResourceTypeInfoResponse.newInstance();
    response.setResourceTypeInfo(ResourceUtils.getResourcesTypeInfo());
    return response;
  }
}
