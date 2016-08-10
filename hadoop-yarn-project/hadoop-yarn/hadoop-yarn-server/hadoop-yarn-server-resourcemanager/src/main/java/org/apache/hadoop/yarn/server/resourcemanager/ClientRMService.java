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
import java.security.AccessControlException;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.lang.math.LongRange;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.io.Text;
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
import org.apache.hadoop.yarn.api.protocolrecords.ReservationSubmissionRequest;
import org.apache.hadoop.yarn.api.protocolrecords.ReservationSubmissionResponse;
import org.apache.hadoop.yarn.api.protocolrecords.ReservationUpdateRequest;
import org.apache.hadoop.yarn.api.protocolrecords.ReservationUpdateResponse;
import org.apache.hadoop.yarn.api.protocolrecords.SubmitApplicationRequest;
import org.apache.hadoop.yarn.api.protocolrecords.SubmitApplicationResponse;
import org.apache.hadoop.yarn.api.records.ApplicationAccessType;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptReport;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerReport;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.NodeState;
import org.apache.hadoop.yarn.api.records.QueueACL;
import org.apache.hadoop.yarn.api.records.QueueInfo;
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
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.Plan;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.ReservationInputValidator;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.ReservationSystem;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.exceptions.PlanningException;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppKillByClientEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppMoveEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppState;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerAppReport;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerNodeReport;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.YarnScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.security.QueueACLsManager;
import org.apache.hadoop.yarn.server.resourcemanager.security.RMDelegationTokenSecretManager;
import org.apache.hadoop.yarn.server.resourcemanager.security.authorize.RMPolicyProvider;
import org.apache.hadoop.yarn.server.security.ApplicationACLsManager;
import org.apache.hadoop.yarn.server.utils.BuilderUtils;
import org.apache.hadoop.yarn.util.Clock;
import org.apache.hadoop.yarn.util.Records;
import org.apache.hadoop.yarn.util.UTCClock;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.SettableFuture;

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
  InetSocketAddress clientBindAddress;

  private final ApplicationACLsManager applicationsACLsManager;
  private final QueueACLsManager queueACLsManager;

  // For Reservation APIs
  private Clock clock;
  private ReservationSystem reservationSystem;
  private ReservationInputValidator rValidator;

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
   * @param callerUGI
   * @param owner
   * @param operationPerformed
   * @param application
   * @return
   */
  private boolean checkAccess(UserGroupInformation callerUGI, String owner,
      ApplicationAccessType operationPerformed,
      RMApp application) {
    return applicationsACLsManager.checkAccess(callerUGI, operationPerformed,
        owner, application.getApplicationId())
        || queueACLsManager.checkAccess(callerUGI, QueueACL.ADMINISTER_QUEUE,
            application.getQueue());
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
          + applicationId + "' doesn't exist in RM.");
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
    ApplicationAttemptId appAttemptId = request.getApplicationAttemptId();
    UserGroupInformation callerUGI;
    try {
      callerUGI = UserGroupInformation.getCurrentUser();
    } catch (IOException ie) {
      LOG.info("Error getting UGI ", ie);
      throw RPCUtil.getRemoteException(ie);
    }
    RMApp application = this.rmContext.getRMApps().get(
        appAttemptId.getApplicationId());
    if (application == null) {
      // If the RM doesn't have the application, throw
      // ApplicationNotFoundException and let client to handle.
      throw new ApplicationNotFoundException("Application with id '"
          + request.getApplicationAttemptId().getApplicationId()
          + "' doesn't exist in RM.");
    }

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
          + " does not have privilage to see this attempt " + appAttemptId);
    }
    return response;
  }
  
  @Override
  public GetApplicationAttemptsResponse getApplicationAttempts(
      GetApplicationAttemptsRequest request) throws YarnException, IOException {
    ApplicationId appId = request.getApplicationId();
    UserGroupInformation callerUGI;
    try {
      callerUGI = UserGroupInformation.getCurrentUser();
    } catch (IOException ie) {
      LOG.info("Error getting UGI ", ie);
      throw RPCUtil.getRemoteException(ie);
    }
    RMApp application = this.rmContext.getRMApps().get(appId);
    if (application == null) {
      // If the RM doesn't have the application, throw
      // ApplicationNotFoundException and let client to handle.
      throw new ApplicationNotFoundException("Application with id '" + appId
          + "' doesn't exist in RM.");
    }
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
          + " does not have privilage to see this aplication " + appId);
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
    UserGroupInformation callerUGI;
    try {
      callerUGI = UserGroupInformation.getCurrentUser();
    } catch (IOException ie) {
      LOG.info("Error getting UGI ", ie);
      throw RPCUtil.getRemoteException(ie);
    }
    RMApp application = this.rmContext.getRMApps().get(appId);
    if (application == null) {
      // If the RM doesn't have the application, throw
      // ApplicationNotFoundException and let client to handle.
      throw new ApplicationNotFoundException("Application with id '" + appId
          + "' doesn't exist in RM.");
    }
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
      RMContainer rmConatiner = this.rmContext.getScheduler().getRMContainer(
          containerId);
      if (rmConatiner == null) {
        throw new ContainerNotFoundException("Container with id '" + containerId
            + "' doesn't exist in RM.");
      }
      response = GetContainerReportResponse.newInstance(rmConatiner
          .createContainerReport());
    } else {
      throw new YarnException("User " + callerUGI.getShortUserName()
          + " does not have privilage to see this aplication " + appId);
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
    UserGroupInformation callerUGI;
    try {
      callerUGI = UserGroupInformation.getCurrentUser();
    } catch (IOException ie) {
      LOG.info("Error getting UGI ", ie);
      throw RPCUtil.getRemoteException(ie);
    }
    RMApp application = this.rmContext.getRMApps().get(appId);
    if (application == null) {
      // If the RM doesn't have the application, throw
      // ApplicationNotFoundException and let client to handle.
      throw new ApplicationNotFoundException("Application with id '" + appId
          + "' doesn't exist in RM.");
    }
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
          + " does not have privilage to see this aplication " + appId);
    }
    return response;
  }

  @Override
  public SubmitApplicationResponse submitApplication(
      SubmitApplicationRequest request) throws YarnException {
    ApplicationSubmissionContext submissionContext = request
        .getApplicationSubmissionContext();
    ApplicationId applicationId = submissionContext.getApplicationId();

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
          "Exception in submitting application", applicationId);
      throw RPCUtil.getRemoteException(ie);
    }

    // Check whether app has already been put into rmContext,
    // If it is, simply return the response
    if (rmContext.getRMApps().get(applicationId) != null) {
      LOG.info("This is an earlier submitted application: " + applicationId);
      return SubmitApplicationResponse.newInstance();
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

    try {
      // call RMAppManager to submit application directly
      rmAppManager.submitApplication(submissionContext,
          System.currentTimeMillis(), user);

      LOG.info("Application with id " + applicationId.getId() + 
          " submitted by user " + user);
      RMAuditLogger.logSuccess(user, AuditConstants.SUBMIT_APP_REQUEST,
          "ClientRMService", applicationId);
    } catch (YarnException e) {
      LOG.info("Exception in submitting application with id " +
          applicationId.getId(), e);
      RMAuditLogger.logFailure(user, AuditConstants.SUBMIT_APP_REQUEST,
          e.getMessage(), "ClientRMService",
          "Exception in submitting application", applicationId);
      throw e;
    }

    SubmitApplicationResponse response = recordFactory
        .newRecordInstance(SubmitApplicationResponse.class);
    return response;
  }

  @SuppressWarnings("unchecked")
  @Override
  public KillApplicationResponse forceKillApplication(
      KillApplicationRequest request) throws YarnException {

    ApplicationId applicationId = request.getApplicationId();

    UserGroupInformation callerUGI;
    try {
      callerUGI = UserGroupInformation.getCurrentUser();
    } catch (IOException ie) {
      LOG.info("Error getting UGI ", ie);
      RMAuditLogger.logFailure("UNKNOWN", AuditConstants.KILL_APP_REQUEST,
          "UNKNOWN", "ClientRMService" , "Error getting UGI",
          applicationId);
      throw RPCUtil.getRemoteException(ie);
    }

    RMApp application = this.rmContext.getRMApps().get(applicationId);
    if (application == null) {
      RMAuditLogger.logFailure(callerUGI.getUserName(),
          AuditConstants.KILL_APP_REQUEST, "UNKNOWN", "ClientRMService",
          "Trying to kill an absent application", applicationId);
      throw new ApplicationNotFoundException("Trying to kill an absent"
          + " application " + applicationId);
    }

    if (!checkAccess(callerUGI, application.getUser(),
        ApplicationAccessType.MODIFY_APP, application)) {
      RMAuditLogger.logFailure(callerUGI.getShortUserName(),
          AuditConstants.KILL_APP_REQUEST,
          "User doesn't have permissions to "
              + ApplicationAccessType.MODIFY_APP.toString(), "ClientRMService",
          AuditConstants.UNAUTHORIZED_USER, applicationId);
      throw RPCUtil.getRemoteException(new AccessControlException("User "
          + callerUGI.getShortUserName() + " cannot perform operation "
          + ApplicationAccessType.MODIFY_APP.name() + " on " + applicationId));
    }

    if (application.isAppFinalStateStored()) {
      return KillApplicationResponse.newInstance(true);
    }

    String message = "Kill application " + applicationId + " received from "
        + callerUGI;
    InetAddress remoteAddress = Server.getRemoteIp();
    if (null != remoteAddress) {
      message += " at " + remoteAddress.getHostAddress();
    }
    this.rmContext.getDispatcher().getEventHandler()
        .handle(new RMAppKillByClientEvent(applicationId, message, callerUGI,
            remoteAddress));

    // For UnmanagedAMs, return true so they don't retry
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
    response.setClusterMetrics(ymetrics);
    return response;
  }
  
  @Override
  public GetApplicationsResponse getApplications(
      GetApplicationsRequest request) throws YarnException {
    return getApplications(request, true);
  }

  /**
   * Get applications matching the {@link GetApplicationsRequest}. If
   * caseSensitive is set to false, applicationTypes in
   * GetApplicationRequest are expected to be in all-lowercase
   */
  @Private
  public GetApplicationsResponse getApplications(
      GetApplicationsRequest request, boolean caseSensitive)
      throws YarnException {
    UserGroupInformation callerUGI;
    try {
      callerUGI = UserGroupInformation.getCurrentUser();
    } catch (IOException ie) {
      LOG.info("Error getting UGI ", ie);
      throw RPCUtil.getRemoteException(ie);
    }

    Set<String> applicationTypes = request.getApplicationTypes();
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
        String appTypeToMatch = caseSensitive
            ? application.getApplicationType()
            : StringUtils.toLowerCase(application.getApplicationType());
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

      reports.add(application.createAndGetApplicationReport(
          callerUGI.getUserName(), allowAccess));
    }

    GetApplicationsResponse response =
      recordFactory.newRecordInstance(GetApplicationsResponse.class);
    response.setApplicationList(reports);
    return response;
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
    } catch (IOException ioe) {
      LOG.info("Failed to getQueueInfo for " + request.getQueueName(), ioe);
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
            rmNode.getNodeLabels());

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
      Token<RMDelegationTokenIdentifier> realRMDTtoken =
          new Token<RMDelegationTokenIdentifier>(tokenIdentifier,
              this.rmDTSecretManager);
      response.setRMDelegationToken(
          BuilderUtils.newDelegationToken(
              realRMDTtoken.getIdentifier(),
              realRMDTtoken.getKind().toString(),
              realRMDTtoken.getPassword(),
              realRMDTtoken.getService().toString()
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

    UserGroupInformation callerUGI;
    try {
      callerUGI = UserGroupInformation.getCurrentUser();
    } catch (IOException ie) {
      LOG.info("Error getting UGI ", ie);
      RMAuditLogger.logFailure("UNKNOWN", AuditConstants.MOVE_APP_REQUEST,
          "UNKNOWN", "ClientRMService" , "Error getting UGI",
          applicationId);
      throw RPCUtil.getRemoteException(ie);
    }

    RMApp application = this.rmContext.getRMApps().get(applicationId);
    if (application == null) {
      RMAuditLogger.logFailure(callerUGI.getUserName(),
          AuditConstants.MOVE_APP_REQUEST, "UNKNOWN", "ClientRMService",
          "Trying to move an absent application", applicationId);
      throw new ApplicationNotFoundException("Trying to move an absent"
          + " application " + applicationId);
    }

    if (!checkAccess(callerUGI, application.getUser(),
        ApplicationAccessType.MODIFY_APP, application)) {
      RMAuditLogger.logFailure(callerUGI.getShortUserName(),
          AuditConstants.MOVE_APP_REQUEST,
          "User doesn't have permissions to "
              + ApplicationAccessType.MODIFY_APP.toString(), "ClientRMService",
          AuditConstants.UNAUTHORIZED_USER, applicationId);
      throw RPCUtil.getRemoteException(new AccessControlException("User "
          + callerUGI.getShortUserName() + " cannot perform operation "
          + ApplicationAccessType.MODIFY_APP.name() + " on " + applicationId));
    }
    
    // Moves only allowed when app is in a state that means it is tracked by
    // the scheduler
    if (EnumSet.of(RMAppState.NEW, RMAppState.NEW_SAVING, RMAppState.FAILED,
        RMAppState.FINAL_SAVING, RMAppState.FINISHING, RMAppState.FINISHED,
        RMAppState.KILLED, RMAppState.KILLING, RMAppState.FAILED)
        .contains(application.getState())) {
      String msg = "App in " + application.getState() + " state cannot be moved.";
      RMAuditLogger.logFailure(callerUGI.getShortUserName(),
          AuditConstants.MOVE_APP_REQUEST, "UNKNOWN", "ClientRMService", msg);
      throw new YarnException(msg);
    }

    SettableFuture<Object> future = SettableFuture.create();
    this.rmContext.getDispatcher().getEventHandler().handle(
        new RMAppMoveEvent(applicationId, request.getTargetQueue(), future));
    
    try {
      Futures.get(future, YarnException.class);
    } catch (YarnException ex) {
      RMAuditLogger.logFailure(callerUGI.getShortUserName(),
          AuditConstants.MOVE_APP_REQUEST, "UNKNOWN", "ClientRMService",
          ex.getMessage());
      throw ex;
    }

    RMAuditLogger.logSuccess(callerUGI.getShortUserName(), 
        AuditConstants.MOVE_APP_REQUEST, "ClientRMService" , applicationId);
    MoveApplicationAcrossQueuesResponse response = recordFactory
        .newRecordInstance(MoveApplicationAcrossQueuesResponse.class);
    return response;
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
  public ReservationSubmissionResponse submitReservation(
      ReservationSubmissionRequest request) throws YarnException, IOException {
    // Check if reservation system is enabled
    checkReservationSytem(AuditConstants.SUBMIT_RESERVATION_REQUEST);
    ReservationSubmissionResponse response =
        recordFactory.newRecordInstance(ReservationSubmissionResponse.class);
    // Create a new Reservation Id
    ReservationId reservationId = reservationSystem.getNewReservationId();
    // Validate the input
    Plan plan =
        rValidator.validateReservationSubmissionRequest(reservationSystem,
            request, reservationId);
    // Check ACLs
    String queueName = request.getQueue();
    String user =
        checkReservationACLs(queueName,
            AuditConstants.SUBMIT_RESERVATION_REQUEST);
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
        response.setReservationId(reservationId);
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
    checkReservationSytem(AuditConstants.UPDATE_RESERVATION_REQUEST);
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
            AuditConstants.UPDATE_RESERVATION_REQUEST);
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
    checkReservationSytem(AuditConstants.DELETE_RESERVATION_REQUEST);
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
            AuditConstants.DELETE_RESERVATION_REQUEST);
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
  public GetNodesToLabelsResponse getNodeToLabels(
      GetNodesToLabelsRequest request) throws YarnException, IOException {
    RMNodeLabelsManager labelsMgr = rmContext.getNodeLabelManager();
    GetNodesToLabelsResponse response =
        GetNodesToLabelsResponse.newInstance(labelsMgr.getNodeLabels());
    return response;
  }

  @Override
  public GetLabelsToNodesResponse getLabelsToNodes(
      GetLabelsToNodesRequest request) throws YarnException, IOException {
    RMNodeLabelsManager labelsMgr = rmContext.getNodeLabelManager();
    if (request.getNodeLabels() == null || request.getNodeLabels().isEmpty()) {
      return GetLabelsToNodesResponse.newInstance(
          labelsMgr.getLabelsToNodes());
    } else {
      return GetLabelsToNodesResponse.newInstance(
          labelsMgr.getLabelsToNodes(request.getNodeLabels()));
    }
  }

  @Override
  public GetClusterNodeLabelsResponse getClusterNodeLabels(
      GetClusterNodeLabelsRequest request) throws YarnException, IOException {
    RMNodeLabelsManager labelsMgr = rmContext.getNodeLabelManager();
    GetClusterNodeLabelsResponse response =
        GetClusterNodeLabelsResponse.newInstance(
            labelsMgr.getClusterNodeLabels());
    return response;
  }

  private void checkReservationSytem(String auditConstant) throws YarnException {
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
      reservationSystem.synchronizePlan(planName);
      LOG.info(MessageFormat.format("Created reservation {0} synchronously.",
          reservationId));
    }
  }

  private String checkReservationACLs(String queueName, String auditConstant)
      throws YarnException {
    UserGroupInformation callerUGI;
    try {
      callerUGI = UserGroupInformation.getCurrentUser();
    } catch (IOException ie) {
      RMAuditLogger.logFailure("UNKNOWN", auditConstant, queueName,
          "ClientRMService", "Error getting UGI");
      throw RPCUtil.getRemoteException(ie);
    }
    // Check if user has access on the managed queue
    if (!queueACLsManager.checkAccess(callerUGI, QueueACL.SUBMIT_APPLICATIONS,
        queueName)) {
      RMAuditLogger.logFailure(
          callerUGI.getShortUserName(),
          auditConstant,
          "User doesn't have permissions to "
              + QueueACL.SUBMIT_APPLICATIONS.toString(), "ClientRMService",
          AuditConstants.UNAUTHORIZED_USER);
      throw RPCUtil.getRemoteException(new AccessControlException("User "
          + callerUGI.getShortUserName() + " cannot perform operation "
          + QueueACL.SUBMIT_APPLICATIONS.name() + " on queue" + queueName));
    }
    return callerUGI.getShortUserName();
  }
}
