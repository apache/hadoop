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

package org.apache.hadoop.yarn.server.resourcemanager.amlauncher;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.security.PrivilegedAction;
import java.util.Map;

import javax.crypto.SecretKey;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DataInputByteBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.ContainerManager;
import org.apache.hadoop.yarn.api.protocolrecords.StartContainerRequest;
import org.apache.hadoop.yarn.api.protocolrecords.StopContainerRequest;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.yarn.security.ApplicationTokenIdentifier;
import org.apache.hadoop.yarn.security.ContainerTokenIdentifier;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptEventType;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.event.RMAppAttemptLaunchFailedEvent;
import org.apache.hadoop.yarn.util.ProtoUtils;

/**
 * The launch of the AM itself.
 */
public class AMLauncher implements Runnable {

  private static final Log LOG = LogFactory.getLog(AMLauncher.class);

  private ContainerManager containerMgrProxy;

  private final RMAppAttempt application;
  private final Configuration conf;
  private final RecordFactory recordFactory = 
      RecordFactoryProvider.getRecordFactory(null);
  private final AMLauncherEventType eventType;
  private final RMContext rmContext;
  
  @SuppressWarnings("rawtypes")
  private final EventHandler handler;
  
  public AMLauncher(RMContext rmContext, RMAppAttempt application,
      AMLauncherEventType eventType, Configuration conf) {
    this.application = application;
    this.conf = conf;
    this.eventType = eventType;
    this.rmContext = rmContext;
    this.handler = rmContext.getDispatcher().getEventHandler();
  }
  
  private void connect() throws IOException {
    ContainerId masterContainerID = application.getMasterContainer().getId();
    
    containerMgrProxy = getContainerMgrProxy(masterContainerID);
  }
  
  private void launch() throws IOException {
    connect();
    ContainerId masterContainerID = application.getMasterContainer().getId();
    ApplicationSubmissionContext applicationContext =
      application.getSubmissionContext();
    LOG.info("Setting up container " + application.getMasterContainer() 
        + " for AM " + application.getAppAttemptId());  
    ContainerLaunchContext launchContext =
        createAMContainerLaunchContext(applicationContext, masterContainerID);
    StartContainerRequest request = 
        recordFactory.newRecordInstance(StartContainerRequest.class);
    request.setContainerLaunchContext(launchContext);
    containerMgrProxy.startContainer(request);
    LOG.info("Done launching container " + application.getMasterContainer() 
        + " for AM " + application.getAppAttemptId());
  }
  
  private void cleanup() throws IOException {
    connect();
    ContainerId containerId = application.getMasterContainer().getId();
    StopContainerRequest stopRequest = 
        recordFactory.newRecordInstance(StopContainerRequest.class);
    stopRequest.setContainerId(containerId);
    containerMgrProxy.stopContainer(stopRequest);
  }

  // Protected. For tests.
  protected ContainerManager getContainerMgrProxy(
      final ContainerId containerId) {

    Container container = application.getMasterContainer();

    final NodeId node = container.getNodeId();
    final InetSocketAddress containerManagerBindAddress =
        NetUtils.createSocketAddrForHost(node.getHost(), node.getPort());

    final YarnRPC rpc = YarnRPC.create(conf); // TODO: Don't create again and again.

    UserGroupInformation currentUser = UserGroupInformation
        .createRemoteUser(containerId.toString());
    if (UserGroupInformation.isSecurityEnabled()) {
      Token<ContainerTokenIdentifier> token =
          ProtoUtils.convertFromProtoFormat(container.getContainerToken(),
                                            containerManagerBindAddress);
      currentUser.addToken(token);
    }
    return currentUser.doAs(new PrivilegedAction<ContainerManager>() {
      @Override
      public ContainerManager run() {
        return (ContainerManager) rpc.getProxy(ContainerManager.class,
            containerManagerBindAddress, conf);
      }
    });
  }

  private ContainerLaunchContext createAMContainerLaunchContext(
      ApplicationSubmissionContext applicationMasterContext,
      ContainerId containerID) throws IOException {

    // Construct the actual Container
    ContainerLaunchContext container = 
        applicationMasterContext.getAMContainerSpec();
    LOG.info("Command to launch container "
        + containerID
        + " : "
        + StringUtils.arrayToString(container.getCommands().toArray(
            new String[0])));
    
    // Finalize the container
    container.setContainerId(containerID);
    container.setUser(applicationMasterContext.getUser());
    setupTokensAndEnv(container);
    
    return container;
  }

  private void setupTokensAndEnv(
      ContainerLaunchContext container)
      throws IOException {
    Map<String, String> environment = container.getEnvironment();

    environment.put(ApplicationConstants.APPLICATION_WEB_PROXY_BASE_ENV,
        application.getWebProxyBase());
    // Set the AppAttemptId, containerId, NMHTTPAdress, AppSubmitTime to be
    // consumable by the AM.
    environment.put(ApplicationConstants.AM_CONTAINER_ID_ENV, container
        .getContainerId().toString());
    environment.put(ApplicationConstants.NM_HOST_ENV, application
        .getMasterContainer().getNodeId().getHost());
    environment.put(ApplicationConstants.NM_PORT_ENV,
        String.valueOf(application.getMasterContainer().getNodeId().getPort()));
    String parts[] =
        application.getMasterContainer().getNodeHttpAddress().split(":");
    environment.put(ApplicationConstants.NM_HTTP_PORT_ENV, parts[1]);
    ApplicationId applicationId =
        application.getAppAttemptId().getApplicationId();
    environment.put(
        ApplicationConstants.APP_SUBMIT_TIME_ENV,
        String.valueOf(rmContext.getRMApps()
            .get(applicationId)
            .getSubmitTime()));
 
    if (UserGroupInformation.isSecurityEnabled()) {
      // TODO: Security enabled/disabled info should come from RM.

      Credentials credentials = new Credentials();

      DataInputByteBuffer dibb = new DataInputByteBuffer();
      if (container.getContainerTokens() != null) {
        // TODO: Don't do this kind of checks everywhere.
        dibb.reset(container.getContainerTokens());
        credentials.readTokenStorageStream(dibb);
      }

      ApplicationTokenIdentifier id = new ApplicationTokenIdentifier(
          application.getAppAttemptId());
      Token<ApplicationTokenIdentifier> token =
          new Token<ApplicationTokenIdentifier>(id,
              this.rmContext.getApplicationTokenSecretManager());
      InetSocketAddress serviceAddr = conf.getSocketAddr(
          YarnConfiguration.RM_SCHEDULER_ADDRESS,
          YarnConfiguration.DEFAULT_RM_SCHEDULER_ADDRESS,
          YarnConfiguration.DEFAULT_RM_SCHEDULER_PORT);
      // normally the client should set the service after acquiring the token,
      // but this token is directly provided to the tasks
      SecurityUtil.setTokenService(token, serviceAddr);
      String appMasterTokenEncoded = token.encodeToUrlString();
      LOG.debug("Putting appMaster token in env : " + token);
      environment.put(
          ApplicationConstants.APPLICATION_MASTER_TOKEN_ENV_NAME,
          appMasterTokenEncoded);

      // Add the RM token
      credentials.addToken(token.getService(), token);
      DataOutputBuffer dob = new DataOutputBuffer();
      credentials.writeTokenStorageToStream(dob);
      container.setContainerTokens(
          ByteBuffer.wrap(dob.getData(), 0, dob.getLength()));

      SecretKey clientSecretKey =
          this.rmContext.getClientToAMTokenSecretManager().getMasterKey(
            application.getAppAttemptId());
      String encoded =
          Base64.encodeBase64URLSafeString(clientSecretKey.getEncoded());
      environment.put(
          ApplicationConstants.APPLICATION_CLIENT_SECRET_ENV_NAME, 
          encoded);
    }
  }
  
  @SuppressWarnings("unchecked")
  public void run() {
    switch (eventType) {
    case LAUNCH:
      try {
        LOG.info("Launching master" + application.getAppAttemptId());
        launch();
        handler.handle(new RMAppAttemptEvent(application.getAppAttemptId(),
            RMAppAttemptEventType.LAUNCHED));
      } catch(Exception ie) {
        String message = "Error launching " + application.getAppAttemptId()
            + ". Got exception: " + StringUtils.stringifyException(ie);
        LOG.info(message);
        handler.handle(new RMAppAttemptLaunchFailedEvent(application
            .getAppAttemptId(), message));
      }
      break;
    case CLEANUP:
      try {
        LOG.info("Cleaning master " + application.getAppAttemptId());
        cleanup();
      } catch(IOException ie) {
        LOG.info("Error cleaning master ", ie);
      }
      break;
    default:
      LOG.warn("Received unknown event-type " + eventType + ". Ignoring.");
      break;
    }
  }
}
