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
import org.apache.hadoop.io.Text;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.Credentials;
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
import org.apache.hadoop.yarn.api.records.ContainerToken;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.yarn.security.ApplicationTokenIdentifier;
import org.apache.hadoop.yarn.security.ApplicationTokenSecretManager;
import org.apache.hadoop.yarn.security.ContainerTokenIdentifier;
import org.apache.hadoop.yarn.security.client.ClientToAMSecretManager;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptEventType;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.event.RMAppAttemptLaunchFailedEvent;

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
  private final ApplicationTokenSecretManager applicationTokenSecretManager;
  private final ClientToAMSecretManager clientToAMSecretManager;
  private final AMLauncherEventType eventType;
  private final RMContext rmContext;
  
  @SuppressWarnings("rawtypes")
  private final EventHandler handler;
  
  public AMLauncher(RMContext rmContext, RMAppAttempt application,
      AMLauncherEventType eventType,
      ApplicationTokenSecretManager applicationTokenSecretManager,
      ClientToAMSecretManager clientToAMSecretManager, Configuration conf) {
    this.application = application;
    this.conf = conf;
    this.applicationTokenSecretManager = applicationTokenSecretManager;
    this.clientToAMSecretManager = clientToAMSecretManager;
    this.eventType = eventType;
    this.rmContext = rmContext;
    this.handler = rmContext.getDispatcher().getEventHandler();
  }
  
  private void connect() throws IOException {
    ContainerId masterContainerID = application.getMasterContainer().getId();
    
    containerMgrProxy =
        getContainerMgrProxy(
            masterContainerID.getApplicationAttemptId().getApplicationId());
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

  protected ContainerManager getContainerMgrProxy(
      final ApplicationId applicationID) throws IOException {

    Container container = application.getMasterContainer();

    final String containerManagerBindAddress = container.getNodeId().toString();

    final YarnRPC rpc = YarnRPC.create(conf); // TODO: Don't create again and again.

    UserGroupInformation currentUser =
        UserGroupInformation.createRemoteUser("yarn"); // TODO
    if (UserGroupInformation.isSecurityEnabled()) {
      ContainerToken containerToken = container.getContainerToken();
      Token<ContainerTokenIdentifier> token =
          new Token<ContainerTokenIdentifier>(
              containerToken.getIdentifier().array(),
              containerToken.getPassword().array(), new Text(
                  containerToken.getKind()), new Text(
                  containerToken.getService()));
      currentUser.addToken(token);
    }
    return currentUser.doAs(new PrivilegedAction<ContainerManager>() {
      @Override
      public ContainerManager run() {
        return (ContainerManager) rpc.getProxy(ContainerManager.class,
            NetUtils.createSocketAddr(containerManagerBindAddress), conf);
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

    // Set the AppAttemptId, containerId, NMHTTPAdress, AppSubmitTime to be
    // consumable by the AM.
    environment.put(ApplicationConstants.AM_CONTAINER_ID_ENV, container
        .getContainerId().toString());
    environment.put(ApplicationConstants.NM_HTTP_ADDRESS_ENV, application
        .getMasterContainer().getNodeHttpAddress());
    environment.put(
        ApplicationConstants.APP_SUBMIT_TIME_ENV,
        String.valueOf(rmContext.getRMApps()
            .get(application.getAppAttemptId().getApplicationId())
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
          application.getAppAttemptId().getApplicationId());
      Token<ApplicationTokenIdentifier> token =
          new Token<ApplicationTokenIdentifier>(id,
              this.applicationTokenSecretManager);
      String schedulerAddressStr =
          this.conf.get(YarnConfiguration.RM_SCHEDULER_ADDRESS,
              YarnConfiguration.DEFAULT_RM_SCHEDULER_ADDRESS);
      InetSocketAddress unresolvedAddr =
          NetUtils.createSocketAddr(schedulerAddressStr);
      String resolvedAddr =
          unresolvedAddr.getAddress().getHostAddress() + ":"
              + unresolvedAddr.getPort();
      token.setService(new Text(resolvedAddr));
      String appMasterTokenEncoded = token.encodeToUrlString();
      LOG.debug("Putting appMaster token in env : " + appMasterTokenEncoded);
      environment.put(
          ApplicationConstants.APPLICATION_MASTER_TOKEN_ENV_NAME,
          appMasterTokenEncoded);

      // Add the RM token
      credentials.addToken(new Text(resolvedAddr), token);
      DataOutputBuffer dob = new DataOutputBuffer();
      credentials.writeTokenStorageToStream(dob);
      container.setContainerTokens(
          ByteBuffer.wrap(dob.getData(), 0, dob.getLength()));

      ApplicationTokenIdentifier identifier = new ApplicationTokenIdentifier(
          application.getAppAttemptId().getApplicationId());
      SecretKey clientSecretKey =
          this.clientToAMSecretManager.getMasterKey(identifier);
      String encoded =
          Base64.encodeBase64URLSafeString(clientSecretKey.getEncoded());
      LOG.debug("The encoded client secret-key to be put in env : " + encoded);
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
