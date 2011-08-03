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

package org.apache.hadoop.yarn.server;

import static org.junit.Assert.fail;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.List;

import junit.framework.Assert;

import org.apache.avro.AvroRuntimeException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.UnsupportedFileSystemException;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.SecurityInfo;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.yarn.api.AMRMProtocol;
import org.apache.hadoop.yarn.api.ContainerManager;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationReportRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetContainerStatusRequest;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterRequest;
import org.apache.hadoop.yarn.api.protocolrecords.SubmitApplicationRequest;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationMaster;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ApplicationState;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerToken;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.api.records.URL;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnRemoteException;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.yarn.security.ApplicationTokenIdentifier;
import org.apache.hadoop.yarn.security.ApplicationTokenSecretManager;
import org.apache.hadoop.yarn.security.ContainerManagerSecurityInfo;
import org.apache.hadoop.yarn.security.ContainerTokenIdentifier;
import org.apache.hadoop.yarn.security.SchedulerSecurityInfo;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptState;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestContainerTokenSecretManager {

  private static Log LOG = LogFactory
      .getLog(TestContainerTokenSecretManager.class);
  private static final RecordFactory recordFactory = RecordFactoryProvider
      .getRecordFactory(null);
  private static FileContext localFS = null;
  private static final File localDir = new File("target",
      TestContainerTokenSecretManager.class.getName() + "-localDir")
      .getAbsoluteFile();

  @BeforeClass
  public static void setup() throws AccessControlException,
      FileNotFoundException, UnsupportedFileSystemException, IOException {
    localFS = FileContext.getLocalFSFileContext();
    localFS.delete(new Path(localDir.getAbsolutePath()), true);
    localDir.mkdir();
  }

  @Test
  public void test() throws IOException, InterruptedException {

    final ApplicationId appID = recordFactory.newRecordInstance(ApplicationId.class);
    appID.setClusterTimestamp(1234);
    appID.setId(5);

    final Configuration conf = new Configuration();
    conf.set(CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHENTICATION,
        "kerberos");
    // Set AM expiry interval to be very long.
    conf.setLong(YarnConfiguration.AM_EXPIRY_INTERVAL, 100000L);
    UserGroupInformation.setConfiguration(conf);
    MiniYARNCluster yarnCluster =
        new MiniYARNCluster(TestContainerTokenSecretManager.class.getName());
    yarnCluster.init(conf);
    yarnCluster.start();

    ResourceManager resourceManager = yarnCluster.getResourceManager();

    final YarnRPC yarnRPC = YarnRPC.create(conf);

    // Submit an application
    ApplicationSubmissionContext appSubmissionContext =
        recordFactory.newRecordInstance(ApplicationSubmissionContext.class);
    appSubmissionContext.setApplicationId(appID);
    appSubmissionContext.setMasterCapability(recordFactory
        .newRecordInstance(Resource.class));
    appSubmissionContext.getMasterCapability().setMemory(1024);
//    appSubmissionContext.resources = new HashMap<String, URL>();
    appSubmissionContext.setUser("testUser");
//    appSubmissionContext.environment = new HashMap<String, String>();
//    appSubmissionContext.command = new ArrayList<String>();
    appSubmissionContext.addCommand("sleep");
    appSubmissionContext.addCommand("100");

    // TODO: Use a resource to work around bugs. Today NM doesn't create local
    // app-dirs if there are no file to download!!
    File file = new File(localDir.getAbsolutePath(), "testFile");
    FileWriter tmpFile = new FileWriter(file);
    tmpFile.write("testing");
    tmpFile.close();
    URL testFileURL =
        ConverterUtils.getYarnUrlFromPath(FileContext.getFileContext()
            .makeQualified(new Path(localDir.getAbsolutePath(), "testFile")));
    LocalResource rsrc = recordFactory.newRecordInstance(LocalResource.class);
    rsrc.setResource(testFileURL);
    rsrc.setSize(file.length());
    rsrc.setTimestamp(file.lastModified());
    rsrc.setType(LocalResourceType.FILE);
    rsrc.setVisibility(LocalResourceVisibility.PRIVATE);
    appSubmissionContext.setResourceTodo("testFile", rsrc);
    SubmitApplicationRequest submitRequest = recordFactory
        .newRecordInstance(SubmitApplicationRequest.class);
    submitRequest.setApplicationSubmissionContext(appSubmissionContext);
    resourceManager.getClientRMService().submitApplication(submitRequest);

    // Wait till container gets allocated for AM
    int waitCounter = 0;
    RMApp app = resourceManager.getRMContext().getRMApps().get(appID);
    RMAppAttempt appAttempt = app == null ? null : app.getCurrentAppAttempt();
    RMAppAttemptState state = appAttempt == null ? null : appAttempt
        .getAppAttemptState();
    while ((app == null || appAttempt == null || state == null
        || !state.equals(RMAppAttemptState.LAUNCHED)) && waitCounter++ != 20) {
      LOG.info("Waiting for applicationAttempt to be created.. ");
      Thread.sleep(1000);
      app = resourceManager.getRMContext().getRMApps().get(appID);
      appAttempt = app == null ? null : app.getCurrentAppAttempt();
      state = appAttempt == null ? null : appAttempt.getAppAttemptState();
    }
    Assert.assertNotNull(app);
    Assert.assertNotNull(appAttempt);
    Assert.assertNotNull(state);
    Assert.assertEquals(RMAppAttemptState.LAUNCHED, state);

    UserGroupInformation currentUser = UserGroupInformation.getCurrentUser();

    // Ask for a container from the RM
    String schedulerAddressString =
        conf.get(YarnConfiguration.SCHEDULER_ADDRESS,
            YarnConfiguration.DEFAULT_SCHEDULER_BIND_ADDRESS);
    final InetSocketAddress schedulerAddr =
        NetUtils.createSocketAddr(schedulerAddressString);
    ApplicationTokenIdentifier appTokenIdentifier =
        new ApplicationTokenIdentifier(appID);
    ApplicationTokenSecretManager appTokenSecretManager =
        new ApplicationTokenSecretManager();
    appTokenSecretManager.setMasterKey(ApplicationTokenSecretManager
        .createSecretKey("Dummy".getBytes())); // TODO: FIX. Be in Sync with
                                               // ResourceManager.java
    Token<ApplicationTokenIdentifier> appToken =
        new Token<ApplicationTokenIdentifier>(appTokenIdentifier,
            appTokenSecretManager);
    appToken.setService(new Text(schedulerAddressString));
    currentUser.addToken(appToken);

    conf.setClass(
        YarnConfiguration.YARN_SECURITY_INFO,
        SchedulerSecurityInfo.class, SecurityInfo.class);
    AMRMProtocol scheduler =
        currentUser.doAs(new PrivilegedAction<AMRMProtocol>() {
          @Override
          public AMRMProtocol run() {
            return (AMRMProtocol) yarnRPC.getProxy(AMRMProtocol.class,
                schedulerAddr, conf);
          }
        });       

    // Register the appMaster 
    RegisterApplicationMasterRequest request =
        recordFactory
            .newRecordInstance(RegisterApplicationMasterRequest.class);
    ApplicationMaster applicationMaster = recordFactory
        .newRecordInstance(ApplicationMaster.class);
    request.setApplicationAttemptId(resourceManager.getRMContext()
        .getRMApps().get(appID).getCurrentAppAttempt().getAppAttemptId());
    scheduler.registerApplicationMaster(request);

    // Now request a container allocation.
    List<ResourceRequest> ask = new ArrayList<ResourceRequest>();
    ResourceRequest rr = recordFactory.newRecordInstance(ResourceRequest.class);
    rr.setCapability(recordFactory.newRecordInstance(Resource.class));
    rr.getCapability().setMemory(1024);
    rr.setHostName("*");
    rr.setNumContainers(1);
    rr.setPriority(recordFactory.newRecordInstance(Priority.class));
    rr.getPriority().setPriority(0);
    ask.add(rr);
    ArrayList<Container> release = new ArrayList<Container>();
    
    AllocateRequest allocateRequest =
        recordFactory.newRecordInstance(AllocateRequest.class);
    allocateRequest.setApplicationAttemptId(appAttempt.getAppAttemptId());
    allocateRequest.setResponseId(0);
    allocateRequest.addAllAsks(ask);
    allocateRequest.addAllReleases(release);
    List<Container> allocatedContainers = scheduler.allocate(allocateRequest)
        .getAMResponse().getNewContainerList();

    waitCounter = 0;
    while ((allocatedContainers == null || allocatedContainers.size() == 0)
        && waitCounter++ != 20) {
      LOG.info("Waiting for container to be allocated..");
      Thread.sleep(1000);
      allocateRequest.setResponseId(allocateRequest.getResponseId() + 1);
      allocatedContainers =
          scheduler.allocate(allocateRequest).getAMResponse()
              .getNewContainerList();
    }

    Assert.assertNotNull("Container is not allocted!", allocatedContainers);
    Assert.assertEquals("Didn't get one container!", 1,
        allocatedContainers.size());

    // Now talk to the NM for launching the container.
    final Container allocatedContainer = allocatedContainers.get(0);
    ContainerToken containerToken = allocatedContainer.getContainerToken();
    Token<ContainerTokenIdentifier> token =
        new Token<ContainerTokenIdentifier>(
            containerToken.getIdentifier().array(),
            containerToken.getPassword().array(), new Text(
                containerToken.getKind()), new Text(
                containerToken.getService()));
    currentUser.addToken(token);
    conf.setClass(
        YarnConfiguration.YARN_SECURITY_INFO,
        ContainerManagerSecurityInfo.class, SecurityInfo.class);
    currentUser.doAs(new PrivilegedAction<Void>() {
      @Override
      public Void run() {
        ContainerManager client = (ContainerManager) yarnRPC.getProxy(
            ContainerManager.class, NetUtils
                .createSocketAddr(allocatedContainer.getNodeId().toString()),
            conf);
        try {
          LOG.info("Going to make a getContainerStatus() legal request");
          GetContainerStatusRequest request =
              recordFactory
                  .newRecordInstance(GetContainerStatusRequest.class);
          ContainerId containerID =
              recordFactory.newRecordInstance(ContainerId.class);
          containerID.setAppId(appID);
          containerID.setId(1);
          request.setContainerId(containerID);
          client.getContainerStatus(request);
        } catch (YarnRemoteException e) {
          LOG.info("Error", e);
        } catch (AvroRuntimeException e) {
          LOG.info("Got the expected exception");
        }
        return null;
      }
    });

    UserGroupInformation maliceUser =
        UserGroupInformation.createRemoteUser(currentUser.getShortUserName());
    byte[] identifierBytes = containerToken.getIdentifier().array();
    DataInputBuffer di = new DataInputBuffer();
    di.reset(identifierBytes, identifierBytes.length);
    ContainerTokenIdentifier dummyIdentifier = new ContainerTokenIdentifier();
    dummyIdentifier.readFields(di);
    Resource modifiedResource = recordFactory.newRecordInstance(Resource.class);
    modifiedResource.setMemory(2048);
    ContainerTokenIdentifier modifiedIdentifier =
        new ContainerTokenIdentifier(dummyIdentifier.getContainerID(),
            dummyIdentifier.getNmHostName(), modifiedResource);
    // Malice user modifies the resource amount
    Token<ContainerTokenIdentifier> modifiedToken =
        new Token<ContainerTokenIdentifier>(modifiedIdentifier.getBytes(),
            containerToken.getPassword().array(), new Text(
                containerToken.getKind()), new Text(
                containerToken.getService()));
    maliceUser.addToken(modifiedToken);
    maliceUser.doAs(new PrivilegedAction<Void>() {
      @Override
      public Void run() {
        ContainerManager client = (ContainerManager) yarnRPC.getProxy(
            ContainerManager.class, NetUtils
                .createSocketAddr(allocatedContainer.getNodeId().toString()),
            conf);
        ContainerId containerID;

        LOG.info("Going to contact NM:  ilLegal request");
        GetContainerStatusRequest request =
              recordFactory
                  .newRecordInstance(GetContainerStatusRequest.class);
        containerID =
              recordFactory.newRecordInstance(ContainerId.class);
        containerID.setAppId(appID);
        containerID.setId(1);
        request.setContainerId(containerID);
        try {
          client.getContainerStatus(request);
          fail("Connection initiation with illegally modified "
              + "tokens is expected to fail.");
        } catch (YarnRemoteException e) {
          LOG.error("Got exception", e);
          fail("Cannot get a YARN remote exception as " +
          		"it will indicate RPC success");
        } catch (Exception e) {
          Assert.assertEquals(
              java.lang.reflect.UndeclaredThrowableException.class
                  .getCanonicalName(), e.getClass().getCanonicalName());
          Assert
              .assertEquals(
                  "DIGEST-MD5: digest response format violation. Mismatched response.",
                  e.getCause().getCause().getMessage());
        }
        return null;
      }
    });
  }
}
