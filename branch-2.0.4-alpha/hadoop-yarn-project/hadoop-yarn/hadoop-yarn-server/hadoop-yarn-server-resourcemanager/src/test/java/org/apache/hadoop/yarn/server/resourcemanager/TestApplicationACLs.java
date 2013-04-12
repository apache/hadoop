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
import java.net.InetSocketAddress;
import java.security.PrivilegedExceptionAction;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import junit.framework.Assert;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.AccessControlList;
import org.apache.hadoop.yarn.api.ClientRMProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.GetAllApplicationsRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationReportRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationRequest;
import org.apache.hadoop.yarn.api.protocolrecords.KillApplicationRequest;
import org.apache.hadoop.yarn.api.protocolrecords.SubmitApplicationRequest;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationAccessType;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ApplicationResourceUsageReport;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnRemoteException;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.RMStateStore;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.RMStateStoreFactory;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppState;
import org.apache.hadoop.yarn.service.Service.STATE;
import org.apache.hadoop.yarn.util.BuilderUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestApplicationACLs {

  private static final String APP_OWNER = "owner";
  private static final String FRIEND = "friend";
  private static final String ENEMY = "enemy";
  private static final String SUPER_USER = "superUser";
  private static final String FRIENDLY_GROUP = "friendly-group";
  private static final String SUPER_GROUP = "superGroup";
  private static final String UNAVAILABLE = "N/A";

  private static final Log LOG = LogFactory.getLog(TestApplicationACLs.class);

  static MockRM resourceManager;
  static Configuration conf = new YarnConfiguration();
  final static YarnRPC rpc = YarnRPC.create(conf);
  final static InetSocketAddress rmAddress = conf.getSocketAddr(
      YarnConfiguration.RM_ADDRESS,
      YarnConfiguration.DEFAULT_RM_ADDRESS,
      YarnConfiguration.DEFAULT_RM_PORT);
  private static ClientRMProtocol rmClient;

  private static RecordFactory recordFactory = RecordFactoryProvider
      .getRecordFactory(conf);

  @BeforeClass
  public static void setup() throws InterruptedException, IOException {
    RMStateStore store = RMStateStoreFactory.getStore(conf);
    conf.setBoolean(YarnConfiguration.YARN_ACL_ENABLE, true);
    AccessControlList adminACL = new AccessControlList("");
    adminACL.addGroup(SUPER_GROUP);
    conf.set(YarnConfiguration.YARN_ADMIN_ACL, adminACL.getAclString());
    resourceManager = new MockRM(conf) {
      protected ClientRMService createClientRMService() {
        return new ClientRMService(getRMContext(), this.scheduler,
            this.rmAppManager, this.applicationACLsManager, null);
      };
    };
    new Thread() {
      public void run() {
        UserGroupInformation.createUserForTesting(ENEMY, new String[] {});
        UserGroupInformation.createUserForTesting(FRIEND,
            new String[] { FRIENDLY_GROUP });
        UserGroupInformation.createUserForTesting(SUPER_USER,
            new String[] { SUPER_GROUP });
        resourceManager.start();
      };
    }.start();
    int waitCount = 0;
    while (resourceManager.getServiceState() == STATE.INITED
        && waitCount++ < 60) {
      LOG.info("Waiting for RM to start...");
      Thread.sleep(1500);
    }
    if (resourceManager.getServiceState() != STATE.STARTED) {
      // RM could have failed.
      throw new IOException(
          "ResourceManager failed to start. Final state is "
              + resourceManager.getServiceState());
    }

    UserGroupInformation owner = UserGroupInformation
        .createRemoteUser(APP_OWNER);
    rmClient = owner.doAs(new PrivilegedExceptionAction<ClientRMProtocol>() {
      @Override
      public ClientRMProtocol run() throws Exception {
        return (ClientRMProtocol) rpc.getProxy(ClientRMProtocol.class,
            rmAddress, conf);
      }
    });
  }

  @AfterClass
  public static void tearDown() {
    if(resourceManager != null) {
      resourceManager.stop();
    }
  }

  @Test
  public void testApplicationACLs() throws Exception {

    verifyOwnerAccess();

    verifySuperUserAccess();

    verifyFriendAccess();

    verifyEnemyAccess();
  }

  private ApplicationId submitAppAndGetAppId(AccessControlList viewACL,
      AccessControlList modifyACL) throws Exception {
    SubmitApplicationRequest submitRequest = recordFactory
        .newRecordInstance(SubmitApplicationRequest.class);
    ApplicationSubmissionContext context = recordFactory
        .newRecordInstance(ApplicationSubmissionContext.class);

    ApplicationId applicationId = rmClient.getNewApplication(
        recordFactory.newRecordInstance(GetNewApplicationRequest.class))
        .getApplicationId();
    context.setApplicationId(applicationId);

    Map<ApplicationAccessType, String> acls
        = new HashMap<ApplicationAccessType, String>();
    acls.put(ApplicationAccessType.VIEW_APP, viewACL.getAclString());
    acls.put(ApplicationAccessType.MODIFY_APP, modifyACL.getAclString());

    ContainerLaunchContext amContainer = recordFactory
        .newRecordInstance(ContainerLaunchContext.class);
    Resource resource = BuilderUtils.newResource(1024, 1);
    amContainer.setResource(resource);
    amContainer.setApplicationACLs(acls);
    context.setAMContainerSpec(amContainer);
    submitRequest.setApplicationSubmissionContext(context);
    rmClient.submitApplication(submitRequest);
    resourceManager.waitForState(applicationId, RMAppState.ACCEPTED);
    return applicationId;
  }

  private ClientRMProtocol getRMClientForUser(String user)
      throws IOException, InterruptedException {
    UserGroupInformation userUGI = UserGroupInformation
        .createRemoteUser(user);
    ClientRMProtocol userClient = userUGI
        .doAs(new PrivilegedExceptionAction<ClientRMProtocol>() {
          @Override
          public ClientRMProtocol run() throws Exception {
            return (ClientRMProtocol) rpc.getProxy(ClientRMProtocol.class,
                rmAddress, conf);
          }
        });
    return userClient;
  }

  private void verifyOwnerAccess() throws Exception {

    AccessControlList viewACL = new AccessControlList("");
    viewACL.addGroup(FRIENDLY_GROUP);
    AccessControlList modifyACL = new AccessControlList("");
    modifyACL.addUser(FRIEND);
    ApplicationId applicationId = submitAppAndGetAppId(viewACL, modifyACL);

    final GetApplicationReportRequest appReportRequest = recordFactory
        .newRecordInstance(GetApplicationReportRequest.class);
    appReportRequest.setApplicationId(applicationId);
    final KillApplicationRequest finishAppRequest = recordFactory
        .newRecordInstance(KillApplicationRequest.class);
    finishAppRequest.setApplicationId(applicationId);

    // View as owner
    rmClient.getApplicationReport(appReportRequest);

    // List apps as owner
    Assert.assertEquals("App view by owner should list the apps!!", 1,
        rmClient.getAllApplications(
            recordFactory.newRecordInstance(GetAllApplicationsRequest.class))
            .getApplicationList().size());

    // Kill app as owner
    rmClient.forceKillApplication(finishAppRequest);
    resourceManager.waitForState(applicationId, RMAppState.KILLED);
  }

  private void verifySuperUserAccess() throws Exception {

    AccessControlList viewACL = new AccessControlList("");
    viewACL.addGroup(FRIENDLY_GROUP);
    AccessControlList modifyACL = new AccessControlList("");
    modifyACL.addUser(FRIEND);
    ApplicationId applicationId = submitAppAndGetAppId(viewACL, modifyACL);

    final GetApplicationReportRequest appReportRequest = recordFactory
        .newRecordInstance(GetApplicationReportRequest.class);
    appReportRequest.setApplicationId(applicationId);
    final KillApplicationRequest finishAppRequest = recordFactory
        .newRecordInstance(KillApplicationRequest.class);
    finishAppRequest.setApplicationId(applicationId);

    ClientRMProtocol superUserClient = getRMClientForUser(SUPER_USER);

    // View as the superUser
    superUserClient.getApplicationReport(appReportRequest);

    // List apps as superUser
    Assert.assertEquals("App view by super-user should list the apps!!", 2,
        superUserClient.getAllApplications(
            recordFactory.newRecordInstance(GetAllApplicationsRequest.class))
            .getApplicationList().size());

    // Kill app as the superUser
    superUserClient.forceKillApplication(finishAppRequest);
    resourceManager.waitForState(applicationId, RMAppState.KILLED);
  }

  private void verifyFriendAccess() throws Exception {

    AccessControlList viewACL = new AccessControlList("");
    viewACL.addGroup(FRIENDLY_GROUP);
    AccessControlList modifyACL = new AccessControlList("");
    modifyACL.addUser(FRIEND);
    ApplicationId applicationId = submitAppAndGetAppId(viewACL, modifyACL);

    final GetApplicationReportRequest appReportRequest = recordFactory
        .newRecordInstance(GetApplicationReportRequest.class);
    appReportRequest.setApplicationId(applicationId);
    final KillApplicationRequest finishAppRequest = recordFactory
        .newRecordInstance(KillApplicationRequest.class);
    finishAppRequest.setApplicationId(applicationId);

    ClientRMProtocol friendClient = getRMClientForUser(FRIEND);

    // View as the friend
    friendClient.getApplicationReport(appReportRequest);

    // List apps as friend
    Assert.assertEquals("App view by a friend should list the apps!!", 3,
        friendClient.getAllApplications(
            recordFactory.newRecordInstance(GetAllApplicationsRequest.class))
            .getApplicationList().size());

    // Kill app as the friend
    friendClient.forceKillApplication(finishAppRequest);
    resourceManager.waitForState(applicationId, RMAppState.KILLED);
  }

  private void verifyEnemyAccess() throws Exception {

    AccessControlList viewACL = new AccessControlList("");
    viewACL.addGroup(FRIENDLY_GROUP);
    AccessControlList modifyACL = new AccessControlList("");
    modifyACL.addUser(FRIEND);
    ApplicationId applicationId = submitAppAndGetAppId(viewACL, modifyACL);

    final GetApplicationReportRequest appReportRequest = recordFactory
        .newRecordInstance(GetApplicationReportRequest.class);
    appReportRequest.setApplicationId(applicationId);
    final KillApplicationRequest finishAppRequest = recordFactory
        .newRecordInstance(KillApplicationRequest.class);
    finishAppRequest.setApplicationId(applicationId);

    ClientRMProtocol enemyRmClient = getRMClientForUser(ENEMY);

    // View as the enemy
    ApplicationReport appReport = enemyRmClient.getApplicationReport(
        appReportRequest).getApplicationReport();
    verifyEnemyAppReport(appReport);

    // List apps as enemy
    List<ApplicationReport> appReports = enemyRmClient
        .getAllApplications(recordFactory
            .newRecordInstance(GetAllApplicationsRequest.class))
        .getApplicationList();
    Assert.assertEquals("App view by enemy should list the apps!!", 4,
        appReports.size());
    for (ApplicationReport report : appReports) {
      verifyEnemyAppReport(report);
    }

    // Kill app as the enemy
    try {
      enemyRmClient.forceKillApplication(finishAppRequest);
      Assert.fail("App killing by the enemy should fail!!");
    } catch (YarnRemoteException e) {
      LOG.info("Got exception while killing app as the enemy", e);
      Assert
          .assertTrue(e.getMessage().contains(
              "User enemy cannot perform operation MODIFY_APP on "
                  + applicationId));
    }

    rmClient.forceKillApplication(finishAppRequest);
  }

  private void verifyEnemyAppReport(ApplicationReport appReport) {
    Assert.assertEquals("Enemy should not see app host!",
        UNAVAILABLE, appReport.getHost());
    Assert.assertEquals("Enemy should not see app rpc port!",
        -1, appReport.getRpcPort());
    Assert.assertEquals("Enemy should not see app client token!",
        null, appReport.getClientToken());
    Assert.assertEquals("Enemy should not see app diagnostics!",
        UNAVAILABLE, appReport.getDiagnostics());
    Assert.assertEquals("Enemy should not see app tracking url!",
        UNAVAILABLE, appReport.getTrackingUrl());
    Assert.assertEquals("Enemy should not see app original tracking url!",
        UNAVAILABLE, appReport.getOriginalTrackingUrl());
    ApplicationResourceUsageReport usageReport =
        appReport.getApplicationResourceUsageReport();
    Assert.assertEquals("Enemy should not see app used containers",
        -1, usageReport.getNumUsedContainers());
    Assert.assertEquals("Enemy should not see app reserved containers",
        -1, usageReport.getNumReservedContainers());
    Assert.assertEquals("Enemy should not see app used resources",
        -1, usageReport.getUsedResources().getMemory());
    Assert.assertEquals("Enemy should not see app reserved resources",
        -1, usageReport.getReservedResources().getMemory());
    Assert.assertEquals("Enemy should not see app needed resources",
        -1, usageReport.getNeededResources().getMemory());
  }
}
