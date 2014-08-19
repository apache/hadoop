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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.security.AccessControlException;
import java.security.PrivilegedExceptionAction;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.protocolrecords.KillApplicationRequest;
import org.apache.hadoop.yarn.api.protocolrecords.MoveApplicationAcrossQueuesRequest;
import org.apache.hadoop.yarn.api.protocolrecords.MoveApplicationAcrossQueuesResponse;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.QueueACL;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppState;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fifo.FifoScheduler;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestMoveApplication {
  private ResourceManager resourceManager = null;
  private static boolean failMove;
  private Configuration conf;

  @Before
  public void setUp() throws Exception {
    conf = new YarnConfiguration();
    conf.setClass(YarnConfiguration.RM_SCHEDULER, FifoSchedulerWithMove.class,
        FifoSchedulerWithMove.class);
    conf.set(YarnConfiguration.YARN_ADMIN_ACL, " ");
    conf.setBoolean(YarnConfiguration.YARN_ACL_ENABLE, true);
    resourceManager = new ResourceManager();
    resourceManager.init(conf);
    resourceManager.getRMContext().getContainerTokenSecretManager().rollMasterKey();
    resourceManager.getRMContext().getNMTokenSecretManager().rollMasterKey();
    resourceManager.start();
    failMove = false;
  }
  
  @After
  public void tearDown() {
    resourceManager.stop();
  }
  
  @Test
  public void testMoveRejectedByScheduler() throws Exception {
    failMove = true;
    
    // Submit application
    Application application = new Application("user1", resourceManager);
    application.submit();

    // Wait for app to be accepted
    RMApp app = resourceManager.rmContext.getRMApps()
            .get(application.getApplicationId());
    while (app.getState() != RMAppState.ACCEPTED) {
      Thread.sleep(100);
    }

    ClientRMService clientRMService = resourceManager.getClientRMService();
    try {
      // FIFO scheduler does not support moves
      clientRMService.moveApplicationAcrossQueues(
          MoveApplicationAcrossQueuesRequest.newInstance(
              application.getApplicationId(), "newqueue"));
      fail("Should have hit exception");
    } catch (YarnException ex) {
      assertEquals("Move not supported", ex.getCause().getMessage());
    }
  }
  
  @Test (timeout = 10000)
  public void testMoveTooLate() throws Exception {
    // Submit application
    Application application = new Application("user1", resourceManager);
    ApplicationId appId = application.getApplicationId();
    application.submit();
    
    ClientRMService clientRMService = resourceManager.getClientRMService();
    // Kill the application
    clientRMService.forceKillApplication(
        KillApplicationRequest.newInstance(appId));
    RMApp rmApp = resourceManager.getRMContext().getRMApps().get(appId);
    // wait until it's dead
    while (rmApp.getState() != RMAppState.KILLED) {
      Thread.sleep(100);
    }
    
    try {
      clientRMService.moveApplicationAcrossQueues(
          MoveApplicationAcrossQueuesRequest.newInstance(appId, "newqueue"));
      fail("Should have hit exception");
    } catch (YarnException ex) {
      assertEquals(YarnException.class,
          ex.getClass());
      assertEquals("App in KILLED state cannot be moved.", ex.getMessage());
    }
  }
  
  @Test (timeout = 10000)
      public
      void testMoveSuccessful() throws Exception {
    MockRM rm1 = new MockRM(conf);
    rm1.start();
    RMApp app = rm1.submitApp(1024);
    ClientRMService clientRMService = rm1.getClientRMService();
    // FIFO scheduler does not support moves
    clientRMService
      .moveApplicationAcrossQueues(MoveApplicationAcrossQueuesRequest
        .newInstance(app.getApplicationId(), "newqueue"));

    RMApp rmApp = rm1.getRMContext().getRMApps().get(app.getApplicationId());
    assertEquals("newqueue", rmApp.getQueue());
    rm1.stop();
  }

  @Test
  public void testMoveRejectedByPermissions() throws Exception {
    failMove = true;
    
    // Submit application
    final Application application = new Application("user1", resourceManager);
    application.submit();

    final ClientRMService clientRMService = resourceManager.getClientRMService();
    try {
      UserGroupInformation.createRemoteUser("otheruser").doAs(
          new PrivilegedExceptionAction<MoveApplicationAcrossQueuesResponse>() {
            @Override
            public MoveApplicationAcrossQueuesResponse run() throws Exception {
              return clientRMService.moveApplicationAcrossQueues(
                  MoveApplicationAcrossQueuesRequest.newInstance(
                      application.getApplicationId(), "newqueue"));
            }
            
          });
      fail("Should have hit exception");
    } catch (Exception ex) {
      assertEquals(AccessControlException.class, ex.getCause().getCause().getClass());
    }
  }
  
  public static class FifoSchedulerWithMove extends FifoScheduler {
    @Override
    public String moveApplication(ApplicationId appId, String newQueue)
        throws YarnException {
      if (failMove) {
        throw new YarnException("Move not supported");
      }
      return newQueue;
    }
    
    
    @Override
    public synchronized boolean checkAccess(UserGroupInformation callerUGI,
        QueueACL acl, String queueName) {
      return acl != QueueACL.ADMINISTER_QUEUE;
    }
  }
}
