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

package org.apache.hadoop.yarn.server.resourcemanager.applicationsmanager;

import junit.framework.Assert;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationReportRequest;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterRequest;
import org.apache.hadoop.yarn.api.protocolrecords.SubmitApplicationRequest;
import org.apache.hadoop.yarn.api.records.AMResponse;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationState;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.server.resourcemanager.ApplicationMasterService;
import org.apache.hadoop.yarn.server.resourcemanager.ClientRMService;
import org.apache.hadoop.yarn.server.resourcemanager.MockAM;
import org.apache.hadoop.yarn.server.resourcemanager.MockNM;
import org.apache.hadoop.yarn.server.resourcemanager.MockRM;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptState;
import org.apache.hadoop.yarn.util.Records;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mortbay.log.Log;

public class TestAMRMRPCResponseId {

  private static final RecordFactory recordFactory = RecordFactoryProvider
      .getRecordFactory(null);

  private MockRM rm;
  ApplicationMasterService amService = null;
  private ClientRMService clientService;
  
  private RMContext context;

  @Before
  public void setUp() {
    this.rm = new MockRM();
    rm.start();
    this.clientService = rm.getClientRMService();
    amService = rm.getApplicationMasterService();
  }
  
  @After
  public void tearDown() {
    if (rm != null) {
      this.rm.stop();
    }
  }

  @Test
  public void testARRMResponseId() throws Exception {

    MockNM nm1 = rm.registerNode("h1:1234", 5000);

    RMApp app = rm.submitApp(2000);

    // Trigger the scheduling so the AM gets 'launched'
    nm1.nodeHeartbeat(true);

    RMAppAttempt attempt = app.getCurrentAppAttempt();
    MockAM am = rm.sendAMLaunched(attempt.getAppAttemptId());

    am.registerAppAttempt();
    
    AllocateRequest allocateRequest = recordFactory.newRecordInstance(AllocateRequest.class);
    allocateRequest.setApplicationAttemptId(attempt.getAppAttemptId());

    AMResponse response = amService.allocate(allocateRequest).getAMResponse();
    Assert.assertEquals(1, response.getResponseId());
    Assert.assertFalse(response.getReboot());
    allocateRequest.setResponseId(response.getResponseId());
    
    response = amService.allocate(allocateRequest).getAMResponse();
    Assert.assertEquals(2, response.getResponseId());
    /* try resending */
    response = amService.allocate(allocateRequest).getAMResponse();
    Assert.assertEquals(2, response.getResponseId());
    
    /** try sending old **/
    allocateRequest.setResponseId(0);
    response = amService.allocate(allocateRequest).getAMResponse();
    Assert.assertTrue(response.getReboot());
  }
}
