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

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import junit.framework.Assert;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateRequest;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterRequest;
import org.apache.hadoop.yarn.api.protocolrecords.SubmitApplicationRequest;
import org.apache.hadoop.yarn.api.records.AMResponse;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationMaster;
import org.apache.hadoop.yarn.api.records.ApplicationStatus;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.QueueInfo;
import org.apache.hadoop.yarn.api.records.QueueUserACLInfo;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.security.ApplicationTokenSecretManager;
import org.apache.hadoop.yarn.server.resourcemanager.ClientRMService;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.RMContextImpl;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.resourcemanager.ams.ApplicationMasterService;
import org.apache.hadoop.yarn.server.resourcemanager.applicationsmanager.events.ApplicationEventType;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.ApplicationsStore.ApplicationStore;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.MemStore;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.Allocation;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.YarnScheduler;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestAMRMRPCResponseId {
  private static RecordFactory recordFactory = RecordFactoryProvider.getRecordFactory(null);
  ApplicationMasterService amService = null;
  ApplicationTokenSecretManager appTokenManager = new ApplicationTokenSecretManager();
  DummyApplicationsManager applicationsManager;
  private ClientRMService clientService;
  DummyScheduler scheduler;
  private final static Container[] EMPTY_CONTAINER_ARRAY = new Container[] {};
  private final static List<Container> EMPTY_CONTAINER_LIST = Arrays.asList(EMPTY_CONTAINER_ARRAY);
  
  private RMContext context;
  private class DummyApplicationsManager extends ApplicationsManagerImpl {
    public DummyApplicationsManager(
        ApplicationTokenSecretManager applicationTokenSecretManager,
        YarnScheduler scheduler, RMContext asmContext) {
      super(applicationTokenSecretManager, scheduler, asmContext);      
    }
  }
  
  
  private class DummyScheduler implements YarnScheduler {
    @Override
    public Allocation allocate(ApplicationId applicationId,
        List<ResourceRequest> ask, List<Container> release) throws IOException {
      return new Allocation(EMPTY_CONTAINER_LIST, recordFactory.newRecordInstance(Resource.class));
    }

    @Override
    public QueueInfo getQueueInfo(String queueName,
        boolean includeChildQueues,
        boolean recursive) throws IOException {
      return null;
    }

    @Override
    public List<QueueUserACLInfo> getQueueUserAclInfo() {
      return null;
    }

    @Override
    public void addApplication(ApplicationId applicationId,
        ApplicationMaster master, String user, String queue, Priority priority, 
        ApplicationStore store)
        throws IOException { 
    }

    @Override
    public Resource getMaximumResourceCapability() {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public Resource getMinimumResourceCapability() {
      // TODO Auto-generated method stub
      return null;
    }
  }
  
  @Before
  public void setUp() {
    context = new RMContextImpl(new MemStore());

    context.getDispatcher().register(ApplicationEventType.class,
        new ResourceManager.ApplicationEventDispatcher(context));

    scheduler = new DummyScheduler();
    applicationsManager = new DummyApplicationsManager(new 
        ApplicationTokenSecretManager(), scheduler, context);
    Configuration conf = new Configuration();
    this.clientService = new ClientRMService(context, applicationsManager
        .getAmLivelinessMonitor(), applicationsManager
        .getClientToAMSecretManager(), scheduler);
    this.clientService.init(conf);
    amService = new ApplicationMasterService(appTokenManager, scheduler,
        context);
    applicationsManager.init(conf);
    amService.init(conf);
    context.getDispatcher().init(conf);
    context.getDispatcher().start();
  }
  
  @After
  public void tearDown() {
    
  }

  @Test
  public void testARRMResponseId() throws Exception {
    ApplicationId applicationID = clientService.getNewApplicationId();
    ApplicationSubmissionContext context = recordFactory.newRecordInstance(ApplicationSubmissionContext.class);
    context.setApplicationId(applicationID);
    SubmitApplicationRequest submitRequest = recordFactory
        .newRecordInstance(SubmitApplicationRequest.class);
    submitRequest.setApplicationSubmissionContext(context);
    clientService.submitApplication(submitRequest);
    ApplicationMaster applicationMaster = recordFactory.newRecordInstance(ApplicationMaster.class);
    applicationMaster.setApplicationId(applicationID);
    applicationMaster.setStatus(recordFactory.newRecordInstance(ApplicationStatus.class));
    RegisterApplicationMasterRequest request = recordFactory.newRecordInstance(RegisterApplicationMasterRequest.class);
    request.setApplicationMaster(applicationMaster);
    amService.registerApplicationMaster(request);
    ApplicationStatus status = recordFactory.newRecordInstance(ApplicationStatus.class);
    status.setApplicationId(applicationID);
    
    AllocateRequest allocateRequest = recordFactory.newRecordInstance(AllocateRequest.class);
    allocateRequest.setApplicationStatus(status);
    AMResponse response = amService.allocate(allocateRequest).getAMResponse();
    Assert.assertEquals(1, response.getResponseId());
    Assert.assertFalse(response.getReboot());
    status.setResponseId(response.getResponseId());
    
    allocateRequest.setApplicationStatus(status);
    response = amService.allocate(allocateRequest).getAMResponse();
    Assert.assertEquals(2, response.getResponseId());
    /* try resending */
    response = amService.allocate(allocateRequest).getAMResponse();
    Assert.assertEquals(2, response.getResponseId());
    
    /** try sending old **/
    status.setResponseId(0);
    allocateRequest.setApplicationStatus(status);
    response = amService.allocate(allocateRequest).getAMResponse();
    Assert.assertTrue(response.getReboot());
  }
}
