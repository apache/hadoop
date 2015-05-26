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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.security.PrivilegedAction;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.yarn.api.ApplicationMasterProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateRequest;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterRequest;
import org.apache.hadoop.yarn.api.records.ApplicationAccessType;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerExitStatus;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.QueueInfo;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceBlacklistRequest;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.api.records.impl.pb.ResourceRequestPBImpl;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.exceptions.InvalidResourceBlacklistRequestException;
import org.apache.hadoop.yarn.exceptions.InvalidResourceRequestException;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.yarn.nodelabels.CommonNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.MockNM;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.TestAMAuthorization.MockRMWithAMS;
import org.apache.hadoop.yarn.server.resourcemanager.TestAMAuthorization.MyContainerManager;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.NullRMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppState;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptState;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.AppAddedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.AppRemovedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.SchedulerEvent;
import org.apache.hadoop.yarn.server.utils.BuilderUtils;
import org.apache.hadoop.yarn.util.Records;
import org.apache.hadoop.yarn.util.resource.DefaultResourceCalculator;
import org.apache.hadoop.yarn.util.resource.DominantResourceCalculator;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

public class TestSchedulerUtils {

  private static final Log LOG = LogFactory.getLog(TestSchedulerUtils.class);
  
  private RMContext rmContext = getMockRMContext();
  
  @Test (timeout = 30000)
  public void testNormalizeRequest() {
    ResourceCalculator resourceCalculator = new DefaultResourceCalculator();
    
    final int minMemory = 1024;
    final int maxMemory = 8192;
    Resource minResource = Resources.createResource(minMemory, 0);
    Resource maxResource = Resources.createResource(maxMemory, 0);
    
    ResourceRequest ask = new ResourceRequestPBImpl();

    // case negative memory
    ask.setCapability(Resources.createResource(-1024));
    SchedulerUtils.normalizeRequest(ask, resourceCalculator, null, minResource,
        maxResource);
    assertEquals(minMemory, ask.getCapability().getMemory());

    // case zero memory
    ask.setCapability(Resources.createResource(0));
    SchedulerUtils.normalizeRequest(ask, resourceCalculator, null, minResource,
        maxResource);
    assertEquals(minMemory, ask.getCapability().getMemory());

    // case memory is a multiple of minMemory
    ask.setCapability(Resources.createResource(2 * minMemory));
    SchedulerUtils.normalizeRequest(ask, resourceCalculator, null, minResource,
        maxResource);
    assertEquals(2 * minMemory, ask.getCapability().getMemory());

    // case memory is not a multiple of minMemory
    ask.setCapability(Resources.createResource(minMemory + 10));
    SchedulerUtils.normalizeRequest(ask, resourceCalculator, null, minResource,
        maxResource);
    assertEquals(2 * minMemory, ask.getCapability().getMemory());

    // case memory is equal to max allowed
    ask.setCapability(Resources.createResource(maxMemory));
    SchedulerUtils.normalizeRequest(ask, resourceCalculator, null, minResource,
        maxResource);
    assertEquals(maxMemory, ask.getCapability().getMemory());

    // case memory is just less than max
    ask.setCapability(Resources.createResource(maxMemory - 10));
    SchedulerUtils.normalizeRequest(ask, resourceCalculator, null, minResource,
        maxResource);
    assertEquals(maxMemory, ask.getCapability().getMemory());

    // max is not a multiple of min
    maxResource = Resources.createResource(maxMemory - 10, 0);
    ask.setCapability(Resources.createResource(maxMemory - 100));
    // multiple of minMemory > maxMemory, then reduce to maxMemory
    SchedulerUtils.normalizeRequest(ask, resourceCalculator, null, minResource,
        maxResource);
    assertEquals(maxResource.getMemory(), ask.getCapability().getMemory());

    // ask is more than max
    maxResource = Resources.createResource(maxMemory, 0);
    ask.setCapability(Resources.createResource(maxMemory + 100));
    SchedulerUtils.normalizeRequest(ask, resourceCalculator, null, minResource,
        maxResource);
    assertEquals(maxResource.getMemory(), ask.getCapability().getMemory());
  }
  
  @Test (timeout = 30000)
  public void testNormalizeRequestWithDominantResourceCalculator() {
    ResourceCalculator resourceCalculator = new DominantResourceCalculator();
    
    Resource minResource = Resources.createResource(1024, 1);
    Resource maxResource = Resources.createResource(10240, 10);
    Resource clusterResource = Resources.createResource(10 * 1024, 10);
    
    ResourceRequest ask = new ResourceRequestPBImpl();

    // case negative memory/vcores
    ask.setCapability(Resources.createResource(-1024, -1));
    SchedulerUtils.normalizeRequest(
        ask, resourceCalculator, clusterResource, minResource, maxResource);
    assertEquals(minResource, ask.getCapability());

    // case zero memory/vcores
    ask.setCapability(Resources.createResource(0, 0));
    SchedulerUtils.normalizeRequest(
        ask, resourceCalculator, clusterResource, minResource, maxResource);
    assertEquals(minResource, ask.getCapability());
    assertEquals(1, ask.getCapability().getVirtualCores());
    assertEquals(1024, ask.getCapability().getMemory());

    // case non-zero memory & zero cores
    ask.setCapability(Resources.createResource(1536, 0));
    SchedulerUtils.normalizeRequest(
        ask, resourceCalculator, clusterResource, minResource, maxResource);
    assertEquals(Resources.createResource(2048, 1), ask.getCapability());
    assertEquals(1, ask.getCapability().getVirtualCores());
    assertEquals(2048, ask.getCapability().getMemory());
  }
  
  @Test (timeout = 30000)
  public void testValidateResourceRequestWithErrorLabelsPermission()
      throws IOException {
    // mock queue and scheduler
    YarnScheduler scheduler = mock(YarnScheduler.class);
    Set<String> queueAccessibleNodeLabels = Sets.newHashSet();
    QueueInfo queueInfo = mock(QueueInfo.class);
    when(queueInfo.getQueueName()).thenReturn("queue");
    when(queueInfo.getAccessibleNodeLabels()).thenReturn(queueAccessibleNodeLabels);
    when(scheduler.getQueueInfo(any(String.class), anyBoolean(), anyBoolean()))
        .thenReturn(queueInfo);
    
    Resource maxResource = Resources.createResource(
        YarnConfiguration.DEFAULT_RM_SCHEDULER_MAXIMUM_ALLOCATION_MB,
        YarnConfiguration.DEFAULT_RM_SCHEDULER_MAXIMUM_ALLOCATION_VCORES);

    // queue has labels, success cases
    try {
      // set queue accessible node labesl to [x, y]
      queueAccessibleNodeLabels.clear();
      queueAccessibleNodeLabels.addAll(Arrays.asList("x", "y"));
      rmContext.getNodeLabelManager().addToCluserNodeLabels(
          ImmutableSet.of("x", "y"));
      Resource resource = Resources.createResource(
          0,
          YarnConfiguration.DEFAULT_RM_SCHEDULER_MINIMUM_ALLOCATION_VCORES);
      ResourceRequest resReq = BuilderUtils.newResourceRequest(
          mock(Priority.class), ResourceRequest.ANY, resource, 1);
      resReq.setNodeLabelExpression("x");
      SchedulerUtils.normalizeAndvalidateRequest(resReq, maxResource, "queue",
          scheduler, rmContext);

      resReq.setNodeLabelExpression("y");
      SchedulerUtils.normalizeAndvalidateRequest(resReq, maxResource, "queue",
          scheduler, rmContext);
      
      resReq.setNodeLabelExpression("");
      SchedulerUtils.normalizeAndvalidateRequest(resReq, maxResource, "queue",
          scheduler, rmContext);
      
      resReq.setNodeLabelExpression(" ");
      SchedulerUtils.normalizeAndvalidateRequest(resReq, maxResource, "queue",
          scheduler, rmContext);
    } catch (InvalidResourceRequestException e) {
      e.printStackTrace();
      fail("Should be valid when request labels is a subset of queue labels");
    } finally {
      rmContext.getNodeLabelManager().removeFromClusterNodeLabels(
          Arrays.asList("x", "y"));
    }
    
    // same as above, but cluster node labels don't contains label being
    // requested. should fail
    try {
      // set queue accessible node labesl to [x, y]
      queueAccessibleNodeLabels.clear();
      queueAccessibleNodeLabels.addAll(Arrays.asList("x", "y"));
      Resource resource = Resources.createResource(
          0,
          YarnConfiguration.DEFAULT_RM_SCHEDULER_MINIMUM_ALLOCATION_VCORES);
      ResourceRequest resReq = BuilderUtils.newResourceRequest(
          mock(Priority.class), ResourceRequest.ANY, resource, 1);
      resReq.setNodeLabelExpression("x");
      SchedulerUtils.normalizeAndvalidateRequest(resReq, maxResource, "queue",
          scheduler, rmContext);
      
      fail("Should fail");
    } catch (InvalidResourceRequestException e) {
    }
    
    // queue has labels, failed cases (when ask a label not included by queue)
    try {
      // set queue accessible node labesl to [x, y]
      queueAccessibleNodeLabels.clear();
      queueAccessibleNodeLabels.addAll(Arrays.asList("x", "y"));
      rmContext.getNodeLabelManager().addToCluserNodeLabels(
          ImmutableSet.of("x",
              "y"));
      
      Resource resource = Resources.createResource(
          0,
          YarnConfiguration.DEFAULT_RM_SCHEDULER_MINIMUM_ALLOCATION_VCORES);
      ResourceRequest resReq = BuilderUtils.newResourceRequest(
          mock(Priority.class), ResourceRequest.ANY, resource, 1);
      resReq.setNodeLabelExpression("z");
      SchedulerUtils.normalizeAndvalidateRequest(resReq, maxResource, "queue",
          scheduler, rmContext);
      fail("Should fail");
    } catch (InvalidResourceRequestException e) {
    } finally {
      rmContext.getNodeLabelManager().removeFromClusterNodeLabels(
          Arrays.asList("x", "y"));
    }
    
    // we don't allow specify more than two node labels in a single expression
    // now
    try {
      // set queue accessible node labesl to [x, y]
      queueAccessibleNodeLabels.clear();
      queueAccessibleNodeLabels.addAll(Arrays.asList("x", "y"));
      rmContext.getNodeLabelManager().addToCluserNodeLabels(
          ImmutableSet.of("x", "y"));
      
      Resource resource = Resources.createResource(
          0,
          YarnConfiguration.DEFAULT_RM_SCHEDULER_MINIMUM_ALLOCATION_VCORES);
      ResourceRequest resReq = BuilderUtils.newResourceRequest(
          mock(Priority.class), ResourceRequest.ANY, resource, 1);
      resReq.setNodeLabelExpression("x && y");
      SchedulerUtils.normalizeAndvalidateRequest(resReq, maxResource, "queue",
          scheduler, rmContext);
      fail("Should fail");
    } catch (InvalidResourceRequestException e) {
    } finally {
      rmContext.getNodeLabelManager().removeFromClusterNodeLabels(
          Arrays.asList("x", "y"));
    }
    
    // queue doesn't have label, succeed (when request no label)
    queueAccessibleNodeLabels.clear();
    try {
      // set queue accessible node labels to empty
      queueAccessibleNodeLabels.clear();
      
      Resource resource = Resources.createResource(
          0,
          YarnConfiguration.DEFAULT_RM_SCHEDULER_MINIMUM_ALLOCATION_VCORES);
      ResourceRequest resReq = BuilderUtils.newResourceRequest(
          mock(Priority.class), ResourceRequest.ANY, resource, 1);
      SchedulerUtils.normalizeAndvalidateRequest(resReq, maxResource, "queue",
          scheduler, rmContext);
      
      resReq.setNodeLabelExpression("");
      SchedulerUtils.normalizeAndvalidateRequest(resReq, maxResource, "queue",
          scheduler, rmContext);
      
      resReq.setNodeLabelExpression("  ");
      SchedulerUtils.normalizeAndvalidateRequest(resReq, maxResource, "queue",
          scheduler, rmContext);
    } catch (InvalidResourceRequestException e) {
      e.printStackTrace();
      fail("Should be valid when request labels is empty");
    }
    
    // queue doesn't have label, failed (when request any label)
    try {
      // set queue accessible node labels to empty
      queueAccessibleNodeLabels.clear();
      
      rmContext.getNodeLabelManager().addToCluserNodeLabels(
          ImmutableSet.of("x"));
      
      Resource resource = Resources.createResource(
          0,
          YarnConfiguration.DEFAULT_RM_SCHEDULER_MINIMUM_ALLOCATION_VCORES);
      ResourceRequest resReq = BuilderUtils.newResourceRequest(
          mock(Priority.class), ResourceRequest.ANY, resource, 1);
      resReq.setNodeLabelExpression("x");
      SchedulerUtils.normalizeAndvalidateRequest(resReq, maxResource, "queue",
          scheduler, rmContext);
      fail("Should fail");
    } catch (InvalidResourceRequestException e) {
    } finally {
      rmContext.getNodeLabelManager().removeFromClusterNodeLabels(
          Arrays.asList("x"));
    }
    
    // queue is "*", always succeeded
    try {
      // set queue accessible node labels to empty
      queueAccessibleNodeLabels.clear();
      queueAccessibleNodeLabels.add(RMNodeLabelsManager.ANY);
      
      rmContext.getNodeLabelManager().addToCluserNodeLabels(
          ImmutableSet.of("x", "y", "z"));
      
      Resource resource = Resources.createResource(
          0,
          YarnConfiguration.DEFAULT_RM_SCHEDULER_MINIMUM_ALLOCATION_VCORES);
      ResourceRequest resReq = BuilderUtils.newResourceRequest(
          mock(Priority.class), ResourceRequest.ANY, resource, 1);
      resReq.setNodeLabelExpression("x");
      SchedulerUtils.normalizeAndvalidateRequest(resReq, maxResource, "queue",
          scheduler, rmContext);
      
      resReq.setNodeLabelExpression("y");
      SchedulerUtils.normalizeAndvalidateRequest(resReq, maxResource, "queue",
          scheduler, rmContext);
      
      resReq.setNodeLabelExpression("z");
      SchedulerUtils.normalizeAndvalidateRequest(resReq, maxResource, "queue",
          scheduler, rmContext);
    } catch (InvalidResourceRequestException e) {
      e.printStackTrace();
      fail("Should be valid when queue can access any labels");
    } finally {
      rmContext.getNodeLabelManager().removeFromClusterNodeLabels(
          Arrays.asList("x", "y", "z"));
    }
    
    // same as above, but cluster node labels don't contains label, should fail
    try {
      // set queue accessible node labels to empty
      queueAccessibleNodeLabels.clear();
      queueAccessibleNodeLabels.add(RMNodeLabelsManager.ANY);
      
      Resource resource = Resources.createResource(
          0,
          YarnConfiguration.DEFAULT_RM_SCHEDULER_MINIMUM_ALLOCATION_VCORES);
      ResourceRequest resReq = BuilderUtils.newResourceRequest(
          mock(Priority.class), ResourceRequest.ANY, resource, 1);
      resReq.setNodeLabelExpression("x");
      SchedulerUtils.normalizeAndvalidateRequest(resReq, maxResource, "queue",
          scheduler, rmContext);
      fail("Should fail");
    } catch (InvalidResourceRequestException e) {
    }
    
    // we don't allow resource name other than ANY and specify label
    try {
      // set queue accessible node labesl to [x, y]
      queueAccessibleNodeLabels.clear();
      queueAccessibleNodeLabels.addAll(Arrays.asList("x", "y"));
      rmContext.getNodeLabelManager().addToCluserNodeLabels(
          ImmutableSet.of("x", "y"));
      
      Resource resource = Resources.createResource(
          0,
          YarnConfiguration.DEFAULT_RM_SCHEDULER_MINIMUM_ALLOCATION_VCORES);
      ResourceRequest resReq = BuilderUtils.newResourceRequest(
          mock(Priority.class), "rack", resource, 1);
      resReq.setNodeLabelExpression("x");
      SchedulerUtils.normalizeAndvalidateRequest(resReq, maxResource, "queue",
          scheduler, rmContext);
      fail("Should fail");
    } catch (InvalidResourceRequestException e) {
    } finally {
      rmContext.getNodeLabelManager().removeFromClusterNodeLabels(
          Arrays.asList("x", "y"));
    }
    
    // we don't allow resource name other than ANY and specify label even if
    // queue has accessible label = *
    try {
      // set queue accessible node labesl to *
      queueAccessibleNodeLabels.clear();
      queueAccessibleNodeLabels.addAll(Arrays
          .asList(CommonNodeLabelsManager.ANY));
      rmContext.getNodeLabelManager().addToCluserNodeLabels(
          ImmutableSet.of("x"));
      
      Resource resource = Resources.createResource(
          0,
          YarnConfiguration.DEFAULT_RM_SCHEDULER_MINIMUM_ALLOCATION_VCORES);
      ResourceRequest resReq = BuilderUtils.newResourceRequest(
          mock(Priority.class), "rack", resource, 1);
      resReq.setNodeLabelExpression("x");
      SchedulerUtils.normalizeAndvalidateRequest(resReq, maxResource, "queue",
          scheduler, rmContext);
      fail("Should fail");
    } catch (InvalidResourceRequestException e) {
    } finally {
      rmContext.getNodeLabelManager().removeFromClusterNodeLabels(
          Arrays.asList("x"));
    }
  }

  @Test (timeout = 30000)
  public void testValidateResourceRequest() {
    YarnScheduler mockScheduler = mock(YarnScheduler.class);

    Resource maxResource =
        Resources.createResource(
            YarnConfiguration.DEFAULT_RM_SCHEDULER_MAXIMUM_ALLOCATION_MB,
            YarnConfiguration.DEFAULT_RM_SCHEDULER_MAXIMUM_ALLOCATION_VCORES);

    // zero memory
    try {
      Resource resource =
          Resources.createResource(0,
              YarnConfiguration.DEFAULT_RM_SCHEDULER_MINIMUM_ALLOCATION_VCORES);
      ResourceRequest resReq =
          BuilderUtils.newResourceRequest(mock(Priority.class),
              ResourceRequest.ANY, resource, 1);
      SchedulerUtils.normalizeAndvalidateRequest(resReq, maxResource, null,
          mockScheduler, rmContext);
    } catch (InvalidResourceRequestException e) {
      fail("Zero memory should be accepted");
    }

    // zero vcores
    try {
      Resource resource =
          Resources.createResource(
              YarnConfiguration.DEFAULT_RM_SCHEDULER_MINIMUM_ALLOCATION_MB, 0);
      ResourceRequest resReq =
          BuilderUtils.newResourceRequest(mock(Priority.class),
              ResourceRequest.ANY, resource, 1);
      SchedulerUtils.normalizeAndvalidateRequest(resReq, maxResource, null,
          mockScheduler, rmContext);
    } catch (InvalidResourceRequestException e) {
      fail("Zero vcores should be accepted");
    }

    // max memory
    try {
      Resource resource =
          Resources.createResource(
              YarnConfiguration.DEFAULT_RM_SCHEDULER_MAXIMUM_ALLOCATION_MB,
              YarnConfiguration.DEFAULT_RM_SCHEDULER_MINIMUM_ALLOCATION_VCORES);
      ResourceRequest resReq =
          BuilderUtils.newResourceRequest(mock(Priority.class),
              ResourceRequest.ANY, resource, 1);
      SchedulerUtils.normalizeAndvalidateRequest(resReq, maxResource, null,
          mockScheduler, rmContext);
    } catch (InvalidResourceRequestException e) {
      fail("Max memory should be accepted");
    }

    // max vcores
    try {
      Resource resource =
          Resources.createResource(
              YarnConfiguration.DEFAULT_RM_SCHEDULER_MINIMUM_ALLOCATION_MB,
              YarnConfiguration.DEFAULT_RM_SCHEDULER_MAXIMUM_ALLOCATION_VCORES);
      ResourceRequest resReq =
          BuilderUtils.newResourceRequest(mock(Priority.class),
              ResourceRequest.ANY, resource, 1);
      SchedulerUtils.normalizeAndvalidateRequest(resReq, maxResource, null,
          mockScheduler, rmContext);
    } catch (InvalidResourceRequestException e) {
      fail("Max vcores should not be accepted");
    }

    // negative memory
    try {
      Resource resource =
          Resources.createResource(-1,
              YarnConfiguration.DEFAULT_RM_SCHEDULER_MINIMUM_ALLOCATION_VCORES);
      ResourceRequest resReq =
          BuilderUtils.newResourceRequest(mock(Priority.class),
              ResourceRequest.ANY, resource, 1);
      SchedulerUtils.normalizeAndvalidateRequest(resReq, maxResource, null,
          mockScheduler, rmContext);
      fail("Negative memory should not be accepted");
    } catch (InvalidResourceRequestException e) {
      // expected
    }

    // negative vcores
    try {
      Resource resource =
          Resources.createResource(
              YarnConfiguration.DEFAULT_RM_SCHEDULER_MINIMUM_ALLOCATION_MB, -1);
      ResourceRequest resReq =
          BuilderUtils.newResourceRequest(mock(Priority.class),
              ResourceRequest.ANY, resource, 1);
      SchedulerUtils.normalizeAndvalidateRequest(resReq, maxResource, null,
          mockScheduler, rmContext);
      fail("Negative vcores should not be accepted");
    } catch (InvalidResourceRequestException e) {
      // expected
    }

    // more than max memory
    try {
      Resource resource =
          Resources.createResource(
              YarnConfiguration.DEFAULT_RM_SCHEDULER_MAXIMUM_ALLOCATION_MB + 1,
              YarnConfiguration.DEFAULT_RM_SCHEDULER_MINIMUM_ALLOCATION_VCORES);
      ResourceRequest resReq =
          BuilderUtils.newResourceRequest(mock(Priority.class),
              ResourceRequest.ANY, resource, 1);
      SchedulerUtils.normalizeAndvalidateRequest(resReq, maxResource, null,
          mockScheduler, rmContext);
      fail("More than max memory should not be accepted");
    } catch (InvalidResourceRequestException e) {
      // expected
    }

    // more than max vcores
    try {
      Resource resource =
          Resources
              .createResource(
                  YarnConfiguration.DEFAULT_RM_SCHEDULER_MINIMUM_ALLOCATION_MB,
                  YarnConfiguration.DEFAULT_RM_SCHEDULER_MAXIMUM_ALLOCATION_VCORES + 1);
      ResourceRequest resReq =
          BuilderUtils.newResourceRequest(mock(Priority.class),
              ResourceRequest.ANY, resource, 1);
      SchedulerUtils.normalizeAndvalidateRequest(resReq, maxResource, null,
          mockScheduler, rmContext);
      fail("More than max vcores should not be accepted");
    } catch (InvalidResourceRequestException e) {
      // expected
    }
  }
  
  @Test
  public void testValidateResourceBlacklistRequest() throws Exception {

    MyContainerManager containerManager = new MyContainerManager();
    final MockRMWithAMS rm =
        new MockRMWithAMS(new YarnConfiguration(), containerManager);
    rm.start();

    MockNM nm1 = rm.registerNode("localhost:1234", 5120);

    Map<ApplicationAccessType, String> acls =
        new HashMap<ApplicationAccessType, String>(2);
    acls.put(ApplicationAccessType.VIEW_APP, "*");
    RMApp app = rm.submitApp(1024, "appname", "appuser", acls);

    nm1.nodeHeartbeat(true);

    RMAppAttempt attempt = app.getCurrentAppAttempt();
    ApplicationAttemptId applicationAttemptId = attempt.getAppAttemptId();
    waitForLaunchedState(attempt);

    // Create a client to the RM.
    final Configuration conf = rm.getConfig();
    final YarnRPC rpc = YarnRPC.create(conf);

    UserGroupInformation currentUser = 
        UserGroupInformation.createRemoteUser(applicationAttemptId.toString());
    Credentials credentials = containerManager.getContainerCredentials();
    final InetSocketAddress rmBindAddress =
        rm.getApplicationMasterService().getBindAddress();
    Token<? extends TokenIdentifier> amRMToken =
        MockRMWithAMS.setupAndReturnAMRMToken(rmBindAddress,
          credentials.getAllTokens());
    currentUser.addToken(amRMToken);
    ApplicationMasterProtocol client =
        currentUser.doAs(new PrivilegedAction<ApplicationMasterProtocol>() {
          @Override
          public ApplicationMasterProtocol run() {
            return (ApplicationMasterProtocol) rpc.getProxy(
              ApplicationMasterProtocol.class, rmBindAddress, conf);
          }
        });

    RegisterApplicationMasterRequest request = Records
        .newRecord(RegisterApplicationMasterRequest.class);
    client.registerApplicationMaster(request);

    ResourceBlacklistRequest blacklistRequest =
        ResourceBlacklistRequest.newInstance(
            Collections.singletonList(ResourceRequest.ANY), null);

    AllocateRequest allocateRequest =
        AllocateRequest.newInstance(0, 0.0f, null, null, blacklistRequest);
    boolean error = false;
    try {
      client.allocate(allocateRequest);
    } catch (InvalidResourceBlacklistRequestException e) {
      error = true;
    }

    rm.stop();
    
    Assert.assertTrue(
        "Didn't not catch InvalidResourceBlacklistRequestException", error);
  }

  private void waitForLaunchedState(RMAppAttempt attempt)
      throws InterruptedException {
    int waitCount = 0;
    while (attempt.getAppAttemptState() != RMAppAttemptState.LAUNCHED
        && waitCount++ < 20) {
      LOG.info("Waiting for AppAttempt to reach LAUNCHED state. "
          + "Current state is " + attempt.getAppAttemptState());
      Thread.sleep(1000);
    }
    Assert.assertEquals(attempt.getAppAttemptState(),
        RMAppAttemptState.LAUNCHED);
  }

  @Test
  public void testComparePriorities(){
    Priority high = Priority.newInstance(1);
    Priority low = Priority.newInstance(2);
    assertTrue(high.compareTo(low) > 0);
  }

  @Test
  public void testCreateAbnormalContainerStatus() {
    ContainerStatus cd = SchedulerUtils.createAbnormalContainerStatus(
        ContainerId.newContainerId(ApplicationAttemptId.newInstance(
          ApplicationId.newInstance(System.currentTimeMillis(), 1), 1), 1), "x");
    Assert.assertEquals(ContainerExitStatus.ABORTED, cd.getExitStatus());
  }

  @Test
  public void testCreatePreemptedContainerStatus() {
    ContainerStatus cd = SchedulerUtils.createPreemptedContainerStatus(
        ContainerId.newContainerId(ApplicationAttemptId.newInstance(
          ApplicationId.newInstance(System.currentTimeMillis(), 1), 1), 1), "x");
    Assert.assertEquals(ContainerExitStatus.PREEMPTED, cd.getExitStatus());
  }
  
  @Test (timeout = 30000)
  public void testNormalizeNodeLabelExpression()
      throws IOException {
    // mock queue and scheduler
    YarnScheduler scheduler = mock(YarnScheduler.class);
    Set<String> queueAccessibleNodeLabels = Sets.newHashSet();
    QueueInfo queueInfo = mock(QueueInfo.class);
    when(queueInfo.getQueueName()).thenReturn("queue");
    when(queueInfo.getAccessibleNodeLabels()).thenReturn(queueAccessibleNodeLabels);
    when(queueInfo.getDefaultNodeLabelExpression()).thenReturn(" x ");
    when(scheduler.getQueueInfo(any(String.class), anyBoolean(), anyBoolean()))
        .thenReturn(queueInfo);
    
    Resource maxResource = Resources.createResource(
        YarnConfiguration.DEFAULT_RM_SCHEDULER_MAXIMUM_ALLOCATION_MB,
        YarnConfiguration.DEFAULT_RM_SCHEDULER_MAXIMUM_ALLOCATION_VCORES);

    // queue has labels, success cases
    try {
      // set queue accessible node labels to [x, y]
      queueAccessibleNodeLabels.clear();
      queueAccessibleNodeLabels.addAll(Arrays.asList("x", "y"));
      rmContext.getNodeLabelManager().addToCluserNodeLabels(
          ImmutableSet.of("x", "y"));
      Resource resource = Resources.createResource(
          0,
          YarnConfiguration.DEFAULT_RM_SCHEDULER_MINIMUM_ALLOCATION_VCORES);
      ResourceRequest resReq = BuilderUtils.newResourceRequest(
          mock(Priority.class), ResourceRequest.ANY, resource, 1);
      SchedulerUtils.normalizeAndvalidateRequest(resReq, maxResource, "queue",
          scheduler, rmContext);
      Assert.assertTrue(resReq.getNodeLabelExpression().equals("x"));
      
      resReq.setNodeLabelExpression(" y ");
      SchedulerUtils.normalizeAndvalidateRequest(resReq, maxResource, "queue",
          scheduler, rmContext);
      Assert.assertTrue(resReq.getNodeLabelExpression().equals("y"));
    } catch (InvalidResourceRequestException e) {
      e.printStackTrace();
      fail("Should be valid when request labels is a subset of queue labels");
    } finally {
      rmContext.getNodeLabelManager().removeFromClusterNodeLabels(
          Arrays.asList("x", "y"));
    }
  }

  public static SchedulerApplication<SchedulerApplicationAttempt>
      verifyAppAddedAndRemovedFromScheduler(
          Map<ApplicationId, SchedulerApplication<SchedulerApplicationAttempt>> applications,
          EventHandler<SchedulerEvent> handler, String queueName)
          throws Exception {
    ApplicationId appId =
        ApplicationId.newInstance(System.currentTimeMillis(), 1);
    AppAddedSchedulerEvent appAddedEvent =
        new AppAddedSchedulerEvent(appId, queueName, "user");
    handler.handle(appAddedEvent);
    SchedulerApplication<SchedulerApplicationAttempt> app =
        applications.get(appId);
    // verify application is added.
    Assert.assertNotNull(app);
    Assert.assertEquals("user", app.getUser());

    AppRemovedSchedulerEvent appRemoveEvent =
        new AppRemovedSchedulerEvent(appId, RMAppState.FINISHED);
    handler.handle(appRemoveEvent);
    Assert.assertNull(applications.get(appId));
    return app;
  }
  
  private static RMContext getMockRMContext() {
    RMContext rmContext = mock(RMContext.class);
    RMNodeLabelsManager nlm = new NullRMNodeLabelsManager();
    nlm.init(new Configuration(false));
    when(rmContext.getNodeLabelManager()).thenReturn(nlm);
    return rmContext;
  }
}
