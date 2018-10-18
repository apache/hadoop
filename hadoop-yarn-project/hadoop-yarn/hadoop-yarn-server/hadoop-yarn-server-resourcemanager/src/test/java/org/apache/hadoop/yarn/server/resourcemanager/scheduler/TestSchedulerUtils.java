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

import static org.apache.hadoop.yarn.exceptions
        .InvalidResourceRequestException.InvalidResourceType
        .GREATER_THEN_MAX_ALLOCATION;
import static org.apache.hadoop.yarn.exceptions
        .InvalidResourceRequestException.InvalidResourceType.LESS_THAN_ZERO;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.security.PrivilegedAction;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.ImmutableMap;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.yarn.LocalConfigurationProvider;
import org.apache.hadoop.yarn.api.ApplicationMasterProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateRequest;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterRequest;
import org.apache.hadoop.yarn.api.records.ApplicationAccessType;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerExitStatus;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.NodeLabel;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.QueueInfo;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceBlacklistRequest;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.api.records.impl.pb.ResourceRequestPBImpl;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.exceptions.InvalidLabelResourceRequestException;
import org.apache.hadoop.yarn.exceptions.InvalidResourceBlacklistRequestException;
import org.apache.hadoop.yarn.exceptions.InvalidResourceRequestException;
import org.apache.hadoop.yarn.exceptions.InvalidResourceRequestException
        .InvalidResourceType;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.yarn.nodelabels.CommonNodeLabelsManager;
import org.apache.hadoop.yarn.resourcetypes.ResourceTypesTestHelper;
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
import org.apache.hadoop.yarn.util.resource.ResourceUtils;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import org.junit.rules.ExpectedException;
import org.mockito.Mockito;

public class TestSchedulerUtils {

  private static final Log LOG = LogFactory.getLog(TestSchedulerUtils.class);
  private static Resource configuredMaxAllocation;

  private static class CustomResourceTypesConfigurationProvider
          extends LocalConfigurationProvider {

    @Override
    public InputStream getConfigurationInputStream(Configuration bootstrapConf,
            String name) throws YarnException, IOException {
      if (YarnConfiguration.RESOURCE_TYPES_CONFIGURATION_FILE.equals(name)) {
        return new ByteArrayInputStream(
                ("<configuration>\n" +
                        " <property>\n" +
                        "   <name>yarn.resource-types</name>\n" +
                        "   <value>custom-resource-1," +
                        "custom-resource-2,custom-resource-3</value>\n" +
                        " </property>\n" +
                        " <property>\n" +
                        "   <name>yarn.resource-types" +
                        ".custom-resource-1.units</name>\n" +
                        "   <value>G</value>\n" +
                        " </property>\n" +
                        " <property>\n" +
                        "   <name>yarn.resource-types" +
                        ".custom-resource-2.units</name>\n" +
                        "   <value>G</value>\n" +
                        " </property>\n" +
                        "</configuration>\n").getBytes());
      } else {
        return super.getConfigurationInputStream(bootstrapConf, name);
      }
    }
  }
  private RMContext rmContext = getMockRMContext();

  private static YarnConfiguration conf = new YarnConfiguration();

  @Rule
  public ExpectedException exception = ExpectedException.none();

  private void initResourceTypes() {
    Configuration yarnConf = new Configuration();
    yarnConf.set(YarnConfiguration.RM_CONFIGURATION_PROVIDER_CLASS,
            CustomResourceTypesConfigurationProvider.class.getName());
    ResourceUtils.resetResourceTypes(yarnConf);
  }

  @Before
  public void setUp() {
    initResourceTypes();
    //this needs to be initialized after initResourceTypes is called
    configuredMaxAllocation = Resource.newInstance(8192, 4,
            ImmutableMap.<String,
                    Long>builder()
                    .put("custom-resource-1", Long.MAX_VALUE)
                    .put("custom-resource-2", Long.MAX_VALUE)
                    .put("custom-resource-3", Long.MAX_VALUE)
                    .build());
  }

  @Test(timeout = 30000)
  public void testNormalizeRequest() {
    ResourceCalculator resourceCalculator = new DefaultResourceCalculator();

    final int minMemory = 1024;
    final int maxMemory = 8192;
    Resource minResource = Resources.createResource(minMemory, 0);
    Resource maxResource = Resources.createResource(maxMemory, 0);

    ResourceRequest ask = new ResourceRequestPBImpl();

    // case negative memory
    ask.setCapability(Resources.createResource(-1024));
    SchedulerUtils.normalizeRequest(ask, resourceCalculator, minResource,
            maxResource);
    assertEquals(minMemory, ask.getCapability().getMemorySize());

    // case zero memory
    ask.setCapability(Resources.createResource(0));
    SchedulerUtils.normalizeRequest(ask, resourceCalculator, minResource,
            maxResource);
    assertEquals(minMemory, ask.getCapability().getMemorySize());

    // case memory is a multiple of minMemory
    ask.setCapability(Resources.createResource(2 * minMemory));
    SchedulerUtils.normalizeRequest(ask, resourceCalculator, minResource,
            maxResource);
    assertEquals(2 * minMemory, ask.getCapability().getMemorySize());

    // case memory is not a multiple of minMemory
    ask.setCapability(Resources.createResource(minMemory + 10));
    SchedulerUtils.normalizeRequest(ask, resourceCalculator, minResource,
            maxResource);
    assertEquals(2 * minMemory, ask.getCapability().getMemorySize());

    // case memory is equal to max allowed
    ask.setCapability(Resources.createResource(maxMemory));
    SchedulerUtils.normalizeRequest(ask, resourceCalculator, minResource,
            maxResource);
    assertEquals(maxMemory, ask.getCapability().getMemorySize());

    // case memory is just less than max
    ask.setCapability(Resources.createResource(maxMemory - 10));
    SchedulerUtils.normalizeRequest(ask, resourceCalculator, minResource,
            maxResource);
    assertEquals(maxMemory, ask.getCapability().getMemorySize());

    // max is not a multiple of min
    maxResource = Resources.createResource(maxMemory - 10, 0);
    ask.setCapability(Resources.createResource(maxMemory - 100));
    // multiple of minMemory > maxMemory, then reduce to maxMemory
    SchedulerUtils.normalizeRequest(ask, resourceCalculator, minResource,
            maxResource);
    assertEquals(maxResource.getMemorySize(),
            ask.getCapability().getMemorySize());

    // ask is more than max
    maxResource = Resources.createResource(maxMemory, 0);
    ask.setCapability(Resources.createResource(maxMemory + 100));
    SchedulerUtils.normalizeRequest(ask, resourceCalculator, minResource,
            maxResource);
    assertEquals(maxResource.getMemorySize(),
            ask.getCapability().getMemorySize());
  }

  @Test(timeout = 30000)
  public void testNormalizeRequestWithDominantResourceCalculator() {
    ResourceCalculator resourceCalculator = new DominantResourceCalculator();

    Resource minResource = Resources.createResource(1024, 1);
    Resource maxResource = Resources.createResource(10240, 10);
    Resource clusterResource = Resources.createResource(10 * 1024, 10);

    ResourceRequest ask = new ResourceRequestPBImpl();

    // case negative memory/vcores
    ask.setCapability(Resources.createResource(-1024, -1));
    SchedulerUtils.normalizeRequest(
            ask, resourceCalculator, minResource, maxResource);
    assertEquals(minResource, ask.getCapability());

    // case zero memory/vcores
    ask.setCapability(Resources.createResource(0, 0));
    SchedulerUtils.normalizeRequest(
            ask, resourceCalculator, minResource, maxResource);
    assertEquals(minResource, ask.getCapability());
    assertEquals(1, ask.getCapability().getVirtualCores());
    assertEquals(1024, ask.getCapability().getMemorySize());

    // case non-zero memory & zero cores
    ask.setCapability(Resources.createResource(1536, 0));
    SchedulerUtils.normalizeRequest(
            ask, resourceCalculator, minResource, maxResource);
    assertEquals(Resources.createResource(2048, 1), ask.getCapability());
    assertEquals(1, ask.getCapability().getVirtualCores());
    assertEquals(2048, ask.getCapability().getMemorySize());
  }

  @Test(timeout = 30000)
  public void testValidateResourceRequestWithErrorLabelsPermission()
          throws IOException {
    // mock queue and scheduler
    ResourceScheduler scheduler = mock(ResourceScheduler.class);
    Set<String> queueAccessibleNodeLabels = Sets.newHashSet();
    QueueInfo queueInfo = mock(QueueInfo.class);
    when(queueInfo.getQueueName()).thenReturn("queue");
    when(queueInfo.getAccessibleNodeLabels())
            .thenReturn(queueAccessibleNodeLabels);
    when(scheduler.getQueueInfo(any(String.class), anyBoolean(), anyBoolean()))
            .thenReturn(queueInfo);

    when(rmContext.getScheduler()).thenReturn(scheduler);

    Resource maxResource = Resources.createResource(
            YarnConfiguration.DEFAULT_RM_SCHEDULER_MAXIMUM_ALLOCATION_MB,
            YarnConfiguration.DEFAULT_RM_SCHEDULER_MAXIMUM_ALLOCATION_VCORES);

    // queue has labels, success cases
    try {
      // set queue accessible node labesl to [x, y]
      queueAccessibleNodeLabels.clear();
      queueAccessibleNodeLabels.addAll(Arrays.asList("x", "y"));
      rmContext.getNodeLabelManager().addToCluserNodeLabels(
              ImmutableSet.of(NodeLabel.newInstance("x"),
                      NodeLabel.newInstance("y")));
      Resource resource = Resources.createResource(
              0,
              YarnConfiguration.DEFAULT_RM_SCHEDULER_MINIMUM_ALLOCATION_VCORES);
      ResourceRequest resReq = BuilderUtils.newResourceRequest(
              mock(Priority.class), ResourceRequest.ANY, resource, 1);
      resReq.setNodeLabelExpression("x");
      normalizeAndvalidateRequest(resReq, "queue",
              scheduler, rmContext, maxResource);

      resReq.setNodeLabelExpression("y");
      normalizeAndvalidateRequest(resReq, "queue",
              scheduler, rmContext, maxResource);

      resReq.setNodeLabelExpression("");
      normalizeAndvalidateRequest(resReq, "queue",
              scheduler, rmContext, maxResource);

      resReq.setNodeLabelExpression(" ");
      normalizeAndvalidateRequest(resReq, "queue",
              scheduler, rmContext, maxResource);
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
      normalizeAndvalidateRequest(resReq, "queue",
              scheduler, rmContext, maxResource);

      fail("Should fail");
    } catch (InvalidResourceRequestException e) {
    }

    // queue has labels, failed cases (when ask a label not included by queue)
    try {
      // set queue accessible node labesl to [x, y]
      queueAccessibleNodeLabels.clear();
      queueAccessibleNodeLabels.addAll(Arrays.asList("x", "y"));
      rmContext.getNodeLabelManager().addToCluserNodeLabels(
              ImmutableSet.of(NodeLabel.newInstance("x"),
                      NodeLabel.newInstance("y")));

      Resource resource = Resources.createResource(
              0,
              YarnConfiguration.DEFAULT_RM_SCHEDULER_MINIMUM_ALLOCATION_VCORES);
      ResourceRequest resReq = BuilderUtils.newResourceRequest(
              mock(Priority.class), ResourceRequest.ANY, resource, 1);
      resReq.setNodeLabelExpression("z");
      normalizeAndvalidateRequest(resReq, "queue",
              scheduler, rmContext, maxResource);
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
              ImmutableSet.of(NodeLabel.newInstance("x"),
                      NodeLabel.newInstance("y")));

      Resource resource = Resources.createResource(
              0,
              YarnConfiguration.DEFAULT_RM_SCHEDULER_MINIMUM_ALLOCATION_VCORES);
      ResourceRequest resReq = BuilderUtils.newResourceRequest(
              mock(Priority.class), ResourceRequest.ANY, resource, 1);
      resReq.setNodeLabelExpression("x && y");
      normalizeAndvalidateRequest(resReq, "queue",
              scheduler, rmContext, maxResource);
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
      normalizeAndvalidateRequest(resReq, "queue",
              scheduler, rmContext, maxResource);

      resReq.setNodeLabelExpression("");
      normalizeAndvalidateRequest(resReq, "queue",
              scheduler, rmContext, maxResource);

      resReq.setNodeLabelExpression("  ");
      normalizeAndvalidateRequest(resReq, "queue",
              scheduler, rmContext, maxResource);
    } catch (InvalidResourceRequestException e) {
      e.printStackTrace();
      fail("Should be valid when request labels is empty");
    }
    boolean invalidlabelexception = false;
    // queue doesn't have label, failed (when request any label)
    try {
      // set queue accessible node labels to empty
      queueAccessibleNodeLabels.clear();

      rmContext.getNodeLabelManager().addToCluserNodeLabels(
              ImmutableSet.of(NodeLabel.newInstance("x")));

      Resource resource = Resources.createResource(
              0,
              YarnConfiguration.DEFAULT_RM_SCHEDULER_MINIMUM_ALLOCATION_VCORES);
      ResourceRequest resReq = BuilderUtils.newResourceRequest(
              mock(Priority.class), ResourceRequest.ANY, resource, 1);
      resReq.setNodeLabelExpression("x");
      normalizeAndvalidateRequest(resReq, "queue",
              scheduler, rmContext, maxResource);
      fail("Should fail");
    } catch (InvalidLabelResourceRequestException e) {
      invalidlabelexception = true;
    } catch (InvalidResourceRequestException e) {
    } finally {
      rmContext.getNodeLabelManager().removeFromClusterNodeLabels(
              Arrays.asList("x"));
    }
    Assert.assertTrue("InvalidLabelResourceRequestException expected",
            invalidlabelexception);
    // queue is "*", always succeeded
    try {
      // set queue accessible node labels to empty
      queueAccessibleNodeLabels.clear();
      queueAccessibleNodeLabels.add(RMNodeLabelsManager.ANY);

      rmContext.getNodeLabelManager().addToCluserNodeLabels(
              ImmutableSet.of(NodeLabel.newInstance("x"),
                      NodeLabel.newInstance("y"), NodeLabel.newInstance("z")));

      Resource resource = Resources.createResource(
              0,
              YarnConfiguration.DEFAULT_RM_SCHEDULER_MINIMUM_ALLOCATION_VCORES);
      ResourceRequest resReq = BuilderUtils.newResourceRequest(
              mock(Priority.class), ResourceRequest.ANY, resource, 1);
      resReq.setNodeLabelExpression("x");
      normalizeAndvalidateRequest(resReq, "queue",
              scheduler, rmContext, maxResource);

      resReq.setNodeLabelExpression("y");
      normalizeAndvalidateRequest(resReq, "queue",
              scheduler, rmContext, maxResource);

      resReq.setNodeLabelExpression("z");
      normalizeAndvalidateRequest(resReq, "queue",
              scheduler, rmContext, maxResource);
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
      normalizeAndvalidateRequest(resReq, "queue",
              scheduler, rmContext, maxResource);
      fail("Should fail");
    } catch (InvalidResourceRequestException e) {
    }

    // we don't allow resource name other than ANY and specify label
    try {
      // set queue accessible node labesl to [x, y]
      queueAccessibleNodeLabels.clear();
      queueAccessibleNodeLabels.addAll(Arrays.asList("x", "y"));
      rmContext.getNodeLabelManager().addToCluserNodeLabels(
              ImmutableSet.of(NodeLabel.newInstance("x"),
                      NodeLabel.newInstance("y")));

      Resource resource = Resources.createResource(
              0,
              YarnConfiguration.DEFAULT_RM_SCHEDULER_MINIMUM_ALLOCATION_VCORES);
      ResourceRequest resReq = BuilderUtils.newResourceRequest(
              mock(Priority.class), "rack", resource, 1);
      resReq.setNodeLabelExpression("x");
      normalizeAndvalidateRequest(resReq, "queue",
              scheduler, rmContext, maxResource);
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
              ImmutableSet.of(NodeLabel.newInstance("x")));

      Resource resource = Resources.createResource(
              0,
              YarnConfiguration.DEFAULT_RM_SCHEDULER_MINIMUM_ALLOCATION_VCORES);
      ResourceRequest resReq = BuilderUtils.newResourceRequest(
              mock(Priority.class), "rack", resource, 1);
      resReq.setNodeLabelExpression("x");
      normalizeAndvalidateRequest(resReq, "queue",
              scheduler, rmContext, maxResource);
      fail("Should fail");
    } catch (InvalidResourceRequestException e) {
    } finally {
      rmContext.getNodeLabelManager().removeFromClusterNodeLabels(
              Arrays.asList("x"));
    }
    try {
      Resource resource = Resources.createResource(0,
              YarnConfiguration.DEFAULT_RM_SCHEDULER_MINIMUM_ALLOCATION_VCORES);
      ResourceRequest resReq1 = BuilderUtils
              .newResourceRequest(mock(Priority.class), "*", resource, 1, "x");
      normalizeAndvalidateRequest(resReq1, "queue",
              scheduler, rmContext, maxResource);
      fail("Should fail");
    } catch (InvalidResourceRequestException e) {
      assertEquals("Invalid label resource request, cluster do not contain , "
              + "label= x", e.getMessage());
    }

    try {
      rmContext.getYarnConfiguration()
              .set(YarnConfiguration.NODE_LABELS_ENABLED, "false");
      Resource resource = Resources.createResource(0,
              YarnConfiguration.DEFAULT_RM_SCHEDULER_MINIMUM_ALLOCATION_VCORES);
      ResourceRequest resReq1 = BuilderUtils
              .newResourceRequest(mock(Priority.class), "*", resource, 1, "x");
      normalizeAndvalidateRequest(resReq1, "queue",
              scheduler, rmContext, maxResource);
      Assert.assertEquals(RMNodeLabelsManager.NO_LABEL,
              resReq1.getNodeLabelExpression());
    } catch (InvalidResourceRequestException e) {
      assertEquals("Invalid resource request, node label not enabled but "
              + "request contains label expression", e.getMessage());
    }
  }

  @Test(timeout = 30000)
  public void testValidateResourceRequest() throws IOException {
    ResourceScheduler mockScheduler = mock(ResourceScheduler.class);

    QueueInfo queueInfo = mock(QueueInfo.class);
    when(queueInfo.getQueueName()).thenReturn("queue");

    Resource maxResource =
            Resources.createResource(
                    YarnConfiguration.DEFAULT_RM_SCHEDULER_MAXIMUM_ALLOCATION_MB,
                    YarnConfiguration.DEFAULT_RM_SCHEDULER_MAXIMUM_ALLOCATION_VCORES);

    when(rmContext.getScheduler()).thenReturn(mockScheduler);
    when(mockScheduler.getQueueInfo(Mockito.anyString(), Mockito.anyBoolean(),
        Mockito.anyBoolean())).thenReturn(queueInfo);

    // zero memory
    try {
      Resource resource =
              Resources.createResource(0,
                      YarnConfiguration.DEFAULT_RM_SCHEDULER_MINIMUM_ALLOCATION_VCORES);
      ResourceRequest resReq =
              BuilderUtils.newResourceRequest(mock(Priority.class),
                      ResourceRequest.ANY, resource, 1);
      normalizeAndvalidateRequest(resReq, null,
              mockScheduler, rmContext, maxResource);
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
      normalizeAndvalidateRequest(resReq, null,
              mockScheduler, rmContext, maxResource);
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
      normalizeAndvalidateRequest(resReq, null,
              mockScheduler, rmContext, maxResource);
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
      normalizeAndvalidateRequest(resReq, null,
              mockScheduler, rmContext, maxResource);
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
      normalizeAndvalidateRequest(resReq, null,
              mockScheduler, rmContext, maxResource);
      fail("Negative memory should not be accepted");
    } catch (InvalidResourceRequestException e) {
      assertEquals(LESS_THAN_ZERO, e.getInvalidResourceType());
    }

    // negative vcores
    try {
      Resource resource =
              Resources.createResource(
                      YarnConfiguration.DEFAULT_RM_SCHEDULER_MINIMUM_ALLOCATION_MB, -1);
      ResourceRequest resReq =
              BuilderUtils.newResourceRequest(mock(Priority.class),
                      ResourceRequest.ANY, resource, 1);
      normalizeAndvalidateRequest(resReq, null,
              mockScheduler, rmContext, maxResource);
      fail("Negative vcores should not be accepted");
    } catch (InvalidResourceRequestException e) {
      assertEquals(LESS_THAN_ZERO, e.getInvalidResourceType());
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
      normalizeAndvalidateRequest(resReq, null,
              mockScheduler, rmContext, maxResource);
      fail("More than max memory should not be accepted");
    } catch (InvalidResourceRequestException e) {
      assertEquals(GREATER_THEN_MAX_ALLOCATION, e.getInvalidResourceType());
    }

    // more than max vcores
    try {
      Resource resource = Resources.createResource(
              YarnConfiguration.DEFAULT_RM_SCHEDULER_MINIMUM_ALLOCATION_MB,
              YarnConfiguration.DEFAULT_RM_SCHEDULER_MAXIMUM_ALLOCATION_VCORES + 1);
      ResourceRequest resReq =
              BuilderUtils.newResourceRequest(mock(Priority.class),
                      ResourceRequest.ANY, resource, 1);
      normalizeAndvalidateRequest(resReq, null,
              mockScheduler, rmContext, maxResource);
      fail("More than max vcores should not be accepted");
    } catch (InvalidResourceRequestException e) {
      assertEquals(GREATER_THEN_MAX_ALLOCATION, e.getInvalidResourceType());
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
    final Configuration yarnConf = rm.getConfig();
    final YarnRPC rpc = YarnRPC.create(yarnConf);

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
                        ApplicationMasterProtocol.class, rmBindAddress, yarnConf);
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
  public void testComparePriorities() {
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

  @Test(timeout = 30000)
  public void testNormalizeNodeLabelExpression()
          throws IOException {
    // mock queue and scheduler
    ResourceScheduler scheduler = mock(ResourceScheduler.class);
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

    when(rmContext.getScheduler()).thenReturn(scheduler);

    // queue has labels, success cases
    try {
      // set queue accessible node labels to [x, y]
      queueAccessibleNodeLabels.clear();
      queueAccessibleNodeLabels.addAll(Arrays.asList("x", "y"));
      rmContext.getNodeLabelManager().addToCluserNodeLabels(
              ImmutableSet.of(NodeLabel.newInstance("x"),
                      NodeLabel.newInstance("y")));
      Resource resource = Resources.createResource(
              0,
              YarnConfiguration.DEFAULT_RM_SCHEDULER_MINIMUM_ALLOCATION_VCORES);
      ResourceRequest resReq = BuilderUtils.newResourceRequest(
              mock(Priority.class), ResourceRequest.ANY, resource, 1);
      normalizeAndvalidateRequest(resReq, "queue",
              scheduler, rmContext, maxResource);
      Assert.assertEquals("x", resReq.getNodeLabelExpression());

      resReq.setNodeLabelExpression(" y ");
      normalizeAndvalidateRequest(resReq, "queue",
              scheduler, rmContext, maxResource);
      Assert.assertEquals("y", resReq.getNodeLabelExpression());
    } catch (InvalidResourceRequestException e) {
      e.printStackTrace();
      fail("Should be valid when request labels is a subset of queue labels");
    } finally {
      rmContext.getNodeLabelManager().removeFromClusterNodeLabels(
              Arrays.asList("x", "y"));
    }
  }

  @Test
  public void testCustomResourceRequestedUnitIsSmallerThanAvailableUnit()
          throws InvalidResourceRequestException {
    Resource requestedResource =
            ResourceTypesTestHelper.newResource(1, 1,
                    ImmutableMap.of("custom-resource-1", "11"));

    Resource availableResource =
            ResourceTypesTestHelper.newResource(1, 1,
                    ImmutableMap.of("custom-resource-1", "0G"));

    exception.expect(InvalidResourceRequestException.class);
    exception.expectMessage(InvalidResourceRequestExceptionMessageGenerator
            .create().withRequestedResourceType("custom-resource-1")
            .withRequestedResource(requestedResource)
            .withAvailableAllocation(availableResource)
            .withMaxAllocation(configuredMaxAllocation)
            .withInvalidResourceType(GREATER_THEN_MAX_ALLOCATION)
            .build());

    SchedulerUtils.checkResourceRequestAgainstAvailableResource(
            requestedResource, availableResource);
  }

  @Test
  public void testCustomResourceRequestedUnitIsSmallerThanAvailableUnit2() {
    Resource requestedResource =
            ResourceTypesTestHelper.newResource(1, 1,
                    ImmutableMap.of("custom-resource-1", "11"));

    Resource availableResource =
            ResourceTypesTestHelper.newResource(1, 1,
                    ImmutableMap.of("custom-resource-1", "1G"));

    try {
      SchedulerUtils.checkResourceRequestAgainstAvailableResource(
              requestedResource, availableResource);
    } catch (InvalidResourceRequestException e) {
      fail(String.format(
              "Resource request should be accepted. Requested: %s, available: %s",
              requestedResource, availableResource));
    }
  }

  @Test
  public void testCustomResourceRequestedUnitIsGreaterThanAvailableUnit()
          throws InvalidResourceRequestException {
    Resource requestedResource =
            ResourceTypesTestHelper.newResource(1, 1,
                    ImmutableMap.of("custom-resource-1", "1M"));

    Resource availableResource = ResourceTypesTestHelper.newResource(1, 1,
            ImmutableMap.<String, String>builder().put("custom-resource-1",
                    "120k")
                    .build());

    exception.expect(InvalidResourceRequestException.class);
    exception.expectMessage(InvalidResourceRequestExceptionMessageGenerator
            .create().withRequestedResourceType("custom-resource-1")
            .withRequestedResource(requestedResource)
            .withAvailableAllocation(availableResource)
            .withMaxAllocation(configuredMaxAllocation)
            .withInvalidResourceType(GREATER_THEN_MAX_ALLOCATION)
            .build());
    SchedulerUtils.checkResourceRequestAgainstAvailableResource(
            requestedResource, availableResource);
  }

  @Test
  public void testCustomResourceRequestedUnitIsGreaterThanAvailableUnit2() {
    Resource requestedResource = ResourceTypesTestHelper.newResource(1, 1,
            ImmutableMap.<String, String>builder().put("custom-resource-1", "11M")
                    .build());

    Resource availableResource =
            ResourceTypesTestHelper.newResource(1, 1,
                    ImmutableMap.of("custom-resource-1", "1G"));

    try {
      SchedulerUtils.checkResourceRequestAgainstAvailableResource(
              requestedResource, availableResource);
    } catch (InvalidResourceRequestException e) {
      fail(String.format(
              "Resource request should be accepted. Requested: %s, available: %s",
              requestedResource, availableResource));
    }
  }

  @Test
  public void testCustomResourceRequestedUnitIsSameAsAvailableUnit() {
    Resource requestedResource = ResourceTypesTestHelper.newResource(1, 1,
            ImmutableMap.of("custom-resource-1", "11M"));

    Resource availableResource = ResourceTypesTestHelper.newResource(1, 1,
            ImmutableMap.of("custom-resource-1", "100M"));

    try {
      SchedulerUtils.checkResourceRequestAgainstAvailableResource(
              requestedResource, availableResource);
    } catch (InvalidResourceRequestException e) {
      fail(String.format(
              "Resource request should be accepted. Requested: %s, available: %s",
              requestedResource, availableResource));
    }
  }

  @Test
  public void testCustomResourceRequestedUnitIsSameAsAvailableUnit2()
          throws InvalidResourceRequestException {
    Resource requestedResource = ResourceTypesTestHelper.newResource(1, 1,
            ImmutableMap.of("custom-resource-1", "110M"));

    Resource availableResource = ResourceTypesTestHelper.newResource(1, 1,
            ImmutableMap.of("custom-resource-1", "100M"));

    exception.expect(InvalidResourceRequestException.class);
    exception.expectMessage(InvalidResourceRequestExceptionMessageGenerator
            .create().withRequestedResourceType("custom-resource-1")
            .withRequestedResource(requestedResource)
            .withAvailableAllocation(availableResource)
            .withInvalidResourceType(GREATER_THEN_MAX_ALLOCATION)
            .withMaxAllocation(configuredMaxAllocation)
            .build());

    SchedulerUtils.checkResourceRequestAgainstAvailableResource(
            requestedResource, availableResource);
  }

  public static void waitSchedulerApplicationAttemptStopped(
          AbstractYarnScheduler ys,
          ApplicationAttemptId attemptId) throws InterruptedException {
    SchedulerApplicationAttempt schedulerApp =
            ys.getApplicationAttempt(attemptId);
    if (null == schedulerApp) {
      return;
    }

    // Wait at most 5 secs to make sure SchedulerApplicationAttempt stopped
    int tick = 0;
    while (tick < 100) {
      if (schedulerApp.isStopped()) {
        return;
      }
      tick++;
      Thread.sleep(50);
    }

    // Only print, don't throw exception
    System.err.println("Failed to wait scheduler application attempt stopped.");
  }

  public static SchedulerApplication<SchedulerApplicationAttempt>
  verifyAppAddedAndRemovedFromScheduler(
          Map<ApplicationId, SchedulerApplication<SchedulerApplicationAttempt>> applications,
          EventHandler<SchedulerEvent> handler, String queueName) {

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
    when(rmContext.getYarnConfiguration()).thenReturn(conf);
    rmContext.getYarnConfiguration().set(YarnConfiguration.NODE_LABELS_ENABLED,
            "true");
    when(rmContext.getNodeLabelManager()).thenReturn(nlm);
    return rmContext;
  }

  private static void normalizeAndvalidateRequest(ResourceRequest resReq,
      String queueName, YarnScheduler scheduler, RMContext rmContext,
      Resource maxAllocation)
      throws InvalidResourceRequestException {
    SchedulerUtils.normalizeAndValidateRequest(resReq, maxAllocation, queueName,
        scheduler, rmContext, null);
  }

  private static class InvalidResourceRequestExceptionMessageGenerator {

    private StringBuilder sb;
    private Resource requestedResource;
    private Resource availableAllocation;
    private Resource configuredMaxAllowedAllocation;
    private String resourceType;
    private InvalidResourceType invalidResourceType;

    InvalidResourceRequestExceptionMessageGenerator(StringBuilder sb) {
      this.sb = sb;
    }

    public static InvalidResourceRequestExceptionMessageGenerator create() {
      return new InvalidResourceRequestExceptionMessageGenerator(
              new StringBuilder());
    }

    InvalidResourceRequestExceptionMessageGenerator withRequestedResource(
            Resource r) {
      this.requestedResource = r;
      return this;
    }

    InvalidResourceRequestExceptionMessageGenerator withRequestedResourceType(
            String rt) {
      this.resourceType = rt;
      return this;
    }

    InvalidResourceRequestExceptionMessageGenerator withAvailableAllocation(
            Resource r) {
      this.availableAllocation = r;
      return this;
    }

    InvalidResourceRequestExceptionMessageGenerator withMaxAllocation(
            Resource r) {
      this.configuredMaxAllowedAllocation = r;
      return this;
    }

    InvalidResourceRequestExceptionMessageGenerator
    withInvalidResourceType(InvalidResourceType invalidResourceType) {
      this.invalidResourceType = invalidResourceType;
      return this;
    }

    public String build() {
      if (invalidResourceType == LESS_THAN_ZERO) {
        return sb.append("Invalid resource request! " +
                "Cannot allocate containers as " +
                "requested resource is less than 0! ")
                .append("Requested resource type=[")
                .append(resourceType).append("]")
                .append(", Requested resource=")
                .append(requestedResource).toString();

      } else if (invalidResourceType == GREATER_THEN_MAX_ALLOCATION) {
        return sb.append("Invalid resource request! " +
                "Cannot allocate containers as "
                + "requested resource is greater than " +
                "maximum allowed allocation. ")
                .append("Requested resource type=[").append(resourceType)
                .append("], ")
                .append("Requested resource=").append(requestedResource)
                .append(", maximum allowed allocation=")
                .append(availableAllocation)
                .append(", please note that maximum allowed allocation is " +
                        "calculated by scheduler based on maximum resource " +
                        "of registered NodeManagers, which might be less " +
                        "than configured maximum allocation=")
                .append(configuredMaxAllowedAllocation)
                .toString();
      }
      throw new IllegalStateException("Wrong type of InvalidResourceType is " +
              "detected!");
    }
  }
}
