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

package org.apache.hadoop.yarn.server.resourcemanager.webapp;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import javax.ws.rs.core.MediaType;

import com.sun.jersey.api.json.JSONJAXBContext;
import com.sun.jersey.api.json.JSONMarshaller;
import org.apache.hadoop.http.JettyUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.NodeState;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceInformation;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.MockAM;
import org.apache.hadoop.yarn.server.resourcemanager.MockNM;
import org.apache.hadoop.yarn.server.resourcemanager.MockRMAppSubmissionData;
import org.apache.hadoop.yarn.server.resourcemanager.MockRMAppSubmitter;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.QueueMetrics;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeUpdateSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.ClusterScalingInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.NewNMCandidates;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.NodeInstanceType;
import org.apache.hadoop.yarn.server.api.records.NodeHealthStatus;
import org.apache.hadoop.yarn.server.api.records.NodeStatus;
import org.apache.hadoop.yarn.server.resourcemanager.MockRM;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNodeImpl;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNodeStatusEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerNodeReport;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.NodeInstanceTypeList;
import org.apache.hadoop.yarn.util.resource.DefaultResourceCalculator;
import org.apache.hadoop.yarn.util.resource.DominantResourceCalculator;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.apache.hadoop.yarn.webapp.GenericExceptionHandler;
import org.apache.hadoop.yarn.webapp.GuiceServletConfig;
import org.apache.hadoop.yarn.webapp.JerseyTestBase;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.inject.Guice;
import com.google.inject.servlet.ServletModule;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.guice.spi.container.servlet.GuiceContainer;
import com.sun.jersey.test.framework.WebAppDescriptor;

public class TestRMWebServicesNodesScaling extends JerseyTestBase {

  protected final int GB = 1024;

  private static MockRM rm;
  private static YarnConfiguration conf;

  private static String userName;

  private static class WebServletModule extends ServletModule {
    @Override
    protected void configureServlets() {
      bind(JAXBContextResolver.class);
      try {
        userName = UserGroupInformation.getCurrentUser().getShortUserName();
      } catch (IOException ioe) {
        throw new RuntimeException("Unable to get current user name "
            + ioe.getMessage(), ioe);
      }

      conf = new YarnConfiguration(setupMultiNodeLookupConfiguration());
      conf.set(YarnConfiguration.YARN_ADMIN_ACL, userName);
      bind(RMWebServices.class);
      bind(GenericExceptionHandler.class);
      rm = new MockRM(conf);
      rm.getRMContext().getContainerTokenSecretManager().rollMasterKey();
      rm.getRMContext().getNMTokenSecretManager().rollMasterKey();
      rm.disableDrainEventsImplicitly();
      bind(ResourceManager.class).toInstance(rm);
      serve("/*").with(GuiceContainer.class);
    }

    private CapacitySchedulerConfiguration setupMultiNodeLookupConfiguration() {
      String POLICY_CLASS_NAME =
          "org.apache.hadoop.yarn.server.resourcemanager.scheduler.placement." +
              "LayeredNodeUsageBinPackingPolicy";

      CapacitySchedulerConfiguration csConfig =
          new CapacitySchedulerConfiguration();
      csConfig.set(CapacitySchedulerConfiguration.RESOURCE_CALCULATOR_CLASS,
          DominantResourceCalculator.class.getName());
      CapacitySchedulerConfiguration csConf =
          new CapacitySchedulerConfiguration(csConfig);
      csConf.set(CapacitySchedulerConfiguration.RESOURCE_CALCULATOR_CLASS,
          DominantResourceCalculator.class.getName());
      csConf.set(CapacitySchedulerConfiguration.MULTI_NODE_PLACEMENT_ENABLED,
          "true");
      csConf.setClass(YarnConfiguration.RM_SCHEDULER, CapacityScheduler.class,
          ResourceScheduler.class);
      csConf.set(CapacitySchedulerConfiguration.MULTI_NODE_SORTING_POLICIES,
          "resource-based");
      csConf.set(CapacitySchedulerConfiguration.MULTI_NODE_SORTING_POLICY_NAME,
          "resource-based");
      String policyName =
          CapacitySchedulerConfiguration.MULTI_NODE_SORTING_POLICY_NAME
              + ".resource-based" + ".class";
      csConf.set(policyName, POLICY_CLASS_NAME);
      csConf.setBoolean(
          CapacitySchedulerConfiguration.MULTI_NODE_PLACEMENT_ENABLED,
          true);
      csConf.setInt("yarn.scheduler.minimum-allocation-mb", 512);
      csConf.setInt("yarn.scheduler.minimum-allocation-vcores", 1);
      csConf.setInt("yarn.scheduler.maximum-allocation-mb", 102400);
      csConf.setInt("yarn.scheduler.maximum-allocation-vcores", 100);
      return csConf;
    }

  }

  static {
    GuiceServletConfig.setInjector(
        Guice.createInjector(new WebServletModule()));
  }

  @Before
  @Override
  public void setUp() throws Exception {
    super.setUp();
    GuiceServletConfig.setInjector(
        Guice.createInjector(new WebServletModule()));
  }

  public TestRMWebServicesNodesScaling() {
    super(new WebAppDescriptor.Builder(
        "org.apache.hadoop.yarn.server.resourcemanager.webapp")
        .contextListenerClass(GuiceServletConfig.class)
        .filterClass(com.google.inject.servlet.GuiceFilter.class)
        .contextPath("jersey-guice-filter").servletPath("/").build());
  }

  protected void waitforNMRegistered(ResourceScheduler scheduler, int nodecount,
      int timesec) throws InterruptedException {
    long start = System.currentTimeMillis();
    while (System.currentTimeMillis() - start < timesec * 1000) {
      if (scheduler.getNumClusterNodes() < nodecount) {
        Thread.sleep(100);
      } else {
        break;
      }
    }
  }

  @SuppressWarnings("rawtypes")
  private String toJson(Object nsli, Class klass) throws Exception {
    StringWriter sw = new StringWriter();
    JSONJAXBContext ctx = new JSONJAXBContext(klass);
    JSONMarshaller jm = ctx.createJSONMarshaller();
    jm.marshallToJSON(nsli, sw);
    return sw.toString();
  }

  @Test
  public void testClusterScalingInfoJsonInvalidQuery() throws Exception {
    rm.start();
    ResourceScheduler scheduler = rm.getRMContext().getScheduler();
    MockNM[] nms = new MockNM[3];
    MockNM nm1 = rm.registerNode("127.0.0.1:1234", 10 * GB, 4);
    MockNM nm2 = rm.registerNode("127.0.0.2:1234", 10 * GB, 4);
    MockNM nm3 = rm.registerNode("127.0.0.3:1234", 10 * GB, 4);
    nms[0] = nm1;
    nms[1] = nm2;
    nms[2] = nm3;
    waitforNMRegistered(scheduler, 3, 5);

    NodeId id1 = nm1.getNodeId();
    rm.waitForState(id1, NodeState.RUNNING);

    NodeId id2 = nm2.getNodeId();
    rm.waitForState(id2, NodeState.RUNNING);

    NodeId id3 = nm3.getNodeId();
    rm.waitForState(id3, NodeState.RUNNING);

    assertEquals(scheduler.getNumClusterNodes(), 3);

    WebResource r = resource();
    NodeInstanceTypeList niTypeList = new NodeInstanceTypeList();
    niTypeList.getInstanceTypes().addAll(fakeInstanceTypes(2));
    String niTypeListStr = toJson(niTypeList, NodeInstanceTypeList.class);
    ClientResponse response = r.path("ws").path("v1").path("cluster")
        .path("scaling")
        .queryParam(RMWSConsts.UPSCALING_FACTOR_IN_NODE_RESOURCE_TYPES_KEY,
            ResourceInformation.MEMORY_URI)
        .header(RMWSConsts.SCALING_CUSTOM_HEADER_KEY,
            "v2")
        .entity(niTypeListStr, MediaType.APPLICATION_JSON)
        .accept("application/json").post(ClientResponse.class);

    assertEquals(MediaType.APPLICATION_JSON_TYPE + "; " + JettyUtils.UTF_8,
        response.getType().toString());

    // Case 1. Wrong api-version header
    JSONObject json = response.getEntity(JSONObject.class);
    String msg = json.getJSONObject("RemoteException")
        .getString("message");
    assertEquals("Exception message should relates to versioning",
        true, msg.contains("v2 is not supported"));

    // Case 2. No api-version header specified is ok
    r = resource();
    response = r.path("ws").path("v1").path("cluster")
        .path("scaling")
        .queryParam(RMWSConsts.UPSCALING_FACTOR_IN_NODE_RESOURCE_TYPES_KEY,
            ResourceInformation.MEMORY_URI)
        .entity(niTypeListStr, MediaType.APPLICATION_JSON)
        .accept("application/json").post(ClientResponse.class);
    json = response.getEntity(JSONObject.class);
    assertEquals("incorrect number of elements", 5, json.length());

    // Case 3. No memory-mb resource type specified is ok
    r = resource();
    response = r.path("ws").path("v1").path("cluster")
        .path("scaling")
        .entity(niTypeListStr, MediaType.APPLICATION_JSON)
        .accept("application/json").post(ClientResponse.class);
    json = response.getEntity(JSONObject.class);
    assertEquals("incorrect number of elements", 5, json.length());

    // Case 4. No node instance types specified fails
    r = resource();
    response = r.path("ws").path("v1").path("cluster")
        .path("scaling")
        .accept("application/json").post(ClientResponse.class);
    assertEquals(500, response.getStatusInfo().getStatusCode());

    // Case 5. Specified wrong string for node instance types
    r = resource();
    response = r.path("ws").path("v1").path("cluster")
        .path("scaling")
        .queryParam(RMWSConsts.UPSCALING_FACTOR_IN_NODE_RESOURCE_TYPES_KEY,
            ResourceInformation.MEMORY_URI)
        .entity(niTypeListStr.substring(0, niTypeListStr.length() - 3),
            MediaType.APPLICATION_JSON)
        .accept("application/json").post(ClientResponse.class);
    assertEquals(400, response.getStatusInfo().getStatusCode());

    rm.stop();
  }

  @Test
  public void testClusterScalingInfoJson() throws JSONException, Exception {
    rm.start();
    ResourceScheduler scheduler = rm.getRMContext().getScheduler();
    MockNM[] nms = new MockNM[3];
    MockNM nm1 = rm.registerNode("127.0.0.1:1234", 10 * GB, 4);
    MockNM nm2 = rm.registerNode("127.0.0.2:1234", 10 * GB, 4);
    MockNM nm3 = rm.registerNode("127.0.0.3:1234", 10 * GB, 4);
    nms[0] = nm1;
    nms[1] = nm2;
    nms[2] = nm3;
    waitforNMRegistered(scheduler, 3, 5);

    WebResource r = resource();
    NodeInstanceTypeList niTypeList = new NodeInstanceTypeList();
    niTypeList.getInstanceTypes().addAll(fakeInstanceTypes(1));
    String niTypeListStr = toJson(niTypeList, NodeInstanceTypeList.class);
    ClientResponse response = r.path("ws").path("v1").path("cluster")
        .path("scaling")
        .queryParam(RMWSConsts.UPSCALING_FACTOR_IN_NODE_RESOURCE_TYPES_KEY,
            ResourceInformation.MEMORY_URI)
        .header(RMWSConsts.SCALING_CUSTOM_HEADER_KEY,
            RMWSConsts.SCALING_CUSTOM_HEADER_VERSION_V1)
        .entity(niTypeListStr, MediaType.APPLICATION_JSON)
        .accept("application/json").post(ClientResponse.class);

    assertEquals(MediaType.APPLICATION_JSON_TYPE + "; " + JettyUtils.UTF_8,
        response.getType().toString());

    JSONObject json = response.getEntity(JSONObject.class);
    assertEquals("incorrect number of elements", 5, json.length());
    JSONObject newCandidates = json.getJSONObject("newNMCandidates");

    // 0 nodes needs for upscaling
    assertTrue("Should have no upscaling info.",
        !newCandidates.has("candidates"));
    // 3 nodes can be decommissioned
    JSONObject decommissionCandidates =
        json.getJSONObject("decommissionCandidates");
    JSONArray cnds = decommissionCandidates.getJSONArray("candidates");
    assertEquals(3, cnds.length());

    NodeId id1 = nm1.getNodeId();
    rm.waitForState(id1, NodeState.RUNNING);

    NodeId id2 = nm2.getNodeId();
    rm.waitForState(id2, NodeState.RUNNING);

    NodeId id3 = nm3.getNodeId();
    rm.waitForState(id3, NodeState.RUNNING);

    assertEquals(scheduler.getNumClusterNodes(), 3);

    MockRMAppSubmissionData data1 =
        MockRMAppSubmissionData.Builder.createWithMemory(2 * GB, rm)
            .withAppName("app-1")
            .withUser("user1")
            .withAcls(null)
            .withQueue("default")
            .build();
    RMApp app1 = MockRMAppSubmitter.submit(rm, data1);
    MockAM am1 = MockRM.launchAndRegisterAM(app1, rm, nms);

    Resource cResource = Resources.createResource(2 * GB, 1);
    am1.allocate("*", cResource, 1,
        new ArrayList<ContainerId>(), null);

    heartbeat(rm, nm1);
    heartbeat(rm, nm2);
    heartbeat(rm, nm3);
    // Get the first chosen node
    MockNM firstChosenNode = null;
    int firstChosenNodeIndex = 0;
    for (int i = 0; i < nms.length; i++) {
      SchedulerNodeReport reportNM =
          rm.getResourceScheduler().getNodeReport(nms[i].getNodeId());
      if (reportNM.getUsedResource().getMemorySize() == 4 * GB) {
        firstChosenNode = nms[i];
        firstChosenNodeIndex = i;
        break;
      }
    }

    SchedulerNodeReport reportNm1 =
        rm.getResourceScheduler().getNodeReport(firstChosenNode.getNodeId());
    // check node report
    Assert.assertEquals(4 * GB,
        reportNm1.getUsedResource().getMemorySize());
    Assert.assertEquals((10 - 4) * GB,
        reportNm1.getAvailableResource().getMemorySize());

    // report status
    RMNodeImpl node1 =
        (RMNodeImpl) rm.getRMContext().getRMNodes()
            .get(firstChosenNode.getNodeId());

    NodeStatus nodeStatus = createNodeStatus(
        nms[firstChosenNodeIndex].getNodeId(), app1, 2);
    node1.handle(new RMNodeStatusEvent(nm1.getNodeId(), nodeStatus));
    Assert.assertEquals(1, node1.getRunningApps().size());

    // One will be pending.
    cResource = Resources.createResource(9 * GB, 4);
    am1.allocate("*", cResource, 3,
        new ArrayList<ContainerId>(), null);
    heartbeat(rm, nm1);
    heartbeat(rm, nm2);
    heartbeat(rm, nm3);

    List<FiCaSchedulerNode> allNodes = ((CapacityScheduler)
        rm.getResourceScheduler()).getAllNodes();
    for (FiCaSchedulerNode node : allNodes) {
      if (node.getNodeID().equals(nms[firstChosenNodeIndex].getNodeId())) {
        assertEquals(node.getCopiedListOfRunningContainers().size(), 2);
        assertEquals(node.getAllocatedResource().getMemorySize(), 4 * GB);
      } else {
        assertEquals(node.getCopiedListOfRunningContainers().size(), 1);
        assertEquals(node.getAllocatedResource().getMemorySize(), 9 * GB);
        nodeStatus = createNodeStatus(
            node.getNodeID(), app1, 1);
        ((RMNodeImpl)node.getRMNode()).handle(new RMNodeStatusEvent(nm1.getNodeId(), nodeStatus));
        Assert.assertEquals(1, node1.getRunningApps().size());
      }
    }

    r = resource();
    response = r.path("ws").path("v1").path("cluster")
        .path("scaling")
        .queryParam(RMWSConsts.UPSCALING_FACTOR_IN_NODE_RESOURCE_TYPES_KEY,
            ResourceInformation.MEMORY_URI)
        .header(RMWSConsts.SCALING_CUSTOM_HEADER_KEY,
            RMWSConsts.SCALING_CUSTOM_HEADER_VERSION_V1)
        .entity(niTypeListStr, MediaType.APPLICATION_JSON)
        .accept("application/json").post(ClientResponse.class);

    assertEquals(MediaType.APPLICATION_JSON_TYPE + "; " + JettyUtils.UTF_8,
        response.getType().toString());

    json = response.getEntity(JSONObject.class);
    assertEquals("incorrect number of elements", 5, json.length());
    newCandidates = json.getJSONObject("newNMCandidates");
    JSONArray newNodesArray = newCandidates.getJSONArray("candidates");
    assertEquals(1, newNodesArray.length());
    // 1 new node recommendation
    assertEquals("Incorrect upscaling node count", "1",
        newNodesArray.getJSONObject(0).getString("count"));
    assertEquals("Incorrect upscaling node type", "m5.xlarge",
        newNodesArray.getJSONObject(0).getString("modelName"));
    // 2 nodes can be decommissioned
    decommissionCandidates =
        json.getJSONObject("decommissionCandidates");
    assertTrue("Should be empty of downscalign",
        !decommissionCandidates.has("candidates"));

    rm.stop();
  }

  private List<NodeInstanceType> fakeInstanceTypes(int count) {
    List<NodeInstanceType> types = new ArrayList<>();
    Resource r1 = Resource.newInstance(16 * GB, 4);
    types.add(new NodeInstanceType("m5.xlarge", r1));

    if (count > 1) {
      Resource r2 = Resource.newInstance(2 * GB, 2);
      types.add(new NodeInstanceType("a1.medium", r2));
    }
    return types;
  }

  @Test
  public void testClusterUpscalingRecommendationAlgorithm() {
    ResourceCalculator rc = new DefaultResourceCalculator();
    Resource cResource = Resources.createResource(10 * GB, 1);
    Resource cResource2 = Resources.createResource(1 * GB, 2);

    Map<Resource, Integer> containerAskToCount =
        new TreeMap<>(new Comparator<Resource>() {
      private int getRealLength(ResourceInformation[] ris) {
        int ret = 0;
        for (ResourceInformation ri : ris) {
          if (ri.getValue() != 0) {
            ret++;
          }
        }
        return ret;
      }
      @Override
      public int compare(Resource o1, Resource o2) {
        int realLength1 = getRealLength(o1.getResources());
        int realLength2 = getRealLength(o2.getResources());
        if (realLength1 > realLength2) {
          return -1;
        } else if (realLength1 < realLength2){
          return 1;
        } else {
          return o2.compareTo(o1);
        }
      }
    });

    // Case 1. One 10 GB request, One 2 GB request. One m5.xlarge should serve
    containerAskToCount.put(cResource, 1);
    containerAskToCount.put(cResource2, 1);

    // m5.xlarge with 16 GB, a1.medium with 2GB
    List<NodeInstanceType> allTypes = fakeInstanceTypes(2);
    NewNMCandidates newNMCandidates = new NewNMCandidates();
    ClusterScalingInfo.recommendNewInstances(containerAskToCount,
        newNMCandidates, allTypes, rc);
    assertEquals("Incorrect upscaling node count", 1,
        newNMCandidates.getCandidates().size());
    assertEquals("Incorrect upscaling node type", "m5.xlarge",
        newNMCandidates.getCandidates().get(0).getModelName());

    // Case 2. Three 1 GB, should be served by 2 a1.medium
    containerAskToCount.clear();
    containerAskToCount.put(cResource2, 3);
    newNMCandidates = new NewNMCandidates();
    ClusterScalingInfo.recommendNewInstances(containerAskToCount,
        newNMCandidates, allTypes, rc);
    assertEquals("Incorrect upscaling node count", 2,
        newNMCandidates.getCandidates().get(0).getCount());
    assertEquals("Incorrect upscaling node type", "a1.medium",
        newNMCandidates.getCandidates().get(0).getModelName());
  }

  @Test
  public void testContainerAskToCountWontLeakMemory() {
    QueueMetrics metrics = rm.getResourceScheduler().getRootQueueMetrics();
    Resource cResource = Resources.createResource(10 * GB, 1);
    Resource cResource2 = Resources.createResource(1 * GB, 2);
    Resource cResource3 = Resources.createResource(3 * GB, 2);
    String partition = null;
    // Case 1.
    metrics.incrPendingResources(partition, userName, 2, cResource2);
    int count = metrics.getContainerAskToCount().get(cResource2);
    assertEquals("There should be 2 container pending", true,
        count == 2);
    metrics.decrPendingResources(partition, userName, 2, cResource2);
    // Case 2.
    int i = 10000;
    while (i-- > 0) {
      metrics.incrPendingResources(partition, userName, 1, cResource);
      metrics.decrPendingResources(partition, userName, 1, cResource);
      metrics.incrPendingResources(partition, userName, 2, cResource2);
      metrics.incrPendingResources(partition, userName, 3, cResource3);
      metrics.decrPendingResources(partition, userName, 3, cResource3);
      metrics.decrPendingResources(partition, userName, 2, cResource2);
    }

    assertEquals("There should be no container pending", true,
        metrics.getContainerAskToCount().size() == 0);



  }

  private void heartbeat(MockRM rm, MockNM nm) {
    RMNode node = rm.getRMContext().getRMNodes().get(nm.getNodeId());
    // Send a heartbeat to kick the tires on the Scheduler
    NodeUpdateSchedulerEvent nodeUpdate = new NodeUpdateSchedulerEvent(node);
    rm.getResourceScheduler().handle(nodeUpdate);
  }

  private NodeStatus createNodeStatus(
      NodeId nodeId, RMApp app, int numRunningContainers) {
    return NodeStatus.newInstance(
        nodeId, 0, getContainerStatuses(app, numRunningContainers),
        Collections.emptyList(),
        NodeHealthStatus.newInstance(
            true,  "", System.currentTimeMillis() - 1000),
        null, null, null);
  }

  // Get mocked ContainerStatus for bunch of containers,
  // where numRunningContainers are RUNNING.
  private List<ContainerStatus> getContainerStatuses(
      RMApp app, int numRunningContainers) {
    // Total 3 containers
    final int total = 3;
    numRunningContainers = Math.min(total, numRunningContainers);
    List<ContainerStatus> output = new ArrayList<ContainerStatus>();
    for (int i = 0; i < total; i++) {
      ContainerState cstate = (i >= numRunningContainers)?
          ContainerState.COMPLETE : ContainerState.RUNNING;
      output.add(ContainerStatus.newInstance(
          ContainerId.newContainerId(
              ApplicationAttemptId.newInstance(app.getApplicationId(), 0), i),
          cstate, "", 0));
    }
    return output;
  }

}
