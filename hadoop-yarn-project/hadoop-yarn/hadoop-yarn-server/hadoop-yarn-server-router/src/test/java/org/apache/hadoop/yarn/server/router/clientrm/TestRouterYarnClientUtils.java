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

package org.apache.hadoop.yarn.server.router.clientrm;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.Map;
import java.util.HashMap;
import java.util.HashSet;

import org.apache.commons.collections.CollectionUtils;
import org.apache.hadoop.thirdparty.com.google.common.collect.ImmutableSet;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationsResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetClusterMetricsResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetNodesToLabelsResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetClusterNodeLabelsResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetLabelsToNodesResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetQueueUserAclsInfoResponse;
import org.apache.hadoop.yarn.api.protocolrecords.ReservationListResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetAllResourceTypeInfoResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetResourceProfileResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetAllResourceProfilesResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetAttributesToNodesResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetClusterNodeAttributesResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetNodesToAttributesResponse;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ApplicationResourceUsageReport;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.api.records.YarnClusterMetrics;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.NodeLabel;
import org.apache.hadoop.yarn.api.records.QueueACL;
import org.apache.hadoop.yarn.api.records.QueueUserACLInfo;
import org.apache.hadoop.yarn.api.records.ReservationAllocationState;
import org.apache.hadoop.yarn.api.records.ReservationDefinition;
import org.apache.hadoop.yarn.api.records.ReservationId;
import org.apache.hadoop.yarn.api.records.ResourceTypeInfo;
import org.apache.hadoop.yarn.api.records.NodeAttribute;
import org.apache.hadoop.yarn.api.records.NodeAttributeType;
import org.apache.hadoop.yarn.api.records.NodeAttributeKey;
import org.apache.hadoop.yarn.api.records.NodeToAttributeValue;
import org.apache.hadoop.yarn.api.records.NodeAttributeInfo;
import org.apache.hadoop.yarn.util.Records;
import org.apache.hadoop.yarn.server.uam.UnmanagedApplicationManager;
import org.junit.Assert;
import org.junit.Test;

/**
 * Test class for RouterYarnClientUtils.
 */
public class TestRouterYarnClientUtils {

  private final static String PARTIAL_REPORT = "Partial Report ";

  @Test
  public void testClusterMetricsMerge() {
    ArrayList<GetClusterMetricsResponse> responses = new ArrayList<>();
    responses.add(getClusterMetricsResponse(1));
    responses.add(getClusterMetricsResponse(2));
    GetClusterMetricsResponse result = RouterYarnClientUtils.merge(responses);
    YarnClusterMetrics resultMetrics = result.getClusterMetrics();
    Assert.assertEquals(3, resultMetrics.getNumNodeManagers());
    Assert.assertEquals(3, resultMetrics.getNumActiveNodeManagers());
    Assert.assertEquals(3, resultMetrics.getNumDecommissioningNodeManagers());
    Assert.assertEquals(3, resultMetrics.getNumDecommissionedNodeManagers());
    Assert.assertEquals(3, resultMetrics.getNumLostNodeManagers());
    Assert.assertEquals(3, resultMetrics.getNumRebootedNodeManagers());
    Assert.assertEquals(3, resultMetrics.getNumUnhealthyNodeManagers());
    Assert.assertEquals(3, resultMetrics.getNumShutdownNodeManagers());
  }

  public GetClusterMetricsResponse getClusterMetricsResponse(int value) {
    YarnClusterMetrics metrics = YarnClusterMetrics.newInstance(value);
    metrics.setNumUnhealthyNodeManagers(value);
    metrics.setNumRebootedNodeManagers(value);
    metrics.setNumLostNodeManagers(value);
    metrics.setNumDecommissioningNodeManagers(value);
    metrics.setNumDecommissionedNodeManagers(value);
    metrics.setNumActiveNodeManagers(value);
    metrics.setNumNodeManagers(value);
    metrics.setNumShutdownNodeManagers(value);
    return GetClusterMetricsResponse.newInstance(metrics);
  }

  /**
   * This test validates the correctness of
   * RouterYarnClientUtils#mergeApplications.
   */
  @Test
  public void testMergeApplications() {
    ArrayList<GetApplicationsResponse> responses = new ArrayList<>();
    responses.add(getApplicationsResponse(1, false));
    responses.add(getApplicationsResponse(2, false));
    GetApplicationsResponse result = RouterYarnClientUtils.
        mergeApplications(responses, false);
    Assert.assertNotNull(result);
    Assert.assertEquals(2, result.getApplicationList().size());

    String appName1 = result.getApplicationList().get(0).getName();
    String appName2 = result.getApplicationList().get(1).getName();

    // Check that no Unmanaged applications are added to the result
    Assert.assertEquals(false,
        appName1.contains(UnmanagedApplicationManager.APP_NAME));
    Assert.assertEquals(false,
        appName2.contains(UnmanagedApplicationManager.APP_NAME));
  }

  /**
   * This test validates the correctness of
   * RouterYarnClientUtils#mergeApplications.
   */
  @Test
  public void testMergeUnmanagedApplications() {
    ArrayList<GetApplicationsResponse> responses = new ArrayList<>();
    responses.add(getApplicationsResponse(1, true));

    // Check response if partial results are enabled
    GetApplicationsResponse result = RouterYarnClientUtils.
        mergeApplications(responses, true);
    Assert.assertNotNull(result);
    Assert.assertEquals(1, result.getApplicationList().size());
    ApplicationReport appReport = result.getApplicationList().iterator().next();
    String appName = appReport.getName();
    Assert.assertTrue(appName.startsWith(PARTIAL_REPORT));

    // Check ApplicationResourceUsageReport merge
    ApplicationResourceUsageReport resourceUsageReport =
        appReport.getApplicationResourceUsageReport();

    Assert.assertEquals(2, resourceUsageReport.getNumUsedContainers());
    Assert.assertEquals(4, resourceUsageReport.getNumReservedContainers());

    // Check response if partial results are disabled
    result = RouterYarnClientUtils.
        mergeApplications(responses, false);
    Assert.assertNotNull(result);
    Assert.assertTrue(result.getApplicationList().isEmpty());
  }

  /**
   * This test validates the correctness of
   * RouterYarnClientUtils#mergeApplications when
   * ApplicationResourceUsageReport might be null.
   */
  @Test
  public void testMergeApplicationsNullResourceUsage() {
    ApplicationId appId = ApplicationId.newInstance(1234, 1);
    ApplicationReport appReport = ApplicationReport.newInstance(
        appId, ApplicationAttemptId.newInstance(appId, 1),
        "user", "queue", "app1", "host",
        124, null, YarnApplicationState.RUNNING,
        "diagnostics", "url", 0, 0,
        0, FinalApplicationStatus.SUCCEEDED, null, "N/A",
        0.53789f, "YARN", null, null, false, null, null, null);

    ApplicationReport uamAppReport = ApplicationReport.newInstance(
        appId, ApplicationAttemptId.newInstance(appId, 1),
        "user", "queue", "app1", "host",
        124, null, YarnApplicationState.RUNNING,
        "diagnostics", "url", 0, 0,
        0, FinalApplicationStatus.SUCCEEDED, null, "N/A",
        0.53789f, "YARN", null, null, true, null, null, null);


    ArrayList<GetApplicationsResponse> responses = new ArrayList<>();
    List<ApplicationReport> applications = new ArrayList<>();
    applications.add(appReport);
    applications.add(uamAppReport);
    responses.add(GetApplicationsResponse.newInstance(applications));

    GetApplicationsResponse result = RouterYarnClientUtils.
        mergeApplications(responses, false);
    Assert.assertNotNull(result);
    Assert.assertEquals(1, result.getApplicationList().size());

    String appName = result.getApplicationList().get(0).getName();

    // Check that no Unmanaged applications are added to the result
    Assert.assertFalse(appName.contains(UnmanagedApplicationManager.APP_NAME));
  }

  /**
   * This generates a GetApplicationsResponse with 2 applications with
   * same ApplicationId.
   * @param value Used as Id in ApplicationId
   * @param uamOnly If set to true, only unmanaged applications are added in
   *                response, else one managed and one unmanaged applications
   *                are added with same ApplicationId.
   * @return GetApplicationsResponse
   */
  private GetApplicationsResponse getApplicationsResponse(int value,
      boolean uamOnly) {
    String appName = uamOnly? UnmanagedApplicationManager.APP_NAME: "appname";
    List<ApplicationReport> applications = new ArrayList<>();

    // Create first application report. This is a managed app by default.
    // If uamOnly is true, this becomes unmanaged application.
    ApplicationId appId = ApplicationId.newInstance(1234, value);
    Resource resource = Resource.newInstance(1024, 1);
    ApplicationResourceUsageReport appResourceUsageReport =
        ApplicationResourceUsageReport.newInstance(
            1, 2, resource, resource,
            resource, null, 0.1f,
            0.1f, null);

    ApplicationReport appReport = ApplicationReport.newInstance(
        appId, ApplicationAttemptId.newInstance(appId, 1),
        "user", "queue", appName, "host",
        124, null, YarnApplicationState.RUNNING,
        "diagnostics", "url", 0, 0,
        0, FinalApplicationStatus.SUCCEEDED, appResourceUsageReport, "N/A",
        0.53789f, "YARN", null, null, uamOnly, null, null, null);

    // Create second application report. This is always unmanaged application.
    ApplicationId appId2 = ApplicationId.newInstance(1234, value);
    ApplicationReport appReport2 = ApplicationReport.newInstance(
        appId2, ApplicationAttemptId.newInstance(appId, 1),
        "user", "queue", UnmanagedApplicationManager.APP_NAME, "host",
        124, null, YarnApplicationState.RUNNING,
        "diagnostics", "url", 0, 0,
        0, FinalApplicationStatus.SUCCEEDED, appResourceUsageReport, "N/A",
        0.53789f, "YARN", null, null, true, null, null, null);

    applications.add(appReport);
    applications.add(appReport2);

    return GetApplicationsResponse.newInstance(applications);
  }

  @Test
  public void testMergeNodesToLabelsResponse() {
    NodeId node1 = NodeId.fromString("SubCluster1Node1:1111");
    NodeId node2 = NodeId.fromString("SubCluster1Node2:2222");
    NodeId node3 = NodeId.fromString("SubCluster2Node1:1111");

    Map<NodeId, Set<String>> nodeLabelsMapSC1 = new HashMap<>();
    nodeLabelsMapSC1.put(node1, ImmutableSet.of("node1"));
    nodeLabelsMapSC1.put(node2, ImmutableSet.of("node2"));
    nodeLabelsMapSC1.put(node3, ImmutableSet.of("node3"));

    // normal response
    GetNodesToLabelsResponse response1 = Records.newRecord(
        GetNodesToLabelsResponse.class);
    response1.setNodeToLabels(nodeLabelsMapSC1);

    // empty response
    Map<NodeId, Set<String>> nodeLabelsMapSC2 = new HashMap<>();
    GetNodesToLabelsResponse response2 = Records.newRecord(
        GetNodesToLabelsResponse.class);
    response2.setNodeToLabels(nodeLabelsMapSC2);

    // null response
    GetNodesToLabelsResponse response3 = null;

    Map<NodeId, Set<String>> expectedResponse = new HashMap<>();
    expectedResponse.put(node1, ImmutableSet.of("node1"));
    expectedResponse.put(node2, ImmutableSet.of("node2"));
    expectedResponse.put(node3, ImmutableSet.of("node3"));

    List<GetNodesToLabelsResponse> responses = new ArrayList<>();
    responses.add(response1);
    responses.add(response2);
    responses.add(response3);

    GetNodesToLabelsResponse response = RouterYarnClientUtils.
        mergeNodesToLabelsResponse(responses);
    Assert.assertEquals(expectedResponse, response.getNodeToLabels());
  }

  @Test
  public void testMergeClusterNodeLabelsResponse() {
    NodeLabel nodeLabel1 = NodeLabel.newInstance("nodeLabel1");
    NodeLabel nodeLabel2 = NodeLabel.newInstance("nodeLabel2");
    NodeLabel nodeLabel3 = NodeLabel.newInstance("nodeLabel3");

    // normal response
    List<NodeLabel> nodeLabelListSC1 = new ArrayList<>();
    nodeLabelListSC1.add(nodeLabel1);
    nodeLabelListSC1.add(nodeLabel2);
    nodeLabelListSC1.add(nodeLabel3);

    GetClusterNodeLabelsResponse response1 = Records.newRecord(
        GetClusterNodeLabelsResponse.class);
    response1.setNodeLabelList(nodeLabelListSC1);

    // empty response
    List<NodeLabel> nodeLabelListSC2 = new ArrayList<>();

    GetClusterNodeLabelsResponse response2 = Records.newRecord(
        GetClusterNodeLabelsResponse.class);
    response2.setNodeLabelList(nodeLabelListSC2);

    // null response
    GetClusterNodeLabelsResponse response3 = null;

    List<GetClusterNodeLabelsResponse> responses = new ArrayList<>();
    responses.add(response1);
    responses.add(response2);
    responses.add(response3);

    List<NodeLabel> expectedResponse = new ArrayList<>();
    expectedResponse.add(nodeLabel1);
    expectedResponse.add(nodeLabel2);
    expectedResponse.add(nodeLabel3);

    GetClusterNodeLabelsResponse response = RouterYarnClientUtils.
        mergeClusterNodeLabelsResponse(responses);
    Assert.assertTrue(CollectionUtils.isEqualCollection(expectedResponse,
        response.getNodeLabelList()));
  }

  @Test
  public void testMergeLabelsToNodes(){
    NodeId node1 = NodeId.fromString("SubCluster1Node1:1111");
    NodeId node2 = NodeId.fromString("SubCluster1Node2:2222");
    NodeId node3 = NodeId.fromString("SubCluster2node1:1111");
    NodeId node4 = NodeId.fromString("SubCluster2node2:2222");

    Map<String, Set<NodeId>> labelsToNodesSC1 = new HashMap<>();

    Set<NodeId> nodeIdSet1 = new HashSet<>();
    nodeIdSet1.add(node1);
    nodeIdSet1.add(node2);
    labelsToNodesSC1.put("Label1", nodeIdSet1);

    // normal response
    GetLabelsToNodesResponse response1 = Records.newRecord(
        GetLabelsToNodesResponse.class);
    response1.setLabelsToNodes(labelsToNodesSC1);
    Map<String, Set<NodeId>> labelsToNodesSC2 = new HashMap<>();
    Set<NodeId> nodeIdSet2 = new HashSet<>();
    nodeIdSet2.add(node3);
    Set<NodeId> nodeIdSet3 = new HashSet<>();
    nodeIdSet3.add(node4);
    labelsToNodesSC2.put("Label1", nodeIdSet2);
    labelsToNodesSC2.put("Label2", nodeIdSet3);

    GetLabelsToNodesResponse response2 = Records.newRecord(
        GetLabelsToNodesResponse.class);
    response2.setLabelsToNodes(labelsToNodesSC2);

    // empty response
    GetLabelsToNodesResponse response3 = Records.newRecord(
        GetLabelsToNodesResponse.class);

    // null response
    GetLabelsToNodesResponse response4 = null;

    List<GetLabelsToNodesResponse> responses = new ArrayList<>();
    responses.add(response1);
    responses.add(response2);
    responses.add(response3);
    responses.add(response4);

    Map<String, Set<NodeId>> expectedResponse = new HashMap<>();
    Set<NodeId> nodeIdMergedSet1 = new HashSet<>();
    nodeIdMergedSet1.add(node1);
    nodeIdMergedSet1.add(node2);
    nodeIdMergedSet1.add(node3);

    Set<NodeId> nodeIdMergedSet2 = new HashSet<>();
    nodeIdMergedSet2.add(node4);
    expectedResponse.put("Label1", nodeIdMergedSet1);
    expectedResponse.put("Label2", nodeIdMergedSet2);

    GetLabelsToNodesResponse response = RouterYarnClientUtils.
        mergeLabelsToNodes(responses);

    Assert.assertEquals(expectedResponse, response.getLabelsToNodes());
  }

  @Test
  public void testMergeQueueUserAclsResponse() {

    List<QueueACL> submitOnlyAcl = new ArrayList<>();
    submitOnlyAcl.add(QueueACL.SUBMIT_APPLICATIONS);

    List<QueueACL> administerOnlyAcl = new ArrayList<>();
    administerOnlyAcl.add(QueueACL.ADMINISTER_QUEUE);

    List<QueueACL> submitAndAdministerAcl = new ArrayList<>();
    submitAndAdministerAcl.add(QueueACL.ADMINISTER_QUEUE);
    submitAndAdministerAcl.add(QueueACL.SUBMIT_APPLICATIONS);

    QueueUserACLInfo queueUserACLInfo1 = QueueUserACLInfo.newInstance(
        "root", submitAndAdministerAcl);

    QueueUserACLInfo queueUserACLInfo2 = QueueUserACLInfo.newInstance(
        "default", submitOnlyAcl);

    QueueUserACLInfo queueUserACLInfo3 = QueueUserACLInfo.newInstance(
        "root", submitAndAdministerAcl);

    QueueUserACLInfo queueUserACLInfo4 = QueueUserACLInfo.newInstance(
        "yarn", administerOnlyAcl);

    List<QueueUserACLInfo> queueUserACLInfoList1 = new ArrayList<>();
    List<QueueUserACLInfo> queueUserACLInfoList2 = new ArrayList<>();

    queueUserACLInfoList1.add(queueUserACLInfo1);
    queueUserACLInfoList1.add(queueUserACLInfo2);
    queueUserACLInfoList2.add(queueUserACLInfo3);
    queueUserACLInfoList2.add(queueUserACLInfo4);

    // normal response
    GetQueueUserAclsInfoResponse response1 = Records.newRecord(
        GetQueueUserAclsInfoResponse.class);
    response1.setUserAclsInfoList(queueUserACLInfoList1);
    GetQueueUserAclsInfoResponse response2 = Records.newRecord(
        GetQueueUserAclsInfoResponse.class);
    response2.setUserAclsInfoList(queueUserACLInfoList2);

    // empty response
    GetQueueUserAclsInfoResponse response3 = Records.newRecord(
        GetQueueUserAclsInfoResponse.class);

    // null response
    GetQueueUserAclsInfoResponse response4 = null;

    List<GetQueueUserAclsInfoResponse> responses = new ArrayList<>();
    responses.add(response1);
    responses.add(response2);
    responses.add(response3);
    responses.add(response4);

    // expected user acls
    List<QueueUserACLInfo> expectedOutput = new ArrayList<>();
    expectedOutput.add(queueUserACLInfo1);
    expectedOutput.add(queueUserACLInfo2);
    expectedOutput.add(queueUserACLInfo4);

    GetQueueUserAclsInfoResponse response =
        RouterYarnClientUtils.mergeQueueUserAcls(responses);
    Assert.assertTrue(CollectionUtils.isEqualCollection(expectedOutput,
        response.getUserAclsInfoList()));
  }

  @Test
  public void testMergeReservationsList() {

    // normal response
    ReservationListResponse response1 = createReservationListResponse(
        165348678000L, 165348690000L, 165348678000L, 1L);

    ReservationListResponse response2 = createReservationListResponse(
        165348750000L, 165348768000L, 165348750000L, 1L);

    // empty response
    ReservationListResponse response3 = ReservationListResponse.newInstance(new ArrayList<>());

    // null response
    ReservationListResponse response4 = null;

    List<ReservationListResponse> responses = new ArrayList<>();
    responses.add(response1);
    responses.add(response2);
    responses.add(response3);
    responses.add(response4);

    // expected response
    List<ReservationAllocationState> expectedResponse = new ArrayList<>();
    expectedResponse.addAll(response1.getReservationAllocationState());
    expectedResponse.addAll(response2.getReservationAllocationState());

    ReservationListResponse response =
        RouterYarnClientUtils.mergeReservationsList(responses);
    Assert.assertEquals(expectedResponse, response.getReservationAllocationState());
  }

  private ReservationListResponse createReservationListResponse(long startTime,
      long endTime, long reservationTime, long reservationNumber) {
    List<ReservationAllocationState> reservationsList = new ArrayList<>();
    ReservationDefinition reservationDefinition =
        Records.newRecord(ReservationDefinition.class);
    reservationDefinition.setArrival(startTime);
    reservationDefinition.setDeadline(endTime);
    ReservationAllocationState reservationAllocationState =
        Records.newRecord(ReservationAllocationState.class);
    ReservationId reservationId = ReservationId.newInstance(reservationTime,
        reservationNumber);
    reservationAllocationState.setReservationDefinition(reservationDefinition);
    reservationAllocationState.setReservationId(reservationId);
    reservationsList.add(reservationAllocationState);
    return ReservationListResponse.newInstance(reservationsList);
  }

  @Test
  public void testMergeResourceTypes() {

    ResourceTypeInfo resourceTypeInfo1 = ResourceTypeInfo.newInstance("vcores");
    ResourceTypeInfo resourceTypeInfo2 = ResourceTypeInfo.newInstance("gpu");
    ResourceTypeInfo resourceTypeInfo3 = ResourceTypeInfo.newInstance("memory-mb");

    List<ResourceTypeInfo> resourceTypeInfoList1 = new ArrayList<>();
    resourceTypeInfoList1.add(resourceTypeInfo1);
    resourceTypeInfoList1.add(resourceTypeInfo3);

    List<ResourceTypeInfo> resourceTypeInfoList2 = new ArrayList<>();
    resourceTypeInfoList2.add(resourceTypeInfo3);
    resourceTypeInfoList2.add(resourceTypeInfo2);

    // normal response
    GetAllResourceTypeInfoResponse response1 =
        Records.newRecord(GetAllResourceTypeInfoResponse.class);
    response1.setResourceTypeInfo(resourceTypeInfoList1);

    GetAllResourceTypeInfoResponse response2 =
        Records.newRecord(GetAllResourceTypeInfoResponse.class);
    response2.setResourceTypeInfo(resourceTypeInfoList2);

    // empty response
    GetAllResourceTypeInfoResponse response3 =
        Records.newRecord(GetAllResourceTypeInfoResponse.class);

    // null response
    GetAllResourceTypeInfoResponse response4 = null;

    List<GetAllResourceTypeInfoResponse> responses = new ArrayList<>();
    responses.add(response1);
    responses.add(response2);
    responses.add(response3);
    responses.add(response4);

    // expected response
    List<ResourceTypeInfo> expectedResponse = new ArrayList<>();
    expectedResponse.add(resourceTypeInfo1);
    expectedResponse.add(resourceTypeInfo2);
    expectedResponse.add(resourceTypeInfo3);
    GetAllResourceTypeInfoResponse response =
        RouterYarnClientUtils.mergeResourceTypes(responses);
    Assert.assertTrue(CollectionUtils.isEqualCollection(expectedResponse,
        response.getResourceTypeInfo()));
  }

  @Test
  public void testMergeResourceProfiles() {
    // normal response1
    Map<String, Resource> profiles = new HashMap<>();
    Resource resource1 = Resource.newInstance(1024, 1);
    GetAllResourceProfilesResponse response1 = GetAllResourceProfilesResponse.newInstance();
    profiles.put("maximum", resource1);
    response1.setResourceProfiles(profiles);

    // normal response2
    profiles = new HashMap<>();
    Resource resource2 = Resource.newInstance(2048, 2);
    GetAllResourceProfilesResponse response2 = GetAllResourceProfilesResponse.newInstance();
    profiles.put("maximum", resource2);
    response2.setResourceProfiles(profiles);

    // empty response
    GetAllResourceProfilesResponse response3 = GetAllResourceProfilesResponse.newInstance();

    // null response
    GetAllResourceProfilesResponse response4 = null;

    List<GetAllResourceProfilesResponse> responses = new ArrayList<>();
    responses.add(response1);
    responses.add(response2);
    responses.add(response3);
    responses.add(response4);

    GetAllResourceProfilesResponse response =
        RouterYarnClientUtils.mergeClusterResourceProfilesResponse(responses);
    Resource resource = response.getResourceProfiles().get("maximum");
    Assert.assertEquals(3, resource.getVirtualCores());
    Assert.assertEquals(3072, resource.getMemorySize());
  }

  @Test
  public void testMergeResourceProfile() {
    // normal response1
    Resource resource1 = Resource.newInstance(1024, 1);
    GetResourceProfileResponse response1 =
        Records.newRecord(GetResourceProfileResponse.class);
    response1.setResource(resource1);

    // normal response2
    Resource resource2 = Resource.newInstance(2048, 2);
    GetResourceProfileResponse response2 =
        Records.newRecord(GetResourceProfileResponse.class);
    response2.setResource(resource2);

    // empty response
    GetResourceProfileResponse response3 =
        Records.newRecord(GetResourceProfileResponse.class);

    // null response
    GetResourceProfileResponse response4 = null;

    List<GetResourceProfileResponse> responses = new ArrayList<>();
    responses.add(response1);
    responses.add(response2);
    responses.add(response3);
    responses.add(response4);

    GetResourceProfileResponse response =
        RouterYarnClientUtils.mergeClusterResourceProfileResponse(responses);
    Resource resource = response.getResource();
    Assert.assertEquals(3, resource.getVirtualCores());
    Assert.assertEquals(3072, resource.getMemorySize());
  }

  @Test
  public void testMergeAttributesToNodesResponse() {
    // normal response1
    NodeAttribute gpu = NodeAttribute.newInstance(NodeAttribute.PREFIX_CENTRALIZED, "GPU",
        NodeAttributeType.STRING, "nvidia");
    Map<NodeAttributeKey, List<NodeToAttributeValue>> map1 = new HashMap<>();
    List<NodeToAttributeValue> lists1 = new ArrayList<>();
    NodeToAttributeValue attributeValue1 =
        NodeToAttributeValue.newInstance("node1", gpu.getAttributeValue());
    lists1.add(attributeValue1);
    map1.put(gpu.getAttributeKey(), lists1);
    GetAttributesToNodesResponse response1 = GetAttributesToNodesResponse.newInstance(map1);

    // normal response2
    NodeAttribute docker = NodeAttribute.newInstance(NodeAttribute.PREFIX_DISTRIBUTED, "DOCKER",
        NodeAttributeType.STRING, "docker0");
    Map<NodeAttributeKey, List<NodeToAttributeValue>> map2 = new HashMap<>();
    List<NodeToAttributeValue> lists2 = new ArrayList<>();
    NodeToAttributeValue attributeValue2 =
        NodeToAttributeValue.newInstance("node2", docker.getAttributeValue());
    lists2.add(attributeValue2);
    map2.put(docker.getAttributeKey(), lists2);
    GetAttributesToNodesResponse response2 = GetAttributesToNodesResponse.newInstance(map2);

    // empty response3
    GetAttributesToNodesResponse response3 =
        GetAttributesToNodesResponse.newInstance(new HashMap<>());

    // null response4
    GetAttributesToNodesResponse response4 = null;

    List<GetAttributesToNodesResponse> responses = new ArrayList<>();
    responses.add(response1);
    responses.add(response2);
    responses.add(response3);
    responses.add(response4);

    GetAttributesToNodesResponse response =
        RouterYarnClientUtils.mergeAttributesToNodesResponse(responses);

    Assert.assertNotNull(response);
    Assert.assertEquals(2, response.getAttributesToNodes().size());

    Map<NodeAttributeKey, List<NodeToAttributeValue>> attrs = response.getAttributesToNodes();

    NodeAttributeKey gpuKey = gpu.getAttributeKey();
    Assert.assertEquals(attributeValue1.toString(), attrs.get(gpuKey).get(0).toString());

    NodeAttributeKey dockerKey = docker.getAttributeKey();
    Assert.assertEquals(attributeValue2.toString(), attrs.get(dockerKey).get(0).toString());
  }

  @Test
  public void testMergeClusterNodeAttributesResponse() {
    // normal response1
    NodeAttributeInfo nodeAttributeInfo1 =
        NodeAttributeInfo.newInstance(NodeAttributeKey.newInstance("GPU"),
        NodeAttributeType.STRING);
    Set<NodeAttributeInfo> attributes1 = new HashSet<>();
    attributes1.add(nodeAttributeInfo1);
    GetClusterNodeAttributesResponse response1 =
        GetClusterNodeAttributesResponse.newInstance(attributes1);

    // normal response2
    NodeAttributeInfo nodeAttributeInfo2 =
        NodeAttributeInfo.newInstance(NodeAttributeKey.newInstance("CPU"),
        NodeAttributeType.STRING);
    Set<NodeAttributeInfo> attributes2 = new HashSet<>();
    attributes2.add(nodeAttributeInfo2);
    GetClusterNodeAttributesResponse response2 =
        GetClusterNodeAttributesResponse.newInstance(attributes2);

    // empty response3
    GetClusterNodeAttributesResponse response3 =
        GetClusterNodeAttributesResponse.newInstance(new HashSet<>());

    // null response4
    GetClusterNodeAttributesResponse response4 = null;

    List<GetClusterNodeAttributesResponse> responses = new ArrayList<>();
    responses.add(response1);
    responses.add(response2);
    responses.add(response3);
    responses.add(response4);

    GetClusterNodeAttributesResponse response =
        RouterYarnClientUtils.mergeClusterNodeAttributesResponse(responses);

    Assert.assertNotNull(response);

    Set<NodeAttributeInfo> nodeAttributeInfos = response.getNodeAttributes();
    Assert.assertEquals(2, nodeAttributeInfos.size());
    Assert.assertTrue(nodeAttributeInfos.contains(nodeAttributeInfo1));
    Assert.assertTrue(nodeAttributeInfos.contains(nodeAttributeInfo2));
  }

  @Test
  public void testMergeNodesToAttributesResponse() {
    // normal response1
    NodeAttribute gpu = NodeAttribute.newInstance(NodeAttribute.PREFIX_CENTRALIZED, "GPU",
        NodeAttributeType.STRING, "nvida");
    NodeAttribute os = NodeAttribute.newInstance(NodeAttribute.PREFIX_CENTRALIZED, "OS",
        NodeAttributeType.STRING, "windows64");
    NodeAttribute dist = NodeAttribute.newInstance(NodeAttribute.PREFIX_DISTRIBUTED, "VERSION",
        NodeAttributeType.STRING, "3_0_2");
    Map<String, Set<NodeAttribute>> node1Map = new HashMap<>();
    node1Map.put("node1", ImmutableSet.of(gpu, os, dist));
    GetNodesToAttributesResponse response1 = GetNodesToAttributesResponse.newInstance(node1Map);

    // normal response2
    NodeAttribute docker = NodeAttribute.newInstance(NodeAttribute.PREFIX_DISTRIBUTED, "DOCKER",
        NodeAttributeType.STRING, "docker0");
    Map<String, Set<NodeAttribute>> node2Map = new HashMap<>();
    node2Map.put("node2", ImmutableSet.of(docker));
    GetNodesToAttributesResponse response2 = GetNodesToAttributesResponse.newInstance(node2Map);

    // empty response3
    GetNodesToAttributesResponse response3 =
        GetNodesToAttributesResponse.newInstance(new HashMap<>());

    // null response4
    GetNodesToAttributesResponse response4 = null;

    List<GetNodesToAttributesResponse> responses = new ArrayList<>();
    responses.add(response1);
    responses.add(response2);
    responses.add(response3);
    responses.add(response4);

    GetNodesToAttributesResponse response =
        RouterYarnClientUtils.mergeNodesToAttributesResponse(responses);

    Assert.assertNotNull(response);

    Map<String, Set<NodeAttribute>> hostToAttrs = response.getNodeToAttributes();
    Assert.assertNotNull(hostToAttrs);
    Assert.assertEquals(2, hostToAttrs.size());
    Assert.assertTrue(hostToAttrs.get("node1").contains(dist));
    Assert.assertTrue(hostToAttrs.get("node1").contains(gpu));
    Assert.assertTrue(hostToAttrs.get("node1").contains(os));
    Assert.assertTrue(hostToAttrs.get("node2").contains(docker));
  }
}
