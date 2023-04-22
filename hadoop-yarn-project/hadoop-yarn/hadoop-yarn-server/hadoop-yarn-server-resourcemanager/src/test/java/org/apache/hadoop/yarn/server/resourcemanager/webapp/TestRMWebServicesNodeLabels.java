/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.server.resourcemanager.webapp;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriBuilder;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.util.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.http.JettyUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.MockRM;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.LabelsToNodesInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.NodeLabelInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.NodeLabelsInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.NodeToLabelsEntry;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.NodeToLabelsInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.NodeToLabelsEntryList;
import org.apache.hadoop.yarn.webapp.GenericExceptionHandler;
import org.apache.hadoop.yarn.webapp.GuiceServletConfig;
import org.apache.hadoop.yarn.webapp.JerseyTestBase;
import org.apache.hadoop.yarn.webapp.WebServicesTestUtils;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.inject.Guice;
import com.google.inject.servlet.ServletModule;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.api.json.JSONJAXBContext;
import com.sun.jersey.api.json.JSONMarshaller;
import com.sun.jersey.core.util.MultivaluedMapImpl;
import com.sun.jersey.guice.spi.container.servlet.GuiceContainer;
import com.sun.jersey.test.framework.WebAppDescriptor;

public class TestRMWebServicesNodeLabels extends JerseyTestBase {

  private static final int BAD_REQUEST_CODE = 400;

  private static final Logger LOG = LoggerFactory
      .getLogger(TestRMWebServicesNodeLabels.class);
  private static final String NODE_0 = "nid:0";
  private static final String NODE_1 = "nid1:0";
  private static final String NODE_2 = "nid2:0";
  private static final String LABEL_A = "a";
  private static final String LABEL_B = "b";
  private static final String LABEL_X = "x";
  private static final String LABEL_Y = "y";
  private static final String LABEL_Z = "z";
  public static final boolean DEFAULT_NL_EXCLUSIVITY = true;
  private static final String PATH_WS = "ws";
  private static final String PATH_V1 = "v1";
  private static final String PATH_NODES = "nodes";
  private static final String PATH_CLUSTER = "cluster";
  private static final String PATH_REPLACE_NODE_TO_LABELS = "replace-node-to-labels";
  private static final String PATH_LABEL_MAPPINGS = "label-mappings";
  private static final String PATH_GET_LABELS = "get-labels";
  private static final String PATH_REPLACE_LABELS = "replace-labels";
  private static final String PATH_REMOVE_LABELS = "remove-node-labels";
  private static final String PATH_GET_NODE_LABELS = "get-node-labels";
  private static final String PATH_GET_NODE_TO_LABELS = "get-node-to-labels";
  private static final String QUERY_USER_NAME = "user.name";
  private static final String PATH_ADD_NODE_LABELS = "add-node-labels";

  private static MockRM rm;
  private static YarnConfiguration conf;

  private static String userName;
  private static String notUserName;
  private static RMWebServices rmWebService;
  private WebResource resource;


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
      notUserName = userName + "abc123";
      conf = new YarnConfiguration();
      conf.set(YarnConfiguration.YARN_ADMIN_ACL, userName);
      rm = new MockRM(conf);
      rmWebService = new RMWebServices(rm, conf);
      bind(RMWebServices.class).toInstance(rmWebService);
      bind(GenericExceptionHandler.class);
      bind(ResourceManager.class).toInstance(rm);
      filter("/*").through(
          TestRMWebServicesAppsModification.TestRMCustomAuthFilter.class);
      serve("/*").with(GuiceContainer.class);
    }
  }

  @Override
  @Before
  public void setUp() throws Exception {
    super.setUp();
    GuiceServletConfig.setInjector(
        Guice.createInjector(new WebServletModule()));
    resource = resource();
  }

  public TestRMWebServicesNodeLabels() {
    super(new WebAppDescriptor.Builder(
        "org.apache.hadoop.yarn.server.resourcemanager.webapp")
        .contextListenerClass(GuiceServletConfig.class)
        .filterClass(com.google.inject.servlet.GuiceFilter.class)
        .contextPath("jersey-guice-filter").servletPath("/").build());
  }

  private WebResource getClusterWebResource() {
    return resource.path(PATH_WS).path(PATH_V1).path(PATH_CLUSTER);
  }

  private ClientResponse get(String path) {
    return getClusterWebResource()
        .path(path)
        .queryParam(QUERY_USER_NAME, userName)
        .accept(MediaType.APPLICATION_JSON).get(ClientResponse.class);
  }

  private ClientResponse get(String path, MultivaluedMapImpl queryParams) {
    return getClusterWebResource()
        .path(path)
        .queryParam(QUERY_USER_NAME, userName)
        .queryParams(queryParams)
        .accept(MediaType.APPLICATION_JSON).get(ClientResponse.class);
  }

  private ClientResponse post(String path, String userName, Object payload,
      Class<?> payloadClass) throws Exception {
    return getClusterWebResource()
        .path(path)
        .queryParam(QUERY_USER_NAME, userName)
        .accept(MediaType.APPLICATION_JSON)
        .entity(toJson(payload, payloadClass),
            MediaType.APPLICATION_JSON)
        .post(ClientResponse.class);
  }

  private ClientResponse post(String path, String userName, Object payload,
      Class<?> payloadClass, MultivaluedMapImpl queryParams) throws Exception {
    WebResource.Builder builder = getClusterWebResource()
        .path(path)
        .queryParam(QUERY_USER_NAME, userName)
        .queryParams(queryParams)
        .accept(MediaType.APPLICATION_JSON);

    if (payload != null && payloadClass != null) {
      builder.entity(toJson(payload, payloadClass), MediaType.APPLICATION_JSON);
    }
    return builder.post(ClientResponse.class);
  }

  @Test
  public void testNodeLabels() throws Exception {
    ClientResponse response;

    // Add a label
    response = addNodeLabels(Lists.newArrayList(Pair.of(LABEL_A, DEFAULT_NL_EXCLUSIVITY)));
    assertHttp200(response);

    // Verify
    response = getNodeLabels();
    assertApplicationJsonUtf8Response(response);
    assertNodeLabelsInfo(response.getEntity(NodeLabelsInfo.class), Lists.newArrayList(
        Pair.of(LABEL_A, true)));

    // Add another
    response = addNodeLabels(Lists.newArrayList(Pair.of(LABEL_B, false)));
    assertHttp200(response);

    // Verify
    response = getNodeLabels();
    assertApplicationJsonUtf8Response(response);
    // Verify exclusivity for 'b' as false
    assertNodeLabelsInfo(response.getEntity(NodeLabelsInfo.class),
        Lists.newArrayList(
            Pair.of(LABEL_A, true),
            Pair.of(LABEL_B, false)));

    // Add labels to a node
    response = replaceLabelsOnNode(NODE_0, LABEL_A);
    assertHttp200(response);

    // Add labels to another node
    response = replaceLabelsOnNode(NODE_1, LABEL_B);
    assertHttp200(response);

    // Add labels to another node
    response = replaceLabelsOnNode(NODE_2, LABEL_B);
    assertHttp200(response);

    // Verify all, using get-labels-to-Nodes
    response = getNodeLabelMappings();
    assertApplicationJsonUtf8Response(response);
    LabelsToNodesInfo labelsToNodesInfo = response.getEntity(LabelsToNodesInfo.class);
    assertLabelsToNodesInfo(labelsToNodesInfo, 2, Lists.newArrayList(
        Pair.of(Pair.of(LABEL_B, false), Lists.newArrayList(NODE_1, NODE_2)),
        Pair.of(Pair.of(LABEL_A, DEFAULT_NL_EXCLUSIVITY), Lists.newArrayList(NODE_0))
    ));

    // Verify, using get-labels-to-Nodes for specified set of labels
    response = getNodeLabelMappingsByLabels(LABEL_A);
    assertApplicationJsonUtf8Response(response);
    labelsToNodesInfo = response.getEntity(LabelsToNodesInfo.class);
    assertLabelsToNodesInfo(labelsToNodesInfo, 1, Lists.newArrayList(
        Pair.of(Pair.of(LABEL_A, DEFAULT_NL_EXCLUSIVITY), Lists.newArrayList(NODE_0))
    ));

    // Verify
    response = getLabelsOfNode(NODE_0);
    assertApplicationJsonUtf8Response(response);
    assertNodeLabelsInfoContains(response.getEntity(NodeLabelsInfo.class),
        Pair.of(LABEL_A, DEFAULT_NL_EXCLUSIVITY));

    // Replace
    response = replaceLabelsOnNode(NODE_0, LABEL_B);
    assertHttp200(response);

    // Verify
    response = getLabelsOfNode(NODE_0);
    assertApplicationJsonUtf8Response(response);
    assertNodeLabelsInfoContains(response.getEntity(NodeLabelsInfo.class), Pair.of(LABEL_B, false));

    // Replace labels using node-to-labels
    response = replaceNodeToLabels(Lists.newArrayList(Pair.of(NODE_0,
        Lists.newArrayList(LABEL_A))));
    assertHttp200(response);

    // Verify, using node-to-labels
    response = getNodeToLabels();
    assertApplicationJsonUtf8Response(response);
    NodeToLabelsInfo nodeToLabelsInfo = response.getEntity(NodeToLabelsInfo.class);
    NodeLabelsInfo nodeLabelsInfo = nodeToLabelsInfo.getNodeToLabels().get(NODE_0);
    assertNodeLabelsSize(nodeLabelsInfo, 1);
    assertNodeLabelsInfoContains(nodeLabelsInfo, Pair.of(LABEL_A, DEFAULT_NL_EXCLUSIVITY));

    // Remove all
    response = replaceLabelsOnNode(NODE_0, "");
    assertHttp200(response);
    // Verify
    response = getLabelsOfNode(NODE_0);
    assertApplicationJsonUtf8Response(response);
    assertNodeLabelsSize(response.getEntity(NodeLabelsInfo.class), 0);

    // Add a label back for auth tests
    response = replaceLabelsOnNode(NODE_0, LABEL_A);
    assertHttp200(response);

    // Verify
    response = getLabelsOfNode(NODE_0);
    assertApplicationJsonUtf8Response(response);
    assertNodeLabelsInfoContains(response.getEntity(NodeLabelsInfo.class),
        Pair.of(LABEL_A, DEFAULT_NL_EXCLUSIVITY));

    // Auth fail replace labels on node
    response = replaceLabelsOnNodeWithUserName(NODE_0, notUserName, LABEL_B);
    assertHttp401(response);
    // Verify
    response = getLabelsOfNode(NODE_0);
    assertApplicationJsonUtf8Response(response);
    assertNodeLabelsInfoContains(response.getEntity(NodeLabelsInfo.class),
        Pair.of(LABEL_A, DEFAULT_NL_EXCLUSIVITY));

    // Fail to add a label with wrong user
    response = addNodeLabelsWithUser(Lists.newArrayList(Pair.of("c", DEFAULT_NL_EXCLUSIVITY)),
        notUserName);
    assertHttp401(response);

    // Verify
    response = getNodeLabels();
    assertApplicationJsonUtf8Response(response);
    assertNodeLabelsSize(response.getEntity(NodeLabelsInfo.class), 2);

    // Remove cluster label (succeed, we no longer need it)
    response = removeNodeLabel(LABEL_B);
    assertHttp200(response);
    // Verify
    response = getNodeLabels();
    assertApplicationJsonUtf8Response(response);
    assertNodeLabelsInfo(response.getEntity(NodeLabelsInfo.class),
        Lists.newArrayList(Pair.of(LABEL_A, true)));

    // Remove cluster label with post
    response = removeNodeLabel(LABEL_A);
    assertHttp200(response);
    // Verify
    response = getNodeLabels();
    assertApplicationJsonUtf8Response(response);
    nodeLabelsInfo = response.getEntity(NodeLabelsInfo.class);
    assertEquals(0, nodeLabelsInfo.getNodeLabels().size());

    // Following test cases are to test replace when distributed node label
    // configuration is on
    // Reset for testing : add cluster labels
    response = addNodeLabels(Lists.newArrayList(
        Pair.of(LABEL_X, false), Pair.of(LABEL_Y, false)));
    assertHttp200(response);
    // Reset for testing : Add labels to a node
    response = replaceLabelsOnNode(NODE_0, LABEL_Y);
    assertHttp200(response);

    //setting rmWebService for non-centralized NodeLabel Configuration
    rmWebService.isCentralizedNodeLabelConfiguration = false;

    // Case1 : Replace labels using node-to-labels
    response = replaceNodeToLabels(Lists.newArrayList(Pair.of(NODE_0,
        Lists.newArrayList(LABEL_X))));
    assertHttp404(response);

    // Verify, using node-to-labels that previous operation has failed
    response = getNodeToLabels();
    assertApplicationJsonUtf8Response(response);
    nodeToLabelsInfo = response.getEntity(NodeToLabelsInfo.class);
    nodeLabelsInfo = nodeToLabelsInfo.getNodeToLabels().get(NODE_0);
    assertNodeLabelsSize(nodeLabelsInfo, 1);
    assertNodeLabelsInfoDoesNotContain(nodeLabelsInfo, Pair.of(LABEL_X, false));

    // Case2 : failure to Replace labels using replace-labels
    response = replaceLabelsOnNode(NODE_0, LABEL_X);
    assertHttp404(response);

    // Verify, using node-to-labels that previous operation has failed
    response = getNodeToLabels();
    assertApplicationJsonUtf8Response(response);
    nodeToLabelsInfo = response.getEntity(NodeToLabelsInfo.class);
    nodeLabelsInfo = nodeToLabelsInfo.getNodeToLabels().get(NODE_0);
    assertNodeLabelsSize(nodeLabelsInfo, 1);
    assertNodeLabelsInfoDoesNotContain(nodeLabelsInfo, Pair.of(LABEL_X, false));

    //  Case3 : Remove cluster label should be successful
    response = removeNodeLabel(LABEL_X);
    assertHttp200(response);
    // Verify
    response = getNodeLabels();
    assertApplicationJsonUtf8Response(response);
    assertNodeLabelsInfoAtPosition(response.getEntity(NodeLabelsInfo.class), Pair.of(LABEL_Y,
        false), 0);

    // Remove y
    response = removeNodeLabel(LABEL_Y);
    assertHttp200(response);

    // Verify
    response = getNodeLabels();
    assertApplicationJsonUtf8Response(response);
    assertNodeLabelsSize(response.getEntity(NodeLabelsInfo.class), 0);

    // add a new nodelabel with exclusivity=false
    response = addNodeLabels(Lists.newArrayList(Pair.of(LABEL_Z, false)));
    assertHttp200(response);
    // Verify
    response = getNodeLabels();
    assertApplicationJsonUtf8Response(response);
    assertNodeLabelsInfoAtPosition(response.getEntity(NodeLabelsInfo.class),
        Pair.of(LABEL_Z, false), 0);
    assertNodeLabelsSize(nodeLabelsInfo, 1);
  }

  private void assertLabelsToNodesInfo(LabelsToNodesInfo labelsToNodesInfo, int size,
      List<Pair<Pair<String, Boolean>, List<String>>> nodeLabelsToNodesList) {
    Map<NodeLabelInfo, NodeIDsInfo> labelsToNodes = labelsToNodesInfo.getLabelsToNodes();
    assertNotNull("Labels to nodes mapping should not be null.", labelsToNodes);
    assertEquals("Size of label to nodes mapping is not the expected.", size, labelsToNodes.size());

    for (Pair<Pair<String, Boolean>, List<String>> nodeLabelToNodes : nodeLabelsToNodesList) {
      Pair<String, Boolean> expectedNLData = nodeLabelToNodes.getLeft();
      List<String> expectedNodes = nodeLabelToNodes.getRight();
      NodeLabelInfo expectedNLInfo = new NodeLabelInfo(expectedNLData.getLeft(),
          expectedNLData.getRight());
      NodeIDsInfo actualNodes = labelsToNodes.get(expectedNLInfo);
      assertNotNull(String.format("Node info not found. Expected NodeLabel data: %s",
          expectedNLData), actualNodes);
      for (String expectedNode : expectedNodes) {
        assertTrue(String.format("Can't find node ID in actual Node IDs list: %s",
                actualNodes.getNodeIDs()), actualNodes.getNodeIDs().contains(expectedNode));
      }
    }
  }

  private void assertNodeLabelsInfo(NodeLabelsInfo nodeLabelsInfo,
      List<Pair<String, Boolean>> nlInfos) {
    assertEquals(nlInfos.size(), nodeLabelsInfo.getNodeLabels().size());

    for (int i = 0; i < nodeLabelsInfo.getNodeLabelsInfo().size(); i++) {
      Pair<String, Boolean> expected = nlInfos.get(i);
      NodeLabelInfo actual = nodeLabelsInfo.getNodeLabelsInfo().get(i);
      LOG.debug("Checking NodeLabelInfo: {}", actual);
      assertEquals(expected.getLeft(), actual.getName());
      assertEquals(expected.getRight(), actual.getExclusivity());
    }
  }

  private void assertNodeLabelsInfoAtPosition(NodeLabelsInfo nodeLabelsInfo, Pair<String,
      Boolean> nlInfo, int pos) {
    NodeLabelInfo actual = nodeLabelsInfo.getNodeLabelsInfo().get(pos);
    LOG.debug("Checking NodeLabelInfo: {}", actual);
    assertEquals(nlInfo.getLeft(), actual.getName());
    assertEquals(nlInfo.getRight(), actual.getExclusivity());
  }

  private void assertNodeLabelsInfoContains(NodeLabelsInfo nodeLabelsInfo,
      Pair<String, Boolean> nlInfo) {
    NodeLabelInfo nodeLabelInfo = new NodeLabelInfo(nlInfo.getLeft(), nlInfo.getRight());
    assertTrue(String.format("Cannot find nodeLabelInfo '%s' among items of node label info list:" +
            " %s", nodeLabelInfo, nodeLabelsInfo.getNodeLabelsInfo()),
        nodeLabelsInfo.getNodeLabelsInfo().contains(nodeLabelInfo));
  }

  private void assertNodeLabelsInfoDoesNotContain(NodeLabelsInfo nodeLabelsInfo, Pair<String,
      Boolean> nlInfo) {
    NodeLabelInfo nodeLabelInfo = new NodeLabelInfo(nlInfo.getLeft(), nlInfo.getRight());
    assertFalse(String.format("Should have not found nodeLabelInfo '%s' among " +
        "items of node label info list: %s", nodeLabelInfo, nodeLabelsInfo.getNodeLabelsInfo()),
        nodeLabelsInfo.getNodeLabelsInfo().contains(nodeLabelInfo));
  }

  private void assertNodeLabelsSize(NodeLabelsInfo nodeLabelsInfo, int expectedSize) {
    assertEquals(expectedSize, nodeLabelsInfo.getNodeLabelsInfo().size());
  }

  private ClientResponse replaceNodeToLabels(List<Pair<String, List<String>>> nodeToLabelInfos) throws Exception {
    NodeToLabelsEntryList nodeToLabelsEntries = new NodeToLabelsEntryList();

    for (Pair<String, List<String>> nodeToLabelInfo : nodeToLabelInfos) {
      ArrayList<String> labelList = new ArrayList<>(nodeToLabelInfo.getRight());
      String nodeId = nodeToLabelInfo.getLeft();
      NodeToLabelsEntry nli = new NodeToLabelsEntry(nodeId, labelList);
      nodeToLabelsEntries.getNodeToLabels().add(nli);
    }
    return post(PATH_REPLACE_NODE_TO_LABELS, userName, nodeToLabelsEntries, NodeToLabelsEntryList.class);
  }

  private ClientResponse getNodeLabelMappings() {
    return get(PATH_LABEL_MAPPINGS);
  }

  private ClientResponse getNodeLabelMappingsByLabels(String... labelNames) {
    MultivaluedMapImpl params = createMultiValuedMap(labelNames);
    return get(PATH_LABEL_MAPPINGS, params);
  }

  private ClientResponse replaceLabelsOnNode(String node, String... labelNames) throws Exception {
    return replaceLabelsOnNodeWithUserName(node, userName, labelNames);
  }

  private ClientResponse replaceLabelsOnNodeWithUserName(String node,
      String userName, String... labelNames) throws Exception {
    LOG.info("Replacing labels on node '{}', label(s): {}", node, labelNames);
    MultivaluedMapImpl params = createMultiValuedMap(labelNames);
    String path = UriBuilder.fromPath(PATH_NODES).path(node)
        .path(PATH_REPLACE_LABELS).build().toString();
    return post(path, userName, null, null, params);
  }

  private static MultivaluedMapImpl createMultiValuedMap(String[] labelNames) {
    MultivaluedMapImpl params = new MultivaluedMapImpl();
    for (String labelName : labelNames) {
      params.add("labels", labelName);
    }
    return params;
  }

  private ClientResponse removeNodeLabel(String... labelNames) throws Exception {
    MultivaluedMapImpl params = createMultiValuedMap(labelNames);
    return post(PATH_REMOVE_LABELS, userName, null, null, params);
  }

  private ClientResponse getLabelsOfNode(String node) {
    String path = UriBuilder.fromPath(PATH_NODES).path(node)
        .path(PATH_GET_LABELS).build().toString();
    return get(path);
  }

  private ClientResponse getNodeLabels() {
    return get(PATH_GET_NODE_LABELS);
  }

  private ClientResponse getNodeToLabels() {
    return get(PATH_GET_NODE_TO_LABELS);
  }

  private ClientResponse addNodeLabels(List<Pair<String, Boolean>> nlInfos) throws Exception {
    return addNodeLabelsInternal(nlInfos, userName);
  }

  private ClientResponse addNodeLabelsWithUser(List<Pair<String, Boolean>> nlInfos,
      String userName) throws Exception {
    return addNodeLabelsInternal(nlInfos, userName);
  }

  private ClientResponse addNodeLabelsInternal(List<Pair<String, Boolean>> nlInfos,
      String userName) throws Exception {
    NodeLabelsInfo nodeLabelsInfo = new NodeLabelsInfo();
    for (Pair<String, Boolean> nlInfo : nlInfos) {
      NodeLabelInfo nodeLabelInfo = new NodeLabelInfo(nlInfo.getLeft(), nlInfo.getRight());
      nodeLabelsInfo.getNodeLabelsInfo().add(nodeLabelInfo);
    }
    return post(PATH_ADD_NODE_LABELS, userName, nodeLabelsInfo, NodeLabelsInfo.class);
  }

  private void assertApplicationJsonUtf8Response(ClientResponse response) {
    assertEquals(MediaType.APPLICATION_JSON_TYPE + "; " + JettyUtils.UTF_8,
        response.getType().toString());
  }

  private void assertHttp200(ClientResponse response) {
    assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());
  }

  private void assertHttp401(ClientResponse response) {
    assertEquals(Response.Status.UNAUTHORIZED.getStatusCode(), response.getStatus());
  }

  private void assertHttp404(ClientResponse response) {
    assertEquals(Response.Status.NOT_FOUND.getStatusCode(), response.getStatus());
  }

  @Test
  public void testLabelInvalidAddition()
      throws Exception {
    // Add a invalid label
    ClientResponse response = addNodeLabels(Lists.newArrayList(Pair.of("a&",
        DEFAULT_NL_EXCLUSIVITY)));
    String expectedMessage =
        "java.io.IOException: label name should only contains"
            + " {0-9, a-z, A-Z, -, _} and should not started with"
            + " {-,_}, now it is= a&";
    validateJsonExceptionContent(response, expectedMessage);
  }

  @Test
  public void testLabelChangeExclusivity()
      throws Exception {
    ClientResponse response;
    response = addNodeLabels(Lists.newArrayList(Pair.of("newLabel", DEFAULT_NL_EXCLUSIVITY)));
    assertHttp200(response);
    // new info and change exclusivity
    response = addNodeLabels(Lists.newArrayList(Pair.of("newLabel", false)));
    String expectedMessage =
        "java.io.IOException: Exclusivity cannot be modified for an existing"
            + " label with : <newLabel:exclusivity=false>";
    validateJsonExceptionContent(response, expectedMessage);
  }

  private void validateJsonExceptionContent(ClientResponse response,
      String expectedMessage)
      throws JSONException {
    Assert.assertEquals(BAD_REQUEST_CODE, response.getStatus());
    JSONObject msg = response.getEntity(JSONObject.class);
    JSONObject exception = msg.getJSONObject("RemoteException");
    String message = exception.getString("message");
    assertEquals("incorrect number of elements", 3, exception.length());
    String type = exception.getString("exception");
    String classname = exception.getString("javaClassName");
    WebServicesTestUtils.checkStringMatch("exception type",
        "BadRequestException", type);
    WebServicesTestUtils.checkStringMatch("exception classname",
        "org.apache.hadoop.yarn.webapp.BadRequestException", classname);
    WebServicesTestUtils.checkStringContains("exception message",
        expectedMessage, message);
  }

  @Test
  public void testLabelInvalidReplace()
      throws Exception {
    ClientResponse response;
    // replace label which doesn't exist
    response = replaceLabelsOnNode(NODE_0, "idontexist");

    String expectedMessage =
        "Not all labels being replaced contained by known label"
            + " collections, please check, new labels=[idontexist]";
    validateJsonExceptionContent(response, expectedMessage);
  }

  @Test
  public void testLabelInvalidRemove()
      throws Exception {
    ClientResponse response;
    response = removeNodeLabel("ireallydontexist");
    String expectedMessage =
        "java.io.IOException: Node label=ireallydontexist to be"
            + " removed doesn't existed in cluster node labels"
            + " collection.";
    validateJsonExceptionContent(response, expectedMessage);
  }

  @Test
  public void testNodeLabelPartitionInfo() throws Exception {
    ClientResponse response;

    // Add a node label
    response = addNodeLabels(Lists.newArrayList(Pair.of(LABEL_A, DEFAULT_NL_EXCLUSIVITY)));
    assertHttp200(response);

    // Verify partition info in get-node-labels
    response = getNodeLabels();
    assertApplicationJsonUtf8Response(response);
    NodeLabelsInfo nodeLabelsInfo = response.getEntity(NodeLabelsInfo.class);
    assertNodeLabelsSize(nodeLabelsInfo, 1);
    for (NodeLabelInfo nl : nodeLabelsInfo.getNodeLabelsInfo()) {
      assertEquals(LABEL_A, nl.getName());
      assertTrue(nl.getExclusivity());
      assertNotNull(nl.getPartitionInfo());
      assertNotNull(nl.getPartitionInfo().getResourceAvailable());
    }

    // Add node label to a node
    response = replaceLabelsOnNode("nodeId:0", LABEL_A);
    assertHttp200(response);

    // Verify partition info in label-mappings
    response = getNodeLabelMappings();
    assertApplicationJsonUtf8Response(response);
    LabelsToNodesInfo labelsToNodesInfo = response.getEntity(LabelsToNodesInfo.class);
    assertLabelsToNodesInfo(labelsToNodesInfo, 1, Lists.newArrayList(
        Pair.of(Pair.of(LABEL_A, DEFAULT_NL_EXCLUSIVITY), Lists.newArrayList("nodeId:0"))
    ));
    NodeIDsInfo nodes = labelsToNodesInfo.getLabelsToNodes().get(new NodeLabelInfo(LABEL_A));
    assertNotNull(nodes.getPartitionInfo());
    assertNotNull(nodes.getPartitionInfo().getResourceAvailable());
  }

  @SuppressWarnings("rawtypes")
  private String toJson(Object obj, Class klass) throws Exception {
    StringWriter sw = new StringWriter();
    JSONJAXBContext ctx = new JSONJAXBContext(klass);
    JSONMarshaller jm = ctx.createJSONMarshaller();
    jm.marshallToJSON(obj, sw);
    return sw.toString();
  }
}
