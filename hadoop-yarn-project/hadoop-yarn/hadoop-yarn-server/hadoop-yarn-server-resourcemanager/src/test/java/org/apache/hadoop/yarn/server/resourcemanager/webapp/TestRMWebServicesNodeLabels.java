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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.List;

import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

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

  private static MockRM rm;
  private static YarnConfiguration conf;

  private static String userName;
  private static String notUserName;
  private static RMWebServices rmWebService;
  public static final boolean DEFAULT_NL_EXCLUSIVITY = true;

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
      rmWebService = new RMWebServices(rm,conf);
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
  }

  public TestRMWebServicesNodeLabels() {
    super(new WebAppDescriptor.Builder(
        "org.apache.hadoop.yarn.server.resourcemanager.webapp")
        .contextListenerClass(GuiceServletConfig.class)
        .filterClass(com.google.inject.servlet.GuiceFilter.class)
        .contextPath("jersey-guice-filter").servletPath("/").build());
  }

  @Test
  public void testNodeLabels() throws Exception {
    WebResource r = resource();

    ClientResponse response;

    // Add a label
    response = addNodeLabels(r, Lists.newArrayList(Pair.of("a", DEFAULT_NL_EXCLUSIVITY)));
    assertHttp200(response);

    // Verify
    response = getNodeLabels(r);
    assertApplicationJsonUtf8Response(response);
    assertNodeLabelsInfo(response.getEntity(NodeLabelsInfo.class), Lists.newArrayList(
        Pair.of("a", true)));
    
    // Add another
    response = addNodeLabels(r, Lists.newArrayList(Pair.of("b", false)));
    assertHttp200(response);
    
    // Verify
    response = getNodeLabels(r);
    assertApplicationJsonUtf8Response(response);
    // Verify exclusivity for 'b' as false
    assertNodeLabelsInfo(response.getEntity(NodeLabelsInfo.class), 
        Lists.newArrayList(
            Pair.of("a", true),
            Pair.of("b", false)));
    
    // Add labels to a node
    MultivaluedMapImpl params = new MultivaluedMapImpl();
    params.add("labels", "a");
    response = replaceLabelsOnNode(r, "nid:0", params);
    assertHttp200(response);
    LOG.info("posted node nodelabel");

    // Add labels to another node
    params = new MultivaluedMapImpl();
    params.add("labels", "b");
    response = replaceLabelsOnNode(r, "nid1:0", params);
    assertHttp200(response);
    LOG.info("posted node nodelabel");

    // Add labels to another node
    params = new MultivaluedMapImpl();
    params.add("labels", "b");
    response = replaceLabelsOnNode(r, "nid2:0", params);
    assertHttp200(response);
    LOG.info("posted node nodelabel");

    // Verify, using get-labels-to-Nodes
    response = getNodeLabelMappings(r);
    assertApplicationJsonUtf8Response(response);
    LabelsToNodesInfo labelsToNodesInfo = response.getEntity(LabelsToNodesInfo.class);
    assertEquals(2, labelsToNodesInfo.getLabelsToNodes().size());
    NodeIDsInfo nodes = labelsToNodesInfo.getLabelsToNodes().get(
        new NodeLabelInfo("b", false));
    assertTrue(nodes.getNodeIDs().contains("nid2:0"));
    assertTrue(nodes.getNodeIDs().contains("nid1:0"));
    nodes = labelsToNodesInfo.getLabelsToNodes().get(new NodeLabelInfo("a"));
    assertTrue(nodes.getNodeIDs().contains("nid:0"));

    // Verify, using get-labels-to-Nodes for specified set of labels
    params = new MultivaluedMapImpl();
    params.add("labels", "a");
    response = getNodeLabelMappings(r, params);
    assertApplicationJsonUtf8Response(response);
    labelsToNodesInfo = response.getEntity(LabelsToNodesInfo.class);
    assertEquals(1, labelsToNodesInfo.getLabelsToNodes().size());
    nodes = labelsToNodesInfo.getLabelsToNodes().get(new NodeLabelInfo("a"));
    assertTrue(nodes.getNodeIDs().contains("nid:0"));

    // Verify
    response = getLabelsOfNode(r, "nid:0");
    assertApplicationJsonUtf8Response(response);
    assertNodeLabelsInfoContains(response.getEntity(NodeLabelsInfo.class), 
        Pair.of("a", DEFAULT_NL_EXCLUSIVITY));

    
    // Replace
    params = new MultivaluedMapImpl();
    params.add("labels", "b");
    response = replaceLabelsOnNode(r, "nid:0", params);
    assertHttp200(response);
    LOG.info("posted node nodelabel");

    // Verify
    response = getLabelsOfNode(r, "nid:0");
    assertApplicationJsonUtf8Response(response);
    assertNodeLabelsInfoContains(response.getEntity(NodeLabelsInfo.class), Pair.of("b", false));
            
    // Replace labels using node-to-labels
    NodeToLabelsEntryList nodeToLabelsEntries = new NodeToLabelsEntryList();
    ArrayList<String> labels = new ArrayList<>();
    labels.add("a");
    NodeToLabelsEntry nli = new NodeToLabelsEntry("nid:0", labels);
    nodeToLabelsEntries.getNodeToLabels().add(nli);
    response = replaceNodeToLabels(r, nodeToLabelsEntries);
    assertHttp200(response);
        
    // Verify, using node-to-labels
    response =
        r.path("ws").path("v1").path("cluster")
            .path("get-node-to-labels").queryParam("user.name", userName)
            .accept(MediaType.APPLICATION_JSON).get(ClientResponse.class);
    assertApplicationJsonUtf8Response(response);
    NodeToLabelsInfo nodeToLabelsInfo = response.getEntity(NodeToLabelsInfo.class);
    NodeLabelsInfo nodeLabelsInfo = nodeToLabelsInfo.getNodeToLabels().get("nid:0");
    assertNodeLabelsSize(nodeLabelsInfo, 1);
    assertNodeLabelsInfoContains(nodeLabelsInfo, Pair.of("a", DEFAULT_NL_EXCLUSIVITY));
    
    // Remove all
    params = new MultivaluedMapImpl();
    params.add("labels", "");
    response = replaceLabelsOnNode(r, "nid:0", params);
    assertHttp200(response);
    LOG.info("posted node nodelabel");
    // Verify
    response = getLabelsOfNode(r, "nid:0");
    assertApplicationJsonUtf8Response(response);
    assertNodeLabelsSize(response.getEntity(NodeLabelsInfo.class), 0);
    
    // Add a label back for auth tests
    params = new MultivaluedMapImpl();
    params.add("labels", "a");
    response = replaceLabelsOnNode(r, "nid:0", params);
    assertHttp200(response);
    LOG.info("posted node nodelabel");

    // Verify
    response = getLabelsOfNode(r, "nid:0");
    assertApplicationJsonUtf8Response(response);
    assertNodeLabelsInfoContains(response.getEntity(NodeLabelsInfo.class), 
        Pair.of("a", DEFAULT_NL_EXCLUSIVITY));
    
    // Auth fail replace labels on node
    params = new MultivaluedMapImpl();
    params.add("labels", "b");
    response = replaceLabelsOnNode(r, "nid:0", params, notUserName);
    assertHttp401(response);
    // Verify
    response = getLabelsOfNode(r, "nid:0");
    assertApplicationJsonUtf8Response(response);
    assertNodeLabelsInfoContains(response.getEntity(NodeLabelsInfo.class), 
        Pair.of("a", DEFAULT_NL_EXCLUSIVITY));
    
    // Fail to add a label with post
    response =
        r.path("ws").path("v1").path("cluster")
            .path("add-node-labels").queryParam("user.name", notUserName)
            .accept(MediaType.APPLICATION_JSON)
            .entity("{\"nodeLabels\":\"c\"}", MediaType.APPLICATION_JSON)
            .post(ClientResponse.class);
    assertHttp401(response);

    // Verify
    response = getNodeLabels(r);
    assertApplicationJsonUtf8Response(response);
    assertNodeLabelsSize(response.getEntity(NodeLabelsInfo.class), 2);
    
    // Remove cluster label (succeed, we no longer need it)
    params = new MultivaluedMapImpl();
    params.add("labels", "b");
    response = removeNodeLabel(r, params);
    assertHttp200(response);
    // Verify
    response = getNodeLabels(r);
    assertApplicationJsonUtf8Response(response);
    assertNodeLabelsInfo(response.getEntity(NodeLabelsInfo.class),
        Lists.newArrayList(Pair.of("a", true)));
    
    // Remove cluster label with post
    params = new MultivaluedMapImpl();
    params.add("labels", "a");
    response = removeNodeLabel(r, params);
    assertHttp200(response);
    // Verify
    response = getNodeLabels(r);
    assertApplicationJsonUtf8Response(response);
    nodeLabelsInfo = response.getEntity(NodeLabelsInfo.class);
    assertEquals(0, nodeLabelsInfo.getNodeLabels().size());

    // Following test cases are to test replace when distributed node label
    // configuration is on
    // Reset for testing : add cluster labels
    response = addNodeLabels(r, Lists.newArrayList(
        Pair.of("x", false), Pair.of("y", false)));
    assertHttp200(response);
    // Reset for testing : Add labels to a node
    params = new MultivaluedMapImpl();
    params.add("labels", "y");
    response = replaceLabelsOnNode(r, "nid:0", params);
    assertHttp200(response);
    LOG.info("posted node nodelabel");

    //setting rmWebService for non-centralized NodeLabel Configuration
    rmWebService.isCentralizedNodeLabelConfiguration = false;

    // Case1 : Replace labels using node-to-labels
    nodeToLabelsEntries = new NodeToLabelsEntryList();
    labels = new ArrayList<>();
    labels.add("x");
    nli = new NodeToLabelsEntry("nid:0", labels);
    nodeToLabelsEntries.getNodeToLabels().add(nli);
    response = replaceNodeToLabels(r, nodeToLabelsEntries);
    assertHttp404(response);

    // Verify, using node-to-labels that previous operation has failed
    response =
        r.path("ws").path("v1").path("cluster").path("get-node-to-labels")
            .queryParam("user.name", userName)
            .accept(MediaType.APPLICATION_JSON).get(ClientResponse.class);
    assertApplicationJsonUtf8Response(response);
    nodeToLabelsInfo = response.getEntity(NodeToLabelsInfo.class);
    nodeLabelsInfo = nodeToLabelsInfo.getNodeToLabels().get("nid:0");
    assertNodeLabelsSize(nodeLabelsInfo, 1);
    assertNodeLabelsInfoDoesNotContain(nodeLabelsInfo, Pair.of("x", false));

    // Case2 : failure to Replace labels using replace-labels
    response =
        r.path("ws").path("v1").path("cluster").path("nodes").path("nid:0")
            .path("replace-labels").queryParam("user.name", userName)
            .accept(MediaType.APPLICATION_JSON)
            .entity("{\"nodeLabelName\": [\"x\"]}", MediaType.APPLICATION_JSON)
            .post(ClientResponse.class);
    assertHttp404(response);
    LOG.info("posted node nodelabel");

    // Verify, using node-to-labels that previous operation has failed
    response =
        r.path("ws").path("v1").path("cluster").path("get-node-to-labels")
            .queryParam("user.name", userName)
            .accept(MediaType.APPLICATION_JSON).get(ClientResponse.class);
    assertApplicationJsonUtf8Response(response);
    nodeToLabelsInfo = response.getEntity(NodeToLabelsInfo.class);
    nodeLabelsInfo = nodeToLabelsInfo.getNodeToLabels().get("nid:0");
    assertNodeLabelsSize(nodeLabelsInfo, 1);
    assertNodeLabelsInfoDoesNotContain(nodeLabelsInfo, Pair.of("x", false));

    //  Case3 : Remove cluster label should be successful
    params = new MultivaluedMapImpl();
    params.add("labels", "x");
    response = removeNodeLabel(r, params);
    assertHttp200(response);
    // Verify
    response = getNodeLabels(r);
    assertApplicationJsonUtf8Response(response);
    assertNodeLabelsInfoAtPosition(response.getEntity(NodeLabelsInfo.class),
        Pair.of("y", false), 0);

    // Remove y
    params = new MultivaluedMapImpl();
    params.add("labels", "y");
    response = removeNodeLabel(r, params);
    assertHttp200(response);

    // Verify
    response = getNodeLabels(r);
    assertApplicationJsonUtf8Response(response);
    assertNodeLabelsSize(response.getEntity(NodeLabelsInfo.class), 0);

    // add a new nodelabel with exclusivity
    response = addNodeLabels(r, Lists.newArrayList(Pair.of("z", false)));
    assertHttp200(response);
    // Verify
    response = getNodeLabels(r);
    assertApplicationJsonUtf8Response(response);
    assertNodeLabelsInfoAtPosition(response.getEntity(NodeLabelsInfo.class),
        Pair.of("z", false), 0);
    assertNodeLabelsSize(nodeLabelsInfo, 1);
  }

  private void assertNodeLabelsInfo(NodeLabelsInfo nodeLabelsInfo, List<Pair<String, Boolean>> nlInfos) {
    assertEquals(nlInfos.size(), nodeLabelsInfo.getNodeLabels().size());
    
    for (int i = 0; i < nodeLabelsInfo.getNodeLabelsInfo().size();i++) {
      Pair<String, Boolean> expected = nlInfos.get(i);
      NodeLabelInfo actual = nodeLabelsInfo.getNodeLabelsInfo().get(i);
      assertEquals(expected.getLeft(), actual.getName());
      assertEquals(expected.getRight(), actual.getExclusivity());
    }
  }

  private void assertNodeLabelsInfoAtPosition(NodeLabelsInfo nodeLabelsInfo, Pair<String, Boolean> nlInfo, int pos) {
    NodeLabelInfo actual = nodeLabelsInfo.getNodeLabelsInfo().get(pos);
    assertEquals(nlInfo.getLeft(), actual.getName());
    assertEquals(nlInfo.getRight(), actual.getExclusivity());
  }

  private void assertNodeLabelsInfoContains(NodeLabelsInfo nodeLabelsInfo, Pair<String, Boolean> nlInfo) {
    NodeLabelInfo nodeLabelInfo = new NodeLabelInfo(nlInfo.getLeft(), nlInfo.getRight());
    assertTrue(nodeLabelsInfo.getNodeLabelsInfo().contains(nodeLabelInfo));
  }

  private void assertNodeLabelsInfoDoesNotContain(NodeLabelsInfo nodeLabelsInfo, Pair<String, Boolean> nlInfo) {
    NodeLabelInfo nodeLabelInfo = new NodeLabelInfo(nlInfo.getLeft(), nlInfo.getRight());
    assertFalse(nodeLabelsInfo.getNodeLabelsInfo().contains(nodeLabelInfo));
  }

  private void assertNodeLabelsSize(NodeLabelsInfo nodeLabelsInfo, int expectedSize) {
    assertEquals(expectedSize, nodeLabelsInfo.getNodeLabelsInfo().size());
  }

  private ClientResponse replaceNodeToLabels(WebResource r,
      NodeToLabelsEntryList nodeToLabelsEntries) throws Exception {
    ClientResponse response;
    response =
        r.path("ws").path("v1").path("cluster")
            .path("replace-node-to-labels")
            .queryParam("user.name", userName)
            .accept(MediaType.APPLICATION_JSON)
            .entity(toJson(nodeToLabelsEntries, NodeToLabelsEntryList.class),
              MediaType.APPLICATION_JSON)
            .post(ClientResponse.class);
    return response;
  }

  private ClientResponse getNodeLabelMappings(WebResource r) {
    ClientResponse response;
    response =
        r.path("ws").path("v1").path("cluster")
            .path("label-mappings").queryParam("user.name", userName)
            .accept(MediaType.APPLICATION_JSON).get(ClientResponse.class);
    return response;
  }

  private ClientResponse getNodeLabelMappings(WebResource r, MultivaluedMapImpl params) {
    ClientResponse response;
    response =
        r.path("ws").path("v1").path("cluster")
            .path("label-mappings").queryParam("user.name", userName)
            .queryParams(params)
            .accept(MediaType.APPLICATION_JSON).get(ClientResponse.class);
    return response;
  }

  private ClientResponse replaceLabelsOnNode(WebResource r, String node, MultivaluedMapImpl params) {
    return replaceLabelsOnNode(r, node, params, userName);
  }

  private ClientResponse replaceLabelsOnNode(WebResource r, String node, MultivaluedMapImpl params, String userName) {
    ClientResponse response;
    response =
        r.path("ws").path("v1").path("cluster")
            .path("nodes").path(node)
            .path("replace-labels")
            .queryParam("user.name", userName)
            .queryParams(params)
            .accept(MediaType.APPLICATION_JSON)
            .post(ClientResponse.class);
    return response;
  }

  private ClientResponse removeNodeLabel(WebResource r, MultivaluedMapImpl params) {
    ClientResponse response;
    response =
        r.path("ws").path("v1").path("cluster")
            .path("remove-node-labels")
            .queryParam("user.name", userName)
            .queryParams(params)
            .accept(MediaType.APPLICATION_JSON)
            .post(ClientResponse.class);
    return response;
  }

  private ClientResponse getLabelsOfNode(WebResource r, String node) {
    return r.path("ws").path("v1").path("cluster")
        .path("nodes").path(node)
        .path("get-labels").queryParam("user.name", userName)
        .accept(MediaType.APPLICATION_JSON).get(ClientResponse.class);
  }

  private ClientResponse getNodeLabels(WebResource r) {
    return r.path("ws").path("v1").path("cluster")
        .path("get-node-labels")
        .queryParam("user.name", userName)
        .accept(MediaType.APPLICATION_JSON).get(ClientResponse.class);
  }

  private ClientResponse addNodeLabels(WebResource r, List<Pair<String, Boolean>> nlInfos) throws Exception {
    NodeLabelsInfo nodeLabelsInfo = new NodeLabelsInfo();
    for (Pair<String, Boolean> nlInfo : nlInfos) {
      nodeLabelsInfo.getNodeLabelsInfo().add
          (new NodeLabelInfo(nlInfo.getLeft(), nlInfo.getRight()));
    }
    
    return r.path("ws").path("v1").path("cluster")
        .path("add-node-labels")
        .queryParam("user.name", userName)
        .accept(MediaType.APPLICATION_JSON)
        .entity(toJson(nodeLabelsInfo, NodeLabelsInfo.class),
            MediaType.APPLICATION_JSON)
        .post(ClientResponse.class);
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
    WebResource r = resource();
    // Add a invalid label
    ClientResponse response = addNodeLabels(r, Lists.newArrayList(Pair.of("a&", DEFAULT_NL_EXCLUSIVITY)));
    String expectedMessage =
        "java.io.IOException: label name should only contains"
            + " {0-9, a-z, A-Z, -, _} and should not started with"
            + " {-,_}, now it is= a&";
    validateJsonExceptionContent(response, expectedMessage);
  }

  @Test
  public void testLabelChangeExclusivity()
      throws Exception {
    WebResource r = resource();
    ClientResponse response;
    response = addNodeLabels(r, Lists.newArrayList(Pair.of("newLabel", DEFAULT_NL_EXCLUSIVITY)));
    assertHttp200(response);
    // new info and change exclusivity
    response = addNodeLabels(r, Lists.newArrayList(Pair.of("newLabel", false)));
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
    WebResource r = resource();
    ClientResponse response;
    // replace label which doesn't exist
    MultivaluedMapImpl params = new MultivaluedMapImpl();
    params.add("labels", "idontexist");
    response = replaceLabelsOnNode(r, "nid:0", params);

    String expectedMessage =
        "Not all labels being replaced contained by known label"
            + " collections, please check, new labels=[idontexist]";
    validateJsonExceptionContent(response, expectedMessage);
  }

  @Test
  public void testLabelInvalidRemove()
      throws Exception {
    WebResource r = resource();
    ClientResponse response;
    MultivaluedMapImpl params = new MultivaluedMapImpl();
    params.add("labels", "irealldontexist");
    response = removeNodeLabel(r, params);
    String expectedMessage =
        "java.io.IOException: Node label=irealldontexist to be"
            + " removed doesn't existed in cluster node labels"
            + " collection.";
    validateJsonExceptionContent(response, expectedMessage);
  }

  @Test
  public void testNodeLabelPartitionInfo() throws Exception {
    WebResource r = resource();

    ClientResponse response;

    // Add a node label
    response = addNodeLabels(r, Lists.newArrayList(Pair.of("a", DEFAULT_NL_EXCLUSIVITY)));
    assertHttp200(response);

    // Verify partition info in get-node-labels
    response = getNodeLabels(r);
    assertApplicationJsonUtf8Response(response);
    NodeLabelsInfo nodeLabelsInfo = response.getEntity(NodeLabelsInfo.class);
    assertNodeLabelsSize(nodeLabelsInfo, 1);
    for (NodeLabelInfo nl : nodeLabelsInfo.getNodeLabelsInfo()) {
      assertEquals("a", nl.getName());
      assertTrue(nl.getExclusivity());
      assertNotNull(nl.getPartitionInfo());
      assertNotNull(nl.getPartitionInfo().getResourceAvailable());
    }

    // Add node label to a node
    MultivaluedMapImpl params = new MultivaluedMapImpl();
    params.add("labels", "a");
    response = replaceLabelsOnNode(r, "nodeId:0", params);
    assertHttp200(response);

    // Verify partition info in label-mappings
    response = getNodeLabelMappings(r);
    assertApplicationJsonUtf8Response(response);
    LabelsToNodesInfo labelsToNodesInfo = response.getEntity(LabelsToNodesInfo.class);
    assertEquals(1, labelsToNodesInfo.getLabelsToNodes().size());
    NodeIDsInfo nodes = labelsToNodesInfo.getLabelsToNodes().get(
        new NodeLabelInfo("a"));
    assertTrue(nodes.getNodeIDs().contains("nodeId:0"));
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
