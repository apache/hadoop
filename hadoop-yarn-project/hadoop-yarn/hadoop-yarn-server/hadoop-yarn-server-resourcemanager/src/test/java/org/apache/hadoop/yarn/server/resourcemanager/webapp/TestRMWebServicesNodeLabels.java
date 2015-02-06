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
import java.io.StringReader;
import java.io.StringWriter;

import javax.ws.rs.core.MediaType;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.MockRM;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.NodeLabelsInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.NodeToLabelsInfo;
import org.apache.hadoop.yarn.webapp.GenericExceptionHandler;
import org.apache.hadoop.yarn.webapp.JerseyTestBase;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.junit.Test;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.servlet.GuiceServletContextListener;
import com.google.inject.servlet.ServletModule;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.api.json.JSONJAXBContext;
import com.sun.jersey.api.json.JSONMarshaller;
import com.sun.jersey.api.json.JSONUnmarshaller;
import com.sun.jersey.guice.spi.container.servlet.GuiceContainer;
import com.sun.jersey.test.framework.WebAppDescriptor;

public class TestRMWebServicesNodeLabels extends JerseyTestBase {

  private static final Log LOG = LogFactory
      .getLog(TestRMWebServicesNodeLabels.class);

  private static MockRM rm;
  private YarnConfiguration conf;

  private String userName;
  private String notUserName;

  private Injector injector = Guice.createInjector(new ServletModule() {
    @Override
    protected void configureServlets() {
      bind(JAXBContextResolver.class);
      bind(RMWebServices.class);
      bind(GenericExceptionHandler.class);
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
      bind(ResourceManager.class).toInstance(rm);
      filter("/*").through(
          TestRMWebServicesAppsModification.TestRMCustomAuthFilter.class);
      serve("/*").with(GuiceContainer.class);
    }
  });

  public class GuiceServletConfig extends GuiceServletContextListener {

    @Override
    protected Injector getInjector() {
      return injector;
    }
  }

  public TestRMWebServicesNodeLabels() {
    super(new WebAppDescriptor.Builder(
        "org.apache.hadoop.yarn.server.resourcemanager.webapp")
        .contextListenerClass(GuiceServletConfig.class)
        .filterClass(com.google.inject.servlet.GuiceFilter.class)
        .contextPath("jersey-guice-filter").servletPath("/").build());
  }

  @Test
  public void testNodeLabels() throws JSONException, Exception {
    WebResource r = resource();

    ClientResponse response;
    JSONObject json;
    JSONArray jarr;
    String responseString;

    // Add a label
    response =
        r.path("ws").path("v1").path("cluster")
            .path("add-node-labels").queryParam("user.name", userName)
            .accept(MediaType.APPLICATION_JSON)
            .entity("{\"nodeLabels\":\"a\"}", MediaType.APPLICATION_JSON)
            .post(ClientResponse.class);

    // Verify
    response =
        r.path("ws").path("v1").path("cluster")
            .path("get-node-labels").queryParam("user.name", userName)
            .accept(MediaType.APPLICATION_JSON).get(ClientResponse.class);
    assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
    json = response.getEntity(JSONObject.class);
    assertEquals("a", json.getString("nodeLabels"));
    
    // Add another
    response =
        r.path("ws").path("v1").path("cluster")
            .path("add-node-labels").queryParam("user.name", userName)
            .accept(MediaType.APPLICATION_JSON)
            .entity("{\"nodeLabels\":\"b\"}", MediaType.APPLICATION_JSON)
            .post(ClientResponse.class);

    // Verify
    response =
        r.path("ws").path("v1").path("cluster")
            .path("get-node-labels").queryParam("user.name", userName)
            .accept(MediaType.APPLICATION_JSON).get(ClientResponse.class);
    assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
    json = response.getEntity(JSONObject.class);
    jarr = json.getJSONArray("nodeLabels");
    assertEquals(2, jarr.length());
    
    // Add labels to a node
    response =
        r.path("ws").path("v1").path("cluster")
            .path("nodes").path("nid:0")
            .path("replace-labels")
            .queryParam("user.name", userName)
            .accept(MediaType.APPLICATION_JSON)
            .entity("{\"nodeLabels\": [\"a\"]}",
              MediaType.APPLICATION_JSON)
            .post(ClientResponse.class);
    LOG.info("posted node nodelabel");

    // Verify
    response =
        r.path("ws").path("v1").path("cluster")
            .path("nodes").path("nid:0")
            .path("get-labels").queryParam("user.name", userName)
            .accept(MediaType.APPLICATION_JSON).get(ClientResponse.class);
    assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
    json = response.getEntity(JSONObject.class);
    assertEquals("a", json.getString("nodeLabels"));

    
    // Replace
    response =
        r.path("ws").path("v1").path("cluster")
            .path("nodes").path("nid:0")
            .path("replace-labels")
            .queryParam("user.name", userName)
            .accept(MediaType.APPLICATION_JSON)
            .entity("{\"nodeLabels\":\"b\"}", MediaType.APPLICATION_JSON)
            .post(ClientResponse.class);
    LOG.info("posted node nodelabel");

    // Verify
    response =
        r.path("ws").path("v1").path("cluster")
            .path("nodes").path("nid:0")
            .path("get-labels").queryParam("user.name", userName)
            .accept(MediaType.APPLICATION_JSON).get(ClientResponse.class);
    assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
    json = response.getEntity(JSONObject.class);
    assertEquals("b", json.getString("nodeLabels"));
            
    // Replace labels using node-to-labels
    NodeToLabelsInfo ntli = new NodeToLabelsInfo();
    NodeLabelsInfo nli = new NodeLabelsInfo();
    nli.getNodeLabels().add("a");
    ntli.getNodeToLabels().put("nid:0", nli);
    response =
        r.path("ws").path("v1").path("cluster")
            .path("replace-node-to-labels")
            .queryParam("user.name", userName)
            .accept(MediaType.APPLICATION_JSON)
            .entity(toJson(ntli, NodeToLabelsInfo.class),
              MediaType.APPLICATION_JSON)
            .post(ClientResponse.class);
        
    // Verify, using node-to-labels
    response =
        r.path("ws").path("v1").path("cluster")
            .path("get-node-to-labels").queryParam("user.name", userName)
            .accept(MediaType.APPLICATION_JSON).get(ClientResponse.class);
    assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
    ntli = response.getEntity(NodeToLabelsInfo.class);
    nli = ntli.getNodeToLabels().get("nid:0");
    assertEquals(1, nli.getNodeLabels().size());
    assertTrue(nli.getNodeLabels().contains("a"));
    
    // Remove all
    response =
        r.path("ws").path("v1").path("cluster")
            .path("nodes").path("nid:0")
            .path("replace-labels")
            .queryParam("user.name", userName)
            .accept(MediaType.APPLICATION_JSON)
            .entity("{\"nodeLabels\"}", MediaType.APPLICATION_JSON)
            .post(ClientResponse.class);
    LOG.info("posted node nodelabel");
    // Verify
    response =
        r.path("ws").path("v1").path("cluster")
            .path("nodes").path("nid:0")
            .path("get-labels").queryParam("user.name", userName)
            .accept(MediaType.APPLICATION_JSON).get(ClientResponse.class);
    assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
    json = response.getEntity(JSONObject.class);
    assertEquals("", json.getString("nodeLabels"));
    
    // Add a label back for auth tests
    response =
        r.path("ws").path("v1").path("cluster")
            .path("nodes").path("nid:0")
            .path("replace-labels")
            .queryParam("user.name", userName)
            .accept(MediaType.APPLICATION_JSON)
            .entity("{\"nodeLabels\": \"a\"}",
              MediaType.APPLICATION_JSON)
            .post(ClientResponse.class);
    LOG.info("posted node nodelabel");

    // Verify
    response =
        r.path("ws").path("v1").path("cluster")
            .path("nodes").path("nid:0")
            .path("get-labels").queryParam("user.name", userName)
            .accept(MediaType.APPLICATION_JSON).get(ClientResponse.class);
    assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
    json = response.getEntity(JSONObject.class);
    assertEquals("a", json.getString("nodeLabels"));
    
    // Auth fail replace labels on node
    response =
        r.path("ws").path("v1").path("cluster")
            .path("nodes").path("nid:0")
            .path("replace-labels")
            .queryParam("user.name", notUserName)
            .accept(MediaType.APPLICATION_JSON)
            .entity("{\"nodeLabels\": [\"b\"]}",
              MediaType.APPLICATION_JSON)
            .post(ClientResponse.class);
    // Verify
    response =
        r.path("ws").path("v1").path("cluster")
            .path("nodes").path("nid:0")
            .path("get-labels").queryParam("user.name", userName)
            .accept(MediaType.APPLICATION_JSON).get(ClientResponse.class);
    assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
    json = response.getEntity(JSONObject.class);
    assertEquals("a", json.getString("nodeLabels"));
    
    // Fail to add a label with post
    response =
        r.path("ws").path("v1").path("cluster")
            .path("add-node-labels").queryParam("user.name", notUserName)
            .accept(MediaType.APPLICATION_JSON)
            .entity("{\"nodeLabels\":\"c\"}", MediaType.APPLICATION_JSON)
            .post(ClientResponse.class);

    // Verify
    response =
        r.path("ws").path("v1").path("cluster")
            .path("get-node-labels").queryParam("user.name", userName)
            .accept(MediaType.APPLICATION_JSON).get(ClientResponse.class);
    assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
    json = response.getEntity(JSONObject.class);
    jarr = json.getJSONArray("nodeLabels");
    assertEquals(2, jarr.length());
    
    // Remove cluster label (succeed, we no longer need it)
    response =
        r.path("ws").path("v1").path("cluster")
            .path("remove-node-labels")
            .queryParam("user.name", userName)
            .accept(MediaType.APPLICATION_JSON)
            .entity("{\"nodeLabels\":\"b\"}", MediaType.APPLICATION_JSON)
            .post(ClientResponse.class);
    // Verify
    response =
        r.path("ws").path("v1").path("cluster")
            .path("get-node-labels").queryParam("user.name", userName)
            .accept(MediaType.APPLICATION_JSON).get(ClientResponse.class);
    assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
    json = response.getEntity(JSONObject.class);
    assertEquals("a", json.getString("nodeLabels"));
    
    
    // Remove cluster label with post
    response =
        r.path("ws").path("v1").path("cluster")
            .path("remove-node-labels")
            .queryParam("user.name", userName)
            .accept(MediaType.APPLICATION_JSON)
            .entity("{\"nodeLabels\":\"a\"}", MediaType.APPLICATION_JSON)
            .post(ClientResponse.class);
    // Verify
    response =
        r.path("ws").path("v1").path("cluster")
            .path("get-node-labels").queryParam("user.name", userName)
            .accept(MediaType.APPLICATION_JSON).get(ClientResponse.class);
    assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
    String res = response.getEntity(String.class);
    assertTrue(res.equals("null"));
  }

  @SuppressWarnings("rawtypes")
  private String toJson(Object nsli, Class klass) throws Exception {
    StringWriter sw = new StringWriter();
    JSONJAXBContext ctx = new JSONJAXBContext(klass);
    JSONMarshaller jm = ctx.createJSONMarshaller();
    jm.marshallToJSON(nsli, sw);
    return sw.toString();
  }

  @SuppressWarnings({ "rawtypes", "unchecked" })
  private Object fromJson(String json, Class klass) throws Exception {
    StringReader sr = new StringReader(json);
    JSONJAXBContext ctx = new JSONJAXBContext(klass);
    JSONUnmarshaller jm = ctx.createJSONUnmarshaller();
    return jm.unmarshalFromJSON(sr, klass);
  }

}
