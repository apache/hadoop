/*
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

package org.apache.slider.client.rest;

import com.google.common.base.Preconditions;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientHandlerException;
import com.sun.jersey.api.client.GenericType;
import com.sun.jersey.api.client.UniformInterfaceException;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.api.representation.Form;
import org.apache.commons.lang.StringUtils;
import org.apache.slider.api.types.ApplicationLivenessInformation;
import org.apache.slider.api.types.ComponentInformation;
import org.apache.slider.api.types.ContainerInformation;
import org.apache.slider.api.SliderApplicationApi;
import org.apache.slider.api.types.NodeInformation;
import org.apache.slider.api.types.NodeInformationList;
import org.apache.slider.core.conf.AggregateConf;
import org.apache.slider.core.conf.ConfTree;
import org.apache.slider.core.conf.ConfTreeOperations;
import org.apache.slider.core.exceptions.ExceptionConverter;
import org.apache.slider.core.restclient.HttpVerb;
import org.apache.slider.api.types.PingInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.core.MediaType;
import java.io.IOException;
import java.util.Map;

import static org.apache.slider.server.appmaster.web.rest.RestPaths.*;

/**
 * Implementation of the {@link SliderApplicationApi}
 */
public class SliderApplicationApiRestClient extends BaseRestClient
      implements SliderApplicationApi {
  private static final Logger log =
      LoggerFactory.getLogger(SliderApplicationApiRestClient.class);
  private WebResource appResource;

  /**
   * Create an instance
   * @param jerseyClient jersey client for operations
   * @param appResource resource of application API
   */
  public SliderApplicationApiRestClient(Client jerseyClient,
      WebResource appResource) {
    super(jerseyClient);
    this.appResource = appResource;
  }

  /**
   * Create an instance
   * @param jerseyClient jersey client for operations
   * @param appmaster URL of appmaster/proxy to AM
   */
  public SliderApplicationApiRestClient(Client jerseyClient, String appmaster) {
    super(jerseyClient);
    WebResource amResource = jerseyClient.resource(appmaster);
    amResource.type(MediaType.APPLICATION_JSON);
    this.appResource = amResource.path(SLIDER_PATH_APPLICATION);
  }


  @Override
  public String toString() {
    final StringBuilder sb =
        new StringBuilder("SliderApplicationApiRestClient{");
    sb.append("appResource=").append(appResource);
    sb.append('}');
    return sb.toString();
  }

  /**
   * Create a resource under the application path
   * @param subpath path under application
   * @return a resource under the application path
   */
  public WebResource applicationResource(String subpath) {
    Preconditions.checkArgument(!StringUtils.isEmpty(subpath),
        "empty path");
    Preconditions.checkNotNull(appResource, "Null app resource");
    return appResource.path(subpath);
  }
  
  /**
   * Get operation against a path under the Application
   * @param <T> type expected
   * @param subpath path
   * @param c class to instantiate
   * @return instance
   * @throws IOException on any problem
   */
  public <T> T getApplicationResource(String subpath, Class<T> c)
      throws IOException {
    return appResourceOperation(HttpVerb.GET, subpath, c);
  } 
  
  /**
   * Get operation against a path under the Application
   * @param <T> type expected
   * @param subpath path
   * @param t type info
   * @return instance
   * @throws IOException on any problem
   */
  public <T> T getApplicationResource(String subpath, GenericType<T> t)
      throws IOException {
    return appResourceOperation(HttpVerb.GET, subpath, t);
  }

  /**
   * 
   * @param method method to exec
   * @param <T> type expected
   * @param subpath path
   * @param c class to instantiate
   * @return instance
   * @throws IOException on any problem
   */
  public <T> T appResourceOperation(HttpVerb method, String subpath, Class<T> c)
      throws IOException {
    return exec(method, applicationResource(subpath), c);
  }
  
  
  /**
   * Get operation against a path under the Application
   * @param <T> type expected
   * @param subpath path
   * @param t type info
   * @return instance
   * @throws IOException on any problem
   */
  public <T> T appResourceOperation(HttpVerb method, String subpath,
      GenericType<T> t)
      throws IOException {
    return exec(method, applicationResource(subpath), t);
  }


  @Override
  public AggregateConf getDesiredModel() throws IOException {
    return getApplicationResource(MODEL_DESIRED, AggregateConf.class);
  }
  
  @Override
  public ConfTreeOperations getDesiredAppconf() throws IOException {
    ConfTree resource =
        getApplicationResource(MODEL_DESIRED_APPCONF, ConfTree.class);
    return new ConfTreeOperations(resource); 
  }

  @Override
  public ConfTreeOperations getDesiredResources() throws IOException {
    ConfTree resource =
        getApplicationResource(MODEL_DESIRED_RESOURCES, ConfTree.class);
    return new ConfTreeOperations(resource); 
  }

  @Override
  public void putDesiredResources(ConfTree updated) throws IOException {
    WebResource resource = applicationResource(MODEL_DESIRED_RESOURCES);
    try {

      // put operation. The result is discarded; it does help validate
      // that the operation returned a JSON data structure as well as a 200
      // response.

      resource.accept(MediaType.APPLICATION_JSON_TYPE)
              .type(MediaType.APPLICATION_JSON_TYPE)
              .entity(updated)
              .put(ConfTree.class);
    } catch (ClientHandlerException ex) {
        throw ExceptionConverter.convertJerseyException("PUT",
            resource.getURI().toString(),
            ex);
      } catch (UniformInterfaceException ex) {
      throw ExceptionConverter.convertJerseyException("PUT",
          resource.getURI().toString(), ex);
      }
  }

  @Override
  public AggregateConf getResolvedModel() throws IOException {
    return getApplicationResource(MODEL_RESOLVED, AggregateConf.class);
  }


  @Override
  public ConfTreeOperations getResolvedAppconf() throws IOException {
    ConfTree resource =
        getApplicationResource(MODEL_RESOLVED_APPCONF, ConfTree.class);
    return new ConfTreeOperations(resource); 
  }

  @Override
  public ConfTreeOperations getResolvedResources() throws IOException {
    ConfTree resource =
        getApplicationResource(MODEL_RESOLVED_RESOURCES, ConfTree.class);
    return new ConfTreeOperations(resource); 
  }

  @Override
  public ConfTreeOperations getLiveResources() throws IOException {
    ConfTree resource =
        getApplicationResource(LIVE_RESOURCES, ConfTree.class);
    return new ConfTreeOperations(resource); 
  }

  @Override
  public Map<String, ContainerInformation> enumContainers() throws
      IOException {
    return getApplicationResource(LIVE_CONTAINERS,
        new GenericType<Map<String, ContainerInformation>>() {
        });
  }

  @Override
  public ContainerInformation getContainer(String containerId) throws
      IOException {
    return getApplicationResource(LIVE_CONTAINERS + "/" + containerId,
        ContainerInformation.class);
  }

  @Override
  public Map<String, ComponentInformation> enumComponents() throws
      IOException {
    return getApplicationResource(LIVE_COMPONENTS,
        new GenericType<Map<String, ComponentInformation>>() { });
  }

  @Override
  public ComponentInformation getComponent(String componentName) throws
      IOException {
    return getApplicationResource(LIVE_COMPONENTS + "/" + componentName,
        ComponentInformation.class);
  }

  @Override
  public NodeInformationList getLiveNodes() throws IOException {
    return getApplicationResource(LIVE_NODES, NodeInformationList.class);
  }

  @Override
  public NodeInformation getLiveNode(String hostname) throws IOException {
    return getApplicationResource(LIVE_NODES + "/" + hostname,
        NodeInformation.class);
  }

  @Override
  public PingInformation ping(String text) throws IOException {
    return pingPost(text);
  }
  
  /**
   * Ping as a GET
   * @param text text to include
   * @return the response
   * @throws IOException on any failure
   */
  public PingInformation pingGet(String text) throws IOException {
    WebResource pingResource = applicationResource(ACTION_PING);
    pingResource.getUriBuilder().queryParam("body", text);
    return pingResource.get(PingInformation.class);
  }
  
  /**
   * Ping as a POST
   * @param text text to include
   * @return the response
   * @throws IOException on any failure
   */
  public PingInformation pingPost(String text) throws IOException {
    WebResource pingResource = applicationResource(ACTION_PING);
    Form f = new Form();
    f.add("text", text);
    return pingResource
        .type(MediaType.APPLICATION_JSON_TYPE)
        .post(PingInformation.class, f);
  }
  
  /**
   * Ping as a POST
   * @param text text to include
   * @return the response
   * @throws IOException on any failure
   */
  public PingInformation pingPut(String text) throws IOException {
    WebResource pingResource = applicationResource(ACTION_PING);
    Form f = new Form();
    return pingResource
        .type(MediaType.TEXT_PLAIN)
        .put(PingInformation.class, text);
  }

  @Override
  public void stop(String text) throws IOException {
    WebResource resource = applicationResource(ACTION_STOP);
    resource.post(text);
  }

  @Override
  public ApplicationLivenessInformation getApplicationLiveness() throws IOException {
    return getApplicationResource(LIVE_LIVENESS,
        ApplicationLivenessInformation.class);
  }
}
