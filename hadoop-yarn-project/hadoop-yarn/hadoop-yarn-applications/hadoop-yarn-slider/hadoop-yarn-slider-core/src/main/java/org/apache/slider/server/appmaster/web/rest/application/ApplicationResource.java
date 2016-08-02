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

package org.apache.slider.server.appmaster.web.rest.application;

import com.google.common.collect.Lists;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.webapp.BadRequestException;
import org.apache.hadoop.yarn.webapp.NotFoundException;
import org.apache.slider.api.types.ApplicationLivenessInformation;
import org.apache.slider.api.types.ComponentInformation;
import org.apache.slider.api.types.ContainerInformation;
import org.apache.slider.api.types.NodeInformation;
import org.apache.slider.api.types.NodeInformationList;
import org.apache.slider.core.conf.AggregateConf;
import org.apache.slider.core.conf.ConfTree;
import org.apache.slider.core.exceptions.NoSuchNodeException;
import org.apache.slider.core.persist.ConfTreeSerDeser;
import org.apache.slider.server.appmaster.actions.ActionFlexCluster;
import org.apache.slider.server.appmaster.actions.AsyncAction;
import org.apache.slider.server.appmaster.actions.QueueAccess;
import org.apache.slider.server.appmaster.state.RoleInstance;
import org.apache.slider.server.appmaster.state.StateAccessForProviders;
import org.apache.slider.server.appmaster.web.WebAppApi;
import org.apache.slider.server.appmaster.web.rest.AbstractSliderResource;
import static org.apache.slider.server.appmaster.web.rest.RestPaths.*;

import org.apache.slider.server.appmaster.web.rest.application.actions.RestActionStop;
import org.apache.slider.server.appmaster.web.rest.application.actions.StopResponse;
import org.apache.slider.server.appmaster.web.rest.application.resources.ContentCache;
import org.apache.slider.server.appmaster.web.rest.application.actions.RestActionPing;
import org.apache.slider.api.types.PingInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Singleton;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.HEAD;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Context;

import static javax.ws.rs.core.MediaType.*;

import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@Singleton
@SuppressWarnings("unchecked")
public class ApplicationResource extends AbstractSliderResource {
  private static final Logger log =
      LoggerFactory.getLogger(ApplicationResource.class);

  public static final List<String> LIVE_ENTRIES = toJsonList("resources",
      "containers",
      "components",
      "nodes",
      "statistics",
      "internal");

  public static final List<String> ROOT_ENTRIES =
      toJsonList("model", "live", "actions");

  public static final List<String> MODEL_ENTRIES =
      toJsonList("desired", "resolved");

  /**
   * This is the cache of all content ... each entry is
   * designed to be self-refreshing on get operations, 
   * so is never very out of date, yet many GETs don't
   * overload the rest of the system.
   */
  private final ContentCache cache;
  private final StateAccessForProviders state;
  private final QueueAccess actionQueues;

  public ApplicationResource(WebAppApi slider) {
    super(slider);
    state = slider.getAppState();
    cache = slider.getContentCache();
    actionQueues = slider.getQueues();
  }

  /**
   * Build a new JSON-marshallable list of string elements
   * @param elements elements
   * @return something that can be returned
   */
  private static List<String> toJsonList(String... elements) {
    return Lists.newArrayList(elements);
  }

  @GET
  @Path("/")
  @Produces({APPLICATION_JSON})
  public List<String> getRoot() {
    markGet(SLIDER_SUBPATH_APPLICATION);
    return ROOT_ENTRIES;
  }

  /**
   * Enum model values: desired and resolved
   * @return the desired and resolved model
   */
  @GET
  @Path(MODEL)
  @Produces({APPLICATION_JSON})
  public List<String> getModel() {
    markGet(SLIDER_SUBPATH_APPLICATION, MODEL);
    return MODEL_ENTRIES;
  }

  @GET
  @Path(MODEL_DESIRED)
  @Produces({APPLICATION_JSON})
  public AggregateConf getModelDesired() {
    markGet(SLIDER_SUBPATH_APPLICATION, MODEL_DESIRED);
    return lookupAggregateConf(MODEL_DESIRED);
  }
  
  @GET
  @Path(MODEL_DESIRED_APPCONF)
  @Produces({APPLICATION_JSON})
  public ConfTree getModelDesiredAppconf() {
    markGet(SLIDER_SUBPATH_APPLICATION, MODEL_DESIRED_APPCONF);
    return lookupConfTree(MODEL_DESIRED_APPCONF);
  }

  @GET
  @Path(MODEL_DESIRED_RESOURCES)
  @Produces({APPLICATION_JSON})
  public ConfTree getModelDesiredResources() {
    markGet(SLIDER_SUBPATH_APPLICATION, MODEL_DESIRED_RESOURCES);
    return lookupConfTree(MODEL_DESIRED_RESOURCES);
  }

/*
  @PUT
  @Path(MODEL_DESIRED_RESOURCES)
//  @Consumes({APPLICATION_JSON, TEXT_PLAIN})
  @Consumes({TEXT_PLAIN})
  @Produces({APPLICATION_JSON})
*/
  public ConfTree setModelDesiredResources(
      String json) {
    markPut(SLIDER_SUBPATH_APPLICATION, MODEL_DESIRED_RESOURCES);
    int size = json != null ? json.length() : 0;
    log.info("PUT {} {} bytes:\n{}", MODEL_DESIRED_RESOURCES,
        size,
        json);
    if (size == 0) {
      log.warn("No JSON in PUT request; rejecting");
      throw new BadRequestException("No JSON in PUT");
    }
    
    try {
      ConfTreeSerDeser serDeser = new ConfTreeSerDeser();
      ConfTree updated = serDeser.fromJson(json);
      queue(new ActionFlexCluster("flex",
          1, TimeUnit.MILLISECONDS,
          updated));
      // return the updated value, even though it potentially hasn't yet
      // been executed
      return updated;
    } catch (Exception e) {
      throw buildException("PUT to "+ MODEL_DESIRED_RESOURCES , e);
    }
  }
  @PUT
  @Path(MODEL_DESIRED_RESOURCES)
  @Consumes({APPLICATION_JSON})
  @Produces({APPLICATION_JSON})
  public ConfTree setModelDesiredResources(
      ConfTree updated) {
    try {
      queue(new ActionFlexCluster("flex",
          1, TimeUnit.MILLISECONDS,
          updated));
      // return the updated value, even though it potentially hasn't yet
      // been executed
      return updated;
    } catch (Exception e) {
      throw buildException("PUT to "+ MODEL_DESIRED_RESOURCES , e);
    }
  }
  
  

  @GET
  @Path(MODEL_RESOLVED)
  @Produces({APPLICATION_JSON})
  public AggregateConf getModelResolved() {
    markGet(SLIDER_SUBPATH_APPLICATION, MODEL_RESOLVED);
    return lookupAggregateConf(MODEL_RESOLVED);
  }

  @GET
  @Path(MODEL_RESOLVED_APPCONF)
  @Produces({APPLICATION_JSON})
  public ConfTree getModelResolvedAppconf() {
    markGet(SLIDER_SUBPATH_APPLICATION, MODEL_RESOLVED_APPCONF);
    return lookupConfTree(MODEL_RESOLVED_APPCONF);
  }

  @GET
  @Path(MODEL_RESOLVED_RESOURCES)
  @Produces({APPLICATION_JSON})
  public ConfTree getModelResolvedResources() {
    markGet(SLIDER_SUBPATH_APPLICATION, MODEL_RESOLVED_RESOURCES);
    return lookupConfTree(MODEL_RESOLVED_RESOURCES);
  }
  
  @GET
  @Path(LIVE)
  @Produces({APPLICATION_JSON})
  public List<String> getLive() {
    markGet(SLIDER_SUBPATH_APPLICATION, LIVE);
    return LIVE_ENTRIES;
  }

  @GET
  @Path(LIVE_RESOURCES)
  @Produces({APPLICATION_JSON})
  public ConfTree getLiveResources() {
    markGet(SLIDER_SUBPATH_APPLICATION, LIVE_RESOURCES);
    return lookupConfTree(LIVE_RESOURCES);
  }
  
  @GET
  @Path(LIVE_CONTAINERS)
  @Produces({APPLICATION_JSON})
  public Map<String, ContainerInformation> getLiveContainers() {
    markGet(SLIDER_SUBPATH_APPLICATION, LIVE_CONTAINERS);
    try {
      return (Map<String, ContainerInformation>)cache.lookup(
          LIVE_CONTAINERS);
    } catch (Exception e) {
      throw buildException(LIVE_CONTAINERS, e);
    }
  }

  @GET
  @Path(LIVE_CONTAINERS + "/{containerId}")
  @Produces({APPLICATION_JSON})
  public ContainerInformation getLiveContainer(
      @PathParam("containerId") String containerId) {
    markGet(SLIDER_SUBPATH_APPLICATION, LIVE_CONTAINERS);
    try {
      RoleInstance id = state.getLiveInstanceByContainerID(containerId);
      return id.serialize();
    } catch (NoSuchNodeException e) {
      throw new NotFoundException("Unknown container: " + containerId);
    } catch (Exception e) {
      throw buildException(LIVE_CONTAINERS + "/"+ containerId, e);
    }
  }

  @GET
  @Path(LIVE_COMPONENTS)
  @Produces({APPLICATION_JSON})
  public Map<String, ComponentInformation> getLiveComponents() {
    markGet(SLIDER_SUBPATH_APPLICATION, LIVE_COMPONENTS);
    try {
      return (Map<String, ComponentInformation>) cache.lookup(LIVE_COMPONENTS);
    } catch (Exception e) {
      throw buildException(LIVE_COMPONENTS, e);
    }
  }
  
  @GET
  @Path(LIVE_COMPONENTS + "/{component}")
  @Produces({APPLICATION_JSON})
  public ComponentInformation getLiveComponent(
      @PathParam("component") String component) {
    markGet(SLIDER_SUBPATH_APPLICATION, LIVE_COMPONENTS);
    try {
      return state.getComponentInformation(component);
    } catch (YarnRuntimeException e) {
      throw new NotFoundException("Unknown component: " + component);
    } catch (Exception e) {
      throw buildException(LIVE_CONTAINERS +"/" + component, e);
    }
  }

  /**
   * Liveness information for the application as a whole
   * @return snapshot of liveness
   */
  @GET
  @Path(LIVE_LIVENESS)
  @Produces({APPLICATION_JSON})
  public ApplicationLivenessInformation getLivenessInformation() {
    markGet(SLIDER_SUBPATH_APPLICATION, LIVE_LIVENESS);
    try {
      return state.getApplicationLivenessInformation();
    } catch (Exception e) {
      throw buildException(LIVE_CONTAINERS, e);
    }
  }

/*
TODO: decide what structure to return here, then implement

  @GET
  @Path(LIVE_LIVENESS + "/{component}")
  @Produces({APPLICATION_JSON})
  public ApplicationLivenessInformation getLivenessForComponent(
      @PathParam("component") String component) {
    markGet(SLIDER_SUBPATH_APPLICATION, LIVE_COMPONENTS);
    try {
      RoleStatus roleStatus = state.lookupRoleStatus(component);
      ApplicationLivenessInformation info = new ApplicationLivenessInformation();
      info.requested = roleStatus.getRequested();
      info.allRequestsSatisfied = info.requested == 0;
      return info;
    } catch (YarnRuntimeException e) {
      throw new NotFoundException("Unknown component: " + component);
    } catch (Exception e) {
      throw buildException(LIVE_LIVENESS + "/" + component, e);
    }
  }
*/


  @GET
  @Path(LIVE_NODES)
  @Produces({APPLICATION_JSON})
  public NodeInformationList getLiveNodes() {
    markGet(SLIDER_SUBPATH_APPLICATION, LIVE_COMPONENTS);
    try {
      return (NodeInformationList) cache.lookup(LIVE_NODES);
    } catch (Exception e) {
      throw buildException(LIVE_COMPONENTS, e);
    }
  }

  @GET
  @Path(LIVE_NODES + "/{hostname}")
  @Produces({APPLICATION_JSON})
  public NodeInformation getLiveNode(@PathParam("hostname") String hostname) {
    markGet(SLIDER_SUBPATH_APPLICATION, LIVE_COMPONENTS);
    try {
      NodeInformation ni = state.getNodeInformation(hostname);
      if (ni != null) {
        return ni;
      } else {
        throw new NotFoundException("Unknown node: " + hostname);
      }
    } catch (NotFoundException e) {
      throw e;
    } catch (Exception e) {
      throw buildException(LIVE_COMPONENTS + "/" + hostname, e);
    }
  }

  /**
   * Statistics of the application
   * @return snapshot statistics
   */
  @GET
  @Path(LIVE_STATISTICS)
  @Produces({APPLICATION_JSON})
  public Map<String, Integer> getLiveStatistics() {
    markGet(SLIDER_SUBPATH_APPLICATION, LIVE_LIVENESS);
    try {
      return (Map<String, Integer>) cache.lookup(LIVE_STATISTICS);
    } catch (Exception e) {
      throw buildException(LIVE_STATISTICS, e);
    }
  }

  /**
   * Helper method; look up an aggregate configuration in the cache from
   * a key, or raise an exception
   * @param key key to resolve
   * @return the configuration
   * @throws WebApplicationException on a failure
   */
  protected AggregateConf lookupAggregateConf(String key) {
    try {
      return (AggregateConf) cache.lookup(key);
    } catch (Exception e) {
      throw buildException(key, e);
    }
  }


  /**
   * Helper method; look up an conf tree in the cache from
   * a key, or raise an exception
   * @param key key to resolve
   * @return the configuration
   * @throws WebApplicationException on a failure
   */
  protected ConfTree lookupConfTree(String key) {
    try {
      return (ConfTree) cache.lookup(key);
    } catch (Exception e) {
      throw buildException(key, e);
    }
  }

  /* ************************************************************************
  
  ACTION PING
  
  **************************************************************************/
  
  @GET
  @Path(ACTION_PING)
  @Produces({APPLICATION_JSON})
  public PingInformation actionPingGet(@Context HttpServletRequest request,
      @Context UriInfo uriInfo) {
    markGet(SLIDER_SUBPATH_APPLICATION, ACTION_PING);
    return new RestActionPing().ping(request, uriInfo, "");
  }
  
  @POST
  @Path(ACTION_PING)
  @Produces({APPLICATION_JSON})
  public PingInformation actionPingPost(@Context HttpServletRequest request,
      @Context UriInfo uriInfo,
      String body) {
    markPost(SLIDER_SUBPATH_APPLICATION, ACTION_PING);
    return new RestActionPing().ping(request, uriInfo, body);
  }
  
  @PUT
  @Path(ACTION_PING)
  @Consumes({TEXT_PLAIN})
  @Produces({APPLICATION_JSON})
  public PingInformation actionPingPut(@Context HttpServletRequest request,
      @Context UriInfo uriInfo,
      String body) {
    markPut(SLIDER_SUBPATH_APPLICATION, ACTION_PING);
    return new RestActionPing().ping(request, uriInfo, body);
  }
  
  @DELETE
  @Path(ACTION_PING)
  @Consumes({APPLICATION_JSON})
  @Produces({APPLICATION_JSON})
  public PingInformation actionPingDelete(@Context HttpServletRequest request,
      @Context UriInfo uriInfo) {
    markDelete(SLIDER_SUBPATH_APPLICATION, ACTION_PING);
    return new RestActionPing().ping(request, uriInfo, "");
  }
  
  @HEAD
  @Path(ACTION_PING)
  public Object actionPingHead(@Context HttpServletRequest request,
      @Context UriInfo uriInfo) {
    mark("HEAD", SLIDER_SUBPATH_APPLICATION, ACTION_PING);
    return new RestActionPing().ping(request, uriInfo, "");
  }
  
  /* ************************************************************************
  
  ACTION STOP
  
  **************************************************************************/


  @POST
  @Path(ACTION_STOP)
  @Produces({APPLICATION_JSON})
  public StopResponse actionStop(@Context HttpServletRequest request,
      @Context UriInfo uriInfo,
      String body) {
    markPost(SLIDER_SUBPATH_APPLICATION, ACTION_STOP);
    return new RestActionStop(slider).stop(request, uriInfo, body);
  }

  /**
   * Schedule an action
   * @param action for delayed execution
   */
  public void schedule(AsyncAction action) {
    actionQueues.schedule(action);
  }

  /**
   * Put an action on the immediate queue -to be executed when the queue
   * reaches it.
   * @param action action to queue
   */
  public void queue(AsyncAction action) {
    actionQueues.put(action);
  }
}
