/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.yarn.server.router.ssproxy;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.server.federation.store.impl.HttpProxyFederationStateStoreConsts;
import org.apache.hadoop.yarn.server.router.Router;
import org.apache.hadoop.yarn.server.federation.store.records.dao.ApplicationHomeSubClusterDAO;
import org.apache.hadoop.yarn.server.federation.store.records.dao.SubClusterDeregisterDAO;
import org.apache.hadoop.yarn.server.federation.store.records.dao.SubClusterHeartbeatDAO;
import org.apache.hadoop.yarn.server.federation.store.records.dao.SubClusterInfoDAO;
import org.apache.hadoop.yarn.server.federation.store.records.dao.SubClusterPolicyConfigurationDAO;
import org.apache.hadoop.yarn.server.router.webapp.RouterWebServices;
import org.apache.hadoop.yarn.util.LRUCacheHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.inject.Inject;
import com.google.inject.Singleton;

/**
 * RouterStateStoreProxyWebServices is a service that runs on each router that
 * can be used to intercept and inspect {@link StateStoreWebServiceProtocol}
 * messages from client to the cluster FederationStateStore. It listens
 * {@link StateStoreWebServiceProtocol} REST messages from the client and
 * creates a request intercepting pipeline instance for each client. The
 * pipeline is a chain of {@link StateStoreProxyInterceptor} instances that can
 * inspect and modify the request/response as needed. The main difference with
 * AMRMProxyService is the protocol they implement.
 */
@Singleton
@Path(HttpProxyFederationStateStoreConsts.ROOT)
public class RouterStateStoreProxyWebServices
    implements StateStoreWebServiceProtocol {
  private static final Logger LOG =
      LoggerFactory.getLogger(RouterStateStoreProxyWebServices.class);
  private final Router router;
  private final Configuration conf;
  private @Context HttpServletResponse response;

  private Map<String, RequestInterceptorChainWrapper> userPipelineMap;

  @Inject
  public RouterStateStoreProxyWebServices(final Router router,
      Configuration conf) {
    this.router = router;
    this.conf = conf;
    int maxCacheSize =
        conf.getInt(YarnConfiguration.ROUTER_PIPELINE_CACHE_MAX_SIZE,
            YarnConfiguration.DEFAULT_ROUTER_PIPELINE_CACHE_MAX_SIZE);
    this.userPipelineMap = Collections.synchronizedMap(
        new LRUCacheHashMap<String, RequestInterceptorChainWrapper>(
            maxCacheSize, true));
  }

  /**
   * Returns the comma separated intercepter class names from the configuration.
   *
   * @param config
   * @return the intercepter class names as an instance of ArrayList
   */
  private List<String> getInterceptorClassNames(Configuration config) {
    String configuredInterceptorClassNames = config
        .get(YarnConfiguration.ROUTER_SSPROXY_INTERCEPTOR_CLASS_PIPELINE,
            YarnConfiguration.DEFAULT_ROUTER_SSPROXY_INTERCEPTOR_CLASS);

    List<String> interceptorClassNames = new ArrayList<String>();
    Collection<String> tempList =
        StringUtils.getStringCollection(configuredInterceptorClassNames);
    for (String item : tempList) {
      interceptorClassNames.add(item.trim());
    }

    return interceptorClassNames;
  }

  private void init() {
    // clear content type
    response.setContentType(null);
  }

  @POST
  @Path(HttpProxyFederationStateStoreConsts.PATH_REGISTER)
  @Consumes({ MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML })
  public Response registerSubCluster(
      SubClusterInfoDAO scInfoDAO) throws YarnException {
    init();
    RequestInterceptorChainWrapper pipeline = getInterceptorChain(null);
    return pipeline.getRootInterceptor().registerSubCluster(scInfoDAO);
  }

  @POST
  @Path(HttpProxyFederationStateStoreConsts.PATH_DEREGISTER)
  @Consumes({ MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML })
  public Response deregisterSubCluster(
      SubClusterDeregisterDAO deregisterDAO) throws YarnException {
    init();
    RequestInterceptorChainWrapper pipeline = getInterceptorChain(null);
    return pipeline.getRootInterceptor().deregisterSubCluster(deregisterDAO);
  }

  @POST @Path(HttpProxyFederationStateStoreConsts.PATH_HEARTBEAT)
  @Consumes({ MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML })
  public Response subClusterHeartBeat(
      SubClusterHeartbeatDAO heartbeatDAO) throws YarnException {
    init();
    RequestInterceptorChainWrapper pipeline = getInterceptorChain(null);
    return pipeline.getRootInterceptor().subClusterHeartBeat(heartbeatDAO);
  }

  @GET
  @Path(HttpProxyFederationStateStoreConsts.PATH_SUBCLUSTERS_SCID)
  @Produces({ MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML })
  public Response getSubCluster(@PathParam(HttpProxyFederationStateStoreConsts.PARAM_SCID)
      String subClusterId) throws YarnException {
    init();
    RequestInterceptorChainWrapper pipeline = getInterceptorChain(null);
    return pipeline.getRootInterceptor().getSubCluster(subClusterId);
  }

  @GET
  @Path(HttpProxyFederationStateStoreConsts.PATH_SUBCLUSTERS)
  @Produces({ MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML })
  public Response getSubClusters(
      @QueryParam(HttpProxyFederationStateStoreConsts.QUERY_SC_FILTER)
      @DefaultValue("true") boolean filterInactiveSubClusters)
      throws YarnException {
    init();
    RequestInterceptorChainWrapper pipeline = getInterceptorChain(null);
    return pipeline.getRootInterceptor()
        .getSubClusters(filterInactiveSubClusters);
  }

  @GET
  @Path(HttpProxyFederationStateStoreConsts.PATH_POLICY)
  @Produces({ MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML })
  public Response getPoliciesConfigurations()
      throws YarnException {
    init();
    RequestInterceptorChainWrapper pipeline = getInterceptorChain(null);
    return pipeline.getRootInterceptor().getPoliciesConfigurations();
  }

  @GET
  @Path(HttpProxyFederationStateStoreConsts.PATH_POLICY_QUEUE)
  @Produces({ MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML })
  public Response getPolicyConfiguration(
      @PathParam(HttpProxyFederationStateStoreConsts.PARAM_QUEUE) String queue)
      throws YarnException {
    init();
    RequestInterceptorChainWrapper pipeline = getInterceptorChain(null);
    return pipeline.getRootInterceptor().getPolicyConfiguration(queue);
  }

  @POST
  @Path(HttpProxyFederationStateStoreConsts.PATH_POLICY)
  @Consumes({ MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML })
  public Response setPolicyConfiguration(
      SubClusterPolicyConfigurationDAO policyConf) throws YarnException {
    init();
    RequestInterceptorChainWrapper pipeline = getInterceptorChain(null);
    return pipeline.getRootInterceptor().setPolicyConfiguration(policyConf);
  }

  @POST
  @Path(HttpProxyFederationStateStoreConsts.PATH_APP_HOME)
  @Consumes({ MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML })
  @Produces({ MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML })
  public Response addApplicationHomeSubCluster(
      ApplicationHomeSubClusterDAO appHomeDAO) throws YarnException {
    init();
    RequestInterceptorChainWrapper pipeline = getInterceptorChain(null);
    return pipeline.getRootInterceptor()
        .addApplicationHomeSubCluster(appHomeDAO);
  }

  @PUT
  @Path(HttpProxyFederationStateStoreConsts.PATH_APP_HOME)
  @Consumes({ MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML })
  public Response updateApplicationHomeSubCluster(
      ApplicationHomeSubClusterDAO appHomeDAO) throws YarnException {
    init();
    RequestInterceptorChainWrapper pipeline = getInterceptorChain(null);
    return pipeline.getRootInterceptor()
        .updateApplicationHomeSubCluster(appHomeDAO);
  }

  @GET
  @Path(HttpProxyFederationStateStoreConsts.PATH_APP_HOME_APPID)
  @Produces({ MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML })
  public Response getApplicationHomeSubCluster(
      @PathParam(HttpProxyFederationStateStoreConsts.PARAM_APPID) String appId)
      throws YarnException {
    init();
    RequestInterceptorChainWrapper pipeline = getInterceptorChain(null);
    return pipeline.getRootInterceptor().getApplicationHomeSubCluster(appId);
  }

  @GET
  @Path(HttpProxyFederationStateStoreConsts.PATH_APP_HOME)
  @Produces({ MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML })
  public Response getApplicationsHomeSubCluster()
      throws YarnException {
    init();
    RequestInterceptorChainWrapper pipeline = getInterceptorChain(null);
    return pipeline.getRootInterceptor().getApplicationsHomeSubCluster();
  }

  @DELETE
  @Path(HttpProxyFederationStateStoreConsts.PATH_APP_HOME_APPID)
  @Produces({ MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML })
  public Response deleteApplicationHomeSubCluster(
      @PathParam(HttpProxyFederationStateStoreConsts.PARAM_APPID) String appId)
      throws YarnException {
    init();
    RequestInterceptorChainWrapper pipeline = getInterceptorChain(null);
    return pipeline.getRootInterceptor().deleteApplicationHomeSubCluster(appId);
  }

  @VisibleForTesting
  protected RequestInterceptorChainWrapper getInterceptorChain(
      HttpServletRequest hsr) {
    String user = "";
    if (hsr != null) {
      user = hsr.getRemoteUser();
    }
    try {
      if (user == null || user.equals("")) {
        // Yarn Router user
        user = UserGroupInformation.getCurrentUser().getUserName();
      }
    } catch (IOException e) {
      LOG.error("IOException " + e.getMessage());
    }
    if (!userPipelineMap.containsKey(user)) {
      initializePipeline(user);
    }
    RequestInterceptorChainWrapper chain = userPipelineMap.get(user);
    if (chain != null && chain.getRootInterceptor() != null) {
      return chain;
    }
    return initializePipeline(user);
  }

  /**
   * Gets the Request intercepter chains for all the users.
   *
   * @return the request intercepter chains.
   */
  @VisibleForTesting
  protected Map<String, RequestInterceptorChainWrapper> getPipelines() {
    return this.userPipelineMap;
  }

  /**
   * This method creates and returns reference of the first intercepter in the
   * chain of request intercepter instances.
   *
   * @return the reference of the first intercepter in the chain
   */
  @VisibleForTesting
  protected StateStoreProxyInterceptor createRequestInterceptorChain() {
    List<String> interceptorClassNames = getInterceptorClassNames(conf);

    StateStoreProxyInterceptor pipeline = null;
    StateStoreProxyInterceptor current = null;
    for (String interceptorClassName : interceptorClassNames) {
      try {
        Class<?> interceptorClass = conf.getClassByName(interceptorClassName);
        if (StateStoreProxyInterceptor.class
            .isAssignableFrom(interceptorClass)) {
          StateStoreProxyInterceptor interceptorInstance =
              (StateStoreProxyInterceptor) ReflectionUtils
                  .newInstance(interceptorClass, conf);
          if (pipeline == null) {
            pipeline = interceptorInstance;
            current = interceptorInstance;
            continue;
          } else {
            current.setNextInterceptor(interceptorInstance);
            current = interceptorInstance;
          }
        } else {
          throw new YarnRuntimeException(
              "Class: " + interceptorClassName + " not instance of "
                  + StateStoreProxyInterceptor.class.getCanonicalName());
        }
      } catch (ClassNotFoundException e) {
        throw new YarnRuntimeException(
            "Could not instantiate RESTRequestInterceptor: "
                + interceptorClassName, e);
      }
    }

    if (pipeline == null) {
      throw new YarnRuntimeException(
          "RequestInterceptor pipeline is not configured in the system");
    }
    return pipeline;
  }

  /**
   * Initializes the request intercepter pipeline for the specified user.
   *
   * @param user
   */
  private RequestInterceptorChainWrapper initializePipeline(String user) {
    synchronized (this.userPipelineMap) {
      if (this.userPipelineMap.containsKey(user)) {
        LOG.info("Request to start an already existing user: {}"
            + " was received, so ignoring.", user);
        return userPipelineMap.get(user);
      }

      RequestInterceptorChainWrapper chainWrapper =
          new RequestInterceptorChainWrapper();
      try {
        // We should init the pipeline instance after it is created and then
        // add to the map, to ensure thread safe.
        LOG.info("Initializing request processing pipeline for user: {}", user);
        StateStoreProxyInterceptor interceptorChain =
            this.createRequestInterceptorChain();
        interceptorChain.init(user);
        chainWrapper.init(interceptorChain);
      } catch (Exception e) {
        LOG.error("Init StateStoreProxyInterceptor error for user: " + user, e);
        throw e;
      }
      this.userPipelineMap.put(user, chainWrapper);
      return chainWrapper;
    }
  }

  /**
   * Private structure for encapsulating RequestInterceptor and user instances.
   */
  @Private
  public static class RequestInterceptorChainWrapper {
    private StateStoreProxyInterceptor rootInterceptor;

    /**
     * Initializes the wrapper with the specified parameters.
     *
     * @param interceptor the first interceptor in the pipeline
     */
    public synchronized void init(StateStoreProxyInterceptor interceptor) {
      this.rootInterceptor = interceptor;
    }

    /**
     * Gets the root request intercepter.
     *
     * @return the root request intercepter
     */
    public synchronized StateStoreProxyInterceptor getRootInterceptor() {
      return rootInterceptor;
    }

    /**
     * Shutdown the chain of interceptors when the object is destroyed.
     */
    @Override
    protected void finalize() {
      rootInterceptor.shutdown();
    }
  }
}