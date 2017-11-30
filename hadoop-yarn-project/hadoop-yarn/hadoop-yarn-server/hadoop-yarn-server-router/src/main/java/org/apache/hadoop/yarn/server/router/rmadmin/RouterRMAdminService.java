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

package org.apache.hadoop.yarn.server.router.rmadmin;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.ipc.StandbyException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.yarn.server.api.ResourceManagerAdministrationProtocol;
import org.apache.hadoop.yarn.server.api.protocolrecords.AddToClusterNodeLabelsRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.AddToClusterNodeLabelsResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.CheckForDecommissioningNodesRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.CheckForDecommissioningNodesResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.RefreshAdminAclsRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.RefreshAdminAclsResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.RefreshClusterMaxPriorityRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.RefreshClusterMaxPriorityResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.RefreshNodesRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.RefreshNodesResourcesRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.RefreshNodesResourcesResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.RefreshNodesResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.RefreshQueuesRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.RefreshQueuesResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.RefreshServiceAclsRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.RefreshServiceAclsResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.RefreshSuperUserGroupsConfigurationRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.RefreshSuperUserGroupsConfigurationResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.RefreshUserToGroupsMappingsRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.RefreshUserToGroupsMappingsResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.RemoveFromClusterNodeLabelsRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.RemoveFromClusterNodeLabelsResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.ReplaceLabelsOnNodeRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.ReplaceLabelsOnNodeResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.UpdateNodeResourceRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.UpdateNodeResourceResponse;
import org.apache.hadoop.yarn.util.LRUCacheHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;

/**
 * RouterRMAdminService is a service that runs on each router that can be used
 * to intercept and inspect {@code ResourceManagerAdministrationProtocol}
 * messages from client to the cluster resource manager. It listens
 * {@code ResourceManagerAdministrationProtocol} messages from the client and
 * creates a request intercepting pipeline instance for each client. The
 * pipeline is a chain of intercepter instances that can inspect and modify the
 * request/response as needed. The main difference with AMRMProxyService is the
 * protocol they implement.
 */
public class RouterRMAdminService extends AbstractService
    implements ResourceManagerAdministrationProtocol {

  private static final Logger LOG =
      LoggerFactory.getLogger(RouterRMAdminService.class);

  private Server server;
  private InetSocketAddress listenerEndpoint;

  // For each user we store an interceptors' pipeline.
  // For performance issue we use LRU cache to keep in memory the newest ones
  // and remove the oldest used ones.
  private Map<String, RequestInterceptorChainWrapper> userPipelineMap;

  public RouterRMAdminService() {
    super(RouterRMAdminService.class.getName());
  }

  @Override
  protected void serviceStart() throws Exception {
    LOG.info("Starting Router RMAdmin Service");
    Configuration conf = getConfig();
    YarnRPC rpc = YarnRPC.create(conf);
    UserGroupInformation.setConfiguration(conf);

    this.listenerEndpoint =
        conf.getSocketAddr(YarnConfiguration.ROUTER_BIND_HOST,
            YarnConfiguration.ROUTER_RMADMIN_ADDRESS,
            YarnConfiguration.DEFAULT_ROUTER_RMADMIN_ADDRESS,
            YarnConfiguration.DEFAULT_ROUTER_RMADMIN_PORT);

    int maxCacheSize =
        conf.getInt(YarnConfiguration.ROUTER_PIPELINE_CACHE_MAX_SIZE,
            YarnConfiguration.DEFAULT_ROUTER_PIPELINE_CACHE_MAX_SIZE);
    this.userPipelineMap = Collections.synchronizedMap(
        new LRUCacheHashMap<String, RequestInterceptorChainWrapper>(
            maxCacheSize, true));

    Configuration serverConf = new Configuration(conf);

    int numWorkerThreads =
        serverConf.getInt(YarnConfiguration.RM_ADMIN_CLIENT_THREAD_COUNT,
            YarnConfiguration.DEFAULT_RM_ADMIN_CLIENT_THREAD_COUNT);

    this.server = rpc.getServer(ResourceManagerAdministrationProtocol.class,
        this, listenerEndpoint, serverConf, null, numWorkerThreads);

    this.server.start();
    LOG.info("Router RMAdminService listening on address: "
        + this.server.getListenerAddress());
    super.serviceStart();
  }

  @Override
  protected void serviceStop() throws Exception {
    LOG.info("Stopping Router RMAdminService");
    if (this.server != null) {
      this.server.stop();
    }
    userPipelineMap.clear();
    super.serviceStop();
  }

  /**
   * Returns the comma separated intercepter class names from the configuration.
   *
   * @param conf
   * @return the intercepter class names as an instance of ArrayList
   */
  private List<String> getInterceptorClassNames(Configuration conf) {
    String configuredInterceptorClassNames =
        conf.get(YarnConfiguration.ROUTER_RMADMIN_INTERCEPTOR_CLASS_PIPELINE,
            YarnConfiguration.DEFAULT_ROUTER_RMADMIN_INTERCEPTOR_CLASS);

    List<String> interceptorClassNames = new ArrayList<String>();
    Collection<String> tempList =
        StringUtils.getStringCollection(configuredInterceptorClassNames);
    for (String item : tempList) {
      interceptorClassNames.add(item.trim());
    }

    return interceptorClassNames;
  }

  private RequestInterceptorChainWrapper getInterceptorChain()
      throws IOException {
    String user = UserGroupInformation.getCurrentUser().getUserName();
    if (!userPipelineMap.containsKey(user)) {
      initializePipeline(user);
    }
    return userPipelineMap.get(user);
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
  protected RMAdminRequestInterceptor createRequestInterceptorChain() {
    Configuration conf = getConfig();

    List<String> interceptorClassNames = getInterceptorClassNames(conf);

    RMAdminRequestInterceptor pipeline = null;
    RMAdminRequestInterceptor current = null;
    for (String interceptorClassName : interceptorClassNames) {
      try {
        Class<?> interceptorClass = conf.getClassByName(interceptorClassName);
        if (RMAdminRequestInterceptor.class
            .isAssignableFrom(interceptorClass)) {
          RMAdminRequestInterceptor interceptorInstance =
              (RMAdminRequestInterceptor) ReflectionUtils
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
                  + RMAdminRequestInterceptor.class.getCanonicalName());
        }
      } catch (ClassNotFoundException e) {
        throw new YarnRuntimeException(
            "Could not instantiate RMAdminRequestInterceptor: "
                + interceptorClassName,
            e);
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
  private void initializePipeline(String user) {
    RequestInterceptorChainWrapper chainWrapper = null;
    synchronized (this.userPipelineMap) {
      if (this.userPipelineMap.containsKey(user)) {
        LOG.info("Request to start an already existing user: {}"
            + " was received, so ignoring.", user);
        return;
      }

      chainWrapper = new RequestInterceptorChainWrapper();
      this.userPipelineMap.put(user, chainWrapper);
    }

    // We register the pipeline instance in the map first and then initialize it
    // later because chain initialization can be expensive and we would like to
    // release the lock as soon as possible to prevent other applications from
    // blocking when one application's chain is initializing
    LOG.info("Initializing request processing pipeline for the user: {}", user);

    try {
      RMAdminRequestInterceptor interceptorChain =
          this.createRequestInterceptorChain();
      interceptorChain.init(user);
      chainWrapper.init(interceptorChain);
    } catch (Exception e) {
      synchronized (this.userPipelineMap) {
        this.userPipelineMap.remove(user);
      }
      throw e;
    }
  }

  /**
   * Private structure for encapsulating RequestInterceptor and user instances.
   *
   */
  @Private
  public static class RequestInterceptorChainWrapper {
    private RMAdminRequestInterceptor rootInterceptor;

    /**
     * Initializes the wrapper with the specified parameters.
     *
     * @param interceptor the first interceptor in the pipeline
     */
    public synchronized void init(RMAdminRequestInterceptor interceptor) {
      this.rootInterceptor = interceptor;
    }

    /**
     * Gets the root request intercepter.
     *
     * @return the root request intercepter
     */
    public synchronized RMAdminRequestInterceptor getRootInterceptor() {
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

  @Override
  public String[] getGroupsForUser(String user) throws IOException {
    RequestInterceptorChainWrapper pipeline = getInterceptorChain();
    return pipeline.getRootInterceptor().getGroupsForUser(user);
  }

  @Override
  public RefreshQueuesResponse refreshQueues(RefreshQueuesRequest request)
      throws StandbyException, YarnException, IOException {
    RequestInterceptorChainWrapper pipeline = getInterceptorChain();
    return pipeline.getRootInterceptor().refreshQueues(request);

  }

  @Override
  public RefreshNodesResponse refreshNodes(RefreshNodesRequest request)
      throws StandbyException, YarnException, IOException {
    RequestInterceptorChainWrapper pipeline = getInterceptorChain();
    return pipeline.getRootInterceptor().refreshNodes(request);

  }

  @Override
  public RefreshSuperUserGroupsConfigurationResponse refreshSuperUserGroupsConfiguration(
      RefreshSuperUserGroupsConfigurationRequest request)
      throws StandbyException, YarnException, IOException {
    RequestInterceptorChainWrapper pipeline = getInterceptorChain();
    return pipeline.getRootInterceptor()
        .refreshSuperUserGroupsConfiguration(request);

  }

  @Override
  public RefreshUserToGroupsMappingsResponse refreshUserToGroupsMappings(
      RefreshUserToGroupsMappingsRequest request)
      throws StandbyException, YarnException, IOException {
    RequestInterceptorChainWrapper pipeline = getInterceptorChain();
    return pipeline.getRootInterceptor().refreshUserToGroupsMappings(request);

  }

  @Override
  public RefreshAdminAclsResponse refreshAdminAcls(
      RefreshAdminAclsRequest request) throws YarnException, IOException {
    RequestInterceptorChainWrapper pipeline = getInterceptorChain();
    return pipeline.getRootInterceptor().refreshAdminAcls(request);

  }

  @Override
  public RefreshServiceAclsResponse refreshServiceAcls(
      RefreshServiceAclsRequest request) throws YarnException, IOException {
    RequestInterceptorChainWrapper pipeline = getInterceptorChain();
    return pipeline.getRootInterceptor().refreshServiceAcls(request);

  }

  @Override
  public UpdateNodeResourceResponse updateNodeResource(
      UpdateNodeResourceRequest request) throws YarnException, IOException {
    RequestInterceptorChainWrapper pipeline = getInterceptorChain();
    return pipeline.getRootInterceptor().updateNodeResource(request);

  }

  @Override
  public RefreshNodesResourcesResponse refreshNodesResources(
      RefreshNodesResourcesRequest request) throws YarnException, IOException {
    RequestInterceptorChainWrapper pipeline = getInterceptorChain();
    return pipeline.getRootInterceptor().refreshNodesResources(request);

  }

  @Override
  public AddToClusterNodeLabelsResponse addToClusterNodeLabels(
      AddToClusterNodeLabelsRequest request) throws YarnException, IOException {
    RequestInterceptorChainWrapper pipeline = getInterceptorChain();
    return pipeline.getRootInterceptor().addToClusterNodeLabels(request);

  }

  @Override
  public RemoveFromClusterNodeLabelsResponse removeFromClusterNodeLabels(
      RemoveFromClusterNodeLabelsRequest request)
      throws YarnException, IOException {
    RequestInterceptorChainWrapper pipeline = getInterceptorChain();
    return pipeline.getRootInterceptor().removeFromClusterNodeLabels(request);

  }

  @Override
  public ReplaceLabelsOnNodeResponse replaceLabelsOnNode(
      ReplaceLabelsOnNodeRequest request) throws YarnException, IOException {
    RequestInterceptorChainWrapper pipeline = getInterceptorChain();
    return pipeline.getRootInterceptor().replaceLabelsOnNode(request);

  }

  @Override
  public CheckForDecommissioningNodesResponse checkForDecommissioningNodes(
      CheckForDecommissioningNodesRequest checkForDecommissioningNodesRequest)
      throws YarnException, IOException {
    RequestInterceptorChainWrapper pipeline = getInterceptorChain();
    return pipeline.getRootInterceptor()
        .checkForDecommissioningNodes(checkForDecommissioningNodesRequest);
  }

  @Override
  public RefreshClusterMaxPriorityResponse refreshClusterMaxPriority(
      RefreshClusterMaxPriorityRequest request)
      throws YarnException, IOException {
    RequestInterceptorChainWrapper pipeline = getInterceptorChain();
    return pipeline.getRootInterceptor().refreshClusterMaxPriority(request);
  }
}
