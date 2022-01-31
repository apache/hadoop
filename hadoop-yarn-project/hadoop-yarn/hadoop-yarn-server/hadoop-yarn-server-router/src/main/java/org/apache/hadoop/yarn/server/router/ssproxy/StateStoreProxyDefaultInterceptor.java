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
package org.apache.hadoop.yarn.server.router.ssproxy;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.server.federation.store.impl.HttpProxyFederationStateStoreConsts;
import org.apache.hadoop.yarn.server.federation.store.records.ApplicationHomeSubCluster;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterId;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterInfo;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterPolicyConfiguration;
import org.apache.hadoop.yarn.server.federation.utils.FederationStateStoreFacade;
import org.apache.hadoop.yarn.server.federation.store.records.dao.ApplicationHomeSubClusterDAO;
import org.apache.hadoop.yarn.server.federation.store.records.dao.ApplicationsHomeSubClusterDAO;
import org.apache.hadoop.yarn.server.federation.store.records.dao.SubClusterDeregisterDAO;
import org.apache.hadoop.yarn.server.federation.store.records.dao.SubClusterHeartbeatDAO;
import org.apache.hadoop.yarn.server.federation.store.records.dao.SubClusterIdDAO;
import org.apache.hadoop.yarn.server.federation.store.records.dao.SubClusterInfoDAO;
import org.apache.hadoop.yarn.server.federation.store.records.dao.SubClusterPoliciesConfigurationsDAO;
import org.apache.hadoop.yarn.server.federation.store.records.dao.SubClusterPolicyConfigurationDAO;
import org.apache.hadoop.yarn.server.federation.store.records.dao.SubClusterStateDAO;
import org.apache.hadoop.yarn.server.federation.store.records.dao.SubClustersInfoDAO;
import org.apache.hadoop.yarn.util.Apps;
import org.apache.hadoop.yarn.webapp.NotFoundException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.PathParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import java.util.List;
import java.util.Map;

/**
 * Extends the {@code StateStoreProxyInterceptor} class and provides an
 * implementation that simply forwards the client requests to the
 * FederationStateStore.
 */
public class StateStoreProxyDefaultInterceptor
    implements StateStoreProxyInterceptor {

  private static final Logger LOG = LoggerFactory
      .getLogger(StateStoreProxyDefaultInterceptor.class.getName());

  private FederationStateStoreFacade stateStoreFacade;
  private @Context Configuration conf;

  @Override
  public void init(String user) {
    this.stateStoreFacade = FederationStateStoreFacade.getInstance();
  }

  @Override
  public void shutdown() {

  }

  @Override
  public void setNextInterceptor(
      StateStoreProxyInterceptor nextInterceptor) {
    throw new YarnRuntimeException(
        this.getClass().getName() + " should be the last interceptor");
  }

  @Override
  public StateStoreProxyInterceptor getNextInterceptor() {
    return null;
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  @Override
  public Configuration getConf() {
    return conf;
  }

  @Override
  public Response registerSubCluster(SubClusterInfoDAO scInfoDAO)
      throws YarnException {
    LOG.info("Registering SubCluster {}", scInfoDAO.subClusterId);
    try {
      stateStoreFacade.registerSubCluster(scInfoDAO.toSubClusterInfo());
    } catch (YarnException e) {
      LOG.error("Could not register SubCluster {}", scInfoDAO.subClusterId, e);
      throw e;
    }
    LOG.info("Registered SubCluster {}", scInfoDAO.subClusterId);
    return Response.status(Response.Status.OK).build();
  }

  @Override
  public Response deregisterSubCluster(
      SubClusterDeregisterDAO deregisterDAO) throws YarnException {
    SubClusterIdDAO scIdDAO = deregisterDAO.getSubClusterId();
    SubClusterStateDAO scStateDAO = deregisterDAO.getScState();
    LOG.info("Deregistering SubCluster {}", scIdDAO.subClusterId);
    try {
      stateStoreFacade
          .deregisterSubCluster(SubClusterId.newInstance(scIdDAO.subClusterId),
              scStateDAO.getSubClusterState());
    } catch (YarnException e) {
      LOG.error("Could not deregister SubCluster {}", scIdDAO.subClusterId, e);
      throw e;
    }
    LOG.info("Deregistered SubCluster {}", scIdDAO.subClusterId);
    return Response.status(Response.Status.OK).build();
  }

  @Override
  public Response subClusterHeartBeat(
      SubClusterHeartbeatDAO heartbeatDAO) throws YarnException {
    SubClusterIdDAO scIdDAO = heartbeatDAO.getSubClusterId();
    SubClusterStateDAO scStateDAO = heartbeatDAO.getScState();
    LOG.info("Heartbeating for SubCluster {}", scIdDAO.subClusterId);
    try {
      stateStoreFacade
          .subClusterHeartBeat(SubClusterId.newInstance(scIdDAO.subClusterId),
              scStateDAO.getSubClusterState(), heartbeatDAO.getCapability());
    } catch (YarnException e) {
      LOG.error("Could not heartbeat for SubCluster {}", scIdDAO.subClusterId,
          e);
      throw e;
    }
    LOG.info("Heartbeat for SubCluster {}", scIdDAO.subClusterId);
    return Response.status(Response.Status.OK).build();
  }

  @Override
  public Response getSubCluster(String subClusterId)
      throws YarnException {

    if (subClusterId == null || subClusterId.isEmpty()) {
      throw new NotFoundException("subClusterId is empty or null");
    }

    LOG.debug("Fetching subcluster info for subcluster: " + subClusterId);

    SubClusterInfo resp =
        stateStoreFacade.getSubCluster(SubClusterId.newInstance(subClusterId));

    LOG.debug("Retrieved subcluster info for subcluster: " + subClusterId
        + ". Subcluster details:" + resp);

    if (resp == null) {
      LOG.warn("Subcluster {} does not exist", subClusterId);
      return Response.status(Response.Status.NOT_FOUND).build();
    }

    return Response.status(Response.Status.OK)
        .entity(new SubClusterInfoDAO(resp)).build();
  }

  @Override
  public Response getSubClusters(boolean filterInactiveSubClusters)
      throws YarnException {
    LOG.debug("Fetching info for all subclusters. filterInactiveSubClusters="
        + filterInactiveSubClusters);

    Map<SubClusterId, SubClusterInfo> resp =
        stateStoreFacade.getSubClusters(filterInactiveSubClusters);

    LOG.info(
        "Retrieved subcluster info for all subclusters filter={} count is={}",
        filterInactiveSubClusters, resp.size());

    return Response.status(Response.Status.OK)
        .entity(new SubClustersInfoDAO(resp.values())).build();
  }

  @Override
  public Response getPoliciesConfigurations() throws YarnException {
    LOG.debug("Fetching policy info for all queues");

    Map<String, SubClusterPolicyConfiguration> resp =
        stateStoreFacade.getPoliciesConfigurations();

    LOG.debug(
        "Retrieved policy info for all queues. Policy count is=" + resp.size());

    return Response.status(Response.Status.OK)
        .entity(new SubClusterPoliciesConfigurationsDAO(resp.values())).build();
  }

  @Override
  public Response getPolicyConfiguration(
      @PathParam(HttpProxyFederationStateStoreConsts.PARAM_QUEUE) String queue)
      throws YarnException {

    if (queue == null || queue.isEmpty()) {
      throw new NotFoundException("queue name is empty or null");
    }

    LOG.debug("Fetching policy info for queue: " + queue);

    SubClusterPolicyConfiguration resp =
        stateStoreFacade.getPolicyConfiguration(queue);

    if (resp == null) {
      LOG.warn("Policy for queue {} does not exist", queue);
      return Response.status(Response.Status.NOT_FOUND).build();
    }

    LOG.debug("Retrieved policy info for queue: " + queue + ". Policy details:"
        + resp);

    return Response.status(Response.Status.OK)
        .entity(new SubClusterPolicyConfigurationDAO(resp)).build();
  }

  @Override
  public Response setPolicyConfiguration(
      SubClusterPolicyConfigurationDAO policyConf) throws YarnException {

    String queue = policyConf.queueName;

    if (queue == null || queue.isEmpty()) {
      throw new NotFoundException("queue name is empty or null");
    }
    LOG.info("Setting policy info for queue: " + queue);

    try {
      stateStoreFacade
          .setPolicyConfiguration(policyConf.toSubClusterPolicyConfiguration());
    } catch (YarnException e) {
      LOG.error("Could not set policy ", e);
      throw e;
    }

    LOG.info("Set policy info for queue: " + queue + ". Policy details:");
    return Response.status(Response.Status.OK).build();
  }

  @Override
  public Response addApplicationHomeSubCluster(
      ApplicationHomeSubClusterDAO appHomeDAO) throws YarnException {
    LOG.info("Adding home SubCluster for application {} as {}",
        appHomeDAO.getAppId(), appHomeDAO.getSubClusterId().subClusterId);
    SubClusterId ret;
    try {
      ret = stateStoreFacade.addApplicationHomeSubCluster(
          appHomeDAO.toApplicationHomeSubCluster());
    } catch (YarnException e) {
      LOG.error("Could not add home SubCluster ", e);
      throw e;
    }
    LOG.info("Added home SubCluster for application {} as {}",
        appHomeDAO.getAppId(), appHomeDAO.getSubClusterId().subClusterId);
    return Response.status(Response.Status.OK).entity(new SubClusterIdDAO(ret))
        .build();
  }

  @Override
  public Response updateApplicationHomeSubCluster(
      ApplicationHomeSubClusterDAO appHomeDAO) throws YarnException {
    LOG.info("Updating home SubCluster for application {} to {}",
        appHomeDAO.getAppId(), appHomeDAO.getSubClusterId());
    SubClusterId ret;
    try {
      stateStoreFacade.updateApplicationHomeSubCluster(
          appHomeDAO.toApplicationHomeSubCluster());
    } catch (YarnException e) {
      LOG.error("Could not update home SubCluster ", e);
      throw e;
    }
    LOG.info("Updating home SubCluster for application {} as {}",
        appHomeDAO.getAppId(), appHomeDAO.getSubClusterId());
    return Response.status(Response.Status.OK).build();
  }

  @Override
  public Response getApplicationHomeSubCluster(
      @PathParam(HttpProxyFederationStateStoreConsts.PARAM_APPID) String appId)
      throws YarnException {
    LOG.debug("Getting home SubCluster for application {}", appId);
    SubClusterId resp;
    try {
      resp = stateStoreFacade.getApplicationHomeSubCluster(Apps.toAppID(appId));
    } catch (YarnException e) {
      LOG.error("Could not get home SubCluster ", e);
      throw e;
    }

    if (resp == null) {
      LOG.warn("Home subcluster for application {} does not exist", appId);
      return Response.status(Response.Status.NOT_FOUND).build();
    }

    LOG.info("Retrieved home SubCluster for application {}", appId);
    return Response.status(Response.Status.OK).entity(new SubClusterIdDAO(resp))
        .build();
  }

  @Override
  public Response getApplicationsHomeSubCluster()
      throws YarnException {
    LOG.debug("Getting home SubCluster for applications");
    List<ApplicationHomeSubCluster> resp;
    try {
      resp = stateStoreFacade.getApplicationsHomeSubCluster();
    } catch (YarnException e) {
      LOG.error("Could not get home SubClusters ", e);
      throw e;
    }

    LOG.debug("Retrieved home SubCluster for applications {}", resp);
    return Response.status(Response.Status.OK)
        .entity(new ApplicationsHomeSubClusterDAO(resp)).build();
  }

  @Override
  public Response deleteApplicationHomeSubCluster(
      @PathParam(HttpProxyFederationStateStoreConsts.PARAM_APPID) String appId)
      throws YarnException {
    LOG.info("Deleting home SubCluster for application {}", appId);
    try {
      stateStoreFacade.deleteApplicationHomeSubCluster(Apps.toAppID(appId));
    } catch (YarnException e) {
      LOG.error("Could not delete home SubCluster ", e);
      throw e;
    }
    LOG.info("Deleted home SubCluster for application {}", appId);
    return Response.status(Response.Status.OK).build();
  }
}
