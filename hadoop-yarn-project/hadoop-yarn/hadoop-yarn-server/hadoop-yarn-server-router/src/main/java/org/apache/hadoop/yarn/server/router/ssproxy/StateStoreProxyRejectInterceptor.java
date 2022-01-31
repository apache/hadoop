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
import org.apache.hadoop.yarn.server.federation.store.records.dao.ApplicationHomeSubClusterDAO;
import org.apache.hadoop.yarn.server.federation.store.records.dao.SubClusterDeregisterDAO;
import org.apache.hadoop.yarn.server.federation.store.records.dao.SubClusterHeartbeatDAO;
import org.apache.hadoop.yarn.server.federation.store.records.dao.SubClusterInfoDAO;
import org.apache.hadoop.yarn.server.federation.store.records.dao.SubClusterPolicyConfigurationDAO;
import org.apache.hadoop.yarn.server.federation.utils.FederationStateStoreFacade;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;

/**
 * Extends the {@code StateStoreProxyInterceptor} class and provides an
 * implementation that simply rejects all the client requests.
 */
public class StateStoreProxyRejectInterceptor
    implements StateStoreProxyInterceptor {

  private static final Logger LOG =
      LoggerFactory.getLogger(StateStoreProxyRejectInterceptor.class.getName());

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
    return Response.status(501).entity(
        "Request reached " + StateStoreProxyRejectInterceptor.class.getName())
        .build();
  }

  @Override
  public Response deregisterSubCluster(
      SubClusterDeregisterDAO deregisterDAO) throws YarnException {
    return Response.status(501).entity(
        "Request reached " + StateStoreProxyRejectInterceptor.class.getName())
        .build();
  }

  @Override
  public Response subClusterHeartBeat(
      SubClusterHeartbeatDAO heartbeatDAO) throws YarnException {
    return Response.status(501).entity(
        "Request reached " + StateStoreProxyRejectInterceptor.class.getName())
        .build();
  }

  @Override
  public Response getSubCluster(String subClusterId)
      throws YarnException {
    return Response.status(501).entity(
        "Request reached " + StateStoreProxyRejectInterceptor.class.getName())
        .build();
  }

  @Override
  public Response getSubClusters(boolean filterInactiveSubclusters)
      throws YarnException {
    return Response.status(501).entity(
        "Request reached " + StateStoreProxyRejectInterceptor.class.getName())
        .build();
  }

  @Override
  public Response getPoliciesConfigurations() throws YarnException {
    return Response.status(501).entity(
        "Request reached " + StateStoreProxyRejectInterceptor.class.getName())
        .build();
  }

  @Override
  public Response getPolicyConfiguration(String queue)
      throws YarnException {
    return Response.status(501).entity(
        "Request reached " + StateStoreProxyRejectInterceptor.class.getName())
        .build();
  }

  @Override
  public Response setPolicyConfiguration(
      SubClusterPolicyConfigurationDAO policyConf) throws YarnException {
    return Response.status(501).entity(
        "Request reached " + StateStoreProxyRejectInterceptor.class.getName())
        .build();
  }

  @Override
  public Response addApplicationHomeSubCluster(
      ApplicationHomeSubClusterDAO appHomeDAO) throws YarnException {
    return Response.status(501).entity(
        "Request reached " + StateStoreProxyRejectInterceptor.class.getName())
        .build();
  }

  @Override
  public Response updateApplicationHomeSubCluster(
      ApplicationHomeSubClusterDAO appHomeDAO) throws YarnException {
    return Response.status(501).entity(
        "Request reached " + StateStoreProxyRejectInterceptor.class.getName())
        .build();
  }

  @Override
  public Response getApplicationHomeSubCluster(String appId)
      throws YarnException {
    return Response.status(501).entity(
        "Request reached " + StateStoreProxyRejectInterceptor.class.getName())
        .build();
  }

  @Override
  public Response getApplicationsHomeSubCluster()
      throws YarnException {
    return Response.status(501).entity(
        "Request reached " + StateStoreProxyRejectInterceptor.class.getName())
        .build();
  }

  @Override
  public Response deleteApplicationHomeSubCluster(String appId)
      throws YarnException {
    return Response.status(501).entity(
        "Request reached " + StateStoreProxyRejectInterceptor.class.getName())
        .build();
  }
}

