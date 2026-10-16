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

package org.apache.hadoop.yarn.server.resourcemanager.federation;

import java.io.StringWriter;

import javax.ws.rs.core.MediaType;
import javax.xml.bind.JAXBContext;
import javax.xml.bind.Marshaller;

import org.apache.hadoop.yarn.server.federation.store.FederationStateStore;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterHeartbeatRequest;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterId;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterState;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.JAXBContextResolver;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.ClusterMetricsInfo;

import org.eclipse.persistence.jaxb.MarshallerProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Periodic heart beat from a <code>ResourceManager</code> participating in
 * federation to indicate liveliness. The heart beat publishes the current
 * capabilities as represented by {@link ClusterMetricsInfo} of the sub cluster.
 *
 */
public class FederationStateStoreHeartbeat implements Runnable {

  private static final Logger LOG =
      LoggerFactory.getLogger(FederationStateStoreHeartbeat.class);

  private final SubClusterId subClusterId;
  private final FederationStateStore stateStoreService;
  private final ResourceScheduler rs;
  private String capability;
  private JAXBContextResolver resolver;

  public FederationStateStoreHeartbeat(
      SubClusterId subClusterId,
      FederationStateStore stateStoreClient,
      ResourceScheduler scheduler,
      JAXBContextResolver resolver
  ) {
    this.stateStoreService = stateStoreClient;
    this.subClusterId = subClusterId;
    this.rs = scheduler;
    this.resolver = resolver;
    LOG.info("Initialized Federation membership for cluster with timestamp: {}. ",
        ResourceManager.getClusterTimeStamp());
  }

  /**
   * Get the current cluster state as a JSON string representation of the
   * {@link ClusterMetricsInfo}.
   */
  private void updateClusterState() {
    try {
      // get the current state
      ClusterMetricsInfo clusterMetricsInfo = new ClusterMetricsInfo(rs);
      JAXBContext context = resolver.getContext(ClusterMetricsInfo.class);
      Marshaller marshaller = context.createMarshaller();
      marshaller.setProperty(MarshallerProperties.MEDIA_TYPE, MediaType.APPLICATION_JSON);
      StringWriter stringWriter = new StringWriter();
      marshaller.marshal(clusterMetricsInfo, stringWriter);
      capability = stringWriter.toString();
    } catch (Exception e) {
      LOG.warn("Exception while trying to generate cluster state,"
          + " so reverting to last know state.", e);
    }
  }

  @Override
  public synchronized void run() {
    try {
      updateClusterState();
      SubClusterHeartbeatRequest request = SubClusterHeartbeatRequest
          .newInstance(subClusterId, SubClusterState.SC_RUNNING, capability);
      stateStoreService.subClusterHeartbeat(request);
      LOG.debug("Sending the heartbeat with capability: {}", capability);
    } catch (Exception e) {
      LOG.warn("Exception when trying to heartbeat.", e);
    }
  }
}