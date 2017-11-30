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

package org.apache.hadoop.yarn.server.resourcemanager;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ha.HAServiceProtocol;
import org.apache.hadoop.ha.HAServiceProtocol.HAServiceState;
import org.apache.hadoop.yarn.conf.ConfigurationProvider;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.server.resourcemanager.ahs.RMApplicationHistoryWriter;
import org.apache.hadoop.yarn.server.resourcemanager.metrics.SystemMetricsPublisher;
import org.apache.hadoop.yarn.server.resourcemanager.timelineservice.RMTimelineCollectorManager;

/**
 * RMServiceContext class maintains "Always On" services. Services that need to
 * run always irrespective of the HA state of the RM. This is created during
 * initialization of RMContextImpl.
 * <p>
 * <b>Note:</b> If any services to be added in this class, make sure service
 * will be running always irrespective of the HA state of the RM
 */
@Private
@Unstable
public class RMServiceContext {

  private Dispatcher rmDispatcher;
  private boolean isHAEnabled;
  private HAServiceState haServiceState =
      HAServiceProtocol.HAServiceState.INITIALIZING;
  private AdminService adminService;
  private ConfigurationProvider configurationProvider;
  private Configuration yarnConfiguration;
  private RMApplicationHistoryWriter rmApplicationHistoryWriter;
  private SystemMetricsPublisher systemMetricsPublisher;
  private EmbeddedElector elector;
  private final Object haServiceStateLock = new Object();
  private ResourceManager resourceManager;
  private RMTimelineCollectorManager timelineCollectorManager;

  public ResourceManager getResourceManager() {
    return resourceManager;
  }

  public void setResourceManager(ResourceManager rm) {
    this.resourceManager = rm;
  }

  public ConfigurationProvider getConfigurationProvider() {
    return this.configurationProvider;
  }

  public void setConfigurationProvider(
      ConfigurationProvider configurationProvider) {
    this.configurationProvider = configurationProvider;
  }

  public Dispatcher getDispatcher() {
    return this.rmDispatcher;
  }

  void setDispatcher(Dispatcher dispatcher) {
    this.rmDispatcher = dispatcher;
  }

  public EmbeddedElector getLeaderElectorService() {
    return this.elector;
  }

  public void setLeaderElectorService(EmbeddedElector embeddedElector) {
    this.elector = embeddedElector;
  }

  public AdminService getRMAdminService() {
    return this.adminService;
  }

  void setRMAdminService(AdminService service) {
    this.adminService = service;
  }

  void setHAEnabled(boolean rmHAEnabled) {
    this.isHAEnabled = rmHAEnabled;
  }

  public boolean isHAEnabled() {
    return isHAEnabled;
  }

  public HAServiceState getHAServiceState() {
    synchronized (haServiceStateLock) {
      return haServiceState;
    }
  }

  void setHAServiceState(HAServiceState serviceState) {
    synchronized (haServiceStateLock) {
      this.haServiceState = serviceState;
    }
  }

  public RMApplicationHistoryWriter getRMApplicationHistoryWriter() {
    return this.rmApplicationHistoryWriter;
  }

  public void setRMApplicationHistoryWriter(
      RMApplicationHistoryWriter applicationHistoryWriter) {
    this.rmApplicationHistoryWriter = applicationHistoryWriter;
  }

  public void setSystemMetricsPublisher(
      SystemMetricsPublisher metricsPublisher) {
    this.systemMetricsPublisher = metricsPublisher;
  }

  public SystemMetricsPublisher getSystemMetricsPublisher() {
    return this.systemMetricsPublisher;
  }

  public Configuration getYarnConfiguration() {
    return this.yarnConfiguration;
  }

  public void setYarnConfiguration(Configuration yarnConfiguration) {
    this.yarnConfiguration = yarnConfiguration;
  }

  public RMTimelineCollectorManager getRMTimelineCollectorManager() {
    return timelineCollectorManager;
  }

  public void setRMTimelineCollectorManager(
      RMTimelineCollectorManager collectorManager) {
    this.timelineCollectorManager = collectorManager;
  }

  public String getHAZookeeperConnectionState() {
    if (elector == null) {
      return "Could not find leader elector. Verify both HA and automatic "
          + "failover are enabled.";
    } else {
      return elector.getZookeeperConnectionState();
    }
  }
}
