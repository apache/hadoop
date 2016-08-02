/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.slider.server.appmaster.web;

import org.apache.hadoop.registry.client.api.RegistryOperations;
import org.apache.slider.api.SliderClusterProtocol;
import org.apache.slider.providers.ProviderService;
import org.apache.slider.server.appmaster.AppMasterActionOperations;
import org.apache.slider.server.appmaster.actions.QueueAccess;
import org.apache.slider.server.appmaster.management.MetricsAndMonitoring;
import org.apache.slider.server.appmaster.state.AppState;
import org.apache.slider.server.appmaster.state.RoleStatus;
import org.apache.slider.server.appmaster.state.StateAccessForProviders;
import org.apache.slider.server.appmaster.web.rest.agent.AgentRestOperations;
import org.apache.slider.server.appmaster.web.rest.application.resources.ContentCache;
import org.apache.slider.server.services.security.CertificateManager;

import java.util.Map;

/**
 * Interface to pass information from the Slider AppMaster to the WebApp
 */
public interface WebAppApi {

  /**
   * The {@link AppState} for the current cluster
   */
  StateAccessForProviders getAppState();
  
  /**
   * The {@link ProviderService} for the current cluster
   */
  ProviderService getProviderService();


  /**
   * The {@link CertificateManager} for the current cluster
   */
  CertificateManager getCertificateManager();

  /**
   * Generate a mapping from role name to its {@link RoleStatus}. Be aware that this
   * is a computed value and not just a getter
   */
  Map<String, RoleStatus> getRoleStatusByName();

  /**
   * Returns an interface that can support the agent-based REST operations.
   */
  AgentRestOperations getAgentRestOperations();
  
  /**
   * Registry operations accessor
   * @return registry access
   */
  RegistryOperations getRegistryOperations();

  /**
   * Metrics and monitoring service
   * @return the (singleton) instance
   */
  MetricsAndMonitoring getMetricsAndMonitoring();

  /**
   * Get the queue accessor
   * @return the immediate and scheduled queues
   */
  QueueAccess getQueues();

  /**
   * API for AM operations
   * @return current operations implementation
   */
  AppMasterActionOperations getAMOperations();

  /**
   * Local cache of content
   * @return the cache
   */
  ContentCache getContentCache();
}
