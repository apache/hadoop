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
import org.apache.slider.providers.ProviderService;
import org.apache.slider.server.appmaster.AppMasterActionOperations;
import org.apache.slider.server.appmaster.actions.QueueAccess;
import org.apache.slider.server.appmaster.management.MetricsAndMonitoring;
import org.apache.slider.server.appmaster.state.StateAccessForProviders;
import org.apache.slider.server.appmaster.web.rest.application.resources.ContentCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * 
 */
public class WebAppApiImpl implements WebAppApi {
  private static final Logger log = LoggerFactory.getLogger(WebAppApiImpl.class);

  protected final StateAccessForProviders appState;
  protected final ProviderService provider;
  private final RegistryOperations registryOperations;
  private final MetricsAndMonitoring metricsAndMonitoring;
  private final QueueAccess queues;
  private final AppMasterActionOperations appMasterOperations;
  private final ContentCache contentCache;

  public WebAppApiImpl(StateAccessForProviders appState,
      ProviderService provider, RegistryOperations registryOperations,
      MetricsAndMonitoring metricsAndMonitoring, QueueAccess queues,
      AppMasterActionOperations appMasterOperations, ContentCache contentCache) {
    this.appMasterOperations = appMasterOperations;
    this.contentCache = contentCache;
    checkNotNull(appState);
    checkNotNull(provider);
    this.queues = queues;

    this.registryOperations = registryOperations;
    this.appState = appState;
    this.provider = provider;
    this.metricsAndMonitoring = metricsAndMonitoring;
  }

  @Override
  public StateAccessForProviders getAppState() {
    return appState;
  }

  @Override
  public ProviderService getProviderService() {
    return provider;
  }

  @Override
  public RegistryOperations getRegistryOperations() {
    return registryOperations;
  }

  @Override
  public MetricsAndMonitoring getMetricsAndMonitoring() {
    return metricsAndMonitoring;
  }

  @Override
  public QueueAccess getQueues() {
    return queues;
  }


  @Override
  public ContentCache getContentCache() {
    return contentCache;
  }
}
