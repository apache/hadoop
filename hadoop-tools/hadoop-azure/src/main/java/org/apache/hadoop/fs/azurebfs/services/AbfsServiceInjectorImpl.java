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

package org.apache.hadoop.fs.azurebfs.services;

import java.util.HashMap;
import java.util.Map;

import com.google.inject.AbstractModule;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.azurebfs.contracts.services.AbfsHttpClientFactory;
import org.apache.hadoop.fs.azurebfs.contracts.services.AbfsHttpService;
import org.apache.hadoop.fs.azurebfs.contracts.services.ConfigurationService;
import org.apache.hadoop.fs.azurebfs.contracts.services.TracingService;

/**
 * This class is responsible to configure all the services used by Azure Blob File System.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
class AbfsServiceInjectorImpl extends AbstractModule {
  private final Configuration configuration;
  private final Map<Class, Class> providers;
  private final Map<Class, Object> instances;

  AbfsServiceInjectorImpl(Configuration configuration) {
    this.providers = new HashMap<>();
    this.instances = new HashMap<>();
    this.configuration = configuration;

    this.instances.put(Configuration.class, this.configuration);

    this.providers.put(ConfigurationService.class, ConfigurationServiceImpl.class);

    this.providers.put(AbfsHttpService.class, AbfsHttpServiceImpl.class);
    this.providers.put(AbfsHttpClientFactory.class, AbfsHttpClientFactoryImpl.class);

    this.providers.put(TracingService.class, TracingServiceImpl.class);
  }

  @Override
  protected void configure() {
    for (Map.Entry<Class, Object> entrySet : this.instances.entrySet()) {
      bind(entrySet.getKey()).toInstance(entrySet.getValue());
    }

    for (Map.Entry<Class, Class> entrySet : this.providers.entrySet()) {
      bind(entrySet.getKey()).to(entrySet.getValue());
    }
  }

  protected Configuration getConfiguration() {
    return this.configuration;
  }

  protected Map<Class, Class> getProviders() {
    return this.providers;
  }

  protected Map<Class, Object> getInstances() {
    return this.instances;
  }
}