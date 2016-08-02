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

package org.apache.slider.providers;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.slider.common.SliderKeys;
import org.apache.slider.common.SliderXmlConfKeys;
import org.apache.slider.core.exceptions.BadClusterStateException;
import org.apache.slider.core.exceptions.SliderException;
import org.apache.slider.providers.agent.AgentKeys;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Base class for factories
 */
public abstract class SliderProviderFactory extends Configured {

  public static final String DEFAULT_CLUSTER_TYPE = AgentKeys.PROVIDER_AGENT;
  
  protected static final Logger log =
    LoggerFactory.getLogger(SliderProviderFactory.class);
  public static final String PROVIDER_NOT_FOUND =
    "Unable to find provider of application type %s";

  public SliderProviderFactory(Configuration conf) {
    super(conf);
  }

  protected SliderProviderFactory() {
  }

  public abstract AbstractClientProvider createClientProvider();

  public abstract ProviderService createServerProvider();

  /**
   * Create a provider for a specific application
   * @param application app
   * @return app instance
   * @throws SliderException on any instantiation problem
   */
  public static SliderProviderFactory createSliderProviderFactory(String application) throws
      SliderException {
    Configuration conf = loadSliderConfiguration();
    if (application == null) {
      application = DEFAULT_CLUSTER_TYPE;
    }
    String providerKey =
      String.format(SliderXmlConfKeys.KEY_PROVIDER, application);
    if (application.contains(".")) {
      log.debug("Treating {} as a classname", application);
      String name = "classname.key";
      conf.set(name, application);
      providerKey = name;
    }
    
    Class<? extends SliderProviderFactory> providerClass;
    try {
      providerClass = conf.getClass(providerKey, null, SliderProviderFactory.class);
    } catch (RuntimeException e) {
      throw new BadClusterStateException(e, "Failed to load provider %s: %s", application, e);
    }
    if (providerClass == null) {
      throw new BadClusterStateException(PROVIDER_NOT_FOUND, application);
    }

    Exception ex;
    try {
      SliderProviderFactory providerFactory = providerClass.newInstance();
      providerFactory.setConf(conf);
      return providerFactory;
    } catch (Exception e) {
      ex = e;
    }
    //by here the operation failed and ex is set to the value 
    throw new BadClusterStateException(ex,
                              "Failed to create an instance of %s : %s",
                              providerClass,
                              ex);
  }

  /**
   * Load a configuration with the {@link SliderKeys#SLIDER_XML} resource
   * included
   * @return a configuration instance
   */
  public static Configuration loadSliderConfiguration() {
    Configuration conf = new Configuration();
    conf.addResource(SliderKeys.SLIDER_XML);
    return conf;
  }
}
