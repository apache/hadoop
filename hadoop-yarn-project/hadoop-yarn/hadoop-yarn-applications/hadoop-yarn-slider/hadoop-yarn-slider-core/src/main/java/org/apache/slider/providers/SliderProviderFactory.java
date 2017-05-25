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

import org.apache.slider.api.resource.Artifact;
import org.apache.slider.core.exceptions.SliderException;
import org.apache.slider.providers.docker.DockerProviderFactory;
import org.apache.slider.providers.tarball.TarballProviderFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Base class for factories.
 */
public abstract class SliderProviderFactory {
  protected static final Logger LOG =
      LoggerFactory.getLogger(SliderProviderFactory.class);

  protected SliderProviderFactory() {}

  public abstract AbstractClientProvider createClientProvider();

  public abstract ProviderService createServerProvider();

  public static synchronized ProviderService getProviderService(Artifact
      artifact) {
    return createSliderProviderFactory(artifact).createServerProvider();
  }

  public static synchronized AbstractClientProvider getClientProvider(Artifact
      artifact) {
    return createSliderProviderFactory(artifact).createClientProvider();
  }

  /**
   * Create a provider for a specific application
   * @param artifact artifact
   * @return provider factory
   * @throws SliderException on any instantiation problem
   */
  public static synchronized SliderProviderFactory createSliderProviderFactory(
      Artifact artifact) {
    if (artifact == null || artifact.getType() == null) {
      LOG.debug("Loading service provider type default");
      return DefaultProviderFactory.getInstance();
    }
    LOG.debug("Loading service provider type {}", artifact.getType());
    switch (artifact.getType()) {
      // TODO add handling for custom types?
      // TODO handle application
      case DOCKER:
        return DockerProviderFactory.getInstance();
      case TARBALL:
        return TarballProviderFactory.getInstance();
      default:
        throw new IllegalArgumentException(String.format("Resolution error, " +
                "%s should not be passed to createSliderProviderFactory",
            artifact.getType()));
    }
  }
}
