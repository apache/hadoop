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

package org.apache.hadoop.yarn.service.provider;

import org.apache.hadoop.yarn.service.provider.defaultImpl.DefaultProviderFactory;
import org.apache.hadoop.yarn.service.api.records.Artifact;
import org.apache.hadoop.yarn.service.provider.docker.DockerProviderFactory;
import org.apache.hadoop.yarn.service.provider.tarball.TarballProviderFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Base class for factories.
 */
public abstract class ProviderFactory {
  protected static final Logger LOG =
      LoggerFactory.getLogger(ProviderFactory.class);

  protected ProviderFactory() {}

  public abstract AbstractClientProvider createClientProvider();

  public abstract ProviderService createServerProvider();

  public static synchronized ProviderService getProviderService(Artifact
      artifact) {
    return createServiceProviderFactory(artifact).createServerProvider();
  }

  public static synchronized AbstractClientProvider getClientProvider(Artifact
      artifact) {
    return createServiceProviderFactory(artifact).createClientProvider();
  }

  /**
   * Create a provider for a specific service
   * @param artifact artifact
   * @return provider factory
   */
  public static synchronized ProviderFactory createServiceProviderFactory(
      Artifact artifact) {
    if (artifact == null || artifact.getType() == null) {
      LOG.debug("Loading service provider type default");
      return DefaultProviderFactory.getInstance();
    }
    LOG.debug("Loading service provider type {}", artifact.getType());
    switch (artifact.getType()) {
      // TODO add handling for custom types?
      // TODO handle service
      case DOCKER:
        return DockerProviderFactory.getInstance();
      case TARBALL:
        return TarballProviderFactory.getInstance();
      default:
        throw new IllegalArgumentException(String.format("Resolution error, " +
                "%s should not be passed to createServiceProviderFactory",
            artifact.getType()));
    }
  }
}
