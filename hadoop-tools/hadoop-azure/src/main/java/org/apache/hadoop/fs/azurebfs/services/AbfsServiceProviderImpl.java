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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.inject.Guice;
import com.google.inject.Injector;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.ServiceResolutionException;
import org.apache.hadoop.fs.azurebfs.contracts.services.AbfsServiceProvider;
import org.apache.hadoop.fs.azurebfs.contracts.services.InjectableService;

/**
 * Dependency injected Azure Storage services provider.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public final class AbfsServiceProviderImpl implements AbfsServiceProvider {
  private static AbfsServiceProviderImpl abfsServiceProvider;
  private final Injector abfsServiceInjector;

  private AbfsServiceProviderImpl(final Configuration configuration) {
    this.abfsServiceInjector = Guice.createInjector(new AbfsServiceInjectorImpl(Preconditions.checkNotNull(configuration, "configuration")));
  }

  @VisibleForTesting
  private AbfsServiceProviderImpl(final Injector abfsServiceInjector) {
    Preconditions.checkNotNull(abfsServiceInjector, "abfsServiceInjector");
    this.abfsServiceInjector = abfsServiceInjector;
  }

  /**
   * Create an instance or returns existing instance of service provider.
   * This method must be marked as synchronized to ensure thread-safety.
   * @param configuration hadoop configuration.
   * @return AbfsServiceProvider the service provider instance.
   */
  public static synchronized AbfsServiceProvider create(final Configuration configuration) {
    if (abfsServiceProvider == null) {
      abfsServiceProvider = new AbfsServiceProviderImpl(configuration);
    }

    return abfsServiceProvider;
  }

  /**
   * Returns current instance of service provider.
   * @return AbfsServiceProvider the service provider instance.
   */
  public static AbfsServiceProvider instance() {
    return abfsServiceProvider;
  }

  @VisibleForTesting
  static synchronized AbfsServiceProvider create(Injector serviceInjector) {
    abfsServiceProvider = new AbfsServiceProviderImpl(serviceInjector);
    return abfsServiceProvider;
  }

  /**
   * Returns an instance of resolved injectable service by class name.
   * The injectable service must be configured first to be resolvable.
   * @param clazz the injectable service which is expected to be returned.
   * @param <T> The type of injectable service.
   * @return T instance
   * @throws ServiceResolutionException if the service is not resolvable.
   */
  @Override
  public <T extends InjectableService> T get(final Class<T> clazz) throws ServiceResolutionException {
    try {
      return this.abfsServiceInjector.getInstance(clazz);
    } catch (Exception ex) {
      throw new ServiceResolutionException(clazz.getSimpleName(), ex);
    }
  }
}