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

package org.apache.hadoop.fs.azurebfs.contracts.services;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.ServiceResolutionException;

/**
 * Dependency injected Azure Storage services provider interface.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public interface AbfsServiceProvider {
  /**
   * Returns an instance of resolved injectable service by class name.
   * The injectable service must be configured first to be resolvable.
   * @param clazz the injectable service which is expected to be returned.
   * @param <T> The type of injectable service.
   * @return T instance
   * @throws ServiceResolutionException if the service is not resolvable.
   */
  <T extends InjectableService> T get(Class<T> clazz) throws ServiceResolutionException;
}