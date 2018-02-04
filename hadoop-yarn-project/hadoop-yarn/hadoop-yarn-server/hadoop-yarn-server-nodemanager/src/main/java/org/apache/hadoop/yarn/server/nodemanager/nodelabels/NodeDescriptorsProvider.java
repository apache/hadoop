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

package org.apache.hadoop.yarn.server.nodemanager.nodelabels;

import java.util.Set;

/**
 * Interface which will be responsible for fetching node descriptors,
 * a node descriptor could be a
 * {@link org.apache.hadoop.yarn.api.records.NodeLabel} or a
 * {@link org.apache.hadoop.yarn.api.records.NodeAttribute}.
 */
public interface NodeDescriptorsProvider<T> {

  /**
   * Provides the descriptors. The provider is expected to give same
   * descriptors continuously until there is a change.
   * If null is returned then an empty set is assumed by the caller.
   *
   * @return Set of node descriptors applicable for a node
   */
  Set<T> getDescriptors();

  /**
   * Sets a set of descriptors to the provider.
   * @param descriptors node descriptors.
   */
  void setDescriptors(Set<T> descriptors);
}