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

package org.apache.hadoop.ipc;

import org.apache.hadoop.conf.Configuration;

/**
 * Used by {@link DecayRpcScheduler} to get the cost of users' operations. This
 * is configurable using
 * {@link org.apache.hadoop.fs.CommonConfigurationKeys#IPC_COST_PROVIDER_KEY}.
 */
public interface CostProvider {

  /**
   * Initialize this provider using the given configuration, examining only
   * ones which fall within the provided namespace.
   *
   * @param namespace The namespace to use when looking up configurations.
   * @param conf The configuration
   */
  void init(String namespace, Configuration conf);

  /**
   * Get cost from {@link ProcessingDetails} which will be used in scheduler.
   *
   * @param details Process details
   * @return The cost of the call
   */
  long getCost(ProcessingDetails details);
}
