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

package org.apache.hadoop.yarn.server.api;

import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Unstable;


/**
 * This API is used by NodeManager to decide if a given container's logs
 * should be aggregated at run time.
 */
@Public
@Unstable
public interface ContainerLogAggregationPolicy {

  /**
   * <p>
   * The method used by the NodeManager log aggregation service
   * to initial the policy object with parameters specified by the application
   * or the cluster-wide setting.
   * </p>
   *
   * @param parameters parameters with scheme defined by the policy class.
   */
  void parseParameters(String parameters);

  /**
   * <p>
   * The method used by the NodeManager log aggregation service
   * to ask the policy object if a given container's logs should be aggregated.
   * </p>
   *
   * @param logContext ContainerLogContext
   * @return Whether or not the container's logs should be aggregated.
   */
  boolean shouldDoLogAggregation(ContainerLogContext logContext);
}