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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.common;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.placement.AppPlacementAllocator;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.placement.LocalityAppPlacementAllocator;
/**
 * This class will keep all Scheduling env's names which will help in
 * placement calculations.
 */
public class ApplicationSchedulingConfig {
  @InterfaceAudience.Private
  public static final String ENV_APPLICATION_PLACEMENT_TYPE_CLASS =
      "APPLICATION_PLACEMENT_TYPE_CLASS";

  @InterfaceAudience.Private
  public static final Class<? extends AppPlacementAllocator>
      DEFAULT_APPLICATION_PLACEMENT_TYPE_CLASS = LocalityAppPlacementAllocator.class;

  @InterfaceAudience.Private
  public static final String ENV_MULTI_NODE_SORTING_POLICY_CLASS =
      "MULTI_NODE_SORTING_POLICY_CLASS";
}
