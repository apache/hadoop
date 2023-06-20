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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.ApplicationSchedulingConfig;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.placement.AppPlacementAllocator;
import org.apache.hadoop.yarn.server.scheduler.SchedulerRequestKey;

/**
 * Factory class to build various application placement policies.
 */
@Public
@Unstable
public class ApplicationPlacementAllocatorFactory {

  /**
   * Get AppPlacementAllocator related to the placement type requested.
   *
   * @param appPlacementAllocatorName
   *          allocator class name.
   * @param appSchedulingInfo app SchedulingInfo.
   * @param schedulerRequestKey scheduler RequestKey.
   * @param rmContext RMContext.
   * @return Specific AppPlacementAllocator instance based on type
   */
  public static AppPlacementAllocator<SchedulerNode> getAppPlacementAllocator(
      String appPlacementAllocatorName, AppSchedulingInfo appSchedulingInfo,
      SchedulerRequestKey schedulerRequestKey, RMContext rmContext) {
    Class<?> policyClass;
    try {
      if (StringUtils.isEmpty(appPlacementAllocatorName)) {
        policyClass = ApplicationSchedulingConfig.DEFAULT_APPLICATION_PLACEMENT_TYPE_CLASS;
      } else {
        policyClass = Class.forName(appPlacementAllocatorName);
      }
    } catch (ClassNotFoundException e) {
      policyClass = ApplicationSchedulingConfig.DEFAULT_APPLICATION_PLACEMENT_TYPE_CLASS;
    }

    if (!AppPlacementAllocator.class.isAssignableFrom(policyClass)) {
      policyClass = ApplicationSchedulingConfig.DEFAULT_APPLICATION_PLACEMENT_TYPE_CLASS;
    }

    @SuppressWarnings("unchecked")
    AppPlacementAllocator<SchedulerNode> placementAllocatorInstance = (AppPlacementAllocator<SchedulerNode>) ReflectionUtils
        .newInstance(policyClass, null);
    placementAllocatorInstance.initialize(appSchedulingInfo,
        schedulerRequestKey, rmContext);
    return placementAllocatorInstance;
  }
}
