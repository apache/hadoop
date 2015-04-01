/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.server.nodemanager.util;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.ResourceCalculatorPlugin;

@InterfaceAudience.Private
@InterfaceStability.Unstable
public class NodeManagerHardwareUtils {

  /**
   *
   * Returns the fraction of CPU cores that should be used for YARN containers.
   * The number is derived based on various configuration params such as
   * YarnConfiguration.NM_RESOURCE_PERCENTAGE_PHYSICAL_CPU_LIMIT
   *
   * @param conf
   *          - Configuration object
   * @return Fraction of CPU cores to be used for YARN containers
   */
  public static float getContainersCores(Configuration conf) {
    ResourceCalculatorPlugin plugin =
        ResourceCalculatorPlugin.getResourceCalculatorPlugin(null, conf);
    return NodeManagerHardwareUtils.getContainersCores(plugin, conf);
  }

  /**
   *
   * Returns the fraction of CPU cores that should be used for YARN containers.
   * The number is derived based on various configuration params such as
   * YarnConfiguration.NM_RESOURCE_PERCENTAGE_PHYSICAL_CPU_LIMIT
   *
   * @param plugin
   *          - ResourceCalculatorPlugin object to determine hardware specs
   * @param conf
   *          - Configuration object
   * @return Fraction of CPU cores to be used for YARN containers
   */
  public static float getContainersCores(ResourceCalculatorPlugin plugin,
      Configuration conf) {
    int numProcessors = plugin.getNumProcessors();
    int nodeCpuPercentage = getNodeCpuPercentage(conf);

    return (nodeCpuPercentage * numProcessors) / 100.0f;
  }

  /**
   * Gets the percentage of physical CPU that is configured for YARN containers.
   * This is percent {@literal >} 0 and {@literal <=} 100 based on
   * {@link YarnConfiguration#NM_RESOURCE_PERCENTAGE_PHYSICAL_CPU_LIMIT}
   * @param conf Configuration object
   * @return percent {@literal >} 0 and {@literal <=} 100
   */
  public static int getNodeCpuPercentage(Configuration conf) {
    int nodeCpuPercentage =
        Math.min(conf.getInt(
          YarnConfiguration.NM_RESOURCE_PERCENTAGE_PHYSICAL_CPU_LIMIT,
          YarnConfiguration.DEFAULT_NM_RESOURCE_PERCENTAGE_PHYSICAL_CPU_LIMIT),
          100);
    nodeCpuPercentage = Math.max(0, nodeCpuPercentage);

    if (nodeCpuPercentage == 0) {
      String message =
          "Illegal value for "
              + YarnConfiguration.NM_RESOURCE_PERCENTAGE_PHYSICAL_CPU_LIMIT
              + ". Value cannot be less than or equal to 0.";
      throw new IllegalArgumentException(message);
    }
    return nodeCpuPercentage;
  }
}
