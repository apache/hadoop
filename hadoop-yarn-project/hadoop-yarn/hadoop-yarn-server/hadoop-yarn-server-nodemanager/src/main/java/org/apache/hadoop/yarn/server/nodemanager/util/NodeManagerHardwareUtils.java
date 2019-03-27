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
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceInformation;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.util.ResourceCalculatorPlugin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.yarn.util.resource.ResourceUtils;

import java.util.Map;

/**
 * Helper class to determine hardware related characteristics such as the
 * number of processors and the amount of memory on the node.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class NodeManagerHardwareUtils {

  private static final Logger LOG =
       LoggerFactory.getLogger(NodeManagerHardwareUtils.class);

  private static boolean isHardwareDetectionEnabled(Configuration conf) {
    return conf.getBoolean(
        YarnConfiguration.NM_ENABLE_HARDWARE_CAPABILITY_DETECTION,
        YarnConfiguration.DEFAULT_NM_ENABLE_HARDWARE_CAPABILITY_DETECTION);
  }

  /**
   *
   * Returns the number of CPUs on the node. This value depends on the
   * configuration setting which decides whether to count logical processors
   * (such as hyperthreads) as cores or not.
   *
   * @param conf
   *          - Configuration object
   * @return Number of CPUs
   */
  public static int getNodeCPUs(Configuration conf) {
    ResourceCalculatorPlugin plugin =
        ResourceCalculatorPlugin.getResourceCalculatorPlugin(null, conf);
    return NodeManagerHardwareUtils.getNodeCPUs(plugin, conf);
  }

  /**
   *
   * Returns the number of CPUs on the node. This value depends on the
   * configuration setting which decides whether to count logical processors
   * (such as hyperthreads) as cores or not.
   *
   * @param plugin
   *          - ResourceCalculatorPlugin object to determine hardware specs
   * @param conf
   *          - Configuration object
   * @return Number of CPU cores on the node.
   */
  public static int getNodeCPUs(ResourceCalculatorPlugin plugin,
      Configuration conf) {
    int numProcessors = plugin.getNumProcessors();
    boolean countLogicalCores =
        conf.getBoolean(YarnConfiguration.NM_COUNT_LOGICAL_PROCESSORS_AS_CORES,
          YarnConfiguration.DEFAULT_NM_COUNT_LOGICAL_PROCESSORS_AS_CORES);
    if (!countLogicalCores) {
      numProcessors = plugin.getNumCores();
    }
    return numProcessors;
  }

  /**
   *
   * Returns the fraction of CPUs that should be used for YARN containers.
   * The number is derived based on various configuration params such as
   * YarnConfiguration.NM_RESOURCE_PERCENTAGE_PHYSICAL_CPU_LIMIT
   *
   * @param conf
   *          - Configuration object
   * @return Fraction of CPUs to be used for YARN containers
   */
  public static float getContainersCPUs(Configuration conf) {
    ResourceCalculatorPlugin plugin =
        ResourceCalculatorPlugin.getResourceCalculatorPlugin(null, conf);
    return NodeManagerHardwareUtils.getContainersCPUs(plugin, conf);
  }

  /**
   *
   * Returns the fraction of CPUs that should be used for YARN containers.
   * The number is derived based on various configuration params such as
   * YarnConfiguration.NM_RESOURCE_PERCENTAGE_PHYSICAL_CPU_LIMIT
   *
   * @param plugin
   *          - ResourceCalculatorPlugin object to determine hardware specs
   * @param conf
   *          - Configuration object
   * @return Fraction of CPUs to be used for YARN containers
   */
  public static float getContainersCPUs(ResourceCalculatorPlugin plugin,
      Configuration conf) {
    int numProcessors = getNodeCPUs(plugin, conf);
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

  private static int getConfiguredVCores(Configuration conf) {
    int cores = conf.getInt(YarnConfiguration.NM_VCORES,
        YarnConfiguration.DEFAULT_NM_VCORES);
    if (cores == -1) {
      cores = YarnConfiguration.DEFAULT_NM_VCORES;
    }
    return cores;
  }

  /**
   * Function to return the number of vcores on the system that can be used for
   * YARN containers. If a number is specified in the configuration file, then
   * that number is returned. If nothing is specified - 1. If the OS is an
   * "unknown" OS(one for which we don't have ResourceCalculatorPlugin
   * implemented), return the default NodeManager cores. 2. If the config
   * variable yarn.nodemanager.cpu.use_logical_processors is set to true, it
   * returns the logical processor count(count hyperthreads as cores), else it
   * returns the physical cores count.
   *
   * @param conf
   *          - the configuration for the NodeManager
   * @return the number of cores to be used for YARN containers
   *
   */
  public static int getVCores(Configuration conf) {
    if (!isHardwareDetectionEnabled(conf)) {
      return getConfiguredVCores(conf);
    }
    // is this os for which we can determine cores?
    ResourceCalculatorPlugin plugin =
        ResourceCalculatorPlugin.getResourceCalculatorPlugin(null, conf);
    if (plugin == null) {
      return getConfiguredVCores(conf);
    }
    return getVCoresInternal(plugin, conf);
  }

  /**
   * Function to return the number of vcores on the system that can be used for
   * YARN containers. If a number is specified in the configuration file, then
   * that number is returned. If nothing is specified - 1. If the OS is an
   * "unknown" OS(one for which we don't have ResourceCalculatorPlugin
   * implemented), return the default NodeManager cores. 2. If the config
   * variable yarn.nodemanager.cpu.use_logical_processors is set to true, it
   * returns the logical processor count(count hyperthreads as cores), else it
   * returns the physical cores count.
   *
   * @param plugin
   *          - ResourceCalculatorPlugin object to determine hardware specs
   * @param conf
   *          - the configuration for the NodeManager
   * @return the number of cores to be used for YARN containers
   *
   */
  public static int getVCores(ResourceCalculatorPlugin plugin,
      Configuration conf) {
    if (!isHardwareDetectionEnabled(conf) || plugin == null) {
      return getConfiguredVCores(conf);
    }
    return getVCoresInternal(plugin, conf);
  }

  private static int getVCoresInternal(ResourceCalculatorPlugin plugin,
      Configuration conf) {
    String message;
    int cores = conf.getInt(YarnConfiguration.NM_VCORES, -1);
    if (cores == -1) {
      float physicalCores =
          NodeManagerHardwareUtils.getContainersCPUs(plugin, conf);
      float multiplier =
          conf.getFloat(YarnConfiguration.NM_PCORES_VCORES_MULTIPLIER,
              YarnConfiguration.DEFAULT_NM_PCORES_VCORES_MULTIPLIER);
      if (multiplier > 0) {
        float tmp = physicalCores * multiplier;
        if (tmp > 0 && tmp < 1) {
          // on a single core machine - tmp can be between 0 and 1
          cores = 1;
        } else {
          cores = Math.round(tmp);
        }
      } else {
        message = "Illegal value for "
            + YarnConfiguration.NM_PCORES_VCORES_MULTIPLIER
            + ". Value must be greater than 0.";
        throw new IllegalArgumentException(message);
      }
    }
    if(cores <= 0) {
      message = "Illegal value for " + YarnConfiguration.NM_VCORES
          + ". Value must be greater than 0.";
      throw new IllegalArgumentException(message);
    }

    return cores;
  }

  private static long getConfiguredMemoryMB(Configuration conf) {
    long memoryMb = conf.getLong(YarnConfiguration.NM_PMEM_MB,
        YarnConfiguration.DEFAULT_NM_PMEM_MB);
    if (memoryMb == -1) {
      memoryMb = YarnConfiguration.DEFAULT_NM_PMEM_MB;
    }
    return memoryMb;
  }

  /**
   * Function to return how much memory we should set aside for YARN containers.
   * If a number is specified in the configuration file, then that number is
   * returned. If nothing is specified - 1. If the OS is an "unknown" OS(one for
   * which we don't have ResourceCalculatorPlugin implemented), return the
   * default NodeManager physical memory. 2. If the OS has a
   * ResourceCalculatorPlugin implemented, the calculation is 0.8 * (RAM - 2 *
   * JVM-memory) i.e. use 80% of the memory after accounting for memory used by
   * the DataNode and the NodeManager. If the number is less than 1GB, log a
   * warning message.
   *
   * @param conf
   *          - the configuration for the NodeManager
   * @return the amount of memory that will be used for YARN containers in MB.
   */
  public static long getContainerMemoryMB(Configuration conf) {
    if (!isHardwareDetectionEnabled(conf)) {
      return getConfiguredMemoryMB(conf);
    }
    ResourceCalculatorPlugin plugin =
        ResourceCalculatorPlugin.getResourceCalculatorPlugin(null, conf);
    if (plugin == null) {
      return getConfiguredMemoryMB(conf);
    }
    return getContainerMemoryMBInternal(plugin, conf);
  }

  /**
   * Function to return how much memory we should set aside for YARN containers.
   * If a number is specified in the configuration file, then that number is
   * returned. If nothing is specified - 1. If the OS is an "unknown" OS(one for
   * which we don't have ResourceCalculatorPlugin implemented), return the
   * default NodeManager physical memory. 2. If the OS has a
   * ResourceCalculatorPlugin implemented, the calculation is 0.8 * (RAM - 2 *
   * JVM-memory) i.e. use 80% of the memory after accounting for memory used by
   * the DataNode and the NodeManager. If the number is less than 1GB, log a
   * warning message.
   *
   * @param plugin
   *          - ResourceCalculatorPlugin object to determine hardware specs
   * @param conf
   *          - the configuration for the NodeManager
   * @return the amount of memory that will be used for YARN containers in MB.
   */
  public static long getContainerMemoryMB(ResourceCalculatorPlugin plugin,
      Configuration conf) {
    if (!isHardwareDetectionEnabled(conf) || plugin == null) {
      return getConfiguredMemoryMB(conf);
    }
    return getContainerMemoryMBInternal(plugin, conf);
  }

  private static long getContainerMemoryMBInternal(ResourceCalculatorPlugin plugin,
      Configuration conf) {
    long memoryMb = conf.getInt(YarnConfiguration.NM_PMEM_MB, -1);
    if (memoryMb == -1) {
      long physicalMemoryMB = (plugin.getPhysicalMemorySize() / (1024 * 1024));
      long hadoopHeapSizeMB = (Runtime.getRuntime().maxMemory()
          / (1024 * 1024));
      long containerPhysicalMemoryMB = (long) (0.8f
          * (physicalMemoryMB - (2 * hadoopHeapSizeMB)));
      long reservedMemoryMB = conf
          .getInt(YarnConfiguration.NM_SYSTEM_RESERVED_PMEM_MB, -1);
      if (reservedMemoryMB != -1) {
        containerPhysicalMemoryMB = physicalMemoryMB - reservedMemoryMB;
      }
      if (containerPhysicalMemoryMB <= 0) {
        LOG.error("Calculated memory for YARN containers is too low."
            + " Node memory is " + physicalMemoryMB
            + " MB, system reserved memory is " + reservedMemoryMB + " MB.");
      }
      containerPhysicalMemoryMB = Math.max(containerPhysicalMemoryMB, 0);
      memoryMb = containerPhysicalMemoryMB;
    }
    if(memoryMb <= 0) {
      String message = "Illegal value for " + YarnConfiguration.NM_PMEM_MB
          + ". Value must be greater than 0.";
      throw new IllegalArgumentException(message);
    }
    return memoryMb;
  }

  /**
   * Get the resources for the node.
   * @param configuration configuration file
   * @return the resources for the node
   */
  public static Resource getNodeResources(Configuration configuration) {
    Configuration conf = new Configuration(configuration);
    String memory = ResourceInformation.MEMORY_MB.getName();
    String vcores = ResourceInformation.VCORES.getName();

    Resource ret = Resource.newInstance(0, 0);
    Map<String, ResourceInformation> resourceInformation =
        ResourceUtils.getNodeResourceInformation(conf);
    for (Map.Entry<String, ResourceInformation> entry : resourceInformation
        .entrySet()) {
      ret.setResourceInformation(entry.getKey(), entry.getValue());
      LOG.debug("Setting key {} to {}", entry.getKey(), entry.getValue());
    }
    if (resourceInformation.containsKey(memory)) {
      Long value = resourceInformation.get(memory).getValue();
      if (value > Integer.MAX_VALUE) {
        throw new YarnRuntimeException("Value '" + value
            + "' for resource memory is more than the maximum for an integer.");
      }
      ResourceInformation memResInfo = resourceInformation.get(memory);
      if(memResInfo.getValue() == 0) {
        ret.setMemorySize(getContainerMemoryMB(conf));
        LOG.debug("Set memory to {}", ret.getMemorySize());
      }
    }
    if (resourceInformation.containsKey(vcores)) {
      Long value = resourceInformation.get(vcores).getValue();
      if (value > Integer.MAX_VALUE) {
        throw new YarnRuntimeException("Value '" + value
            + "' for resource vcores is more than the maximum for an integer.");
      }
      ResourceInformation vcoresResInfo = resourceInformation.get(vcores);
      if(vcoresResInfo.getValue() == 0) {
        ret.setVirtualCores(getVCores(conf));
        LOG.debug("Set vcores to {}", ret.getVirtualCores());
      }
    }
    LOG.debug("Node resource information map is {}", ret);
    return ret;
  }
}
