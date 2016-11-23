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
package org.apache.hadoop.yarn.util;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.SysInfo;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

/**
 * Plugin to calculate resource information on the system.
 */
@InterfaceAudience.LimitedPrivate({"YARN", "MAPREDUCE"})
@InterfaceStability.Unstable
public class ResourceCalculatorPlugin extends Configured {
  private static final Log LOG =
      LogFactory.getLog(ResourceCalculatorPlugin.class);

  private final SysInfo sys;

  protected ResourceCalculatorPlugin() {
    this(SysInfo.newInstance());
  }

  public ResourceCalculatorPlugin(SysInfo sys) {
    this.sys = sys;
  }

  /**
   * Obtain the total size of the virtual memory present in the system.
   *
   * @return virtual memory size in bytes.
   */
  public long getVirtualMemorySize() {
    return sys.getVirtualMemorySize();
  }

  /**
   * Obtain the total size of the physical memory present in the system.
   *
   * @return physical memory size bytes.
   */
  public long getPhysicalMemorySize() {
    return sys.getPhysicalMemorySize();
  }

  /**
   * Obtain the total size of the available virtual memory present
   * in the system.
   *
   * @return available virtual memory size in bytes.
   */
  public long getAvailableVirtualMemorySize() {
    return sys.getAvailableVirtualMemorySize();
  }

  /**
   * Obtain the total size of the available physical memory present
   * in the system.
   *
   * @return available physical memory size bytes.
   */
  public long getAvailablePhysicalMemorySize() {
    return sys.getAvailablePhysicalMemorySize();
  }

  /**
   * Obtain the total number of logical processors present on the system.
   *
   * @return number of logical processors
   */
  public int getNumProcessors() {
    return sys.getNumProcessors();
  }

  /**
   * Obtain total number of physical cores present on the system.
   *
   * @return number of physical cores
   */
  public int getNumCores() {
    return sys.getNumCores();
  }

  /**
   * Obtain the CPU frequency of on the system.
   *
   * @return CPU frequency in kHz
   */
  public long getCpuFrequency() {
    return sys.getCpuFrequency();
  }

  /**
   * Obtain the cumulative CPU time since the system is on.
   *
   * @return cumulative CPU time in milliseconds
   */
  public long getCumulativeCpuTime() {
    return sys.getCumulativeCpuTime();
  }

  /**
   * Obtain the CPU usage % of the machine. Return -1 if it is unavailable.
   *
   * @return CPU usage in %
   */
  public float getCpuUsagePercentage() {
    return sys.getCpuUsagePercentage();
  }

  /**
   * Obtain the number of VCores used. Return -1 if it is unavailable.
   *
   * @return Number of VCores used a percentage (from 0 to #VCores)
   */
  public float getNumVCoresUsed() {
    return sys.getNumVCoresUsed();
  }

   /**
   * Obtain the aggregated number of bytes read over the network.
   * @return total number of bytes read.
   */
  public long getNetworkBytesRead() {
    return sys.getNetworkBytesRead();
  }

  /**
   * Obtain the aggregated number of bytes written to the network.
   * @return total number of bytes written.
   */
  public long getNetworkBytesWritten() {
    return sys.getNetworkBytesWritten();
  }

  /**
   * Obtain the aggregated number of bytes read from disks.
   *
   * @return total number of bytes read.
   */
  public long getStorageBytesRead() {
    return sys.getStorageBytesRead();
  }

  /**
   * Obtain the aggregated number of bytes written to disks.
   *
   * @return total number of bytes written.
   */
  public long getStorageBytesWritten() {
    return sys.getStorageBytesWritten();
  }

  /**
   * Create the ResourceCalculatorPlugin from the class name and configure it. If
   * class name is null, this method will try and return a memory calculator
   * plugin available for this system.
   *
   * @param clazz ResourceCalculator plugin class-name
   * @param conf configure the plugin with this.
   * @return ResourceCalculatorPlugin or null if ResourceCalculatorPlugin is not
   * 		 available for current system
   */
  public static ResourceCalculatorPlugin getResourceCalculatorPlugin(
      Class<? extends ResourceCalculatorPlugin> clazz, Configuration conf) {

    if (clazz != null) {
      return ReflectionUtils.newInstance(clazz, conf);
    }
    try {
      return new ResourceCalculatorPlugin();
    } catch (UnsupportedOperationException ue) {
      LOG.warn("Failed to instantiate default resource calculator. "
          + ue.getMessage());
    } catch (Throwable t) {
      LOG.warn(t + ": Failed to instantiate default resource calculator.", t);
    }
    return null;
  }

  /**
   * Create the ResourceCalculatorPlugin for the containers monitor in the Node
   * Manager and configure it. If the plugin is not configured, this method
   * will try and return a memory calculator plugin available for this system.
   *
   * @param conf Configure the plugin with this.
   * @return ResourceCalculatorPlugin or null if ResourceCalculatorPlugin is
   *         not available for current system.
   */
  public static ResourceCalculatorPlugin getContainersMonitorPlugin(
      Configuration conf) {
    Class<? extends ResourceCalculatorPlugin> clazzNM = conf.getClass(
        YarnConfiguration.NM_MON_RESOURCE_CALCULATOR, null,
        ResourceCalculatorPlugin.class);
    Class<? extends ResourceCalculatorPlugin> clazz = conf.getClass(
        YarnConfiguration.NM_CONTAINER_MON_RESOURCE_CALCULATOR, clazzNM,
        ResourceCalculatorPlugin.class);
    return ResourceCalculatorPlugin.getResourceCalculatorPlugin(clazz, conf);
  }

  /**
   * Create the ResourceCalculatorPlugin for the node resource monitor in the
   * Node Manager and configure it. If the plugin is not configured, this
   * method will try and return a memory calculator plugin available for this
   * system.
   *
   * @param conf Configure the plugin with this.
   * @return ResourceCalculatorPlugin or null if ResourceCalculatorPlugin is
   *         not available for current system.
   */
  public static ResourceCalculatorPlugin getNodeResourceMonitorPlugin(
      Configuration conf) {
    Class<? extends ResourceCalculatorPlugin> clazz = conf.getClass(
        YarnConfiguration.NM_MON_RESOURCE_CALCULATOR, null,
        ResourceCalculatorPlugin.class);
    return ResourceCalculatorPlugin.getResourceCalculatorPlugin(clazz, conf);
  }

}
