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

package org.apache.hadoop.util;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * Plugin to calculate resource information on the system.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public abstract class SysInfo {

  /**
   * Return default OS instance.
   * @throws UnsupportedOperationException If cannot determine OS.
   * @return Default instance for the detected OS.
   */
  public static SysInfo newInstance() {
    if (Shell.LINUX) {
      return new SysInfoLinux();
    }
    if (Shell.WINDOWS) {
      return new SysInfoWindows();
    }
    throw new UnsupportedOperationException("Could not determine OS");
  }

  /**
   * Obtain the total size of the virtual memory present in the system.
   *
   * @return virtual memory size in bytes.
   */
  public abstract long getVirtualMemorySize();

  /**
   * Obtain the total size of the physical memory present in the system.
   *
   * @return physical memory size bytes.
   */
  public abstract long getPhysicalMemorySize();

  /**
   * Obtain the total size of the available virtual memory present
   * in the system.
   *
   * @return available virtual memory size in bytes.
   */
  public abstract long getAvailableVirtualMemorySize();

  /**
   * Obtain the total size of the available physical memory present
   * in the system.
   *
   * @return available physical memory size bytes.
   */
  public abstract long getAvailablePhysicalMemorySize();

  /**
   * Obtain the total number of logical processors present on the system.
   *
   * @return number of logical processors
   */
  public abstract int getNumProcessors();

  /**
   * Obtain total number of physical cores present on the system.
   *
   * @return number of physical cores
   */
  public abstract int getNumCores();

  /**
   * Obtain the CPU frequency of on the system.
   *
   * @return CPU frequency in kHz
   */
  public abstract long getCpuFrequency();

  /**
   * Obtain the cumulative CPU time since the system is on.
   *
   * @return cumulative CPU time in milliseconds
   */
  public abstract long getCumulativeCpuTime();

  /**
   * Obtain the CPU usage % of the machine. Return -1 if it is unavailable
   *
   * @return CPU usage as a percentage (from 0 to 100) of available cycles.
   */
  public abstract float getCpuUsagePercentage();

  /**
   * Obtain the number of VCores used. Return -1 if it is unavailable
   *
   * @return Number of VCores used a percentage (from 0 to #VCores).
   */
  public abstract float getNumVCoresUsed();

  /**
   * Obtain the aggregated number of bytes read over the network.
   * @return total number of bytes read.
   */
  public abstract long getNetworkBytesRead();

  /**
   * Obtain the aggregated number of bytes written to the network.
   * @return total number of bytes written.
   */
  public abstract long getNetworkBytesWritten();

  /**
   * Obtain the aggregated number of bytes read from disks.
   *
   * @return total number of bytes read.
   */
  public abstract long getStorageBytesRead();

  /**
   * Obtain the aggregated number of bytes written to disks.
   *
   * @return total number of bytes written.
   */
  public abstract long getStorageBytesWritten();

}
