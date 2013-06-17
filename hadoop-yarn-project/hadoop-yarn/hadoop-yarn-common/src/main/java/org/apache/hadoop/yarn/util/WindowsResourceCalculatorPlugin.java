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

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.util.Shell;
import org.apache.hadoop.util.Shell.ShellCommandExecutor;
import org.apache.hadoop.util.StringUtils;

@Private
public class WindowsResourceCalculatorPlugin extends ResourceCalculatorPlugin {
  
  static final Log LOG = LogFactory
      .getLog(WindowsResourceCalculatorPlugin.class);
  
  long vmemSize;
  long memSize;
  long vmemAvailable;
  long memAvailable;
  int numProcessors;
  long cpuFrequencyKhz;
  long cumulativeCpuTimeMs;
  float cpuUsage;
  
  long lastRefreshTime;
  private final int refreshIntervalMs = 1000;
  
  WindowsBasedProcessTree pTree = null;
  
  public WindowsResourceCalculatorPlugin() {
    lastRefreshTime = 0;
    reset();
  }
  
  void reset() {
    vmemSize = -1;
    memSize = -1;
    vmemAvailable = -1;
    memAvailable = -1;
    numProcessors = -1;
    cpuFrequencyKhz = -1;
    cumulativeCpuTimeMs = -1;
    cpuUsage = -1;
  }

  String getSystemInfoInfoFromShell() {
    ShellCommandExecutor shellExecutor = new ShellCommandExecutor(
        new String[] { Shell.WINUTILS, "systeminfo" });
    try {
      shellExecutor.execute();
      return shellExecutor.getOutput();
    } catch (IOException e) {
      LOG.error(StringUtils.stringifyException(e));
    }
    return null;
  }
  
  void refreshIfNeeded() {
    long now = System.currentTimeMillis();
    if (now - lastRefreshTime > refreshIntervalMs) {
      long refreshInterval = now - lastRefreshTime;
      lastRefreshTime = now;
      long lastCumCpuTimeMs = cumulativeCpuTimeMs;
      reset();
      String sysInfoStr = getSystemInfoInfoFromShell();
      if (sysInfoStr != null) {
        final int sysInfoSplitCount = 7;
        String[] sysInfo = sysInfoStr.substring(0, sysInfoStr.indexOf("\r\n"))
            .split(",");
        if (sysInfo.length == sysInfoSplitCount) {
          try {
            vmemSize = Long.parseLong(sysInfo[0]);
            memSize = Long.parseLong(sysInfo[1]);
            vmemAvailable = Long.parseLong(sysInfo[2]);
            memAvailable = Long.parseLong(sysInfo[3]);
            numProcessors = Integer.parseInt(sysInfo[4]);
            cpuFrequencyKhz = Long.parseLong(sysInfo[5]);
            cumulativeCpuTimeMs = Long.parseLong(sysInfo[6]);
            if (lastCumCpuTimeMs != -1) {
              cpuUsage = (cumulativeCpuTimeMs - lastCumCpuTimeMs)
                  / (refreshInterval * 1.0f);
            }

          } catch (NumberFormatException nfe) {
            LOG.warn("Error parsing sysInfo." + nfe);
          }
        } else {
          LOG.warn("Expected split length of sysInfo to be "
              + sysInfoSplitCount + ". Got " + sysInfo.length);
        }
      }
    }
  }
  
  /** {@inheritDoc} */
  @Override
  public long getVirtualMemorySize() {
    refreshIfNeeded();
    return vmemSize;
  }

  /** {@inheritDoc} */
  @Override
  public long getPhysicalMemorySize() {
    refreshIfNeeded();
    return memSize;
  }

  /** {@inheritDoc} */
  @Override
  public long getAvailableVirtualMemorySize() {
    refreshIfNeeded();
    return vmemAvailable;
  }

  /** {@inheritDoc} */
  @Override
  public long getAvailablePhysicalMemorySize() {
    refreshIfNeeded();
    return memAvailable;
  }

  /** {@inheritDoc} */
  @Override
  public int getNumProcessors() {
    refreshIfNeeded();
    return numProcessors;
  }

  /** {@inheritDoc} */
  @Override
  public long getCpuFrequency() {
    refreshIfNeeded();
    return -1;
  }

  /** {@inheritDoc} */
  @Override
  public long getCumulativeCpuTime() {
    refreshIfNeeded();
    return cumulativeCpuTimeMs;
  }

  /** {@inheritDoc} */
  @Override
  public float getCpuUsage() {
    refreshIfNeeded();
    return cpuUsage;
  }
}
