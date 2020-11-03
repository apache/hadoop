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

import java.io.IOException;

import org.apache.hadoop.thirdparty.com.google.common.annotations.VisibleForTesting;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.util.Shell.ShellCommandExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Plugin to calculate resource information on Windows systems.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class SysInfoWindows extends SysInfo {

  private static final Logger LOG =
      LoggerFactory.getLogger(SysInfoWindows.class);

  private long vmemSize;
  private long memSize;
  private long vmemAvailable;
  private long memAvailable;
  private int numProcessors;
  private long cpuFrequencyKhz;
  private long cumulativeCpuTimeMs;
  private float cpuUsage;
  private long storageBytesRead;
  private long storageBytesWritten;
  private long netBytesRead;
  private long netBytesWritten;

  private long lastRefreshTime;
  static final int REFRESH_INTERVAL_MS = 1000;

  public SysInfoWindows() {
    lastRefreshTime = 0;
    reset();
  }

  @VisibleForTesting
  long now() {
    return Time.monotonicNow();
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
    storageBytesRead = -1;
    storageBytesWritten = -1;
    netBytesRead = -1;
    netBytesWritten = -1;
  }

  String getSystemInfoInfoFromShell() {
    try {
      ShellCommandExecutor shellExecutor = new ShellCommandExecutor(
          new String[] {Shell.getWinUtilsFile().getCanonicalPath(),
              "systeminfo" });
      shellExecutor.execute();
      return shellExecutor.getOutput();
    } catch (IOException e) {
      LOG.error(StringUtils.stringifyException(e));
    }
    return null;
  }

  synchronized void refreshIfNeeded() {
    long now = now();
    if (now - lastRefreshTime > REFRESH_INTERVAL_MS) {
      long refreshInterval = now - lastRefreshTime;
      lastRefreshTime = now;
      long lastCumCpuTimeMs = cumulativeCpuTimeMs;
      reset();
      String sysInfoStr = getSystemInfoInfoFromShell();
      if (sysInfoStr != null) {
        final int sysInfoSplitCount = 11;
        int index = sysInfoStr.indexOf("\r\n");
        if (index >= 0) {
          String[] sysInfo = sysInfoStr.substring(0, index).split(",");
          if (sysInfo.length == sysInfoSplitCount) {
            try {
              vmemSize = Long.parseLong(sysInfo[0]);
              memSize = Long.parseLong(sysInfo[1]);
              vmemAvailable = Long.parseLong(sysInfo[2]);
              memAvailable = Long.parseLong(sysInfo[3]);
              numProcessors = Integer.parseInt(sysInfo[4]);
              cpuFrequencyKhz = Long.parseLong(sysInfo[5]);
              cumulativeCpuTimeMs = Long.parseLong(sysInfo[6]);
              storageBytesRead = Long.parseLong(sysInfo[7]);
              storageBytesWritten = Long.parseLong(sysInfo[8]);
              netBytesRead = Long.parseLong(sysInfo[9]);
              netBytesWritten = Long.parseLong(sysInfo[10]);
              if (lastCumCpuTimeMs != -1) {
                /**
                 * This number will be the aggregated usage across all cores in
                 * [0.0, 100.0]. For example, it will be 400.0 if there are 8
                 * cores and each of them is running at 50% utilization.
                 */
                cpuUsage = (cumulativeCpuTimeMs - lastCumCpuTimeMs)
                    * 100F / refreshInterval;
              }
            } catch (NumberFormatException nfe) {
              LOG.warn("Error parsing sysInfo", nfe);
            }
          } else {
            LOG.warn("Expected split length of sysInfo to be "
                + sysInfoSplitCount + ". Got " + sysInfo.length);
          }
        } else {
          LOG.warn("Wrong output from sysInfo: " + sysInfoStr);
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
  public synchronized int getNumProcessors() {
    refreshIfNeeded();
    return numProcessors;
  }

  /** {@inheritDoc} */
  @Override
  public int getNumCores() {
    return getNumProcessors();
  }

  /** {@inheritDoc} */
  @Override
  public long getCpuFrequency() {
    refreshIfNeeded();
    return cpuFrequencyKhz;
  }

  /** {@inheritDoc} */
  @Override
  public long getCumulativeCpuTime() {
    refreshIfNeeded();
    return cumulativeCpuTimeMs;
  }

  /** {@inheritDoc} */
  @Override
  public synchronized float getCpuUsagePercentage() {
    refreshIfNeeded();
    float ret = cpuUsage;
    if (ret != -1) {
      ret = ret / numProcessors;
    }
    return ret;
  }

  /** {@inheritDoc} */
  @Override
  public synchronized float getNumVCoresUsed() {
    refreshIfNeeded();
    float ret = cpuUsage;
    if (ret != -1) {
      ret = ret / 100F;
    }
    return ret;
  }

  /** {@inheritDoc} */
  @Override
  public long getNetworkBytesRead() {
    refreshIfNeeded();
    return netBytesRead;
  }

  /** {@inheritDoc} */
  @Override
  public long getNetworkBytesWritten() {
    refreshIfNeeded();
    return netBytesWritten;
  }

  @Override
  public long getStorageBytesRead() {
    refreshIfNeeded();
    return storageBytesRead;
  }

  @Override
  public long getStorageBytesWritten() {
    refreshIfNeeded();
    return storageBytesWritten;
  }

}
