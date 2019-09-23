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
import java.math.BigInteger;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.util.CpuTimeTracker;
import org.apache.hadoop.util.Shell;
import org.apache.hadoop.util.Shell.ShellCommandExecutor;
import org.apache.hadoop.util.StringUtils;

@Private
public class WindowsBasedProcessTree extends ResourceCalculatorProcessTree {

  private static final Logger LOG = LoggerFactory
      .getLogger(WindowsBasedProcessTree.class);

  static class ProcessInfo {
    String pid; // process pid
    long vmem; // virtual memory
    long workingSet; // working set, RAM used
    long cpuTimeMs; // total cpuTime in millisec
    long cpuTimeMsDelta; // delta of cpuTime since last update
    int age = 1;
  }
  private String taskProcessId = null;
  private long cpuTimeMs = UNAVAILABLE;
  private Map<String, ProcessInfo> processTree =
      new HashMap<String, ProcessInfo>();

  /** Track CPU utilization. */
  private final CpuTimeTracker cpuTimeTracker;
  /** Clock to account for CPU utilization. */
  private Clock clock;

  public static boolean isAvailable() {
    if (Shell.WINDOWS) {
      if (!Shell.hasWinutilsPath()) {
        return false;
      }
      ShellCommandExecutor shellExecutor = new ShellCommandExecutor(
          new String[] { Shell.getWinUtilsPath(), "help" });
      try {
        shellExecutor.execute();
      } catch (IOException e) {
        LOG.error(StringUtils.stringifyException(e));
      } finally {
        String output = shellExecutor.getOutput();
        if (output != null &&
            output.contains("Prints to stdout a list of processes in the task")) {
          return true;
        }
      }
    }
    return false;
  }

  /**
   * Create a monitor for a Windows process tree.
   * @param pid Identifier of the job object.
   */
  public WindowsBasedProcessTree(final String pid) {
    this(pid, SystemClock.getInstance());
  }

  /**
   * Create a monitor for a Windows process tree.
   * @param pid Identifier of the job object.
   * @param pClock Clock to keep track of time for CPU utilization.
   */
  public WindowsBasedProcessTree(final String pid, final Clock pClock) {
    super(pid);
    this.taskProcessId = pid;
    this.clock = pClock;
    // Instead of jiffies, Windows uses milliseconds directly; 1ms = 1 jiffy
    this.cpuTimeTracker = new CpuTimeTracker(1L);
  }

  // helper method to override while testing
  String getAllProcessInfoFromShell() {
    try {
      ShellCommandExecutor shellExecutor = new ShellCommandExecutor(
          new String[] {Shell.getWinUtilsFile().getCanonicalPath(),
              "task", "processList", taskProcessId });
      shellExecutor.execute();
      return shellExecutor.getOutput();
    } catch (IOException e) {
      LOG.error(StringUtils.stringifyException(e));
    }
    return null;
  }

  /**
   * Parses string of process info lines into ProcessInfo objects
   * @param processesInfoStr
   * @return Map of pid string to ProcessInfo objects
   */
  Map<String, ProcessInfo> createProcessInfo(String processesInfoStr) {
    String[] processesStr = processesInfoStr.split("\r\n");
    Map<String, ProcessInfo> allProcs = new HashMap<String, ProcessInfo>();
    final int procInfoSplitCount = 4;
    for (String processStr : processesStr) {
      if (processStr != null) {
        String[] procInfo = processStr.split(",");
        if (procInfo.length == procInfoSplitCount) {
          try {
            ProcessInfo pInfo = new ProcessInfo();
            pInfo.pid = procInfo[0];
            pInfo.vmem = Long.parseLong(procInfo[1]);
            pInfo.workingSet = Long.parseLong(procInfo[2]);
            pInfo.cpuTimeMs = Long.parseLong(procInfo[3]);
            allProcs.put(pInfo.pid, pInfo);
          } catch (NumberFormatException nfe) {
            LOG.debug("Error parsing procInfo.", nfe);
          }
        } else {
          LOG.debug("Expected split length of proc info to be {}. Got {}",
              procInfoSplitCount, procInfo.length);
        }
      }
    }
    return allProcs;
  }

  @Override
  public void updateProcessTree() {
    if(taskProcessId != null) {
      // taskProcessId can be null in some tests
      String processesInfoStr = getAllProcessInfoFromShell();
      if (processesInfoStr != null && processesInfoStr.length() > 0) {
        Map<String, ProcessInfo> allProcessInfo = createProcessInfo(processesInfoStr);

        for (Map.Entry<String, ProcessInfo> entry : allProcessInfo.entrySet()) {
          String pid = entry.getKey();
          ProcessInfo pInfo = entry.getValue();
          ProcessInfo oldInfo = processTree.get(pid);
          if (oldInfo != null) {
            // existing process, update age and replace value
            pInfo.age += oldInfo.age;
            // calculate the delta since the last refresh. totals are being kept
            // in the WindowsBasedProcessTree object
            pInfo.cpuTimeMsDelta = pInfo.cpuTimeMs - oldInfo.cpuTimeMs;
          } else {
            // new process. delta cpu == total cpu
            pInfo.cpuTimeMsDelta = pInfo.cpuTimeMs;
          }
        }
        processTree.clear();
        processTree = allProcessInfo;
      } else {
        // clearing process tree to mimic semantics of existing Procfs impl
        processTree.clear();
      }
    }
  }

  @Override
  public boolean checkPidPgrpidForMatch() {
    // This is always true on Windows, because the pid doubles as a job object
    // name for task management.
    return true;
  }

  @Override
  public String getProcessTreeDump() {
    StringBuilder ret = new StringBuilder();
    // The header.
    ret.append(String.format("\t|- PID " + "CPU_TIME(MILLIS) "
        + "VMEM(BYTES) WORKING_SET(BYTES)%n"));
    for (ProcessInfo p : processTree.values()) {
      if (p != null) {
        ret.append(String.format("\t|- %s %d %d %d%n", p.pid,
            p.cpuTimeMs, p.vmem, p.workingSet));
      }
    }
    return ret.toString();
  }

  @Override
  public long getVirtualMemorySize(int olderThanAge) {
    long total = UNAVAILABLE;
    for (ProcessInfo p : processTree.values()) {
      if (p != null) {
        if (total == UNAVAILABLE) {
          total = 0;
        }
        if (p.age > olderThanAge) {
          total += p.vmem;
        }
      }
    }
    return total;
  }

  @Override
  public long getRssMemorySize(int olderThanAge) {
    long total = UNAVAILABLE;
    for (ProcessInfo p : processTree.values()) {
      if (p != null) {
        if (total == UNAVAILABLE) {
          total = 0;
        }
        if (p.age > olderThanAge) {
          total += p.workingSet;
        }
      }
    }
    return total;
  }

  @Override
  public long getCumulativeCpuTime() {
    for (ProcessInfo p : processTree.values()) {
      if (cpuTimeMs == UNAVAILABLE) {
        cpuTimeMs = 0;
      }
      cpuTimeMs += p.cpuTimeMsDelta;
    }
    return cpuTimeMs;
  }

  /**
   * Get the number of used ms for all the processes under the monitored job
   * object.
   * @return Total consumed milliseconds by all processes in the job object.
   */
  private BigInteger getTotalProcessMs() {
    long totalMs = 0;
    for (ProcessInfo p : processTree.values()) {
      if (p != null) {
        totalMs += p.cpuTimeMs;
      }
    }
    return BigInteger.valueOf(totalMs);
  }

  /**
   * Get the CPU usage by all the processes in the process-tree in Windows.
   * Note: UNAVAILABLE will be returned in case when CPU usage is not
   * available. It is NOT advised to return any other error code.
   *
   * @return percentage CPU usage since the process-tree was created,
   * {@link #UNAVAILABLE} if CPU usage cannot be calculated or not available.
   */
  @Override
  public float getCpuUsagePercent() {
    BigInteger processTotalMs = getTotalProcessMs();
    cpuTimeTracker.updateElapsedJiffies(processTotalMs, clock.getTime());

    return cpuTimeTracker.getCpuTrackerUsagePercent();
  }

}
