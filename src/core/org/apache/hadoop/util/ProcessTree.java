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
import java.util.Arrays;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.util.Shell.ExitCodeException;
import org.apache.hadoop.util.Shell.ShellCommandExecutor;

/** 
 * Process tree related operations
 */
public class ProcessTree {

  private static final Log LOG = LogFactory.getLog(ProcessTree.class);

  public static final long DEFAULT_SLEEPTIME_BEFORE_SIGKILL = 5000L;

  public static final boolean isSetsidAvailable = isSetsidSupported();
  private static boolean isSetsidSupported() {
    ShellCommandExecutor shexec = null;
    boolean setsidSupported = true;
    try {
      String[] args = {"setsid", "bash", "-c", "echo $$"};
      shexec = new ShellCommandExecutor(args);
      shexec.execute();
    } catch (IOException ioe) {
      LOG.warn("setsid is not available on this machine. So not using it.");
      setsidSupported = false;
    } finally { // handle the exit code
      LOG.info("setsid exited with exit code " + shexec.getExitCode());
      return setsidSupported;
    }
  }

  /**
   * Kills the process(OR process group) by sending the signal SIGKILL
   * in the current thread
   * @param pid Process id(OR process group id) of to-be-deleted-process
   * @param isProcessGroup Is pid a process group id of to-be-deleted-processes
   * @param sleepTimeBeforeSigKill wait time before sending SIGKILL after
   *  sending SIGTERM
   */
  private static void sigKillInCurrentThread(String pid, boolean isProcessGroup,
      long sleepTimeBeforeSigKill) {
    // Kill the subprocesses of root process(even if the root process is not
    // alive) if process group is to be killed.
    if (isProcessGroup || ProcessTree.isAlive(pid)) {
      try {
        // Sleep for some time before sending SIGKILL
        Thread.sleep(sleepTimeBeforeSigKill);
      } catch (InterruptedException i) {
        LOG.warn("Thread sleep is interrupted.");
      }

      ShellCommandExecutor shexec = null;

      try {
        String pid_pgrpid;
        if(isProcessGroup) {//kill the whole process group
          pid_pgrpid = "-" + pid;
        }
        else {//kill single process
          pid_pgrpid = pid;
        }
        
        String[] args = { "kill", "-9", pid_pgrpid };
        shexec = new ShellCommandExecutor(args);
        shexec.execute();
      } catch (IOException ioe) {
        LOG.warn("Error executing shell command " + ioe);
      } finally {
        if(isProcessGroup) {
          LOG.info("Killing process group" + pid + " with SIGKILL. Exit code "
            + shexec.getExitCode());
        }
        else {
          LOG.info("Killing process " + pid + " with SIGKILL. Exit code "
                    + shexec.getExitCode());
        }
      }
    }
  }

  /** Kills the process(OR process group) by sending the signal SIGKILL
   * @param pid Process id(OR process group id) of to-be-deleted-process
   * @param isProcessGroup Is pid a process group id of to-be-deleted-processes
   * @param sleeptimeBeforeSigkill The time to wait before sending SIGKILL
   *                               after sending SIGTERM
   * @param inBackground Process is to be killed in the back ground with
   *                     a separate thread
   */
  private static void sigKill(String pid, boolean isProcessGroup,
                        long sleeptimeBeforeSigkill, boolean inBackground) {

    if(inBackground) { // use a separate thread for killing
      SigKillThread sigKillThread = new SigKillThread(pid, isProcessGroup,
                                                      sleeptimeBeforeSigkill);
      sigKillThread.setDaemon(true);
      sigKillThread.start();
    }
    else {
      sigKillInCurrentThread(pid, isProcessGroup, sleeptimeBeforeSigkill);
    }
  }

  /** Destroy the process.
   * @param pid Process id of to-be-killed-process
   * @param sleeptimeBeforeSigkill The time to wait before sending SIGKILL
   *                               after sending SIGTERM
   * @param inBackground Process is to be killed in the back ground with
   *                     a separate thread
   */
  protected static void destroyProcess(String pid, long sleeptimeBeforeSigkill,
                                    boolean inBackground) {
    ShellCommandExecutor shexec = null;
    try {
      String[] args = { "kill", pid };
      shexec = new ShellCommandExecutor(args);
      shexec.execute();
    } catch (IOException ioe) {
      LOG.warn("Error executing shell command " + ioe);
    } finally {
      LOG.info("Killing process " + pid +
               " with SIGTERM. Exit code " + shexec.getExitCode());
    }
    
    sigKill(pid, false, sleeptimeBeforeSigkill, inBackground);
  }
  
  /** Destroy the process group.
   * @param pgrpId Process group id of to-be-killed-processes
   * @param sleeptimeBeforeSigkill The time to wait before sending SIGKILL
   *                               after sending SIGTERM
   * @param inBackground Process group is to be killed in the back ground with
   *                     a separate thread
   */
  protected static void destroyProcessGroup(String pgrpId,
                       long sleeptimeBeforeSigkill, boolean inBackground) {
    ShellCommandExecutor shexec = null;
    try {
      String[] args = { "kill", "--", "-" + pgrpId };
      shexec = new ShellCommandExecutor(args);
      shexec.execute();
    } catch (IOException ioe) {
      LOG.warn("Error executing shell command " + ioe);
    } finally {
      LOG.info("Killing all processes in the process group " + pgrpId +
               " with SIGTERM. Exit code " + shexec.getExitCode());
    }
    
    sigKill(pgrpId, true, sleeptimeBeforeSigkill, inBackground);
  }

  /**
   * Destroy the process-tree.
   * @param pid process id of the root process of the subtree of processes
   *            to be killed
   * @param sleeptimeBeforeSigkill The time to wait before sending SIGKILL
   *                               after sending SIGTERM
   * @param isProcessGroup pid is a process group leader or not
   * @param inBackground Process is to be killed in the back ground with
   *                     a separate thread
   */
  public static void destroy(String pid, long sleeptimeBeforeSigkill,
                             boolean isProcessGroup, boolean inBackground) {
    if(isProcessGroup) {
      destroyProcessGroup(pid, sleeptimeBeforeSigkill, inBackground);
    }
    else {
      //TODO: Destroy all the processes in the subtree in this case also.
      // For the time being, killing only the root process.
      destroyProcess(pid, sleeptimeBeforeSigkill, inBackground);
    }
  }


  /**
   * Is the process with PID pid still alive?
   * This method assumes that isAlive is called on a pid that was alive not
   * too long ago, and hence assumes no chance of pid-wrapping-around.
   */
  public static boolean isAlive(String pid) {
    ShellCommandExecutor shexec = null;
    try {
      String[] args = { "kill", "-0", pid };
      shexec = new ShellCommandExecutor(args);
      shexec.execute();
    } catch (ExitCodeException ee) {
      return false;
    } catch (IOException ioe) {
      LOG.warn("Error executing shell command "
          + Arrays.toString(shexec.getExecString()) + ioe);
      return false;
    }
    return (shexec.getExitCode() == 0 ? true : false);
  }

  /**
   * Helper thread class that kills process-tree with SIGKILL in background
   */
  static class SigKillThread extends Thread {
    private String pid = null;
    private boolean isProcessGroup = false;

    private long sleepTimeBeforeSigKill = DEFAULT_SLEEPTIME_BEFORE_SIGKILL;

    private SigKillThread(String pid, boolean isProcessGroup, long interval) {
      this.pid = pid;
      this.isProcessGroup = isProcessGroup;
      this.setName(this.getClass().getName() + "-" + pid);
      sleepTimeBeforeSigKill = interval;
    }

    public void run() {
      sigKillInCurrentThread(pid, isProcessGroup, sleepTimeBeforeSigKill);
    }
  }
}
