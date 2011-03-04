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
  
  /**
   * The constants for the signals.
   */
  public static enum Signal {
    QUIT(3), KILL(9), TERM(15);
    private int value;
    private Signal(int value) {
      this.value = value;
    }
    public int getValue() {
      return value;
    }
  }

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
    }
    return setsidSupported;
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
      if(isProcessGroup) {
        killProcessGroup(pid, Signal.KILL);
      } else {
        killProcess(pid, Signal.KILL);
      }
    }  
  }
  
  /**
   * Sends signal to process, forcefully terminating the process.
   * 
   * @param pid process id
   * @param signal the signal number to send
   */
  public static void killProcess(String pid, Signal signal) {

    //If process tree is not alive then return immediately.
    if(!ProcessTree.isAlive(pid)) {
      return;
    }
    String[] args = { "kill", "-" + signal.getValue(), pid };
    ShellCommandExecutor shexec = new ShellCommandExecutor(args);
    try {
      shexec.execute();
    } catch (IOException e) {
      LOG.warn("Error sending signal " + signal + " to process "+ pid + " ."+ 
          StringUtils.stringifyException(e));
    } finally {
      LOG.info("Killing process " + pid + " with signal " + signal + 
               ". Exit code " + shexec.getExitCode());
    }
  }

  /**
   * Sends signal to all process belonging to same process group,
   * forcefully terminating the process group.
   * 
   * @param pgrpId process group id
   * @param signal the signal number to send
   */
  public static void killProcessGroup(String pgrpId, Signal signal) {

    //If process tree is not alive then return immediately.
    if(!ProcessTree.isProcessGroupAlive(pgrpId)) {
      return;
    }

    String[] args = { "kill", "-" + signal.getValue() , "-"+pgrpId };
    ShellCommandExecutor shexec = new ShellCommandExecutor(args);
    try {
      shexec.execute();
    } catch (IOException e) {
      LOG.warn("Error sending signal " + signal + " to process group "+ 
               pgrpId + " ."+ 
          StringUtils.stringifyException(e));
    } finally {
      LOG.info("Killing process group" + pgrpId + " with signal " + signal + 
               ". Exit code " + shexec.getExitCode());
    }
  }
  
  /**
   * Is the process with PID pid still alive?
   * This method assumes that isAlive is called on a pid that was alive not
   * too long ago, and hence assumes no chance of pid-wrapping-around.
   * 
   * @param pid pid of the process to check.
   * @return true if process is alive.
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
   * Is the process group with  still alive?
   * 
   * This method assumes that isAlive is called on a pid that was alive not
   * too long ago, and hence assumes no chance of pid-wrapping-around.
   * 
   * @param pgrpId process group id
   * @return true if any of process in group is alive.
   */
  public static boolean isProcessGroupAlive(String pgrpId) {
    ShellCommandExecutor shexec = null;
    try {
      String[] args = { "kill", "-0", "-"+pgrpId };
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

    private final long sleepTimeBeforeSigKill;

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
