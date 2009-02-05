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

package org.apache.hadoop.mapred;

import java.io.File;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.List;
import java.util.ArrayList;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapred.TaskTracker;
import org.apache.hadoop.mapred.TaskTracker.TaskInProgress;
import org.apache.hadoop.util.ProcfsBasedProcessTree;

/**
 * Manages memory usage of tasks running under this TT. Kills any task-trees
 * that overflow and over-step memory limits.
 */
class TaskMemoryManagerThread extends Thread {

  private static Log LOG = LogFactory.getLog(TaskMemoryManagerThread.class);

  private TaskTracker taskTracker;
  private long monitoringInterval;
  private long sleepTimeBeforeSigKill;

  private long maxMemoryAllowedForAllTasks;

  private Map<TaskAttemptID, ProcessTreeInfo> processTreeInfoMap;
  private Map<TaskAttemptID, ProcessTreeInfo> tasksToBeAdded;
  private List<TaskAttemptID> tasksToBeRemoved;

  public TaskMemoryManagerThread(TaskTracker taskTracker) {
    this.taskTracker = taskTracker;
    setName(this.getClass().getName());

    processTreeInfoMap = new HashMap<TaskAttemptID, ProcessTreeInfo>();
    tasksToBeAdded = new HashMap<TaskAttemptID, ProcessTreeInfo>();
    tasksToBeRemoved = new ArrayList<TaskAttemptID>();

    maxMemoryAllowedForAllTasks =
        taskTracker.getTotalVirtualMemoryOnTT()
            - taskTracker.getReservedVirtualMemory();

    monitoringInterval = taskTracker.getJobConf().getLong(
        "mapred.tasktracker.taskmemorymanager.monitoring-interval", 5000L);
    sleepTimeBeforeSigKill = taskTracker.getJobConf().getLong(
        "mapred.tasktracker.procfsbasedprocesstree.sleeptime-before-sigkill",
        ProcfsBasedProcessTree.DEFAULT_SLEEPTIME_BEFORE_SIGKILL);
  }

  public void addTask(TaskAttemptID tid, long memLimit, String pidFile) {
    synchronized (tasksToBeAdded) {
      LOG.debug("Tracking ProcessTree " + tid + " for the first time");
      ProcessTreeInfo ptInfo = new ProcessTreeInfo(tid, null, null, memLimit,
          sleepTimeBeforeSigKill, pidFile);
      tasksToBeAdded.put(tid, ptInfo);
    }
  }

  public void removeTask(TaskAttemptID tid) {
    synchronized (tasksToBeRemoved) {
      tasksToBeRemoved.add(tid);
    }
  }

  private static class ProcessTreeInfo {
    private TaskAttemptID tid;
    private String pid;
    private ProcfsBasedProcessTree pTree;
    private long memLimit;
    private String pidFile;

    public ProcessTreeInfo(TaskAttemptID tid, String pid,
        ProcfsBasedProcessTree pTree, long memLimit, 
        long sleepTimeBeforeSigKill, String pidFile) {
      this.tid = tid;
      this.pid = pid;
      this.pTree = pTree;
      if (this.pTree != null) {
        this.pTree.setSigKillInterval(sleepTimeBeforeSigKill);
      }
      this.memLimit = memLimit;
      this.pidFile = pidFile;
    }

    public TaskAttemptID getTID() {
      return tid;
    }

    public String getPID() {
      return pid;
    }

    public void setPid(String pid) {
      this.pid = pid;
    }

    public ProcfsBasedProcessTree getProcessTree() {
      return pTree;
    }

    public void setProcessTree(ProcfsBasedProcessTree pTree) {
      this.pTree = pTree;
    }

    public long getMemLimit() {
      return memLimit;
    }
  }

  @Override
  public void run() {

    LOG.info("Starting thread: " + this.getClass());

    while (true) {
      // Print the processTrees for debugging.
      if (LOG.isDebugEnabled()) {
        StringBuffer tmp = new StringBuffer("[ ");
        for (ProcessTreeInfo p : processTreeInfoMap.values()) {
          tmp.append(p.getPID());
          tmp.append(" ");
        }
        LOG.debug("Current ProcessTree list : "
            + tmp.substring(0, tmp.length()) + "]");
      }

      //Add new Tasks
      synchronized (tasksToBeAdded) {
        processTreeInfoMap.putAll(tasksToBeAdded);
        tasksToBeAdded.clear();
      }

      //Remove finished Tasks
      synchronized (tasksToBeRemoved) {
        for (TaskAttemptID tid : tasksToBeRemoved) {
          processTreeInfoMap.remove(tid);
        }
        tasksToBeRemoved.clear();
      }

      long memoryStillInUsage = 0;
      // Now, check memory usage and kill any overflowing tasks
      for (Iterator<Map.Entry<TaskAttemptID, ProcessTreeInfo>> it = processTreeInfoMap
          .entrySet().iterator(); it.hasNext();) {

        Map.Entry<TaskAttemptID, ProcessTreeInfo> entry = it.next();
        TaskAttemptID tid = entry.getKey();
        ProcessTreeInfo ptInfo = entry.getValue();
        String pId = ptInfo.getPID();

        // Initialize any uninitialized processTrees
        if (pId == null) {
          // get pid from pid-file
          pId = getPid(ptInfo.pidFile); 
          if (pId != null) {
            // PID will be null, either if the pid file is yet to be created
            // or if the tip is finished and we removed pidFile, but the TIP
            // itself is still retained in runningTasks till successful
            // transmission to JT

            // create process tree object
            ProcfsBasedProcessTree pt = new ProcfsBasedProcessTree(pId);
            LOG.debug("Tracking ProcessTree " + pId + " for the first time");

            ptInfo.setPid(pId);
            ptInfo.setProcessTree(pt);
            processTreeInfoMap.put(tid, ptInfo);
          }
        }
        // End of initializing any uninitialized processTrees

        if (pId == null) {
          continue; // processTree cannot be tracked
        }

        LOG.debug("Constructing ProcessTree for : PID = " + pId + " TID = "
            + tid);
        ProcfsBasedProcessTree pTree = ptInfo.getProcessTree();
        pTree = pTree.getProcessTree(); // get the updated process-tree
        ptInfo.setProcessTree(pTree); // update ptInfo with proces-tree of
                                      // updated state
        long currentMemUsage = pTree.getCumulativeVmem();
        long limit = ptInfo.getMemLimit();
        LOG.info("Memory usage of ProcessTree " + pId + " :" + currentMemUsage
            + "bytes. Limit : " + limit + "bytes");

        if (limit > taskTracker.getLimitMaxVMemPerTask()) {
          // TODO: With monitoring enabled and no scheduling based on
          // memory,users can seriously hijack the system by specifying memory
          // requirements well above the cluster wide limit. Ideally these jobs
          // should have been rejected by JT/scheduler. Because we can't do
          // that, in the minimum we should fail the tasks and hence the job.
          LOG.warn("Task " + tid
              + " 's maxVmemPerTask is greater than TT's limitMaxVmPerTask");
        }

        if (limit != JobConf.DISABLED_MEMORY_LIMIT
            && currentMemUsage > limit) {
          // Task (the root process) is still alive and overflowing memory.
          // Clean up.
          String msg = "TaskTree [pid=" + pId + ",tipID=" + tid
              + "] is running beyond memory-limits. Current usage : "
              + currentMemUsage + "bytes. Limit : " + limit + "bytes. Killing task.";
          LOG.warn(msg);
          taskTracker.cleanUpOverMemoryTask(tid, true, msg);

          // Now destroy the ProcessTree, remove it from monitoring map.
          pTree.destroy();
          it.remove();
          LOG.info("Removed ProcessTree with root " + pId);
        } else {
          // Accounting the total memory in usage for all tasks that are still
          // alive and within limits.
          memoryStillInUsage += currentMemUsage;
        }
      }

      LOG.debug("Memory still in usage across all tasks : " + memoryStillInUsage
          + "bytes. Total limit : " + maxMemoryAllowedForAllTasks);

      if (memoryStillInUsage > maxMemoryAllowedForAllTasks) {
        LOG.warn("The total memory usage is still overflowing TTs limits."
            + " Trying to kill a few tasks with the least progress.");
        killTasksWithLeastProgress(memoryStillInUsage);
      }
    
      // Sleep for some time before beginning next cycle
      try {
        LOG.debug(this.getClass() + " : Sleeping for " + monitoringInterval
            + " ms");
        Thread.sleep(monitoringInterval);
      } catch (InterruptedException ie) {
        LOG.warn(this.getClass()
            + " interrupted. Finishing the thread and returning.");
        return;
      }
    }
  }

  private void killTasksWithLeastProgress(long memoryStillInUsage) {

    List<TaskAttemptID> tasksToKill = new ArrayList<TaskAttemptID>();
    List<TaskAttemptID> tasksToExclude = new ArrayList<TaskAttemptID>();
    // Find tasks to kill so as to get memory usage under limits.
    while (memoryStillInUsage > maxMemoryAllowedForAllTasks) {
      // Exclude tasks that are already marked for
      // killing.
      TaskInProgress task = taskTracker.findTaskToKill(tasksToExclude);
      if (task == null) {
        break; // couldn't find any more tasks to kill.
      }

      TaskAttemptID tid = task.getTask().getTaskID();
      if (processTreeInfoMap.containsKey(tid)) {
        ProcessTreeInfo ptInfo = processTreeInfoMap.get(tid);
        ProcfsBasedProcessTree pTree = ptInfo.getProcessTree();
        memoryStillInUsage -= pTree.getCumulativeVmem();
        tasksToKill.add(tid);
      }
      // Exclude this task from next search because it is already
      // considered.
      tasksToExclude.add(tid);
    }

    // Now kill the tasks.
    if (!tasksToKill.isEmpty()) {
      for (TaskAttemptID tid : tasksToKill) {
        String msg =
            "Killing one of the least progress tasks - " + tid
                + ", as the cumulative memory usage of all the tasks on "
                + "the TaskTracker exceeds virtual memory limit "
                + maxMemoryAllowedForAllTasks + ".";
        LOG.warn(msg);
        // Kill the task and mark it as killed.
        taskTracker.cleanUpOverMemoryTask(tid, false, msg);
        // Now destroy the ProcessTree, remove it from monitoring map.
        ProcessTreeInfo ptInfo = processTreeInfoMap.get(tid);
        ProcfsBasedProcessTree pTree = ptInfo.getProcessTree();
        pTree.destroy();
        processTreeInfoMap.remove(tid);
        LOG.info("Removed ProcessTree with root " + ptInfo.getPID());
      }
    } else {
      LOG.info("The total memory usage is overflowing TTs limits. "
          + "But found no alive task to kill for freeing memory.");
    }
  }

  /**
   * Load pid of the task from the pidFile.
   * 
   * @param pidFileName
   * @return the pid of the task process.
   */
  private String getPid(String pidFileName) {
    if ((new File(pidFileName)).exists()) {
      return ProcfsBasedProcessTree.getPidFromPidFile(pidFileName);
     }
     return null;
  }
}
