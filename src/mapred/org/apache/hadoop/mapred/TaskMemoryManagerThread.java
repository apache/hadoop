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
import org.apache.hadoop.util.StringUtils;

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
    
    this(taskTracker.getTotalMemoryAllottedForTasksOnTT() * 1024 * 1024L,
      taskTracker.getJobConf().getLong(
        "mapred.tasktracker.taskmemorymanager.monitoring-interval", 
        5000L),
      taskTracker.getJobConf().getLong(
        "mapred.tasktracker.procfsbasedprocesstree.sleeptime-before-sigkill",
        ProcfsBasedProcessTree.DEFAULT_SLEEPTIME_BEFORE_SIGKILL));

    this.taskTracker = taskTracker;
  }

  // mainly for test purposes. note that the tasktracker variable is
  // not set here.
  TaskMemoryManagerThread(long maxMemoryAllowedForAllTasks,
                            long monitoringInterval,
                            long sleepTimeBeforeSigKill) {
    setName(this.getClass().getName());

    processTreeInfoMap = new HashMap<TaskAttemptID, ProcessTreeInfo>();
    tasksToBeAdded = new HashMap<TaskAttemptID, ProcessTreeInfo>();
    tasksToBeRemoved = new ArrayList<TaskAttemptID>();

    this.maxMemoryAllowedForAllTasks = maxMemoryAllowedForAllTasks;

    this.monitoringInterval = monitoringInterval;
    
    this.sleepTimeBeforeSigKill = sleepTimeBeforeSigKill;
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
        try {
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
          // as processes begin with an age 1, we want to see if there 
          // are processes more than 1 iteration old.
          long curMemUsageOfAgedProcesses = pTree.getCumulativeVmem(1);
          long limit = ptInfo.getMemLimit();
          LOG.info("Memory usage of ProcessTree " + pId + " :"
              + currentMemUsage + "bytes. Limit : " + limit + "bytes");

          if (isProcessTreeOverLimit(tid.toString(), currentMemUsage, 
                                      curMemUsageOfAgedProcesses, limit)) {
            // Task (the root process) is still alive and overflowing memory.
            // Clean up.
            String msg =
                "TaskTree [pid=" + pId + ",tipID=" + tid
                    + "] is running beyond memory-limits. Current usage : "
                    + currentMemUsage + "bytes. Limit : " + limit
                    + "bytes. Killing task.";
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
        } catch (Exception e) {
          // Log the exception and proceed to the next task.
          LOG.warn("Uncaught exception in TaskMemoryManager "
              + "while managing memory of " + tid + " : "
              + StringUtils.stringifyException(e));
        }
      }

      if (memoryStillInUsage > maxMemoryAllowedForAllTasks) {
        LOG.warn("The total memory in usage " + memoryStillInUsage
            + " is still overflowing TTs limits "
            + maxMemoryAllowedForAllTasks
            + ". Trying to kill a few tasks with the least progress.");
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

  /**
   * Check whether a task's process tree's current memory usage is over limit.
   * 
   * When a java process exec's a program, it could momentarily account for
   * double the size of it's memory, because the JVM does a fork()+exec()
   * which at fork time creates a copy of the parent's memory. If the 
   * monitoring thread detects the memory used by the task tree at the same
   * instance, it could assume it is over limit and kill the tree, for no
   * fault of the process itself.
   * 
   * We counter this problem by employing a heuristic check:
   * - if a process tree exceeds the memory limit by more than twice, 
   * it is killed immediately
   * - if a process tree has processes older than the monitoring interval
   * exceeding the memory limit by even 1 time, it is killed. Else it is given
   * the benefit of doubt to lie around for one more iteration.
   * 
   * @param tId Task Id for the task tree
   * @param currentMemUsage Memory usage of a task tree
   * @param curMemUsageOfAgedProcesses Memory usage of processes older than
   *                                    an iteration in a task tree
   * @param limit The limit specified for the task
   * @return true if the memory usage is more than twice the specified limit,
   *              or if processes in the tree, older than this thread's 
   *              monitoring interval, exceed the memory limit. False, 
   *              otherwise.
   */
  boolean isProcessTreeOverLimit(String tId, 
                                  long currentMemUsage, 
                                  long curMemUsageOfAgedProcesses, 
                                  long limit) {
    boolean isOverLimit = false;
    
    if (currentMemUsage > (2*limit)) {
      LOG.warn("Process tree for task: " + tId + " running over twice " +
                "the configured limit. Limit=" + limit + 
                ", current usage = " + currentMemUsage);
      isOverLimit = true;
    } else if (curMemUsageOfAgedProcesses > limit) {
      LOG.warn("Process tree for task: " + tId + " has processes older than 1 " +
          "iteration running over the configured limit. Limit=" + limit + 
          ", current usage = " + curMemUsageOfAgedProcesses);
      isOverLimit = true;
    }

    return isOverLimit; 
  }

  // method provided just for easy testing purposes
  boolean isProcessTreeOverLimit(ProcfsBasedProcessTree pTree, 
                                    String tId, long limit) {
    long currentMemUsage = pTree.getCumulativeVmem();
    // as processes begin with an age 1, we want to see if there are processes
    // more than 1 iteration old.
    long curMemUsageOfAgedProcesses = pTree.getCumulativeVmem(1);
    return isProcessTreeOverLimit(tId, currentMemUsage, 
                                  curMemUsageOfAgedProcesses, limit);
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
