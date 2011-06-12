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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapreduce.MRConfig;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.mapreduce.server.jobtracker.JTConfig;
import org.apache.hadoop.conf.Configuration;

class MemoryMatcher {

  private static final Log LOG = LogFactory.getLog(MemoryMatcher.class);
  private static JobTracker.MemoryLimits schedulerMemLimits =
      new JobTracker.MemoryLimits();


  public MemoryMatcher() {
    // initialize memory limits using official JobTracker values
    schedulerMemLimits.setMemSizeForMapSlot(
        JobTracker.getMemSizeForMapSlot());
    schedulerMemLimits.setMemSizeForReduceSlot(
        JobTracker.getMemSizeForReduceSlot());
    schedulerMemLimits.setMaxMemForMapTasks(
        JobTracker.getLimitMaxMemForMapTasks());
    schedulerMemLimits.setMaxMemForReduceTasks(
        JobTracker.getLimitMaxMemForReduceTasks());
  }

  /**
   * Find the memory that is already used by all the running tasks
   * residing on the given TaskTracker.
   * 
   * @param taskTracker
   * @param taskType 
   * @return amount of memory that is used by the residing tasks,
   *          null if memory cannot be computed for some reason.
   */
  synchronized Long getMemReservedForTasks(
      TaskTrackerStatus taskTracker, TaskType taskType) {
    long vmem = 0;

    for (TaskStatus task : taskTracker.getTaskReports()) {
      // the following task states are one in which the slot is
      // still occupied and hence memory of the task should be
      // accounted in used memory.
      if ((task.getRunState() == TaskStatus.State.RUNNING) ||
          (task.getRunState() == TaskStatus.State.UNASSIGNED) ||
          (task.inTaskCleanupPhase())) {
        // Get the memory "allotted" for this task based on number of slots
        long myVmem = 0;
        if (task.getIsMap() && taskType == TaskType.MAP) {
          long memSizePerMapSlot = getMemSizeForMapSlot();
          myVmem = 
            memSizePerMapSlot * task.getNumSlots();
        } else if (!task.getIsMap()
            && taskType == TaskType.REDUCE) {
          long memSizePerReduceSlot = getMemSizeForReduceSlot();
          myVmem = memSizePerReduceSlot * task.getNumSlots();
        }
        vmem += myVmem;
      }
    }

    return Long.valueOf(vmem);
  }

  /**
   * Check if a TT has enough memory to run of task specified from this job.
   * @param job
   * @param taskType 
   * @param taskTracker
   * @return true if this TT has enough memory for this job. False otherwise.
   */
  boolean matchesMemoryRequirements(JobInProgress job,TaskType taskType, 
                                    TaskTrackerStatus taskTracker) {

    if (LOG.isDebugEnabled()) {
      LOG.debug("Matching memory requirements of " + job.getJobID().toString()
                + " for scheduling on " + taskTracker.trackerName);
    }

    if (!isSchedulingBasedOnMemEnabled()) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Scheduling based on job's memory requirements is disabled."
                  + " Ignoring any value set by job.");
      }
      return true;
    }

    Long memUsedOnTT = getMemReservedForTasks(taskTracker, taskType);
    long totalMemUsableOnTT = 0;
    long memForThisTask = 0;
    if (taskType == TaskType.MAP) {
      memForThisTask = job.getMemoryForMapTask();
      totalMemUsableOnTT =
          getMemSizeForMapSlot() * taskTracker.getMaxMapSlots();
    } else if (taskType == TaskType.REDUCE) {
      memForThisTask = job.getMemoryForReduceTask();
      totalMemUsableOnTT =
          getMemSizeForReduceSlot()
              * taskTracker.getMaxReduceSlots();
    }

    long freeMemOnTT = totalMemUsableOnTT - memUsedOnTT.longValue();
    if (memForThisTask > freeMemOnTT) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("memForThisTask (" + memForThisTask + ") > freeMemOnTT ("
                  + freeMemOnTT + "). A " + taskType + " task from "
                  + job.getJobID().toString() + " cannot be scheduled on TT "
                  + taskTracker.trackerName);
      }
      return false;
    }

    if (LOG.isDebugEnabled()) {
      LOG.debug("memForThisTask = " + memForThisTask + ". freeMemOnTT = "
                + freeMemOnTT + ". A " + taskType.toString() + " task from "
                + job.getJobID().toString() + " matches memory requirements "
                + "on TT "+ taskTracker.trackerName);
    }
    return true;
  }

  static boolean isSchedulingBasedOnMemEnabled() {
    return schedulerMemLimits.isMemoryConfigSet();
  }

  public static void initializeMemoryRelatedConf(Configuration conf) {
    //handling @deprecated
    if (conf.get(
      CapacitySchedulerConf.DEFAULT_PERCENTAGE_OF_PMEM_IN_VMEM_PROPERTY) !=
      null) {
      LOG.warn(
        JobConf.deprecatedString(
          CapacitySchedulerConf.DEFAULT_PERCENTAGE_OF_PMEM_IN_VMEM_PROPERTY));
    }

    //handling @deprecated
    if (conf.get(CapacitySchedulerConf.UPPER_LIMIT_ON_TASK_PMEM_PROPERTY) !=
      null) {
      LOG.warn(
        JobConf.deprecatedString(
          CapacitySchedulerConf.UPPER_LIMIT_ON_TASK_PMEM_PROPERTY));
    }

    // Our local schedulerMemLimits instance and everything from here to the
    // end of the file is really just for TestCapacityScheduler, which assumes
    // it can set weird values and have them automatically reinitialized prior
    // to the subsequent test.  Otherwise the official JobTracker versions
    // would suffice.

    JobTracker.initializeMemoryRelatedConfig(conf, schedulerMemLimits);

    LOG.info(new StringBuilder().append("Scheduler configured with ")
        .append("(memSizeForMapSlotOnJT, memSizeForReduceSlotOnJT,")
        .append(" limitMaxMemForMapTasks, limitMaxMemForReduceTasks) (")
        .append(getMemSizeForMapSlot()).append(", ")
        .append(getMemSizeForReduceSlot()).append(", ")
        .append(getLimitMaxMemForMapSlot()).append(", ")
        .append(getLimitMaxMemForReduceSlot()).append(")"));
  }

  static long getMemSizeForMapSlot() {
    return schedulerMemLimits.getMemSizeForMapSlot();
  }

  static long getMemSizeForReduceSlot() {
    return schedulerMemLimits.getMemSizeForReduceSlot();
  }

  static long getLimitMaxMemForMapSlot() {
    return schedulerMemLimits.getMaxMemForMapTasks();
  }

  static long getLimitMaxMemForReduceSlot() {
    return schedulerMemLimits.getMaxMemForReduceTasks();
  }
}
