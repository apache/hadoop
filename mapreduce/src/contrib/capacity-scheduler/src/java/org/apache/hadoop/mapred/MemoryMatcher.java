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
  static long memSizeForMapSlotOnJT = JobConf.DISABLED_MEMORY_LIMIT;
  static long memSizeForReduceSlotOnJT = JobConf.DISABLED_MEMORY_LIMIT;
  static long limitMaxMemForMapTasks = JobConf.DISABLED_MEMORY_LIMIT;
  static long limitMaxMemForReduceTasks = JobConf.DISABLED_MEMORY_LIMIT;


  public MemoryMatcher() {
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

    LOG.debug("Matching memory requirements of " + job.getJobID().toString()
        + " for scheduling on " + taskTracker.trackerName);

    if (!isSchedulingBasedOnMemEnabled()) {
      LOG.debug("Scheduling based on job's memory requirements is disabled."
          + " Ignoring any value set by job.");
      return true;
    }

    Long memUsedOnTT = getMemReservedForTasks(taskTracker, taskType);
    long totalMemUsableOnTT = 0;
    long memForThisTask = 0;
    if (taskType == TaskType.MAP) {
      memForThisTask = job.getJobConf().getMemoryForMapTask();
      totalMemUsableOnTT =
          getMemSizeForMapSlot() * taskTracker.getMaxMapSlots();
    } else if (taskType == TaskType.REDUCE) {
      memForThisTask = job.getJobConf().getMemoryForReduceTask();
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
    if (getLimitMaxMemForMapSlot()
                                  == JobConf.DISABLED_MEMORY_LIMIT
        || getLimitMaxMemForReduceSlot()
                                  == JobConf.DISABLED_MEMORY_LIMIT
        || getMemSizeForMapSlot()
                                  == JobConf.DISABLED_MEMORY_LIMIT
        || getMemSizeForReduceSlot()
                                  == JobConf.DISABLED_MEMORY_LIMIT) {
      return false;
    }
    return true;
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

    if (conf.get(JobConf.MAPRED_TASK_DEFAULT_MAXVMEM_PROPERTY) != null) {
      LOG.warn(
        JobConf.deprecatedString(
          JobConf.MAPRED_TASK_DEFAULT_MAXVMEM_PROPERTY));
    }

    memSizeForMapSlotOnJT =
        JobConf.normalizeMemoryConfigValue(conf.getLong(
            MRConfig.MAPMEMORY_MB, JobConf.DISABLED_MEMORY_LIMIT));
    memSizeForReduceSlotOnJT =
        JobConf.normalizeMemoryConfigValue(conf.getLong(
            MRConfig.REDUCEMEMORY_MB,
            JobConf.DISABLED_MEMORY_LIMIT));

    //handling @deprecated values
    if (conf.get(JobConf.UPPER_LIMIT_ON_TASK_VMEM_PROPERTY) != null) {
      LOG.warn(
        JobConf.deprecatedString(
          JobConf.UPPER_LIMIT_ON_TASK_VMEM_PROPERTY)+
          " instead use " + JTConfig.JT_MAX_MAPMEMORY_MB +
          " and " + JTConfig.JT_MAX_REDUCEMEMORY_MB
      );

      limitMaxMemForMapTasks = limitMaxMemForReduceTasks =
        JobConf.normalizeMemoryConfigValue(
          conf.getLong(
            JobConf.UPPER_LIMIT_ON_TASK_VMEM_PROPERTY,
            JobConf.DISABLED_MEMORY_LIMIT));
      if (limitMaxMemForMapTasks != JobConf.DISABLED_MEMORY_LIMIT &&
        limitMaxMemForMapTasks >= 0) {
        limitMaxMemForMapTasks = limitMaxMemForReduceTasks =
          limitMaxMemForMapTasks /
            (1024 * 1024); //Converting old values in bytes to MB
      }
    } else {
      limitMaxMemForMapTasks =
        JobConf.normalizeMemoryConfigValue(
          conf.getLong(
            JTConfig.JT_MAX_MAPMEMORY_MB, JobConf.DISABLED_MEMORY_LIMIT));
      limitMaxMemForReduceTasks =
        JobConf.normalizeMemoryConfigValue(
          conf.getLong(
            JTConfig.JT_MAX_REDUCEMEMORY_MB, JobConf.DISABLED_MEMORY_LIMIT));
    }
    LOG.info(String.format("Scheduler configured with "
        + "(memSizeForMapSlotOnJT, memSizeForReduceSlotOnJT, "
        + "limitMaxMemForMapTasks, limitMaxMemForReduceTasks)"
        + " (%d,%d,%d,%d)", Long.valueOf(memSizeForMapSlotOnJT), Long
        .valueOf(memSizeForReduceSlotOnJT), Long
        .valueOf(limitMaxMemForMapTasks), Long
        .valueOf(limitMaxMemForReduceTasks)));
  }

  static long  getMemSizeForMapSlot() {
    return memSizeForMapSlotOnJT;
  }

  static long getMemSizeForReduceSlot() {
    return memSizeForReduceSlotOnJT;
  }

  static long getLimitMaxMemForMapSlot() {
    return limitMaxMemForMapTasks;
  }

  static long getLimitMaxMemForReduceSlot() {
    return limitMaxMemForReduceTasks;
  }
}
