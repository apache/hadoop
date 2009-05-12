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

class MemoryMatcher {

  private static final Log LOG = LogFactory.getLog(MemoryMatcher.class);
  private CapacityTaskScheduler scheduler;

  public MemoryMatcher(CapacityTaskScheduler capacityTaskScheduler) {
    this.scheduler = capacityTaskScheduler;
  }

  boolean isSchedulingBasedOnVmemEnabled() {
    LOG.debug("defaultMaxVmPerTask : " + scheduler.defaultMaxVmPerTask
        + " limitMaxVmemForTasks : " + scheduler.limitMaxVmemForTasks);
    if (scheduler.defaultMaxVmPerTask == JobConf.DISABLED_MEMORY_LIMIT
        || scheduler.limitMaxVmemForTasks == JobConf.DISABLED_MEMORY_LIMIT) {
      return false;
    }
    return true;
  }

  boolean isSchedulingBasedOnPmemEnabled() {
    LOG.debug("defaultPercentOfPmemInVmem : "
        + scheduler.defaultPercentOfPmemInVmem + " limitMaxPmemForTasks : "
        + scheduler.limitMaxPmemForTasks);
    if (scheduler.defaultPercentOfPmemInVmem == JobConf.DISABLED_MEMORY_LIMIT
        || scheduler.limitMaxPmemForTasks == JobConf.DISABLED_MEMORY_LIMIT) {
      return false;
    }
    return true;
  }

  /**
   * Obtain the virtual memory allocated for a job's tasks.
   * 
   * If the job has a configured value for the max-virtual memory, that will be
   * returned. Else, the cluster-wide default max-virtual memory for tasks is
   * returned.
   * 
   * This method can only be called after
   * {@link CapacityTaskScheduler#initializeMemoryRelatedConf()} is invoked.
   * 
   * @param jConf JobConf of the job
   * @return the virtual memory allocated for the job's tasks.
   */
  private long getVirtualMemoryForTask(JobConf jConf) {
    long vMemForTask = jConf.getMaxVirtualMemoryForTask();
    if (vMemForTask == JobConf.DISABLED_MEMORY_LIMIT) {
      vMemForTask =
          new JobConf().getLong(JobConf.MAPRED_TASK_DEFAULT_MAXVMEM_PROPERTY,
              scheduler.defaultMaxVmPerTask);
    }
    return vMemForTask;
  }

  /**
   * Obtain the physical memory allocated for a job's tasks.
   * 
   * If the job has a configured value for the max physical memory, that
   * will be returned. Else, the cluster-wide default physical memory for
   * tasks is returned.
   * 
   * This method can only be called after
   * {@link CapacityTaskScheduler#initializeMemoryRelatedConf()} is invoked.
   * 
   * @param jConf JobConf of the job
   * @return the physical memory allocated for the job's tasks
   */
  private long getPhysicalMemoryForTask(JobConf jConf) {
    long pMemForTask = jConf.getMaxPhysicalMemoryForTask();
    if (pMemForTask == JobConf.DISABLED_MEMORY_LIMIT) {
      pMemForTask =
          Math.round(getVirtualMemoryForTask(jConf)
              * scheduler.defaultPercentOfPmemInVmem);
    }
    return pMemForTask;
  }

  static class Memory {
    long vmem;
    long pmem;

    Memory(long vm, long pm) {
      this.vmem = vm;
      this.pmem = pm;
    }
  }

  /**
   * Find the memory that is already used by all the running tasks
   * residing on the given TaskTracker.
   * 
   * @param taskTracker
   * @return amount of memory that is used by the residing tasks,
   *          null if memory cannot be computed for some reason.
   */
  private synchronized Memory getMemReservedForTasks(
      TaskTrackerStatus taskTracker) {
    boolean disabledVmem = false;
    boolean disabledPmem = false;

    if (scheduler.defaultMaxVmPerTask == JobConf.DISABLED_MEMORY_LIMIT) {
      disabledVmem = true;
    }

    if (scheduler.defaultPercentOfPmemInVmem == JobConf.DISABLED_MEMORY_LIMIT) {
      disabledPmem = true;
    }

    if (disabledVmem && disabledPmem) {
      return new Memory(JobConf.DISABLED_MEMORY_LIMIT,
          JobConf.DISABLED_MEMORY_LIMIT);
    }

    long vmem = 0;
    long pmem = 0;

    for (TaskStatus task : taskTracker.getTaskReports()) {
      // the following task states are one in which the slot is
      // still occupied and hence memory of the task should be
      // accounted in used memory.
      if ((task.getRunState() == TaskStatus.State.RUNNING)
          || (task.getRunState() == TaskStatus.State.COMMIT_PENDING)) {
        JobInProgress job = scheduler.taskTrackerManager.getJob(
                                              task.getTaskID().getJobID());
        if (job == null) {
          // This scenario can happen if a job was completed/killed
          // and retired from JT's memory. In this state, we can ignore 
          // the running task status and compute memory for the rest of 
          // the tasks. However, any scheduling done with this computation
          // could result in over-subscribing of memory for tasks on this
          // TT (as the unaccounted for task is still running).
          // So, it is safer to not schedule anything for this TT
          // One of the ways of doing that is to return null from here
          // and check for null in the calling method.
          LOG.info("Task tracker: " + taskTracker.getHost() + " is reporting "
                    + "a running / commit pending task: " + task.getTaskID()
                    + " but no corresponding job was found. "
                    + "Maybe job was retired. Not computing "
                    + "memory values for this TT.");
          return null;
        }
        
        JobConf jConf =
            scheduler.taskTrackerManager.getJob(task.getTaskID().getJobID())
                .getJobConf();
        if (!disabledVmem) {
          vmem += getVirtualMemoryForTask(jConf);
        }
        if (!disabledPmem) {
          pmem += getPhysicalMemoryForTask(jConf);
        }
      }
    }

    return new Memory(vmem, pmem);
  }

  /**
   * Check if a TT has enough pmem and vmem to run this job.
   * @param job
   * @param taskTracker
   * @return true if this TT has enough memory for this job. False otherwise.
   */
  boolean matchesMemoryRequirements(JobInProgress job,
      TaskTrackerStatus taskTracker) {

    // ////////////// vmem based scheduling
    if (!isSchedulingBasedOnVmemEnabled()) {
      LOG.debug("One of the configuration parameters defaultMaxVmPerTask "
          + "and limitMaxVmemPerTasks is not configured. Scheduling based "
          + "on job's memory requirements is disabled, ignoring any value "
          + "set by job.");
      return true;
    }

    TaskTrackerStatus.ResourceStatus resourceStatus =
        taskTracker.getResourceStatus();
    long totalVMemOnTT = resourceStatus.getTotalVirtualMemory();
    long reservedVMemOnTT = resourceStatus.getReservedTotalMemory();

    if (totalVMemOnTT == JobConf.DISABLED_MEMORY_LIMIT
        || reservedVMemOnTT == JobConf.DISABLED_MEMORY_LIMIT) {
      return true;
    }

    if (reservedVMemOnTT > totalVMemOnTT) {
      return true;
    }

    long jobVMemForTask = job.getMaxVirtualMemoryForTask();
    if (jobVMemForTask == JobConf.DISABLED_MEMORY_LIMIT) {
      jobVMemForTask = scheduler.defaultMaxVmPerTask;
    }

    Memory memReservedForTasks = getMemReservedForTasks(taskTracker);
    if (memReservedForTasks == null) {
      // For some reason, maybe because we could not find the job
      // corresponding to a running task (as can happen if the job
      // is retired in between), we could not compute the memory state
      // on this TT. Treat this as an error, and fail memory
      // requirements.
      LOG.info("Could not compute memory for taskTracker: " 
                + taskTracker.getHost() + ". Failing memory requirements.");
      return false;
    }
    long vmemUsedOnTT = memReservedForTasks.vmem;
    long pmemUsedOnTT = memReservedForTasks.pmem;

    long freeVmemUsedOnTT = totalVMemOnTT - vmemUsedOnTT - reservedVMemOnTT;

    if (jobVMemForTask > freeVmemUsedOnTT) {
      return false;
    }

    // ////////////// pmem based scheduling

    long totalPmemOnTT = resourceStatus.getTotalPhysicalMemory();
    long reservedPmemOnTT = resourceStatus.getReservedPhysicalMemory();
    long jobPMemForTask = job.getMaxPhysicalMemoryForTask();
    long freePmemUsedOnTT = 0;

    if (isSchedulingBasedOnPmemEnabled()) {
      if (totalPmemOnTT == JobConf.DISABLED_MEMORY_LIMIT
          || reservedPmemOnTT == JobConf.DISABLED_MEMORY_LIMIT) {
        return true;
      }

      if (reservedPmemOnTT > totalPmemOnTT) {
        return true;
      }

      if (jobPMemForTask == JobConf.DISABLED_MEMORY_LIMIT) {
        jobPMemForTask =
            Math.round(jobVMemForTask * scheduler.defaultPercentOfPmemInVmem);
      }

      freePmemUsedOnTT = totalPmemOnTT - pmemUsedOnTT - reservedPmemOnTT;

      if (jobPMemForTask > freePmemUsedOnTT) {
        return false;
      }
    } else {
      LOG.debug("One of the configuration parameters "
          + "defaultPercentOfPmemInVmem and limitMaxPmemPerTasks is not "
          + "configured. Scheduling based on job's physical memory "
          + "requirements is disabled, ignoring any value set by job.");
    }

    LOG.debug("freeVMemOnTT = " + freeVmemUsedOnTT + " totalVMemOnTT = "
        + totalVMemOnTT + " freePMemOnTT = " + freePmemUsedOnTT
        + " totalPMemOnTT = " + totalPmemOnTT + " jobVMemForTask = "
        + jobVMemForTask + " jobPMemForTask = " + jobPMemForTask);
    return true;
  }
}
