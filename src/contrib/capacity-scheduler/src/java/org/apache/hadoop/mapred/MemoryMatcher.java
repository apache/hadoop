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

  boolean isSchedulingBasedOnMemEnabled() {
    if (scheduler.getLimitMaxMemForMapSlot()
                                  == JobConf.DISABLED_MEMORY_LIMIT
        || scheduler.getLimitMaxMemForReduceSlot()
                                  == JobConf.DISABLED_MEMORY_LIMIT
        || scheduler.getMemSizeForMapSlot()
                                  == JobConf.DISABLED_MEMORY_LIMIT
        || scheduler.getMemSizeForReduceSlot()
                                  == JobConf.DISABLED_MEMORY_LIMIT) {
      return false;
    }
    return true;
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
      TaskTrackerStatus taskTracker, CapacityTaskScheduler.TYPE taskType) {
    long vmem = 0;

    for (TaskStatus task : taskTracker.getTaskReports()) {
      // the following task states are one in which the slot is
      // still occupied and hence memory of the task should be
      // accounted in used memory.
      if ((task.getRunState() == TaskStatus.State.RUNNING)
          || (task.getRunState() == TaskStatus.State.COMMIT_PENDING)) {
        JobInProgress job =
            scheduler.taskTrackerManager.getJob(task.getTaskID().getJobID());
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

        JobConf jConf = job.getJobConf();

        // Get the memory "allotted" for this task by rounding off the job's
        // tasks' memory limits to the nearest multiple of the slot-memory-size
        // set on JT. This essentially translates to tasks of a high memory job
        // using multiple slots.
        long myVmem = 0;
        if (task.getIsMap() && taskType.equals(CapacityTaskScheduler.TYPE.MAP)) {
          myVmem = jConf.getMemoryForMapTask();
          myVmem =
              (long) (scheduler.getMemSizeForMapSlot() * Math
                  .ceil((float) myVmem
                      / (float) scheduler.getMemSizeForMapSlot()));
        } else if (!task.getIsMap()
            && taskType.equals(CapacityTaskScheduler.TYPE.REDUCE)) {
          myVmem = jConf.getMemoryForReduceTask();
          myVmem =
              (long) (scheduler.getMemSizeForReduceSlot() * Math
                  .ceil((float) myVmem
                      / (float) scheduler.getMemSizeForReduceSlot()));
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
  boolean matchesMemoryRequirements(JobInProgress job,
      CapacityTaskScheduler.TYPE taskType, TaskTrackerStatus taskTracker) {

    LOG.debug("Matching memory requirements of " + job.getJobID().toString()
        + " for scheduling on " + taskTracker.trackerName);

    if (!isSchedulingBasedOnMemEnabled()) {
      LOG.debug("Scheduling based on job's memory requirements is disabled."
          + " Ignoring any value set by job.");
      return true;
    }

    Long memUsedOnTT = getMemReservedForTasks(taskTracker, taskType);
    if (memUsedOnTT == null) {
      // For some reason, maybe because we could not find the job
      // corresponding to a running task (as can happen if the job
      // is retired in between), we could not compute the memory state
      // on this TT. Treat this as an error, and fail memory
      // requirements.
      LOG.info("Could not compute memory for taskTracker: "
          + taskTracker.getHost() + ". Failing memory requirements.");
      return false;
    }

    long totalMemUsableOnTT = 0;

    long memForThisTask = 0;
    if (taskType.equals(CapacityTaskScheduler.TYPE.MAP)) {
      memForThisTask = job.getJobConf().getMemoryForMapTask();
      totalMemUsableOnTT =
          scheduler.getMemSizeForMapSlot() * taskTracker.getMaxMapTasks();
    } else if (taskType.equals(CapacityTaskScheduler.TYPE.REDUCE)) {
      memForThisTask = job.getJobConf().getMemoryForReduceTask();
      totalMemUsableOnTT =
          scheduler.getMemSizeForReduceSlot()
              * taskTracker.getMaxReduceTasks();
    }

    long freeMemOnTT = totalMemUsableOnTT - memUsedOnTT.longValue();
    if (memForThisTask > freeMemOnTT) {
      LOG.debug("memForThisTask (" + memForThisTask + ") > freeMemOnTT ("
          + freeMemOnTT + "). A " + taskType + " task from "
          + job.getJobID().toString() + " cannot be scheduled on TT "
          + taskTracker.trackerName);
      return false;
    }

    LOG.debug("memForThisTask = " + memForThisTask + ". freeMemOnTT = "
        + freeMemOnTT + ". A " + taskType.toString() + " task from "
        + job.getJobID().toString() + " matches memory requirements on TT "
        + taskTracker.trackerName);
    return true;
  }
}
