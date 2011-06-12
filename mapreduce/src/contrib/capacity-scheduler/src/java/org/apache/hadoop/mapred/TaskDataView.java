/**
 * Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
* <p/>
* http://www.apache.org/licenses/LICENSE-2.0
* <p/>
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/

package org.apache.hadoop.mapred;

import org.apache.hadoop.mapreduce.TaskType;

/**
 * Task view class for the job .
 * returns the running pending and other information for a Job(JobInProgress).
 *
 * has a factory method which provides
 * map and reduce data view based on the type
 *
 */
abstract class TaskDataView {
  abstract int getRunningTasks(JobInProgress job);

  abstract int getPendingTasks(JobInProgress job);

  abstract int getSlotsPerTask(JobInProgress job);

  abstract TaskSchedulingContext getTSI(QueueSchedulingContext qsi);

  abstract int getNumReservedTaskTrackers(JobInProgress job);

  int getSlotsOccupied(JobInProgress job) {
    return (getNumReservedTaskTrackers(job) + getRunningTasks(job)) *
      getSlotsPerTask(job);
  }

  /**
   * Check if the given job has sufficient reserved tasktrackers for all its
   * pending tasks.
   *
   * @param job job to check for sufficient reserved tasktrackers
   * @return <code>true</code> if the job has reserved tasktrackers,
   *         else <code>false</code>
   */
  boolean hasSufficientReservedTaskTrackers(JobInProgress job) {
    return getNumReservedTaskTrackers(job) >= getPendingTasks(job);
  }

  private static TaskDataView mapTaskDataView;
  private static TaskDataView reduceTaskDataView;

  static TaskDataView getTaskDataView(TaskType type) {
     if(type == TaskType.MAP) {
       if(mapTaskDataView == null) {
         mapTaskDataView = new MapTaskDataView();
       }
       return mapTaskDataView;
     }else if(type == TaskType.REDUCE) {
       if(reduceTaskDataView == null) {
         reduceTaskDataView = new ReduceTaskDataView();
       }
       return reduceTaskDataView;
     }
    return null;
  }

  /**
   * The data view for map tasks
   */
  static class MapTaskDataView extends TaskDataView {
    MapTaskDataView() {
    }

    @Override
    int getRunningTasks(JobInProgress job) {
      return job.runningMaps();
    }

    @Override
    int getPendingTasks(JobInProgress job) {
      return job.pendingMaps();
    }

    @Override
    int getSlotsPerTask(JobInProgress job) {
      return
        job.getJobConf().computeNumSlotsPerMap(
          MemoryMatcher.getMemSizeForMapSlot());
    }

    @Override
    TaskSchedulingContext getTSI(QueueSchedulingContext qsi) {
      return qsi.getMapTSC();
    }

    int getNumReservedTaskTrackers(JobInProgress job) {
      return job.getNumReservedTaskTrackersForMaps();
    }

  }

  /**
   *  The data view for reduce tasks
   */
  static class ReduceTaskDataView extends TaskDataView {
    ReduceTaskDataView() {
    }

    @Override
    int getRunningTasks(JobInProgress job) {
      return job.runningReduces();
    }

    @Override
    int getPendingTasks(JobInProgress job) {
      return job.pendingReduces();
    }

    @Override
    int getSlotsPerTask(JobInProgress job) {
      return
        job.getJobConf().
          computeNumSlotsPerReduce(MemoryMatcher.getMemSizeForReduceSlot());
    }

    @Override
    TaskSchedulingContext getTSI(QueueSchedulingContext qsi) {
      return qsi.getReduceTSC();
    }

    int getNumReservedTaskTrackers(JobInProgress job) {
      return job.getNumReservedTaskTrackersForReduces();
    }

  }
}
