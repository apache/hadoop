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

package org.apache.hadoop.mapreduce.v2.app;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptId;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskAttemptEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskAttemptEventType;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.util.AbstractLivelinessMonitor;
import org.apache.hadoop.yarn.util.SystemClock;

/**
 * This class generates TA_TIMED_OUT if the task attempt stays in FINISHING
 * state for too long.
 */
@SuppressWarnings({"unchecked", "rawtypes"})
public class TaskAttemptFinishingMonitor extends
    AbstractLivelinessMonitor<TaskAttemptId> {

  private EventHandler eventHandler;

  public TaskAttemptFinishingMonitor(EventHandler eventHandler) {
    super("TaskAttemptFinishingMonitor", SystemClock.getInstance());
    this.eventHandler = eventHandler;
  }

  public void init(Configuration conf) {
    super.init(conf);
    int expireIntvl = conf.getInt(MRJobConfig.TASK_EXIT_TIMEOUT,
        MRJobConfig.TASK_EXIT_TIMEOUT_DEFAULT);
    int checkIntvl = conf.getInt(
        MRJobConfig.TASK_EXIT_TIMEOUT_CHECK_INTERVAL_MS,
        MRJobConfig.TASK_EXIT_TIMEOUT_CHECK_INTERVAL_MS_DEFAULT);

    setExpireInterval(expireIntvl);
    setMonitorInterval(checkIntvl);
  }

  @Override
  protected void expire(TaskAttemptId id) {
    eventHandler.handle(
        new TaskAttemptEvent(id,
        TaskAttemptEventType.TA_TIMED_OUT));
  }
}
