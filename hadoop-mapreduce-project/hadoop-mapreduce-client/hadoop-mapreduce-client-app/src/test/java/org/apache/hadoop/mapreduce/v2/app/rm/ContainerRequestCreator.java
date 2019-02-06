/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.mapreduce.v2.app.rm;

import org.apache.hadoop.mapreduce.v2.api.records.JobId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskType;
import org.apache.hadoop.mapreduce.v2.util.MRBuilderUtils;
import org.apache.hadoop.net.NetworkTopology;
import org.apache.hadoop.yarn.api.records.Resource;

final class ContainerRequestCreator {

  private ContainerRequestCreator() {}

  static ContainerRequestEvent createRequest(JobId jobId, int taskAttemptId,
          Resource resource, String[] hosts) {
    return createRequest(jobId, taskAttemptId, resource, hosts,
            false, false);
  }

  static ContainerRequestEvent createRequest(JobId jobId, int taskAttemptId,
          Resource resource, String[] hosts, boolean earlierFailedAttempt,
          boolean reduce) {
    final TaskId taskId;
    if (reduce) {
      taskId = MRBuilderUtils.newTaskId(jobId, 0, TaskType.REDUCE);
    } else {
      taskId = MRBuilderUtils.newTaskId(jobId, 0, TaskType.MAP);
    }
    TaskAttemptId attemptId = MRBuilderUtils.newTaskAttemptId(taskId,
            taskAttemptId);

    if (earlierFailedAttempt) {
      return ContainerRequestEvent
              .createContainerRequestEventForFailedContainer(attemptId,
                      resource);
    }
    return new ContainerRequestEvent(attemptId, resource, hosts,
            new String[]{NetworkTopology.DEFAULT_RACK});
  }
}
