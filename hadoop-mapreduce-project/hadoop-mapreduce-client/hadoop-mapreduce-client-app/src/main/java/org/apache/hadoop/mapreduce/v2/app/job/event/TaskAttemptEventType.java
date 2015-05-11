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

package org.apache.hadoop.mapreduce.v2.app.job.event;

/**
 * Event types handled by TaskAttempt.
 */
public enum TaskAttemptEventType {

  //Producer:Task
  TA_SCHEDULE,
  TA_RESCHEDULE,
  TA_RECOVER,

  //Producer:Client, Task
  TA_KILL,

  //Producer:ContainerAllocator
  TA_ASSIGNED,
  TA_CONTAINER_COMPLETED,

  //Producer:ContainerLauncher
  TA_CONTAINER_LAUNCHED,
  TA_CONTAINER_LAUNCH_FAILED,
  TA_CONTAINER_CLEANED,

  //Producer:TaskAttemptListener
  TA_DIAGNOSTICS_UPDATE,
  TA_COMMIT_PENDING, 
  TA_DONE,
  TA_FAILMSG,
  TA_UPDATE,
  TA_TIMED_OUT,
  TA_PREEMPTED,

  //Producer:Client
  TA_FAILMSG_BY_CLIENT,

  //Producer:TaskCleaner
  TA_CLEANUP_DONE,

  //Producer:Job
  TA_TOO_MANY_FETCH_FAILURE,
}
