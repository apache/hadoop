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

package org.apache.hadoop.mapreduce.v2.app2.job.event;

/**
 * Event types handled by TaskAttempt.
 */
public enum TaskAttemptEventType {

  //Producer:Task, Speculator
  TA_SCHEDULE,

  //Producer: TaskAttemptListener
  TA_STARTED_REMOTELY,
  TA_STATUS_UPDATE,
  TA_DIAGNOSTICS_UPDATE,
  TA_COMMIT_PENDING,
  TA_DONE,
  TA_FAILED,
  TA_TIMED_OUT,
  
  //Producer: Client
  TA_FAIL_REQUEST,
  
  //Producer: Client, Scheduler, On speculation.
  TA_KILL_REQUEST,

  //Producer: Container / Scheduler.
  // Indicates that the RM considers the container to be complete. Implies the 
  // JVM is done, except in one case. TOOD: document the case.
  TA_CONTAINER_TERMINATING,
  TA_CONTAINER_TERMINATED,
  TA_NODE_FAILED,
  
  //Producer: Job
  TA_TOO_MANY_FETCH_FAILURES,
  
  //Older unused.
//  TA_KILL,
//  TA_ASSIGNED,
//  TA_CONTAINER_LAUNCHED,
//  TA_CONTAINER_LAUNCH_FAILED,
//  TA_CONTAINER_CLEANED,
//  TA_FAILMSG,
//  TA_UPDATE,
//  TA_CLEANUP_DONE,
//  TA_TOO_MANY_FETCH_FAILURE
}
