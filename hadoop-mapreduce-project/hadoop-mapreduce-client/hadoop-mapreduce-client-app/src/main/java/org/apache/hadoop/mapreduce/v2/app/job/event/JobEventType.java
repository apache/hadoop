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
 * Event types handled by Job.
 */
public enum JobEventType {

  //Producer:Client
  JOB_KILL,

  //Producer:MRAppMaster
  JOB_INIT,
  JOB_START,

  //Producer:Task
  JOB_TASK_COMPLETED,
  JOB_MAP_TASK_RESCHEDULED,
  JOB_TASK_ATTEMPT_COMPLETED,

  //Producer:CommitterEventHandler
  JOB_SETUP_COMPLETED,
  JOB_SETUP_FAILED,
  JOB_COMMIT_COMPLETED,
  JOB_COMMIT_FAILED,
  JOB_ABORT_COMPLETED,

  //Producer:Job
  JOB_COMPLETED,

  //Producer:Any component
  JOB_DIAGNOSTIC_UPDATE,
  INTERNAL_ERROR,
  JOB_COUNTER_UPDATE,
  
  //Producer:TaskAttemptListener
  JOB_TASK_ATTEMPT_FETCH_FAILURE,
  
  //Producer:RMContainerAllocator
  JOB_UPDATED_NODES
  
}
