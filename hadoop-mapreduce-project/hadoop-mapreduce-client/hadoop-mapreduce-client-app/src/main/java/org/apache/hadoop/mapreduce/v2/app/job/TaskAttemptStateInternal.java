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

package org.apache.hadoop.mapreduce.v2.app.job;

import org.apache.hadoop.classification.InterfaceAudience.Private;

/**
* TaskAttemptImpl internal state machine states.
*
*/
@Private
public enum TaskAttemptStateInternal {
  NEW, 
  UNASSIGNED, 
  ASSIGNED, 
  RUNNING, 
  COMMIT_PENDING,

  // Transition into SUCCESS_FINISHING_CONTAINER
  // After the attempt finishes successfully from
  // TaskUmbilicalProtocol's point of view, it will transition to
  // SUCCESS_FINISHING_CONTAINER state. That will give a chance for the
  // container to exit by itself. In the transition,
  // the attempt will notify the task via T_ATTEMPT_SUCCEEDED so that
  // from job point of view, the task is considered succeeded.

  // Transition out of SUCCESS_FINISHING_CONTAINER
  // The attempt will transition from SUCCESS_FINISHING_CONTAINER to
  // SUCCESS_CONTAINER_CLEANUP if it doesn't receive container exit
  // notification within TASK_EXIT_TIMEOUT;
  // Or it will transition to SUCCEEDED if it receives container exit
  // notification from YARN.
  SUCCESS_FINISHING_CONTAINER,

  // Transition into FAIL_FINISHING_CONTAINER
  // After the attempt fails from
  // TaskUmbilicalProtocol's point of view, it will transition to
  // FAIL_FINISHING_CONTAINER state. That will give a chance for the container
  // to exit by itself. In the transition,
  // the attempt will notify the task via T_ATTEMPT_FAILED so that
  // from job point of view, the task is considered failed.

  // Transition out of FAIL_FINISHING_CONTAINER
  // The attempt will transition from FAIL_FINISHING_CONTAINER to
  // FAIL_CONTAINER_CLEANUP if it doesn't receive container exit
  // notification within TASK_EXIT_TIMEOUT;
  // Or it will transition to FAILED if it receives container exit
  // notification from YARN.
  FAIL_FINISHING_CONTAINER,

  SUCCESS_CONTAINER_CLEANUP,
  SUCCEEDED,
  FAIL_CONTAINER_CLEANUP, 
  FAIL_TASK_CLEANUP, 
  FAILED, 
  KILL_CONTAINER_CLEANUP, 
  KILL_TASK_CLEANUP, 
  KILLED,
}