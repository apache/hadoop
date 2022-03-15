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

package org.apache.hadoop.mapreduce.v2.app.webapp;

/**
 * Params constants for the AM webapp and the history webapp.
 */
public interface AMParams {
  String RM_WEB = "rm.web";
  String APP_ID = "app.id";
  String JOB_ID = "job.id";
  String TASK_ID = "task.id";
  String TASK_TYPE = "task.type";
  String TASK_STATE = "task.state";
  String ATTEMPT_STATE = "attempt.state";
  String COUNTER_GROUP = "counter.group";
  String COUNTER_NAME = "counter.name";
}
