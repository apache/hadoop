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

import static org.apache.hadoop.yarn.util.StringHelper.pajoin;

import org.apache.hadoop.yarn.webapp.GenericExceptionHandler;
import org.apache.hadoop.yarn.webapp.WebApp;

/**
 * Application master webapp
 */
public class AMWebApp extends WebApp implements AMParams {

  @Override
  public void setup() {
    bind(JAXBContextResolver.class);
    bind(GenericExceptionHandler.class);
    bind(AMWebServices.class);
    route("/", AppController.class);
    route("/app", AppController.class);
    route(pajoin("/job", JOB_ID), AppController.class, "job");
    route(pajoin("/conf", JOB_ID), AppController.class, "conf");
    route(pajoin("/jobcounters", JOB_ID), AppController.class, "jobCounters");
    route(pajoin("/singlejobcounter",JOB_ID, COUNTER_GROUP, COUNTER_NAME),
        AppController.class, "singleJobCounter");
    route(pajoin("/tasks", JOB_ID, TASK_TYPE, TASK_STATE), AppController.class, "tasks");
    route(pajoin("/attempts", JOB_ID, TASK_TYPE, ATTEMPT_STATE),
        AppController.class, "attempts");
    route(pajoin("/task", TASK_ID), AppController.class, "task");
    route(pajoin("/taskcounters", TASK_ID), AppController.class, "taskCounters");
    route(pajoin("/singletaskcounter",TASK_ID, COUNTER_GROUP, COUNTER_NAME),
        AppController.class, "singleTaskCounter");
  }
}
