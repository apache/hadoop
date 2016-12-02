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

package org.apache.hadoop.mapreduce.v2.hs.webapp;

import static org.apache.hadoop.yarn.util.StringHelper.pajoin;
import static org.apache.hadoop.yarn.webapp.YarnWebParams.APP_OWNER;
import static org.apache.hadoop.yarn.webapp.YarnWebParams.CONTAINER_ID;
import static org.apache.hadoop.yarn.webapp.YarnWebParams.CONTAINER_LOG_TYPE;
import static org.apache.hadoop.yarn.webapp.YarnWebParams.ENTITY_STRING;
import static org.apache.hadoop.yarn.webapp.YarnWebParams.NM_NODENAME;

import org.apache.hadoop.mapreduce.v2.app.AppContext;
import org.apache.hadoop.mapreduce.v2.app.webapp.AMParams;
import org.apache.hadoop.mapreduce.v2.hs.HistoryContext;
import org.apache.hadoop.yarn.webapp.GenericExceptionHandler;
import org.apache.hadoop.yarn.webapp.WebApp;

public class HsWebApp extends WebApp implements AMParams {

  private HistoryContext history;

  public HsWebApp(HistoryContext history) {
    this.history = history;
  }

  @Override
  public void setup() {
    bind(HsWebServices.class);
    bind(JAXBContextResolver.class);
    bind(GenericExceptionHandler.class);
    bind(AppContext.class).toInstance(history);
    bind(HistoryContext.class).toInstance(history);
    route("/", HsController.class);
    route("/app", HsController.class);
    route(pajoin("/job", JOB_ID), HsController.class, "job");
    route(pajoin("/conf", JOB_ID), HsController.class, "conf");
    routeWithoutDefaultView(pajoin("/downloadconf", JOB_ID),
        HsController.class, "downloadConf");
    route(pajoin("/jobcounters", JOB_ID), HsController.class, "jobCounters");
    route(pajoin("/singlejobcounter",JOB_ID, COUNTER_GROUP, COUNTER_NAME),
        HsController.class, "singleJobCounter");
    route(pajoin("/tasks", JOB_ID, TASK_TYPE), HsController.class, "tasks");
    route(pajoin("/attempts", JOB_ID, TASK_TYPE, ATTEMPT_STATE),
        HsController.class, "attempts");
    route(pajoin("/task", TASK_ID), HsController.class, "task");
    route(pajoin("/taskcounters", TASK_ID), HsController.class, "taskCounters");
    route(pajoin("/singletaskcounter",TASK_ID, COUNTER_GROUP, COUNTER_NAME),
        HsController.class, "singleTaskCounter");
    route("/about", HsController.class, "about");
    route(pajoin("/logs", NM_NODENAME, CONTAINER_ID, ENTITY_STRING, APP_OWNER,
        CONTAINER_LOG_TYPE), HsController.class, "logs");
    route(pajoin("/nmlogs", NM_NODENAME, CONTAINER_ID, ENTITY_STRING, APP_OWNER,
        CONTAINER_LOG_TYPE), HsController.class, "nmlogs");
  }
}

