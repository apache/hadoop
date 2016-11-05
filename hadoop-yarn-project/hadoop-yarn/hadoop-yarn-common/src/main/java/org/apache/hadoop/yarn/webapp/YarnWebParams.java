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

package org.apache.hadoop.yarn.webapp;

import org.apache.hadoop.classification.InterfaceAudience;

@InterfaceAudience.LimitedPrivate({"YARN", "MapReduce"})
public interface YarnWebParams {
  static final String RM_WEB_UI = "ResourceManager";
  static final String APP_HISTORY_WEB_UI = "ApplicationHistoryServer";
  
  String NM_NODENAME = "nm.id";
  String APPLICATION_ID = "app.id";
  String APPLICATION_ATTEMPT_ID = "appattempt.id";
  String CONTAINER_ID = "container.id";
  String CONTAINER_LOG_TYPE= "log.type";
  String ENTITY_STRING = "entity.string";
  String APP_OWNER = "app.owner";
  String APP_STATE = "app.state";
  String APP_START_TIME_BEGIN = "app.started-time.begin";
  String APP_START_TIME_END = "app.started-time.end";
  String APPS_NUM = "apps.num";
  String QUEUE_NAME = "queue.name";
  String NODE_STATE = "node.state";
  String NODE_LABEL = "node.label";
  String WEB_UI_TYPE = "web.ui.type";
  String NEXT_REFRESH_INTERVAL = "next.refresh.interval";
  String ERROR_MESSAGE = "error.message";
}
