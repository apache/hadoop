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

package org.apache.hadoop.yarn.server.resourcemanager.webapp;

import static org.apache.hadoop.yarn.util.StringHelper.pajoin;

import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.security.ApplicationACLsManager;
import org.apache.hadoop.yarn.webapp.WebApp;

/**
 * The RM webapp
 */
public class RMWebApp extends WebApp {
  static final String APP_ID = "app.id";
  static final String QUEUE_NAME = "queue.name";
  static final String NODE_STATE = "node.state";

  private final ResourceManager rm;

  public RMWebApp(ResourceManager rm) {
    this.rm = rm;
  }

  @Override
  public void setup() {
    if (rm != null) {
      bind(ResourceManager.class).toInstance(rm);
      bind(RMContext.class).toInstance(rm.getRMContext());
      bind(ApplicationACLsManager.class).toInstance(
          rm.getApplicationACLsManager());
    }
    route("/", RmController.class);
    route(pajoin("/nodes", NODE_STATE), RmController.class, "nodes");
    route("/apps", RmController.class);
    route("/cluster", RmController.class, "about");
    route(pajoin("/app", APP_ID), RmController.class, "app");
    route("/scheduler", RmController.class, "scheduler");
    route(pajoin("/queue", QUEUE_NAME), RmController.class, "queue");
  }
}
