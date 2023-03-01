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

package org.apache.hadoop.yarn.server.router.webapp;

import org.apache.hadoop.yarn.server.resourcemanager.webapp.JAXBContextResolver;
import org.apache.hadoop.yarn.server.router.Router;
import org.apache.hadoop.yarn.webapp.GenericExceptionHandler;
import org.apache.hadoop.yarn.webapp.WebApp;
import org.apache.hadoop.yarn.webapp.YarnWebParams;

import static org.apache.hadoop.yarn.util.StringHelper.pajoin;

/**
 * The Router webapp.
 */
public class RouterWebApp extends WebApp implements YarnWebParams {
  private Router router;

  public RouterWebApp(Router router) {
    this.router = router;
  }

  @Override
  public void setup() {
    bind(JAXBContextResolver.class);
    bind(RouterWebServices.class);
    bind(GenericExceptionHandler.class);
    bind(RouterWebApp.class).toInstance(this);

    if (router != null) {
      bind(Router.class).toInstance(router);
    }
    route("/", RouterController.class);
    route("/cluster", RouterController.class, "about");
    route("/about", RouterController.class, "about");
    route(pajoin("/apps", APP_SC, APP_STATE), RouterController.class, "apps");
    route(pajoin("/nodes", NODE_SC), RouterController.class, "nodes");
    route("/federation", RouterController.class, "federation");
    route(pajoin("/nodelabels", NODE_SC), RouterController.class, "nodeLabels");
  }
}
