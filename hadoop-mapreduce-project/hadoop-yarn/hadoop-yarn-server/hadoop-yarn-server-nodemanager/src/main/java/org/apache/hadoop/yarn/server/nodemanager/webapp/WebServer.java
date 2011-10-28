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

package org.apache.hadoop.yarn.server.nodemanager.webapp;

import static org.apache.hadoop.yarn.util.StringHelper.pajoin;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.YarnException;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.nodemanager.Context;
import org.apache.hadoop.yarn.server.nodemanager.ResourceView;
import org.apache.hadoop.yarn.server.security.ApplicationACLsManager;
import org.apache.hadoop.yarn.service.AbstractService;
import org.apache.hadoop.yarn.webapp.WebApp;
import org.apache.hadoop.yarn.webapp.WebApps;

public class WebServer extends AbstractService {

  private static final Log LOG = LogFactory.getLog(WebServer.class);

  private final Context nmContext;
  private final NMWebApp nmWebApp;
  private WebApp webApp;

  public WebServer(Context nmContext, ResourceView resourceView,
      ApplicationACLsManager aclsManager) {
    super(WebServer.class.getName());
    this.nmContext = nmContext;
    this.nmWebApp = new NMWebApp(resourceView, aclsManager);
  }

  @Override
  public synchronized void init(Configuration conf) {
    super.init(conf);
  }

  @Override
  public synchronized void start() {
    String bindAddress = getConfig().get(YarnConfiguration.NM_WEBAPP_ADDRESS,
        YarnConfiguration.DEFAULT_NM_WEBAPP_ADDRESS);
    LOG.info("Instantiating NMWebApp at " + bindAddress);
    try {
      this.webApp = WebApps.$for("node", Context.class, this.nmContext).at(
          bindAddress).with(getConfig()).start(this.nmWebApp);
    } catch (Exception e) {
      String msg = "NMWebapps failed to start.";
      LOG.error(msg, e);
      throw new YarnException(msg);
    }
    super.start();
  }

  @Override
  public synchronized void stop() {
    if (this.webApp != null) {
      this.webApp.stop();
    }
    super.stop();
  }

  public static class NMWebApp extends WebApp implements NMWebParams {

    private final ResourceView resourceView;
    private final ApplicationACLsManager aclsManager;

    public NMWebApp(ResourceView resourceView,
        ApplicationACLsManager aclsManager) {
      this.resourceView = resourceView;
      this.aclsManager = aclsManager;
    }

    @Override
    public void setup() {
      bind(ResourceView.class).toInstance(this.resourceView);
      bind(ApplicationACLsManager.class).toInstance(this.aclsManager);
      route("/", NMController.class, "info");
      route("/node", NMController.class, "node");
      route("/allApplications", NMController.class, "allApplications");
      route("/allContainers", NMController.class, "allContainers");
      route(pajoin("/application", APPLICATION_ID), NMController.class,
          "application");
      route(pajoin("/container", CONTAINER_ID), NMController.class,
          "container");
      route(
          pajoin("/containerlogs", CONTAINER_ID, APP_OWNER, CONTAINER_LOG_TYPE),
          NMController.class, "logs");
    }

  }
}
