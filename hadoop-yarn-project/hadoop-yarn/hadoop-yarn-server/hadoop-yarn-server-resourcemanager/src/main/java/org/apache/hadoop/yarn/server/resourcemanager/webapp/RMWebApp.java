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

import java.net.InetSocketAddress;

import org.apache.hadoop.ha.HAServiceProtocol.HAServiceState;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.util.RMHAUtils;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.resourcemanager.security.QueueACLsManager;
import org.apache.hadoop.yarn.server.security.ApplicationACLsManager;
import org.apache.hadoop.yarn.webapp.GenericExceptionHandler;
import org.apache.hadoop.yarn.webapp.WebApp;
import org.apache.hadoop.yarn.webapp.YarnWebParams;

import com.sun.jersey.guice.spi.container.servlet.GuiceContainer;

/**
 * The RM webapp
 */
public class RMWebApp extends WebApp implements YarnWebParams {

  private final ResourceManager rm;
  private boolean standby = false;

  public RMWebApp(ResourceManager rm) {
    this.rm = rm;
  }

  @Override
  public void setup() {
    bind(JAXBContextResolver.class);
    bind(RMWebServices.class);
    bind(GenericExceptionHandler.class);
    bind(RMWebApp.class).toInstance(this);

    if (rm != null) {
      bind(ResourceManager.class).toInstance(rm);
      bind(RMContext.class).toInstance(rm.getRMContext());
      bind(ApplicationACLsManager.class).toInstance(
          rm.getApplicationACLsManager());
      bind(QueueACLsManager.class).toInstance(rm.getQueueACLsManager());
    }
    route("/", RmController.class);
    route(pajoin("/nodes", NODE_STATE), RmController.class, "nodes");
    route(pajoin("/apps", APP_STATE), RmController.class);
    route("/cluster", RmController.class, "about");
    route(pajoin("/app", APPLICATION_ID), RmController.class, "app");
    route("/scheduler", RmController.class, "scheduler");
    route(pajoin("/queue", QUEUE_NAME), RmController.class, "queue");
  }

  @Override
  protected Class<? extends GuiceContainer> getWebAppFilterClass() {
    return RMWebAppFilter.class;
  }

  public void checkIfStandbyRM() {
    standby = (rm.getRMContext().getHAServiceState() == HAServiceState.STANDBY);
  }

  public boolean isStandby() {
    return standby;
  }

  @Override
  public String getRedirectPath() {
    if (standby) {
      return buildRedirectPath();
    } else
      return super.getRedirectPath();
  }

  private String buildRedirectPath() {
    // make a copy of the original configuration so not to mutate it. Also use
    // an YarnConfiguration to force loading of yarn-site.xml.
    YarnConfiguration yarnConf = new YarnConfiguration(rm.getConfig());
    String activeRMHAId = RMHAUtils.findActiveRMHAId(yarnConf);
    String path = "";
    if (activeRMHAId != null) {
      yarnConf.set(YarnConfiguration.RM_HA_ID, activeRMHAId);

      InetSocketAddress sock = YarnConfiguration.useHttps(yarnConf)
          ? yarnConf.getSocketAddr(YarnConfiguration.RM_WEBAPP_HTTPS_ADDRESS,
              YarnConfiguration.DEFAULT_RM_WEBAPP_HTTPS_ADDRESS,
              YarnConfiguration.DEFAULT_RM_WEBAPP_HTTPS_PORT)
          : yarnConf.getSocketAddr(YarnConfiguration.RM_WEBAPP_ADDRESS,
              YarnConfiguration.DEFAULT_RM_WEBAPP_ADDRESS,
              YarnConfiguration.DEFAULT_RM_WEBAPP_PORT);

      path = sock.getHostName() + ":" + Integer.toString(sock.getPort());
      path = YarnConfiguration.useHttps(yarnConf)
          ? "https://" + path
          : "http://" + path;
    }
    return path;
  }
}
