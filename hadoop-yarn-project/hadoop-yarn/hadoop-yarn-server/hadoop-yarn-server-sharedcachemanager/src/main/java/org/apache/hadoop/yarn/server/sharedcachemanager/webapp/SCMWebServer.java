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

package org.apache.hadoop.yarn.server.sharedcachemanager.webapp;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.sharedcachemanager.SharedCacheManager;
import org.apache.hadoop.yarn.webapp.WebApp;
import org.apache.hadoop.yarn.webapp.WebApps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A very simple web interface for the metrics reported by
 * {@link org.apache.hadoop.yarn.server.sharedcachemanager.SharedCacheManager}
 * TODO: Security for web ui (See YARN-2774)
 */
@Private
@Unstable
public class SCMWebServer extends AbstractService {
  private static final Logger LOG =
      LoggerFactory.getLogger(SCMWebServer.class);

  private final SharedCacheManager scm;
  private WebApp webApp;
  private String bindAddress;

  public SCMWebServer(SharedCacheManager scm) {
    super(SCMWebServer.class.getName());
    this.scm = scm;
  }

  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    this.bindAddress = getBindAddress(conf);
    super.serviceInit(conf);
  }

  private String getBindAddress(Configuration conf) {
    return conf.get(YarnConfiguration.SCM_WEBAPP_ADDRESS,
        YarnConfiguration.DEFAULT_SCM_WEBAPP_ADDRESS);
  }

  @Override
  protected void serviceStart() throws Exception {
    SCMWebApp scmWebApp = new SCMWebApp(scm);
    this.webApp = WebApps.$for("sharedcache").at(bindAddress).start(scmWebApp);
    LOG.info("Instantiated " + SCMWebApp.class.getName() + " at " + bindAddress);
  }

  @Override
  protected void serviceStop() throws Exception {
    if (this.webApp != null) {
      this.webApp.stop();
    }
  }

  private class SCMWebApp extends WebApp {
    private final SharedCacheManager scm;

    public SCMWebApp(SharedCacheManager scm) {
      this.scm = scm;
    }

    @Override
    public void setup() {
      if (scm != null) {
        bind(SharedCacheManager.class).toInstance(scm);
      }
      route("/", SCMController.class, "overview");
    }
  }
}