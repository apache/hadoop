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
package org.apache.hadoop.yarn.server.globalpolicygenerator.webapp;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.server.globalpolicygenerator.GlobalPolicyGenerator;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.JAXBContextResolver;
import org.apache.hadoop.yarn.webapp.GenericExceptionHandler;
import org.apache.hadoop.yarn.webapp.WebApp;
import org.glassfish.jersey.internal.inject.AbstractBinder;
import org.glassfish.jersey.jettison.JettisonFeature;
import org.glassfish.jersey.server.ResourceConfig;

/**
 * The GPG webapp.
 */
public class GPGWebApp extends WebApp {
  private GlobalPolicyGenerator gpg;

  public GPGWebApp(GlobalPolicyGenerator gpg) {
    this.gpg = gpg;
  }

  @Override
  public void setup() {
    bind(GPGWebApp.class).toInstance(this);
    if (gpg != null) {
      bind(GlobalPolicyGenerator.class).toInstance(gpg);
    }
    route("/", GPGController.class, "overview");
    route("/policies", GPGController.class, "policies");
  }

  public ResourceConfig resourceConfig() {
    ResourceConfig config = new ResourceConfig();
    config.packages("org.apache.hadoop.yarn.server.globalpolicygenerator.webapp");
    config.register(new JerseyBinder());
    config.register(GPGWebServices.class);
    config.register(GenericExceptionHandler.class);
    config.register(new JettisonFeature()).register(JAXBContextResolver.class);
    return config;
  }

  private class JerseyBinder extends AbstractBinder {
    @Override
    protected void configure() {
      bind(gpg).to(GlobalPolicyGenerator.class).named("gpg");
      bind(gpg.getConfig()).to(Configuration.class).named("conf");
    }
  }
}
