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

import javax.servlet.Filter;
import org.glassfish.jersey.internal.inject.AbstractBinder;
import org.glassfish.jersey.jettison.JettisonFeature;
import org.glassfish.jersey.server.ResourceConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.AuthenticationFilterInitializer;
import org.apache.hadoop.security.HttpCrossOriginFilterInitializer;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.server.nodemanager.Context;
import org.apache.hadoop.yarn.server.nodemanager.LocalDirsHandlerService;
import org.apache.hadoop.yarn.server.nodemanager.ResourceView;
import org.apache.hadoop.yarn.server.security.ApplicationACLsManager;
import org.apache.hadoop.yarn.webapp.GenericExceptionHandler;
import org.apache.hadoop.yarn.webapp.WebApp;
import org.apache.hadoop.yarn.webapp.WebApps;
import org.apache.hadoop.yarn.webapp.YarnWebParams;
import org.apache.hadoop.yarn.webapp.util.WebAppUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class WebServer extends AbstractService {

  private static final Logger LOG =
       LoggerFactory.getLogger(WebServer.class);

  private final Context nmContext;
  private final NMWebApp nmWebApp;
  private final ResourceView resourceView;
  private WebApp webApp;
  private int port;

  public WebServer(Context nmContext, ResourceView resView,
      ApplicationACLsManager aclsManager,
      LocalDirsHandlerService dirsHandler) {
    super(WebServer.class.getName());
    this.nmContext = nmContext;
    this.nmWebApp = new NMWebApp(resView, aclsManager, dirsHandler);
    this.resourceView = resView;
  }

  protected ResourceConfig configure() {
    ResourceConfig config = new ResourceConfig();
    config.packages("org.apache.hadoop.yarn.server.nodemanager.webapp");
    config.register(new JerseyBinder());
    config.register(NMWebServices.class);
    config.register(GenericExceptionHandler.class);
    config.register(new JettisonFeature()).register(JAXBContextResolver.class);
    return config;
  }

  private class JerseyBinder extends AbstractBinder {
    @Override
    protected void configure() {
      bind(nmContext).to(Context.class).named("nm");
      bind(nmWebApp).to(WebApp.class).named("webapp");
      bind(resourceView).to(ResourceView.class).named("view");
    }
  }

  @Override
  protected void serviceStart() throws Exception {
    Configuration conf = getConfig();
    Map<String, String> params = new HashMap<>();
    Map<String, String> terminalParams = new HashMap<>();
    terminalParams.put("resourceBase", WebServer.class
        .getClassLoader().getResource("TERMINAL").toExternalForm());
    terminalParams.put("dirAllowed", "false");
    terminalParams.put("pathInfoOnly", "true");
    String bindAddress = WebAppUtils.getWebAppBindURL(conf,
        YarnConfiguration.NM_BIND_HOST, WebAppUtils.getNMWebAppURLWithoutScheme(conf));
    boolean enableCors = conf
        .getBoolean(YarnConfiguration.NM_WEBAPP_ENABLE_CORS_FILTER,
            YarnConfiguration.DEFAULT_NM_WEBAPP_ENABLE_CORS_FILTER);
    if (enableCors) {
      getConfig().setBoolean(HttpCrossOriginFilterInitializer.PREFIX
          + HttpCrossOriginFilterInitializer.ENABLED_SUFFIX, true);
    }

    // Always load pseudo authentication filter to parse "user.name" in an URL
    // to identify a HTTP request's user.
    boolean hasHadoopAuthFilterInitializer = false;
    String filterInitializerConfKey = "hadoop.http.filter.initializers";
    Class<?>[] initializersClasses = conf.getClasses(filterInitializerConfKey);
    List<String> targets = new ArrayList<>();
    if (initializersClasses != null) {
      for (Class<?> initializer : initializersClasses) {
        if (initializer.getName().equals(
            AuthenticationFilterInitializer.class.getName())) {
          hasHadoopAuthFilterInitializer = true;
          break;
        }
        targets.add(initializer.getName());
      }
    }
    if (!hasHadoopAuthFilterInitializer) {
      targets.add(AuthenticationFilterInitializer.class.getName());
      conf.set(filterInitializerConfKey, StringUtils.join(",", targets));
    }
    ContainerShellWebSocket.init(nmContext);
    LOG.info("Instantiating NMWebApp at {}.", bindAddress);
    try {
      this.webApp = WebApps
          .$for("node", Context.class, this.nmContext, "jersey-ws")
          .at(bindAddress)
          .withServlet("ContainerShellWebSocket", "/container/*",
           ContainerShellWebSocketServlet.class, params, false)
          .withServlet("Terminal", "/terminal/*",
           TerminalServlet.class, terminalParams, false)
          .with(conf)
          .withHttpSpnegoPrincipalKey(YarnConfiguration.NM_WEBAPP_SPNEGO_USER_NAME_KEY)
          .withHttpSpnegoKeytabKey(YarnConfiguration.NM_WEBAPP_SPNEGO_KEYTAB_FILE_KEY)
          .withCSRFProtection(YarnConfiguration.NM_CSRF_PREFIX)
          .withXFSProtection(YarnConfiguration.NM_XFS_PREFIX)
          .withResourceConfig(configure())
          .start(this.nmWebApp);
      this.port = this.webApp.httpServer().getConnectorAddress(0).getPort();
    } catch (Exception e) {
      String msg = "NMWebapps failed to start.";
      LOG.error(msg, e);
      throw new YarnRuntimeException(msg, e);
    }
    super.serviceStart();
  }

  public int getPort() {
    return this.port;
  }

  @Override
  protected void serviceStop() throws Exception {
    if (this.webApp != null) {
      LOG.debug("Stopping webapp");
      this.webApp.stop();
    }
    super.serviceStop();
  }

  public static class NMWebApp extends WebApp implements YarnWebParams {

    private final ResourceView resourceView;
    private final ApplicationACLsManager aclsManager;
    private final LocalDirsHandlerService dirsHandler;

    public NMWebApp(ResourceView resourceView,
        ApplicationACLsManager aclsManager,
        LocalDirsHandlerService dirsHandler) {
      this.resourceView = resourceView;
      this.aclsManager = aclsManager;
      this.dirsHandler = dirsHandler;
    }

    @Override
    public void setup() {
      bind(GenericExceptionHandler.class);
      bind(JAXBContextResolver.class);
      bind(ResourceView.class).toInstance(this.resourceView);
      bind(ApplicationACLsManager.class).toInstance(this.aclsManager);
      bind(LocalDirsHandlerService.class).toInstance(dirsHandler);
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
      route("/errors-and-warnings", NMController.class, "errorsAndWarnings");
    }

    @Override
    protected Class<? extends Filter> getWebAppFilterClass() {
      return NMWebAppFilter.class;
    }
  }
}
