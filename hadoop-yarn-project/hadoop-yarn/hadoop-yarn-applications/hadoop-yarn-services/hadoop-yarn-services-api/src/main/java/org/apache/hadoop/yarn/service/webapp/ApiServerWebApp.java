/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.service.webapp;

import org.apache.hadoop.http.HttpServer2;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.AuthenticationFilterInitializer;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.webapp.GenericExceptionHandler;
import org.apache.hadoop.yarn.webapp.YarnJacksonJaxbJsonProvider;
import org.eclipse.jetty.webapp.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.apache.hadoop.yarn.conf.YarnConfiguration.RM_WEBAPP_SPNEGO_KEYTAB_FILE_KEY;
import static org.apache.hadoop.yarn.conf.YarnConfiguration.RM_WEBAPP_SPNEGO_USER_NAME_KEY;
import static org.apache.hadoop.yarn.service.conf.YarnServiceConf.*;

/**
 * This class launches the web service using Hadoop HttpServer2 (which uses
 * an embedded Jetty container). This is the entry point to your service.
 * The Java command used to launch this app should call the main method.
 */
public class ApiServerWebApp extends AbstractService {
  private static final Logger logger = LoggerFactory
      .getLogger(ApiServerWebApp.class);
  private static final String SEP = ";";

  // REST API server for YARN native services
  private HttpServer2 apiServer;
  private InetSocketAddress bindAddress;

  public static void main(String[] args) throws IOException {
    ApiServerWebApp apiWebApp = new ApiServerWebApp();
    try {
      apiWebApp.init(new YarnConfiguration());
      apiWebApp.serviceStart();
    } catch (Exception e) {
      logger.error("Got exception starting", e);
      apiWebApp.close();
    }
  }

  public ApiServerWebApp() {
    super(ApiServerWebApp.class.getName());
  }

  @Override
  protected void serviceStart() throws Exception {
    bindAddress = getConfig().getSocketAddr(API_SERVER_ADDRESS,
        DEFAULT_API_SERVER_ADDRESS, DEFAULT_API_SERVER_PORT);
    logger.info("YARN API server running on " + bindAddress);
    if (UserGroupInformation.isSecurityEnabled()) {
      doSecureLogin(getConfig());
    }
    startWebApp();
    super.serviceStart();
  }

  @Override
  protected void serviceStop() throws Exception {
    if (apiServer != null) {
      apiServer.stop();
    }
    super.serviceStop();
  }

  private void doSecureLogin(org.apache.hadoop.conf.Configuration conf)
      throws IOException {
    SecurityUtil.login(conf, YarnConfiguration.RM_KEYTAB,
        YarnConfiguration.RM_PRINCIPAL, bindAddress.getHostName());
    addFilters(conf);
  }

  private void addFilters(org.apache.hadoop.conf.Configuration conf) {
    // Always load pseudo authentication filter to parse "user.name" in an URL
    // to identify a HTTP request's user.
    boolean hasHadoopAuthFilterInitializer = false;
    String filterInitializerConfKey = "hadoop.http.filter.initializers";
    Class<?>[] initializersClasses =
        conf.getClasses(filterInitializerConfKey);
    List<String> targets = new ArrayList<String>();
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
  }

  private void startWebApp() throws IOException {
    URI uri = URI.create("http://" + NetUtils.getHostPortString(bindAddress));

    apiServer = new HttpServer2.Builder()
        .setName("api-server")
        .setConf(getConfig())
        .setSecurityEnabled(UserGroupInformation.isSecurityEnabled())
        .setUsernameConfKey(RM_WEBAPP_SPNEGO_USER_NAME_KEY)
        .setKeytabConfKey(RM_WEBAPP_SPNEGO_KEYTAB_FILE_KEY)
        .addEndpoint(uri).build();

    String apiPackages =
        ApiServer.class.getPackage().getName() + SEP
            + GenericExceptionHandler.class.getPackage().getName() + SEP
            + YarnJacksonJaxbJsonProvider.class.getPackage().getName();
    apiServer.addJerseyResourcePackage(apiPackages, "/*");

    try {
      logger.info("Service starting up. Logging start...");
      apiServer.start();
      logger.info("Server status = {}", apiServer.toString());
      for (Configuration conf : apiServer.getWebAppContext()
          .getConfigurations()) {
        logger.info("Configurations = {}", conf);
      }
      logger.info("Context Path = {}", Collections.singletonList(
          apiServer.getWebAppContext().getContextPath()));
      logger.info("ResourceBase = {}", Collections.singletonList(
          apiServer.getWebAppContext().getResourceBase()));
      logger.info("War = {}", Collections
          .singletonList(apiServer.getWebAppContext().getWar()));
    } catch (Exception ex) {
      logger.error("Hadoop HttpServer2 App **failed**", ex);
      throw ex;
    }
  }
}
