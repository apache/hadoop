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

package org.apache.hadoop.yarn.services.webapp;

import static org.apache.hadoop.yarn.services.utils.RestApiConstants.*;

import java.io.IOException;
import java.net.InetAddress;
import java.net.URI;
import java.util.Arrays;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.http.HttpServer2;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.yarn.webapp.GenericExceptionHandler;
import org.apache.hadoop.yarn.webapp.YarnJacksonJaxbJsonProvider;
import org.mortbay.jetty.webapp.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class launches the web application using Hadoop HttpServer2 (which uses
 * an embedded Jetty container). This is the entry point to your application.
 * The Java command used to launch this app should call the main method.
 */
public class ApplicationApiWebApp extends AbstractService {
  private static final Logger logger = LoggerFactory
      .getLogger(ApplicationApiWebApp.class);
  private static final String SEP = ";";

  // REST API server for YARN native services
  private HttpServer2 applicationApiServer;

  public static void main(String[] args) throws IOException {
    ApplicationApiWebApp apiWebApp = new ApplicationApiWebApp();
    try {
      apiWebApp.startWebApp();
    } catch (Exception e) {
      if (apiWebApp != null) {
        apiWebApp.close();
      }
    }
  }

  public ApplicationApiWebApp() {
    super(ApplicationApiWebApp.class.getName());
  }

  @Override
  protected void serviceStart() throws Exception {
    startWebApp();
    super.serviceStart();
  }

  @Override
  protected void serviceStop() throws Exception {
    if (applicationApiServer != null) {
      applicationApiServer.stop();
    }
    super.serviceStop();
  }

  protected void startWebApp() throws IOException {
    // The port that we should run on can be set into an environment variable
    // Look for that variable and default to 9191 if it isn't there.
    String webPort = System.getenv(PROPERTY_REST_SERVICE_PORT);
    if (StringUtils.isEmpty(webPort)) {
      webPort = "9191";
    }

    String webHost = System.getenv(PROPERTY_REST_SERVICE_HOST);
    if (StringUtils.isEmpty(webHost)) {
      webHost = InetAddress.getLocalHost().getHostName();
    }
    logger.info("YARN native services REST API running on host {} and port {}",
        webHost, webPort);
    logger.info("Configuration = {}", getConfig());

    applicationApiServer = new HttpServer2.Builder()
        .setName("services-rest-api")
        .addEndpoint(URI.create("http://" + webHost + ":" + webPort)).build();

    String apiPackages = "org.apache.hadoop.yarn.services.api" + SEP
        + "org.apache.hadoop.yarn.services.api.impl" + SEP
        + "org.apache.hadoop.yarn.services.resource" + SEP
        + "org.apache.hadoop.yarn.services.utils" + SEP
        + "org.apache.hadoop.yarn.services.webapp" + SEP
        + GenericExceptionHandler.class.getPackage().getName() + SEP
        + YarnJacksonJaxbJsonProvider.class.getPackage().getName();
    applicationApiServer.addJerseyResourcePackage(apiPackages, CONTEXT_ROOT
        + "/*");

    try {
      logger.info("Application starting up. Logging start...");
      applicationApiServer.start();
      logger.info("Server status = {}", applicationApiServer.toString());
      for (Configuration conf : applicationApiServer.getWebAppContext()
          .getConfigurations()) {
        logger.info("Configurations = {}", conf);
      }
      logger.info("Context Path = {}", Arrays.asList(applicationApiServer
          .getWebAppContext().getContextPath()));
      logger.info("ResourceBase = {}", Arrays.asList(applicationApiServer
          .getWebAppContext().getResourceBase()));
      logger.info("War = {}",
          Arrays.asList(applicationApiServer.getWebAppContext().getWar()));
    } catch (Exception ex) {
      logger.error("Hadoop HttpServer2 App **failed**", ex);
      throw ex;
    }
  }
}
