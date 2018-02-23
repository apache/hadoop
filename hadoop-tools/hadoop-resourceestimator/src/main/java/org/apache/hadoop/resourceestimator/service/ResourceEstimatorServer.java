/*
 *
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
 *
 */

package org.apache.hadoop.resourceestimator.service;

import java.io.IOException;
import java.net.URI;

import javax.ws.rs.core.UriBuilder;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.http.HttpServer2;
import org.apache.hadoop.resourceestimator.common.config.ResourceEstimatorConfiguration;
import org.apache.hadoop.service.CompositeService;
import org.apache.hadoop.util.ShutdownHookManager;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.webapp.GenericExceptionHandler;
import org.apache.hadoop.yarn.webapp.YarnJacksonJaxbJsonProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A simple embedded Hadoop HTTP server.
 */
public final class ResourceEstimatorServer extends CompositeService {
  private static final Logger LOGGER =
      LoggerFactory.getLogger(ResourceEstimatorServer.class.getName());
  private HttpServer2 webServer;
  private static URI baseURI;

  public ResourceEstimatorServer() {
    super(ResourceEstimatorServer.class.getName());
  }

  private static URI getBaseURI(Configuration config) {
    baseURI = UriBuilder.fromUri(ResourceEstimatorConfiguration.SERVICE_URI)
        .port(getPort(config)).build();
    return baseURI;
  }

  private static int getPort(Configuration config) {
    return config.getInt(ResourceEstimatorConfiguration.SERVICE_PORT,
        ResourceEstimatorConfiguration.DEFAULT_SERVICE_PORT);
  }

  @Override protected void serviceInit(Configuration conf) throws Exception {
    super.serviceInit(conf);
  }

  @Override protected void serviceStart() throws Exception {
    super.serviceStart();
    startResourceEstimatorApp();
  }

  private void join() {
    // keep the main thread that started the server up until it receives a stop
    // signal
    if (webServer != null) {
      try {
        webServer.join();
      } catch (InterruptedException ignore) {
      }
    }
  }

  @Override protected void serviceStop() throws Exception {
    if (webServer != null) {
      webServer.stop();
    }
    super.serviceStop();
  }

  private void startResourceEstimatorApp() throws IOException {
    Configuration config = new YarnConfiguration();
    config.addResource(ResourceEstimatorConfiguration.CONFIG_FILE);
    HttpServer2.Builder builder =
        new HttpServer2.Builder().setName("ResourceEstimatorServer")
            .setConf(config)
            //.setFindPort(true)
            .addEndpoint(getBaseURI(config));
    webServer = builder.build();
    webServer.addJerseyResourcePackage(
        ResourceEstimatorService.class.getPackage().getName() + ";"
            + GenericExceptionHandler.class.getPackage().getName() + ";"
            + YarnJacksonJaxbJsonProvider.class.getPackage().getName(), "/*");
    webServer.start();
  }

  /**
   * Start embedded Hadoop HTTP server.
   *
   * @return an instance of the started HTTP server.
   * @throws IOException in case there is an error while starting server.
   */
  static ResourceEstimatorServer startResourceEstimatorServer()
      throws IOException, InterruptedException {
    Configuration config = new YarnConfiguration();
    config.addResource(ResourceEstimatorConfiguration.CONFIG_FILE);
    ResourceEstimatorServer resourceEstimatorServer = null;
    try {
      resourceEstimatorServer = new ResourceEstimatorServer();
      ShutdownHookManager.get().addShutdownHook(
          new CompositeServiceShutdownHook(resourceEstimatorServer), 30);
      resourceEstimatorServer.init(config);
      resourceEstimatorServer.start();
    } catch (Throwable t) {
      LOGGER.error("Error starting ResourceEstimatorServer", t);
    }

    return resourceEstimatorServer;
  }

  public static void main(String[] args)
      throws InterruptedException, IOException {
    ResourceEstimatorServer server = startResourceEstimatorServer();
    server.join();
  }

  /**
   * Stop embedded Hadoop HTTP server.
   *
   * @throws Exception in case the HTTP server fails to shut down.
   */
  public void shutdown() throws Exception {
    LOGGER.info("Stopping resourceestimator service at: {}.",
        baseURI.toString());
    webServer.stop();
  }
}
