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

package org.apache.hadoop.fs.http.server;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.http.server.metrics.HttpFSServerMetrics;
import org.apache.hadoop.lib.server.ServerException;
import org.apache.hadoop.lib.service.FileSystemAccess;
import org.apache.hadoop.lib.servlet.ServerWebApp;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.util.JvmPauseMonitor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Bootstrap class that manages the initialization and destruction of the
 * HttpFSServer server, it is a <code>javax.servlet.ServletContextListener
 * </code> implementation that is wired in HttpFSServer's WAR
 * <code>WEB-INF/web.xml</code>.
 * <p>
 * It provides acces to the server context via the singleton {@link #get}.
 * <p>
 * All the configuration is loaded from configuration properties prefixed
 * with <code>httpfs.</code>.
 */
@InterfaceAudience.Private
public class HttpFSServerWebApp extends ServerWebApp {
  private static final Logger LOG =
    LoggerFactory.getLogger(HttpFSServerWebApp.class);

  /**
   * Server name and prefix for all configuration properties.
   */
  public static final String NAME = "httpfs";

  /**
   * Configuration property that defines HttpFSServer admin group.
   */
  public static final String CONF_ADMIN_GROUP = "admin.group";

  private static HttpFSServerWebApp SERVER;
  private static HttpFSServerMetrics metrics;

  private String adminGroup;

  /**
   * Default constructor.
   *
   * @throws IOException thrown if the home/conf/log/temp directory paths
   * could not be resolved.
   */
  public HttpFSServerWebApp() throws IOException {
    super(NAME);
  }

  /**
   * Constructor used for testing purposes.
   */
  public HttpFSServerWebApp(String homeDir, String configDir, String logDir,
                               String tempDir, Configuration config) {
    super(NAME, homeDir, configDir, logDir, tempDir, config);
  }

  /**
   * Constructor used for testing purposes.
   */
  public HttpFSServerWebApp(String homeDir, Configuration config) {
    super(NAME, homeDir, config);
  }

  /**
   * Initializes the HttpFSServer server, loads configuration and required
   * services.
   *
   * @throws ServerException thrown if HttpFSServer server could not be
   * initialized.
   */
  @Override
  public void init() throws ServerException {
    if (SERVER != null) {
      throw new RuntimeException("HttpFSServer server already initialized");
    }
    SERVER = this;
    super.init();
    adminGroup = getConfig().get(getPrefixedName(CONF_ADMIN_GROUP), "admin");
    LOG.info("Connects to Namenode [{}]",
             get().get(FileSystemAccess.class).getFileSystemConfiguration().
               get(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY));
    setMetrics(getConfig());
  }

  /**
   * Shutdowns all running services.
   */
  @Override
  public void destroy() {
    SERVER = null;
    if (metrics != null) {
      metrics.shutdown();
    }
    super.destroy();
  }

  private static void setMetrics(Configuration config) {
    LOG.info("Initializing HttpFSServerMetrics");
    metrics = HttpFSServerMetrics.create(config, "HttpFSServer");
    JvmPauseMonitor pauseMonitor = new JvmPauseMonitor();
    pauseMonitor.init(config);
    pauseMonitor.start();
    metrics.getJvmMetrics().setPauseMonitor(pauseMonitor);
    FSOperations.setBufferSize(config);
    DefaultMetricsSystem.initialize("HttpFSServer");
  }
  /**
   * Returns HttpFSServer server singleton, configuration and services are
   * accessible through it.
   *
   * @return the HttpFSServer server singleton.
   */
  public static HttpFSServerWebApp get() {
    return SERVER;
  }

  /**
   * gets the HttpFSServerMetrics instance.
   * @return the HttpFSServerMetrics singleton.
   */
  public static HttpFSServerMetrics getMetrics() {
    return metrics;
  }

  /**
   * Returns HttpFSServer admin group.
   *
   * @return httpfs admin group.
   */
  public String getAdminGroup() {
    return adminGroup;
  }

}
