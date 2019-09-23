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

package org.apache.hadoop.yarn.server.webproxy;

import java.io.IOException;
import java.net.InetSocketAddress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.source.JvmMetrics;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.service.CompositeService;
import org.apache.hadoop.util.ExitUtil;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.JvmPauseMonitor;
import org.apache.hadoop.util.ShutdownHookManager;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.YarnUncaughtExceptionHandler;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ProxyServer will sit in between the end user and AppMaster
 * web interfaces.
 */
public class WebAppProxyServer extends CompositeService {

  /**
   * Priority of the ResourceManager shutdown hook.
   */
  public static final int SHUTDOWN_HOOK_PRIORITY = 30;

  private static final Logger LOG = LoggerFactory.getLogger(
      WebAppProxyServer.class);

  private WebAppProxy proxy = null;

  private JvmPauseMonitor pauseMonitor;

  public WebAppProxyServer() {
    super(WebAppProxyServer.class.getName());
  }

  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    Configuration config = new YarnConfiguration(conf);
    doSecureLogin(conf);
    proxy = new WebAppProxy();
    addService(proxy);

    DefaultMetricsSystem.initialize("WebAppProxyServer");
    JvmMetrics jm = JvmMetrics.initSingleton("WebAppProxyServer", null);
    pauseMonitor = new JvmPauseMonitor();
    addService(pauseMonitor);
    jm.setPauseMonitor(pauseMonitor);

    super.serviceInit(config);
  }

  @Override
  protected void serviceStart() throws Exception {
    super.serviceStart();
  }

  @Override
  protected void serviceStop() throws Exception {
    super.serviceStop();
    DefaultMetricsSystem.shutdown();
  }

  /**
   * Log in as the Kerberose principal designated for the proxy
   * @param conf the configuration holding this information in it.
   * @throws IOException on any error.
   */
  protected void doSecureLogin(Configuration conf) throws IOException {
    InetSocketAddress socAddr = getBindAddress(conf);  
    SecurityUtil.login(conf, YarnConfiguration.PROXY_KEYTAB,
        YarnConfiguration.PROXY_PRINCIPAL, socAddr.getHostName());
  }

  /**
   * Retrieve PROXY bind address from configuration
   *
   * @param conf
   * @return InetSocketAddress
   */
  public static InetSocketAddress getBindAddress(Configuration conf) {
    return conf.getSocketAddr(
        YarnConfiguration.PROXY_BIND_HOST,
        YarnConfiguration.PROXY_ADDRESS,
        YarnConfiguration.DEFAULT_PROXY_ADDRESS,
        YarnConfiguration.DEFAULT_PROXY_PORT);
  }

  public static void main(String[] args) {
    Thread.setDefaultUncaughtExceptionHandler(new YarnUncaughtExceptionHandler());
    StringUtils.startupShutdownMessage(WebAppProxyServer.class, args, LOG);
    try {
      YarnConfiguration configuration = new YarnConfiguration();
      new GenericOptionsParser(configuration, args);
      WebAppProxyServer proxyServer = startServer(configuration);
      proxyServer.proxy.join();
    } catch (Throwable t) {
      ExitUtil.terminate(-1, t);
    }
  }

  /**
   * Start proxy server.
   * 
   * @return proxy server instance.
   */
  protected static WebAppProxyServer startServer(Configuration configuration)
      throws Exception {
    WebAppProxyServer proxy = new WebAppProxyServer();
    ShutdownHookManager.get().addShutdownHook(
        new CompositeServiceShutdownHook(proxy), SHUTDOWN_HOOK_PRIORITY);
    proxy.init(configuration);
    proxy.start();
    return proxy;
  }

}
