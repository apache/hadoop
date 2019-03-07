/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.hdds.server;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdds.conf.HddsConfServlet;
import org.apache.hadoop.http.HttpConfig;
import org.apache.hadoop.http.HttpServer2;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.net.NetUtils;
import org.eclipse.jetty.webapp.WebAppContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.http.HttpServlet;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Optional;

import static org.apache.hadoop.hdds.HddsUtils.getHostNameFromConfigKeys;
import static org.apache.hadoop.hdds.HddsUtils.getPortNumberFromConfigKeys;

/**
 * Base class for HTTP server of the Ozone related components.
 */
public abstract class BaseHttpServer {

  private static final Logger LOG =
      LoggerFactory.getLogger(BaseHttpServer.class);
  protected static final String PROMETHEUS_SINK = "PROMETHEUS_SINK";

  private HttpServer2 httpServer;
  private final Configuration conf;

  private InetSocketAddress httpAddress;
  private InetSocketAddress httpsAddress;

  private HttpConfig.Policy policy;

  private String name;
  private PrometheusMetricsSink prometheusMetricsSink;

  private boolean prometheusSupport;

  private boolean profilerSupport;

  public BaseHttpServer(Configuration conf, String name) throws IOException {
    this.name = name;
    this.conf = conf;
    policy = DFSUtil.getHttpPolicy(conf);
    if (isEnabled()) {
      this.httpAddress = getHttpBindAddress();
      this.httpsAddress = getHttpsBindAddress();
      HttpServer2.Builder builder = null;
      builder = DFSUtil.httpServerTemplateForNNAndJN(conf, this.httpAddress,
          this.httpsAddress, name, getSpnegoPrincipal(), getKeytabFile());

      final boolean xFrameEnabled = conf.getBoolean(
          DFSConfigKeys.DFS_XFRAME_OPTION_ENABLED,
          DFSConfigKeys.DFS_XFRAME_OPTION_ENABLED_DEFAULT);

      final String xFrameOptionValue = conf.getTrimmed(
          DFSConfigKeys.DFS_XFRAME_OPTION_VALUE,
          DFSConfigKeys.DFS_XFRAME_OPTION_VALUE_DEFAULT);

      builder.configureXFrame(xFrameEnabled).setXFrameOption(xFrameOptionValue);

      httpServer = builder.build();
      httpServer.addServlet("conf", "/conf", HddsConfServlet.class);

      prometheusSupport =
          conf.getBoolean(HddsConfigKeys.HDDS_PROMETHEUS_ENABLED, false);

      profilerSupport =
          conf.getBoolean(HddsConfigKeys.HDDS_PROFILER_ENABLED, false);

      if (prometheusSupport) {
        prometheusMetricsSink = new PrometheusMetricsSink();
        httpServer.getWebAppContext().getServletContext()
            .setAttribute(PROMETHEUS_SINK, prometheusMetricsSink);
        httpServer.addServlet("prometheus", "/prom", PrometheusServlet.class);
      }

      if (profilerSupport) {
        LOG.warn(
            "/prof java profiling servlet is activated. Not safe for "
                + "production!");
        httpServer.addServlet("profile", "/prof", ProfileServlet.class);
      }
    }

  }

  /**
   * Add a servlet to BaseHttpServer.
   *
   * @param servletName The name of the servlet
   * @param pathSpec    The path spec for the servlet
   * @param clazz       The servlet class
   */
  protected void addServlet(String servletName, String pathSpec,
      Class<? extends HttpServlet> clazz) {
    httpServer.addServlet(servletName, pathSpec, clazz);
  }

  /**
   * Returns the WebAppContext associated with this HttpServer.
   *
   * @return WebAppContext
   */
  protected WebAppContext getWebAppContext() {
    return httpServer.getWebAppContext();
  }

  protected InetSocketAddress getBindAddress(String bindHostKey,
      String addressKey, String bindHostDefault, int bindPortdefault) {
    final Optional<String> bindHost =
        getHostNameFromConfigKeys(conf, bindHostKey);

    final Optional<Integer> addressPort =
        getPortNumberFromConfigKeys(conf, addressKey);

    final Optional<String> addressHost =
        getHostNameFromConfigKeys(conf, addressKey);

    String hostName = bindHost.orElse(addressHost.orElse(bindHostDefault));

    return NetUtils.createSocketAddr(
        hostName + ":" + addressPort.orElse(bindPortdefault));
  }

  /**
   * Retrieve the socket address that should be used by clients to connect
   * to the  HTTPS web interface.
   *
   * @return Target InetSocketAddress for the Ozone HTTPS endpoint.
   */
  public InetSocketAddress getHttpsBindAddress() {
    return getBindAddress(getHttpsBindHostKey(), getHttpsAddressKey(),
        getBindHostDefault(), getHttpsBindPortDefault());
  }

  /**
   * Retrieve the socket address that should be used by clients to connect
   * to the  HTTP web interface.
   * <p>
   * * @return Target InetSocketAddress for the Ozone HTTP endpoint.
   */
  public InetSocketAddress getHttpBindAddress() {
    return getBindAddress(getHttpBindHostKey(), getHttpAddressKey(),
        getBindHostDefault(), getHttpBindPortDefault());

  }

  public void start() throws IOException {
    if (httpServer != null && isEnabled()) {
      httpServer.start();
      if (prometheusSupport) {
        DefaultMetricsSystem.instance()
            .register("prometheus", "Hadoop metrics prometheus exporter",
                prometheusMetricsSink);
      }
      updateConnectorAddress();
    }

  }

  private boolean isEnabled() {
    return conf.getBoolean(getEnabledKey(), true);
  }

  public void stop() throws Exception {
    if (httpServer != null) {
      httpServer.stop();
    }
  }

  /**
   * Update the configured listen address based on the real port
   * <p>
   * (eg. replace :0 with real port)
   */
  public void updateConnectorAddress() {
    int connIdx = 0;
    if (policy.isHttpEnabled()) {
      httpAddress = httpServer.getConnectorAddress(connIdx++);
      String realAddress = NetUtils.getHostPortString(httpAddress);
      conf.set(getHttpAddressKey(), realAddress);
      LOG.info(
          String.format("HTTP server of %s is listening at http://%s",
              name.toUpperCase(), realAddress));
    }

    if (policy.isHttpsEnabled()) {
      httpsAddress = httpServer.getConnectorAddress(connIdx);
      String realAddress = NetUtils.getHostPortString(httpsAddress);
      conf.set(getHttpsAddressKey(), realAddress);
      LOG.info(
          String.format("HTTP server of %s is listening at https://%s",
              name.toUpperCase(), realAddress));
    }
  }

  public InetSocketAddress getHttpAddress() {
    return httpAddress;
  }

  public InetSocketAddress getHttpsAddress() {
    return httpsAddress;
  }

  protected abstract String getHttpAddressKey();

  protected abstract String getHttpsAddressKey();

  protected abstract String getHttpBindHostKey();

  protected abstract String getHttpsBindHostKey();

  protected abstract String getBindHostDefault();

  protected abstract int getHttpBindPortDefault();

  protected abstract int getHttpsBindPortDefault();

  protected abstract String getKeytabFile();

  protected abstract String getSpnegoPrincipal();

  protected abstract String getEnabledKey();

}
