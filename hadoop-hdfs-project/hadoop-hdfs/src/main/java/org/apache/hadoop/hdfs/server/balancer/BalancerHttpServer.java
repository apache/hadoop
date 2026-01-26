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
package org.apache.hadoop.hdfs.server.balancer;

import java.io.IOException;
import java.net.InetSocketAddress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.server.common.JspHelper;
import org.apache.hadoop.http.HttpConfig;
import org.apache.hadoop.http.HttpServer2;
import org.apache.hadoop.net.NetUtils;

public class BalancerHttpServer {

  private static final String BALANCER_ATTRIBUTE_KEY = "current.balancer";

  private final Configuration conf;
  private InetSocketAddress httpAddress;
  private InetSocketAddress httpsAddress;
  private HttpServer2 httpServer;

  public BalancerHttpServer(Configuration conf) {
    this.conf = conf;
  }

  public void start() throws IOException {
    String webApp = "balancer";
    // Get HTTP address
    httpAddress = conf.getSocketAddr(DFSConfigKeys.DFS_BALANCER_HTTP_BIND_HOST_KEY,
        DFSConfigKeys.DFS_BALANCER_HTTP_ADDRESS_KEY,
        DFSConfigKeys.DFS_BALANCER_HTTP_ADDRESS_DEFAULT,
        DFSConfigKeys.DFS_BALANCER_HTTP_PORT_DEFAULT);

    // Get HTTPs address
    httpsAddress = conf.getSocketAddr(DFSConfigKeys.DFS_BALANCER_HTTPS_BIND_HOST_KEY,
        DFSConfigKeys.DFS_BALANCER_HTTPS_ADDRESS_KEY,
        DFSConfigKeys.DFS_BALANCER_HTTPS_ADDRESS_DEFAULT,
        DFSConfigKeys.DFS_BALANCER_HTTPS_PORT_DEFAULT);

    HttpServer2.Builder builder =
        DFSUtil.getHttpServerTemplate(conf, httpAddress, httpsAddress, webApp,
            DFSConfigKeys.DFS_BALANCER_KERBEROS_INTERNAL_SPNEGO_PRINCIPAL_KEY,
            DFSConfigKeys.DFS_BALANCER_KEYTAB_FILE_KEY);

    final boolean xFrameEnabled = conf.getBoolean(DFSConfigKeys.DFS_XFRAME_OPTION_ENABLED,
        DFSConfigKeys.DFS_XFRAME_OPTION_ENABLED_DEFAULT);

    final String xFrameOptionValue = conf.getTrimmed(DFSConfigKeys.DFS_XFRAME_OPTION_VALUE,
        DFSConfigKeys.DFS_XFRAME_OPTION_VALUE_DEFAULT);

    builder.configureXFrame(xFrameEnabled).setXFrameOption(xFrameOptionValue);

    httpServer = builder.build();
    httpServer.setAttribute(JspHelper.CURRENT_CONF, conf);
    httpServer.start();

    HttpConfig.Policy policy = DFSUtil.getHttpPolicy(conf);
    int connIdx = 0;
    if (policy.isHttpEnabled()) {
      httpAddress = httpServer.getConnectorAddress(connIdx++);
      if (httpAddress != null) {
        conf.set(DFSConfigKeys.DFS_BALANCER_HTTP_ADDRESS_KEY,
            NetUtils.getHostPortString(httpAddress));
      }
    }
    if (policy.isHttpsEnabled()) {
      httpsAddress = httpServer.getConnectorAddress(connIdx);
      if (httpsAddress != null) {
        conf.set(DFSConfigKeys.DFS_BALANCER_HTTPS_ADDRESS_KEY,
            NetUtils.getHostPortString(httpsAddress));
      }
    }
  }

  public void setBalancerAttribute(Balancer balancer) {
    httpServer.setAttribute(BALANCER_ATTRIBUTE_KEY, balancer);
  }

  public void stop() throws IOException {
    if (httpServer != null) {
      try {
        httpServer.stop();
      } catch (Exception e) {
        throw new IOException(e);
      }
    }
  }

  public InetSocketAddress getHttpAddress() {
    return httpAddress;
  }

  public InetSocketAddress getHttpsAddress() {
    return httpsAddress;
  }
}
