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
package org.apache.hadoop.hdfs.server.federation.router;

import java.net.InetSocketAddress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.server.common.JspHelper;
import org.apache.hadoop.hdfs.server.namenode.NameNodeHttpServer;
import org.apache.hadoop.http.HttpServer2;
import org.apache.hadoop.service.AbstractService;

import javax.servlet.ServletContext;

/**
 * Web interface for the {@link Router}. It exposes the Web UI and the WebHDFS
 * methods from {@link RouterWebHdfsMethods}.
 */
public class RouterHttpServer extends AbstractService {

  protected static final String NAMENODE_ATTRIBUTE_KEY = "name.node";


  /** Configuration for the Router HTTP server. */
  private Configuration conf;

  /** Router using this HTTP server. */
  private final Router router;

  /** HTTP server. */
  private HttpServer2 httpServer;

  /** HTTP addresses. */
  private InetSocketAddress httpAddress;
  private InetSocketAddress httpsAddress;


  public RouterHttpServer(Router router) {
    super(RouterHttpServer.class.getName());
    this.router = router;
  }

  @Override
  protected void serviceInit(Configuration configuration) throws Exception {
    this.conf = configuration;

    // Get HTTP address
    this.httpAddress = conf.getSocketAddr(
        RBFConfigKeys.DFS_ROUTER_HTTP_BIND_HOST_KEY,
        RBFConfigKeys.DFS_ROUTER_HTTP_ADDRESS_KEY,
        RBFConfigKeys.DFS_ROUTER_HTTP_ADDRESS_DEFAULT,
        RBFConfigKeys.DFS_ROUTER_HTTP_PORT_DEFAULT);

    // Get HTTPs address
    this.httpsAddress = conf.getSocketAddr(
        RBFConfigKeys.DFS_ROUTER_HTTPS_BIND_HOST_KEY,
        RBFConfigKeys.DFS_ROUTER_HTTPS_ADDRESS_KEY,
        RBFConfigKeys.DFS_ROUTER_HTTPS_ADDRESS_DEFAULT,
        RBFConfigKeys.DFS_ROUTER_HTTPS_PORT_DEFAULT);

    super.serviceInit(conf);
  }

  @Override
  protected void serviceStart() throws Exception {
    // Build and start server
    String webApp = "router";
    HttpServer2.Builder builder = DFSUtil.httpServerTemplateForNNAndJN(
        this.conf, this.httpAddress, this.httpsAddress, webApp,
        DFSConfigKeys.DFS_NAMENODE_KERBEROS_INTERNAL_SPNEGO_PRINCIPAL_KEY,
        DFSConfigKeys.DFS_NAMENODE_KEYTAB_FILE_KEY);

    this.httpServer = builder.build();

    NameNodeHttpServer.initWebHdfs(conf, httpAddress.getHostName(), httpServer,
        RouterWebHdfsMethods.class.getPackage().getName());

    this.httpServer.setAttribute(NAMENODE_ATTRIBUTE_KEY, this.router);
    this.httpServer.setAttribute(JspHelper.CURRENT_CONF, this.conf);
    setupServlets(this.httpServer, this.conf);

    this.httpServer.start();

    // The server port can be ephemeral... ensure we have the correct info
    InetSocketAddress listenAddress = this.httpServer.getConnectorAddress(0);
    if (listenAddress != null) {
      this.httpAddress = new InetSocketAddress(this.httpAddress.getHostName(),
          listenAddress.getPort());
    }
    super.serviceStart();
  }

  @Override
  protected void serviceStop() throws Exception {
    if(this.httpServer != null) {
      this.httpServer.stop();
    }
    super.serviceStop();
  }

  private static void setupServlets(
      HttpServer2 httpServer, Configuration conf) {
    // TODO Add servlets for FSCK, etc
    httpServer.addInternalServlet(IsRouterActiveServlet.SERVLET_NAME,
        IsRouterActiveServlet.PATH_SPEC,
        IsRouterActiveServlet.class);
  }

  public InetSocketAddress getHttpAddress() {
    return this.httpAddress;
  }

  public InetSocketAddress getHttpsAddress() {
    return this.httpsAddress;
  }

  public static Router getRouterFromContext(ServletContext context) {
    return (Router)context.getAttribute(NAMENODE_ATTRIBUTE_KEY);
  }
}