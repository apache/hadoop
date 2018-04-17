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
package org.apache.hadoop.hdfs.qjournal.server;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;

import javax.servlet.ServletContext;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.server.common.JspHelper;
import org.apache.hadoop.http.HttpConfig;
import org.apache.hadoop.http.HttpServer2;
import org.apache.hadoop.net.NetUtils;

/**
 * Encapsulates the HTTP server started by the Journal Service.
 */
@InterfaceAudience.Private
public class JournalNodeHttpServer {
  public static final String JN_ATTRIBUTE_KEY = "localjournal";

  private HttpServer2 httpServer;
  private final JournalNode localJournalNode;

  private InetSocketAddress httpAddress;
  private InetSocketAddress httpsAddress;
  private final InetSocketAddress bindAddress;

  private final Configuration conf;

  JournalNodeHttpServer(Configuration conf, JournalNode jn,
      InetSocketAddress bindAddress) {
    this.conf = conf;
    this.localJournalNode = jn;
    this.bindAddress = bindAddress;
  }

  void start() throws IOException {
    final InetSocketAddress httpAddr = bindAddress;

    final String httpsAddrString = conf.get(
        DFSConfigKeys.DFS_JOURNALNODE_HTTPS_ADDRESS_KEY,
        DFSConfigKeys.DFS_JOURNALNODE_HTTPS_ADDRESS_DEFAULT);
    InetSocketAddress httpsAddr = NetUtils.createSocketAddr(httpsAddrString);

    if (httpsAddr != null) {
      // If DFS_JOURNALNODE_HTTPS_BIND_HOST_KEY exists then it overrides the
      // host name portion of DFS_NAMENODE_HTTPS_ADDRESS_KEY.
      final String bindHost =
          conf.getTrimmed(DFSConfigKeys.DFS_JOURNALNODE_HTTPS_BIND_HOST_KEY);
      if (bindHost != null && !bindHost.isEmpty()) {
        httpsAddr = new InetSocketAddress(bindHost, httpsAddr.getPort());
      }
    }

    HttpServer2.Builder builder = DFSUtil.httpServerTemplateForNNAndJN(conf,
        httpAddr, httpsAddr, "journal",
        DFSConfigKeys.DFS_JOURNALNODE_KERBEROS_INTERNAL_SPNEGO_PRINCIPAL_KEY,
        DFSConfigKeys.DFS_JOURNALNODE_KEYTAB_FILE_KEY);

    httpServer = builder.build();
    httpServer.setAttribute(JN_ATTRIBUTE_KEY, localJournalNode);
    httpServer.setAttribute(JspHelper.CURRENT_CONF, conf);
    httpServer.addInternalServlet("getJournal", "/getJournal",
        GetJournalEditServlet.class, true);
    httpServer.start();

    HttpConfig.Policy policy = DFSUtil.getHttpPolicy(conf);
    int connIdx = 0;
    if (policy.isHttpEnabled()) {
      httpAddress = httpServer.getConnectorAddress(connIdx++);
      conf.set(DFSConfigKeys.DFS_JOURNALNODE_HTTP_ADDRESS_KEY,
          NetUtils.getHostPortString(httpAddress));
    }

    if (policy.isHttpsEnabled()) {
      httpsAddress = httpServer.getConnectorAddress(connIdx);
      conf.set(DFSConfigKeys.DFS_JOURNALNODE_HTTPS_ADDRESS_KEY,
          NetUtils.getHostPortString(httpsAddress));
    }
  }

  void stop() throws IOException {
    if (httpServer != null) {
      try {
        httpServer.stop();
      } catch (Exception e) {
        throw new IOException(e);
      }
    }
  }

  /**
   * Return the actual HTTP/HTTPS address bound to by the running server.
   */
  public InetSocketAddress getAddress() {
    assert httpAddress != null || httpsAddress != null;
    return httpAddress != null ? httpAddress : httpsAddress;
  }
  
  /**
   * Return the actual address bound to by the running server.
   */
  public InetSocketAddress getHttpAddress() {
    return httpAddress;
  }

  /**
   * Return the actual address bound to by the running server.
   */
  public InetSocketAddress getHttpsAddress() {
    return httpsAddress;
  }

  /**
   * Return the URI that locates the HTTP server.
   */
  URI getServerURI() {
    // getHttpClientScheme() only returns https for HTTPS_ONLY policy. This
    // matches the behavior that the first connector is a HTTPS connector only
    // for HTTPS_ONLY policy.
    InetSocketAddress addr = httpServer.getConnectorAddress(0);
    return URI.create(DFSUtil.getHttpClientScheme(conf) + "://"
        + NetUtils.getHostPortString(addr));
  }

  public static Journal getJournalFromContext(ServletContext context, String jid)
      throws IOException {
    JournalNode jn = (JournalNode)context.getAttribute(JN_ATTRIBUTE_KEY);
    return jn.getOrCreateJournal(jid);
  }

  public static Configuration getConfFromContext(ServletContext context) {
    return (Configuration) context.getAttribute(JspHelper.CURRENT_CONF);
  }
}
