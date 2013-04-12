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

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_ADMIN;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_JOURNALNODE_KEYTAB_FILE_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_JOURNALNODE_INTERNAL_SPNEGO_USER_NAME_KEY;

import java.io.IOException;
import java.net.InetSocketAddress;

import javax.servlet.ServletContext;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.server.common.JspHelper;
import org.apache.hadoop.http.HttpServer;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.authorize.AccessControlList;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.SecurityUtil;

/**
 * Encapsulates the HTTP server started by the Journal Service.
 */
@InterfaceAudience.Private
public class JournalNodeHttpServer {
  public static final Log LOG = LogFactory.getLog(
      JournalNodeHttpServer.class);

  public static final String JN_ATTRIBUTE_KEY = "localjournal";

  private HttpServer httpServer;
  private int infoPort;
  private JournalNode localJournalNode;

  private final Configuration conf;

  JournalNodeHttpServer(Configuration conf, JournalNode jn) {
    this.conf = conf;
    this.localJournalNode = jn;
  }

  void start() throws IOException {
    final InetSocketAddress bindAddr = getAddress(conf);

    // initialize the webserver for uploading/downloading files.
    LOG.info("Starting web server as: "+ SecurityUtil.getServerPrincipal(conf
        .get(DFS_JOURNALNODE_INTERNAL_SPNEGO_USER_NAME_KEY),
        bindAddr.getHostName()));

    int tmpInfoPort = bindAddr.getPort();
    httpServer = new HttpServer("journal", bindAddr.getHostName(),
        tmpInfoPort, tmpInfoPort == 0, conf, new AccessControlList(conf
            .get(DFS_ADMIN, " "))) {
      {
        if (UserGroupInformation.isSecurityEnabled()) {
          initSpnego(conf, DFS_JOURNALNODE_INTERNAL_SPNEGO_USER_NAME_KEY,
              DFS_JOURNALNODE_KEYTAB_FILE_KEY);
        }
      }
    };
    httpServer.setAttribute(JN_ATTRIBUTE_KEY, localJournalNode);
    httpServer.setAttribute(JspHelper.CURRENT_CONF, conf);
    httpServer.addInternalServlet("getJournal", "/getJournal",
        GetJournalEditServlet.class, true);
    httpServer.start();

    // The web-server port can be ephemeral... ensure we have the correct info
    infoPort = httpServer.getPort();

    LOG.info("Journal Web-server up at: " + bindAddr + ":" + infoPort);
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
   * Return the actual address bound to by the running server.
   */
  public InetSocketAddress getAddress() {
    InetSocketAddress addr = httpServer.getListenerAddress();
    assert addr.getPort() != 0;
    return addr;
  }

  private static InetSocketAddress getAddress(Configuration conf) {
    String addr = conf.get(DFSConfigKeys.DFS_JOURNALNODE_HTTP_ADDRESS_KEY,
        DFSConfigKeys.DFS_JOURNALNODE_HTTP_ADDRESS_DEFAULT);
    return NetUtils.createSocketAddr(addr,
        DFSConfigKeys.DFS_JOURNALNODE_HTTP_PORT_DEFAULT,
        DFSConfigKeys.DFS_JOURNALNODE_HTTP_ADDRESS_KEY);
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
