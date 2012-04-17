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
package org.apache.hadoop.hdfs.server.journalservice;

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_ADMIN;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_JOURNAL_HTTPS_PORT_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_JOURNAL_HTTPS_PORT_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_JOURNAL_KEYTAB_FILE_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_JOURNAL_KRB_HTTPS_USER_NAME_KEY;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.security.PrivilegedExceptionAction;

import javax.servlet.ServletContext;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.server.common.JspHelper;
import org.apache.hadoop.hdfs.server.namenode.NNStorage;

import org.apache.hadoop.http.HttpServer;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.AccessControlList;

/**
 * Encapsulates the HTTP server started by the Journal Service.
 */
@InterfaceAudience.Private
public class JournalHttpServer {

  private HttpServer httpServer;
  private InetSocketAddress httpAddress;
  private String infoBindAddress;
  private int infoPort;
  private int imagePort;

  private final Configuration conf;

  public static final Log LOG = LogFactory.getLog(JournalHttpServer.class
      .getName());

  public static final String NNSTORAGE_ATTRIBUTE_KEY = "name.system.storage";
  protected static final String NAMENODE_ATTRIBUTE_KEY = "name.node";

  private NNStorage storage = null;

  public JournalHttpServer(Configuration conf, NNStorage storage,
      InetSocketAddress bindAddress) throws Exception {
    this.conf = conf;
    this.storage = storage;
    this.httpAddress = bindAddress;
  }

  public void start() throws IOException {
    infoBindAddress = httpAddress.getHostName();

    // initialize the webserver for uploading files.
    // Kerberized SSL servers must be run from the host principal...
    UserGroupInformation httpUGI = UserGroupInformation
        .loginUserFromKeytabAndReturnUGI(SecurityUtil.getServerPrincipal(
            conf.get(DFS_JOURNAL_KRB_HTTPS_USER_NAME_KEY),
            infoBindAddress), conf.get(DFS_JOURNAL_KEYTAB_FILE_KEY));
    try {
      httpServer = httpUGI.doAs(new PrivilegedExceptionAction<HttpServer>() {
        @Override
        public HttpServer run() throws IOException, InterruptedException {
          LOG.info("Starting web server as: "
              + UserGroupInformation.getCurrentUser().getUserName());

          int tmpInfoPort = httpAddress.getPort();
          httpServer = new HttpServer("journal", infoBindAddress,
              tmpInfoPort, tmpInfoPort == 0, conf, new AccessControlList(conf
                  .get(DFS_ADMIN, " ")));

          if (UserGroupInformation.isSecurityEnabled()) {
            SecurityUtil.initKrb5CipherSuites();
            InetSocketAddress secInfoSocAddr = NetUtils
                .createSocketAddr(infoBindAddress
                    + ":"
                    + conf.getInt(DFS_JOURNAL_HTTPS_PORT_KEY,
                        DFS_JOURNAL_HTTPS_PORT_DEFAULT));
            imagePort = secInfoSocAddr.getPort();
            httpServer.addSslListener(secInfoSocAddr, conf, false, true);
          }
          httpServer.setAttribute("journal.node", JournalHttpServer.this);
          httpServer.setAttribute("name.system.storage", storage);
          httpServer.setAttribute(JspHelper.CURRENT_CONF, conf);
          httpServer.start();
          return httpServer;
        }
      });
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }

    LOG.info("Journal web server init done");
    // The web-server port can be ephemeral... ensure we have the correct info
    infoPort = httpServer.getPort();
    if (!UserGroupInformation.isSecurityEnabled()) {
      imagePort = infoPort;
    }

    LOG.info("Journal Web-server up at: " + infoBindAddress + ":"
        + infoPort);
    LOG.info("Journal image servlet up at: " + infoBindAddress + ":"
        + imagePort);
  }

  public void stop() throws Exception {
    if (httpServer != null) {
      httpServer.stop();
    }
  }

  public static NNStorage getNNStorageFromContext(ServletContext context) {
    return (NNStorage) context.getAttribute(NNSTORAGE_ATTRIBUTE_KEY);
  }

  public static Configuration getConfFromContext(ServletContext context) {
    return (Configuration) context.getAttribute(JspHelper.CURRENT_CONF);
  }
}
