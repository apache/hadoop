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
import org.apache.hadoop.hdfs.server.namenode.TransferFsImage;
import org.apache.hadoop.hdfs.server.protocol.RemoteEditLog;
import org.apache.hadoop.hdfs.server.protocol.RemoteEditLogManifest;
import org.apache.hadoop.http.HttpServer;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.AccessControlList;

/**
 * Encapsulates the HTTP server started by the Journal Service.
 */
@InterfaceAudience.Private
public class JournalHttpServer {
  public static final Log LOG = LogFactory.getLog(JournalHttpServer.class);

  public static final String JOURNAL_ATTRIBUTE_KEY = "localjournal";

  private HttpServer httpServer;
  private InetSocketAddress httpAddress;
  private String infoBindAddress;
  private int infoPort;
  private int httpsPort;
  private Journal localJournal;

  private final Configuration conf;

  JournalHttpServer(Configuration conf, Journal journal,
      InetSocketAddress bindAddress) {
    this.conf = conf;
    this.localJournal = journal;
    this.httpAddress = bindAddress;
  }

  void start() throws IOException {
    infoBindAddress = httpAddress.getHostName();

    // initialize the webserver for uploading/downloading files.
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
            // TODO: implementation 
          }
          httpServer.setAttribute(JOURNAL_ATTRIBUTE_KEY, localJournal);
          httpServer.setAttribute(JspHelper.CURRENT_CONF, conf);
          // use "/getimage" because GetJournalEditServlet uses some
          // GetImageServlet methods.
          // TODO: change getimage to getedit
          httpServer.addInternalServlet("getimage", "/getimage",
              GetJournalEditServlet.class, true);
          httpServer.start();
          return httpServer;
        }
      });
    } catch (InterruptedException e) {
      throw new IOException(e);
    }

    // The web-server port can be ephemeral... ensure we have the correct info
    infoPort = httpServer.getPort();
    if (!UserGroupInformation.isSecurityEnabled()) {
      httpsPort = infoPort;
    }

    LOG.info("Journal Web-server up at: " + infoBindAddress + ":" + infoPort
        + " and https port is: " + httpsPort);
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

  InetSocketAddress getHttpAddress() {
    return httpAddress;
  }
  
  public static Journal getJournalFromContext(ServletContext context) {
    return (Journal) context.getAttribute(JOURNAL_ATTRIBUTE_KEY);
  }

  public static Configuration getConfFromContext(ServletContext context) {
    return (Configuration) context.getAttribute(JspHelper.CURRENT_CONF);
  }

  /**
   * Download <code>edits</code> files from another journal service
   * 
   * @return true if a new image has been downloaded and needs to be loaded
   * @throws IOException
   */
  boolean downloadEditFiles(final String jnHostPort,
      final RemoteEditLogManifest manifest) throws IOException {

    // Sanity check manifest
    if (manifest.getLogs().isEmpty()) {
      throw new IOException("Found no edit logs to download");
    }

    try {
      Boolean b = UserGroupInformation.getCurrentUser().doAs(
          new PrivilegedExceptionAction<Boolean>() {

            @Override
            public Boolean run() throws Exception {
              // get edits file
              for (RemoteEditLog log : manifest.getLogs()) {
                TransferFsImage.downloadEditsToStorage(jnHostPort, log,
                    localJournal.getStorage());
              }
              return true;
            }
          });
      return b.booleanValue();
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }
}
