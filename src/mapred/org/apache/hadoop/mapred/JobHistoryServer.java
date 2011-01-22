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

package org.apache.hadoop.mapred;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.http.HttpServer;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.StringUtils;
import org.mortbay.jetty.webapp.WebAppContext;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URL;
import java.security.PrivilegedExceptionAction;

/******************************************************************
 * {@link JobHistoryServer} is responsible for servicing all job history
 * related requests from client.
 *
 * History Server can be initialized in one of two modes
 *   * Emdedded within {@link JobTracker}
 *   * External daemon, can either be run on the job tracker node or outside
 *
 * Two {@link Configuration} entries in mapred-site.xml govern the functioning
 * of the history server
 *
 * mapred.job.history.server.http.address is address to which history web
 *  server is bound to. If operating in embedded mode, the hostname on
 *  history address has to be same as the job tracker host name
 *
 * mapred.job.history.server.embedded (default is true) will cause job tracker
 *  to init history server, else the server need to be started as a
 *  separate daemon process
 *****************************************************************/
public class JobHistoryServer {
  private static final Log LOG = LogFactory.getLog(JobHistoryServer.class);

  static{
    Configuration.addDefaultResource("mapred-default.xml");
    Configuration.addDefaultResource("mapred-site.xml");
  }

  private static final String JH_USER_NAME =
      "mapreduce.jobhistory.kerberos.principal";
  private static final String JH_KEYTAB_FILE =
      "mapreduce.jobhistory.keytab.file";
  public static final String MAPRED_HISTORY_SERVER_HTTP_ADDRESS =
      "mapreduce.history.server.http.address";
  public static final String MAPRED_HISTORY_SERVER_EMBEDDED =
      "mapreduce.history.server.embedded";

  private HttpServer historyServer;
  private JobConf conf;
  private String historyInfoAddr;
  private WebAppContext context;

  /**
   * Starts job history server as a independent process
   *  * Initializes ACL Manager
   *  * Starts a webapp to service history requests
   *
   * @param conf - Mr Cluster configuration
   * @throws IOException - any exception starting history server
   */
  public JobHistoryServer(JobConf conf) throws IOException {

    if (isEmbedded(conf)) {
      throw new IllegalStateException("History server is configured to run " +
          "within JobTracker. Aborting..");
    }

    historyInfoAddr = getBindAddress(conf);
    login(conf);
    ACLsManager aclsManager = initializeACLsManager(conf);
    historyServer = initializeWebContainer(conf, aclsManager);
    initializeWebServer(conf, aclsManager);
  }

  /**
   * Starts job history server as a embedded server within job tracker
   *  * Starts a webapp to service history requests
   *
   * @param conf - MR Cluster configuration
   * @param aclsManager - ACLs Manager for user authentication
   * @param httpServer - Http Server instance
   * @throws IOException - any exception starting history server
   */
  public JobHistoryServer(JobConf conf,
                          ACLsManager aclsManager,
                          HttpServer httpServer) throws IOException {
    historyInfoAddr = getBindAddress(conf);
    this.historyServer = httpServer;
    initializeWebServer(conf, aclsManager);
  }

  private void login(JobConf conf) throws IOException {
    UserGroupInformation.setConfiguration(conf);
    InetSocketAddress infoSocAddr = NetUtils.createSocketAddr(historyInfoAddr);

    SecurityUtil.login(conf, JH_KEYTAB_FILE, JH_USER_NAME, infoSocAddr.getHostName());
    LOG.info("History server login successful");
  }

  private ACLsManager initializeACLsManager(JobConf conf)
      throws IOException {
    LOG.info("Initializing ACLs Manager");

    Configuration queuesConf = new Configuration(conf);
    QueueManager queueManager = new QueueManager(queuesConf);

    return new ACLsManager(conf,
        new JobACLsManager(conf), queueManager);
  }

  /**
   * Start embedded jetty server to host history servlets/pages
   *  - Push history file system, acl Manager and cluster conf for future
   *    reference by the servlets/pages
   *
   * @param conf - Cluster configuration
   * @param aclsManager - ACLs Manager for validating user request
   * @throws IOException - Any exception while starting web server
   */
  private void initializeWebServer(final JobConf conf,
                                            ACLsManager aclsManager)
      throws IOException {

    this.conf = conf;

    FileSystem fs;
    try {
      fs = aclsManager.getMROwner().
        doAs(new PrivilegedExceptionAction<FileSystem>() {
        public FileSystem run() throws IOException {
          return FileSystem.get(conf);
      }});
    } catch (InterruptedException e) {
      throw new IOException("Operation interrupted", e);
    }

    if (!isEmbedded(conf)) {
      JobHistory.initDone(conf, fs, false);
    }
    final String historyLogDir =
      JobHistory.getCompletedJobHistoryLocation().toString();
    FileSystem historyFS = new Path(historyLogDir).getFileSystem(conf);    

    historyServer.setAttribute("historyLogDir", historyLogDir);
    historyServer.setAttribute("fileSys", historyFS);
    historyServer.setAttribute("jobConf", conf);
    historyServer.setAttribute("aclManager", aclsManager);

    historyServer.addServlet("historyfile", "/historyfile",
        RawHistoryFileServlet.class);
  }

  private HttpServer initializeWebContainer(JobConf conf,
                                      ACLsManager aclsManager)
      throws IOException {
    InetSocketAddress infoSocAddr = NetUtils.createSocketAddr(historyInfoAddr);
    int tmpInfoPort = infoSocAddr.getPort();
    return new HttpServer("history", infoSocAddr.getHostName(),
        tmpInfoPort, tmpInfoPort == 0, conf, aclsManager.getAdminsAcl());
  }

  public void start() throws IOException {
    if (!isEmbedded(conf)) {
      historyServer.start();
    }

    InetSocketAddress infoSocAddr = NetUtils.createSocketAddr(historyInfoAddr);
    conf.set(MAPRED_HISTORY_SERVER_HTTP_ADDRESS, infoSocAddr.getHostName() +
        ":" + historyServer.getPort());
    LOG.info("Started job history server at: " + getAddress(conf));
  }

  public void join() throws InterruptedException {
    historyServer.join();
  }

  /**
   * Shutsdown the history server if already initialized
   * @throws Exception - Any exception during shutdown
   */
  public void shutdown() throws Exception {
    if (historyServer != null && !isEmbedded(conf)) {
      LOG.info("Shutting down history server");
      historyServer.stop();
    }
  }

  /**
   * Start job history server as an independent process
   *
   * @param args - Command line arguments
   */
  public static void main(String[] args) {
    StringUtils.startupShutdownMessage(JobHistoryServer.class, args, LOG);

    try {
      JobHistoryServer server = new JobHistoryServer(new JobConf());
      server.start();
      server.join();
    } catch (Throwable e) {
      LOG.fatal(StringUtils.stringifyException(e));
      System.exit(-1);
    }
  }

  static boolean isEmbedded(JobConf conf) {
    return conf.getBoolean(MAPRED_HISTORY_SERVER_EMBEDDED, true);
  }

  static String getAddress(JobConf conf) {
    return conf.get(MAPRED_HISTORY_SERVER_HTTP_ADDRESS);
  }

  static String getHistoryUrlPrefix(JobConf conf) {
    return (isEmbedded(conf) ? "" : "http://" + getAddress(conf));
  }

  private static String getBindAddress(JobConf conf) {
    return conf.get(MAPRED_HISTORY_SERVER_HTTP_ADDRESS, "localhost:0");
  }
}
