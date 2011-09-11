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
package org.apache.hadoop.hdfs.server.namenode;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.security.PrivilegedExceptionAction;

import javax.servlet.ServletContext;

import org.apache.commons.logging.Log;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.server.common.JspHelper;
import org.apache.hadoop.hdfs.server.namenode.web.resources.NamenodeWebHdfsMethods;
import org.apache.hadoop.hdfs.web.WebHdfsFileSystem;
import org.apache.hadoop.hdfs.web.resources.Param;
import org.apache.hadoop.http.HttpServer;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.AccessControlList;

/**
 * Encapsulates the HTTP server started by the NameNode. 
 */
@InterfaceAudience.Private
public class NameNodeHttpServer {
  private HttpServer httpServer;
  private final Configuration conf;
  private final NameNode nn;
  
  private final Log LOG = NameNode.LOG;
  private InetSocketAddress httpAddress;
  
  private InetSocketAddress bindAddress;
  
  
  public static final String NAMENODE_ADDRESS_ATTRIBUTE_KEY = "name.node.address";
  public static final String FSIMAGE_ATTRIBUTE_KEY = "name.system.image";
  protected static final String NAMENODE_ATTRIBUTE_KEY = "name.node";
  
  public NameNodeHttpServer(
      Configuration conf,
      NameNode nn,
      InetSocketAddress bindAddress) {
    this.conf = conf;
    this.nn = nn;
    this.bindAddress = bindAddress;
  }
  
  private String getDefaultServerPrincipal() throws IOException {
    return SecurityUtil.getServerPrincipal(
        conf.get(DFSConfigKeys.DFS_NAMENODE_USER_NAME_KEY),
        nn.getNameNodeAddress().getHostName());
  }
  
  public void start() throws IOException {
    final String infoHost = bindAddress.getHostName();
    
    if(UserGroupInformation.isSecurityEnabled()) {
      String httpsUser = SecurityUtil.getServerPrincipal(conf
          .get(DFSConfigKeys.DFS_NAMENODE_KRB_HTTPS_USER_NAME_KEY), infoHost);
      if (httpsUser == null) {
        LOG.warn(DFSConfigKeys.DFS_NAMENODE_KRB_HTTPS_USER_NAME_KEY
            + " not defined in config. Starting http server as "
            + getDefaultServerPrincipal()
            + ": Kerberized SSL may be not function correctly.");
      } else {
        // Kerberized SSL servers must be run from the host principal...
        LOG.info("Logging in as " + httpsUser + " to start http server.");
        SecurityUtil.login(conf, DFSConfigKeys.DFS_NAMENODE_KEYTAB_FILE_KEY,
            DFSConfigKeys.DFS_NAMENODE_KRB_HTTPS_USER_NAME_KEY, infoHost);
      }
    }

    UserGroupInformation ugi = UserGroupInformation.getLoginUser();
    try {
      this.httpServer = ugi.doAs(new PrivilegedExceptionAction<HttpServer>() {
        @Override
        public HttpServer run() throws IOException, InterruptedException {
          int infoPort = bindAddress.getPort();
          httpServer = new HttpServer("hdfs", infoHost, infoPort,
              infoPort == 0, conf, 
              new AccessControlList(conf.get(DFSConfigKeys.DFS_ADMIN, " ")));

          boolean certSSL = conf.getBoolean("dfs.https.enable", false);
          boolean useKrb = UserGroupInformation.isSecurityEnabled();
          if (certSSL || useKrb) {
            boolean needClientAuth = conf.getBoolean(
                DFSConfigKeys.DFS_CLIENT_HTTPS_NEED_AUTH_KEY,
                DFSConfigKeys.DFS_CLIENT_HTTPS_NEED_AUTH_DEFAULT);
            InetSocketAddress secInfoSocAddr = NetUtils.createSocketAddr(conf
                .get(DFSConfigKeys.DFS_NAMENODE_HTTPS_ADDRESS_KEY,
                    DFSConfigKeys.DFS_NAMENODE_HTTPS_ADDRESS_DEFAULT));
            Configuration sslConf = new HdfsConfiguration(false);
            if (certSSL) {
              sslConf.addResource(conf.get(
                  "dfs.https.server.keystore.resource", "ssl-server.xml"));
            }
            httpServer.addSslListener(secInfoSocAddr, sslConf, needClientAuth,
                useKrb);
            // assume same ssl port for all datanodes
            InetSocketAddress datanodeSslPort = NetUtils.createSocketAddr(conf
                .get("dfs.datanode.https.address", infoHost + ":" + 50475));
            httpServer.setAttribute("datanode.https.port", datanodeSslPort
                .getPort());
          }
          httpServer.setAttribute(NAMENODE_ATTRIBUTE_KEY, nn);
          httpServer.setAttribute(NAMENODE_ADDRESS_ATTRIBUTE_KEY,
              nn.getNameNodeAddress());
          httpServer.setAttribute(FSIMAGE_ATTRIBUTE_KEY, nn.getFSImage());
          httpServer.setAttribute(JspHelper.CURRENT_CONF, conf);
          setupServlets(httpServer);
          httpServer.start();

          // The web-server port can be ephemeral... ensure we have the correct
          // info
          infoPort = httpServer.getPort();
          httpAddress = new InetSocketAddress(infoHost, infoPort);
          LOG.info(nn.getRole() + " Web-server up at: " + httpAddress);
          return httpServer;
        }
      });
    } catch (InterruptedException e) {
      throw new IOException(e);
    } finally {
      if(UserGroupInformation.isSecurityEnabled() && 
          conf.get(DFSConfigKeys.DFS_NAMENODE_KRB_HTTPS_USER_NAME_KEY) != null) {
        // Go back to being the correct Namenode principal
        LOG.info("Logging back in as NameNode user following http server start");
        nn.loginAsNameNodeUser(conf);
      }
    }
  }
  
  public void stop() throws Exception {
    httpServer.stop();
  }

  public InetSocketAddress getHttpAddress() {
    return httpAddress;
  }

  private static void setupServlets(HttpServer httpServer) {
    httpServer.addInternalServlet("getDelegationToken",
        GetDelegationTokenServlet.PATH_SPEC, 
        GetDelegationTokenServlet.class, true);
    httpServer.addInternalServlet("renewDelegationToken", 
        RenewDelegationTokenServlet.PATH_SPEC, 
        RenewDelegationTokenServlet.class, true);
    httpServer.addInternalServlet("cancelDelegationToken", 
        CancelDelegationTokenServlet.PATH_SPEC, 
        CancelDelegationTokenServlet.class, true);
    httpServer.addInternalServlet("fsck", "/fsck", FsckServlet.class,
        true);
    httpServer.addInternalServlet("getimage", "/getimage",
        GetImageServlet.class, true);
    httpServer.addInternalServlet("listPaths", "/listPaths/*",
        ListPathsServlet.class, false);
    httpServer.addInternalServlet("data", "/data/*",
        FileDataServlet.class, false);
    httpServer.addInternalServlet("checksum", "/fileChecksum/*",
        FileChecksumServlets.RedirectServlet.class, false);
    httpServer.addInternalServlet("contentSummary", "/contentSummary/*",
        ContentSummaryServlet.class, false);

    httpServer.addJerseyResourcePackage(
        NamenodeWebHdfsMethods.class.getPackage().getName()
        + ";" + Param.class.getPackage().getName(),
        "/" + WebHdfsFileSystem.PATH_PREFIX + "/*");
  }

  public static FSImage getFsImageFromContext(ServletContext context) {
    return (FSImage)context.getAttribute(FSIMAGE_ATTRIBUTE_KEY);
  }

  public static NameNode getNameNodeFromContext(ServletContext context) {
    return (NameNode)context.getAttribute(NAMENODE_ATTRIBUTE_KEY);
  }

  public static Configuration getConfFromContext(ServletContext context) {
    return (Configuration)context.getAttribute(JspHelper.CURRENT_CONF);
  }

  public static InetSocketAddress getNameNodeAddressFromContext(
      ServletContext context) {
    return (InetSocketAddress)context.getAttribute(
        NAMENODE_ADDRESS_ATTRIBUTE_KEY);
  }
}
