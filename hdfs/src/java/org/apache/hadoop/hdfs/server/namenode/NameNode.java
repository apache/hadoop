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

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.security.PrivilegedExceptionAction;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.FsServerDefaults;
import org.apache.hadoop.fs.Options;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.Trash;
import org.apache.hadoop.fs.UnresolvedLinkException;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.HDFSPolicyProvider;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.BlockListAsLongs;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.protocol.CorruptFileBlocks;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.DirectoryListing;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.FSConstants;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.protocol.UnregisteredNodeException;
import org.apache.hadoop.hdfs.protocol.UnresolvedPathException;
import org.apache.hadoop.hdfs.security.token.block.ExportedBlockKeys;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.hdfs.server.common.HdfsConstants.NamenodeRole;
import org.apache.hadoop.hdfs.server.common.HdfsConstants.StartupOption;
import org.apache.hadoop.hdfs.server.common.IncorrectVersionException;
import org.apache.hadoop.hdfs.server.common.JspHelper;
import org.apache.hadoop.hdfs.server.common.UpgradeStatusReport;
import org.apache.hadoop.hdfs.server.namenode.metrics.NameNodeMetrics;
import org.apache.hadoop.hdfs.server.protocol.BlocksWithLocations;
import org.apache.hadoop.hdfs.server.protocol.DatanodeCommand;
import org.apache.hadoop.hdfs.server.protocol.DatanodeProtocol;
import org.apache.hadoop.hdfs.server.protocol.DatanodeRegistration;
import org.apache.hadoop.hdfs.server.protocol.NamenodeCommand;
import org.apache.hadoop.hdfs.server.protocol.NamenodeProtocol;
import org.apache.hadoop.hdfs.server.protocol.NamenodeProtocols;
import org.apache.hadoop.hdfs.server.protocol.NamenodeRegistration;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo;
import org.apache.hadoop.hdfs.server.protocol.NodeRegistration;
import org.apache.hadoop.hdfs.server.protocol.RemoteEditLogManifest;
import org.apache.hadoop.hdfs.server.protocol.UpgradeCommand;
import org.apache.hadoop.http.HttpServer;
import org.apache.hadoop.io.EnumSetWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.ipc.ProtocolSignature;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.net.Node;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.Groups;
import org.apache.hadoop.security.RefreshUserMappingsProtocol;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.AccessControlList;
import org.apache.hadoop.security.authorize.AuthorizationException;
import org.apache.hadoop.security.authorize.ProxyUsers;
import org.apache.hadoop.security.authorize.RefreshAuthorizationPolicyProtocol;
import org.apache.hadoop.security.token.SecretManager.InvalidToken;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.tools.GetUserMappingsProtocol;
import org.apache.hadoop.util.ServicePlugin;
import org.apache.hadoop.util.StringUtils;

/**********************************************************
 * NameNode serves as both directory namespace manager and
 * "inode table" for the Hadoop DFS.  There is a single NameNode
 * running in any DFS deployment.  (Well, except when there
 * is a second backup/failover NameNode, or when using federated NameNodes.)
 *
 * The NameNode controls two critical tables:
 *   1)  filename->blocksequence (namespace)
 *   2)  block->machinelist ("inodes")
 *
 * The first table is stored on disk and is very precious.
 * The second table is rebuilt every time the NameNode comes up.
 *
 * 'NameNode' refers to both this class as well as the 'NameNode server'.
 * The 'FSNamesystem' class actually performs most of the filesystem
 * management.  The majority of the 'NameNode' class itself is concerned
 * with exposing the IPC interface and the HTTP server to the outside world,
 * plus some configuration management.
 *
 * NameNode implements the
 * {@link org.apache.hadoop.hdfs.protocol.ClientProtocol} interface, which
 * allows clients to ask for DFS services.
 * {@link org.apache.hadoop.hdfs.protocol.ClientProtocol} is not designed for
 * direct use by authors of DFS client code.  End-users should instead use the
 * {@link org.apache.hadoop.fs.FileSystem} class.
 *
 * NameNode also implements the
 * {@link org.apache.hadoop.hdfs.server.protocol.DatanodeProtocol} interface,
 * used by DataNodes that actually store DFS data blocks.  These
 * methods are invoked repeatedly and automatically by all the
 * DataNodes in a DFS deployment.
 *
 * NameNode also implements the
 * {@link org.apache.hadoop.hdfs.server.protocol.NamenodeProtocol} interface,
 * used by secondary namenodes or rebalancing processes to get partial
 * NameNode state, for example partial blocksMap etc.
 **********************************************************/
@InterfaceAudience.Private
public class NameNode implements NamenodeProtocols, FSConstants {
  static{
    HdfsConfiguration.init();
  }
  
  /**
   * HDFS federation configuration can have two types of parameters:
   * <ol>
   * <li>Parameter that is common for all the name services in the cluster.</li>
   * <li>Parameters that are specific to a name service. This keys are suffixed
   * with nameserviceId in the configuration. For example,
   * "dfs.namenode.rpc-address.nameservice1".</li>
   * </ol>
   * 
   * Following are nameservice specific keys.
   */
  public static final String[] NAMESERVICE_SPECIFIC_KEYS = {
    DFSConfigKeys.DFS_NAMENODE_RPC_ADDRESS_KEY,
    DFSConfigKeys.DFS_NAMENODE_NAME_DIR_KEY,
    DFSConfigKeys.DFS_NAMENODE_EDITS_DIR_KEY,
    DFSConfigKeys.DFS_NAMENODE_CHECKPOINT_DIR_KEY,
    DFSConfigKeys.DFS_NAMENODE_CHECKPOINT_EDITS_DIR_KEY,
    DFSConfigKeys.DFS_NAMENODE_SERVICE_RPC_ADDRESS_KEY,
    DFSConfigKeys.DFS_NAMENODE_HTTP_ADDRESS_KEY,
    DFSConfigKeys.DFS_NAMENODE_HTTPS_ADDRESS_KEY,
    DFSConfigKeys.DFS_NAMENODE_KEYTAB_FILE_KEY,
    DFSConfigKeys.DFS_NAMENODE_SECONDARY_HTTP_ADDRESS_KEY,
    DFSConfigKeys.DFS_SECONDARY_NAMENODE_KEYTAB_FILE_KEY,
    DFSConfigKeys.DFS_NAMENODE_BACKUP_ADDRESS_KEY,
    DFSConfigKeys.DFS_NAMENODE_BACKUP_HTTP_ADDRESS_KEY,
    DFSConfigKeys.DFS_NAMENODE_BACKUP_SERVICE_RPC_ADDRESS_KEY
  };
  
  public long getProtocolVersion(String protocol, 
                                 long clientVersion) throws IOException {
    if (protocol.equals(ClientProtocol.class.getName())) {
      return ClientProtocol.versionID; 
    } else if (protocol.equals(DatanodeProtocol.class.getName())){
      return DatanodeProtocol.versionID;
    } else if (protocol.equals(NamenodeProtocol.class.getName())){
      return NamenodeProtocol.versionID;
    } else if (protocol.equals(RefreshAuthorizationPolicyProtocol.class.getName())){
      return RefreshAuthorizationPolicyProtocol.versionID;
    } else if (protocol.equals(RefreshUserMappingsProtocol.class.getName())){
      return RefreshUserMappingsProtocol.versionID;
    } else if (protocol.equals(GetUserMappingsProtocol.class.getName())){
      return GetUserMappingsProtocol.versionID;
    } else {
      throw new IOException("Unknown protocol to name node: " + protocol);
    }
  }
    

  @Override // VersionedProtocol
  public ProtocolSignature getProtocolSignature(String protocol,
      long clientVersion, int clientMethodsHash) throws IOException {
    return ProtocolSignature.getProtocolSignature(
        this, protocol, clientVersion, clientMethodsHash);
  }

  public static final int DEFAULT_PORT = 8020;

  public static final Log LOG = LogFactory.getLog(NameNode.class.getName());
  public static final Log stateChangeLog = LogFactory.getLog("org.apache.hadoop.hdfs.StateChange");
  
  public static final String NAMENODE_ADDRESS_ATTRIBUTE_KEY = "name.node.address";

  protected FSNamesystem namesystem; 
  protected NamenodeRole role;
  /** RPC server. Package-protected for use in tests. */
  Server server;
  /** RPC server for HDFS Services communication.
      BackupNode, Datanodes and all other services
      should be connecting to this server if it is
      configured. Clients should only go to NameNode#server
  */
  protected Server serviceRpcServer;
  /** RPC server address */
  protected InetSocketAddress rpcAddress = null;
  /** RPC server for DN address */
  protected InetSocketAddress serviceRPCAddress = null;
  /** httpServer */
  protected HttpServer httpServer;
  /** HTTP server address */
  protected InetSocketAddress httpAddress = null;
  private Thread emptier;
  /** only used for testing purposes  */
  protected boolean stopRequested = false;
  /** Registration information of this name-node  */
  protected NamenodeRegistration nodeRegistration;
  /** Is service level authorization enabled? */
  private boolean serviceAuthEnabled = false;
  /** Activated plug-ins. */
  private List<ServicePlugin> plugins;
  
  /** Format a new filesystem.  Destroys any filesystem that may already
   * exist at this location.  **/
  public static void format(Configuration conf) throws IOException {
    format(conf, false);
  }

  static NameNodeMetrics metrics;

  /** Return the {@link FSNamesystem} object.
   * @return {@link FSNamesystem} object.
   */
  public FSNamesystem getNamesystem() {
    return namesystem;
  }

  static void initMetrics(Configuration conf, NamenodeRole role) {
    metrics = NameNodeMetrics.create(conf, role);
  }

  public static NameNodeMetrics getNameNodeMetrics() {
    return metrics;
  }
  
  public static InetSocketAddress getAddress(String address) {
    return NetUtils.createSocketAddr(address, DEFAULT_PORT);
  }
  
  /**
   * Set the configuration property for the service rpc address
   * to address
   */
  public static void setServiceAddress(Configuration conf,
                                           String address) {
    LOG.info("Setting ADDRESS " + address);
    conf.set(DFSConfigKeys.DFS_NAMENODE_SERVICE_RPC_ADDRESS_KEY, address);
  }
  
  /**
   * Fetches the address for services to use when connecting to namenode
   * based on the value of fallback returns null if the special
   * address is not specified or returns the default namenode address
   * to be used by both clients and services.
   * Services here are datanodes, backup node, any non client connection
   */
  public static InetSocketAddress getServiceAddress(Configuration conf,
                                                        boolean fallback) {
    String addr = conf.get(DFSConfigKeys.DFS_NAMENODE_SERVICE_RPC_ADDRESS_KEY);
    if (addr == null || addr.isEmpty()) {
      return fallback ? getAddress(conf) : null;
    }
    return getAddress(addr);
  }

  public static InetSocketAddress getAddress(Configuration conf) {
    URI filesystemURI = FileSystem.getDefaultUri(conf);
    return getAddress(filesystemURI);
  }


  /**
   * TODO:FEDERATION
   * @param filesystemURI
   * @return address of file system
   */
  public static InetSocketAddress getAddress(URI filesystemURI) {
    String authority = filesystemURI.getAuthority();
    if (authority == null) {
      throw new IllegalArgumentException(String.format(
          "Invalid URI for NameNode address (check %s): %s has no authority.",
          FileSystem.FS_DEFAULT_NAME_KEY, filesystemURI.toString()));
    }
    if (!FSConstants.HDFS_URI_SCHEME.equalsIgnoreCase(
        filesystemURI.getScheme())) {
      throw new IllegalArgumentException(String.format(
          "Invalid URI for NameNode address (check %s): %s is not of scheme '%s'.",
          FileSystem.FS_DEFAULT_NAME_KEY, filesystemURI.toString(),
          FSConstants.HDFS_URI_SCHEME));
    }
    return getAddress(authority);
  }

  public static URI getUri(InetSocketAddress namenode) {
    int port = namenode.getPort();
    String portString = port == DEFAULT_PORT ? "" : (":"+port);
    return URI.create(FSConstants.HDFS_URI_SCHEME + "://" 
        + namenode.getHostName()+portString);
  }

  /**
   * Compose a "host:port" string from the address.
   */
  public static String getHostPortString(InetSocketAddress addr) {
    return addr.getHostName() + ":" + addr.getPort();
  }

  //
  // Common NameNode methods implementation for the active name-node role.
  //
  public NamenodeRole getRole() {
    return role;
  }

  boolean isRole(NamenodeRole that) {
    return role.equals(that);
  }

  /**
   * Given a configuration get the address of the service rpc server
   * If the service rpc is not configured returns null
   */
  protected InetSocketAddress getServiceRpcServerAddress(Configuration conf)
    throws IOException {
    return NameNode.getServiceAddress(conf, false);
  }

  protected InetSocketAddress getRpcServerAddress(Configuration conf) throws IOException {
    return getAddress(conf);
  }
  
  /**
   * Modifies the configuration passed to contain the service rpc address setting
   */
  protected void setRpcServiceServerAddress(Configuration conf) {
    setServiceAddress(conf, getHostPortString(serviceRPCAddress));
  }

  protected void setRpcServerAddress(Configuration conf) {
    FileSystem.setDefaultUri(conf, getUri(rpcAddress));
  }

  protected InetSocketAddress getHttpServerAddress(Configuration conf) {
    return  NetUtils.createSocketAddr(
        conf.get(DFSConfigKeys.DFS_NAMENODE_HTTP_ADDRESS_KEY, "0.0.0.0:50070"));
  }

  protected void setHttpServerAddress(Configuration conf){
    conf.set(DFSConfigKeys.DFS_NAMENODE_HTTP_ADDRESS_KEY, getHostPortString(httpAddress));
  }

  protected void loadNamesystem(Configuration conf) throws IOException {
    this.namesystem = new FSNamesystem(conf);
  }

  NamenodeRegistration getRegistration() {
    return nodeRegistration;
  }

  NamenodeRegistration setRegistration() {
    nodeRegistration = new NamenodeRegistration(
        getHostPortString(rpcAddress),
        getHostPortString(httpAddress),
        getFSImage().getStorage(), getRole());
    return nodeRegistration;
  }

  /**
   * Initialize name-node.
   * 
   * @param conf the configuration
   */
  protected void initialize(Configuration conf) throws IOException {
    InetSocketAddress socAddr = getRpcServerAddress(conf);
    UserGroupInformation.setConfiguration(conf);
    SecurityUtil.login(conf, DFSConfigKeys.DFS_NAMENODE_KEYTAB_FILE_KEY,
        DFSConfigKeys.DFS_NAMENODE_USER_NAME_KEY, socAddr.getHostName());
    int handlerCount = 
      conf.getInt(DFSConfigKeys.DFS_DATANODE_HANDLER_COUNT_KEY, 
                  DFSConfigKeys.DFS_DATANODE_HANDLER_COUNT_DEFAULT);

    NameNode.initMetrics(conf, this.getRole());
    loadNamesystem(conf);
    // create rpc server
    InetSocketAddress dnSocketAddr = getServiceRpcServerAddress(conf);
    if (dnSocketAddr != null) {
      int serviceHandlerCount =
        conf.getInt(DFSConfigKeys.DFS_NAMENODE_SERVICE_HANDLER_COUNT_KEY,
                    DFSConfigKeys.DFS_NAMENODE_SERVICE_HANDLER_COUNT_DEFAULT);
      this.serviceRpcServer = RPC.getServer(NamenodeProtocols.class, this,
          dnSocketAddr.getHostName(), dnSocketAddr.getPort(), serviceHandlerCount,
          false, conf, namesystem.getDelegationTokenSecretManager());
      this.serviceRPCAddress = this.serviceRpcServer.getListenerAddress();
      setRpcServiceServerAddress(conf);
    }
    this.server = RPC.getServer(NamenodeProtocols.class, this,
                                socAddr.getHostName(), socAddr.getPort(),
                                handlerCount, false, conf, 
                                namesystem.getDelegationTokenSecretManager());

    // set service-level authorization security policy
    if (serviceAuthEnabled =
          conf.getBoolean(
            CommonConfigurationKeys.HADOOP_SECURITY_AUTHORIZATION, false)) {
      this.server.refreshServiceAcl(conf, new HDFSPolicyProvider());
      if (this.serviceRpcServer != null) {
        this.serviceRpcServer.refreshServiceAcl(conf, new HDFSPolicyProvider());
      }
    }

    // The rpc-server port can be ephemeral... ensure we have the correct info
    this.rpcAddress = this.server.getListenerAddress(); 
    setRpcServerAddress(conf);
    
    try {
      validateConfigurationSettings(conf);
    } catch (IOException e) {
      LOG.fatal(e.toString());
      throw e;
    }

    activate(conf);
    LOG.info(getRole() + " up at: " + rpcAddress);
    if (serviceRPCAddress != null) {
      LOG.info(getRole() + " service server is up at: " + serviceRPCAddress); 
    }
  }
  
  /**
   * Verifies that the final Configuration Settings look ok for the NameNode to
   * properly start up
   * Things to check for include:
   * - HTTP Server Port does not equal the RPC Server Port
   * @param conf
   * @throws IOException
   */
  protected void validateConfigurationSettings(final Configuration conf) 
      throws IOException {
    // check to make sure the web port and rpc port do not match 
    if(getHttpServerAddress(conf).getPort() 
        == getRpcServerAddress(conf).getPort()) {
      String errMsg = "dfs.namenode.rpc-address " +
          "("+ getRpcServerAddress(conf) + ") and " +
          "dfs.namenode.http-address ("+ getHttpServerAddress(conf) + ") " +
          "configuration keys are bound to the same port, unable to start " +
          "NameNode. Port: " + getRpcServerAddress(conf).getPort();
      throw new IOException(errMsg);
    } 
  }

  /**
   * Activate name-node servers and threads.
   */
  void activate(Configuration conf) throws IOException {
    if ((isRole(NamenodeRole.NAMENODE))
        && (UserGroupInformation.isSecurityEnabled())) {
      namesystem.activateSecretManager();
    }
    namesystem.activate(conf);
    startHttpServer(conf);
    server.start();  //start RPC server
    if (serviceRpcServer != null) {
      serviceRpcServer.start();      
    }
    startTrashEmptier(conf);
    
    plugins = conf.getInstances("dfs.namenode.plugins", ServicePlugin.class);
    for (ServicePlugin p: plugins) {
      try {
        p.start(this);
      } catch (Throwable t) {
        LOG.warn("ServicePlugin " + p + " could not be started", t);
      }
    }
  }

  private void startTrashEmptier(Configuration conf) throws IOException {
    long trashInterval 
      = conf.getLong(CommonConfigurationKeys.FS_TRASH_INTERVAL_KEY, 
                     CommonConfigurationKeys.FS_TRASH_INTERVAL_DEFAULT);
    if(trashInterval == 0)
      return;
    this.emptier = new Thread(new Trash(conf).getEmptier(), "Trash Emptier");
    this.emptier.setDaemon(true);
    this.emptier.start();
  }
  
  private void startHttpServer(final Configuration conf) throws IOException {
    final InetSocketAddress infoSocAddr = getHttpServerAddress(conf);
    final String infoHost = infoSocAddr.getHostName();
    if(UserGroupInformation.isSecurityEnabled()) {
      String httpsUser = SecurityUtil.getServerPrincipal(conf
          .get(DFSConfigKeys.DFS_NAMENODE_KRB_HTTPS_USER_NAME_KEY), infoHost);
      if (httpsUser == null) {
        LOG.warn(DFSConfigKeys.DFS_NAMENODE_KRB_HTTPS_USER_NAME_KEY
            + " not defined in config. Starting http server as "
            + SecurityUtil.getServerPrincipal(conf
                .get(DFSConfigKeys.DFS_NAMENODE_USER_NAME_KEY), rpcAddress
                .getHostName())
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
          int infoPort = infoSocAddr.getPort();
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
          httpServer.setAttribute("name.node", NameNode.this);
          httpServer.setAttribute(NAMENODE_ADDRESS_ATTRIBUTE_KEY,
              getNameNodeAddress());
          httpServer.setAttribute("name.system.image", getFSImage());
          httpServer.setAttribute(JspHelper.CURRENT_CONF, conf);
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
          httpServer.start();

          // The web-server port can be ephemeral... ensure we have the correct
          // info
          infoPort = httpServer.getPort();
          httpAddress = new InetSocketAddress(infoHost, infoPort);
          setHttpServerAddress(conf);
          LOG.info(getRole() + " Web-server up at: " + httpAddress);
          return httpServer;
        }
      });
    } catch (InterruptedException e) {
      throw new IOException(e);
    } finally {
      if(UserGroupInformation.isSecurityEnabled() && 
          conf.get(DFSConfigKeys.DFS_NAMENODE_KRB_HTTPS_USER_NAME_KEY) != null) {
        // Go back to being the correct Namenode principal
        LOG.info("Logging back in as "
            + SecurityUtil.getServerPrincipal(conf
                .get(DFSConfigKeys.DFS_NAMENODE_USER_NAME_KEY), rpcAddress
                .getHostName()) + " following http server start.");
        SecurityUtil.login(conf, DFSConfigKeys.DFS_NAMENODE_KEYTAB_FILE_KEY,
            DFSConfigKeys.DFS_NAMENODE_USER_NAME_KEY, rpcAddress.getHostName());
      }
    }
  }

  /**
   * Start NameNode.
   * <p>
   * The name-node can be started with one of the following startup options:
   * <ul> 
   * <li>{@link StartupOption#REGULAR REGULAR} - normal name node startup</li>
   * <li>{@link StartupOption#FORMAT FORMAT} - format name node</li>
   * <li>{@link StartupOption#BACKUP BACKUP} - start backup node</li>
   * <li>{@link StartupOption#CHECKPOINT CHECKPOINT} - start checkpoint node</li>
   * <li>{@link StartupOption#UPGRADE UPGRADE} - start the cluster  
   * upgrade and create a snapshot of the current file system state</li> 
   * <li>{@link StartupOption#ROLLBACK ROLLBACK} - roll the  
   *            cluster back to the previous state</li>
   * <li>{@link StartupOption#FINALIZE FINALIZE} - finalize 
   *            previous upgrade</li>
   * <li>{@link StartupOption#IMPORT IMPORT} - import checkpoint</li>
   * </ul>
   * The option is passed via configuration field: 
   * <tt>dfs.namenode.startup</tt>
   * 
   * The conf will be modified to reflect the actual ports on which 
   * the NameNode is up and running if the user passes the port as
   * <code>zero</code> in the conf.
   * 
   * @param conf  confirguration
   * @throws IOException
   */
  public NameNode(Configuration conf) throws IOException {
    this(conf, NamenodeRole.NAMENODE);
  }

  protected NameNode(Configuration conf, NamenodeRole role) 
      throws IOException { 
    this.role = role;
    try {
      initializeGenericKeys(conf);
      initialize(conf);
    } catch (IOException e) {
      this.stop();
      throw e;
    }
  }

  /**
   * Wait for service to finish.
   * (Normally, it runs forever.)
   */
  public void join() {
    try {
      this.server.join();
    } catch (InterruptedException ie) {
    }
  }

  /**
   * Stop all NameNode threads and wait for all to finish.
   */
  public void stop() {
    synchronized(this) {
      if (stopRequested)
        return;
      stopRequested = true;
    }
    if (plugins != null) {
      for (ServicePlugin p : plugins) {
        try {
          p.stop();
        } catch (Throwable t) {
          LOG.warn("ServicePlugin " + p + " could not be stopped", t);
        }
      }
    }
    try {
      if (httpServer != null) httpServer.stop();
    } catch (Exception e) {
      LOG.error("Exception while stopping httpserver", e);
    }
    if(namesystem != null) namesystem.close();
    if(emptier != null) emptier.interrupt();
    if(server != null) server.stop();
    if(serviceRpcServer != null) serviceRpcServer.stop();
    if (metrics != null) {
      metrics.shutdown();
    }
    if (namesystem != null) {
      namesystem.shutdown();
    }
  }

  synchronized boolean isStopRequested() {
    return stopRequested;
  }

  /////////////////////////////////////////////////////
  // NamenodeProtocol
  /////////////////////////////////////////////////////
  @Override // NamenodeProtocol
  public BlocksWithLocations getBlocks(DatanodeInfo datanode, long size)
  throws IOException {
    if(size <= 0) {
      throw new IllegalArgumentException(
        "Unexpected not positive size: "+size);
    }

    return namesystem.getBlocks(datanode, size); 
  }

  @Override // NamenodeProtocol
  public ExportedBlockKeys getBlockKeys() throws IOException {
    return namesystem.getBlockKeys();
  }

  @Override // NamenodeProtocol
  public void errorReport(NamenodeRegistration registration,
                          int errorCode, 
                          String msg) throws IOException {
    verifyRequest(registration);
    LOG.info("Error report from " + registration + ": " + msg);
    if(errorCode == FATAL)
      namesystem.releaseBackupNode(registration);
  }

  @Override // NamenodeProtocol
  public NamenodeRegistration register(NamenodeRegistration registration)
  throws IOException {
    verifyVersion(registration.getVersion());
    NamenodeRegistration myRegistration = setRegistration();
    namesystem.registerBackupNode(registration, myRegistration);
    return myRegistration;
  }

  @Override // NamenodeProtocol
  public NamenodeCommand startCheckpoint(NamenodeRegistration registration)
  throws IOException {
    verifyRequest(registration);
    if(!isRole(NamenodeRole.NAMENODE))
      throw new IOException("Only an ACTIVE node can invoke startCheckpoint.");
    return namesystem.startCheckpoint(registration, setRegistration());
  }

  @Override // NamenodeProtocol
  public void endCheckpoint(NamenodeRegistration registration,
                            CheckpointSignature sig) throws IOException {
    verifyRequest(registration);
    if(!isRole(NamenodeRole.NAMENODE))
      throw new IOException("Only an ACTIVE node can invoke endCheckpoint.");
    namesystem.endCheckpoint(registration, sig);
  }

  @Override // ClientProtocol
  public Token<DelegationTokenIdentifier> getDelegationToken(Text renewer)
      throws IOException {
    return namesystem.getDelegationToken(renewer);
  }

  @Override // ClientProtocol
  public long renewDelegationToken(Token<DelegationTokenIdentifier> token)
      throws InvalidToken, IOException {
    return namesystem.renewDelegationToken(token);
  }

  @Override // ClientProtocol
  public void cancelDelegationToken(Token<DelegationTokenIdentifier> token)
      throws IOException {
    namesystem.cancelDelegationToken(token);
  }
  
  @Override // ClientProtocol
  public LocatedBlocks getBlockLocations(String src, 
                                          long offset, 
                                          long length) 
      throws IOException {
    metrics.incrGetBlockLocations();
    return namesystem.getBlockLocations(getClientMachine(), 
                                        src, offset, length);
  }
  
  @Override // ClientProtocol
  public FsServerDefaults getServerDefaults() throws IOException {
    return namesystem.getServerDefaults();
  }

  @Override // ClientProtocol
  public void create(String src, 
                     FsPermission masked,
                     String clientName, 
                     EnumSetWritable<CreateFlag> flag,
                     boolean createParent,
                     short replication,
                     long blockSize) throws IOException {
    String clientMachine = getClientMachine();
    if (stateChangeLog.isDebugEnabled()) {
      stateChangeLog.debug("*DIR* NameNode.create: file "
                         +src+" for "+clientName+" at "+clientMachine);
    }
    if (!checkPathLength(src)) {
      throw new IOException("create: Pathname too long.  Limit " 
                            + MAX_PATH_LENGTH + " characters, " + MAX_PATH_DEPTH + " levels.");
    }
    namesystem.startFile(src,
        new PermissionStatus(UserGroupInformation.getCurrentUser().getShortUserName(),
            null, masked),
        clientName, clientMachine, flag.get(), createParent, replication, blockSize);
    metrics.incrFilesCreated();
    metrics.incrCreateFileOps();
  }

  @Override // ClientProtocol
  public LocatedBlock append(String src, String clientName) 
      throws IOException {
    String clientMachine = getClientMachine();
    if (stateChangeLog.isDebugEnabled()) {
      stateChangeLog.debug("*DIR* NameNode.append: file "
          +src+" for "+clientName+" at "+clientMachine);
    }
    LocatedBlock info = namesystem.appendFile(src, clientName, clientMachine);
    metrics.incrFilesAppended();
    return info;
  }

  @Override // ClientProtocol
  public boolean recoverLease(String src, String clientName) throws IOException {
    String clientMachine = getClientMachine();
    return namesystem.recoverLease(src, clientName, clientMachine);
  }

  @Override // ClientProtocol
  public boolean setReplication(String src, short replication) 
    throws IOException {  
    return namesystem.setReplication(src, replication);
  }
    
  @Override // ClientProtocol
  public void setPermission(String src, FsPermission permissions)
      throws IOException {
    namesystem.setPermission(src, permissions);
  }

  @Override // ClientProtocol
  public void setOwner(String src, String username, String groupname)
      throws IOException {
    namesystem.setOwner(src, username, groupname);
  }

  @Override // ClientProtocol
  public LocatedBlock addBlock(String src,
                               String clientName,
                               ExtendedBlock previous,
                               DatanodeInfo[] excludedNodes)
      throws IOException {
    if(stateChangeLog.isDebugEnabled()) {
      stateChangeLog.debug("*BLOCK* NameNode.addBlock: file "
          +src+" for "+clientName);
    }
    HashMap<Node, Node> excludedNodesSet = null;
    if (excludedNodes != null) {
      excludedNodesSet = new HashMap<Node, Node>(excludedNodes.length);
      for (Node node:excludedNodes) {
        excludedNodesSet.put(node, node);
      }
    }
    LocatedBlock locatedBlock = 
      namesystem.getAdditionalBlock(src, clientName, previous, excludedNodesSet);
    if (locatedBlock != null)
      metrics.incrAddBlockOps();
    return locatedBlock;
  }

  @Override // ClientProtocol
  public LocatedBlock getAdditionalDatanode(final String src, final ExtendedBlock blk,
      final DatanodeInfo[] existings, final DatanodeInfo[] excludes,
      final int numAdditionalNodes, final String clientName
      ) throws IOException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("getAdditionalDatanode: src=" + src
          + ", blk=" + blk
          + ", existings=" + Arrays.asList(existings)
          + ", excludes=" + Arrays.asList(excludes)
          + ", numAdditionalNodes=" + numAdditionalNodes
          + ", clientName=" + clientName);
    }

    metrics.incrGetAdditionalDatanodeOps();

    HashMap<Node, Node> excludeSet = null;
    if (excludes != null) {
      excludeSet = new HashMap<Node, Node>(excludes.length);
      for (Node node : excludes) {
        excludeSet.put(node, node);
      }
    }
    return namesystem.getAdditionalDatanode(src, blk,
        existings, excludeSet, numAdditionalNodes, clientName);
  }

  /**
   * The client needs to give up on the block.
   */
  public void abandonBlock(ExtendedBlock b, String src, String holder)
      throws IOException {
    if(stateChangeLog.isDebugEnabled()) {
      stateChangeLog.debug("*BLOCK* NameNode.abandonBlock: "
          +b+" of file "+src);
    }
    if (!namesystem.abandonBlock(b, src, holder)) {
      throw new IOException("Cannot abandon block during write to " + src);
    }
  }

  @Override // ClientProtocol
  public boolean complete(String src, String clientName, ExtendedBlock last)
      throws IOException {
    if(stateChangeLog.isDebugEnabled()) {
      stateChangeLog.debug("*DIR* NameNode.complete: "
          + src + " for " + clientName);
    }
    return namesystem.completeFile(src, clientName, last);
  }

  /**
   * The client has detected an error on the specified located blocks 
   * and is reporting them to the server.  For now, the namenode will 
   * mark the block as corrupt.  In the future we might 
   * check the blocks are actually corrupt. 
   */
  @Override
  public void reportBadBlocks(LocatedBlock[] blocks) throws IOException {
    stateChangeLog.info("*DIR* NameNode.reportBadBlocks");
    for (int i = 0; i < blocks.length; i++) {
      ExtendedBlock blk = blocks[i].getBlock();
      DatanodeInfo[] nodes = blocks[i].getLocations();
      for (int j = 0; j < nodes.length; j++) {
        DatanodeInfo dn = nodes[j];
        namesystem.markBlockAsCorrupt(blk, dn);
      }
    }
  }

  @Override // ClientProtocol
  public LocatedBlock updateBlockForPipeline(ExtendedBlock block, String clientName)
      throws IOException {
    return namesystem.updateBlockForPipeline(block, clientName);
  }


  @Override // ClientProtocol
  public void updatePipeline(String clientName, ExtendedBlock oldBlock,
      ExtendedBlock newBlock, DatanodeID[] newNodes)
      throws IOException {
    namesystem.updatePipeline(clientName, oldBlock, newBlock, newNodes);
  }
  
  @Override // DatanodeProtocol
  public void commitBlockSynchronization(ExtendedBlock block,
      long newgenerationstamp, long newlength,
      boolean closeFile, boolean deleteblock, DatanodeID[] newtargets)
      throws IOException {
    namesystem.commitBlockSynchronization(block,
        newgenerationstamp, newlength, closeFile, deleteblock, newtargets);
  }
  
  @Override // ClientProtocol
  public long getPreferredBlockSize(String filename) 
      throws IOException {
    return namesystem.getPreferredBlockSize(filename);
  }
    
  @Deprecated
  @Override // ClientProtocol
  public boolean rename(String src, String dst) throws IOException {
    if(stateChangeLog.isDebugEnabled()) {
      stateChangeLog.debug("*DIR* NameNode.rename: " + src + " to " + dst);
    }
    if (!checkPathLength(dst)) {
      throw new IOException("rename: Pathname too long.  Limit " 
                            + MAX_PATH_LENGTH + " characters, " + MAX_PATH_DEPTH + " levels.");
    }
    boolean ret = namesystem.renameTo(src, dst);
    if (ret) {
      metrics.incrFilesRenamed();
    }
    return ret;
  }
  
  @Override // ClientProtocol
  public void concat(String trg, String[] src) throws IOException {
    namesystem.concat(trg, src);
  }
  
  @Override // ClientProtocol
  public void rename(String src, String dst, Options.Rename... options)
      throws IOException {
    if(stateChangeLog.isDebugEnabled()) {
      stateChangeLog.debug("*DIR* NameNode.rename: " + src + " to " + dst);
    }
    if (!checkPathLength(dst)) {
      throw new IOException("rename: Pathname too long.  Limit " 
                            + MAX_PATH_LENGTH + " characters, " + MAX_PATH_DEPTH + " levels.");
    }
    namesystem.renameTo(src, dst, options);
    metrics.incrFilesRenamed();
  }

  @Deprecated
  @Override // ClientProtocol
  public boolean delete(String src) throws IOException {
    return delete(src, true);
  }

  @Override // ClientProtocol
  public boolean delete(String src, boolean recursive) throws IOException {
    if (stateChangeLog.isDebugEnabled()) {
      stateChangeLog.debug("*DIR* Namenode.delete: src=" + src
          + ", recursive=" + recursive);
    }
    boolean ret = namesystem.delete(src, recursive);
    if (ret) 
      metrics.incrDeleteFileOps();
    return ret;
  }

  /**
   * Check path length does not exceed maximum.  Returns true if
   * length and depth are okay.  Returns false if length is too long 
   * or depth is too great.
   */
  private boolean checkPathLength(String src) {
    Path srcPath = new Path(src);
    return (src.length() <= MAX_PATH_LENGTH &&
            srcPath.depth() <= MAX_PATH_DEPTH);
  }
    
  @Override // ClientProtocol
  public boolean mkdirs(String src, FsPermission masked, boolean createParent)
      throws IOException {
    if(stateChangeLog.isDebugEnabled()) {
      stateChangeLog.debug("*DIR* NameNode.mkdirs: " + src);
    }
    if (!checkPathLength(src)) {
      throw new IOException("mkdirs: Pathname too long.  Limit " 
                            + MAX_PATH_LENGTH + " characters, " + MAX_PATH_DEPTH + " levels.");
    }
    return namesystem.mkdirs(src,
        new PermissionStatus(UserGroupInformation.getCurrentUser().getShortUserName(),
            null, masked), createParent);
  }

  @Override // ClientProtocol
  public void renewLease(String clientName) throws IOException {
    namesystem.renewLease(clientName);        
  }

  @Override // ClientProtocol
  public DirectoryListing getListing(String src, byte[] startAfter,
      boolean needLocation)
  throws IOException {
    DirectoryListing files = namesystem.getListing(
        src, startAfter, needLocation);
    if (files != null) {
      metrics.incrGetListingOps();
      metrics.incrFilesInGetListingOps(files.getPartialListing().length);
    }
    return files;
  }

  @Override // ClientProtocol
  public HdfsFileStatus getFileInfo(String src)  throws IOException {
    metrics.incrFileInfoOps();
    return namesystem.getFileInfo(src, true);
  }

  @Override // ClientProtocol
  public HdfsFileStatus getFileLinkInfo(String src) throws IOException { 
    metrics.incrFileInfoOps();
    return namesystem.getFileInfo(src, false);
  }
  
  @Override
  public long[] getStats() {
    return namesystem.getStats();
  }

  @Override // ClientProtocol
  public DatanodeInfo[] getDatanodeReport(DatanodeReportType type)
      throws IOException {
    DatanodeInfo results[] = namesystem.datanodeReport(type);
    if (results == null ) {
      throw new IOException("Cannot find datanode report");
    }
    return results;
  }
    
  @Override // ClientProtocol
  public boolean setSafeMode(SafeModeAction action) throws IOException {
    return namesystem.setSafeMode(action);
  }

  /**
   * Is the cluster currently in safe mode?
   */
  public boolean isInSafeMode() {
    return namesystem.isInSafeMode();
  }

  @Override // ClientProtocol
  public boolean restoreFailedStorage(String arg) 
      throws AccessControlException {
    return namesystem.restoreFailedStorage(arg);
  }

  @Override // ClientProtocol
  public void saveNamespace() throws IOException {
    namesystem.saveNamespace();
  }

  @Override // ClientProtocol
  public void refreshNodes() throws IOException {
    namesystem.refreshNodes(new HdfsConfiguration());
  }

  @Override // NamenodeProtocol
  public long getTransactionID() {
    return namesystem.getTransactionID();
  }

  @Override // NamenodeProtocol
  public CheckpointSignature rollEditLog() throws IOException {
    return namesystem.rollEditLog();
  }
  
  @Override
  public RemoteEditLogManifest getEditLogManifest(long sinceTxId)
  throws IOException {
    return namesystem.getEditLogManifest(sinceTxId);
  }
    
  @Override // ClientProtocol
  public void finalizeUpgrade() throws IOException {
    namesystem.finalizeUpgrade();
  }

  @Override // ClientProtocol
  public UpgradeStatusReport distributedUpgradeProgress(UpgradeAction action)
      throws IOException {
    return namesystem.distributedUpgradeProgress(action);
  }

  @Override // ClientProtocol
  public void metaSave(String filename) throws IOException {
    namesystem.metaSave(filename);
  }

  @Override // ClientProtocol
  public CorruptFileBlocks listCorruptFileBlocks(String path, String cookie)
      throws IOException {
    Collection<FSNamesystem.CorruptFileBlockInfo> fbs =
      namesystem.listCorruptFileBlocks(path, cookie);
    
    String[] files = new String[fbs.size()];
    String lastCookie = "";
    int i = 0;
    for(FSNamesystem.CorruptFileBlockInfo fb: fbs) {
      files[i++] = fb.path;
      lastCookie = fb.block.getBlockName();
    }
    return new CorruptFileBlocks(files, lastCookie);
  }
  
  @Override // ClientProtocol
  public ContentSummary getContentSummary(String path) throws IOException {
    return namesystem.getContentSummary(path);
  }

  @Override // ClientProtocol
  public void setQuota(String path, long namespaceQuota, long diskspaceQuota) 
      throws IOException {
    namesystem.setQuota(path, namespaceQuota, diskspaceQuota);
  }
  
  @Override // ClientProtocol
  public void fsync(String src, String clientName) throws IOException {
    namesystem.fsync(src, clientName);
  }

  @Override // ClientProtocol
  public void setTimes(String src, long mtime, long atime) 
      throws IOException {
    namesystem.setTimes(src, mtime, atime);
  }

  @Override // ClientProtocol
  public void createSymlink(String target, String link, FsPermission dirPerms,
      boolean createParent) throws IOException {
    metrics.incrCreateSymlinkOps();
    /* We enforce the MAX_PATH_LENGTH limit even though a symlink target 
     * URI may refer to a non-HDFS file system. 
     */
    if (!checkPathLength(link)) {
      throw new IOException("Symlink path exceeds " + MAX_PATH_LENGTH +
                            " character limit");
                            
    }
    if ("".equals(target)) {
      throw new IOException("Invalid symlink target");
    }
    final UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
    namesystem.createSymlink(target, link,
      new PermissionStatus(ugi.getShortUserName(), null, dirPerms), createParent);
  }

  @Override // ClientProtocol
  public String getLinkTarget(String path) throws IOException {
    metrics.incrGetLinkTargetOps();
    /* Resolves the first symlink in the given path, returning a
     * new path consisting of the target of the symlink and any 
     * remaining path components from the original path.
     */
    try {
      HdfsFileStatus stat = namesystem.getFileInfo(path, false);
      if (stat != null) {
        // NB: getSymlink throws IOException if !stat.isSymlink() 
        return stat.getSymlink();
      }
    } catch (UnresolvedPathException e) {
      return e.getResolvedPath().toString();
    } catch (UnresolvedLinkException e) {
      // The NameNode should only throw an UnresolvedPathException
      throw new AssertionError("UnresolvedLinkException thrown");
    }
    return null;
  }


  @Override // DatanodeProtocol
  public DatanodeRegistration registerDatanode(DatanodeRegistration nodeReg)
      throws IOException {
    verifyVersion(nodeReg.getVersion());
    namesystem.registerDatanode(nodeReg);
      
    return nodeReg;
  }

  @Override // DatanodeProtocol
  public DatanodeCommand[] sendHeartbeat(DatanodeRegistration nodeReg,
      long capacity, long dfsUsed, long remaining, long blockPoolUsed,
      int xmitsInProgress, int xceiverCount, int failedVolumes)
      throws IOException {
    verifyRequest(nodeReg);
    return namesystem.handleHeartbeat(nodeReg, capacity, dfsUsed, remaining,
        blockPoolUsed, xceiverCount, xmitsInProgress, failedVolumes);
  }

  @Override // DatanodeProtocol
  public DatanodeCommand blockReport(DatanodeRegistration nodeReg,
      String poolId, long[] blocks) throws IOException {
    verifyRequest(nodeReg);
    BlockListAsLongs blist = new BlockListAsLongs(blocks);
    if(stateChangeLog.isDebugEnabled()) {
      stateChangeLog.debug("*BLOCK* NameNode.blockReport: "
           + "from " + nodeReg.getName() + " " + blist.getNumberOfBlocks()
           + " blocks");
    }

    namesystem.processReport(nodeReg, poolId, blist);
    if (getFSImage().isUpgradeFinalized())
      return new DatanodeCommand.Finalize(poolId);
    return null;
  }

  @Override // DatanodeProtocol
  public void blockReceived(DatanodeRegistration nodeReg, String poolId,
      Block blocks[], String delHints[]) throws IOException {
    verifyRequest(nodeReg);
    if(stateChangeLog.isDebugEnabled()) {
      stateChangeLog.debug("*BLOCK* NameNode.blockReceived: "
          +"from "+nodeReg.getName()+" "+blocks.length+" blocks.");
    }
    for (int i = 0; i < blocks.length; i++) {
      namesystem.blockReceived(nodeReg, poolId, blocks[i], delHints[i]);
    }
  }

  @Override // DatanodeProtocol
  public void errorReport(DatanodeRegistration nodeReg,
                          int errorCode, String msg) throws IOException { 
    String dnName = (nodeReg == null ? "unknown DataNode" : nodeReg.getName());

    if (errorCode == DatanodeProtocol.NOTIFY) {
      LOG.info("Error report from " + dnName + ": " + msg);
      return;
    }
    verifyRequest(nodeReg);

    if (errorCode == DatanodeProtocol.DISK_ERROR) {
      LOG.warn("Disk error on " + dnName + ": " + msg);
    } else if (errorCode == DatanodeProtocol.FATAL_DISK_ERROR) {
      LOG.warn("Fatal disk error on " + dnName + ": " + msg);
      namesystem.removeDatanode(nodeReg);            
    } else {
      LOG.info("Error report from " + dnName + ": " + msg);
    }
  }
    
  @Override // DatanodeProtocol, NamenodeProtocol
  public NamespaceInfo versionRequest() throws IOException {
    return namesystem.getNamespaceInfo();
  }

  @Override // DatanodeProtocol
  public UpgradeCommand processUpgradeCommand(UpgradeCommand comm) throws IOException {
    return namesystem.processDistributedUpgradeCommand(comm);
  }

  /** 
   * Verify request.
   * 
   * Verifies correctness of the datanode version, registration ID, and 
   * if the datanode does not need to be shutdown.
   * 
   * @param nodeReg data node registration
   * @throws IOException
   */
  public void verifyRequest(NodeRegistration nodeReg) throws IOException {
    verifyVersion(nodeReg.getVersion());
    if (!namesystem.getRegistrationID().equals(nodeReg.getRegistrationID())) {
      LOG.warn("Invalid registrationID - expected: "
          + namesystem.getRegistrationID() + " received: "
          + nodeReg.getRegistrationID());
      throw new UnregisteredNodeException(nodeReg);
    }
  }
    
  /**
   * Verify version.
   * 
   * @param version
   * @throws IOException
   */
  public void verifyVersion(int version) throws IOException {
    if (version != LAYOUT_VERSION)
      throw new IncorrectVersionException(version, "data node");
  }
    
  public FSImage getFSImage() {
    return namesystem.dir.fsImage;
  }

  /**
   * Returns the address on which the NameNodes is listening to.
   * @return namenode rpc address
   */
  public InetSocketAddress getNameNodeAddress() {
    return rpcAddress;
  }
  
  /**
   * Returns namenode service rpc address, if set. Otherwise returns
   * namenode rpc address.
   * @return namenode service rpc address used by datanodes
   */
  public InetSocketAddress getServiceRpcAddress() {
    return serviceRPCAddress != null ? serviceRPCAddress : rpcAddress;
  }

  /**
   * Returns the address of the NameNodes http server, 
   * which is used to access the name-node web UI.
   * 
   * @return the http address.
   */
  public InetSocketAddress getHttpAddress() {
    return httpAddress;
  }

  /**
   * Verify that configured directories exist, then
   * Interactively confirm that formatting is desired 
   * for each existing directory and format them.
   * 
   * @param conf
   * @param isConfirmationNeeded
   * @return true if formatting was aborted, false otherwise
   * @throws IOException
   */
  private static boolean format(Configuration conf,
                                boolean isConfirmationNeeded)
      throws IOException {
    if (!conf.getBoolean(DFSConfigKeys.DFS_NAMENODE_SUPPORT_ALLOW_FORMAT_KEY, 
                         DFSConfigKeys.DFS_NAMENODE_SUPPORT_ALLOW_FORMAT_DEFAULT)) {
      throw new IOException("The option " + DFSConfigKeys.DFS_NAMENODE_SUPPORT_ALLOW_FORMAT_KEY
                             + " is set to false for this filesystem, so it "
                             + "cannot be formatted. You will need to set "
                             + DFSConfigKeys.DFS_NAMENODE_SUPPORT_ALLOW_FORMAT_KEY +" parameter "
                             + "to true in order to format this filesystem");
    }
    
    Collection<URI> dirsToFormat = FSNamesystem.getNamespaceDirs(conf);
    Collection<URI> editDirsToFormat = 
                 FSNamesystem.getNamespaceEditsDirs(conf);
    for(Iterator<URI> it = dirsToFormat.iterator(); it.hasNext();) {
      File curDir = new File(it.next().getPath());
      // Its alright for a dir not to exist, or to exist (properly accessible)
      // and be completely empty.
      if (!curDir.exists() ||
          (curDir.isDirectory() && FileUtil.listFiles(curDir).length == 0))
        continue;
      if (isConfirmationNeeded) {
        if (!confirmPrompt("Re-format filesystem in " + curDir + " ?")) {
          System.err.println("Format aborted in "+ curDir);
          return true;
        }
      }
    }

    // if clusterID is not provided - see if you can find the current one
    String clusterId = StartupOption.FORMAT.getClusterId();
    if(clusterId == null || clusterId.equals("")) {
      //Generate a new cluster id
      clusterId = NNStorage.newClusterID();
    }
    System.out.println("Formatting using clusterid: " + clusterId);
    
    FSImage fsImage = new FSImage(conf, null, dirsToFormat, editDirsToFormat);
    FSNamesystem nsys = new FSNamesystem(fsImage, conf);
    nsys.dir.fsImage.format(clusterId);
    return false;
  }

  private static boolean finalize(Configuration conf,
                               boolean isConfirmationNeeded
                               ) throws IOException {
    FSNamesystem nsys = new FSNamesystem(new FSImage(conf), conf);
    System.err.print(
        "\"finalize\" will remove the previous state of the files system.\n"
        + "Recent upgrade will become permanent.\n"
        + "Rollback option will not be available anymore.\n");
    if (isConfirmationNeeded) {
      if (!confirmPrompt("Finalize filesystem state?")) {
        System.err.println("Finalize aborted.");
        return true;
      }
    }
    nsys.dir.fsImage.finalizeUpgrade();
    return false;
  }

  @Override // RefreshAuthorizationPolicyProtocol
  public void refreshServiceAcl() throws IOException {
    if (!serviceAuthEnabled) {
      throw new AuthorizationException("Service Level Authorization not enabled!");
    }

    this.server.refreshServiceAcl(new Configuration(), new HDFSPolicyProvider());
    if (this.serviceRpcServer != null) {
      this.serviceRpcServer.refreshServiceAcl(new Configuration(), new HDFSPolicyProvider());
    }
  }

  @Override // RefreshAuthorizationPolicyProtocol
  public void refreshUserToGroupsMappings() throws IOException {
    LOG.info("Refreshing all user-to-groups mappings. Requested by user: " + 
             UserGroupInformation.getCurrentUser().getShortUserName());
    Groups.getUserToGroupsMappingService().refresh();
  }

  @Override // RefreshAuthorizationPolicyProtocol
  public void refreshSuperUserGroupsConfiguration() {
    LOG.info("Refreshing SuperUser proxy group mapping list ");

    ProxyUsers.refreshSuperUserGroupsConfiguration();
  }
  
  @Override // GetUserMappingsProtocol
  public String[] getGroupsForUser(String user) throws IOException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Getting groups for user " + user);
    }
    return UserGroupInformation.createRemoteUser(user).getGroupNames();
  }

  private static void printUsage() {
    System.err.println(
      "Usage: java NameNode [" +
      StartupOption.BACKUP.getName() + "] | [" +
      StartupOption.CHECKPOINT.getName() + "] | [" +
      StartupOption.FORMAT.getName() + "[" + StartupOption.CLUSTERID.getName() +  
      " cid ]] | [" +
      StartupOption.UPGRADE.getName() + "] | [" +
      StartupOption.ROLLBACK.getName() + "] | [" +
      StartupOption.FINALIZE.getName() + "] | [" +
      StartupOption.IMPORT.getName() + "]");
  }

  private static StartupOption parseArguments(String args[]) {
    int argsLen = (args == null) ? 0 : args.length;
    StartupOption startOpt = StartupOption.REGULAR;
    for(int i=0; i < argsLen; i++) {
      String cmd = args[i];
      if (StartupOption.FORMAT.getName().equalsIgnoreCase(cmd)) {
        startOpt = StartupOption.FORMAT;
        // might be followed by two args
        if (i + 2 < argsLen
            && args[i + 1].equalsIgnoreCase(StartupOption.CLUSTERID.getName())) {
          i += 2;
          startOpt.setClusterId(args[i]);
        }
      } else if (StartupOption.GENCLUSTERID.getName().equalsIgnoreCase(cmd)) {
        startOpt = StartupOption.GENCLUSTERID;
      } else if (StartupOption.REGULAR.getName().equalsIgnoreCase(cmd)) {
        startOpt = StartupOption.REGULAR;
      } else if (StartupOption.BACKUP.getName().equalsIgnoreCase(cmd)) {
        startOpt = StartupOption.BACKUP;
      } else if (StartupOption.CHECKPOINT.getName().equalsIgnoreCase(cmd)) {
        startOpt = StartupOption.CHECKPOINT;
      } else if (StartupOption.UPGRADE.getName().equalsIgnoreCase(cmd)) {
        startOpt = StartupOption.UPGRADE;
        // might be followed by two args
        if (i + 2 < argsLen
            && args[i + 1].equalsIgnoreCase(StartupOption.CLUSTERID.getName())) {
          i += 2;
          startOpt.setClusterId(args[i]);
        }
      } else if (StartupOption.ROLLBACK.getName().equalsIgnoreCase(cmd)) {
        startOpt = StartupOption.ROLLBACK;
      } else if (StartupOption.FINALIZE.getName().equalsIgnoreCase(cmd)) {
        startOpt = StartupOption.FINALIZE;
      } else if (StartupOption.IMPORT.getName().equalsIgnoreCase(cmd)) {
        startOpt = StartupOption.IMPORT;
      } else
        return null;
    }
    return startOpt;
  }

  private static void setStartupOption(Configuration conf, StartupOption opt) {
    conf.set("dfs.namenode.startup", opt.toString());
  }

  static StartupOption getStartupOption(Configuration conf) {
    return StartupOption.valueOf(conf.get("dfs.namenode.startup",
                                          StartupOption.REGULAR.toString()));
  }

  /**
   * Print out a prompt to the user, and return true if the user
   * responds with "Y" or "yes".
   */
  static boolean confirmPrompt(String prompt) throws IOException {
    while (true) {
      System.err.print(prompt + " (Y or N) ");
      StringBuilder responseBuilder = new StringBuilder();
      while (true) {
        int c = System.in.read();
        if (c == -1 || c == '\r' || c == '\n') {
          break;
        }
        responseBuilder.append((char)c);
      }
  
      String response = responseBuilder.toString();
      if (response.equalsIgnoreCase("y") ||
          response.equalsIgnoreCase("yes")) {
        return true;
      } else if (response.equalsIgnoreCase("n") ||
          response.equalsIgnoreCase("no")) {
        return false;
      }
      // else ask them again
    }
  }

  public static NameNode createNameNode(String argv[], Configuration conf)
      throws IOException {
    if (conf == null)
      conf = new HdfsConfiguration();
    StartupOption startOpt = parseArguments(argv);
    if (startOpt == null) {
      printUsage();
      return null;
    }
    setStartupOption(conf, startOpt);

    switch (startOpt) {
      case FORMAT:
        boolean aborted = format(conf, true);
        System.exit(aborted ? 1 : 0);
        return null; // avoid javac warning
      case GENCLUSTERID:
        System.err.println("Generating new cluster id:");
        System.out.println(NNStorage.newClusterID());
        System.exit(0);
        return null;
      case FINALIZE:
        aborted = finalize(conf, true);
        System.exit(aborted ? 1 : 0);
        return null; // avoid javac warning
      case BACKUP:
      case CHECKPOINT:
        NamenodeRole role = startOpt.toNodeRole();
        DefaultMetricsSystem.initialize(role.toString().replace(" ", ""));
        return new BackupNode(conf, role);
      default:
        DefaultMetricsSystem.initialize("NameNode");
        return new NameNode(conf);
    }
  }

  /**
   * In federation configuration is set for a set of
   * namenode and secondary namenode/backup/checkpointer, which are
   * grouped under a logical nameservice ID. The configuration keys specific 
   * to them have suffix set to configured nameserviceId.
   * 
   * This method copies the value from specific key of format key.nameserviceId
   * to key, to set up the generic configuration. Once this is done, only
   * generic version of the configuration is read in rest of the code, for
   * backward compatibility and simpler code changes.
   * 
   * @param conf
   *          Configuration object to lookup specific key and to set the value
   *          to the key passed. Note the conf object is modified
   * @see DFSUtil#setGenericConf(Configuration, String, String...)
   */
  public static void initializeGenericKeys(Configuration conf) {
    final String nameserviceId = DFSUtil.getNameServiceId(conf);
    if ((nameserviceId == null) || nameserviceId.isEmpty()) {
      return;
    }
    
    DFSUtil.setGenericConf(conf, nameserviceId, NAMESERVICE_SPECIFIC_KEYS);
    
    if (conf.get(DFSConfigKeys.DFS_NAMENODE_RPC_ADDRESS_KEY) != null) {
      URI defaultUri = URI.create(FSConstants.HDFS_URI_SCHEME + "://"
          + conf.get(DFSConfigKeys.DFS_NAMENODE_RPC_ADDRESS_KEY));
      conf.set(DFSConfigKeys.FS_DEFAULT_NAME_KEY, defaultUri.toString());
    }
  }
    
  /**
   */
  public static void main(String argv[]) throws Exception {
    try {
      StringUtils.startupShutdownMessage(NameNode.class, argv, LOG);
      NameNode namenode = createNameNode(argv, null);
      if (namenode != null)
        namenode.join();
    } catch (Throwable e) {
      LOG.error("Exception in namenode join", e);
      System.exit(-1);
    }
  }
  
  private static String getClientMachine() {
    String clientMachine = Server.getRemoteAddress();
    if (clientMachine == null) {
      clientMachine = "";
    }
    return clientMachine;
  }
}
