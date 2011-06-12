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
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsServerDefaults;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.Trash;
import org.apache.hadoop.fs.Options;
import org.apache.hadoop.fs.UnresolvedLinkException;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.hdfs.HDFSPolicyProvider;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.BlockListAsLongs;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.DirectoryListing;
import org.apache.hadoop.hdfs.protocol.FSConstants;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.protocol.UnregisteredNodeException;
import org.apache.hadoop.hdfs.protocol.UnresolvedPathException;
import org.apache.hadoop.hdfs.security.ExportedAccessKeys;
import org.apache.hadoop.hdfs.server.common.IncorrectVersionException;
import org.apache.hadoop.hdfs.server.common.UpgradeStatusReport;
import org.apache.hadoop.hdfs.server.common.HdfsConstants.NamenodeRole;
import org.apache.hadoop.hdfs.server.common.HdfsConstants.StartupOption;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem.CompleteFileStatus;
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
import org.apache.hadoop.hdfs.server.protocol.UpgradeCommand;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.http.HttpServer;
import org.apache.hadoop.io.EnumSetWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.net.NetworkTopology;
import org.apache.hadoop.net.Node;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.Groups;
import org.apache.hadoop.security.RefreshUserToGroupMappingsProtocol;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.AuthorizationException;
import org.apache.hadoop.security.authorize.RefreshAuthorizationPolicyProtocol;
import org.apache.hadoop.security.authorize.ServiceAuthorizationManager;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.SecretManager.InvalidToken;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.util.ServicePlugin;
import org.apache.hadoop.util.StringUtils;

/**********************************************************
 * NameNode serves as both directory namespace manager and
 * "inode table" for the Hadoop DFS.  There is a single NameNode
 * running in any DFS deployment.  (Well, except when there
 * is a second backup/failover NameNode.)
 *
 * The NameNode controls two critical tables:
 *   1)  filename->blocksequence (namespace)
 *   2)  block->machinelist ("inodes")
 *
 * The first table is stored on disk and is very precious.
 * The second table is rebuilt every time the NameNode comes
 * up.
 *
 * 'NameNode' refers to both this class as well as the 'NameNode server'.
 * The 'FSNamesystem' class actually performs most of the filesystem
 * management.  The majority of the 'NameNode' class itself is concerned
 * with exposing the IPC interface and the http server to the outside world,
 * plus some configuration management.
 *
 * NameNode implements the ClientProtocol interface, which allows
 * clients to ask for DFS services.  ClientProtocol is not
 * designed for direct use by authors of DFS client code.  End-users
 * should instead use the org.apache.nutch.hadoop.fs.FileSystem class.
 *
 * NameNode also implements the DatanodeProtocol interface, used by
 * DataNode programs that actually store DFS data blocks.  These
 * methods are invoked repeatedly and automatically by all the
 * DataNodes in a DFS deployment.
 *
 * NameNode also implements the NamenodeProtocol interface, used by
 * secondary namenodes or rebalancing processes to get partial namenode's
 * state, for example partial blocksMap etc.
 **********************************************************/
@InterfaceAudience.Private
public class NameNode implements NamenodeProtocols, FSConstants {
  static{
    Configuration.addDefaultResource("hdfs-default.xml");
    Configuration.addDefaultResource("hdfs-site.xml");
  }
  
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
    } else if (protocol.equals(RefreshUserToGroupMappingsProtocol.class.getName())){
      return RefreshUserToGroupMappingsProtocol.versionID;
    } else {
      throw new IOException("Unknown protocol to name node: " + protocol);
    }
  }
    
  public static final int DEFAULT_PORT = 8020;

  public static final Log LOG = LogFactory.getLog(NameNode.class.getName());
  public static final Log stateChangeLog = LogFactory.getLog("org.apache.hadoop.hdfs.StateChange");

  protected FSNamesystem namesystem; 
  protected NamenodeRole role;
  /** RPC server */
  protected Server server;
  /** RPC server address */
  protected InetSocketAddress rpcAddress = null;
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

  static NameNodeMetrics myMetrics;

  /** Return the {@link FSNamesystem} object.
   * @return {@link FSNamesystem} object.
   */
  FSNamesystem getNamesystem() {
    return namesystem;
  }

  static void initMetrics(Configuration conf, NamenodeRole role) {
    myMetrics = new NameNodeMetrics(conf, role);
  }

  public static NameNodeMetrics getNameNodeMetrics() {
    return myMetrics;
  }
  
  public static InetSocketAddress getAddress(String address) {
    return NetUtils.createSocketAddr(address, DEFAULT_PORT);
  }

  public static InetSocketAddress getAddress(Configuration conf) {
    URI filesystemURI = FileSystem.getDefaultUri(conf);
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

  protected InetSocketAddress getRpcServerAddress(Configuration conf) throws IOException {
    return getAddress(conf);
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
        getFSImage(), getRole(), getFSImage().getCheckpointTime());
    return nodeRegistration;
  }

  /**
   * Initialize name-node.
   * 
   * @param conf the configuration
   */
  protected void initialize(Configuration conf) throws IOException {
    InetSocketAddress socAddr = getRpcServerAddress(conf);
    int handlerCount = conf.getInt("dfs.namenode.handler.count", 10);
    
    // set service-level authorization security policy
    if (serviceAuthEnabled = 
          conf.getBoolean(
            ServiceAuthorizationManager.SERVICE_AUTHORIZATION_CONFIG, false)) {
      ServiceAuthorizationManager.refresh(conf, new HDFSPolicyProvider());
    }

    NameNode.initMetrics(conf, this.getRole());
    loadNamesystem(conf);
    // create rpc server 
    this.server = RPC.getServer(NamenodeProtocols.class, this,
                                socAddr.getHostName(), socAddr.getPort(),
                                handlerCount, false, conf, 
				namesystem.getDelegationTokenSecretManager());
    // The rpc-server port can be ephemeral... ensure we have the correct info
    this.rpcAddress = this.server.getListenerAddress(); 
    setRpcServerAddress(conf);

    activate(conf);
    LOG.info(getRole() + " up at: " + rpcAddress);
  }

  /**
   * Activate name-node servers and threads.
   */
  void activate(Configuration conf) throws IOException {
    if ((isRole(NamenodeRole.ACTIVE))
        && (UserGroupInformation.isSecurityEnabled())) {
      namesystem.activateSecretManager();
    }
    namesystem.activate(conf);
    startHttpServer(conf);
    server.start();  //start RPC server
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
    long trashInterval = conf.getLong("fs.trash.interval", 0);
    if(trashInterval == 0)
      return;
    this.emptier = new Thread(new Trash(conf).getEmptier(), "Trash Emptier");
    this.emptier.setDaemon(true);
    this.emptier.start();
  }

  private void startHttpServer(Configuration conf) throws IOException {
    InetSocketAddress infoSocAddr = getHttpServerAddress(conf);
    String infoHost = infoSocAddr.getHostName();
    int infoPort = infoSocAddr.getPort();
    this.httpServer = new HttpServer("hdfs", infoHost, infoPort, 
        infoPort == 0, conf);
    if (conf.getBoolean("dfs.https.enable", false)) {
      boolean needClientAuth = conf.getBoolean(DFSConfigKeys.DFS_CLIENT_HTTPS_NEED_AUTH_KEY,
                                               DFSConfigKeys.DFS_CLIENT_HTTPS_NEED_AUTH_DEFAULT);
      InetSocketAddress secInfoSocAddr = NetUtils.createSocketAddr(conf.get(
          DFSConfigKeys.DFS_NAMENODE_HTTPS_ADDRESS_KEY, infoHost + ":" + 0));
      Configuration sslConf = new HdfsConfiguration(false);
      sslConf.addResource(conf.get("dfs.https.server.keystore.resource",
          "ssl-server.xml"));
      this.httpServer.addSslListener(secInfoSocAddr, sslConf, needClientAuth);
      // assume same ssl port for all datanodes
      InetSocketAddress datanodeSslPort = NetUtils.createSocketAddr(conf.get(
          "dfs.datanode.https.address", infoHost + ":" + 50475));
      this.httpServer.setAttribute("datanode.https.port", datanodeSslPort
          .getPort());
    }
    this.httpServer.setAttribute("name.node", this);
    this.httpServer.setAttribute("name.node.address", getNameNodeAddress());
    this.httpServer.setAttribute("name.system.image", getFSImage());
    this.httpServer.setAttribute("name.conf", conf);
    this.httpServer.addInternalServlet("getDelegationToken", 
        DelegationTokenServlet.PATH_SPEC, DelegationTokenServlet.class);
    this.httpServer.addInternalServlet("fsck", "/fsck", FsckServlet.class);
    this.httpServer.addInternalServlet("getimage", "/getimage", GetImageServlet.class);
    this.httpServer.addInternalServlet("listPaths", "/listPaths/*", ListPathsServlet.class);
    this.httpServer.addInternalServlet("data", "/data/*", FileDataServlet.class);
    this.httpServer.addInternalServlet("checksum", "/fileChecksum/*",
        FileChecksumServlets.RedirectServlet.class);
    this.httpServer.addInternalServlet("contentSummary", "/contentSummary/*",
        ContentSummaryServlet.class);
    this.httpServer.start();

    // The web-server port can be ephemeral... ensure we have the correct info
    infoPort = this.httpServer.getPort();
    this.httpAddress = new InetSocketAddress(infoHost, infoPort);
    setHttpServerAddress(conf);
    LOG.info(getRole() + " Web-server up at: " + httpAddress);
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
    this(conf, NamenodeRole.ACTIVE);
  }

  protected NameNode(Configuration conf, NamenodeRole role) 
      throws IOException { 
    UserGroupInformation.setConfiguration(conf);
    DFSUtil.login(conf, 
        DFSConfigKeys.DFS_NAMENODE_KEYTAB_FILE_KEY,
        DFSConfigKeys.DFS_NAMENODE_USER_NAME_KEY);

    this.role = role;
    try {
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
      LOG.error(StringUtils.stringifyException(e));
    }
    if(namesystem != null) namesystem.close();
    if(emptier != null) emptier.interrupt();
    if(server != null) server.stop();
    if (myMetrics != null) {
      myMetrics.shutdown();
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

  /** {@inheritDoc} */
  public ExportedAccessKeys getAccessKeys() throws IOException {
    return namesystem.getAccessKeys();
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
    namesystem.registerBackupNode(registration);
    return setRegistration();
  }

  @Override // NamenodeProtocol
  public NamenodeCommand startCheckpoint(NamenodeRegistration registration)
  throws IOException {
    verifyRequest(registration);
    if(!isRole(NamenodeRole.ACTIVE))
      throw new IOException("Only an ACTIVE node can invoke startCheckpoint.");
    return namesystem.startCheckpoint(registration, setRegistration());
  }

  @Override // NamenodeProtocol
  public void endCheckpoint(NamenodeRegistration registration,
                            CheckpointSignature sig) throws IOException {
    verifyRequest(registration);
    if(!isRole(NamenodeRole.ACTIVE))
      throw new IOException("Only an ACTIVE node can invoke endCheckpoint.");
    namesystem.endCheckpoint(registration, sig);
  }

  @Override // NamenodeProtocol
  public long journalSize(NamenodeRegistration registration)
  throws IOException {
    verifyRequest(registration);
    return namesystem.getEditLogSize();
  }

  /*
   * Active name-node cannot journal.
   */
  @Override // NamenodeProtocol
  public void journal(NamenodeRegistration registration,
                      int jAction,
                      int length,
                      byte[] args) throws IOException {
    throw new UnsupportedActionException("journal");
  }

  /////////////////////////////////////////////////////
  // ClientProtocol
  /////////////////////////////////////////////////////
  
  public Token<DelegationTokenIdentifier> getDelegationToken(Text renewer)
      throws IOException {
    return namesystem.getDelegationToken(renewer);
  }

  @Override
  public long renewDelegationToken(Token<DelegationTokenIdentifier> token)
      throws InvalidToken, IOException {
    return namesystem.renewDelegationToken(token);
  }

  @Override
  public void cancelDelegationToken(Token<DelegationTokenIdentifier> token)
      throws IOException {
    namesystem.cancelDelegationToken(token);
  }
  
  /** {@inheritDoc} */
  public LocatedBlocks getBlockLocations(String src, 
                                          long offset, 
                                          long length) 
      throws IOException {
    myMetrics.numGetBlockLocations.inc();
    return namesystem.getBlockLocations(getClientMachine(), 
                                        src, offset, length);
  }
  
  /**
   * The specification of this method matches that of
   * {@link getBlockLocations(Path)}
   * except that it does not update the file's access time.
   */
  LocatedBlocks getBlockLocationsNoATime(String src, 
                                         long offset, 
                                         long length)
      throws IOException {
    myMetrics.numGetBlockLocations.inc();
    return namesystem.getBlockLocations(src, offset, length, false);
  }
  
  private static String getClientMachine() {
    String clientMachine = Server.getRemoteAddress();
    if (clientMachine == null) {
      clientMachine = "";
    }
    return clientMachine;
  }

  /** {@inheritDoc} */
  public FsServerDefaults getServerDefaults() throws IOException {
    return namesystem.getServerDefaults();
  }

  /** {@inheritDoc} */
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
    myMetrics.numFilesCreated.inc();
    myMetrics.numCreateFileOps.inc();
  }

  /** {@inheritDoc} */
  public LocatedBlock append(String src, String clientName) 
      throws IOException {
    String clientMachine = getClientMachine();
    if (stateChangeLog.isDebugEnabled()) {
      stateChangeLog.debug("*DIR* NameNode.append: file "
          +src+" for "+clientName+" at "+clientMachine);
    }
    LocatedBlock info = namesystem.appendFile(src, clientName, clientMachine);
    myMetrics.numFilesAppended.inc();
    return info;
  }

  /** {@inheritDoc} */
  public boolean setReplication(String src, short replication) 
    throws IOException {  
    return namesystem.setReplication(src, replication);
  }
    
  /** {@inheritDoc} */
  public void setPermission(String src, FsPermission permissions)
      throws IOException {
    namesystem.setPermission(src, permissions);
  }

  /** {@inheritDoc} */
  public void setOwner(String src, String username, String groupname)
      throws IOException {
    namesystem.setOwner(src, username, groupname);
  }

  @Override
  public LocatedBlock addBlock(String src,
                               String clientName,
                               Block previous,
                               DatanodeInfo[] excludedNodes)
      throws IOException {
    stateChangeLog.debug("*BLOCK* NameNode.addBlock: file "
                         +src+" for "+clientName);
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
      myMetrics.numAddBlockOps.inc();
    return locatedBlock;
  }

  /**
   * The client needs to give up on the block.
   */
  public void abandonBlock(Block b, String src, String holder)
      throws IOException {
    stateChangeLog.debug("*BLOCK* NameNode.abandonBlock: "
                         +b+" of file "+src);
    if (!namesystem.abandonBlock(b, src, holder)) {
      throw new IOException("Cannot abandon block during write to " + src);
    }
  }

  /** {@inheritDoc} */
  public boolean complete(String src, String clientName, Block last)
      throws IOException {
    stateChangeLog.debug("*DIR* NameNode.complete: " + src + " for " + clientName);
    CompleteFileStatus returnCode =
      namesystem.completeFile(src, clientName, last);
    if (returnCode == CompleteFileStatus.STILL_WAITING) {
      return false;
    } else if (returnCode == CompleteFileStatus.COMPLETE_SUCCESS) {
      return true;
    } else {
      throw new IOException("Could not complete write to file " + 
                            src + " by " + clientName);
    }
  }

  /**
   * The client has detected an error on the specified located blocks 
   * and is reporting them to the server.  For now, the namenode will 
   * mark the block as corrupt.  In the future we might 
   * check the blocks are actually corrupt. 
   */
  public void reportBadBlocks(LocatedBlock[] blocks) throws IOException {
    stateChangeLog.info("*DIR* NameNode.reportBadBlocks");
    for (int i = 0; i < blocks.length; i++) {
      Block blk = blocks[i].getBlock();
      DatanodeInfo[] nodes = blocks[i].getLocations();
      for (int j = 0; j < nodes.length; j++) {
        DatanodeInfo dn = nodes[j];
        namesystem.markBlockAsCorrupt(blk, dn);
      }
    }
  }

  /** {@inheritDoc} */
  @Override
  public LocatedBlock updateBlockForPipeline(Block block, String clientName)
      throws IOException {
    return namesystem.updateBlockForPipeline(block, clientName);
  }


  @Override
  public void updatePipeline(String clientName, Block oldBlock,
      Block newBlock, DatanodeID[] newNodes)
      throws IOException {
    namesystem.updatePipeline(clientName, oldBlock, newBlock, newNodes);
  }
  
  /** {@inheritDoc} */
  public void commitBlockSynchronization(Block block,
      long newgenerationstamp, long newlength,
      boolean closeFile, boolean deleteblock, DatanodeID[] newtargets)
      throws IOException {
    namesystem.commitBlockSynchronization(block,
        newgenerationstamp, newlength, closeFile, deleteblock, newtargets);
  }
  
  public long getPreferredBlockSize(String filename) 
      throws IOException {
    return namesystem.getPreferredBlockSize(filename);
  }
    
  /** {@inheritDoc} */
  @Deprecated
  @Override
  public boolean rename(String src, String dst) throws IOException {
    stateChangeLog.debug("*DIR* NameNode.rename: " + src + " to " + dst);
    if (!checkPathLength(dst)) {
      throw new IOException("rename: Pathname too long.  Limit " 
                            + MAX_PATH_LENGTH + " characters, " + MAX_PATH_DEPTH + " levels.");
    }
    boolean ret = namesystem.renameTo(src, dst);
    if (ret) {
      myMetrics.numFilesRenamed.inc();
    }
    return ret;
  }
  
  /** 
   * {@inheritDoc}
   */
  public void concat(String trg, String[] src) throws IOException {
    namesystem.concat(trg, src);
  }
  
  /** {@inheritDoc} */
  @Override
  public void rename(String src, String dst, Options.Rename... options)
      throws IOException {
    stateChangeLog.debug("*DIR* NameNode.rename: " + src + " to " + dst);
    if (!checkPathLength(dst)) {
      throw new IOException("rename: Pathname too long.  Limit " 
                            + MAX_PATH_LENGTH + " characters, " + MAX_PATH_DEPTH + " levels.");
    }
    namesystem.renameTo(src, dst, options);
    myMetrics.numFilesRenamed.inc();
  }

  /**
   */
  @Deprecated
  public boolean delete(String src) throws IOException {
    return delete(src, true);
  }

  /** {@inheritDoc} */
  public boolean delete(String src, boolean recursive) throws IOException {
    if (stateChangeLog.isDebugEnabled()) {
      stateChangeLog.debug("*DIR* Namenode.delete: src=" + src
          + ", recursive=" + recursive);
    }
    boolean ret = namesystem.delete(src, recursive);
    if (ret) 
      myMetrics.numDeleteFileOps.inc();
    return ret;
  }

  /**
   * Check path length does not exceed maximum.  Returns true if
   * length and depth are okay.  Returns false if length is too long 
   * or depth is too great.
   * 
   */
  private boolean checkPathLength(String src) {
    Path srcPath = new Path(src);
    return (src.length() <= MAX_PATH_LENGTH &&
            srcPath.depth() <= MAX_PATH_DEPTH);
  }
    
  /** {@inheritDoc} */
  public boolean mkdirs(String src, FsPermission masked, boolean createParent)
      throws IOException {
    stateChangeLog.debug("*DIR* NameNode.mkdirs: " + src);
    if (!checkPathLength(src)) {
      throw new IOException("mkdirs: Pathname too long.  Limit " 
                            + MAX_PATH_LENGTH + " characters, " + MAX_PATH_DEPTH + " levels.");
    }
    return namesystem.mkdirs(src,
        new PermissionStatus(UserGroupInformation.getCurrentUser().getShortUserName(),
            null, masked), createParent);
  }

  /**
   */
  public void renewLease(String clientName) throws IOException {
    namesystem.renewLease(clientName);        
  }

  /**
   */
  @Override
  public DirectoryListing getListing(String src, byte[] startAfter)
  throws IOException {
    DirectoryListing files = namesystem.getListing(src, startAfter);
    if (files != null) {
      myMetrics.numGetListingOps.inc();
      myMetrics.numFilesInGetListingOps.inc(files.getPartialListing().length);
    }
    return files;
  }

  /**
   * Get the file info for a specific file.
   * @param src The string representation of the path to the file
   * @return object containing information regarding the file
   *         or null if file not found
   */
  public HdfsFileStatus getFileInfo(String src)  throws IOException {
    myMetrics.numFileInfoOps.inc();
    return namesystem.getFileInfo(src, true);
  }

  /**
   * Get the file info for a specific file. If the path refers to a 
   * symlink then the FileStatus of the symlink is returned.
   * @param src The string representation of the path to the file
   * @return object containing information regarding the file
   *         or null if file not found
   */
  public HdfsFileStatus getFileLinkInfo(String src) throws IOException { 
    myMetrics.numFileInfoOps.inc();
    return namesystem.getFileInfo(src, false);
  }
  
  /** @inheritDoc */
  public long[] getStats() {
    return namesystem.getStats();
  }

  /**
   */
  public DatanodeInfo[] getDatanodeReport(DatanodeReportType type)
      throws IOException {
    DatanodeInfo results[] = namesystem.datanodeReport(type);
    if (results == null ) {
      throw new IOException("Cannot find datanode report");
    }
    return results;
  }
    
  /**
   * @inheritDoc
   */
  public boolean setSafeMode(SafeModeAction action) throws IOException {
    return namesystem.setSafeMode(action);
  }

  /**
   * Is the cluster currently in safe mode?
   */
  public boolean isInSafeMode() {
    return namesystem.isInSafeMode();
  }

  /**
   * @throws AccessControlException 
   * @inheritDoc
   */
  public boolean restoreFailedStorage(String arg) 
      throws AccessControlException {
    return namesystem.restoreFailedStorage(arg);
  }

  /**
   * @inheritDoc
   */
  public void saveNamespace() throws IOException {
    namesystem.saveNamespace();
  }

  /**
   * Refresh the list of datanodes that the namenode should allow to  
   * connect.  Re-reads conf by creating new HdfsConfiguration object and 
   * uses the files list in the configuration to update the list. 
   */
  public void refreshNodes() throws IOException {
    namesystem.refreshNodes(new HdfsConfiguration());
  }

  /**
   * Returns the size of the current edit log.
   */
  @Deprecated
  public long getEditLogSize() throws IOException {
    return namesystem.getEditLogSize();
  }

  /**
   * Roll the edit log.
   */
  @Deprecated
  public CheckpointSignature rollEditLog() throws IOException {
    return namesystem.rollEditLog();
  }

  /**
   * Roll the image 
   */
  @Deprecated
  public void rollFsImage() throws IOException {
    namesystem.rollFSImage();
  }
    
  public void finalizeUpgrade() throws IOException {
    namesystem.finalizeUpgrade();
  }

  public UpgradeStatusReport distributedUpgradeProgress(UpgradeAction action)
      throws IOException {
    return namesystem.distributedUpgradeProgress(action);
  }

  /**
   * Dumps namenode state into specified file
   */
  public void metaSave(String filename) throws IOException {
    namesystem.metaSave(filename);
  }

  /** {@inheritDoc} */
  public FileStatus[] getCorruptFiles() 
    throws AccessControlException, IOException {
    
    return namesystem.getCorruptFiles();
    
  }
  
  /** {@inheritDoc} */
  public ContentSummary getContentSummary(String path) throws IOException {
    return namesystem.getContentSummary(path);
  }

  /** {@inheritDoc} */
  public void setQuota(String path, long namespaceQuota, long diskspaceQuota) 
      throws IOException {
    namesystem.setQuota(path, namespaceQuota, diskspaceQuota);
  }
  
  /** {@inheritDoc} */
  public void fsync(String src, String clientName) throws IOException {
    namesystem.fsync(src, clientName);
  }

  /** @inheritDoc */
  public void setTimes(String src, long mtime, long atime) 
      throws IOException {
    namesystem.setTimes(src, mtime, atime);
  }

  /** @inheritDoc */
  public void createSymlink(String target, String link, FsPermission dirPerms, 
                            boolean createParent) 
      throws IOException {
    myMetrics.numcreateSymlinkOps.inc();
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
      new PermissionStatus(ugi.getUserName(), null, dirPerms), createParent);
  }

  /** @inheritDoc */
  public String getLinkTarget(String path) throws IOException {
    myMetrics.numgetLinkTargetOps.inc();
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


  ////////////////////////////////////////////////////////////////
  // DatanodeProtocol
  ////////////////////////////////////////////////////////////////
  /** 
   */
  public DatanodeRegistration registerDatanode(DatanodeRegistration nodeReg)
      throws IOException {
    verifyVersion(nodeReg.getVersion());
    namesystem.registerDatanode(nodeReg);
      
    return nodeReg;
  }

  /**
   * Data node notify the name node that it is alive 
   * Return an array of block-oriented commands for the datanode to execute.
   * This will be either a transfer or a delete operation.
   */
  public DatanodeCommand[] sendHeartbeat(DatanodeRegistration nodeReg,
                                       long capacity,
                                       long dfsUsed,
                                       long remaining,
                                       int xmitsInProgress,
                                       int xceiverCount) throws IOException {
    verifyRequest(nodeReg);
    return namesystem.handleHeartbeat(nodeReg, capacity, dfsUsed, remaining,
        xceiverCount, xmitsInProgress);
  }

  public DatanodeCommand blockReport(DatanodeRegistration nodeReg,
                                     long[] blocks) throws IOException {
    verifyRequest(nodeReg);
    BlockListAsLongs blist = new BlockListAsLongs(blocks);
    stateChangeLog.debug("*BLOCK* NameNode.blockReport: "
           +"from "+nodeReg.getName()+" "+blist.getNumberOfBlocks() +" blocks");

    namesystem.processReport(nodeReg, blist);
    if (getFSImage().isUpgradeFinalized())
      return DatanodeCommand.FINALIZE;
    return null;
  }

  public void blockReceived(DatanodeRegistration nodeReg, 
                            Block blocks[],
                            String delHints[]) throws IOException {
    verifyRequest(nodeReg);
    stateChangeLog.debug("*BLOCK* NameNode.blockReceived: "
                         +"from "+nodeReg.getName()+" "+blocks.length+" blocks.");
    for (int i = 0; i < blocks.length; i++) {
      namesystem.blockReceived(nodeReg, blocks[i], delHints[i]);
    }
  }

  /**
   */
  public void errorReport(DatanodeRegistration nodeReg,
                          int errorCode, 
                          String msg) throws IOException {
    // Log error message from datanode
    String dnName = (nodeReg == null ? "unknown DataNode" : nodeReg.getName());
    LOG.info("Error report from " + dnName + ": " + msg);
    if (errorCode == DatanodeProtocol.NOTIFY) {
      return;
    }
    verifyRequest(nodeReg);
    if (errorCode == DatanodeProtocol.DISK_ERROR) {
      LOG.warn("Volume failed on " + dnName); 
    } else if (errorCode == DatanodeProtocol.FATAL_DISK_ERROR) {
      namesystem.removeDatanode(nodeReg);            
    }
  }
    
  public NamespaceInfo versionRequest() throws IOException {
    return namesystem.getNamespaceInfo();
  }

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
    if (!namesystem.getRegistrationID().equals(nodeReg.getRegistrationID()))
      throw new UnregisteredNodeException(nodeReg);
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

  /**
   * Returns the name of the fsImage file
   */
  public File getFsImageName() throws IOException {
    return getFSImage().getFsImageName();
  }
    
  public FSImage getFSImage() {
    return namesystem.dir.fsImage;
  }

  /**
   * Returns the name of the fsImage file uploaded by periodic
   * checkpointing
   */
  public File[] getFsImageNameCheckpoint() throws IOException {
    return getFSImage().getFsImageNameCheckpoint();
  }

  /**
   * Returns the address on which the NameNodes is listening to.
   * @return the address on which the NameNodes is listening to.
   */
  public InetSocketAddress getNameNodeAddress() {
    return rpcAddress;
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

  NetworkTopology getNetworkTopology() {
    return this.namesystem.clusterMap;
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
    Collection<URI> dirsToFormat = FSNamesystem.getNamespaceDirs(conf);
    Collection<URI> editDirsToFormat = 
                 FSNamesystem.getNamespaceEditsDirs(conf);
    for(Iterator<URI> it = dirsToFormat.iterator(); it.hasNext();) {
      File curDir = new File(it.next().getPath());
      if (!curDir.exists())
        continue;
      if (isConfirmationNeeded) {
        System.err.print("Re-format filesystem in " + curDir +" ? (Y or N) ");
        if (!(System.in.read() == 'Y')) {
          System.err.println("Format aborted in "+ curDir);
          return true;
        }
        while(System.in.read() != '\n'); // discard the enter-key
      }
    }

    FSNamesystem nsys = new FSNamesystem(new FSImage(dirsToFormat,
                                         editDirsToFormat), conf);
    nsys.dir.fsImage.format();
    return false;
  }

  private static boolean finalize(Configuration conf,
                               boolean isConfirmationNeeded
                               ) throws IOException {
    Collection<URI> dirsToFormat = FSNamesystem.getNamespaceDirs(conf);
    Collection<URI> editDirsToFormat = 
                               FSNamesystem.getNamespaceEditsDirs(conf);
    FSNamesystem nsys = new FSNamesystem(new FSImage(dirsToFormat,
                                         editDirsToFormat), conf);
    System.err.print(
        "\"finalize\" will remove the previous state of the files system.\n"
        + "Recent upgrade will become permanent.\n"
        + "Rollback option will not be available anymore.\n");
    if (isConfirmationNeeded) {
      System.err.print("Finalize filesystem state ? (Y or N) ");
      if (!(System.in.read() == 'Y')) {
        System.err.println("Finalize aborted.");
        return true;
      }
      while(System.in.read() != '\n'); // discard the enter-key
    }
    nsys.dir.fsImage.finalizeUpgrade();
    return false;
  }

  @Override
  public void refreshServiceAcl() throws IOException {
    if (!serviceAuthEnabled) {
      throw new AuthorizationException("Service Level Authorization not enabled!");
    }

    ServiceAuthorizationManager.refresh(
        new Configuration(), new HDFSPolicyProvider());
  }

  @Override
  public void refreshUserToGroupsMappings(Configuration conf) throws IOException {
    LOG.info("Refreshing all user-to-groups mappings. Requested by user: " + 
             UserGroupInformation.getCurrentUser().getShortUserName());
    Groups.getUserToGroupsMappingService(conf).refresh();
  }

  private static void printUsage() {
    System.err.println(
      "Usage: java NameNode [" +
      StartupOption.BACKUP.getName() + "] | [" +
      StartupOption.CHECKPOINT.getName() + "] | [" +
      StartupOption.FORMAT.getName() + "] | [" +
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
      } else if (StartupOption.REGULAR.getName().equalsIgnoreCase(cmd)) {
        startOpt = StartupOption.REGULAR;
      } else if (StartupOption.BACKUP.getName().equalsIgnoreCase(cmd)) {
        startOpt = StartupOption.BACKUP;
      } else if (StartupOption.CHECKPOINT.getName().equalsIgnoreCase(cmd)) {
        startOpt = StartupOption.CHECKPOINT;
      } else if (StartupOption.UPGRADE.getName().equalsIgnoreCase(cmd)) {
        startOpt = StartupOption.UPGRADE;
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
      case FINALIZE:
        aborted = finalize(conf, true);
        System.exit(aborted ? 1 : 0);
        return null; // avoid javac warning
      case BACKUP:
      case CHECKPOINT:
        return new BackupNode(conf, startOpt.toNodeRole());
      default:
        return new NameNode(conf);
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
      LOG.error(StringUtils.stringifyException(e));
      System.exit(-1);
    }
  }
}
