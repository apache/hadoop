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
import java.io.PrintStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ha.HAServiceProtocol.HAServiceState;
import org.apache.hadoop.ha.HAServiceProtocol.StateChangeRequestInfo;
import org.apache.hadoop.ha.HAServiceStatus;
import org.apache.hadoop.ha.HealthCheckFailedException;
import org.apache.hadoop.ha.ServiceFailedException;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Trash;
import static org.apache.hadoop.hdfs.DFSConfigKeys.*;
import static org.apache.hadoop.util.ExitUtil.terminate;
import static org.apache.hadoop.util.ToolRunner.confirmPrompt;

import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.HAUtil;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.NamenodeRole;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.StartupOption;
import org.apache.hadoop.hdfs.server.namenode.ha.ActiveState;
import org.apache.hadoop.hdfs.server.namenode.ha.BootstrapStandby;
import org.apache.hadoop.hdfs.server.namenode.ha.HAContext;
import org.apache.hadoop.hdfs.server.namenode.ha.HAState;
import org.apache.hadoop.hdfs.server.namenode.ha.StandbyState;
import org.apache.hadoop.hdfs.server.namenode.metrics.NameNodeMetrics;
import org.apache.hadoop.hdfs.server.protocol.DatanodeProtocol;
import org.apache.hadoop.hdfs.server.protocol.JournalProtocol;
import org.apache.hadoop.hdfs.server.protocol.NamenodeProtocol;
import org.apache.hadoop.hdfs.server.protocol.NamenodeProtocols;
import org.apache.hadoop.hdfs.server.protocol.NamenodeRegistration;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.ipc.StandbyException;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.RefreshUserMappingsProtocol;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.RefreshAuthorizationPolicyProtocol;
import org.apache.hadoop.tools.GetUserMappingsProtocol;
import org.apache.hadoop.util.ExitUtil.ExitException;
import org.apache.hadoop.util.ServicePlugin;
import org.apache.hadoop.util.StringUtils;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

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
public class NameNode {
  static{
    HdfsConfiguration.init();
  }
  
  /**
   * Categories of operations supported by the namenode.
   */
  public static enum OperationCategory {
    /** Operations that are state agnostic */
    UNCHECKED,
    /** Read operation that does not change the namespace state */
    READ,
    /** Write operation that changes the namespace state */
    WRITE,
    /** Operations related to checkpointing */
    CHECKPOINT,
    /** Operations related to {@link JournalProtocol} */
    JOURNAL
  }
  
  /**
   * HDFS configuration can have three types of parameters:
   * <ol>
   * <li>Parameters that are common for all the name services in the cluster.</li>
   * <li>Parameters that are specific to a name service. These keys are suffixed
   * with nameserviceId in the configuration. For example,
   * "dfs.namenode.rpc-address.nameservice1".</li>
   * <li>Parameters that are specific to a single name node. These keys are suffixed
   * with nameserviceId and namenodeId in the configuration. for example,
   * "dfs.namenode.rpc-address.nameservice1.namenode1"</li>
   * </ol>
   * 
   * In the latter cases, operators may specify the configuration without
   * any suffix, with a nameservice suffix, or with a nameservice and namenode
   * suffix. The more specific suffix will take precedence.
   * 
   * These keys are specific to a given namenode, and thus may be configured
   * globally, for a nameservice, or for a specific namenode within a nameservice.
   */
  public static final String[] NAMENODE_SPECIFIC_KEYS = {
    DFS_NAMENODE_RPC_ADDRESS_KEY,
    DFS_NAMENODE_NAME_DIR_KEY,
    DFS_NAMENODE_EDITS_DIR_KEY,
    DFS_NAMENODE_SHARED_EDITS_DIR_KEY,
    DFS_NAMENODE_CHECKPOINT_DIR_KEY,
    DFS_NAMENODE_CHECKPOINT_EDITS_DIR_KEY,
    DFS_NAMENODE_SERVICE_RPC_ADDRESS_KEY,
    DFS_NAMENODE_HTTP_ADDRESS_KEY,
    DFS_NAMENODE_KEYTAB_FILE_KEY,
    DFS_NAMENODE_SECONDARY_HTTP_ADDRESS_KEY,
    DFS_SECONDARY_NAMENODE_KEYTAB_FILE_KEY,
    DFS_NAMENODE_BACKUP_ADDRESS_KEY,
    DFS_NAMENODE_BACKUP_HTTP_ADDRESS_KEY,
    DFS_NAMENODE_BACKUP_SERVICE_RPC_ADDRESS_KEY,
    DFS_NAMENODE_USER_NAME_KEY,
    DFS_HA_FENCE_METHODS_KEY,
    DFS_HA_ZKFC_PORT_KEY,
    DFS_HA_FENCE_METHODS_KEY
  };
  
  /**
   * @see #NAMENODE_SPECIFIC_KEYS
   * These keys are specific to a nameservice, but may not be overridden
   * for a specific namenode.
   */
  public static final String[] NAMESERVICE_SPECIFIC_KEYS = {
    DFS_HA_AUTO_FAILOVER_ENABLED_KEY
  };
  
  private static final String USAGE = "Usage: java NameNode ["
      + StartupOption.BACKUP.getName() + "] | ["
      + StartupOption.CHECKPOINT.getName() + "] | ["
      + StartupOption.FORMAT.getName() + " ["
      + StartupOption.CLUSTERID.getName() + " cid ] ["
      + StartupOption.FORCE.getName() + "] ["
      + StartupOption.NONINTERACTIVE.getName() + "] ] | ["
      + StartupOption.UPGRADE.getName() + "] | ["
      + StartupOption.ROLLBACK.getName() + "] | ["
      + StartupOption.FINALIZE.getName() + "] | ["
      + StartupOption.IMPORT.getName() + "] | ["
      + StartupOption.INITIALIZESHAREDEDITS.getName() + "] | ["
      + StartupOption.BOOTSTRAPSTANDBY.getName() + "] | ["
      + StartupOption.RECOVER.getName() + " [ " + StartupOption.FORCE.getName()
      + " ] ]";
  
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
    
  public static final int DEFAULT_PORT = 8020;
  public static final Log LOG = LogFactory.getLog(NameNode.class.getName());
  public static final Log stateChangeLog = LogFactory.getLog("org.apache.hadoop.hdfs.StateChange");
  public static final Log blockStateChangeLog = LogFactory.getLog("BlockStateChange");
  public static final HAState ACTIVE_STATE = new ActiveState();
  public static final HAState STANDBY_STATE = new StandbyState();
  
  protected FSNamesystem namesystem; 
  protected final Configuration conf;
  protected NamenodeRole role;
  private volatile HAState state;
  private final boolean haEnabled;
  private final HAContext haContext;
  protected boolean allowStaleStandbyReads;

  
  /** httpServer */
  protected NameNodeHttpServer httpServer;
  private Thread emptier;
  /** only used for testing purposes  */
  protected boolean stopRequested = false;
  /** Registration information of this name-node  */
  protected NamenodeRegistration nodeRegistration;
  /** Activated plug-ins. */
  private List<ServicePlugin> plugins;
  
  private NameNodeRpcServer rpcServer;
  
  /** Format a new filesystem.  Destroys any filesystem that may already
   * exist at this location.  **/
  public static void format(Configuration conf) throws IOException {
    format(conf, true, true);
  }

  static NameNodeMetrics metrics;

  /** Return the {@link FSNamesystem} object.
   * @return {@link FSNamesystem} object.
   */
  public FSNamesystem getNamesystem() {
    return namesystem;
  }

  public NamenodeProtocols getRpcServer() {
    return rpcServer;
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
    conf.set(DFS_NAMENODE_SERVICE_RPC_ADDRESS_KEY, address);
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
    String addr = conf.get(DFS_NAMENODE_SERVICE_RPC_ADDRESS_KEY);
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
    if (!HdfsConstants.HDFS_URI_SCHEME.equalsIgnoreCase(
        filesystemURI.getScheme())) {
      throw new IllegalArgumentException(String.format(
          "Invalid URI for NameNode address (check %s): %s is not of scheme '%s'.",
          FileSystem.FS_DEFAULT_NAME_KEY, filesystemURI.toString(),
          HdfsConstants.HDFS_URI_SCHEME));
    }
    return getAddress(authority);
  }

  public static URI getUri(InetSocketAddress namenode) {
    int port = namenode.getPort();
    String portString = port == DEFAULT_PORT ? "" : (":"+port);
    return URI.create(HdfsConstants.HDFS_URI_SCHEME + "://" 
        + namenode.getHostName()+portString);
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
  protected InetSocketAddress getServiceRpcServerAddress(Configuration conf) {
    return NameNode.getServiceAddress(conf, false);
  }

  protected InetSocketAddress getRpcServerAddress(Configuration conf) {
    return getAddress(conf);
  }
  
  /**
   * Modifies the configuration passed to contain the service rpc address setting
   */
  protected void setRpcServiceServerAddress(Configuration conf,
      InetSocketAddress serviceRPCAddress) {
    setServiceAddress(conf, NetUtils.getHostPortString(serviceRPCAddress));
  }

  protected void setRpcServerAddress(Configuration conf,
      InetSocketAddress rpcAddress) {
    FileSystem.setDefaultUri(conf, getUri(rpcAddress));
  }

  protected InetSocketAddress getHttpServerAddress(Configuration conf) {
    return getHttpAddress(conf);
  }

  /** @return the NameNode HTTP address set in the conf. */
  public static InetSocketAddress getHttpAddress(Configuration conf) {
    return  NetUtils.createSocketAddr(
        conf.get(DFS_NAMENODE_HTTP_ADDRESS_KEY, DFS_NAMENODE_HTTP_ADDRESS_DEFAULT));
  }
  
  protected void setHttpServerAddress(Configuration conf) {
    String hostPort = NetUtils.getHostPortString(getHttpAddress());
    conf.set(DFS_NAMENODE_HTTP_ADDRESS_KEY, hostPort);
    LOG.info("Web-server up at: " + hostPort);
  }

  protected void loadNamesystem(Configuration conf) throws IOException {
    this.namesystem = FSNamesystem.loadFromDisk(conf);
  }

  NamenodeRegistration getRegistration() {
    return nodeRegistration;
  }

  NamenodeRegistration setRegistration() {
    nodeRegistration = new NamenodeRegistration(
        NetUtils.getHostPortString(rpcServer.getRpcAddress()),
        NetUtils.getHostPortString(getHttpAddress()),
        getFSImage().getStorage(), getRole());
    return nodeRegistration;
  }

  /**
   * Login as the configured user for the NameNode.
   */
  void loginAsNameNodeUser(Configuration conf) throws IOException {
    InetSocketAddress socAddr = getRpcServerAddress(conf);
    SecurityUtil.login(conf, DFS_NAMENODE_KEYTAB_FILE_KEY,
        DFS_NAMENODE_USER_NAME_KEY, socAddr.getHostName());
  }
  
  /**
   * Initialize name-node.
   * 
   * @param conf the configuration
   */
  protected void initialize(Configuration conf) throws IOException {
    UserGroupInformation.setConfiguration(conf);
    loginAsNameNodeUser(conf);

    NameNode.initMetrics(conf, this.getRole());
    loadNamesystem(conf);

    rpcServer = createRpcServer(conf);
    
    try {
      validateConfigurationSettings(conf);
    } catch (IOException e) {
      LOG.fatal(e.toString());
      throw e;
    }

    startCommonServices(conf);
  }
  
  /**
   * Create the RPC server implementation. Used as an extension point for the
   * BackupNode.
   */
  protected NameNodeRpcServer createRpcServer(Configuration conf)
      throws IOException {
    return new NameNodeRpcServer(conf, this);
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

  /** Start the services common to active and standby states */
  private void startCommonServices(Configuration conf) throws IOException {
    namesystem.startCommonServices(conf, haContext);
    startHttpServer(conf);
    rpcServer.start();
    plugins = conf.getInstances(DFS_NAMENODE_PLUGINS_KEY,
        ServicePlugin.class);
    for (ServicePlugin p: plugins) {
      try {
        p.start(this);
      } catch (Throwable t) {
        LOG.warn("ServicePlugin " + p + " could not be started", t);
      }
    }
    LOG.info(getRole() + " RPC up at: " + rpcServer.getRpcAddress());
    if (rpcServer.getServiceRpcAddress() != null) {
      LOG.info(getRole() + " service RPC up at: "
          + rpcServer.getServiceRpcAddress());
    }
  }
  
  private void stopCommonServices() {
    if(namesystem != null) namesystem.close();
    if(rpcServer != null) rpcServer.stop();
    if (plugins != null) {
      for (ServicePlugin p : plugins) {
        try {
          p.stop();
        } catch (Throwable t) {
          LOG.warn("ServicePlugin " + p + " could not be stopped", t);
        }
      }
    }   
    stopHttpServer();
  }
  
  private void startTrashEmptier(final Configuration conf) throws IOException {
    long trashInterval =
        conf.getLong(FS_TRASH_INTERVAL_KEY, FS_TRASH_INTERVAL_DEFAULT);
    if (trashInterval == 0) {
      return;
    } else if (trashInterval < 0) {
      throw new IOException("Cannot start tresh emptier with negative interval."
          + " Set " + FS_TRASH_INTERVAL_KEY + " to a positive value.");
    }
    
    // This may be called from the transitionToActive code path, in which
    // case the current user is the administrator, not the NN. The trash
    // emptier needs to run as the NN. See HDFS-3972.
    FileSystem fs = SecurityUtil.doAsLoginUser(
        new PrivilegedExceptionAction<FileSystem>() {
          @Override
          public FileSystem run() throws IOException {
            return FileSystem.get(conf);
          }
        });
    this.emptier = new Thread(new Trash(fs, conf).getEmptier(), "Trash Emptier");
    this.emptier.setDaemon(true);
    this.emptier.start();
  }
  
  private void stopTrashEmptier() {
    if (this.emptier != null) {
      emptier.interrupt();
      emptier = null;
    }
  }
  
  private void startHttpServer(final Configuration conf) throws IOException {
    httpServer = new NameNodeHttpServer(conf, this, getHttpServerAddress(conf));
    httpServer.start();
    setHttpServerAddress(conf);
  }
  
  private void stopHttpServer() {
    try {
      if (httpServer != null) httpServer.stop();
    } catch (Exception e) {
      LOG.error("Exception while stopping httpserver", e);
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
   * <li>{@link StartupOption#RECOVERY RECOVERY} - recover name node
   * metadata</li>
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
    this.conf = conf;
    this.role = role;
    String nsId = getNameServiceId(conf);
    String namenodeId = HAUtil.getNameNodeId(conf, nsId);
    this.haEnabled = HAUtil.isHAEnabled(conf, nsId);
    state = createHAState();
    this.allowStaleStandbyReads = HAUtil.shouldAllowStandbyReads(conf);
    this.haContext = createHAContext();
    try {
      initializeGenericKeys(conf, nsId, namenodeId);
      initialize(conf);
      state.prepareToEnterState(haContext);
      state.enterState(haContext);
    } catch (IOException e) {
      this.stop();
      throw e;
    } catch (HadoopIllegalArgumentException e) {
      this.stop();
      throw e;
    }
  }

  protected HAState createHAState() {
    return !haEnabled ? ACTIVE_STATE : STANDBY_STATE;
  }

  protected HAContext createHAContext() {
    return new NameNodeHAContext();
  }

  /**
   * Wait for service to finish.
   * (Normally, it runs forever.)
   */
  public void join() {
    try {
      rpcServer.join();
    } catch (InterruptedException ie) {
      LOG.info("Caught interrupted exception ", ie);
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
    try {
      if (state != null) {
        state.exitState(haContext);
      }
    } catch (ServiceFailedException e) {
      LOG.warn("Encountered exception while exiting state ", e);
    }
    stopCommonServices();
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

  /**
   * Is the cluster currently in safe mode?
   */
  public boolean isInSafeMode() {
    return namesystem.isInSafeMode();
  }
    
  /** get FSImage */
  FSImage getFSImage() {
    return namesystem.dir.fsImage;
  }

  /**
   * @return NameNode RPC address
   */
  public InetSocketAddress getNameNodeAddress() {
    return rpcServer.getRpcAddress();
  }

  /**
   * @return NameNode RPC address in "host:port" string form
   */
  public String getNameNodeAddressHostPortString() {
    return NetUtils.getHostPortString(rpcServer.getRpcAddress());
  }

  /**
   * @return NameNode service RPC address if configured, the
   *    NameNode RPC address otherwise
   */
  public InetSocketAddress getServiceRpcAddress() {
    final InetSocketAddress serviceAddr = rpcServer.getServiceRpcAddress();
    return serviceAddr == null ? rpcServer.getRpcAddress() : serviceAddr;
  }

  /**
   * @return NameNode HTTP address, used by the Web UI, image transfer,
   *    and HTTP-based file system clients like Hftp and WebHDFS
   */
  public InetSocketAddress getHttpAddress() {
    return httpServer.getHttpAddress();
  }

  /**
   * Verify that configured directories exist, then
   * Interactively confirm that formatting is desired 
   * for each existing directory and format them.
   * 
   * @param conf
   * @param force
   * @return true if formatting was aborted, false otherwise
   * @throws IOException
   */
  private static boolean format(Configuration conf, boolean force,
      boolean isInteractive) throws IOException {
    String nsId = DFSUtil.getNamenodeNameServiceId(conf);
    String namenodeId = HAUtil.getNameNodeId(conf, nsId);
    initializeGenericKeys(conf, nsId, namenodeId);
    checkAllowFormat(conf);

    if (UserGroupInformation.isSecurityEnabled()) {
      InetSocketAddress socAddr = getAddress(conf);
      SecurityUtil.login(conf, DFS_NAMENODE_KEYTAB_FILE_KEY,
          DFS_NAMENODE_USER_NAME_KEY, socAddr.getHostName());
    }
    
    Collection<URI> nameDirsToFormat = FSNamesystem.getNamespaceDirs(conf);
    List<URI> sharedDirs = FSNamesystem.getSharedEditsDirs(conf);
    List<URI> dirsToPrompt = new ArrayList<URI>();
    dirsToPrompt.addAll(nameDirsToFormat);
    dirsToPrompt.addAll(sharedDirs);
    List<URI> editDirsToFormat = 
                 FSNamesystem.getNamespaceEditsDirs(conf);

    // if clusterID is not provided - see if you can find the current one
    String clusterId = StartupOption.FORMAT.getClusterId();
    if(clusterId == null || clusterId.equals("")) {
      //Generate a new cluster id
      clusterId = NNStorage.newClusterID();
    }
    System.out.println("Formatting using clusterid: " + clusterId);
    
    FSImage fsImage = new FSImage(conf, nameDirsToFormat, editDirsToFormat);
    FSNamesystem fsn = new FSNamesystem(conf, fsImage);
    fsImage.getEditLog().initJournalsForWrite();
    
    if (!fsImage.confirmFormat(force, isInteractive)) {
      return true; // aborted
    }
    
    fsImage.format(fsn, clusterId);
    return false;
  }

  public static void checkAllowFormat(Configuration conf) throws IOException {
    if (!conf.getBoolean(DFS_NAMENODE_SUPPORT_ALLOW_FORMAT_KEY, 
        DFS_NAMENODE_SUPPORT_ALLOW_FORMAT_DEFAULT)) {
      throw new IOException("The option " + DFS_NAMENODE_SUPPORT_ALLOW_FORMAT_KEY
                + " is set to false for this filesystem, so it "
                + "cannot be formatted. You will need to set "
                + DFS_NAMENODE_SUPPORT_ALLOW_FORMAT_KEY +" parameter "
                + "to true in order to format this filesystem");
    }
  }
  
  @VisibleForTesting
  public static boolean initializeSharedEdits(Configuration conf) throws IOException {
    return initializeSharedEdits(conf, true);
  }
  
  @VisibleForTesting
  public static boolean initializeSharedEdits(Configuration conf,
      boolean force) throws IOException {
    return initializeSharedEdits(conf, force, false);
  }

  /**
   * Clone the supplied configuration but remove the shared edits dirs.
   *
   * @param conf Supplies the original configuration.
   * @return Cloned configuration without the shared edit dirs.
   * @throws IOException on failure to generate the configuration.
   */
  private static Configuration getConfigurationWithoutSharedEdits(
      Configuration conf)
      throws IOException {
    List<URI> editsDirs = FSNamesystem.getNamespaceEditsDirs(conf, false);
    String editsDirsString = Joiner.on(",").join(editsDirs);

    Configuration confWithoutShared = new Configuration(conf);
    confWithoutShared.unset(DFSConfigKeys.DFS_NAMENODE_SHARED_EDITS_DIR_KEY);
    confWithoutShared.setStrings(DFSConfigKeys.DFS_NAMENODE_EDITS_DIR_KEY,
        editsDirsString);
    return confWithoutShared;
  }

  /**
   * Format a new shared edits dir and copy in enough edit log segments so that
   * the standby NN can start up.
   * 
   * @param conf configuration
   * @param force format regardless of whether or not the shared edits dir exists
   * @param interactive prompt the user when a dir exists
   * @return true if the command aborts, false otherwise
   */
  private static boolean initializeSharedEdits(Configuration conf,
      boolean force, boolean interactive) throws IOException {
    String nsId = DFSUtil.getNamenodeNameServiceId(conf);
    String namenodeId = HAUtil.getNameNodeId(conf, nsId);
    initializeGenericKeys(conf, nsId, namenodeId);
    
    if (conf.get(DFSConfigKeys.DFS_NAMENODE_SHARED_EDITS_DIR_KEY) == null) {
      LOG.fatal("No shared edits directory configured for namespace " +
          nsId + " namenode " + namenodeId);
      return false;
    }

    if (UserGroupInformation.isSecurityEnabled()) {
      InetSocketAddress socAddr = getAddress(conf);
      SecurityUtil.login(conf, DFS_NAMENODE_KEYTAB_FILE_KEY,
          DFS_NAMENODE_USER_NAME_KEY, socAddr.getHostName());
    }

    NNStorage existingStorage = null;
    try {
      FSNamesystem fsns =
          FSNamesystem.loadFromDisk(getConfigurationWithoutSharedEdits(conf));
      
      existingStorage = fsns.getFSImage().getStorage();
      NamespaceInfo nsInfo = existingStorage.getNamespaceInfo();
      
      List<URI> sharedEditsDirs = FSNamesystem.getSharedEditsDirs(conf);
      
      FSImage sharedEditsImage = new FSImage(conf,
          Lists.<URI>newArrayList(),
          sharedEditsDirs);
      sharedEditsImage.getEditLog().initJournalsForWrite();
      
      if (!sharedEditsImage.confirmFormat(force, interactive)) {
        return true; // abort
      }
      
      NNStorage newSharedStorage = sharedEditsImage.getStorage();
      // Call Storage.format instead of FSImage.format here, since we don't
      // actually want to save a checkpoint - just prime the dirs with
      // the existing namespace info
      newSharedStorage.format(nsInfo);
      sharedEditsImage.getEditLog().formatNonFileJournals(nsInfo);

      // Need to make sure the edit log segments are in good shape to initialize
      // the shared edits dir.
      fsns.getFSImage().getEditLog().close();
      fsns.getFSImage().getEditLog().initJournalsForWrite();
      fsns.getFSImage().getEditLog().recoverUnclosedStreams();

      copyEditLogSegmentsToSharedDir(fsns, sharedEditsDirs, newSharedStorage,
          conf);
    } catch (IOException ioe) {
      LOG.error("Could not initialize shared edits dir", ioe);
      return true; // aborted
    } finally {
      // Have to unlock storage explicitly for the case when we're running in a
      // unit test, which runs in the same JVM as NNs.
      if (existingStorage != null) {
        try {
          existingStorage.unlockAll();
        } catch (IOException ioe) {
          LOG.warn("Could not unlock storage directories", ioe);
          return true; // aborted
        }
      }
    }
    return false; // did not abort
  }

  private static void copyEditLogSegmentsToSharedDir(FSNamesystem fsns,
      Collection<URI> sharedEditsDirs, NNStorage newSharedStorage,
      Configuration conf) throws IOException {
    Preconditions.checkArgument(!sharedEditsDirs.isEmpty(),
        "No shared edits specified");
    // Copy edit log segments into the new shared edits dir.
    List<URI> sharedEditsUris = new ArrayList<URI>(sharedEditsDirs);
    FSEditLog newSharedEditLog = new FSEditLog(conf, newSharedStorage,
        sharedEditsUris);
    newSharedEditLog.initJournalsForWrite();
    newSharedEditLog.recoverUnclosedStreams();
    
    FSEditLog sourceEditLog = fsns.getFSImage().editLog;
    
    long fromTxId = fsns.getFSImage().getMostRecentCheckpointTxId();
    Collection<EditLogInputStream> streams = sourceEditLog.selectInputStreams(
        fromTxId+1, 0);

    // Set the nextTxid to the CheckpointTxId+1
    newSharedEditLog.setNextTxId(fromTxId + 1);
    
    // Copy all edits after last CheckpointTxId to shared edits dir
    for (EditLogInputStream stream : streams) {
      LOG.debug("Beginning to copy stream " + stream + " to shared edits");
      FSEditLogOp op;
      boolean segmentOpen = false;
      while ((op = stream.readOp()) != null) {
        if (LOG.isTraceEnabled()) {
          LOG.trace("copying op: " + op);
        }
        if (!segmentOpen) {
          newSharedEditLog.startLogSegment(op.txid, false);
          segmentOpen = true;
        }
        
        newSharedEditLog.logEdit(op);

        if (op.opCode == FSEditLogOpCodes.OP_END_LOG_SEGMENT) {
          newSharedEditLog.logSync();
          newSharedEditLog.endCurrentLogSegment(false);
          LOG.debug("ending log segment because of END_LOG_SEGMENT op in " + stream);
          segmentOpen = false;
        }
      }
      
      if (segmentOpen) {
        LOG.debug("ending log segment because of end of stream in " + stream);
        newSharedEditLog.logSync();
        newSharedEditLog.endCurrentLogSegment(false);
        segmentOpen = false;
      }
    }
  }

  private static boolean finalize(Configuration conf,
                               boolean isConfirmationNeeded
                               ) throws IOException {
    String nsId = DFSUtil.getNamenodeNameServiceId(conf);
    String namenodeId = HAUtil.getNameNodeId(conf, nsId);
    initializeGenericKeys(conf, nsId, namenodeId);

    FSNamesystem nsys = new FSNamesystem(conf, new FSImage(conf));
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

  private static void printUsage(PrintStream out) {
    out.println(USAGE + "\n");
  }

  private static StartupOption parseArguments(String args[]) {
    int argsLen = (args == null) ? 0 : args.length;
    StartupOption startOpt = StartupOption.REGULAR;
    for(int i=0; i < argsLen; i++) {
      String cmd = args[i];
      if (StartupOption.FORMAT.getName().equalsIgnoreCase(cmd)) {
        startOpt = StartupOption.FORMAT;
        for (i = i + 1; i < argsLen; i++) {
          if (args[i].equalsIgnoreCase(StartupOption.CLUSTERID.getName())) {
            i++;
            if (i >= argsLen) {
              // if no cluster id specified, return null
              LOG.fatal("Must specify a valid cluster ID after the "
                  + StartupOption.CLUSTERID.getName() + " flag");
              return null;
            }
            String clusterId = args[i];
            // Make sure an id is specified and not another flag
            if (clusterId.isEmpty() ||
                clusterId.equalsIgnoreCase(StartupOption.FORCE.getName()) ||
                clusterId.equalsIgnoreCase(
                    StartupOption.NONINTERACTIVE.getName())) {
              LOG.fatal("Must specify a valid cluster ID after the "
                  + StartupOption.CLUSTERID.getName() + " flag");
              return null;
            }
            startOpt.setClusterId(clusterId);
          }

          if (args[i].equalsIgnoreCase(StartupOption.FORCE.getName())) {
            startOpt.setForceFormat(true);
          }

          if (args[i].equalsIgnoreCase(StartupOption.NONINTERACTIVE.getName())) {
            startOpt.setInteractiveFormat(false);
          }
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
      } else if (StartupOption.BOOTSTRAPSTANDBY.getName().equalsIgnoreCase(cmd)) {
        startOpt = StartupOption.BOOTSTRAPSTANDBY;
        return startOpt;
      } else if (StartupOption.INITIALIZESHAREDEDITS.getName().equalsIgnoreCase(cmd)) {
        startOpt = StartupOption.INITIALIZESHAREDEDITS;
        for (i = i + 1 ; i < argsLen; i++) {
          if (StartupOption.NONINTERACTIVE.getName().equals(args[i])) {
            startOpt.setInteractiveFormat(false);
          } else if (StartupOption.FORCE.getName().equals(args[i])) {
            startOpt.setForceFormat(true);
          } else {
            LOG.fatal("Invalid argument: " + args[i]);
            return null;
          }
        }
        return startOpt;
      } else if (StartupOption.RECOVER.getName().equalsIgnoreCase(cmd)) {
        if (startOpt != StartupOption.REGULAR) {
          throw new RuntimeException("Can't combine -recover with " +
              "other startup options.");
        }
        startOpt = StartupOption.RECOVER;
        while (++i < argsLen) {
          if (args[i].equalsIgnoreCase(
                StartupOption.FORCE.getName())) {
            startOpt.setForce(MetaRecoveryContext.FORCE_FIRST_CHOICE);
          } else {
            throw new RuntimeException("Error parsing recovery options: " + 
              "can't understand option \"" + args[i] + "\"");
          }
        }
      } else {
        return null;
      }
    }
    return startOpt;
  }

  private static void setStartupOption(Configuration conf, StartupOption opt) {
    conf.set(DFS_NAMENODE_STARTUP_KEY, opt.toString());
  }

  static StartupOption getStartupOption(Configuration conf) {
    return StartupOption.valueOf(conf.get(DFS_NAMENODE_STARTUP_KEY,
                                          StartupOption.REGULAR.toString()));
  }

  private static void doRecovery(StartupOption startOpt, Configuration conf)
      throws IOException {
    String nsId = DFSUtil.getNamenodeNameServiceId(conf);
    String namenodeId = HAUtil.getNameNodeId(conf, nsId);
    initializeGenericKeys(conf, nsId, namenodeId);
    if (startOpt.getForce() < MetaRecoveryContext.FORCE_ALL) {
      if (!confirmPrompt("You have selected Metadata Recovery mode.  " +
          "This mode is intended to recover lost metadata on a corrupt " +
          "filesystem.  Metadata recovery mode often permanently deletes " +
          "data from your HDFS filesystem.  Please back up your edit log " +
          "and fsimage before trying this!\n\n" +
          "Are you ready to proceed? (Y/N)\n")) {
        System.err.println("Recovery aborted at user request.\n");
        return;
      }
    }
    MetaRecoveryContext.LOG.info("starting recovery...");
    UserGroupInformation.setConfiguration(conf);
    NameNode.initMetrics(conf, startOpt.toNodeRole());
    FSNamesystem fsn = null;
    try {
      fsn = FSNamesystem.loadFromDisk(conf);
      fsn.saveNamespace();
      MetaRecoveryContext.LOG.info("RECOVERY COMPLETE");
    } catch (IOException e) {
      MetaRecoveryContext.LOG.info("RECOVERY FAILED: caught exception", e);
      throw e;
    } catch (RuntimeException e) {
      MetaRecoveryContext.LOG.info("RECOVERY FAILED: caught exception", e);
      throw e;
    } finally {
      if (fsn != null)
        fsn.close();
    }
  }

  public static NameNode createNameNode(String argv[], Configuration conf)
      throws IOException {
    if (conf == null)
      conf = new HdfsConfiguration();
    StartupOption startOpt = parseArguments(argv);
    if (startOpt == null) {
      printUsage(System.err);
      return null;
    }
    setStartupOption(conf, startOpt);
    
    if (HAUtil.isHAEnabled(conf, DFSUtil.getNamenodeNameServiceId(conf)) &&
        (startOpt == StartupOption.UPGRADE ||
         startOpt == StartupOption.ROLLBACK ||
         startOpt == StartupOption.FINALIZE)) {
      throw new HadoopIllegalArgumentException("Invalid startup option. " +
          "Cannot perform DFS upgrade with HA enabled.");
    }

    switch (startOpt) {
      case FORMAT: {
        boolean aborted = format(conf, startOpt.getForceFormat(),
            startOpt.getInteractiveFormat());
        terminate(aborted ? 1 : 0);
        return null; // avoid javac warning
      }
      case GENCLUSTERID: {
        System.err.println("Generating new cluster id:");
        System.out.println(NNStorage.newClusterID());
        terminate(0);
        return null;
      }
      case FINALIZE: {
        boolean aborted = finalize(conf, true);
        terminate(aborted ? 1 : 0);
        return null; // avoid javac warning
      }
      case BOOTSTRAPSTANDBY: {
        String toolArgs[] = Arrays.copyOfRange(argv, 1, argv.length);
        int rc = BootstrapStandby.run(toolArgs, conf);
        terminate(rc);
        return null; // avoid warning
      }
      case INITIALIZESHAREDEDITS: {
        boolean aborted = initializeSharedEdits(conf,
            startOpt.getForceFormat(),
            startOpt.getInteractiveFormat());
        terminate(aborted ? 1 : 0);
        return null; // avoid warning
      }
      case BACKUP:
      case CHECKPOINT: {
        NamenodeRole role = startOpt.toNodeRole();
        DefaultMetricsSystem.initialize(role.toString().replace(" ", ""));
        return new BackupNode(conf, role);
      }
      case RECOVER: {
        NameNode.doRecovery(startOpt, conf);
        return null;
      }
      default: {
        DefaultMetricsSystem.initialize("NameNode");
        return new NameNode(conf);
      }
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
   * @param nameserviceId name service Id (to distinguish federated NNs)
   * @param namenodeId the namenode ID (to distinguish HA NNs)
   * @see DFSUtil#setGenericConf(Configuration, String, String, String...)
   */
  public static void initializeGenericKeys(Configuration conf,
      String nameserviceId, String namenodeId) {
    if ((nameserviceId != null && !nameserviceId.isEmpty()) || 
        (namenodeId != null && !namenodeId.isEmpty())) {
      if (nameserviceId != null) {
        conf.set(DFS_NAMESERVICE_ID, nameserviceId);
      }
      if (namenodeId != null) {
        conf.set(DFS_HA_NAMENODE_ID_KEY, namenodeId);
      }
      
      DFSUtil.setGenericConf(conf, nameserviceId, namenodeId,
          NAMENODE_SPECIFIC_KEYS);
      DFSUtil.setGenericConf(conf, nameserviceId, null,
          NAMESERVICE_SPECIFIC_KEYS);
    }
    
    // If the RPC address is set use it to (re-)configure the default FS
    if (conf.get(DFS_NAMENODE_RPC_ADDRESS_KEY) != null) {
      URI defaultUri = URI.create(HdfsConstants.HDFS_URI_SCHEME + "://"
          + conf.get(DFS_NAMENODE_RPC_ADDRESS_KEY));
      conf.set(FS_DEFAULT_NAME_KEY, defaultUri.toString());
      LOG.debug("Setting " + FS_DEFAULT_NAME_KEY + " to " + defaultUri.toString());
    }
  }
    
  /** 
   * Get the name service Id for the node
   * @return name service Id or null if federation is not configured
   */
  protected String getNameServiceId(Configuration conf) {
    return DFSUtil.getNamenodeNameServiceId(conf);
  }
  
  /**
   */
  public static void main(String argv[]) throws Exception {
    if (DFSUtil.parseHelpArgument(argv, NameNode.USAGE, System.out, true)) {
      System.exit(0);
    }

    try {
      StringUtils.startupShutdownMessage(NameNode.class, argv, LOG);
      NameNode namenode = createNameNode(argv, null);
      if (namenode != null) {
        namenode.join();
      }
    } catch (Throwable e) {
      LOG.fatal("Exception in namenode join", e);
      terminate(1, e);
    }
  }

  synchronized void monitorHealth() 
      throws HealthCheckFailedException, AccessControlException {
    namesystem.checkSuperuserPrivilege();
    if (!haEnabled) {
      return; // no-op, if HA is not enabled
    }
    getNamesystem().checkAvailableResources();
    if (!getNamesystem().nameNodeHasResourcesAvailable()) {
      throw new HealthCheckFailedException(
          "The NameNode has no resources available");
    }
  }
  
  synchronized void transitionToActive() 
      throws ServiceFailedException, AccessControlException {
    namesystem.checkSuperuserPrivilege();
    if (!haEnabled) {
      throw new ServiceFailedException("HA for namenode is not enabled");
    }
    state.setState(haContext, ACTIVE_STATE);
  }
  
  synchronized void transitionToStandby() 
      throws ServiceFailedException, AccessControlException {
    namesystem.checkSuperuserPrivilege();
    if (!haEnabled) {
      throw new ServiceFailedException("HA for namenode is not enabled");
    }
    state.setState(haContext, STANDBY_STATE);
  }

  synchronized HAServiceStatus getServiceStatus()
      throws ServiceFailedException, AccessControlException {
    namesystem.checkSuperuserPrivilege();
    if (!haEnabled) {
      throw new ServiceFailedException("HA for namenode is not enabled");
    }
    if (state == null) {
      return new HAServiceStatus(HAServiceState.INITIALIZING);
    }
    HAServiceState retState = state.getServiceState();
    HAServiceStatus ret = new HAServiceStatus(retState);
    if (retState == HAServiceState.STANDBY) {
      String safemodeTip = namesystem.getSafeModeTip();
      if (!safemodeTip.isEmpty()) {
        ret.setNotReadyToBecomeActive(
            "The NameNode is in safemode. " +
            safemodeTip);
      } else {
        ret.setReadyToBecomeActive();
      }
    } else if (retState == HAServiceState.ACTIVE) {
      ret.setReadyToBecomeActive();
    } else {
      ret.setNotReadyToBecomeActive("State is " + state);
    }
    return ret;
  }

  synchronized HAServiceState getServiceState() {
    if (state == null) {
      return HAServiceState.INITIALIZING;
    }
    return state.getServiceState();
  }

  /**
   * Shutdown the NN immediately in an ungraceful way. Used when it would be
   * unsafe for the NN to continue operating, e.g. during a failed HA state
   * transition.
   * 
   * @param t exception which warrants the shutdown. Printed to the NN log
   *          before exit.
   * @throws ExitException thrown only for testing.
   */
  protected synchronized void doImmediateShutdown(Throwable t)
      throws ExitException {
    String message = "Error encountered requiring NN shutdown. " +
        "Shutting down immediately.";
    try {
      LOG.fatal(message, t);
    } catch (Throwable ignored) {
      // This is unlikely to happen, but there's nothing we can do if it does.
    }
    terminate(1, t);
  }
  
  /**
   * Class used to expose {@link NameNode} as context to {@link HAState}
   */
  protected class NameNodeHAContext implements HAContext {
    @Override
    public void setState(HAState s) {
      state = s;
    }

    @Override
    public HAState getState() {
      return state;
    }

    @Override
    public void startActiveServices() throws IOException {
      try {
        namesystem.startActiveServices();
        startTrashEmptier(conf);
      } catch (Throwable t) {
        doImmediateShutdown(t);
      }
    }

    @Override
    public void stopActiveServices() throws IOException {
      try {
        if (namesystem != null) {
          namesystem.stopActiveServices();
        }
        stopTrashEmptier();
      } catch (Throwable t) {
        doImmediateShutdown(t);
      }
    }

    @Override
    public void startStandbyServices() throws IOException {
      try {
        namesystem.startStandbyServices(conf);
      } catch (Throwable t) {
        doImmediateShutdown(t);
      }
    }

    @Override
    public void prepareToStopStandbyServices() throws ServiceFailedException {
      try {
        namesystem.prepareToStopStandbyServices();
      } catch (Throwable t) {
        doImmediateShutdown(t);
      }
    }
    
    @Override
    public void stopStandbyServices() throws IOException {
      try {
        if (namesystem != null) {
          namesystem.stopStandbyServices();
        }
      } catch (Throwable t) {
        doImmediateShutdown(t);
      }
    }
    
    @Override
    public void writeLock() {
      namesystem.writeLock();
    }
    
    @Override
    public void writeUnlock() {
      namesystem.writeUnlock();
    }
    
    /** Check if an operation of given category is allowed */
    @Override
    public void checkOperation(final OperationCategory op)
        throws StandbyException {
      state.checkOperation(haContext, op);
    }
    
    @Override
    public boolean allowStaleReads() {
      return allowStaleStandbyReads;
    }

  }
  
  public boolean isStandbyState() {
    return (state.equals(STANDBY_STATE));
  }

  /**
   * Check that a request to change this node's HA state is valid.
   * In particular, verifies that, if auto failover is enabled, non-forced
   * requests from the HAAdmin CLI are rejected, and vice versa.
   *
   * @param req the request to check
   * @throws AccessControlException if the request is disallowed
   */
  void checkHaStateChange(StateChangeRequestInfo req)
      throws AccessControlException {
    boolean autoHaEnabled = conf.getBoolean(DFS_HA_AUTO_FAILOVER_ENABLED_KEY,
        DFS_HA_AUTO_FAILOVER_ENABLED_DEFAULT);
    switch (req.getSource()) {
    case REQUEST_BY_USER:
      if (autoHaEnabled) {
        throw new AccessControlException(
            "Manual HA control for this NameNode is disallowed, because " +
            "automatic HA is enabled.");
      }
      break;
    case REQUEST_BY_USER_FORCED:
      if (autoHaEnabled) {
        LOG.warn("Allowing manual HA control from " +
            Server.getRemoteAddress() +
            " even though automatic HA is enabled, because the user " +
            "specified the force flag");
      }
      break;
    case REQUEST_BY_ZKFC:
      if (!autoHaEnabled) {
        throw new AccessControlException(
            "Request from ZK failover controller at " +
            Server.getRemoteAddress() + " denied since automatic HA " +
            "is not enabled"); 
      }
      break;
    }
  }
}
