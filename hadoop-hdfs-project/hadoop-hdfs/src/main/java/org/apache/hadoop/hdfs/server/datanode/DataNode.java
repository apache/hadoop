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
package org.apache.hadoop.hdfs.server.datanode;


import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_ADMIN;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_CLIENT_HTTPS_NEED_AUTH_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_CLIENT_HTTPS_NEED_AUTH_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_ADDRESS_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_ADDRESS_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_DATA_DIR_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_DATA_DIR_PERMISSION_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_DATA_DIR_PERMISSION_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_DIRECTORYSCAN_INTERVAL_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_DIRECTORYSCAN_INTERVAL_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_DNS_INTERFACE_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_DNS_INTERFACE_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_DNS_NAMESERVER_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_DNS_NAMESERVER_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_HANDLER_COUNT_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_HANDLER_COUNT_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_HOST_NAME_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_HTTP_ADDRESS_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_HTTP_ADDRESS_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_IPC_ADDRESS_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_KEYTAB_FILE_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_PLUGINS_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_SCAN_PERIOD_HOURS_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_SCAN_PERIOD_HOURS_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_STARTUP_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_USER_NAME_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_FEDERATION_NAMESERVICES;

import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.URI;
import java.net.UnknownHostException;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.security.PrivilegedExceptionAction;
import java.util.AbstractList;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.HDFSPolicyProvider;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.BlockLocalPathInfo;
import org.apache.hadoop.hdfs.protocol.ClientDatanodeProtocol;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.HdfsProtoUtil;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.RecoveryInProgressException;
import org.apache.hadoop.hdfs.protocol.datatransfer.BlockConstructionStage;
import org.apache.hadoop.hdfs.protocol.datatransfer.DataTransferProtocol;
import org.apache.hadoop.hdfs.protocol.datatransfer.Sender;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.DNTransferAckProto;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.Status;
import org.apache.hadoop.hdfs.security.token.block.BlockPoolTokenSecretManager;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenIdentifier;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenSecretManager;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenSecretManager.AccessMode;
import org.apache.hadoop.hdfs.security.token.block.ExportedBlockKeys;
import org.apache.hadoop.hdfs.security.token.block.InvalidBlockTokenException;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.ReplicaState;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.StartupOption;
import org.apache.hadoop.hdfs.server.common.JspHelper;
import org.apache.hadoop.hdfs.server.common.StorageInfo;
import org.apache.hadoop.hdfs.server.common.Util;

import static org.apache.hadoop.util.ExitUtil.terminate;

import org.apache.hadoop.hdfs.server.datanode.SecureDataNodeStarter.SecureResources;
import org.apache.hadoop.hdfs.server.datanode.metrics.DataNodeMetrics;
import org.apache.hadoop.hdfs.server.datanode.web.resources.DatanodeWebHdfsMethods;
import org.apache.hadoop.hdfs.server.namenode.FileChecksumServlets;
import org.apache.hadoop.hdfs.server.namenode.StreamFile;
import org.apache.hadoop.hdfs.server.protocol.BlockRecoveryCommand.RecoveringBlock;
import org.apache.hadoop.hdfs.server.protocol.DatanodeProtocol;
import org.apache.hadoop.hdfs.server.protocol.DatanodeRegistration;
import org.apache.hadoop.hdfs.server.protocol.InterDatanodeProtocol;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo;
import org.apache.hadoop.hdfs.server.protocol.ReplicaRecoveryInfo;
import org.apache.hadoop.hdfs.web.WebHdfsFileSystem;
import org.apache.hadoop.hdfs.web.resources.Param;
import org.apache.hadoop.http.HttpServer;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.ipc.ProtocolSignature;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.util.MBeans;
import org.apache.hadoop.net.DNS;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.UserGroupInformation.AuthenticationMethod;
import org.apache.hadoop.security.authorize.AccessControlList;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.util.Daemon;
import org.apache.hadoop.util.DiskChecker;
import org.apache.hadoop.util.DiskChecker.DiskErrorException;
import org.apache.hadoop.util.DiskChecker.DiskOutOfSpaceException;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.ServicePlugin;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.VersionInfo;
import org.mortbay.util.ajax.JSON;



/**********************************************************
 * DataNode is a class (and program) that stores a set of
 * blocks for a DFS deployment.  A single deployment can
 * have one or many DataNodes.  Each DataNode communicates
 * regularly with a single NameNode.  It also communicates
 * with client code and other DataNodes from time to time.
 *
 * DataNodes store a series of named blocks.  The DataNode
 * allows client code to read these blocks, or to write new
 * block data.  The DataNode may also, in response to instructions
 * from its NameNode, delete blocks or copy blocks to/from other
 * DataNodes.
 *
 * The DataNode maintains just one critical table:
 *   block-> stream of bytes (of BLOCK_SIZE or less)
 *
 * This info is stored on a local disk.  The DataNode
 * reports the table's contents to the NameNode upon startup
 * and every so often afterwards.
 *
 * DataNodes spend their lives in an endless loop of asking
 * the NameNode for something to do.  A NameNode cannot connect
 * to a DataNode directly; a NameNode simply returns values from
 * functions invoked by a DataNode.
 *
 * DataNodes maintain an open server socket so that client code 
 * or other DataNodes can read/write data.  The host/port for
 * this server is reported to the NameNode, which then sends that
 * information to clients or other DataNodes that might be interested.
 *
 **********************************************************/
@InterfaceAudience.Private
public class DataNode extends Configured 
    implements InterDatanodeProtocol, ClientDatanodeProtocol,
    DataNodeMXBean {
  public static final Log LOG = LogFactory.getLog(DataNode.class);
  
  static{
    HdfsConfiguration.init();
  }

  public static final String DN_CLIENTTRACE_FORMAT =
        "src: %s" +      // src IP
        ", dest: %s" +   // dst IP
        ", bytes: %s" +  // byte count
        ", op: %s" +     // operation
        ", cliID: %s" +  // DFSClient id
        ", offset: %s" + // offset
        ", srvID: %s" +  // DatanodeRegistration
        ", blockid: %s" + // block id
        ", duration: %s";  // duration time
        
  static final Log ClientTraceLog =
    LogFactory.getLog(DataNode.class.getName() + ".clienttrace");

  /**
   * Use {@link NetUtils#createSocketAddr(String)} instead.
   */
  @Deprecated
  public static InetSocketAddress createSocketAddr(String target
                                                   ) throws IOException {
    return NetUtils.createSocketAddr(target);
  }
  
  /**
   * Manages he BPOfferService objects for the data node.
   * Creation, removal, starting, stopping, shutdown on BPOfferService
   * objects must be done via APIs in this class.
   */
  @InterfaceAudience.Private
  class BlockPoolManager {
    private final Map<String, BPOfferService> bpMapping;
    private final Map<InetSocketAddress, BPOfferService> nameNodeThreads;
 
    //This lock is used only to ensure exclusion of refreshNamenodes
    private final Object refreshNamenodesLock = new Object();
    
    BlockPoolManager(Configuration conf)
        throws IOException {
      bpMapping = new HashMap<String, BPOfferService>();
      nameNodeThreads = new HashMap<InetSocketAddress, BPOfferService>();
  
      List<InetSocketAddress> isas = DFSUtil.getNNServiceRpcAddresses(conf);
      for(InetSocketAddress isa : isas) {
        BPOfferService bpos = new BPOfferService(isa, DataNode.this);
        nameNodeThreads.put(bpos.getNNSocketAddress(), bpos);
      }
    }
    
    synchronized void addBlockPool(BPOfferService t) {
      if (nameNodeThreads.get(t.getNNSocketAddress()) == null) {
        throw new IllegalArgumentException(
            "Unknown BPOfferService thread for namenode address:"
                + t.getNNSocketAddress());
      }
      if (t.getBlockPoolId() == null) {
        throw new IllegalArgumentException("Null blockpool id");
      }
      bpMapping.put(t.getBlockPoolId(), t);
    }
    
    /**
     * Returns the array of BPOfferService objects. 
     * Caution: The BPOfferService returned could be shutdown any time.
     */
    synchronized BPOfferService[] getAllNamenodeThreads() {
      BPOfferService[] bposArray = new BPOfferService[nameNodeThreads.values()
          .size()];
      return nameNodeThreads.values().toArray(bposArray);
    }
    
    synchronized BPOfferService get(InetSocketAddress addr) {
      return nameNodeThreads.get(addr);
    }
    
    synchronized BPOfferService get(String bpid) {
      return bpMapping.get(bpid);
    }
    
    synchronized void remove(BPOfferService t) {
      nameNodeThreads.remove(t.getNNSocketAddress());
      bpMapping.remove(t.getBlockPoolId());
    }
    
    void shutDownAll() throws InterruptedException {
      BPOfferService[] bposArray = this.getAllNamenodeThreads();
      
      for (BPOfferService bpos : bposArray) {
        bpos.stop(); //interrupts the threads
      }
      //now join
      for (BPOfferService bpos : bposArray) {
        bpos.join();
      }
    }
    
    synchronized void startAll() throws IOException {
      try {
        UserGroupInformation.getLoginUser().doAs(
            new PrivilegedExceptionAction<Object>() {
              public Object run() throws Exception {
                for (BPOfferService bpos : nameNodeThreads.values()) {
                  bpos.start();
                }
                return null;
              }
            });
      } catch (InterruptedException ex) {
        IOException ioe = new IOException();
        ioe.initCause(ex.getCause());
        throw ioe;
      }
    }
    
    void joinAll() throws InterruptedException {
      for (BPOfferService bpos: this.getAllNamenodeThreads()) {
        bpos.join();
      }
    }
    
    void refreshNamenodes(Configuration conf)
        throws IOException, InterruptedException {
      LOG.info("Refresh request received for nameservices: "
          + conf.get(DFS_FEDERATION_NAMESERVICES));
      List<InetSocketAddress> newAddresses = 
        DFSUtil.getNNServiceRpcAddresses(conf);
      List<BPOfferService> toShutdown = new ArrayList<BPOfferService>();
      List<InetSocketAddress> toStart = new ArrayList<InetSocketAddress>();
      synchronized (refreshNamenodesLock) {
        synchronized (this) {
          for (InetSocketAddress nnaddr : nameNodeThreads.keySet()) {
            if (!(newAddresses.contains(nnaddr))) {
              toShutdown.add(nameNodeThreads.get(nnaddr));
            }
          }
          for (InetSocketAddress nnaddr : newAddresses) {
            if (!(nameNodeThreads.containsKey(nnaddr))) {
              toStart.add(nnaddr);
            }
          }

          for (InetSocketAddress nnaddr : toStart) {
            BPOfferService bpos = new BPOfferService(nnaddr, DataNode.this);
            nameNodeThreads.put(bpos.getNNSocketAddress(), bpos);
          }
        }

        for (BPOfferService bpos : toShutdown) {
          bpos.stop();
          bpos.join();
        }
        
        // stoping the BPOSes causes them to call remove() on their own when they
        // clean up.
        
        // Now start the threads that are not already running.
        startAll();
      }
    }
  }
  
  volatile boolean shouldRun = true;
  private BlockPoolManager blockPoolManager;
  public volatile FSDatasetInterface data = null;
  private String clusterId = null;

  public final static String EMPTY_DEL_HINT = "";
  AtomicInteger xmitsInProgress = new AtomicInteger();
  Daemon dataXceiverServer = null;
  ThreadGroup threadGroup = null;
  private DNConf dnConf;
  private boolean heartbeatsDisabledForTests = false;
  private DataStorage storage = null;
  private HttpServer infoServer = null;
  DataNodeMetrics metrics;
  private InetSocketAddress selfAddr;
  
  private volatile String hostName; // Host name of this datanode
  private final String confHostName; 
  
  boolean isBlockTokenEnabled;
  BlockPoolTokenSecretManager blockPoolTokenSecretManager;
  
  public DataBlockScanner blockScanner = null;
  private DirectoryScanner directoryScanner = null;
  
  /** Activated plug-ins. */
  private List<ServicePlugin> plugins;
  
  // For InterDataNodeProtocol
  public Server ipcServer;

  private SecureResources secureResources = null;
  private AbstractList<File> dataDirs;
  private Configuration conf;

  private final String userWithLocalPathAccess;

  /**
   * Create the DataNode given a configuration and an array of dataDirs.
   * 'dataDirs' is where the blocks are stored.
   */
  DataNode(final Configuration conf, 
           final AbstractList<File> dataDirs) throws IOException {
    this(conf, dataDirs, null);
  }
  
  /**
   * Create the DataNode given a configuration, an array of dataDirs,
   * and a namenode proxy
   */
  DataNode(final Configuration conf, 
           final AbstractList<File> dataDirs,
           final SecureResources resources) throws IOException {
    super(conf);

    this.userWithLocalPathAccess = conf
        .get(DFSConfigKeys.DFS_BLOCK_LOCAL_PATH_ACCESS_USER_KEY);
    try {
      confHostName = getHostName(conf);
      hostName = confHostName;
      startDataNode(conf, dataDirs, resources);
    } catch (IOException ie) {
      shutdown();
      throw ie;
    }
  }

  private synchronized void setClusterId(final String nsCid, final String bpid
      ) throws IOException {
    if(clusterId != null && !clusterId.equals(nsCid)) {
      throw new IOException ("Cluster IDs not matched: dn cid=" + clusterId 
          + " but ns cid="+ nsCid + "; bpid=" + bpid);
    }
    // else
    clusterId = nsCid;
  }

  private static String getHostName(Configuration config)
      throws UnknownHostException {
    // use configured nameserver & interface to get local hostname
    String name = config.get(DFS_DATANODE_HOST_NAME_KEY);
    if (name == null) {
      name = DNS
          .getDefaultHost(config.get(DFS_DATANODE_DNS_INTERFACE_KEY,
              DFS_DATANODE_DNS_INTERFACE_DEFAULT), config.get(
              DFS_DATANODE_DNS_NAMESERVER_KEY,
              DFS_DATANODE_DNS_NAMESERVER_DEFAULT));
    }
    return name;
  }

  
  private void startInfoServer(Configuration conf) throws IOException {
    // create a servlet to serve full-file content
    InetSocketAddress infoSocAddr = DataNode.getInfoAddr(conf);
    String infoHost = infoSocAddr.getHostName();
    int tmpInfoPort = infoSocAddr.getPort();
    this.infoServer = (secureResources == null) 
       ? new HttpServer("datanode", infoHost, tmpInfoPort, tmpInfoPort == 0, 
           conf, new AccessControlList(conf.get(DFS_ADMIN, " ")))
       : new HttpServer("datanode", infoHost, tmpInfoPort, tmpInfoPort == 0,
           conf, new AccessControlList(conf.get(DFS_ADMIN, " ")),
           secureResources.getListener());
    if(LOG.isDebugEnabled()) {
      LOG.debug("Datanode listening on " + infoHost + ":" + tmpInfoPort);
    }
    if (conf.getBoolean("dfs.https.enable", false)) {
      boolean needClientAuth = conf.getBoolean(DFS_CLIENT_HTTPS_NEED_AUTH_KEY,
                                               DFS_CLIENT_HTTPS_NEED_AUTH_DEFAULT);
      InetSocketAddress secInfoSocAddr = NetUtils.createSocketAddr(conf.get(
          "dfs.datanode.https.address", infoHost + ":" + 0));
      Configuration sslConf = new HdfsConfiguration(false);
      sslConf.addResource(conf.get("dfs.https.server.keystore.resource",
          "ssl-server.xml"));
      this.infoServer.addSslListener(secInfoSocAddr, sslConf, needClientAuth);
      if(LOG.isDebugEnabled()) {
        LOG.debug("Datanode listening for SSL on " + secInfoSocAddr);
      }
    }
    this.infoServer.addInternalServlet(null, "/streamFile/*", StreamFile.class);
    this.infoServer.addInternalServlet(null, "/getFileChecksum/*",
        FileChecksumServlets.GetServlet.class);
    
    this.infoServer.setAttribute("datanode", this);
    this.infoServer.setAttribute(JspHelper.CURRENT_CONF, conf);
    this.infoServer.addServlet(null, "/blockScannerReport", 
                               DataBlockScanner.Servlet.class);

    if (WebHdfsFileSystem.isEnabled(conf, LOG)) {
      infoServer.addJerseyResourcePackage(DatanodeWebHdfsMethods.class
          .getPackage().getName() + ";" + Param.class.getPackage().getName(),
          WebHdfsFileSystem.PATH_PREFIX + "/*");
    }
    this.infoServer.start();
  }
  
  private void startPlugins(Configuration conf) {
    plugins = conf.getInstances(DFS_DATANODE_PLUGINS_KEY, ServicePlugin.class);
    for (ServicePlugin p: plugins) {
      try {
        p.start(this);
        LOG.info("Started plug-in " + p);
      } catch (Throwable t) {
        LOG.warn("ServicePlugin " + p + " could not be started", t);
      }
    }
  }
  

  private void initIpcServer(Configuration conf) throws IOException {
    InetSocketAddress ipcAddr = NetUtils.createSocketAddr(
        conf.get("dfs.datanode.ipc.address"));
    ipcServer = RPC.getServer(DataNode.class, this, ipcAddr.getHostName(),
                              ipcAddr.getPort(), 
                              conf.getInt(DFS_DATANODE_HANDLER_COUNT_KEY, 
                                          DFS_DATANODE_HANDLER_COUNT_DEFAULT), 
                              false, conf, blockPoolTokenSecretManager);
    // set service-level authorization security policy
    if (conf.getBoolean(
        CommonConfigurationKeys.HADOOP_SECURITY_AUTHORIZATION, false)) {
      ipcServer.refreshServiceAcl(conf, new HDFSPolicyProvider());
    }
  }
  
/**
 * Initialize the datanode's periodic scanners:
 *     {@link DataBlockScanner}
 *     {@link DirectoryScanner}
 * They report results on a per-blockpool basis but do their scanning 
 * on a per-Volume basis to minimize competition for disk iops.
 * 
 * @param conf - Configuration has the run intervals and other 
 *               parameters for these periodic scanners
 */
  private void initPeriodicScanners(Configuration conf) {
    initDataBlockScanner(conf);
    initDirectoryScanner(conf);
  }
  
  private void shutdownPeriodicScanners() {
    shutdownDirectoryScanner();
    shutdownDataBlockScanner();
  }
  
  /**
   * See {@link DataBlockScanner}
   */
  private synchronized void initDataBlockScanner(Configuration conf) {
    if (blockScanner != null) {
      return;
    }
    String reason = null;
    assert data != null;
    if (conf.getInt(DFS_DATANODE_SCAN_PERIOD_HOURS_KEY,
                    DFS_DATANODE_SCAN_PERIOD_HOURS_DEFAULT) < 0) {
      reason = "verification is turned off by configuration";
    } else if ("SimulatedFSDataset".equals(data.getClass().getSimpleName())) {
      reason = "verifcation is not supported by SimulatedFSDataset";
    } 
    if (reason == null) {
      blockScanner = new DataBlockScanner(this, data, conf);
      blockScanner.start();
    } else {
      LOG.info("Periodic Block Verification scan is disabled because " +
               reason + ".");
    }
  }
  
  private void shutdownDataBlockScanner() {
    if (blockScanner != null) {
      blockScanner.shutdown();
    }
  }
  
  /**
   * See {@link DirectoryScanner}
   */
  private synchronized void initDirectoryScanner(Configuration conf) {
    if (directoryScanner != null) {
      return;
    }
    String reason = null;
    if (conf.getInt(DFS_DATANODE_DIRECTORYSCAN_INTERVAL_KEY, 
                    DFS_DATANODE_DIRECTORYSCAN_INTERVAL_DEFAULT) < 0) {
      reason = "verification is turned off by configuration";
    } else if ("SimulatedFSDataset".equals(data.getClass().getSimpleName())) {
      reason = "verifcation is not supported by SimulatedFSDataset";
    } 
    if (reason == null) {
      directoryScanner = new DirectoryScanner(this, data, conf);
      directoryScanner.start();
    } else {
      LOG.info("Periodic Directory Tree Verification scan is disabled because " +
               reason + ".");
    }
  }
  
  private synchronized void shutdownDirectoryScanner() {
    if (directoryScanner != null) {
      directoryScanner.shutdown();
    }
  }
  
  private void initDataXceiver(Configuration conf) throws IOException {
    InetSocketAddress socAddr = DataNode.getStreamingAddr(conf);

    // find free port or use privileged port provided
    ServerSocket ss;
    if(secureResources == null) {
      ss = (dnConf.socketWriteTimeout > 0) ? 
          ServerSocketChannel.open().socket() : new ServerSocket();
          Server.bind(ss, socAddr, 0);
    } else {
      ss = secureResources.getStreamingSocket();
    }
    ss.setReceiveBufferSize(HdfsConstants.DEFAULT_DATA_SOCKET_SIZE); 
    // adjust machine name with the actual port
    int tmpPort = ss.getLocalPort();
    selfAddr = new InetSocketAddress(ss.getInetAddress().getHostAddress(),
                                     tmpPort);
    LOG.info("Opened info server at " + tmpPort);
      
    this.threadGroup = new ThreadGroup("dataXceiverServer");
    this.dataXceiverServer = new Daemon(threadGroup, 
        new DataXceiverServer(ss, conf, this));
    this.threadGroup.setDaemon(true); // auto destroy when empty
  }
  
  // calls specific to BP
  protected void notifyNamenodeReceivedBlock(ExtendedBlock block, String delHint) {
    BPOfferService bpos = blockPoolManager.get(block.getBlockPoolId());
    if(bpos != null) {
      bpos.notifyNamenodeReceivedBlock(block, delHint); 
    } else {
      LOG.warn("Cannot find BPOfferService for reporting block received for bpid="
          + block.getBlockPoolId());
    }
  }
  
  // calls specific to BP
  protected void notifyNamenodeDeletedBlock(ExtendedBlock block) {
    BPOfferService bpos = blockPoolManager.get(block.getBlockPoolId());
    if (bpos != null) {
      bpos.notifyNamenodeDeletedBlock(block);
    } else {
      LOG.warn("Cannot find BPOfferService for reporting block deleted for bpid="
          + block.getBlockPoolId());
    }
  }
  
  public void reportBadBlocks(ExtendedBlock block) throws IOException{
    BPOfferService bpos = blockPoolManager.get(block.getBlockPoolId());
    if(bpos == null || bpos.bpNamenode == null) {
      throw new IOException("cannot locate OfferService thread for bp="+block.getBlockPoolId());
    }
    bpos.reportBadBlocks(block);
  }
  
  // used only for testing
  void setHeartbeatsDisabledForTests(
      boolean heartbeatsDisabledForTests) {
    this.heartbeatsDisabledForTests = heartbeatsDisabledForTests;
  }
  
  boolean areHeartbeatsDisabledForTests() {
    return this.heartbeatsDisabledForTests;
  }

  /**
   * This method starts the data node with the specified conf.
   * 
   * @param conf - the configuration
   *  if conf's CONFIG_PROPERTY_SIMULATED property is set
   *  then a simulated storage based data node is created.
   * 
   * @param dataDirs - only for a non-simulated storage data node
   * @throws IOException
   */
  void startDataNode(Configuration conf, 
                     AbstractList<File> dataDirs,
                    // DatanodeProtocol namenode,
                     SecureResources resources
                     ) throws IOException {
    if(UserGroupInformation.isSecurityEnabled() && resources == null)
      throw new RuntimeException("Cannot start secure cluster without " +
      "privileged resources.");

    // settings global for all BPs in the Data Node
    this.secureResources = resources;
    this.dataDirs = dataDirs;
    this.conf = conf;
    this.dnConf = new DNConf(conf);

    storage = new DataStorage();
    
    // global DN settings
    registerMXBean();
    initDataXceiver(conf);
    startInfoServer(conf);
  
    // BlockPoolTokenSecretManager is required to create ipc server.
    this.blockPoolTokenSecretManager = new BlockPoolTokenSecretManager();
    initIpcServer(conf);

    metrics = DataNodeMetrics.create(conf, getMachineName());

    blockPoolManager = new BlockPoolManager(conf);
  }
  
  /**
   * Create a DatanodeRegistration for a specific block pool.
   * @param nsInfo the namespace info from the first part of the NN handshake
   */
  DatanodeRegistration createBPRegistration(NamespaceInfo nsInfo) {
    DatanodeRegistration bpRegistration = createUnknownBPRegistration();
    String blockPoolId = nsInfo.getBlockPoolID();
    
    bpRegistration.setStorageID(getStorageId());
    StorageInfo storageInfo = storage.getBPStorage(blockPoolId);
    if (storageInfo == null) {
      // it's null in the case of SimulatedDataSet
      bpRegistration.storageInfo.layoutVersion = HdfsConstants.LAYOUT_VERSION;
      bpRegistration.setStorageInfo(nsInfo);
    } else {
      bpRegistration.setStorageInfo(storageInfo);
    }
    return bpRegistration;
  }

  /**
   * Check that the registration returned from a NameNode is consistent
   * with the information in the storage. If the storage is fresh/unformatted,
   * sets the storage ID based on this registration.
   * Also updates the block pool's state in the secret manager.
   */
  synchronized void bpRegistrationSucceeded(DatanodeRegistration bpRegistration,
      String blockPoolId)
      throws IOException {
    hostName = bpRegistration.getHost();

    if (storage.getStorageID().equals("")) {
      // This is a fresh datanode -- take the storage ID provided by the
      // NN and persist it.
      storage.setStorageID(bpRegistration.getStorageID());
      storage.writeAll();
      LOG.info("New storage id " + bpRegistration.getStorageID()
          + " is assigned to data-node " + bpRegistration.getName());
    } else if(!storage.getStorageID().equals(bpRegistration.getStorageID())) {
      throw new IOException("Inconsistent storage IDs. Name-node returned "
          + bpRegistration.getStorageID() 
          + ". Expecting " + storage.getStorageID());
    }
    
    registerBlockPoolWithSecretManager(bpRegistration, blockPoolId);
  }
  
  /**
   * After the block pool has contacted the NN, registers that block pool
   * with the secret manager, updating it with the secrets provided by the NN.
   * @param bpRegistration
   * @param blockPoolId
   * @throws IOException
   */
  private void registerBlockPoolWithSecretManager(DatanodeRegistration bpRegistration,
      String blockPoolId) throws IOException {
    ExportedBlockKeys keys = bpRegistration.exportedKeys;
    isBlockTokenEnabled = keys.isBlockTokenEnabled();
    // TODO should we check that all federated nns are either enabled or
    // disabled?
    if (!isBlockTokenEnabled) return;
    
    if (!blockPoolTokenSecretManager.isBlockPoolRegistered(blockPoolId)) {
      long blockKeyUpdateInterval = keys.getKeyUpdateInterval();
      long blockTokenLifetime = keys.getTokenLifetime();
      LOG.info("Block token params received from NN: for block pool " +
          blockPoolId + " keyUpdateInterval="
          + blockKeyUpdateInterval / (60 * 1000)
          + " min(s), tokenLifetime=" + blockTokenLifetime / (60 * 1000)
          + " min(s)");
      final BlockTokenSecretManager secretMgr = 
        new BlockTokenSecretManager(false, 0, blockTokenLifetime);
      blockPoolTokenSecretManager.addBlockPool(blockPoolId, secretMgr);
    }
    
    blockPoolTokenSecretManager.setKeys(blockPoolId,
        bpRegistration.exportedKeys);
    bpRegistration.exportedKeys = ExportedBlockKeys.DUMMY_KEYS;
  }

  /**
   * Remove the given block pool from the block scanner, dataset, and storage.
   */
  void shutdownBlockPool(BPOfferService bpos) {
    blockPoolManager.remove(bpos);

    String bpId = bpos.getBlockPoolId();
    if (blockScanner != null) {
      blockScanner.removeBlockPool(bpId);
    }
  
    if (data != null) { 
      data.shutdownBlockPool(bpId);
    }

    if (storage != null) {
      storage.removeBlockPoolStorage(bpId);
    }
  }

  /**
   * One of the Block Pools has successfully connected to its NN.
   * This initializes the local storage for that block pool,
   * checks consistency of the NN's cluster ID, etc.
   * 
   * If this is the first block pool to register, this also initializes
   * the datanode-scoped storage.
   * 
   * @param nsInfo the handshake response from the NN.
   * @throws IOException if the NN is inconsistent with the local storage.
   */
  void initBlockPool(BPOfferService bpos) throws IOException {
    NamespaceInfo nsInfo = bpos.getNamespaceInfo();
    if (nsInfo == null) {
      throw new IOException("NamespaceInfo not found: Block pool " + bpos
          + " should have retrieved namespace info before initBlockPool.");
    }
    
    // Register the new block pool with the BP manager.
    blockPoolManager.addBlockPool(bpos);

    setClusterId(nsInfo.clusterID, nsInfo.getBlockPoolID());
    
    // In the case that this is the first block pool to connect, initialize
    // the dataset, block scanners, etc.
    initStorage(nsInfo);
    initPeriodicScanners(conf);
    
    data.addBlockPool(nsInfo.getBlockPoolID(), conf);
  }

  /**
   * Create a DatanodeRegistration object with no valid StorageInfo.
   * This is used when reporting an error during handshake - ie
   * before we can load any specific block pool.
   */
  private DatanodeRegistration createUnknownBPRegistration() {
    DatanodeRegistration reg = new DatanodeRegistration(
        confHostName + ":" + getPort());
      
    reg.setInfoPort(infoServer.getPort());
    reg.setIpcPort(getIpcPort());
    return reg;
  }

  BPOfferService[] getAllBpOs() {
    return blockPoolManager.getAllNamenodeThreads();
  }
  
  int getBpOsCount() {
    return blockPoolManager.getAllNamenodeThreads().length;
  }
  
  /**
   * Initializes the {@link #data}. The initialization is done only once, when
   * handshake with the the first namenode is completed.
   */
  private void initStorage(final NamespaceInfo nsInfo) throws IOException {
    final FSDatasetInterface.Factory factory
        = FSDatasetInterface.Factory.getFactory(conf);
    
    if (!factory.isSimulated()) {
      final StartupOption startOpt = getStartupOption(conf);
      if (startOpt == null) {
        throw new IOException("Startup option not set.");
      }
      final String bpid = nsInfo.getBlockPoolID();
      //read storage info, lock data dirs and transition fs state if necessary
      storage.recoverTransitionRead(this, bpid, nsInfo, dataDirs, startOpt);
      final StorageInfo bpStorage = storage.getBPStorage(bpid);
      LOG.info("Setting up storage: nsid=" + bpStorage.getNamespaceID()
          + ";bpid=" + bpid + ";lv=" + storage.getLayoutVersion()
          + ";nsInfo=" + nsInfo);
    }

    synchronized(this)  {
      if (data == null) {
        data = factory.createFSDatasetInterface(this, storage, conf);
      }
    }
  }

  /**
   * Determine the http server's effective addr
   */
  public static InetSocketAddress getInfoAddr(Configuration conf) {
    return NetUtils.createSocketAddr(conf.get(DFS_DATANODE_HTTP_ADDRESS_KEY,
        DFS_DATANODE_HTTP_ADDRESS_DEFAULT));
  }
  
  private void registerMXBean() {
    MBeans.register("DataNode", "DataNodeInfo", this);
  }
  
  int getPort() {
    return selfAddr.getPort();
  }
  
  String getStorageId() {
    return storage.getStorageID();
  }
  
  /** 
   * Get host:port with host set to Datanode host and port set to the
   * port {@link DataXceiver} is serving.
   * @return host:port string
   */
  public String getMachineName() {
    return hostName + ":" + getPort();
  }
  
  public int getIpcPort() {
    return ipcServer.getListenerAddress().getPort();
  }
  
  /**
   * get BP registration by blockPool id
   * @param bpid
   * @return BP registration object
   * @throws IOException
   */
  DatanodeRegistration getDNRegistrationForBP(String bpid) 
  throws IOException {
    BPOfferService bpos = blockPoolManager.get(bpid);
    if(bpos==null || bpos.bpRegistration==null) {
      throw new IOException("cannot find BPOfferService for bpid="+bpid);
    }
    return bpos.bpRegistration;
  }
  
  /**
   * get BP registration by machine and port name (host:port)
   * @param mName
   * @return BP registration 
   * @throws IOException 
   */
  DatanodeRegistration getDNRegistrationByMachineName(String mName) {
    BPOfferService [] bposArray = blockPoolManager.getAllNamenodeThreads();
    for (BPOfferService bpos : bposArray) {
      if(bpos.bpRegistration.getName().equals(mName))
        return bpos.bpRegistration;
    }
    return null;
  }
  
  /**
   * Creates either NIO or regular depending on socketWriteTimeout.
   */
  protected Socket newSocket() throws IOException {
    return (dnConf.socketWriteTimeout > 0) ? 
           SocketChannel.open().socket() : new Socket();                                   
  }

  public static InterDatanodeProtocol createInterDataNodeProtocolProxy(
      DatanodeID datanodeid, final Configuration conf, final int socketTimeout)
    throws IOException {
    final InetSocketAddress addr = NetUtils.createSocketAddr(
        datanodeid.getHost() + ":" + datanodeid.getIpcPort());
    if (InterDatanodeProtocol.LOG.isDebugEnabled()) {
      InterDatanodeProtocol.LOG.debug("InterDatanodeProtocol addr=" + addr);
    }
    UserGroupInformation loginUgi = UserGroupInformation.getLoginUser();
    try {
      return loginUgi
          .doAs(new PrivilegedExceptionAction<InterDatanodeProtocol>() {
            public InterDatanodeProtocol run() throws IOException {
              return (InterDatanodeProtocol) RPC.getProxy(
                  InterDatanodeProtocol.class, InterDatanodeProtocol.versionID,
                  addr, UserGroupInformation.getCurrentUser(), conf,
                  NetUtils.getDefaultSocketFactory(conf), socketTimeout);
            }
          });
    } catch (InterruptedException ie) {
      throw new IOException(ie.getMessage());
    }
  }

  /**
   * get the name node address based on the block pool id
   * @param bpid block pool ID
   * @return namenode address corresponding to the bpid
   */
  public InetSocketAddress getNameNodeAddr(String bpid) {
    BPOfferService bp = blockPoolManager.get(bpid);
    if (bp != null) {
      return bp.getNNSocketAddress();
    }
    LOG.warn("No name node address found for block pool ID " + bpid);
    return null;
  }
  
  public InetSocketAddress getSelfAddr() {
    return selfAddr;
  }
    
  DataNodeMetrics getMetrics() {
    return metrics;
  }
  
  public static void setNewStorageID(DatanodeID dnId) {
    LOG.info("Datanode is " + dnId);
    dnId.storageID = createNewStorageId(dnId.getPort());
  }
  
  
  /**
   * Connect to the NN. This is separated out for easier testing.
   */
  DatanodeProtocol connectToNN(InetSocketAddress nnAddr) throws IOException {
    return (DatanodeProtocol)RPC.waitForProxy(DatanodeProtocol.class,
          DatanodeProtocol.versionID, nnAddr, conf);
  }
  
  static String createNewStorageId(int port) {
    /* Return 
     * "DS-randInt-ipaddr-currentTimeMillis"
     * It is considered extermely rare for all these numbers to match
     * on a different machine accidentally for the following 
     * a) SecureRandom(INT_MAX) is pretty much random (1 in 2 billion), and
     * b) Good chance ip address would be different, and
     * c) Even on the same machine, Datanode is designed to use different ports.
     * d) Good chance that these are started at different times.
     * For a confict to occur all the 4 above have to match!.
     * The format of this string can be changed anytime in future without
     * affecting its functionality.
     */
    String ip = "unknownIP";
    try {
      ip = DNS.getDefaultIP("default");
    } catch (UnknownHostException ignored) {
      LOG.warn("Could not find ip address of \"default\" inteface.");
    }
    
    int rand = DFSUtil.getSecureRandom().nextInt(Integer.MAX_VALUE);
    return "DS-" + rand + "-" + ip + "-" + port + "-"
        + System.currentTimeMillis();
  }
  
  /** Ensure the authentication method is kerberos */
  private void checkKerberosAuthMethod(String msg) throws IOException {
    // User invoking the call must be same as the datanode user
    if (!UserGroupInformation.isSecurityEnabled()) {
      return;
    }
    if (UserGroupInformation.getCurrentUser().getAuthenticationMethod() != 
        AuthenticationMethod.KERBEROS) {
      throw new AccessControlException("Error in " + msg
          + "Only kerberos based authentication is allowed.");
    }
  }
  
  private void checkBlockLocalPathAccess() throws IOException {
    checkKerberosAuthMethod("getBlockLocalPathInfo()");
    String currentUser = UserGroupInformation.getCurrentUser().getShortUserName();
    if (!currentUser.equals(this.userWithLocalPathAccess)) {
      throw new AccessControlException(
          "Can't continue with getBlockLocalPathInfo() "
              + "authorization. The user " + currentUser
              + " is not allowed to call getBlockLocalPathInfo");
    }
  }

  @Override
  public BlockLocalPathInfo getBlockLocalPathInfo(ExtendedBlock block,
      Token<BlockTokenIdentifier> token) throws IOException {
    checkBlockLocalPathAccess();
    checkBlockToken(block, token, BlockTokenSecretManager.AccessMode.READ);
    BlockLocalPathInfo info = data.getBlockLocalPathInfo(block);
    if (LOG.isDebugEnabled()) {
      if (info != null) {
        if (LOG.isTraceEnabled()) {
          LOG.trace("getBlockLocalPathInfo successful block=" + block
              + " blockfile " + info.getBlockPath() + " metafile "
              + info.getMetaPath());
        }
      } else {
        if (LOG.isTraceEnabled()) {
          LOG.trace("getBlockLocalPathInfo for block=" + block
              + " returning null");
        }
      }
    }
    metrics.incrBlocksGetLocalPathInfo();
    return info;
  }
  
  private void checkBlockToken(ExtendedBlock block, Token<BlockTokenIdentifier> token,
      AccessMode accessMode) throws IOException {
    if (isBlockTokenEnabled && UserGroupInformation.isSecurityEnabled()) {
      BlockTokenIdentifier id = new BlockTokenIdentifier();
      ByteArrayInputStream buf = new ByteArrayInputStream(token.getIdentifier());
      DataInputStream in = new DataInputStream(buf);
      id.readFields(in);
      if (LOG.isDebugEnabled()) {
        LOG.debug("Got: " + id.toString());
      }
      blockPoolTokenSecretManager.checkAccess(id, null, block, accessMode);
    }
  }

  /**
   * Shut down this instance of the datanode.
   * Returns only after shutdown is complete.
   * This method can only be called by the offerService thread.
   * Otherwise, deadlock might occur.
   */
  public void shutdown() {
    if (plugins != null) {
      for (ServicePlugin p : plugins) {
        try {
          p.stop();
          LOG.info("Stopped plug-in " + p);
        } catch (Throwable t) {
          LOG.warn("ServicePlugin " + p + " could not be stopped", t);
        }
      }
    }
    
    this.shouldRun = false;
    shutdownPeriodicScanners();
    
    if (infoServer != null) {
      try {
        infoServer.stop();
      } catch (Exception e) {
        LOG.warn("Exception shutting down DataNode", e);
      }
    }
    if (ipcServer != null) {
      ipcServer.stop();
    }
    
    if (dataXceiverServer != null) {
      ((DataXceiverServer) this.dataXceiverServer.getRunnable()).kill();
      this.dataXceiverServer.interrupt();

      // wait for all data receiver threads to exit
      if (this.threadGroup != null) {
        int sleepMs = 2;
        while (true) {
          this.threadGroup.interrupt();
          LOG.info("Waiting for threadgroup to exit, active threads is " +
                   this.threadGroup.activeCount());
          if (this.threadGroup.activeCount() == 0) {
            break;
          }
          try {
            Thread.sleep(sleepMs);
          } catch (InterruptedException e) {}
          sleepMs = sleepMs * 3 / 2; // exponential backoff
          if (sleepMs > 1000) {
            sleepMs = 1000;
          }
        }
      }
      // wait for dataXceiveServer to terminate
      try {
        this.dataXceiverServer.join();
      } catch (InterruptedException ie) {
      }
    }
    
    if(blockPoolManager != null) {
      try {
        this.blockPoolManager.shutDownAll();
      } catch (InterruptedException ie) {
        LOG.warn("Received exception in BlockPoolManager#shutDownAll: ", ie);
      }
    }
    
    if (storage != null) {
      try {
        this.storage.unlockAll();
      } catch (IOException ie) {
        LOG.warn("Exception when unlocking storage: " + ie, ie);
      }
    }
    if (data != null) {
      data.shutdown();
    }
    if (metrics != null) {
      metrics.shutdown();
    }
  }
  
  
  /** Check if there is no space in disk 
   *  @param e that caused this checkDiskError call
   **/
  protected void checkDiskError(Exception e ) throws IOException {
    
    LOG.warn("checkDiskError: exception: ", e);
    
    if (e.getMessage() != null &&
        e.getMessage().startsWith("No space left on device")) {
      throw new DiskOutOfSpaceException("No space left on device");
    } else {
      checkDiskError();
    }
  }
  
  /**
   *  Check if there is a disk failure and if so, handle the error
   *
   **/
  protected void checkDiskError( ) {
    try {
      data.checkDataDir();
    } catch (DiskErrorException de) {
      handleDiskError(de.getMessage());
    }
  }
  
  private void handleDiskError(String errMsgr) {
    final boolean hasEnoughResources = data.hasEnoughResource();
    LOG.warn("DataNode.handleDiskError: Keep Running: " + hasEnoughResources);
    
    // If we have enough active valid volumes then we do not want to 
    // shutdown the DN completely.
    int dpError = hasEnoughResources ? DatanodeProtocol.DISK_ERROR  
                                     : DatanodeProtocol.FATAL_DISK_ERROR;  
    metrics.incrVolumeFailures();

    //inform NameNodes
    for(BPOfferService bpos: blockPoolManager.getAllNamenodeThreads()) {
      DatanodeProtocol nn = bpos.bpNamenode;
      try {
        nn.errorReport(bpos.bpRegistration, dpError, errMsgr);
      } catch(IOException e) {
        LOG.warn("Error reporting disk failure to NameNode", e);
      }
    }
    
    if(hasEnoughResources) {
      scheduleAllBlockReport(0);
      return; // do not shutdown
    }
    
    LOG.warn("DataNode is shutting down: " + errMsgr);
    shouldRun = false;
  }
    
  /** Number of concurrent xceivers per node. */
  @Override // DataNodeMXBean
  public int getXceiverCount() {
    return threadGroup == null ? 0 : threadGroup.activeCount();
  }
    
  UpgradeManagerDatanode getUpgradeManagerDatanode(String bpid) {
    BPOfferService bpos = blockPoolManager.get(bpid);
    if(bpos==null) {
      return null;
    }
    return bpos.getUpgradeManager();
  }

  private void transferBlock( ExtendedBlock block, 
                              DatanodeInfo xferTargets[] 
                              ) throws IOException {
    DatanodeProtocol nn = getBPNamenode(block.getBlockPoolId());
    DatanodeRegistration bpReg = getDNRegistrationForBP(block.getBlockPoolId());
    
    if (!data.isValidBlock(block)) {
      // block does not exist or is under-construction
      String errStr = "Can't send invalid block " + block;
      LOG.info(errStr);
      nn.errorReport(bpReg, DatanodeProtocol.INVALID_BLOCK, errStr);
      return;
    }

    // Check if NN recorded length matches on-disk length 
    long onDiskLength = data.getLength(block);
    if (block.getNumBytes() > onDiskLength) {
      // Shorter on-disk len indicates corruption so report NN the corrupt block
      nn.reportBadBlocks(new LocatedBlock[]{
          new LocatedBlock(block, new DatanodeInfo[] {
              new DatanodeInfo(bpReg)})});
      LOG.warn("Can't replicate block " + block
          + " because on-disk length " + onDiskLength 
          + " is shorter than NameNode recorded length " + block.getNumBytes());
      return;
    }
    
    int numTargets = xferTargets.length;
    if (numTargets > 0) {
      if (LOG.isInfoEnabled()) {
        StringBuilder xfersBuilder = new StringBuilder();
        for (int i = 0; i < numTargets; i++) {
          xfersBuilder.append(xferTargets[i].getName());
          xfersBuilder.append(" ");
        }
        LOG.info(bpReg + " Starting thread to transfer block " + 
                 block + " to " + xfersBuilder);                       
      }

      new Daemon(new DataTransfer(xferTargets, block,
          BlockConstructionStage.PIPELINE_SETUP_CREATE, "")).start();
    }
  }

  void transferBlocks(String poolId, Block blocks[],
      DatanodeInfo xferTargets[][]) {
    for (int i = 0; i < blocks.length; i++) {
      try {
        transferBlock(new ExtendedBlock(poolId, blocks[i]), xferTargets[i]);
      } catch (IOException ie) {
        LOG.warn("Failed to transfer block " + blocks[i], ie);
      }
    }
  }

  /* ********************************************************************
  Protocol when a client reads data from Datanode (Cur Ver: 9):
  
  Client's Request :
  =================
   
     Processed in DataXceiver:
     +----------------------------------------------+
     | Common Header   | 1 byte OP == OP_READ_BLOCK |
     +----------------------------------------------+
     
     Processed in readBlock() :
     +-------------------------------------------------------------------------+
     | 8 byte Block ID | 8 byte genstamp | 8 byte start offset | 8 byte length |
     +-------------------------------------------------------------------------+
     |   vInt length   |  <DFSClient id> |
     +-----------------------------------+
     
     Client sends optional response only at the end of receiving data.
       
  DataNode Response :
  ===================
   
    In readBlock() :
    If there is an error while initializing BlockSender :
       +---------------------------+
       | 2 byte OP_STATUS_ERROR    | and connection will be closed.
       +---------------------------+
    Otherwise
       +---------------------------+
       | 2 byte OP_STATUS_SUCCESS  |
       +---------------------------+
       
    Actual data, sent by BlockSender.sendBlock() :
    
      ChecksumHeader :
      +--------------------------------------------------+
      | 1 byte CHECKSUM_TYPE | 4 byte BYTES_PER_CHECKSUM |
      +--------------------------------------------------+
      Followed by actual data in the form of PACKETS: 
      +------------------------------------+
      | Sequence of data PACKETs ....      |
      +------------------------------------+
    
    A "PACKET" is defined further below.
    
    The client reads data until it receives a packet with 
    "LastPacketInBlock" set to true or with a zero length. It then replies
    to DataNode with one of the status codes:
    - CHECKSUM_OK:    All the chunk checksums have been verified
    - SUCCESS:        Data received; checksums not verified
    - ERROR_CHECKSUM: (Currently not used) Detected invalid checksums

      +---------------+
      | 2 byte Status |
      +---------------+
    
    The DataNode expects all well behaved clients to send the 2 byte
    status code. And if the the client doesn't, the DN will close the
    connection. So the status code is optional in the sense that it
    does not affect the correctness of the data. (And the client can
    always reconnect.)
    
    PACKET : Contains a packet header, checksum and data. Amount of data
    ======== carried is set by BUFFER_SIZE.
    
      +-----------------------------------------------------+
      | 4 byte packet length (excluding packet header)      |
      +-----------------------------------------------------+
      | 8 byte offset in the block | 8 byte sequence number |
      +-----------------------------------------------------+
      | 1 byte isLastPacketInBlock                          |
      +-----------------------------------------------------+
      | 4 byte Length of actual data                        |
      +-----------------------------------------------------+
      | x byte checksum data. x is defined below            |
      +-----------------------------------------------------+
      | actual data ......                                  |
      +-----------------------------------------------------+
      
      x = (length of data + BYTE_PER_CHECKSUM - 1)/BYTES_PER_CHECKSUM *
          CHECKSUM_SIZE
          
      CHECKSUM_SIZE depends on CHECKSUM_TYPE (usually, 4 for CRC32)
      
      The above packet format is used while writing data to DFS also.
      Not all the fields might be used while reading.
    
   ************************************************************************ */

  /**
   * Used for transferring a block of data.  This class
   * sends a piece of data to another DataNode.
   */
  private class DataTransfer implements Runnable {
    final DatanodeInfo[] targets;
    final ExtendedBlock b;
    final BlockConstructionStage stage;
    final private DatanodeRegistration bpReg;
    final String clientname;

    /**
     * Connect to the first item in the target list.  Pass along the 
     * entire target list, the block, and the data.
     */
    DataTransfer(DatanodeInfo targets[], ExtendedBlock b, BlockConstructionStage stage,
        final String clientname) throws IOException {
      if (DataTransferProtocol.LOG.isDebugEnabled()) {
        DataTransferProtocol.LOG.debug(getClass().getSimpleName() + ": "
            + b + " (numBytes=" + b.getNumBytes() + ")"
            + ", stage=" + stage
            + ", clientname=" + clientname
            + ", targests=" + Arrays.asList(targets));
      }
      this.targets = targets;
      this.b = b;
      this.stage = stage;
      BPOfferService bpos = blockPoolManager.get(b.getBlockPoolId());
      bpReg = bpos.bpRegistration;
      this.clientname = clientname;
    }

    /**
     * Do the deed, write the bytes
     */
    public void run() {
      xmitsInProgress.getAndIncrement();
      Socket sock = null;
      DataOutputStream out = null;
      DataInputStream in = null;
      BlockSender blockSender = null;
      final boolean isClient = clientname.length() > 0;
      
      try {
        InetSocketAddress curTarget = 
          NetUtils.createSocketAddr(targets[0].getName());
        sock = newSocket();
        NetUtils.connect(sock, curTarget, dnConf.socketTimeout);
        sock.setSoTimeout(targets.length * dnConf.socketTimeout);

        long writeTimeout = dnConf.socketWriteTimeout + 
                            HdfsServerConstants.WRITE_TIMEOUT_EXTENSION * (targets.length-1);
        OutputStream baseStream = NetUtils.getOutputStream(sock, writeTimeout);
        out = new DataOutputStream(new BufferedOutputStream(baseStream,
            HdfsConstants.SMALL_BUFFER_SIZE));
        blockSender = new BlockSender(b, 0, b.getNumBytes(), 
            false, false, DataNode.this, null);
        DatanodeInfo srcNode = new DatanodeInfo(bpReg);

        //
        // Header info
        //
        Token<BlockTokenIdentifier> accessToken = BlockTokenSecretManager.DUMMY_TOKEN;
        if (isBlockTokenEnabled) {
          accessToken = blockPoolTokenSecretManager.generateToken(b, 
              EnumSet.of(BlockTokenSecretManager.AccessMode.WRITE));
        }

        new Sender(out).writeBlock(b, accessToken, clientname, targets, srcNode,
            stage, 0, 0, 0, 0, blockSender.getChecksum());

        // send data & checksum
        blockSender.sendBlock(out, baseStream, null);

        // no response necessary
        LOG.info(getClass().getSimpleName() + ": Transmitted " + b
            + " (numBytes=" + b.getNumBytes() + ") to " + curTarget);

        // read ack
        if (isClient) {
          in = new DataInputStream(NetUtils.getInputStream(sock));
          DNTransferAckProto closeAck = DNTransferAckProto.parseFrom(
              HdfsProtoUtil.vintPrefixed(in));
          if (LOG.isDebugEnabled()) {
            LOG.debug(getClass().getSimpleName() + ": close-ack=" + closeAck);
          }
          if (closeAck.getStatus() != Status.SUCCESS) {
            if (closeAck.getStatus() == Status.ERROR_ACCESS_TOKEN) {
              throw new InvalidBlockTokenException(
                  "Got access token error for connect ack, targets="
                   + Arrays.asList(targets));
            } else {
              throw new IOException("Bad connect ack, targets="
                  + Arrays.asList(targets));
            }
          }
        }
      } catch (IOException ie) {
        LOG.warn(
            bpReg + ":Failed to transfer " + b + " to " + targets[0].getName()
                + " got ", ie);
        // check if there are any disk problem
        checkDiskError();
        
      } finally {
        xmitsInProgress.getAndDecrement();
        IOUtils.closeStream(blockSender);
        IOUtils.closeStream(out);
        IOUtils.closeStream(in);
        IOUtils.closeSocket(sock);
      }
    }
  }
  
  /**
   * After a block becomes finalized, a datanode increases metric counter,
   * notifies namenode, and adds it to the block scanner
   * @param block
   * @param delHint
   */
  void closeBlock(ExtendedBlock block, String delHint) {
    metrics.incrBlocksWritten();
    BPOfferService bpos = blockPoolManager.get(block.getBlockPoolId());
    if(bpos != null) {
      bpos.notifyNamenodeReceivedBlock(block, delHint);
    } else {
      LOG.warn("Cannot find BPOfferService for reporting block received for bpid="
          + block.getBlockPoolId());
    }
    if (blockScanner != null) {
      blockScanner.addBlock(block);
    }
  }

  /** Start a single datanode daemon and wait for it to finish.
   *  If this thread is specifically interrupted, it will stop waiting.
   */
  public void runDatanodeDaemon() throws IOException {
    blockPoolManager.startAll();

    // start dataXceiveServer
    dataXceiverServer.start();
    ipcServer.start();
    startPlugins(conf);
  }

  /**
   * A data node is considered to be up if one of the bp services is up
   */
  public boolean isDatanodeUp() {
    for (BPOfferService bp : blockPoolManager.getAllNamenodeThreads()) {
      if (bp.isAlive()) {
        return true;
      }
    }
    return false;
  }

  /** Instantiate a single datanode object. This must be run by invoking
   *  {@link DataNode#runDatanodeDaemon()} subsequently. 
   */
  public static DataNode instantiateDataNode(String args[],
                                      Configuration conf) throws IOException {
    return instantiateDataNode(args, conf, null);
  }
  
  /** Instantiate a single datanode object, along with its secure resources. 
   * This must be run by invoking{@link DataNode#runDatanodeDaemon()} 
   * subsequently. 
   */
  public static DataNode instantiateDataNode(String args [], Configuration conf,
      SecureResources resources) throws IOException {
    if (conf == null)
      conf = new HdfsConfiguration();
    
    if (args != null) {
      // parse generic hadoop options
      GenericOptionsParser hParser = new GenericOptionsParser(conf, args);
      args = hParser.getRemainingArgs();
    }
    
    if (!parseArguments(args, conf)) {
      printUsage();
      return null;
    }
    if (conf.get("dfs.network.script") != null) {
      LOG.error("This configuration for rack identification is not supported" +
          " anymore. RackID resolution is handled by the NameNode.");
      System.exit(-1);
    }
    Collection<URI> dataDirs = getStorageDirs(conf);
    UserGroupInformation.setConfiguration(conf);
    SecurityUtil.login(conf, DFS_DATANODE_KEYTAB_FILE_KEY,
        DFS_DATANODE_USER_NAME_KEY);
    return makeInstance(dataDirs, conf, resources);
  }

  static Collection<URI> getStorageDirs(Configuration conf) {
    Collection<String> dirNames =
      conf.getTrimmedStringCollection(DFS_DATANODE_DATA_DIR_KEY);
    return Util.stringCollectionAsURIs(dirNames);
  }

  /** Instantiate & Start a single datanode daemon and wait for it to finish.
   *  If this thread is specifically interrupted, it will stop waiting.
   */
  public static DataNode createDataNode(String args[],
                                 Configuration conf) throws IOException {
    return createDataNode(args, conf, null);
  }
  
  /** Instantiate & Start a single datanode daemon and wait for it to finish.
   *  If this thread is specifically interrupted, it will stop waiting.
   */
  @InterfaceAudience.Private
  public static DataNode createDataNode(String args[], Configuration conf,
      SecureResources resources) throws IOException {
    DataNode dn = instantiateDataNode(args, conf, resources);
    if (dn != null) {
      dn.runDatanodeDaemon();
    }
    return dn;
  }

  void join() {
    while (shouldRun) {
      try {
        blockPoolManager.joinAll();
        if (blockPoolManager.getAllNamenodeThreads() != null
            && blockPoolManager.getAllNamenodeThreads().length == 0) {
          shouldRun = false;
        }
        Thread.sleep(2000);
      } catch (InterruptedException ex) {
        LOG.warn("Received exception in Datanode#join: " + ex);
      }
    }
  }

  /**
   * Make an instance of DataNode after ensuring that at least one of the
   * given data directories (and their parent directories, if necessary)
   * can be created.
   * @param dataDirs List of directories, where the new DataNode instance should
   * keep its files.
   * @param conf Configuration instance to use.
   * @param resources Secure resources needed to run under Kerberos
   * @return DataNode instance for given list of data dirs and conf, or null if
   * no directory from this directory list can be created.
   * @throws IOException
   */
  static DataNode makeInstance(Collection<URI> dataDirs, Configuration conf,
      SecureResources resources) throws IOException {
    LocalFileSystem localFS = FileSystem.getLocal(conf);
    FsPermission permission = new FsPermission(
        conf.get(DFS_DATANODE_DATA_DIR_PERMISSION_KEY,
                 DFS_DATANODE_DATA_DIR_PERMISSION_DEFAULT));
    ArrayList<File> dirs = getDataDirsFromURIs(dataDirs, localFS, permission);
    DefaultMetricsSystem.initialize("DataNode");

    assert dirs.size() > 0 : "number of data directories should be > 0";
    return new DataNode(conf, dirs, resources);
  }

  // DataNode ctor expects AbstractList instead of List or Collection...
  static ArrayList<File> getDataDirsFromURIs(Collection<URI> dataDirs,
      LocalFileSystem localFS, FsPermission permission) throws IOException {
    ArrayList<File> dirs = new ArrayList<File>();
    StringBuilder invalidDirs = new StringBuilder();
    for (URI dirURI : dataDirs) {
      if (!"file".equalsIgnoreCase(dirURI.getScheme())) {
        LOG.warn("Unsupported URI schema in " + dirURI + ". Ignoring ...");
        invalidDirs.append("\"").append(dirURI).append("\" ");
        continue;
      }
      // drop any (illegal) authority in the URI for backwards compatibility
      File dir = new File(dirURI.getPath());
      try {
        DiskChecker.checkDir(localFS, new Path(dir.toURI()), permission);
        dirs.add(dir);
      } catch (IOException ioe) {
        LOG.warn("Invalid " + DFS_DATANODE_DATA_DIR_KEY + " "
            + dir + " : ", ioe);
        invalidDirs.append("\"").append(dir.getCanonicalPath()).append("\" ");
      }
    }
    if (dirs.size() == 0) {
      throw new IOException("All directories in "
          + DFS_DATANODE_DATA_DIR_KEY + " are invalid: "
          + invalidDirs);
    }
    return dirs;
  }

  @Override
  public String toString() {
    return "DataNode{data=" + data + ", localName='" + getMachineName()
        + "', storageID='" + getStorageId() + "', xmitsInProgress="
        + xmitsInProgress.get() + "}";
  }

  private static void printUsage() {
    System.err.println("Usage: java DataNode");
    System.err.println("           [-rollback]");
  }

  /**
   * Parse and verify command line arguments and set configuration parameters.
   *
   * @return false if passed argements are incorrect
   */
  private static boolean parseArguments(String args[], 
                                        Configuration conf) {
    int argsLen = (args == null) ? 0 : args.length;
    StartupOption startOpt = StartupOption.REGULAR;
    for(int i=0; i < argsLen; i++) {
      String cmd = args[i];
      if ("-r".equalsIgnoreCase(cmd) || "--rack".equalsIgnoreCase(cmd)) {
        LOG.error("-r, --rack arguments are not supported anymore. RackID " +
            "resolution is handled by the NameNode.");
        terminate(1);
      } else if ("-rollback".equalsIgnoreCase(cmd)) {
        startOpt = StartupOption.ROLLBACK;
      } else if ("-regular".equalsIgnoreCase(cmd)) {
        startOpt = StartupOption.REGULAR;
      } else
        return false;
    }
    setStartupOption(conf, startOpt);
    return true;
  }

  private static void setStartupOption(Configuration conf, StartupOption opt) {
    conf.set(DFS_DATANODE_STARTUP_KEY, opt.toString());
  }

  static StartupOption getStartupOption(Configuration conf) {
    return StartupOption.valueOf(conf.get(DFS_DATANODE_STARTUP_KEY,
                                          StartupOption.REGULAR.toString()));
  }

  /**
   * This methods  arranges for the data node to send 
   * the block report at the next heartbeat.
   */
  public void scheduleAllBlockReport(long delay) {
    for(BPOfferService bpos : blockPoolManager.getAllNamenodeThreads()) {
      bpos.scheduleBlockReport(delay);
    }
  }

  /**
   * This method is used for testing. 
   * Examples are adding and deleting blocks directly.
   * The most common usage will be when the data node's storage is similated.
   * 
   * @return the fsdataset that stores the blocks
   */
  public FSDatasetInterface getFSDataset() {
    return data;
  }

  public static void secureMain(String args[], SecureResources resources) {
    try {
      StringUtils.startupShutdownMessage(DataNode.class, args, LOG);
      DataNode datanode = createDataNode(args, null, resources);
      if (datanode != null)
        datanode.join();
    } catch (Throwable e) {
      LOG.fatal("Exception in secureMain", e);
      terminate(1);
    } finally {
      // We need to terminate the process here because either shutdown was called
      // or some disk related conditions like volumes tolerated or volumes required
      // condition was not met. Also, In secure mode, control will go to Jsvc
      // and Datanode process hangs if it does not exit.
      LOG.warn("Exiting Datanode");
      terminate(0);
    }
  }
  
  public static void main(String args[]) {
    secureMain(args, null);
  }

  public Daemon recoverBlocks(final Collection<RecoveringBlock> blocks) {
    Daemon d = new Daemon(threadGroup, new Runnable() {
      /** Recover a list of blocks. It is run by the primary datanode. */
      public void run() {
        for(RecoveringBlock b : blocks) {
          try {
            logRecoverBlock("NameNode", b.getBlock(), b.getLocations());
            recoverBlock(b);
          } catch (IOException e) {
            LOG.warn("recoverBlocks FAILED: " + b, e);
          }
        }
      }
    });
    d.start();
    return d;
  }

  // InterDataNodeProtocol implementation
  @Override // InterDatanodeProtocol
  public ReplicaRecoveryInfo initReplicaRecovery(RecoveringBlock rBlock)
  throws IOException {
    return data.initReplicaRecovery(rBlock);
  }

  /**
   * Convenience method, which unwraps RemoteException.
   * @throws IOException not a RemoteException.
   */
  private static ReplicaRecoveryInfo callInitReplicaRecovery(
      InterDatanodeProtocol datanode,
      RecoveringBlock rBlock) throws IOException {
    try {
      return datanode.initReplicaRecovery(rBlock);
    } catch(RemoteException re) {
      throw re.unwrapRemoteException();
    }
  }

  /**
   * Update replica with the new generation stamp and length.  
   */
  @Override // InterDatanodeProtocol
  public ExtendedBlock updateReplicaUnderRecovery(ExtendedBlock oldBlock,
                                          long recoveryId,
                                          long newLength) throws IOException {
    ReplicaInfo r = data.updateReplicaUnderRecovery(oldBlock,
        recoveryId, newLength);
    return new ExtendedBlock(oldBlock.getBlockPoolId(), r);
  }

  /** {@inheritDoc} */
  public long getProtocolVersion(String protocol, long clientVersion
      ) throws IOException {
    if (protocol.equals(InterDatanodeProtocol.class.getName())) {
      return InterDatanodeProtocol.versionID; 
    } else if (protocol.equals(ClientDatanodeProtocol.class.getName())) {
      return ClientDatanodeProtocol.versionID; 
    }
    throw new IOException("Unknown protocol to " + getClass().getSimpleName()
        + ": " + protocol);
  }

  @Override
  public ProtocolSignature getProtocolSignature(String protocol,
      long clientVersion, int clientMethodsHash) throws IOException {
    return ProtocolSignature.getProtocolSignature(
        this, protocol, clientVersion, clientMethodsHash);
  }

  /** A convenient class used in block recovery */
  static class BlockRecord { 
    final DatanodeID id;
    final InterDatanodeProtocol datanode;
    final ReplicaRecoveryInfo rInfo;
    
    BlockRecord(DatanodeID id,
                InterDatanodeProtocol datanode,
                ReplicaRecoveryInfo rInfo) {
      this.id = id;
      this.datanode = datanode;
      this.rInfo = rInfo;
    }

    /** {@inheritDoc} */
    public String toString() {
      return "block:" + rInfo + " node:" + id;
    }
  }

  /** Recover a block */
  private void recoverBlock(RecoveringBlock rBlock) throws IOException {
    ExtendedBlock block = rBlock.getBlock();
    String blookPoolId = block.getBlockPoolId();
    DatanodeInfo[] targets = rBlock.getLocations();
    DatanodeID[] datanodeids = (DatanodeID[])targets;
    List<BlockRecord> syncList = new ArrayList<BlockRecord>(datanodeids.length);
    int errorCount = 0;

    //check generation stamps
    for(DatanodeID id : datanodeids) {
      try {
        BPOfferService bpos = blockPoolManager.get(blookPoolId);
        DatanodeRegistration bpReg = bpos.bpRegistration;
        InterDatanodeProtocol datanode = bpReg.equals(id)?
            this: DataNode.createInterDataNodeProtocolProxy(id, getConf(),
                dnConf.socketTimeout);
        ReplicaRecoveryInfo info = callInitReplicaRecovery(datanode, rBlock);
        if (info != null &&
            info.getGenerationStamp() >= block.getGenerationStamp() &&
            info.getNumBytes() > 0) {
          syncList.add(new BlockRecord(id, datanode, info));
        }
      } catch (RecoveryInProgressException ripE) {
        InterDatanodeProtocol.LOG.warn(
            "Recovery for replica " + block + " on data-node " + id
            + " is already in progress. Recovery id = "
            + rBlock.getNewGenerationStamp() + " is aborted.", ripE);
        return;
      } catch (IOException e) {
        ++errorCount;
        InterDatanodeProtocol.LOG.warn(
            "Failed to obtain replica info for block (=" + block 
            + ") from datanode (=" + id + ")", e);
      }
    }

    if (errorCount == datanodeids.length) {
      throw new IOException("All datanodes failed: block=" + block
          + ", datanodeids=" + Arrays.asList(datanodeids));
    }

    syncBlock(rBlock, syncList);
  }

  /**
   * Get namenode corresponding to a block pool
   * @param bpid Block pool Id
   * @return Namenode corresponding to the bpid
   * @throws IOException
   */
  public DatanodeProtocol getBPNamenode(String bpid) throws IOException {
    BPOfferService bpos = blockPoolManager.get(bpid);
    if (bpos == null) {
      throw new IOException("No block pool offer service for bpid=" + bpid);
    } else if (bpos.bpNamenode == null) {
      throw new IOException("cannot find a namenode proxy for bpid=" + bpid);
    }
    return bpos.bpNamenode;
  }

  /** Block synchronization */
  void syncBlock(RecoveringBlock rBlock,
                         List<BlockRecord> syncList) throws IOException {
    ExtendedBlock block = rBlock.getBlock();
    DatanodeProtocol nn = getBPNamenode(block.getBlockPoolId());
    
    long recoveryId = rBlock.getNewGenerationStamp();
    if (LOG.isDebugEnabled()) {
      LOG.debug("block=" + block + ", (length=" + block.getNumBytes()
          + "), syncList=" + syncList);
    }

    // syncList.isEmpty() means that all data-nodes do not have the block
    // or their replicas have 0 length.
    // The block can be deleted.
    if (syncList.isEmpty()) {
      nn.commitBlockSynchronization(block, recoveryId, 0,
          true, true, DatanodeID.EMPTY_ARRAY);
      return;
    }

    // Calculate the best available replica state.
    ReplicaState bestState = ReplicaState.RWR;
    long finalizedLength = -1;
    for(BlockRecord r : syncList) {
      assert r.rInfo.getNumBytes() > 0 : "zero length replica";
      ReplicaState rState = r.rInfo.getOriginalReplicaState(); 
      if(rState.getValue() < bestState.getValue())
        bestState = rState;
      if(rState == ReplicaState.FINALIZED) {
        if(finalizedLength > 0 && finalizedLength != r.rInfo.getNumBytes())
          throw new IOException("Inconsistent size of finalized replicas. " +
              "Replica " + r.rInfo + " expected size: " + finalizedLength);
        finalizedLength = r.rInfo.getNumBytes();
      }
    }

    // Calculate list of nodes that will participate in the recovery
    // and the new block size
    List<BlockRecord> participatingList = new ArrayList<BlockRecord>();
    final ExtendedBlock newBlock = new ExtendedBlock(block.getBlockPoolId(), block
        .getBlockId(), -1, recoveryId);
    switch(bestState) {
    case FINALIZED:
      assert finalizedLength > 0 : "finalizedLength is not positive";
      for(BlockRecord r : syncList) {
        ReplicaState rState = r.rInfo.getOriginalReplicaState();
        if(rState == ReplicaState.FINALIZED ||
           rState == ReplicaState.RBW &&
                      r.rInfo.getNumBytes() == finalizedLength)
          participatingList.add(r);
      }
      newBlock.setNumBytes(finalizedLength);
      break;
    case RBW:
    case RWR:
      long minLength = Long.MAX_VALUE;
      for(BlockRecord r : syncList) {
        ReplicaState rState = r.rInfo.getOriginalReplicaState();
        if(rState == bestState) {
          minLength = Math.min(minLength, r.rInfo.getNumBytes());
          participatingList.add(r);
        }
      }
      newBlock.setNumBytes(minLength);
      break;
    case RUR:
    case TEMPORARY:
      assert false : "bad replica state: " + bestState;
    }

    List<DatanodeID> failedList = new ArrayList<DatanodeID>();
    List<DatanodeID> successList = new ArrayList<DatanodeID>();
    for(BlockRecord r : participatingList) {
      try {
        ExtendedBlock reply = r.datanode.updateReplicaUnderRecovery(
            new ExtendedBlock(newBlock.getBlockPoolId(), r.rInfo), recoveryId,
            newBlock.getNumBytes());
        assert reply.equals(newBlock) &&
               reply.getNumBytes() == newBlock.getNumBytes() :
          "Updated replica must be the same as the new block.";
        successList.add(r.id);
      } catch (IOException e) {
        InterDatanodeProtocol.LOG.warn("Failed to updateBlock (newblock="
            + newBlock + ", datanode=" + r.id + ")", e);
        failedList.add(r.id);
      }
    }

    // If any of the data-nodes failed, the recovery fails, because
    // we never know the actual state of the replica on failed data-nodes.
    // The recovery should be started over.
    if(!failedList.isEmpty()) {
      StringBuilder b = new StringBuilder();
      for(DatanodeID id : failedList) {
        b.append("\n  " + id);
      }
      throw new IOException("Cannot recover " + block + ", the following "
          + failedList.size() + " data-nodes failed {" + b + "\n}");
    }

    // Notify the name-node about successfully recovered replicas.
    DatanodeID[] nlist = successList.toArray(new DatanodeID[successList.size()]);
    nn.commitBlockSynchronization(block,
        newBlock.getGenerationStamp(), newBlock.getNumBytes(), true, false,
        nlist);
  }
  
  private static void logRecoverBlock(String who,
      ExtendedBlock block, DatanodeID[] targets) {
    StringBuilder msg = new StringBuilder(targets[0].getName());
    for (int i = 1; i < targets.length; i++) {
      msg.append(", " + targets[i].getName());
    }
    LOG.info(who + " calls recoverBlock(block=" + block
        + ", targets=[" + msg + "])");
  }

  // ClientDataNodeProtocol implementation
  /** {@inheritDoc} */
  @Override // ClientDataNodeProtocol
  public long getReplicaVisibleLength(final ExtendedBlock block) throws IOException {
    checkWriteAccess(block);
    return data.getReplicaVisibleLength(block);
  }

  private void checkWriteAccess(final ExtendedBlock block) throws IOException {
    if (isBlockTokenEnabled) {
      Set<TokenIdentifier> tokenIds = UserGroupInformation.getCurrentUser()
          .getTokenIdentifiers();
      if (tokenIds.size() != 1) {
        throw new IOException("Can't continue since none or more than one "
            + "BlockTokenIdentifier is found.");
      }
      for (TokenIdentifier tokenId : tokenIds) {
        BlockTokenIdentifier id = (BlockTokenIdentifier) tokenId;
        if (LOG.isDebugEnabled()) {
          LOG.debug("Got: " + id.toString());
        }
        blockPoolTokenSecretManager.checkAccess(id, null, block,
            BlockTokenSecretManager.AccessMode.READ);
      }
    }
  }

  /**
   * Transfer a replica to the datanode targets.
   * @param b the block to transfer.
   *          The corresponding replica must be an RBW or a Finalized.
   *          Its GS and numBytes will be set to
   *          the stored GS and the visible length. 
   * @param targets
   * @param client
   */
  void transferReplicaForPipelineRecovery(final ExtendedBlock b,
      final DatanodeInfo[] targets, final String client) throws IOException {
    final long storedGS;
    final long visible;
    final BlockConstructionStage stage;

    //get replica information
    synchronized(data) {
      if (data.isValidRbw(b)) {
        stage = BlockConstructionStage.TRANSFER_RBW;
      } else if (data.isValidBlock(b)) {
        stage = BlockConstructionStage.TRANSFER_FINALIZED;
      } else {
        final String r = data.getReplicaString(b.getBlockPoolId(), b.getBlockId());
        throw new IOException(b + " is neither a RBW nor a Finalized, r=" + r);
      }

      storedGS = data.getStoredBlock(b.getBlockPoolId(),
          b.getBlockId()).getGenerationStamp();
      if (storedGS < b.getGenerationStamp()) {
        throw new IOException(
            storedGS + " = storedGS < b.getGenerationStamp(), b=" + b);        
      }
      visible = data.getReplicaVisibleLength(b);
    }

    //set storedGS and visible length
    b.setGenerationStamp(storedGS);
    b.setNumBytes(visible);

    if (targets.length > 0) {
      new DataTransfer(targets, b, stage, client).run();
    }
  }

  /**
   * Finalize a pending upgrade in response to DNA_FINALIZE.
   * @param blockPoolId the block pool to finalize
   */
  void finalizeUpgradeForPool(String blockPoolId) throws IOException {
    storage.finalizeUpgrade(blockPoolId);
  }

  // Determine a Datanode's streaming address
  public static InetSocketAddress getStreamingAddr(Configuration conf) {
    return NetUtils.createSocketAddr(
        conf.get(DFS_DATANODE_ADDRESS_KEY, DFS_DATANODE_ADDRESS_DEFAULT));
  }
  
  @Override // DataNodeMXBean
  public String getVersion() {
    return VersionInfo.getVersion();
  }
  
  @Override // DataNodeMXBean
  public String getRpcPort(){
    InetSocketAddress ipcAddr = NetUtils.createSocketAddr(
        this.getConf().get(DFS_DATANODE_IPC_ADDRESS_KEY));
    return Integer.toString(ipcAddr.getPort());
  }

  @Override // DataNodeMXBean
  public String getHttpPort(){
    return this.getConf().get("dfs.datanode.info.port");
  }
  
  public int getInfoPort(){
    return this.infoServer.getPort();
  }

  /**
   * Returned information is a JSON representation of a map with 
   * name node host name as the key and block pool Id as the value
   */
  @Override // DataNodeMXBean
  public String getNamenodeAddresses() {
    final Map<String, String> info = new HashMap<String, String>();
    for (BPOfferService bpos : blockPoolManager.getAllNamenodeThreads()) {
      if (bpos != null && bpos.bpThread != null) {
        info.put(bpos.getNNSocketAddress().getHostName(), bpos.getBlockPoolId());
      }
    }
    return JSON.toString(info);
  }

  /**
   * Returned information is a JSON representation of a map with 
   * volume name as the key and value is a map of volume attribute 
   * keys to its values
   */
  @Override // DataNodeMXBean
  public String getVolumeInfo() {
    return JSON.toString(data.getVolumeInfoMap());
  }
  
  @Override // DataNodeMXBean
  public synchronized String getClusterId() {
    return clusterId;
  }
  
  public void refreshNamenodes(Configuration conf) throws IOException {
    try {
      blockPoolManager.refreshNamenodes(conf);
    } catch (InterruptedException ex) {
      IOException eio = new IOException();
      eio.initCause(ex);
      throw eio;
    }
  }

  @Override //ClientDatanodeProtocol
  public void refreshNamenodes() throws IOException {
    conf = new Configuration();
    refreshNamenodes(conf);
  }
  
  @Override // ClientDatanodeProtocol
  public void deleteBlockPool(String blockPoolId, boolean force)
      throws IOException {
    LOG.info("deleteBlockPool command received for block pool " + blockPoolId
        + ", force=" + force);
    if (blockPoolManager.get(blockPoolId) != null) {
      LOG.warn("The block pool "+blockPoolId+
          " is still running, cannot be deleted.");
      throw new IOException(
          "The block pool is still running. First do a refreshNamenodes to " +
          "shutdown the block pool service");
    }
   
    data.deleteBlockPool(blockPoolId, force);
  }

  /**
   * @param addr rpc address of the namenode
   * @return true - if BPOfferService corresponding to the namenode is alive
   */
  public boolean isBPServiceAlive(InetSocketAddress addr) {
    BPOfferService bp = blockPoolManager.get(addr);
    return bp != null ? bp.isAlive() : false;
  }
  
  /**
   * @param bpid block pool Id
   * @return true - if BPOfferService thread is alive
   */
  public boolean isBPServiceAlive(String bpid) {
    BPOfferService bp = blockPoolManager.get(bpid);
    return bp != null ? bp.isAlive() : false;
  }

  /**
   * A datanode is considered to be fully started if all the BP threads are
   * alive and all the block pools are initialized.
   * 
   * @return true - if the data node is fully started
   */
  public boolean isDatanodeFullyStarted() {
    for (BPOfferService bp : blockPoolManager.getAllNamenodeThreads()) {
      if (!bp.isInitialized() || !bp.isAlive()) {
        return false;
      }
    }
    return true;
  }
  
  /** Methods used by fault injection tests */
  public DatanodeID getDatanodeId() {
    return new DatanodeID(getMachineName(), getStorageId(),
        infoServer.getPort(), getIpcPort());
  }

  /**
   * Get current value of the max balancer bandwidth in bytes per second.
   *
   * @return bandwidth Blanacer bandwidth in bytes per second for this datanode.
   */
  public Long getBalancerBandwidth() {
    DataXceiverServer dxcs =
                       (DataXceiverServer) this.dataXceiverServer.getRunnable();
    return dxcs.balanceThrottler.getBandwidth();
  }
  
  DNConf getDnConf() {
    return dnConf;
  }

  boolean shouldRun() {
    return shouldRun;
  }
}
