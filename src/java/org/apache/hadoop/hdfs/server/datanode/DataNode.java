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


import static org.apache.hadoop.hdfs.server.common.Util.now;

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.lang.management.ManagementFactory;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.net.URI;
import java.net.UnknownHostException;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.security.NoSuchAlgorithmException;
import java.security.PrivilegedExceptionAction;
import java.security.SecureRandom;
import java.util.AbstractList;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import javax.management.MBeanServer;
import javax.management.ObjectName;

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
import org.apache.hadoop.hdfs.protocol.BlockListAsLongs;
import org.apache.hadoop.hdfs.protocol.ClientDatanodeProtocol;
import org.apache.hadoop.hdfs.protocol.DataTransferProtocol;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.FSConstants;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.RecoveryInProgressException;
import org.apache.hadoop.hdfs.protocol.UnregisteredNodeException;
import org.apache.hadoop.hdfs.protocol.DataTransferProtocol.BlockConstructionStage;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenIdentifier;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenSecretManager;
import org.apache.hadoop.hdfs.security.token.block.ExportedBlockKeys;
import org.apache.hadoop.hdfs.server.common.HdfsConstants;
import org.apache.hadoop.hdfs.server.common.IncorrectVersionException;
import org.apache.hadoop.hdfs.server.common.JspHelper;
import org.apache.hadoop.hdfs.server.common.Storage;
import org.apache.hadoop.hdfs.server.common.Util;
import org.apache.hadoop.hdfs.server.common.HdfsConstants.ReplicaState;
import org.apache.hadoop.hdfs.server.common.HdfsConstants.StartupOption;
import org.apache.hadoop.hdfs.server.datanode.FSDataset.VolumeInfo;
import org.apache.hadoop.hdfs.server.datanode.SecureDataNodeStarter.SecureResources;
import org.apache.hadoop.hdfs.server.datanode.metrics.DataNodeMetrics;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.FileChecksumServlets;
import org.apache.hadoop.hdfs.server.namenode.StreamFile;
import org.apache.hadoop.hdfs.server.protocol.BlockCommand;
import org.apache.hadoop.hdfs.server.protocol.BlockRecoveryCommand;
import org.apache.hadoop.hdfs.server.protocol.DatanodeCommand;
import org.apache.hadoop.hdfs.server.protocol.DatanodeProtocol;
import org.apache.hadoop.hdfs.server.protocol.DatanodeRegistration;
import org.apache.hadoop.hdfs.server.protocol.DisallowedDatanodeException;
import org.apache.hadoop.hdfs.server.protocol.InterDatanodeProtocol;
import org.apache.hadoop.hdfs.server.protocol.KeyUpdateCommand;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo;
import org.apache.hadoop.hdfs.server.protocol.ReplicaRecoveryInfo;
import org.apache.hadoop.hdfs.server.protocol.UpgradeCommand;
import org.apache.hadoop.hdfs.server.protocol.BlockRecoveryCommand.RecoveringBlock;
import org.apache.hadoop.http.HttpServer;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.net.DNS;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.AccessControlList;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.util.Daemon;
import org.apache.hadoop.util.DiskChecker;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.ServicePlugin;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.VersionInfo;
import org.apache.hadoop.util.DiskChecker.DiskErrorException;
import org.apache.hadoop.util.DiskChecker.DiskOutOfSpaceException;
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
    implements InterDatanodeProtocol, ClientDatanodeProtocol, FSConstants,
    DataNodeMXBean {
  public static final Log LOG = LogFactory.getLog(DataNode.class);
  
  static{
    Configuration.addDefaultResource("hdfs-default.xml");
    Configuration.addDefaultResource("hdfs-site.xml");
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
    private final DatanodeRegistration dnReg;
    
    //This lock is used only to ensure exclusion of refreshNamenodes
    private final Object refreshNamenodesLock = new Object();
    
    BlockPoolManager(Configuration conf, DatanodeRegistration dnReg)
        throws IOException {
      this.dnReg = dnReg;
      bpMapping = new HashMap<String, BPOfferService>();
      nameNodeThreads = new HashMap<InetSocketAddress, BPOfferService>();
  
      List<InetSocketAddress> isas = DFSUtil.getNNAddresses(conf);
      for(InetSocketAddress isa : isas) {
        BPOfferService bpos = new BPOfferService(isa, dnReg);
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
        bpos.stop();
      }
    }
    
    synchronized void startAll() {
      for (BPOfferService bpos: nameNodeThreads.values()) {
        bpos.start();
      }
    }
    
    void joinAll() throws InterruptedException {
      for (BPOfferService bpos: this.getAllNamenodeThreads()) {
        bpos.join();
      }
    }
    
    void refreshNamenodes(Configuration conf)
        throws IOException, InterruptedException {
      List<InetSocketAddress> newAddresses = DFSUtil.getNNAddresses(conf);
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
            BPOfferService bpos = new BPOfferService(nnaddr, dnReg);
            nameNodeThreads.put(bpos.getNNSocketAddress(), bpos);
          }

          for (BPOfferService bpos : toShutdown) {
            remove(bpos);
          }
        }

        for (BPOfferService bpos : toShutdown) {
          bpos.stop();
        }
        // Now start the threads that are not already running.
        startAll();
      }
    }
  }
  
  volatile boolean shouldRun = true;
  private BlockPoolManager blockPoolManager;
  public DatanodeProtocol namenodeTODO_FED = null; //TODO:FEDERATION needs to be taken out.
  public FSDatasetInterface data = null;
  public DatanodeRegistration dnRegistration = null;
  private String clusterId = null;

  public final static String EMPTY_DEL_HINT = "";
  AtomicInteger xmitsInProgress = new AtomicInteger();
  Daemon dataXceiverServer = null;
  ThreadGroup threadGroup = null;
  long blockReportInterval;
  boolean resetBlockReportTime = true;
  long initialBlockReportDelay = BLOCKREPORT_INITIAL_DELAY * 1000L;
  long heartBeatInterval;
  private DataStorage storage = null;
  private HttpServer infoServer = null;
  DataNodeMetrics myMetrics;
  private InetSocketAddress nameNodeAddr;
  private InetSocketAddress nameNodeAddrForClient;
  private InetSocketAddress selfAddr;
  static DataNode datanodeObject = null;
  String machineName;
  private static String dnThreadName;
  int socketTimeout;
  int socketWriteTimeout = 0;  
  boolean transferToAllowed = true;
  int writePacketSize = 0;
  boolean isBlockTokenEnabled;
  BlockTokenSecretManager blockTokenSecretManager;
  boolean isBlockTokenInitialized = false;
  
  public DataBlockScanner blockScanner = null;
  public Daemon blockScannerThread = null;
  
  /** Activated plug-ins. */
  private List<ServicePlugin> plugins;
  
  private static final Random R = new Random();
  
  // For InterDataNodeProtocol
  public Server ipcServer;

  private SecureResources secureResources = null;
  private AbstractList<File> dataDirs;
  private Configuration conf;

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

    DataNode.setDataNode(this);
    
    try {
      startDataNode(conf, dataDirs, resources);
    } catch (IOException ie) {
      shutdown();
      throw ie;
    }
  }

  private synchronized void setClusterId(String cid) {
    if(clusterId==null) {
      clusterId = cid;
    }
  }

  private void initConfig(Configuration conf) throws UnknownHostException {
    // use configured nameserver & interface to get local hostname
    if (conf.get(DFSConfigKeys.DFS_DATANODE_HOST_NAME_KEY) != null) {
      machineName = conf.get(DFSConfigKeys.DFS_DATANODE_HOST_NAME_KEY);   
    }
    if (machineName == null) {
      machineName = DNS.getDefaultHost(
                                     conf.get("dfs.datanode.dns.interface","default"),
                                     conf.get("dfs.datanode.dns.nameserver","default"));
    }

    this.socketTimeout =  conf.getInt(DFSConfigKeys.DFS_CLIENT_SOCKET_TIMEOUT_KEY,
                                      HdfsConstants.READ_TIMEOUT);
    this.socketWriteTimeout = conf.getInt("dfs.datanode.socket.write.timeout",
                                          HdfsConstants.WRITE_TIMEOUT);
    /* Based on results on different platforms, we might need set the default 
     * to false on some of them. */
    this.transferToAllowed = conf.getBoolean("dfs.datanode.transferTo.allowed", 
                                             true);
    this.writePacketSize = conf.getInt(DFSConfigKeys.DFS_CLIENT_WRITE_PACKET_SIZE_KEY, 
                                       DFSConfigKeys.DFS_CLIENT_WRITE_PACKET_SIZE_DEFAULT);

    this.blockReportInterval =
      conf.getLong("dfs.blockreport.intervalMsec", BLOCKREPORT_INTERVAL);
    this.initialBlockReportDelay = conf.getLong("dfs.blockreport.initialDelay",
                                            BLOCKREPORT_INITIAL_DELAY)* 1000L; 
    if (this.initialBlockReportDelay >= blockReportInterval) {
      this.initialBlockReportDelay = 0;
      LOG.info("dfs.blockreport.initialDelay is greater than " +
        "dfs.blockreport.intervalMsec." + " Setting initial delay to 0 msec:");
    }
    this.heartBeatInterval = conf.getLong("dfs.heartbeat.interval", HEARTBEAT_INTERVAL) * 1000L;
  }
  
  private void startInfoServer(Configuration conf) throws IOException {
    // create a servlet to serve full-file content
    InetSocketAddress infoSocAddr = DataNode.getInfoAddr(conf);
    String infoHost = infoSocAddr.getHostName();
    int tmpInfoPort = infoSocAddr.getPort();
    this.infoServer = (secureResources == null) 
       ? new HttpServer("datanode", infoHost, tmpInfoPort, tmpInfoPort == 0, 
           conf, new AccessControlList(conf.get(DFSConfigKeys.DFS_ADMIN, " ")))
       : new HttpServer("datanode", infoHost, tmpInfoPort, tmpInfoPort == 0,
           conf, new AccessControlList(conf.get(DFSConfigKeys.DFS_ADMIN, " ")),
           secureResources.getListener());
    if(LOG.isDebugEnabled()) {
      LOG.debug("Datanode listening on " + infoHost + ":" + tmpInfoPort);
    }
    if (conf.getBoolean("dfs.https.enable", false)) {
      boolean needClientAuth = conf.getBoolean(DFSConfigKeys.DFS_CLIENT_HTTPS_NEED_AUTH_KEY,
                                               DFSConfigKeys.DFS_CLIENT_HTTPS_NEED_AUTH_DEFAULT);
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
    this.infoServer.setAttribute("datanode.blockScanner", blockScanner);
    this.infoServer.setAttribute(JspHelper.CURRENT_CONF, conf);
    this.infoServer.addServlet(null, "/blockScannerReport", 
                               DataBlockScanner.Servlet.class);
    this.infoServer.start();
    // adjust info port
    this.dnRegistration.setInfoPort(this.infoServer.getPort());
  }
  
  private void startPlugins(Configuration conf) {
    plugins = conf.getInstances("dfs.datanode.plugins", ServicePlugin.class);
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
        ipcAddr.getPort(), conf.getInt("dfs.datanode.handler.count", 3), false,
        conf, blockTokenSecretManager);
    
    // set service-level authorization security policy
    if (conf.getBoolean(
        CommonConfigurationKeys.HADOOP_SECURITY_AUTHORIZATION, false)) {
      ipcServer.refreshServiceAcl(conf, new HDFSPolicyProvider());
    }

    dnRegistration.setIpcPort(ipcServer.getListenerAddress().getPort());
    LOG.info("dnRegistration = " + dnRegistration);
  }
  

  private void initBlockScanner(Configuration conf) {
    String reason = null;
    if (conf.getInt("dfs.datanode.scan.period.hours", 0) < 0) {
      reason = "verification is turned off by configuration";
    } else if ( !(data instanceof FSDataset) ) {
      reason = "verifcation is supported only with FSDataset";
    } 
    if ( reason == null ) {
      blockScanner = new DataBlockScanner(this, (FSDataset)data, conf);
    } else {
      LOG.info("Periodic Block Verification is disabled because " +
               reason + ".");
    }
  }
  
  private void initDataXceiver(Configuration conf) throws IOException {
    // construct registration
    InetSocketAddress socAddr = DataNode.getStreamingAddr(conf);
    int tmpPort = socAddr.getPort();
    this.dnRegistration = new DatanodeRegistration(machineName + ":" + tmpPort);

    // find free port or use privileged port provided
    ServerSocket ss;
    if(secureResources == null) {
      ss = (socketWriteTimeout > 0) ? 
          ServerSocketChannel.open().socket() : new ServerSocket();
          Server.bind(ss, socAddr, 0);
    } else {
      ss = secureResources.getStreamingSocket();
    }
    ss.setReceiveBufferSize(DEFAULT_DATA_SOCKET_SIZE); 
    // adjust machine name with the actual port
    tmpPort = ss.getLocalPort();
    selfAddr = new InetSocketAddress(ss.getInetAddress().getHostAddress(),
                                     tmpPort);
    this.dnRegistration.setName(machineName + ":" + tmpPort);
    LOG.info("Opened info server at " + tmpPort);
      
    this.threadGroup = new ThreadGroup("dataXceiverServer");
    this.dataXceiverServer = new Daemon(threadGroup, 
        new DataXceiverServer(ss, conf, this));
    this.threadGroup.setDaemon(true); // auto destroy when empty
  }
  
  // calls specific to BP
  protected void notifyNamenodeReceivedBlock(ExtendedBlock block, String delHint) {
    BPOfferService bpos = blockPoolManager.get(block.getPoolId());
    if(bpos != null) {
      bpos.notifyNamenodeReceivedBlock(block, delHint); 
    } else {
      LOG.warn("Cannot find BPOfferService for reporting block received for bpid="
          + block.getPoolId());
    }
  }
  
  public void reportBadBlocks(ExtendedBlock block) throws IOException{
    BPOfferService bpos = blockPoolManager.get(block.getPoolId());
    if(bpos == null || bpos.bpNamenode == null) {
      throw new IOException("cannot locate OfferService thread for bp="+block.getPoolId());
    }
    bpos.reportBadBlocks(block);
  }
  
  /**
   * A thread per namenode to perform:
   * <ul>
   * <li> Pre-registration handshake with namenode</li>
   * <li> Registration with namenode</li>
   * <li> Send periodic heartbeats to the namenode</li>
   * <li> Handle commands received from the datanode</li>
   * </ul>
   */
  @InterfaceAudience.Private
  class BPOfferService implements Runnable {
    final InetSocketAddress nnAddr;
    DatanodeRegistration bpRegistration;
    NamespaceInfo bpNSInfo;
    long lastBlockReport = 0;
    private Thread bpThread;
    private DatanodeProtocol bpNamenode;
    private String blockPoolId;
    private long lastHeartbeat = 0;
    private volatile boolean initialized = false;
    private final LinkedList<Block> receivedBlockList = new LinkedList<Block>();
    private final LinkedList<String> delHints = new LinkedList<String>();
    private volatile boolean shouldServiceRun = true;

    BPOfferService(InetSocketAddress isa, DatanodeRegistration bpRegistration) {
      this.bpRegistration = new DatanodeRegistration(bpRegistration);
      this.nnAddr = isa;
    }

    /**
     * returns true if BP thread has completed initialization of storage
     * and has registered with the corresponding namenode
     * @return true if initialized
     */
    public boolean initialized() {
      return initialized;
    }
    
    public String getBlockPoolId() {
      return blockPoolId;
    }
    
    private InetSocketAddress getNNSocketAddress() {
      return nnAddr;
    }
 
    void setNamespaceInfo(NamespaceInfo nsinfo) {
      bpNSInfo = nsinfo;
      this.blockPoolId = nsinfo.getBlockPoolID();
      blockPoolManager.addBlockPool(this);
    }

    void setNameNode(DatanodeProtocol dnProtocol) {
      this.bpNamenode = dnProtocol;
    }

    private NamespaceInfo handshake() throws IOException {
      NamespaceInfo nsInfo = new NamespaceInfo();
      while (shouldRun && shouldServiceRun) {
        try {
          nsInfo = bpNamenode.versionRequest();
          break;
        } catch(SocketTimeoutException e) {  // namenode is busy
          LOG.info("Problem connecting to server: " + nnAddr);
          try {
            Thread.sleep(1000);
          } catch (InterruptedException ie) {}
        }
      }
      LOG.info("handshake: namespace info = " + nsInfo);
      // TODO:FEDERATION on version mismatch datanode should continue
      // to retry
      // verify build version
      if(! nsInfo.getBuildVersion().equals(Storage.getBuildVersion())) {
        String errorMsg = "Incompatible build versions: namenode BV = " 
          + nsInfo.getBuildVersion() + "; datanode BV = "
          + Storage.getBuildVersion();
        LOG.fatal(errorMsg);
        try {
          bpNamenode.errorReport( bpRegistration,
              DatanodeProtocol.NOTIFY, errorMsg );
        } catch( SocketTimeoutException e ) {  // namenode is busy
          LOG.info("Problem connecting to server: " + nnAddr);
        }
        throw new IOException( errorMsg );
      }
      assert FSConstants.LAYOUT_VERSION == nsInfo.getLayoutVersion() :
        "Data-node and name-node layout versions must be the same."
        + "Expected: "+ FSConstants.LAYOUT_VERSION 
        + " actual "+ nsInfo.getLayoutVersion();
      return nsInfo;
    }


    void setupBP(Configuration conf, AbstractList<File> dataDirs) 
    throws IOException {
      // get NN proxy
      DatanodeProtocol dnp = 
        (DatanodeProtocol)RPC.waitForProxy(DatanodeProtocol.class,
            DatanodeProtocol.versionID, nnAddr, conf);
      LOG.info("NN proxy created in BP="+blockPoolId + " for " + nnAddr);
      setNameNode(dnp);

      // handshake with NN
      NamespaceInfo nsInfo = handshake();
      LOG.info("received namespace info  nsInfo=" + nsInfo);
      setNamespaceInfo(nsInfo);
      setClusterId(nsInfo.clusterID);
      
      StartupOption startOpt = getStartupOption(conf);
      assert startOpt != null : "Startup option must be set.";

      boolean simulatedFSDataset = 
        conf.getBoolean("dfs.datanode.simulateddatastorage", false);
      
      if (simulatedFSDataset) {
        bpRegistration.setStorageID(dnRegistration.getStorageID()); //same as DN
        bpRegistration.storageInfo.layoutVersion = FSConstants.LAYOUT_VERSION;
        bpRegistration.storageInfo.namespaceID = bpNSInfo.namespaceID;
        bpRegistration.storageInfo.clusterID = bpNSInfo.clusterID;
        // TODO: FEDERATION 
        //bpRegistration.storageInfo.blockpoolID = bpNSInfo.blockpoolID;
      } else {
        // read storage info, lock data dirs and transition fs state if necessary          
        storage.recoverTransitionRead(blockPoolId, bpNSInfo, dataDirs, startOpt);
        LOG.info("setting up storage: nsid=" + storage.namespaceID + ";bpid="
            + blockPoolId + ";lv=" + storage.layoutVersion + ";nsInfo="
            + bpNSInfo);

        bpRegistration.setStorageID(storage.getStorageID());
        bpRegistration.setStorageInfo(storage.getBPStorage(blockPoolId));
      }
      initFsDataSet(conf, dataDirs);
      data.addBlockPool(blockPoolId, conf);
    }

    /**
     * This methods  arranges for the data node to send the block report at 
     * the next heartbeat.
     */
    void scheduleBlockReport(long delay) {
      if (delay > 0) { // send BR after random delay
        lastBlockReport = System.currentTimeMillis()
        - ( blockReportInterval - R.nextInt((int)(delay)));
      } else { // send at next heartbeat
        lastBlockReport = lastHeartbeat - blockReportInterval;
      }
      resetBlockReportTime = true; // reset future BRs for randomness
    }

    private void reportBadBlocks(ExtendedBlock block) {
      DatanodeInfo[] dnArr = { new DatanodeInfo(bpRegistration) };
      LocatedBlock[] blocks = { new LocatedBlock(block, dnArr) }; 
      
      try {
        bpNamenode.reportBadBlocks(blocks);  
      } catch (IOException e){
        /* One common reason is that NameNode could be in safe mode.
         * Should we keep on retrying in that case?
         */
        LOG.warn("Failed to report bad block " + block + " to namenode : " +
                 " Exception : " + StringUtils.stringifyException(e));
      }
      
    }
    
    /**
     * Report received blocks and delete hints to the Namenode
     * @throws IOException
     */
    private void reportReceivedBlocks() throws IOException {
      //check if there are newly received blocks
      Block [] blockArray=null;
      String [] delHintArray=null;
      synchronized(receivedBlockList) {
        synchronized(delHints){
          int numBlocks = receivedBlockList.size();
          if (numBlocks > 0) {
            if(numBlocks!=delHints.size()) {
              LOG.warn("Panic: receiveBlockList and delHints are not of " +
              "the same length" );
            }
            //
            // Send newly-received blockids to namenode
            //
            blockArray = receivedBlockList.toArray(new Block[numBlocks]);
            delHintArray = delHints.toArray(new String[numBlocks]);
          }
        }
      }
      if (blockArray != null) {
        if(delHintArray == null || delHintArray.length != blockArray.length ) {
          LOG.warn("Panic: block array & delHintArray are not the same" );
        }
        bpNamenode.blockReceived(bpRegistration, blockPoolId, blockArray,
            delHintArray);
        synchronized(receivedBlockList) {
          synchronized(delHints){
            for(int i=0; i<blockArray.length; i++) {
              receivedBlockList.remove(blockArray[i]);
              delHints.remove(delHintArray[i]);
            }
          }
        }
      }
    }

    /*
     * Informing the name node could take a long long time! Should we wait
     * till namenode is informed before responding with success to the
     * client? For now we don't.
     */
    void notifyNamenodeReceivedBlock(ExtendedBlock block, String delHint) {
      if(block==null || delHint==null) {
        throw new IllegalArgumentException(
            block==null?"Block is null":"delHint is null");
      }
      
      if (!block.getPoolId().equals(blockPoolId)) {
        LOG.warn("BlockPool mismatch " + block.getPoolId() + 
            " vs. " + blockPoolId);
        return;
      }
      
      synchronized (receivedBlockList) {
        synchronized (delHints) {
          receivedBlockList.add(block.getLocalBlock());
          delHints.add(delHint);
          receivedBlockList.notifyAll();
        }
      }
    }


    /**
     * Report the list blocks to the Namenode
     * @throws IOException
     */
    DatanodeCommand blockReport() throws IOException {
      // send block report
      DatanodeCommand cmd = null;
      long startTime = now();
      if (startTime - lastBlockReport > blockReportInterval) {
        //
        // Send latest block report if timer has expired.
        // Get back a list of local block(s) that are obsolete
        // and can be safely GC'ed.
        //
        long brStartTime = now();
        BlockListAsLongs bReport = data.getBlockReport(blockPoolId);
        cmd = bpNamenode.blockReport(bpRegistration, blockPoolId, bReport
            .getBlockListAsLongs());
        long brTime = now() - brStartTime;
        myMetrics.blockReports.inc(brTime);
        LOG.info("BlockReport of " + bReport.getNumberOfBlocks() +
            " blocks got processed in " + brTime + " msecs");
        //
        // If we have sent the first block report, then wait a random
        // time before we start the periodic block reports.
        //
        if (resetBlockReportTime) {
          lastBlockReport = startTime - R.nextInt((int)(blockReportInterval));
          resetBlockReportTime = false;
        } else {
          /* say the last block report was at 8:20:14. The current report
           * should have started around 9:20:14 (default 1 hour interval).
           * If current time is :
           *   1) normal like 9:20:18, next report should be at 10:20:14
           *   2) unexpected like 11:35:43, next report should be at 12:20:14
           */
          lastBlockReport += (now() - lastBlockReport) /
          blockReportInterval * blockReportInterval;
        }
        LOG.info("sent block report, processed command:" + cmd);
      }
      return cmd;
    }
    
    
    DatanodeCommand [] sendHeartBeat() throws IOException {
      return bpNamenode.sendHeartbeat(bpRegistration,
          data.getCapacity(),
          data.getDfsUsed(),
          data.getRemaining(),
          xmitsInProgress.get(),
          getXceiverCount());
    }
    
    //This must be called only by blockPoolManager
    void start() {
      if ((bpThread != null) && (bpThread.isAlive())) {
        //Thread is started already
        return;
      }
      bpThread = new Thread(this, dnThreadName);
      bpThread.setDaemon(true); // needed for JUnit testing
      bpThread.start();
    }
    
    //This must be called only by blockPoolManager.
    void stop() {
      shouldServiceRun = false;
      if (bpThread != null) {
        try {
          bpThread.interrupt();
          bpThread.join();
        } catch (InterruptedException ex) {
          LOG.warn("Received exception: ", ex);
        }
      }
    }
    
    //This must be called only by blockPoolManager
    void join() throws InterruptedException {
      if (bpThread != null) {
        bpThread.join();
      }
    }
    
    //Cleanup method to be called by current thread before exiting.
    private void cleanUp() {
      blockPoolManager.remove(this);
      shouldServiceRun = false;
      RPC.stopProxy(bpNamenode);
    }

    /**
     * Main loop for each BP thread. Run until shutdown,
     * forever calling remote NameNode functions.
     */
    private void offerService() throws Exception {
      LOG.info("For namenode " + nnAddr + " using BLOCKREPORT_INTERVAL of "
          + blockReportInterval + "msec" + " Initial delay: "
          + initialBlockReportDelay + "msec" + "; heartBeatInterval="
          + heartBeatInterval);

      //
      // Now loop for a long time....
      //
      while (shouldRun && shouldServiceRun) {
        try {
          long startTime = now();

          //
          // Every so often, send heartbeat or block-report
          //
          if (startTime - lastHeartbeat > heartBeatInterval) {
            //
            // All heartbeat messages include following info:
            // -- Datanode name
            // -- data transfer port
            // -- Total capacity
            // -- Bytes remaining
            //
            lastHeartbeat = startTime;
            // TODO:FEDERATION include some global DN stats..
            DatanodeCommand[] cmds = sendHeartBeat();
            myMetrics.heartbeats.inc(now() - startTime);
            if (!processCommand(cmds))
              continue;
          }

          reportReceivedBlocks();

          DatanodeCommand cmd = blockReport();
          processCommand(cmd);

          // start block scanner
          if (blockScanner != null) {
            synchronized(blockScanner) { // SHOULD BE MOVED OUT OF THE THREAD.. FEDERATION
              if(blockScannerThread == null && upgradeManager.isUpgradeCompleted()) {
                LOG.info("Starting Periodic block scanner.");
                blockScannerThread = new Daemon(blockScanner);
                blockScannerThread.start();
              }
            }
          }

          //
          // There is no work to do;  sleep until hearbeat timer elapses, 
          // or work arrives, and then iterate again.
          //
          long waitTime = heartBeatInterval - 
          (System.currentTimeMillis() - lastHeartbeat);
          synchronized(receivedBlockList) {
            if (waitTime > 0 && receivedBlockList.size() == 0) {
              try {
                receivedBlockList.wait(waitTime);
              } catch (InterruptedException ie) {
                LOG.warn("BPOfferService for block pool="
                    + this.getBlockPoolId() + " received exception:" + ie);
              }
            }
          } // synchronized
        } catch(RemoteException re) {
          String reClass = re.getClassName();
          if (UnregisteredNodeException.class.getName().equals(reClass) ||
              DisallowedDatanodeException.class.getName().equals(reClass) ||
              IncorrectVersionException.class.getName().equals(reClass)) {
            LOG.warn("blockpool " + blockPoolId + " is shutting down: " + 
                StringUtils.stringifyException(re));
            shouldServiceRun = false;
            return;
          }
          LOG.warn(StringUtils.stringifyException(re));
          try {
            long sleepTime = Math.min(1000, heartBeatInterval);
            Thread.sleep(sleepTime);
          } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
          }
        } catch (IOException e) {
          LOG.warn(StringUtils.stringifyException(e));
        }
      } // while (shouldRun && shouldServiceRun)
    } // offerService

    /**
     * Register one bp with the corresponding NameNode
     * <p>
     * The bpDatanode needs to register with the namenode on startup in order
     * 1) to report which storage it is serving now and 
     * 2) to receive a registrationID
     *  
     * issued by the namenode to recognize registered datanodes.
     * 
     * @see FSNamesystem#registerDatanode(DatanodeRegistration)
     * @throws IOException
     */
    void register() throws IOException {
      LOG.info("in register: sid=" + bpRegistration.getStorageID() + ";SI="
          + bpRegistration.storageInfo); 
                
      while(shouldRun && shouldServiceRun) {
        try {
          // reset name to machineName. Mainly for web interface. Same for all DB
          bpRegistration.name = machineName + ":" + bpRegistration.getPort();
          LOG.info("bpReg before =" + bpRegistration.storageInfo +           
              ";sid=" + bpRegistration.storageID + ";name="+bpRegistration.getName());

          bpRegistration = bpNamenode.registerDatanode(bpRegistration);
          // make sure we got the machine name right (same as NN sees it)
          String [] mNames = bpRegistration.getName().split(":");
          synchronized (dnRegistration) {
            dnRegistration.name = mNames[0] + ":" + dnRegistration.getPort();
          }

          LOG.info("bpReg after =" + bpRegistration.storageInfo + 
              ";sid=" + bpRegistration.storageID + ";name="+bpRegistration.getName());

          break;
        } catch(SocketTimeoutException e) {  // namenode is busy
          LOG.info("Problem connecting to server: " + nnAddr);
          try {
            Thread.sleep(1000);
          } catch (InterruptedException ie) {}
        }
      }

      
      // TODO:FEDERATION - reavaluate the following three checks!!!!!
      assert ("".equals(storage.getStorageID())
          && !"".equals(bpRegistration.getStorageID()))
          || storage.getStorageID().equals(bpRegistration.getStorageID()) :
      "New storageID can be assigned only if data-node is not formatted";

      if (storage.getStorageID().equals("")) {
        storage.setStorageID(bpRegistration.getStorageID());
        storage.writeAll();
        LOG.info("New storage id " + bpRegistration.getStorageID()
            + " is assigned to data-node " + bpRegistration.getName());
      }
      if(! storage.getStorageID().equals(bpRegistration.getStorageID())) {
        throw new IOException("Inconsistent storage IDs. Name-node returned "
            + bpRegistration.getStorageID() 
            + ". Expecting " + storage.getStorageID());
      }

      if (!isBlockTokenInitialized) {
        /* first time registering with NN */
        ExportedBlockKeys keys = bpRegistration.exportedKeys;
        isBlockTokenEnabled = keys.isBlockTokenEnabled();
        if (isBlockTokenEnabled) {
          long blockKeyUpdateInterval = keys.getKeyUpdateInterval();
          long blockTokenLifetime = keys.getTokenLifetime();
          LOG.info("Block token params received from NN: keyUpdateInterval="
              + blockKeyUpdateInterval / (60 * 1000)
              + " min(s), tokenLifetime=" + blockTokenLifetime / (60 * 1000)
              + " min(s)");
          blockTokenSecretManager.setTokenLifetime(blockTokenLifetime);
        }
        isBlockTokenInitialized = true;
      }

      if (isBlockTokenEnabled) {
        blockTokenSecretManager.setKeys(bpRegistration.exportedKeys);
        bpRegistration.exportedKeys = ExportedBlockKeys.DUMMY_KEYS;
      }

      LOG.info("in register:" + ";bpDNR="+bpRegistration.storageInfo);

      // random short delay - helps scatter the BR from all DNs
      scheduleBlockReport(initialBlockReportDelay);
    }


    /**
     * No matter what kind of exception we get, keep retrying to offerService().
     * That's the loop that connects to the NameNode and provides basic DataNode
     * functionality.
     *
     * Only stop when "shouldRun" or "shouldServiceRun" is turned off, which can
     * happen either at shutdown or due to refreshNamenodes.
     */
    @Override
    public void run() {
      LOG.info(bpRegistration + "In BPOfferService.run, data = " + data
          + ";bp=" + blockPoolId);

      try {
        // init stuff
        try {
          // setup storage
          setupBP(conf, dataDirs);
          register();
        } catch (IOException ioe) {
          // Initial handshake, storage recovery or registration failed
          // End BPOfferService thread
          LOG.fatal(bpRegistration + " initialization failed for block pool "
              + blockPoolId, ioe);
          return;
        }

        initialized = true; // bp is initialized;

        while (shouldRun && shouldServiceRun) {
          try {
            // TODO:FEDERATION needs to be moved too
            startDistributedUpgradeIfNeeded();
            offerService();
          } catch (Exception ex) {
            LOG.error("Exception: " + StringUtils.stringifyException(ex));
            if (shouldRun && shouldServiceRun) {
              try {
                Thread.sleep(5000);
              } catch (InterruptedException ie) {
                LOG.warn("Received exception: ", ie);
              }
            }
          }
        }
      } finally {
        LOG.warn(bpRegistration + ":ending block pool service for: " 
            + blockPoolId);
        cleanUp();
      }
    }

    /**
     * Process an array of datanode commands
     * 
     * @param cmds an array of datanode commands
     * @return true if further processing may be required or false otherwise. 
     */
    private boolean processCommand(DatanodeCommand[] cmds) {
      if (cmds != null) {
        for (DatanodeCommand cmd : cmds) {
          try {
            if (processCommand(cmd) == false) {
              return false;
            }
          } catch (IOException ioe) {
            LOG.warn("Error processing datanode Command", ioe);
          }
        }
      }
      return true;
    }

    /**
     * 
     * @param cmd
     * @return true if further processing may be required or false otherwise. 
     * @throws IOException
     */
    private boolean processCommand(DatanodeCommand cmd) throws IOException {
      if (cmd == null)
        return true;
      final BlockCommand bcmd = 
        cmd instanceof BlockCommand? (BlockCommand)cmd: null;

      switch(cmd.getAction()) {
      case DatanodeProtocol.DNA_TRANSFER:
        // Send a copy of a block to another datanode
        transferBlocks(bcmd.getPoolId(), bcmd.getBlocks(), bcmd.getTargets());
        myMetrics.blocksReplicated.inc(bcmd.getBlocks().length);
        break;
      case DatanodeProtocol.DNA_INVALIDATE:
        //
        // Some local block(s) are obsolete and can be 
        // safely garbage-collected.
        //
        Block toDelete[] = bcmd.getBlocks();
        try {
          if (blockScanner != null) {
            blockScanner.deleteBlocks(bcmd.getPoolId(), toDelete);
          }
          // using global fsdataset
          data.invalidate(bcmd.getPoolId(), toDelete);
        } catch(IOException e) {
          checkDiskError();
          throw e;
        }
        myMetrics.blocksRemoved.inc(toDelete.length);
        break;
      case DatanodeProtocol.DNA_SHUTDOWN:
        // shut down the data node
        shouldServiceRun = false;
        return false;
      case DatanodeProtocol.DNA_REGISTER:
        // namenode requested a registration - at start or if NN lost contact
        LOG.info("DatanodeCommand action: DNA_REGISTER");
        if (shouldRun && shouldServiceRun) {
          register();
        }
        break;
      case DatanodeProtocol.DNA_FINALIZE:
        storage.finalizeUpgrade(((DatanodeCommand.Finalize) cmd)
            .getBlockPoolId());
        break;
      case UpgradeCommand.UC_ACTION_START_UPGRADE:
        // start distributed upgrade here
        datanodeObject.namenodeTODO_FED = bpNamenode; //TODO:FEDERATION - this needs to be converted.
        processDistributedUpgradeCommand((UpgradeCommand)cmd);
        break;
      case DatanodeProtocol.DNA_RECOVERBLOCK:
        // TODO:FEDERATION - global storage????? or per BP storage
        recoverBlocks(((BlockRecoveryCommand)cmd).getRecoveringBlocks());
        break;
      case DatanodeProtocol.DNA_ACCESSKEYUPDATE:
        LOG.info("DatanodeCommand action: DNA_ACCESSKEYUPDATE");
        if (isBlockTokenEnabled) {
          blockTokenSecretManager.setKeys(((KeyUpdateCommand) cmd).getExportedKeys());
        }
        break;
      default:
        LOG.warn("Unknown DatanodeCommand action: " + cmd.getAction());
      }
      return true;
    }
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

    storage = new DataStorage();
    
    // global DN settings
    initConfig(conf);
    registerMXBean();
    initDataXceiver(conf);
    startInfoServer(conf);
    initIpcServer(conf); // TODO:FEDERATION redirect the call appropriately 

    myMetrics = new DataNodeMetrics(conf, dnRegistration.getName());

    blockPoolManager = new BlockPoolManager(conf, dnRegistration);
  }
  
  BPOfferService[] getAllBpOs() {
    return blockPoolManager.getAllNamenodeThreads();
  }
  
  /**
   * Initializes the {@link #data}. The initialization is done only once, when
   * handshake with the the first namenode is completed.
   */
  private synchronized void initFsDataSet(Configuration conf,
      AbstractList<File> dataDirs) throws IOException {
    if (data != null) { // Already initialized
      return;
    }

    // get version and id info from the name-node
    boolean simulatedFSDataset = 
      conf.getBoolean("dfs.datanode.simulateddatastorage", false);

    if (simulatedFSDataset) {
      setNewStorageID(dnRegistration);
      conf.set(DFSConfigKeys.DFS_DATANODE_STORAGEID_KEY,
          dnRegistration.getStorageID());

      // it would have been better to pass storage as a parameter to
      // constructor below - need to augment ReflectionUtils used below.

      try {
        // TODO:FEDERATION Equivalent of following (can't do because Simulated
        // is in test dir)
        data = (FSDatasetInterface) ReflectionUtils.newInstance(
            Class.forName(
            "org.apache.hadoop.hdfs.server.datanode.SimulatedFSDataset"),
            conf);
      } catch (ClassNotFoundException e) {
        throw new IOException(StringUtils.stringifyException(e));
      }
      // TODO:FEDERATION do we need set it to the general dnRegistration?????
      // TODO:FEDERATION do we need LV,NSid, cid,bpid for datanode version file?
    } else {
      data = new FSDataset(storage, conf);
    }
  }

  /**
   * Determine the http server's effective addr
   */
  public static InetSocketAddress getInfoAddr(Configuration conf) {
    return NetUtils.createSocketAddr(
        conf.get("dfs.datanode.http.address", "0.0.0.0:50075"));
  }
  
  private void registerMXBean() {
    // register MXBean
    MBeanServer mbs = ManagementFactory.getPlatformMBeanServer(); 
    try {
      ObjectName mxbeanName = new ObjectName("HadoopInfo:type=DataNodeInfo");
      mbs.registerMBean(this, mxbeanName);
    } catch ( javax.management.JMException e ) {
      LOG.warn("Failed to register NameNode MXBean", e);
    }
  }
  
  public DatanodeRegistration getDNRegistrationForBP(String bpid) 
  throws IOException {
    BPOfferService bpos = blockPoolManager.get(bpid);
    if(bpos==null || bpos.bpRegistration==null) {
      throw new IOException("cannot find BPOfferService for bpid="+bpid);
    }
    return bpos.bpRegistration;
  }
  

  /**
   * Creates either NIO or regular depending on socketWriteTimeout.
   */
  protected Socket newSocket() throws IOException {
    return (socketWriteTimeout > 0) ? 
           SocketChannel.open().socket() : new Socket();                                   
  }

  private static void setDataNode(DataNode node) {
    datanodeObject = node;
  }

  /** Return the DataNode object */
  public static DataNode getDataNode() {
    return datanodeObject;
  } 

  public static InterDatanodeProtocol createInterDataNodeProtocolProxy(
      DatanodeID datanodeid, final Configuration conf, final int socketTimeout)
    throws IOException {
    final InetSocketAddress addr = NetUtils.createSocketAddr(
        datanodeid.getHost() + ":" + datanodeid.getIpcPort());
    if (InterDatanodeProtocol.LOG.isDebugEnabled()) {
      InterDatanodeProtocol.LOG.info("InterDatanodeProtocol addr=" + addr);
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

  public InetSocketAddress getNameNodeAddr() {
    return nameNodeAddr;
  }
  
  public InetSocketAddress getNameNodeAddrForClient() {
    return nameNodeAddrForClient;
  }
  
  public InetSocketAddress getSelfAddr() {
    return selfAddr;
  }
    
  DataNodeMetrics getMetrics() {
    return myMetrics;
  }
  
  /** Return DatanodeRegistration */
  public DatanodeRegistration getDatanodeRegistration() {
    return dnRegistration;
  }

  public static void setNewStorageID(DatanodeRegistration dnReg) {
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
    
    int rand = 0;
    try {
      rand = SecureRandom.getInstance("SHA1PRNG").nextInt(Integer.MAX_VALUE);
    } catch (NoSuchAlgorithmException e) {
      LOG.warn("Could not use SecureRandom");
      rand = R.nextInt(Integer.MAX_VALUE);
    }
    dnReg.storageID = "DS-" + rand + "-"+ ip + "-" + dnReg.getPort() + "-" + 
                      System.currentTimeMillis();
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
    
    this.shouldRun = false;
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
    
    try {
      this.blockPoolManager.shutDownAll();
    } catch (InterruptedException ie) {
      LOG.warn("Received exception in BlockPoolManager#shutDownAll: ", ie);
    }

    if(upgradeManager != null)
      upgradeManager.shutdownUpgrade();
    if (blockScannerThread != null) { 
      blockScannerThread.interrupt();
      try {
        blockScannerThread.join(3600000L); // wait for at most 1 hour
      } catch (InterruptedException ie) {
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
    if (myMetrics != null) {
      myMetrics.shutdown();
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
    } catch(DiskErrorException de) {
      handleDiskError(de.getMessage());
    }
  }
  
  private void handleDiskError(String errMsgr) {
    boolean hasEnoughResources = data.hasEnoughResource();
    LOG.warn("DataNode.handleDiskError: Keep Running: " + hasEnoughResources);
    
    int dpError = hasEnoughResources ? DatanodeProtocol.DISK_ERROR  
                                     : DatanodeProtocol.FATAL_DISK_ERROR;  
    //inform NameNodes
    for(BPOfferService bpos: blockPoolManager.getAllNamenodeThreads()) {
      DatanodeProtocol nn = bpos.bpNamenode;
      try {
        nn.errorReport(bpos.bpRegistration, dpError, errMsgr);
      } catch(IOException ignored) { }
    }
    
    if(hasEnoughResources) {
      scheduleAllBlockReport(0);
      return; // do not shutdown
    }
    
    LOG.warn("DataNode is shutting down.\n" + errMsgr);
    shutdown();
  }
    
  /** Number of concurrent xceivers per node. */
  int getXceiverCount() {
    return threadGroup == null ? 0 : threadGroup.activeCount();
  }
    
  // Distributed upgrade manager
  UpgradeManagerDatanode upgradeManager = new UpgradeManagerDatanode(this);

  private void processDistributedUpgradeCommand(UpgradeCommand comm
                                               ) throws IOException {
    assert upgradeManager != null : "DataNode.upgradeManager is null.";
    upgradeManager.processUpgradeCommand(comm);
  }
  /**
   * Start distributed upgrade if it should be initiated by the data-node.
   */
  private void startDistributedUpgradeIfNeeded() throws IOException {
    UpgradeManagerDatanode um = DataNode.getDataNode().upgradeManager;
    assert um != null : "DataNode.upgradeManager is null.";
    if(!um.getUpgradeState())
      return;
    um.setUpgradeState(false, um.getUpgradeVersion());
    um.startUpgrade();
    return;
  }

  private void transferBlock( ExtendedBlock block, 
                              DatanodeInfo xferTargets[] 
                              ) throws IOException {
    DatanodeProtocol nn = getBPNamenode(block.getPoolId());
    
    if (!data.isValidBlock(block)) {
      // block does not exist or is under-construction
      String errStr = "Can't send invalid block " + block;
      LOG.info(errStr);
      nn.errorReport(dnRegistration, 
                           DatanodeProtocol.INVALID_BLOCK, 
                           errStr);
      return;
    }

    // Check if NN recorded length matches on-disk length 
    long onDiskLength = data.getLength(block);
    if (block.getNumBytes() > onDiskLength) {
      // Shorter on-disk len indicates corruption so report NN the corrupt block
      nn.reportBadBlocks(new LocatedBlock[]{
          new LocatedBlock(block, new DatanodeInfo[] {
              new DatanodeInfo(dnRegistration)})});
      LOG.info("Can't replicate block " + block
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
        LOG.info(dnRegistration + " Starting thread to transfer block " + 
                 block + " to " + xfersBuilder);                       
      }

      new Daemon(new DataTransfer(xferTargets, block, this)).start();
    }
  }

  private void transferBlocks(String poolId, Block blocks[],
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
    "LastPacketInBlock" set to true or with a zero length. If there is 
    no checksum error, it replies to DataNode with OP_STATUS_CHECKSUM_OK:
    
    Client optional response at the end of data transmission :
      +------------------------------+
      | 2 byte OP_STATUS_CHECKSUM_OK |
      +------------------------------+
    
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
  class DataTransfer implements Runnable {
    DatanodeInfo targets[];
    ExtendedBlock b;
    DataNode datanode;

    /**
     * Connect to the first item in the target list.  Pass along the 
     * entire target list, the block, and the data.
     */
    public DataTransfer(DatanodeInfo targets[], ExtendedBlock b,
        DataNode datanode) throws IOException {
      this.targets = targets;
      this.b = b;
      this.datanode = datanode;
    }

    /**
     * Do the deed, write the bytes
     */
    public void run() {
      xmitsInProgress.getAndIncrement();
      Socket sock = null;
      DataOutputStream out = null;
      BlockSender blockSender = null;
      
      try {
        InetSocketAddress curTarget = 
          NetUtils.createSocketAddr(targets[0].getName());
        sock = newSocket();
        NetUtils.connect(sock, curTarget, socketTimeout);
        sock.setSoTimeout(targets.length * socketTimeout);

        long writeTimeout = socketWriteTimeout + 
                            HdfsConstants.WRITE_TIMEOUT_EXTENSION * (targets.length-1);
        OutputStream baseStream = NetUtils.getOutputStream(sock, writeTimeout);
        out = new DataOutputStream(new BufferedOutputStream(baseStream, 
                                                            SMALL_BUFFER_SIZE));

        blockSender = new BlockSender(b, 0, b.getNumBytes(), 
            false, false, false, datanode);
        DatanodeInfo srcNode = new DatanodeInfo(dnRegistration);

        //
        // Header info
        //
        Token<BlockTokenIdentifier> accessToken = BlockTokenSecretManager.DUMMY_TOKEN;
        if (isBlockTokenEnabled) {
          accessToken = blockTokenSecretManager.generateToken(b, 
              EnumSet.of(BlockTokenSecretManager.AccessMode.WRITE));
        }
        DataTransferProtocol.Sender.opWriteBlock(out,
            b, 0, BlockConstructionStage.PIPELINE_SETUP_CREATE, 0, 0, 0, "",
            srcNode, targets, accessToken);

        // send data & checksum
        blockSender.sendBlock(out, baseStream, null);

        // no response necessary
        LOG.info(dnRegistration + ":Transmitted block " + b + " to " + curTarget);

      } catch (IOException ie) {
        LOG.warn(dnRegistration + ":Failed to transfer " + b + " to " + targets[0].getName()
            + " got " + StringUtils.stringifyException(ie));
        // check if there are any disk problem
        datanode.checkDiskError();
        
      } finally {
        xmitsInProgress.getAndDecrement();
        IOUtils.closeStream(blockSender);
        IOUtils.closeStream(out);
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
    myMetrics.blocksWritten.inc();
    BPOfferService bpos = blockPoolManager.get(block.getPoolId());
    if(bpos != null) {
      bpos.notifyNamenodeReceivedBlock(block, delHint);
    } else {
      LOG.warn("Cannot find BPOfferService for reporting block received for bpid="
          + block.getPoolId());
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
    initBlockScanner(conf);
    startPlugins(conf);
    
    // BlockTokenSecretManager is created here, but it shouldn't be
    // used until it is initialized in register().
    this.blockTokenSecretManager = new BlockTokenSecretManager(false, 0, 0);    
  }

  public boolean isDatanodeUp() {
    for (BPOfferService bp : blockPoolManager.getAllNamenodeThreads()) {
      // Datanode is up, if one of the bp service is up
      if (bp.bpThread.isAlive()) {
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
    dnThreadName = "DataNode: [" +
                    StringUtils.uriToString(dataDirs.toArray(new URI[0])) + "]";
   UserGroupInformation.setConfiguration(conf);
    SecurityUtil.login(conf, DFSConfigKeys.DFS_DATANODE_KEYTAB_FILE_KEY,
        DFSConfigKeys.DFS_DATANODE_USER_NAME_KEY);
    return makeInstance(dataDirs, conf, resources);
  }

  static Collection<URI> getStorageDirs(Configuration conf) {
    Collection<String> dirNames =
      conf.getStringCollection(DFSConfigKeys.DFS_DATANODE_DATA_DIR_KEY);
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
        conf.get(DFSConfigKeys.DFS_DATANODE_DATA_DIR_PERMISSION_KEY,
                 DFSConfigKeys.DFS_DATANODE_DATA_DIR_PERMISSION_DEFAULT));
    ArrayList<File> dirs = getDataDirsFromURIs(dataDirs, localFS, permission);

    if (dirs.size() > 0) {
      return new DataNode(conf, dirs, resources);
    }
    LOG.error("All directories in "
        + DFSConfigKeys.DFS_DATANODE_DATA_DIR_KEY + " are invalid.");
    return null;
  }

  // DataNode ctor expects AbstractList instead of List or Collection...
  static ArrayList<File> getDataDirsFromURIs(Collection<URI> dataDirs,
      LocalFileSystem localFS, FsPermission permission) {
    ArrayList<File> dirs = new ArrayList<File>();
    for (URI dirURI : dataDirs) {
      if (!"file".equalsIgnoreCase(dirURI.getScheme())) {
        LOG.warn("Unsupported URI schema in " + dirURI + ". Ignoring ...");
        continue;
      }
      // drop any (illegal) authority in the URI for backwards compatibility
      File data = new File(dirURI.getPath());
      try {
        DiskChecker.checkDir(localFS, new Path(data.toURI()), permission);
        dirs.add(data);
      } catch (IOException e) {
        LOG.warn("Invalid directory in: "
                 + DFSConfigKeys.DFS_DATANODE_DATA_DIR_KEY + ": "
                 + e.getMessage());
      }
    }
    return dirs;
  }

  @Override
  public String toString() {
    return "DataNode{" +
      "data=" + data +
      (dnRegistration != null ?
          (", localName='" + dnRegistration.getName() + "'" +
              ", storageID='" + dnRegistration.getStorageID() + "'")
          : "") +
      ", xmitsInProgress=" + xmitsInProgress.get() +
      "}";
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
        System.exit(-1);
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
    conf.set("dfs.datanode.startup", opt.toString());
  }

  static StartupOption getStartupOption(Configuration conf) {
    return StartupOption.valueOf(conf.get("dfs.datanode.startup",
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
      LOG.error(StringUtils.stringifyException(e));
      System.exit(-1);
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
            logRecoverBlock("NameNode", b.getBlock().getLocalBlock(), b.getLocations());
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
    return new ExtendedBlock(oldBlock.getPoolId(), r);
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
    DatanodeInfo[] targets = rBlock.getLocations();
    DatanodeID[] datanodeids = (DatanodeID[])targets;
    List<BlockRecord> syncList = new ArrayList<BlockRecord>(datanodeids.length);
    int errorCount = 0;

    //check generation stamps
    for(DatanodeID id : datanodeids) {
      try {
        InterDatanodeProtocol datanode = dnRegistration.equals(id)?
            this: DataNode.createInterDataNodeProtocolProxy(id, getConf(),
                socketTimeout);
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
   * 
   * @param bpid
   * @return Namenode corresponding to the bpid
   * @throws IOException
   */
  public DatanodeProtocol getBPNamenode(String bpid) throws IOException {
    BPOfferService bpos = blockPoolManager.get(bpid);
    if(bpos == null || bpos.bpNamenode == null) {
      throw new IOException("cannot find a namnode proxy for bpid=" + bpid);
    }
    return bpos.bpNamenode;
  }

  /** Block synchronization */
  void syncBlock(RecoveringBlock rBlock,
                         List<BlockRecord> syncList) throws IOException {
    ExtendedBlock block = rBlock.getBlock();
    DatanodeProtocol nn = getBPNamenode(block.getPoolId());
    
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
    final ExtendedBlock newBlock = new ExtendedBlock(block.getPoolId(), block
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
            new ExtendedBlock(newBlock.getPoolId(), r.rInfo), recoveryId,
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
      Block block, DatanodeID[] targets) {
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
    if (isBlockTokenEnabled) {
      Set<TokenIdentifier> tokenIds = UserGroupInformation.getCurrentUser()
          .getTokenIdentifiers();
      if (tokenIds.size() != 1) {
        throw new IOException("Can't continue with getReplicaVisibleLength() "
            + "authorization since none or more than one BlockTokenIdentifier "
            + "is found.");
      }
      for (TokenIdentifier tokenId : tokenIds) {
        BlockTokenIdentifier id = (BlockTokenIdentifier) tokenId;
        if (LOG.isDebugEnabled()) {
          LOG.debug("Got: " + id.toString());
        }
        blockTokenSecretManager.checkAccess(id, null, block,
            BlockTokenSecretManager.AccessMode.WRITE);
      }
    }

    return data.getReplicaVisibleLength(block);
  }
  
  // Determine a Datanode's streaming address
  public static InetSocketAddress getStreamingAddr(Configuration conf) {
    return NetUtils.createSocketAddr(
        conf.get("dfs.datanode.address", "0.0.0.0:50010"));
  }
  @Override // DataNodeMXBean
  public String getVersion() {
    return VersionInfo.getVersion();
  }
  
  @Override // DataNodeMXBean
  public String getRpcPort(){
    InetSocketAddress ipcAddr = NetUtils.createSocketAddr(
        this.getConf().get("dfs.datanode.ipc.address"));
    return Integer.toString(ipcAddr.getPort());
  }

  @Override // DataNodeMXBean
  public String getHttpPort(){
    return this.getConf().get("dfs.datanode.info.port");
  }

  @Override // DataNodeMXBean
  public String getNamenodeAddress(){
    return nameNodeAddr.getHostName();
  }

  /**
   * Returned information is a JSON representation of a map with 
   * volume name as the key and value is a map of volume attribute 
   * keys to its values
   */
  @Override // DataNodeMXBean
  public String getVolumeInfo() {
    final Map<String, Object> info = new HashMap<String, Object>();
    Collection<VolumeInfo> volumes = ((FSDataset)this.data).getVolumeInfo();
    for (VolumeInfo v : volumes) {
      final Map<String, Object> innerInfo = new HashMap<String, Object>();
      innerInfo.put("usedSpace", v.usedSpace);
      innerInfo.put("freeSpace", v.freeSpace);
      innerInfo.put("reservedSpace", v.reservedSpace);
      info.put(v.directory, innerInfo);
    }
    return JSON.toString(info);
  }
  
  @Override // DataNodeMXBean
  public String getClusterId() {
    return clusterId;
  }
  
  void refreshNamenodes(Configuration conf) throws IOException {
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

  /**
   * @param bpid block pool Id
   * @return true - if BPOfferService thread is alive
   */
  public boolean isBPServiceAlive(String bpid) {
    BPOfferService bp = blockPoolManager.get(bpid);
    if (bp != null) {
      return bp.shouldServiceRun && bp.bpThread.isAlive();
    }
    return false;
  }
}
