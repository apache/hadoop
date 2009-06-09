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

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.AbstractList;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.HDFSPolicyProvider;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.BlockListAsLongs;
import org.apache.hadoop.hdfs.protocol.ClientDatanodeProtocol;
import org.apache.hadoop.hdfs.protocol.DataTransferProtocol;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.FSConstants;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.UnregisteredNodeException;
import org.apache.hadoop.hdfs.server.common.HdfsConstants.StartupOption;
import org.apache.hadoop.hdfs.server.common.HdfsConstants;
import org.apache.hadoop.hdfs.server.common.GenerationStamp;
import org.apache.hadoop.hdfs.server.common.IncorrectVersionException;
import org.apache.hadoop.hdfs.server.common.Storage;
import org.apache.hadoop.hdfs.server.datanode.metrics.DataNodeMetrics;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.FileChecksumServlets;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.namenode.StreamFile;
import org.apache.hadoop.hdfs.server.protocol.BlockCommand;
import org.apache.hadoop.hdfs.server.protocol.BlockMetaDataInfo;
import org.apache.hadoop.hdfs.server.protocol.DatanodeCommand;
import org.apache.hadoop.hdfs.server.protocol.DatanodeProtocol;
import org.apache.hadoop.hdfs.server.protocol.DatanodeRegistration;
import org.apache.hadoop.hdfs.server.protocol.DisallowedDatanodeException;
import org.apache.hadoop.hdfs.server.protocol.KeyUpdateCommand;
import org.apache.hadoop.hdfs.server.protocol.InterDatanodeProtocol;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo;
import org.apache.hadoop.hdfs.server.protocol.UpgradeCommand;
import org.apache.hadoop.http.HttpServer;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.net.DNS;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.AccessToken;
import org.apache.hadoop.security.AccessTokenHandler;
import org.apache.hadoop.security.ExportedAccessKeys;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.authorize.ConfiguredPolicy;
import org.apache.hadoop.security.authorize.PolicyProvider;
import org.apache.hadoop.security.authorize.ServiceAuthorizationManager;
import org.apache.hadoop.util.Daemon;
import org.apache.hadoop.util.DiskChecker;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.ServicePlugin;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.DiskChecker.DiskErrorException;
import org.apache.hadoop.util.DiskChecker.DiskOutOfSpaceException;
import org.apache.hadoop.util.Service;

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
public class DataNode extends Service
    implements InterDatanodeProtocol, ClientDatanodeProtocol, FSConstants, Runnable {
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
  
  public DatanodeProtocol namenode = null;
  public FSDatasetInterface data = null;
  public DatanodeRegistration dnRegistration = null;

  volatile boolean shouldRun = true;
  private LinkedList<Block> receivedBlockList = new LinkedList<Block>();
  /** list of blocks being recovered */
  private final Map<Block, Block> ongoingRecovery = new HashMap<Block, Block>();
  private LinkedList<String> delHints = new LinkedList<String>();
  public final static String EMPTY_DEL_HINT = "";
  AtomicInteger xmitsInProgress = new AtomicInteger();
  Daemon dataXceiverServer = null;
  ThreadGroup threadGroup = null;
  long blockReportInterval;
  //disallow the sending of BR before instructed to do so
  long lastBlockReport = 0;
  boolean resetBlockReportTime = true;
  long initialBlockReportDelay = BLOCKREPORT_INITIAL_DELAY * 1000L;
  long lastHeartbeat = 0;
  long heartBeatInterval;
  private DataStorage storage = null;
  private HttpServer infoServer = null;
  DataNodeMetrics myMetrics;
  private InetSocketAddress nameNodeAddr;
  private InetSocketAddress selfAddr;
  private static DataNode datanodeObject = null;
  private Thread dataNodeThread = null;
  String machineName;
  private static String dnThreadName;
  int socketTimeout;
  int socketWriteTimeout = 0;  
  boolean transferToAllowed = true;
  int writePacketSize = 0;
  boolean isAccessTokenEnabled;
  AccessTokenHandler accessTokenHandler;
  boolean isAccessTokenInitialized = false;
  
  public DataBlockScanner blockScanner = null;
  public Daemon blockScannerThread = null;
  
  /** Activated plug-ins. */
  private List<ServicePlugin> plugins;
  /** data directories */
  private AbstractList<File> dataDirs;
  private static final Random R = new Random();
  
  // For InterDataNodeProtocol
  public Server ipcServer;

  /**
   * Current system time.
   * @return current time in msec.
   */
  static long now() {
    return System.currentTimeMillis();
  }

  /**
   * Create the DataNode given a configuration and an array of dataDirs.
   * 'dataDirs' is where the blocks are stored. This constructor does not start
   * the node, merely initializes it
   *
   * @param conf     configuration to use
   * @param dataDirs list of directories that may be used for data
   * @throws IOException for historical reasons
   */
  DataNode(Configuration conf,
           AbstractList<File> dataDirs) throws IOException {
    super(conf);
    DataNode.setDataNode(this);
    this.dataDirs = dataDirs;
  }

  /////////////////////////////////////////////////////                     
/////////////////////////////////////////////////////                                           
// Lifecycle                                                                                    
/////////////////////////////////////////////////////                                           

  /**
   * Start any work (in separate threads)
   *
   * @throws IOException for any startup failure
   */
  @Override
  public void innerStart() throws IOException {
    startDataNode(getConf(), dataDirs);
  }
    
  /**
   * {@inheritDoc}.
   *
   * This implementation checks for the IPC server running and the
   * DataNode being registered to a namenode.
   *
   * @param status the initial status
   * @throws IOException       for any ping failure
   * @throws LivenessException if the IPC server is not defined 
   */
  @Override
  public void innerPing(ServiceStatus status) throws IOException {
    if (ipcServer == null) {
      status.addThrowable(new LivenessException("No IPC Server running"));
    }
    if (dnRegistration == null) {
      status.addThrowable(
              new LivenessException("Not registered to a namenode"));
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
                     AbstractList<File> dataDirs
                     ) throws IOException {
    // use configured nameserver & interface to get local hostname
    if (conf.get("slave.host.name") != null) {
      machineName = conf.get("slave.host.name");   
    }
    if (machineName == null) {
      machineName = DNS.getDefaultHost(
                                     conf.get("dfs.datanode.dns.interface","default"),
                                     conf.get("dfs.datanode.dns.nameserver","default"));
    }
    this.nameNodeAddr = NameNode.getAddress(conf);
    
    this.socketTimeout =  conf.getInt("dfs.socket.timeout",
                                      HdfsConstants.READ_TIMEOUT);
    this.socketWriteTimeout = conf.getInt("dfs.datanode.socket.write.timeout",
                                          HdfsConstants.WRITE_TIMEOUT);
    /* Based on results on different platforms, we might need set the default 
     * to false on some of them. */
    this.transferToAllowed = conf.getBoolean("dfs.datanode.transferTo.allowed", 
                                             true);
    this.writePacketSize = conf.getInt("dfs.write.packet.size", 64*1024);
    InetSocketAddress socAddr = NetUtils.createSocketAddr(
        conf.get("dfs.datanode.address", "0.0.0.0:50010"));
    int tmpPort = socAddr.getPort();
    storage = new DataStorage();
    // construct registration
    this.dnRegistration = new DatanodeRegistration(machineName + ":" + tmpPort);

    // connect to name node
    this.namenode = (DatanodeProtocol) 
      RPC.waitForProxy(DatanodeProtocol.class,
                       DatanodeProtocol.versionID,
                       nameNodeAddr, 
                       conf);
    // get version and id info from the name-node
    NamespaceInfo nsInfo = handshake();
    StartupOption startOpt = getStartupOption(conf);
    assert startOpt != null : "Startup option must be set.";
    
    boolean simulatedFSDataset = 
        conf.getBoolean("dfs.datanode.simulateddatastorage", false);
    if (simulatedFSDataset) {
        setNewStorageID(dnRegistration);
        dnRegistration.storageInfo.layoutVersion = FSConstants.LAYOUT_VERSION;
        dnRegistration.storageInfo.namespaceID = nsInfo.namespaceID;
        // it would have been better to pass storage as a parameter to
        // constructor below - need to augment ReflectionUtils used below.
        conf.set("StorageId", dnRegistration.getStorageID());
        try {
          //Equivalent of following (can't do because Simulated is in test dir)
          //  this.data = new SimulatedFSDataset(conf);
          this.data = (FSDatasetInterface) ReflectionUtils.newInstance(
              Class.forName("org.apache.hadoop.hdfs.server.datanode.SimulatedFSDataset"), conf);
        } catch (ClassNotFoundException e) {
          throw new IOException(StringUtils.stringifyException(e));
        }
    } else { // real storage
      // read storage info, lock data dirs and transition fs state if necessary
      storage.recoverTransitionRead(nsInfo, dataDirs, startOpt);
      // adjust
      this.dnRegistration.setStorageInfo(storage);
      // initialize data node internal structure
      this.data = new FSDataset(storage, conf);
    }

      
    // find free port
    ServerSocket ss = (socketWriteTimeout > 0) ? 
          ServerSocketChannel.open().socket() : new ServerSocket();
    Server.bind(ss, socAddr, 0);
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

    //initialize periodic block scanner
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

    //create a servlet to serve full-file content
    InetSocketAddress infoSocAddr = NetUtils.createSocketAddr(
        conf.get("dfs.datanode.http.address", "0.0.0.0:50075"));
    String infoHost = infoSocAddr.getHostName();
    int tmpInfoPort = infoSocAddr.getPort();
    this.infoServer = new HttpServer("datanode", infoHost, tmpInfoPort,
        tmpInfoPort == 0, conf);
    if(LOG.isDebugEnabled()) {
      LOG.debug("Datanode listening on " + infoHost + ":" + tmpInfoPort);
    }
    if (conf.getBoolean("dfs.https.enable", false)) {
      boolean needClientAuth = conf.getBoolean("dfs.https.need.client.auth", false);
      InetSocketAddress secInfoSocAddr = NetUtils.createSocketAddr(conf.get(
          "dfs.datanode.https.address", infoHost + ":" + 0));
      Configuration sslConf = new Configuration(false);
      sslConf.addResource(conf.get("dfs.https.server.keystore.resource",
          "ssl-server.xml"));
      if (LOG.isDebugEnabled()) {
        LOG.debug("Datanode listening for SSL on " + secInfoSocAddr);
      }
      this.infoServer.addSslListener(secInfoSocAddr, sslConf, needClientAuth);
    }
    this.infoServer.addInternalServlet(null, "/streamFile/*", StreamFile.class);
    this.infoServer.addInternalServlet(null, "/getFileChecksum/*",
        FileChecksumServlets.GetServlet.class);
    this.infoServer.setAttribute("datanode.blockScanner", blockScanner);
    this.infoServer.addServlet(null, "/blockScannerReport", 
                               DataBlockScanner.Servlet.class);
    this.infoServer.start();
    // adjust info port
    this.dnRegistration.setInfoPort(this.infoServer.getPort());
    myMetrics = new DataNodeMetrics(conf, dnRegistration.getStorageID());
    
    // set service-level authorization security policy
    if (conf.getBoolean(
          ServiceAuthorizationManager.SERVICE_AUTHORIZATION_CONFIG, false)) {
      PolicyProvider policyProvider = 
        (PolicyProvider)(ReflectionUtils.newInstance(
            conf.getClass(PolicyProvider.POLICY_PROVIDER_CONFIG, 
                HDFSPolicyProvider.class, PolicyProvider.class), 
            conf));
      SecurityUtil.setPolicy(new ConfiguredPolicy(conf, policyProvider));
    }

    //init ipc server
    InetSocketAddress ipcAddr = NetUtils.createSocketAddr(
        conf.get("dfs.datanode.ipc.address"));
    ipcServer = RPC.getServer(this, ipcAddr.getHostName(), ipcAddr.getPort(), 
        conf.getInt("dfs.datanode.handler.count", 3), false, conf);
    ipcServer.start();
    dnRegistration.setIpcPort(ipcServer.getListenerAddress().getPort());

    LOG.info("dnRegistration = " + dnRegistration);
    
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

  /**
   * Creates either NIO or regular depending on socketWriteTimeout.
   */
  protected Socket newSocket() throws IOException {
    return (socketWriteTimeout > 0) ? 
           SocketChannel.open().socket() : new Socket();                                   
  }
  
  private NamespaceInfo handshake() throws IOException {
    NamespaceInfo nsInfo = new NamespaceInfo();
    while (shouldRun) {
      try {
        nsInfo = namenode.versionRequest();
        break;
      } catch(SocketTimeoutException e) {  // namenode is busy
        LOG.info("Problem connecting to server: " + getNameNodeAddr());
        try {
          Thread.sleep(1000);
        } catch (InterruptedException ie) {}
      }
    }
    if(!shouldRun) {
      throw new IOException("Datanode shut down during handshake with NameNode "
               + getNameNodeAddr());
    }
    String errorMsg = null;
    // verify build version
    if( ! nsInfo.getBuildVersion().equals( Storage.getBuildVersion() )) {
      errorMsg = "Incompatible build versions: namenode BV = " 
        + nsInfo.getBuildVersion() + "; datanode BV = "
        + Storage.getBuildVersion();
      LOG.fatal( errorMsg );
      try {
        namenode.errorReport( dnRegistration,
                              DatanodeProtocol.NOTIFY, errorMsg );
      } catch( SocketTimeoutException e ) {  // namenode is busy
        LOG.info("Problem connecting to server: " + getNameNodeAddr());
      }
      throw new IOException( errorMsg );
    }
    assert FSConstants.LAYOUT_VERSION == nsInfo.getLayoutVersion() :
      "Data-node and name-node layout versions must be the same."
      + "Expected: "+ FSConstants.LAYOUT_VERSION + " actual "+ nsInfo.getLayoutVersion();
    return nsInfo;
  }

  private static void setDataNode(DataNode node) {
    datanodeObject = node;
  }

  /** Return the DataNode object
   * 
   */
  public static DataNode getDataNode() {
    return datanodeObject;
  } 

  public static InterDatanodeProtocol createInterDataNodeProtocolProxy(
      DatanodeID datanodeid, Configuration conf) throws IOException {
    InetSocketAddress addr = NetUtils.createSocketAddr(
        datanodeid.getHost() + ":" + datanodeid.getIpcPort());
    if (InterDatanodeProtocol.LOG.isDebugEnabled()) {
      InterDatanodeProtocol.LOG.info("InterDatanodeProtocol addr=" + addr);
    }
    return (InterDatanodeProtocol)RPC.getProxy(InterDatanodeProtocol.class,
        InterDatanodeProtocol.versionID, addr, conf);
  }

  public InetSocketAddress getNameNodeAddr() {
    return nameNodeAddr;
  }
  
  public InetSocketAddress getSelfAddr() {
    return selfAddr;
  }
    
  DataNodeMetrics getMetrics() {
    return myMetrics;
  }
  
  /**
   * Return the namenode's identifier
   */
  public String getNamenode() {
    //return namenode.toString();
    return "<namenode>";
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
   * Register datanode
   * <p>
   * The datanode needs to register with the namenode on startup in order
   * 1) to report which storage it is serving now and 
   * 2) to receive a registrationID 
   * issued by the namenode to recognize registered datanodes.
   * 
   * @see FSNamesystem#registerDatanode(DatanodeRegistration)
   * @throws IOException
   */
  protected void register() throws IOException {
    if (dnRegistration.getStorageID().equals("")) {
      setNewStorageID(dnRegistration);
    }
    //if we are LIVE, move into the STARTED state, as registration implies that
    //the node is no longer LIVE
    enterState(ServiceState.LIVE, ServiceState.STARTED);
    //spin until the server is up.
    while(shouldRun) {
      try {
        // reset name to machineName. Mainly for web interface.
        dnRegistration.name = machineName + ":" + dnRegistration.getPort();
        dnRegistration = namenode.register(dnRegistration);
        break;
      } catch(SocketTimeoutException e) {  // namenode is busy
        LOG.info("Problem connecting to server: " + getNameNodeAddr());
        try {
          Thread.sleep(1000);
        } catch (InterruptedException ie) {}
      }
    }
    assert ("".equals(storage.getStorageID()) 
            && !"".equals(dnRegistration.getStorageID()))
            || storage.getStorageID().equals(dnRegistration.getStorageID()) :
            "New storageID can be assigned only if data-node is not formatted";
    if (storage.getStorageID().equals("")) {
      storage.setStorageID(dnRegistration.getStorageID());
      storage.writeAll();
      LOG.info("New storage id " + dnRegistration.getStorageID()
          + " is assigned to data-node " + dnRegistration.getName());
    }
    if(! storage.getStorageID().equals(dnRegistration.getStorageID())) {
      throw new IOException("Inconsistent storage IDs. Name-node returned "
          + dnRegistration.getStorageID() 
          + ". Expecting " + storage.getStorageID());
    }
    
    if (!isAccessTokenInitialized) {
      /* first time registering with NN */
      ExportedAccessKeys keys = dnRegistration.exportedKeys;
      this.isAccessTokenEnabled = keys.isAccessTokenEnabled();
      if (isAccessTokenEnabled) {
        long accessKeyUpdateInterval = keys.getKeyUpdateInterval();
        long accessTokenLifetime = keys.getTokenLifetime();
        LOG.info("Access token params received from NN: keyUpdateInterval="
            + accessKeyUpdateInterval / (60 * 1000) + " min(s), tokenLifetime="
            + accessTokenLifetime / (60 * 1000) + " min(s)");
        this.accessTokenHandler = new AccessTokenHandler(false,
            accessKeyUpdateInterval, accessTokenLifetime);
      }
      isAccessTokenInitialized = true;
    }

    if (isAccessTokenEnabled) {
      accessTokenHandler.setKeys(dnRegistration.exportedKeys);
      dnRegistration.exportedKeys = ExportedAccessKeys.DUMMY_KEYS;
    }

    //at this point the DataNode now considers itself live.  
    enterLiveState();
    // random short delay - helps scatter the BR from all DNs
    scheduleBlockReport(initialBlockReportDelay);
  }


  /**
   * Shut down this instance of the datanode. Returns only after shutdown is
   * complete.
   */
  public void shutdown() {
    closeQuietly();
  }

  /**
   * Shut down this instance of the datanode.
   * Returns only after shutdown is complete.
   * This method can only be called by the offerService thread.
   * Otherwise, deadlock might occur.
   */
  @Override
  protected void innerClose() throws IOException {
    synchronized (this) {
      //disable the should run flag first, so that everything out there starts
      //to shut down
      shouldRun = false;
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
    
    RPC.stopProxy(namenode); // stop the RPC threads
    
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
        LOG.warn("Ignoring exception when unlocking storage: "+ie,
                ie);
      }
    }
    if (dataNodeThread != null) {
      dataNodeThread.interrupt();
      try {
        dataNodeThread.join();
      } catch (InterruptedException ie) {
      }
    }
    if (data != null) {
      data.shutdown();
    }
    if (myMetrics != null) {
      myMetrics.shutdown();
    }
  }
  
  
  /* Check if there is no space in disk or the disk is read-only
   *  when IOException occurs. 
   * If so, handle the error */
  protected void checkDiskError( IOException e ) throws IOException {
    if (e.getMessage() != null && 
        e.getMessage().startsWith("No space left on device")) {
      throw new DiskOutOfSpaceException("No space left on device");
    } else {
      checkDiskError();
    }
  }
  
  /* Check if there is no disk space and if so, handle the error*/
  protected void checkDiskError( ) throws IOException {
    try {
      data.checkDataDir();
    } catch(DiskErrorException de) {
      handleDiskError(de.getMessage());
    }
  }
  
  private void handleDiskError(String errMsgr) {
    LOG.warn("DataNode is shutting down.\n" + errMsgr);
    shouldRun = false;
    try {
      namenode.errorReport(
                           dnRegistration, DatanodeProtocol.DISK_ERROR, errMsgr);
    } catch(IOException ignored) {              
    }
  }
    
  /** Number of concurrent xceivers per node. */
  int getXceiverCount() {
    return threadGroup == null ? 0 : threadGroup.activeCount();
  }
    
  /**
   * Main loop for the DataNode.  Runs until shutdown,
   * forever calling remote NameNode functions.
   */
  public void offerService() throws Exception {
     
    LOG.info("using BLOCKREPORT_INTERVAL of " + blockReportInterval + "msec" + 
       " Initial delay: " + initialBlockReportDelay + "msec");

    //
    // Now loop for a long time....
    //
    while (shouldRun) {
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
          DatanodeCommand[] cmds = namenode.sendHeartbeat(dnRegistration,
                                                       data.getCapacity(),
                                                       data.getDfsUsed(),
                                                       data.getRemaining(),
                                                       xmitsInProgress.get(),
                                                       getXceiverCount());
          myMetrics.heartbeats.inc(now() - startTime);
          //LOG.info("Just sent heartbeat, with name " + localName);
          if (!processCommand(cmds))
            continue;
        }
            
        reportReceivedBlocks();

        DatanodeCommand cmd = blockReport();
        processCommand(cmd);

        // start block scanner
        if (blockScanner != null && blockScannerThread == null &&
            upgradeManager.isUpgradeCompleted()) {
          LOG.info("Starting Periodic block scanner.");
          blockScannerThread = new Daemon(blockScanner);
          blockScannerThread.start();
        }
            
        //
        // There is no work to do;  sleep until hearbeat timer elapses, 
        // or work arrives, and then iterate again.
        //
        long waitTime = heartBeatInterval - (System.currentTimeMillis() - lastHeartbeat);
        synchronized(receivedBlockList) {
          if (waitTime > 0 && receivedBlockList.size() == 0) {
            try {
              receivedBlockList.wait(waitTime);
            } catch (InterruptedException ie) {
            }
          }
        } // synchronized
      } catch(RemoteException re) {
        String reClass = re.getClassName();
        if (UnregisteredNodeException.class.getName().equals(reClass) ||
            DisallowedDatanodeException.class.getName().equals(reClass) ||
            IncorrectVersionException.class.getName().equals(reClass)) {
          LOG.warn("DataNode is shutting down: " + 
                   StringUtils.stringifyException(re));
          shutdown();
          return;
        }
        LOG.warn(StringUtils.stringifyException(re));
      } catch (IOException e) {
        LOG.warn(StringUtils.stringifyException(e));
      }
    } // while (shouldRun)
  } // offerService

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
    final BlockCommand bcmd = cmd instanceof BlockCommand? (BlockCommand)cmd: null;

    switch(cmd.getAction()) {
    case DatanodeProtocol.DNA_TRANSFER:
      // Send a copy of a block to another datanode
      transferBlocks(bcmd.getBlocks(), bcmd.getTargets());
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
          blockScanner.deleteBlocks(toDelete);
        }
        data.invalidate(toDelete);
      } catch(IOException e) {
        checkDiskError();
        throw e;
      }
      myMetrics.blocksRemoved.inc(toDelete.length);
      break;
    case DatanodeProtocol.DNA_SHUTDOWN:
      // shut down the data node
      this.shutdown();
      return false;
    case DatanodeProtocol.DNA_REGISTER:
      // namenode requested a registration - at start or if NN lost contact
      LOG.info("DatanodeCommand action: DNA_REGISTER");
      if (shouldRun) {
        register();
      }
      break;
    case DatanodeProtocol.DNA_FINALIZE:
      storage.finalizeUpgrade();
      break;
    case UpgradeCommand.UC_ACTION_START_UPGRADE:
      // start distributed upgrade here
      processDistributedUpgradeCommand((UpgradeCommand)cmd);
      break;
    case DatanodeProtocol.DNA_RECOVERBLOCK:
      recoverBlocks(bcmd.getBlocks(), bcmd.getTargets());
      break;
    case DatanodeProtocol.DNA_ACCESSKEYUPDATE:
      LOG.info("DatanodeCommand action: DNA_ACCESSKEYUPDATE");
      if (isAccessTokenEnabled) {
        accessTokenHandler.setKeys(((KeyUpdateCommand) cmd).getExportedKeys());
      }
      break;
    default:
      LOG.warn("Unknown DatanodeCommand action: " + cmd.getAction());
    }
    return true;
  }

  // Distributed upgrade manager
  UpgradeManagerDatanode upgradeManager = new UpgradeManagerDatanode(this);

  private void processDistributedUpgradeCommand(UpgradeCommand comm
                                               ) throws IOException {
    assert upgradeManager != null : "DataNode.upgradeManager is null.";
    upgradeManager.processUpgradeCommand(comm);
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
            LOG.warn("Panic: receiveBlockList and delHints are not of the same length" );
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
      namenode.blockReceived(dnRegistration, blockArray, delHintArray);
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

  /**
   * Report the list blocks to the Namenode
   * @throws IOException
   */
  private DatanodeCommand blockReport() throws IOException {
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
      Block[] bReport = data.getBlockReport();

      cmd = namenode.blockReport(dnRegistration,
              BlockListAsLongs.convertToArrayLongs(bReport));
      long brTime = now() - brStartTime;
      myMetrics.blockReports.inc(brTime);
      LOG.info("BlockReport of " + bReport.length +
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
    }
    return cmd;
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

  private void transferBlock( Block block, 
                              DatanodeInfo xferTargets[] 
                              ) throws IOException {
    if (!data.isValidBlock(block)) {
      // block does not exist or is under-construction
      String errStr = "Can't send invalid block " + block;
      LOG.info(errStr);
      namenode.errorReport(dnRegistration, 
                           DatanodeProtocol.INVALID_BLOCK, 
                           errStr);
      return;
    }

    // Check if NN recorded length matches on-disk length 
    long onDiskLength = data.getLength(block);
    if (block.getNumBytes() > onDiskLength) {
      // Shorter on-disk len indicates corruption so report NN the corrupt block
      namenode.reportBadBlocks(new LocatedBlock[]{
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

  private void transferBlocks( Block blocks[], 
                               DatanodeInfo xferTargets[][] 
                               ) {
    for (int i = 0; i < blocks.length; i++) {
      try {
        transferBlock(blocks[i], xferTargets[i]);
      } catch (IOException ie) {
        LOG.warn("Failed to transfer block " + blocks[i], ie);
      }
    }
  }

  /*
   * Informing the name node could take a long long time! Should we wait
   * till namenode is informed before responding with success to the
   * client? For now we don't.
   */
  protected void notifyNamenodeReceivedBlock(Block block, String delHint) {
    if(block==null || delHint==null) {
      throw new IllegalArgumentException(block==null?"Block is null":"delHint is null");
    }
    synchronized (receivedBlockList) {
      synchronized (delHints) {
        receivedBlockList.add(block);
        delHints.add(delHint);
        receivedBlockList.notifyAll();
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
  
  /** Header size for a packet */
  public static final int PKT_HEADER_LEN = ( 4 + /* Packet payload length */
                                      8 + /* offset in block */
                                      8 + /* seqno */
                                      1   /* isLastPacketInBlock */);
  


  /**
   * Used for transferring a block of data.  This class
   * sends a piece of data to another DataNode.
   */
  class DataTransfer implements Runnable {
    DatanodeInfo targets[];
    Block b;
    DataNode datanode;

    /**
     * Connect to the first item in the target list.  Pass along the 
     * entire target list, the block, and the data.
     */
    public DataTransfer(DatanodeInfo targets[], Block b, DataNode datanode) throws IOException {
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

        blockSender = new BlockSender(b, 0, b.getNumBytes(), false, false, false, 
            datanode);
        DatanodeInfo srcNode = new DatanodeInfo(dnRegistration);

        //
        // Header info
        //
        out.writeShort(DataTransferProtocol.DATA_TRANSFER_VERSION);
        out.writeByte(DataTransferProtocol.OP_WRITE_BLOCK);
        out.writeLong(b.getBlockId());
        out.writeLong(b.getGenerationStamp());
        out.writeInt(0);           // no pipelining
        out.writeBoolean(false);   // not part of recovery
        Text.writeString(out, ""); // client
        out.writeBoolean(true); // sending src node information
        srcNode.write(out); // Write src node DatanodeInfo
        // write targets
        out.writeInt(targets.length - 1);
        for (int i = 1; i < targets.length; i++) {
          targets[i].write(out);
        }
        AccessToken accessToken = AccessToken.DUMMY_TOKEN;
        if (isAccessTokenEnabled) {
          accessToken = accessTokenHandler.generateToken(null, b.getBlockId(),
              EnumSet.of(AccessTokenHandler.AccessMode.WRITE));
        }
        accessToken.write(out);
        // send data & checksum
        blockSender.sendBlock(out, baseStream, null);

        // no response necessary
        LOG.info(dnRegistration + ":Transmitted block " + b + " to " + curTarget);

      } catch (IOException ie) {
        LOG.warn(dnRegistration + ":Failed to transfer " + b + " to " + targets[0].getName()
            + " got " + StringUtils.stringifyException(ie));
      } finally {
        xmitsInProgress.getAndDecrement();
        IOUtils.closeStream(blockSender);
        IOUtils.closeStream(out);
        IOUtils.closeSocket(sock);
      }
    }
  }

  /**
   * No matter what kind of exception we get, keep retrying to offerService().
   * That's the loop that connects to the NameNode and provides basic DataNode
   * functionality.
   *
   * Only stop when "shouldRun" is turned off (which can only happen at shutdown).
   */
  public void run() {
    LOG.info(dnRegistration + "In DataNode.run, data = " + data);

    // start dataXceiveServer
    dataXceiverServer.start();
        
    while (shouldRun) {
      try {
        startDistributedUpgradeIfNeeded();
        offerService();
      } catch (Exception ex) {
        LOG.error("Exception while in state " + getServiceState()
                + " and shouldRun=" + shouldRun + ": " + ex,
                ex);
        if (shouldRun) {
          try {
            Thread.sleep(5000);
          } catch (InterruptedException ie) {
          }
        }
      }
    }
        
    LOG.info(dnRegistration + ":Finishing DataNode in: "+data);
    shutdown();
  }
    
  /** Start a single datanode daemon and wait for it to finish.
   *  If this thread is specifically interrupted, it will stop waiting.
   */
  public static void runDatanodeDaemon(DataNode dn) throws IOException {
    if (dn != null) {
      //register datanode
      dn.register();
      dn.dataNodeThread = new Thread(dn, dnThreadName);
      dn.dataNodeThread.setDaemon(true); // needed for JUnit testing
      dn.dataNodeThread.start();
    }
  }
  
  static boolean isDatanodeUp(DataNode dn) {
    return dn.dataNodeThread != null && dn.dataNodeThread.isAlive();
  }

  /** Instantiate a single datanode object. This must be run by invoking
   *  {@link DataNode#runDatanodeDaemon(DataNode)} subsequently. 
   */
  public static DataNode instantiateDataNode(String args[],
                                      Configuration conf) throws IOException {
    if (conf == null)
      conf = new Configuration();
    // parse generic hadoop options
    GenericOptionsParser hParser = new GenericOptionsParser(conf, args);
    args = hParser.getRemainingArgs();
    
    if (!parseArguments(args, conf)) {
      printUsage();
      return null;
    }
    if (conf.get("dfs.network.script") != null) {
      LOG.error("This configuration for rack identification is not supported" +
          " anymore. RackID resolution is handled by the NameNode.");
      System.exit(-1);
    }
    String[] dataDirs = conf.getStrings("dfs.data.dir");
    dnThreadName = "DataNode: [" +
                        StringUtils.arrayToString(dataDirs) + "]";
    return makeInstance(dataDirs, conf);
  }

  /** Instantiate & Start a single datanode daemon and wait for it to finish.
   *  If this thread is specifically interrupted, it will stop waiting.
   */
  public static DataNode createDataNode(String args[],
                                 Configuration conf) throws IOException {
    DataNode dn = instantiateDataNode(args, conf);
    runDatanodeDaemon(dn);
    return dn;
  }

  void join() {
    if (dataNodeThread != null) {
      try {
        dataNodeThread.join();
      } catch (InterruptedException e) {}
    }
  }

  /**
   * Make an instance of DataNode after ensuring that at least one of the
   * given data directories (and their parent directories, if necessary)
   * can be created.
   * @param dataDirs List of directories, where the new DataNode instance should
   * keep its files.
   * @param conf Configuration instance to use.
   * @return DataNode instance for given list of data dirs and conf, or null if
   * no directory from this directory list can be created.
   * @throws IOException if problems occur when starting the data node
   */
  public static DataNode makeInstance(String[] dataDirs, Configuration conf)
    throws IOException {
    ArrayList<File> dirs = new ArrayList<File>();
    StringBuffer invalid = new StringBuffer();
    for (String dataDir : dataDirs) {
      File data = new File(dataDir);
      try {
        DiskChecker.checkDir(data);
        dirs.add(data);
      } catch(DiskErrorException e) {
        LOG.warn("Invalid directory in dfs.data.dir: " + e, e);
        invalid.append(dataDir);
        invalid.append(" ");
      }
    }
    if (dirs.size() > 0) {
      DataNode dataNode = new DataNode(conf, dirs);
      Service.startService(dataNode);
      return dataNode;
    } else {
      LOG.error("All directories in dfs.data.dir are invalid: " + invalid);
      return null;
    }
  }

  /**
   * {@inheritDoc}
   *
   * @return the name of this service
   */
  @Override
  public String getServiceName() {
    return "DataNode";
  }
  
  @Override
  public String toString() {
    return getServiceName() + " {" +
      "data=" + data +
      ", localName='" + dnRegistration.getName() + "'" +
      ", storageID='" + dnRegistration.getStorageID() + "'" +
      ", xmitsInProgress=" + xmitsInProgress.get() +
      ", state=" + getServiceState() +
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
   * This methods  arranges for the data node to send the block report at the next heartbeat.
   */
  public void scheduleBlockReport(long delay) {
    if (delay > 0) { // send BR after random delay
      lastBlockReport = System.currentTimeMillis()
                            - ( blockReportInterval - R.nextInt((int)(delay)));
    } else { // send at next heartbeat
      lastBlockReport = lastHeartbeat - blockReportInterval;
    }
    resetBlockReportTime = true; // reset future BRs for randomness
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

  /**
   */
  public static void main(String args[]) {
    try {
      StringUtils.startupShutdownMessage(DataNode.class, args, LOG);
      DataNode datanode = createDataNode(args, null);
      if (datanode != null)
        datanode.join();
    } catch (Throwable e) {
      LOG.error(StringUtils.stringifyException(e));
      System.exit(-1);
    }
  }

  // InterDataNodeProtocol implementation
  /** {@inheritDoc} */
  public BlockMetaDataInfo getBlockMetaDataInfo(Block block
      ) throws IOException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("block=" + block);
    }
    Block stored = data.getStoredBlock(block.getBlockId());

    if (stored == null) {
      return null;
    }
    BlockMetaDataInfo info = new BlockMetaDataInfo(stored,
                                 blockScanner.getLastScanTime(stored));
    if (LOG.isDebugEnabled()) {
      LOG.debug("getBlockMetaDataInfo successful block=" + stored +
                " length " + stored.getNumBytes() +
                " genstamp " + stored.getGenerationStamp());
    }

    // paranoia! verify that the contents of the stored block
    // matches the block file on disk.
    data.validateBlockMetadata(stored);
    return info;
  }

  public Daemon recoverBlocks(final Block[] blocks, final DatanodeInfo[][] targets) {
    Daemon d = new Daemon(threadGroup, new Runnable() {
      /** Recover a list of blocks. It is run by the primary datanode. */
      public void run() {
        for(int i = 0; i < blocks.length; i++) {
          try {
            logRecoverBlock("NameNode", blocks[i], targets[i]);
            recoverBlock(blocks[i], false, targets[i], true);
          } catch (IOException e) {
            LOG.warn("recoverBlocks FAILED, blocks[" + i + "]=" + blocks[i], e);
          }
        }
      }
    });
    d.start();
    return d;
  }

  /** {@inheritDoc} */
  public void updateBlock(Block oldblock, Block newblock, boolean finalize) throws IOException {
    LOG.info("oldblock=" + oldblock + "(length=" + oldblock.getNumBytes()
        + "), newblock=" + newblock + "(length=" + newblock.getNumBytes()
        + "), datanode=" + dnRegistration.getName());
    data.updateBlock(oldblock, newblock);
    if (finalize) {
      data.finalizeBlock(newblock);
      myMetrics.blocksWritten.inc(); 
      notifyNamenodeReceivedBlock(newblock, EMPTY_DEL_HINT);
      LOG.info("Received block " + newblock +
                " of size " + newblock.getNumBytes() +
                " as part of lease recovery.");
    }
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

  /** A convenient class used in lease recovery */
  private static class BlockRecord { 
    final DatanodeID id;
    final InterDatanodeProtocol datanode;
    final Block block;
    
    BlockRecord(DatanodeID id, InterDatanodeProtocol datanode, Block block) {
      this.id = id;
      this.datanode = datanode;
      this.block = block;
    }

    /** {@inheritDoc} */
    public String toString() {
      return "block:" + block + " node:" + id;
    }
  }

  /** Recover a block */
  private LocatedBlock recoverBlock(Block block, boolean keepLength,
      DatanodeID[] datanodeids, boolean closeFile) throws IOException {

    // If the block is already being recovered, then skip recovering it.
    // This can happen if the namenode and client start recovering the same
    // file at the same time.
    synchronized (ongoingRecovery) {
      Block tmp = new Block();
      tmp.set(block.getBlockId(), block.getNumBytes(), GenerationStamp.WILDCARD_STAMP);
      if (ongoingRecovery.get(tmp) != null) {
        String msg = "Block " + block + " is already being recovered, " +
                     " ignoring this request to recover it.";
        LOG.info(msg);
        throw new IOException(msg);
      }
      ongoingRecovery.put(block, block);
    }
    try {
      List<BlockRecord> syncList = new ArrayList<BlockRecord>();
      long minlength = Long.MAX_VALUE;
      int errorCount = 0;

      //check generation stamps
      for(DatanodeID id : datanodeids) {
        try {
          InterDatanodeProtocol datanode = dnRegistration.equals(id)?
              this: DataNode.createInterDataNodeProtocolProxy(id, getConf());
          BlockMetaDataInfo info = datanode.getBlockMetaDataInfo(block);
          if (info != null && info.getGenerationStamp() >= block.getGenerationStamp()) {
            if (keepLength) {
              if (info.getNumBytes() == block.getNumBytes()) {
                syncList.add(new BlockRecord(id, datanode, new Block(info)));
              }
            }
            else {
              syncList.add(new BlockRecord(id, datanode, new Block(info)));
              if (info.getNumBytes() < minlength) {
                minlength = info.getNumBytes();
              }
            }
          }
        } catch (IOException e) {
          ++errorCount;
          InterDatanodeProtocol.LOG.warn(
              "Failed to getBlockMetaDataInfo for block (=" + block 
              + ") from datanode (=" + id + ")", e);
        }
      }

      if (syncList.isEmpty() && errorCount > 0) {
        throw new IOException("All datanodes failed: block=" + block
            + ", datanodeids=" + Arrays.asList(datanodeids));
      }
      if (!keepLength) {
        block.setNumBytes(minlength);
      }
      return syncBlock(block, syncList, closeFile);
    } finally {
      synchronized (ongoingRecovery) {
        ongoingRecovery.remove(block);
      }
    }
  }

  /** Block synchronization */
  private LocatedBlock syncBlock(Block block, List<BlockRecord> syncList,
      boolean closeFile) throws IOException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("block=" + block + ", (length=" + block.getNumBytes()
          + "), syncList=" + syncList + ", closeFile=" + closeFile);
    }

    //syncList.isEmpty() that all datanodes do not have the block
    //so the block can be deleted.
    if (syncList.isEmpty()) {
      namenode.commitBlockSynchronization(block, 0, 0, closeFile, true,
          DatanodeID.EMPTY_ARRAY);
      return null;
    }

    List<DatanodeID> successList = new ArrayList<DatanodeID>();

    long generationstamp = namenode.nextGenerationStamp(block);
    Block newblock = new Block(block.getBlockId(), block.getNumBytes(), generationstamp);

    for(BlockRecord r : syncList) {
      try {
        r.datanode.updateBlock(r.block, newblock, closeFile);
        successList.add(r.id);
      } catch (IOException e) {
        InterDatanodeProtocol.LOG.warn("Failed to updateBlock (newblock="
            + newblock + ", datanode=" + r.id + ")", e);
      }
    }

    if (!successList.isEmpty()) {
      DatanodeID[] nlist = successList.toArray(new DatanodeID[successList.size()]);

      namenode.commitBlockSynchronization(block,
          newblock.getGenerationStamp(), newblock.getNumBytes(), closeFile, false,
          nlist);
      DatanodeInfo[] info = new DatanodeInfo[nlist.length];
      for (int i = 0; i < nlist.length; i++) {
        info[i] = new DatanodeInfo(nlist[i]);
      }
      return new LocatedBlock(newblock, info); // success
    }

    //failed
    StringBuilder b = new StringBuilder();
    for(BlockRecord r : syncList) {
      b.append("\n  " + r.id);
    }
    throw new IOException("Cannot recover " + block + ", none of these "
        + syncList.size() + " datanodes success {" + b + "\n}");
  }
  
  // ClientDataNodeProtocol implementation
  /** {@inheritDoc} */
  public LocatedBlock recoverBlock(Block block, boolean keepLength, DatanodeInfo[] targets
      ) throws IOException {
    logRecoverBlock("Client", block, targets);
    return recoverBlock(block, keepLength, targets, false);
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
}
