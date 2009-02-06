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
package org.apache.hadoop.dfs;

import org.apache.commons.logging.*;

import org.apache.hadoop.fs.ChecksumException;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.ipc.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.net.DNS;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.net.SocketOutputStream;
import org.apache.hadoop.util.*;
import org.apache.hadoop.util.DiskChecker.DiskErrorException;
import org.apache.hadoop.util.DiskChecker.DiskOutOfSpaceException;
import org.apache.hadoop.dfs.IncorrectVersionException;
import org.apache.hadoop.mapred.StatusHttpServer;
import org.apache.hadoop.dfs.BlockCommand;
import org.apache.hadoop.dfs.DatanodeProtocol;
import org.apache.hadoop.dfs.FSDatasetInterface.MetaDataInputStream;
import org.apache.hadoop.dfs.datanode.metrics.DataNodeMetrics;
import org.apache.hadoop.dfs.BlockMetadataHeader;

import java.io.*;
import java.net.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.*;
import java.util.concurrent.Semaphore;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;

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
public class DataNode extends Configured 
    implements InterDatanodeProtocol, ClientDatanodeProtocol, FSConstants, Runnable {
  public static final Log LOG = LogFactory.getLog("org.apache.hadoop.dfs.DataNode");

  /**
   * Use {@link NetUtils#createSocketAddr(String)} instead.
   */
  @Deprecated
  public static InetSocketAddress createSocketAddr(String target
                                                   ) throws IOException {
    return NetUtils.createSocketAddr(target);
  }

  /**
   * Minimum buffer used while sending data to clients. Used only if
   * transferTo() is enabled. 64KB is not that large. It could be larger, but
   * not sure if there will be much more improvement.
   */
  private static final int MIN_BUFFER_WITH_TRANSFERTO = 64*1024;
  
  DatanodeProtocol namenode = null;
  FSDatasetInterface data = null;
  DatanodeRegistration dnRegistration = null;

  volatile boolean shouldRun = true;
  private LinkedList<Block> receivedBlockList = new LinkedList<Block>();
  private LinkedList<String> delHints = new LinkedList<String>();
  final static String EMPTY_DEL_HINT = "";
  int xmitsInProgress = 0;
  Daemon dataXceiveServer = null;
  ThreadGroup threadGroup = null;
  long blockReportInterval;
  //disallow the sending of BR before instructed to do so
  long lastBlockReport = Long.MAX_VALUE;
  boolean resetBlockReportTime = true;
  long initialBlockReportDelay = BLOCKREPORT_INITIAL_DELAY * 1000L;
  private boolean waitForFirstBlockReportRequest = false;
  long lastHeartbeat = 0;
  long heartBeatInterval;
  private DataStorage storage = null;
  private StatusHttpServer infoServer = null;
  private DataNodeMetrics myMetrics;
  private static InetSocketAddress nameNodeAddr;
  private InetSocketAddress selfAddr;
  private static DataNode datanodeObject = null;
  private Thread dataNodeThread = null;
  String machineName;
  private static String dnThreadName;
  private int socketTimeout;
  private int socketWriteTimeout = 0;  
  private boolean transferToAllowed = true;
  private int writePacketSize = 0;
  
  DataBlockScanner blockScanner = null;
  Daemon blockScannerThread = null;
  
  private static final Random R = new Random();

  /**
   * Maximal number of concurrent xceivers per node.
   * Enforcing the limit is required in order to avoid data-node
   * running out of memory.
   */
  private static final int MAX_XCEIVER_COUNT = 256;
  private int maxXceiverCount = MAX_XCEIVER_COUNT;
  
  /** A manager to make sure that cluster balancing does not
   * take too much resources.
   *
   * It limits the number of block moves for balancing and
   * the total amount of bandwidth they can use.
   */
  private static class BlockBalanceThrottler extends Throttler {
   private int numThreads;

   /**Constructor
    *
    * @param bandwidth Total amount of bandwidth can be used for balancing
    */
   private BlockBalanceThrottler(long bandwidth) {
     super(bandwidth);
     LOG.info("Balancing bandwith is "+ bandwidth + " bytes/s");
   }

   /** Check if the block move can start.
    *
    * Return true if the thread quota is not exceeded and
    * the counter is incremented; False otherwise.
    */
   private synchronized boolean acquire() {
     if (numThreads >= Balancer.MAX_NUM_CONCURRENT_MOVES) {
       return false;
     }
     numThreads++;
     return true;
   }

   /** Mark that the move is completed. The thread counter is decremented. */
   private synchronized void release() {
     numThreads--;
   }
  }

  private BlockBalanceThrottler balancingThrottler;

  /**
   * We need an estimate for block size to check if the disk partition has
   * enough space. For now we set it to be the default block size set
   * in the server side configuration, which is not ideal because the
   * default block size should be a client-size configuration. 
   * A better solution is to include in the header the estimated block size,
   * i.e. either the actual block size or the default block size.
   */
  private long estimateBlockSize;
  
  // For InterDataNodeProtocol
  Server ipcServer;
  
  // Record all sockets opend for data transfer
  Map<Socket, Socket> childSockets = Collections.synchronizedMap(
                                       new HashMap<Socket, Socket>());
  
  /**
   * Current system time.
   * @return current time in msec.
   */
  static long now() {
    return System.currentTimeMillis();
  }

  /**
   * Create the DataNode given a configuration and an array of dataDirs.
   * 'dataDirs' is where the blocks are stored.
   */
  DataNode(Configuration conf, 
           AbstractList<File> dataDirs) throws IOException {
    super(conf);
    datanodeObject = this;

    try {
      startDataNode(conf, dataDirs);
    } catch (IOException ie) {
      shutdown();
      throw ie;
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
    InetSocketAddress nameNodeAddr = NameNode.getAddress(conf);
    
    this.estimateBlockSize = conf.getLong("dfs.block.size", DEFAULT_BLOCK_SIZE);
    this.socketTimeout =  conf.getInt("dfs.socket.timeout",
                                      FSConstants.READ_TIMEOUT);
    this.socketWriteTimeout = conf.getInt("dfs.datanode.socket.write.timeout",
                                          FSConstants.WRITE_TIMEOUT);
    /* Based on results on different platforms, we might need set the default 
     * to false on some of them. */
    this.transferToAllowed = conf.getBoolean("dfs.datanode.transferTo.allowed", 
                                             true);
    this.writePacketSize = conf.getInt("dfs.write.packet.size", 64*1024);
    String address = 
      NetUtils.getServerAddress(conf,
                                "dfs.datanode.bindAddress", 
                                "dfs.datanode.port",
                                "dfs.datanode.address");
    InetSocketAddress socAddr = NetUtils.createSocketAddr(address);
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
              Class.forName("org.apache.hadoop.dfs.SimulatedFSDataset"), conf);
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
      
    this.maxXceiverCount = conf.getInt("dfs.datanode.max.xcievers", MAX_XCEIVER_COUNT);
    this.threadGroup = new ThreadGroup("dataXceiveServer");
    this.dataXceiveServer = new Daemon(threadGroup, new DataXceiveServer(ss));
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
    DataNode.nameNodeAddr = nameNodeAddr;

    this.balancingThrottler = new BlockBalanceThrottler(
      conf.getLong("dfs.balance.bandwidthPerSec", 1024L*1024));

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
    String infoAddr = 
      NetUtils.getServerAddress(conf, 
                              "dfs.datanode.info.bindAddress", 
                              "dfs.datanode.info.port",
                              "dfs.datanode.http.address");
    InetSocketAddress infoSocAddr = NetUtils.createSocketAddr(infoAddr);
    String infoHost = infoSocAddr.getHostName();
    int tmpInfoPort = infoSocAddr.getPort();
    this.infoServer = new StatusHttpServer("datanode", infoHost, tmpInfoPort, tmpInfoPort == 0);
    InetSocketAddress secInfoSocAddr = NetUtils.createSocketAddr(
        conf.get("dfs.datanode.https.address", infoHost + ":" + 0));
    Configuration sslConf = new Configuration(conf);
    sslConf.addResource(conf.get("https.keystore.info.rsrc", "sslinfo.xml"));
    String keyloc = sslConf.get("https.keystore.location");
    if (null != keyloc) {
      this.infoServer.addSslListener(secInfoSocAddr, keyloc,
          sslConf.get("https.keystore.password", ""),
          sslConf.get("https.keystore.keypassword", ""));
    }
    this.infoServer.addServlet(null, "/streamFile/*", StreamFile.class);
    this.infoServer.setAttribute("datanode.blockScanner", blockScanner);
    this.infoServer.addServlet(null, "/blockScannerReport", 
                               DataBlockScanner.Servlet.class);
    this.infoServer.start();
    // adjust info port
    this.dnRegistration.setInfoPort(this.infoServer.getPort());
    myMetrics = new DataNodeMetrics(conf, dnRegistration.getStorageID());
    
    //init ipc server
    InetSocketAddress ipcAddr = NetUtils.createSocketAddr(
        conf.get("dfs.datanode.ipc.address"));
    ipcServer = RPC.getServer(this, ipcAddr.getHostName(), ipcAddr.getPort(), 
        conf.getInt("dfs.datanode.handler.count", 3), false, conf);
    ipcServer.start();
    dnRegistration.setIpcPort(ipcServer.getListenerAddress().getPort());

    LOG.info("dnRegistration = " + dnRegistration);
  }

  /**
   * Creates either NIO or regular depending on socketWriteTimeout.
   */
  private Socket newSocket() throws IOException {
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

  /** Return the DataNode object
   * 
   */
  public static DataNode getDataNode() {
    return datanodeObject;
  } 

  static InterDatanodeProtocol createInterDataNodeProtocolProxy(
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

  static void setNewStorageID(DatanodeRegistration dnReg) {
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
   * @see FSNamesystem#registerDatanode(DatanodeRegistration,String)
   * @throws IOException
   */
  private void register() throws IOException {
    if (dnRegistration.getStorageID().equals("")) {
      setNewStorageID(dnRegistration);
    }
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
    waitForFirstBlockReportRequest = true;
  }

  /**
   * Shut down this instance of the datanode.
   * Returns only after shutdown is complete.
   * This method can only be called by the offerService thread.
   * Otherwise, deadlock might occur.
   */
  public void shutdown() {
    if (infoServer != null) {
      try {
        infoServer.stop();
      } catch (Exception e) {
      }
    }
    if (ipcServer != null) {
      ipcServer.stop();
    }
    this.shouldRun = false;
    if (dataXceiveServer != null) {
      ((DataXceiveServer) this.dataXceiveServer.getRunnable()).kill();
      this.dataXceiveServer.interrupt();

      // wait for all data receiver threads to exit
      if (this.threadGroup != null) {
        while (true) {
          this.threadGroup.interrupt();
          LOG.info("Waiting for threadgroup to exit, active threads is " +
                   this.threadGroup.activeCount());
          if (this.threadGroup.activeCount() == 0) {
            break;
          }
          try {
            Thread.sleep(1000);
          } catch (InterruptedException e) {}
        }
      }
      try {
        this.dataXceiveServer.join();
      } catch (InterruptedException ie) {
      }
    }
    
    RPC.stopProxy(namenode); // stop the RPC threads
    
    if(upgradeManager != null)
      upgradeManager.shutdownUpgrade();
    if (blockScanner != null)
      blockScanner.shutdown();
    if (blockScannerThread != null) 
      blockScannerThread.interrupt();
    if (storage != null) {
      try {
        this.storage.unlockAll();
      } catch (IOException ie) {
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
  private void checkDiskError( IOException e ) throws IOException {
    if (e.getMessage() != null &&
        e.getMessage().startsWith("No space left on device")) {
      throw new DiskOutOfSpaceException("No space left on device");
    } else {
      checkDiskError();
    }
  }
  
  /* Check if there is no disk space and if so, handle the error*/
  private void checkDiskError( ) throws IOException {
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
          DatanodeCommand cmd = namenode.sendHeartbeat(dnRegistration,
                                                       data.getCapacity(),
                                                       data.getDfsUsed(),
                                                       data.getRemaining(),
                                                       xmitsInProgress,
                                                       getXceiverCount());
          myMetrics.heartbeats.inc(now() - startTime);
          //LOG.info("Just sent heartbeat, with name " + localName);
          if (!processCommand(cmd))
            continue;
        }
            
        // check if there are newly received blocks
        Block [] blockArray=null;
        String [] delHintArray=null;
        synchronized(receivedBlockList) {
          synchronized(delHints) {
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
          synchronized (receivedBlockList) {
            synchronized (delHints) {
              for(int i=0; i<blockArray.length; i++) {
                receivedBlockList.remove(blockArray[i]);
                delHints.remove(delHintArray[i]);
              }
            }
          }
        }

        // send block report
        if (startTime - lastBlockReport > blockReportInterval) {
          //
          // Send latest blockinfo report if timer has expired.
          // Get back a list of local block(s) that are obsolete
          // and can be safely GC'ed.
          //
          long brStartTime = now();
          Block[] bReport = data.getBlockReport();
          DatanodeCommand cmd = namenode.blockReport(dnRegistration,
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
          processCommand(cmd);
        }

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
        if (UnregisteredDatanodeException.class.getName().equals(reClass) ||
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
    case DatanodeProtocol.DNA_BLOCKREPORT:
      // only send BR when receive request the 1st time
      if (waitForFirstBlockReportRequest) {
        // dropping all following BR requests
        waitForFirstBlockReportRequest = false;
        // random short delay - helps scatter the BR from all DNs
        scheduleBlockReport(initialBlockReportDelay);
      }
      break;
    case DatanodeProtocol.DNA_RECOVERBLOCK:
      recoverBlocks(bcmd.getBlocks(), bcmd.getTargets());
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
  private void transferBlocks( Block blocks[], 
                               DatanodeInfo xferTargets[][] 
                               ) throws IOException {
    for (int i = 0; i < blocks.length; i++) {
      if (!data.isValidBlock(blocks[i])) {
        String errStr = "Can't send invalid block " + blocks[i];
        LOG.info(errStr);
        namenode.errorReport(dnRegistration, 
                             DatanodeProtocol.INVALID_BLOCK, 
                             errStr);
        break;
      }
      int numTargets = xferTargets[i].length;
      if (numTargets > 0) {
        if (LOG.isInfoEnabled()) {
          StringBuilder xfersBuilder = new StringBuilder();
          for (int j = 0; j < numTargets; j++) {
            DatanodeInfo nodeInfo = xferTargets[i][j];
            xfersBuilder.append(nodeInfo.getName());
            if (j < (numTargets - 1)) {
              xfersBuilder.append(", ");
            }
          }
          String xfersTo = xfersBuilder.toString();
          LOG.info(dnRegistration + " Starting thread to transfer block " + 
                   blocks[i] + " to " + xfersTo);                       
        }
        new Daemon(new DataTransfer(xferTargets[i], blocks[i])).start();
      }
    }
  }

  /* utility function for receiving a response */
  private static void receiveResponse(Socket s, int numTargets) throws IOException {
    // check the response
    DataInputStream reply = new DataInputStream(new BufferedInputStream(
                                NetUtils.getInputStream(s), BUFFER_SIZE));
    try {
      for (int i = 0; i < numTargets; i++) {
        short opStatus = reply.readShort();
        if(opStatus != OP_STATUS_SUCCESS) {
          throw new IOException("operation failed at "+
              s.getInetAddress());
        } 
      }
    } finally {
      IOUtils.closeStream(reply);
    }
  }

  /* utility function for sending a respose */
  private static void sendResponse(Socket s, short opStatus, long timeout) 
                                                       throws IOException {
    DataOutputStream reply = 
      new DataOutputStream(NetUtils.getOutputStream(s, timeout));
    try {
      reply.writeShort(opStatus);
      reply.flush();
    } finally {
      IOUtils.closeStream(reply);
    }
  }

  /*
   * Informing the name node could take a long long time! Should we wait
   * till namenode is informed before responding with success to the
   * client? For now we don't.
   */
  private void notifyNamenodeReceivedBlock(Block block, String delHint) {
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

  /**
   * Server used for receiving/sending a block of data.
   * This is created to listen for requests from clients or 
   * other DataNodes.  This small server does not use the 
   * Hadoop IPC mechanism.
   */
  class DataXceiveServer implements Runnable {
    ServerSocket ss;
    public DataXceiveServer(ServerSocket ss) {
      this.ss = ss;
    }

    /**
     */
    public void run() {
      while (shouldRun) {
        try {
          Socket s = ss.accept();
          s.setTcpNoDelay(true);
          new Daemon(threadGroup, new DataXceiver(s)).start();
        } catch (SocketTimeoutException ignored) {
          // wake up to see if should continue to run
        } catch (IOException ie) {
          LOG.warn(dnRegistration + ":DataXceiveServer: " 
                                  + StringUtils.stringifyException(ie));
        } catch (Throwable te) {
          LOG.error(dnRegistration + ":DataXceiveServer: Exiting due to:" 
                                   + StringUtils.stringifyException(te));
          shouldRun = false;
        }
      }
      try {
        ss.close();
      } catch (IOException ie) {
        LOG.warn(dnRegistration + ":DataXceiveServer: " 
                                + StringUtils.stringifyException(ie));
      }
    }
    public void kill() {
      assert shouldRun == false :
        "shoudRun should be set to false before killing";
      try {
        this.ss.close();
      } catch (IOException ie) {
        LOG.warn(dnRegistration + ":DataXceiveServer.kill(): " 
                                + StringUtils.stringifyException(ie));
      }

      // close all the sockets that were accepted earlier
      synchronized (childSockets) {
        for (Iterator<Socket> it = childSockets.values().iterator();
             it.hasNext();) {
          Socket thissock = it.next();
          try {
            thissock.close();
          } catch (IOException e) {
          }
        }
      }
    }
  }

  /**
   * Thread for processing incoming/outgoing data stream
   */
  class DataXceiver implements Runnable {
    Socket s;
    String remoteAddress; // address of remote side
    String localAddress;  // local address of this daemon
    public DataXceiver(Socket s) {
      this.s = s;
      childSockets.put(s, s);
      InetSocketAddress isock = (InetSocketAddress)s.getRemoteSocketAddress();
      remoteAddress = isock.toString();
      localAddress = s.getInetAddress() + ":" + s.getLocalPort();
      LOG.debug("Number of active connections is: " + getXceiverCount());
    }

    /**
     * Read/write data from/to the DataXceiveServer.
     */
    public void run() {
      DataInputStream in=null; 
      try {
        in = new DataInputStream(
            new BufferedInputStream(NetUtils.getInputStream(s), 
                                    SMALL_BUFFER_SIZE));
        short version = in.readShort();
        if ( version != DATA_TRANSFER_VERSION ) {
          throw new IOException( "Version Mismatch" );
        }
        boolean local = s.getInetAddress().equals(s.getLocalAddress());
        byte op = in.readByte();
        // Make sure the xciver count is not exceeded
        int curXceiverCount = getXceiverCount();
        if (curXceiverCount > maxXceiverCount) {
          throw new IOException("xceiverCount " + curXceiverCount
                                + " exceeds the limit of concurrent xcievers "
                                + maxXceiverCount);
        }
        long startTime = now();
        switch ( op ) {
        case OP_READ_BLOCK:
          readBlock( in );
          myMetrics.readBlockOp.inc(now() - startTime);
          if (local)
            myMetrics.readsFromLocalClient.inc();
          else
            myMetrics.readsFromRemoteClient.inc();
          break;
        case OP_WRITE_BLOCK:
          writeBlock( in );
          myMetrics.writeBlockOp.inc(now() - startTime);
          if (local)
            myMetrics.writesFromLocalClient.inc();
          else
            myMetrics.writesFromRemoteClient.inc();
          break;
        case OP_READ_METADATA:
          readMetadata( in );
          myMetrics.readMetadataOp.inc(now() - startTime);
          break;
        case OP_REPLACE_BLOCK: // for balancing purpose; send to a destination
          replaceBlock(in);
          myMetrics.replaceBlockOp.inc(now() - startTime);
          break;
        case OP_COPY_BLOCK: // for balancing purpose; send to a proxy source
          copyBlock(in);
          myMetrics.copyBlockOp.inc(now() - startTime);
          break;
        default:
          throw new IOException("Unknown opcode " + op + " in data stream");
        }
      } catch (Throwable t) {
        LOG.error(dnRegistration + ":DataXceiver: " + StringUtils.stringifyException(t));
      } finally {
        LOG.debug(dnRegistration + ":Number of active connections is: "
                                 + getXceiverCount());
        IOUtils.closeStream(in);
        IOUtils.closeSocket(s);
        childSockets.remove(s);
      }
    }

    /**
     * Read a block from the disk
     * @param in The stream to read from
     * @throws IOException
     */
    private void readBlock(DataInputStream in) throws IOException {
      //
      // Read in the header
      //
      long blockId = in.readLong();          
      Block block = new Block( blockId, 0 , in.readLong());

      long startOffset = in.readLong();
      long length = in.readLong();

      // send the block
      OutputStream baseStream = NetUtils.getOutputStream(s,socketWriteTimeout);
      DataOutputStream out = new DataOutputStream(
                   new BufferedOutputStream(baseStream, SMALL_BUFFER_SIZE));
      
      BlockSender blockSender = null;
      try {
        try {
          blockSender = new BlockSender(block, startOffset, length, 
                                        true, true, false);
        } catch(IOException e) {
          out.writeShort(OP_STATUS_ERROR);
          throw e;
        }

        out.writeShort(DataNode.OP_STATUS_SUCCESS); // send op status
        long read = blockSender.sendBlock(out, baseStream, null); // send data

        if (blockSender.isBlockReadFully()) {
          // See if client verification succeeded. 
          // This is an optional response from client.
          try {
            if (in.readShort() == OP_STATUS_CHECKSUM_OK  && 
                blockScanner != null) {
              blockScanner.verifiedByClient(block);
            }
          } catch (IOException ignored) {}
        }
        
        myMetrics.bytesRead.inc((int) read);
        myMetrics.blocksRead.inc();
        LOG.info(dnRegistration + " Served block " + block + " to " + s.getInetAddress());
      } catch ( SocketException ignored ) {
        // Its ok for remote side to close the connection anytime.
        myMetrics.blocksRead.inc();
      } catch ( IOException ioe ) {
        /* What exactly should we do here?
         * Earlier version shutdown() datanode if there is disk error.
         */
        LOG.warn(dnRegistration +  ":Got exception while serving " + block + " to " +
                  s.getInetAddress() + ":\n" + 
                  StringUtils.stringifyException(ioe) );
        throw ioe;
      } finally {
        IOUtils.closeStream(out);
        IOUtils.closeStream(blockSender);
      }
    }

    /**
     * Write a block to disk.
     * 
     * @param in The stream to read from
     * @throws IOException
     */
    private void writeBlock(DataInputStream in) throws IOException {
      DatanodeInfo srcDataNode = null;
      LOG.debug("writeBlock receive buf size " + s.getReceiveBufferSize() +
                " tcp no delay " + s.getTcpNoDelay());
      //
      // Read in the header
      //
      Block block = new Block(in.readLong(), estimateBlockSize, in.readLong());
      LOG.info("Receiving block " + block + 
               " src: " + remoteAddress +
               " dest: " + localAddress);
      int pipelineSize = in.readInt(); // num of datanodes in entire pipeline
      boolean isRecovery = in.readBoolean(); // is this part of recovery?
      String client = Text.readString(in); // working on behalf of this client
      boolean hasSrcDataNode = in.readBoolean(); // is src node info present
      if (hasSrcDataNode) {
        srcDataNode = new DatanodeInfo();
        srcDataNode.readFields(in);
      }
      int numTargets = in.readInt();
      if (numTargets < 0) {
        throw new IOException("Mislabelled incoming datastream.");
      }
      DatanodeInfo targets[] = new DatanodeInfo[numTargets];
      for (int i = 0; i < targets.length; i++) {
        DatanodeInfo tmp = new DatanodeInfo();
        tmp.readFields(in);
        targets[i] = tmp;
      }

      DataOutputStream mirrorOut = null;  // stream to next target
      DataInputStream mirrorIn = null;    // reply from next target
      DataOutputStream replyOut = null;   // stream to prev target
      Socket mirrorSock = null;           // socket to next target
      BlockReceiver blockReceiver = null; // responsible for data handling
      String mirrorNode = null;           // the name:port of next target
      String firstBadLink = "";           // first datanode that failed in connection setup
      try {
        // open a block receiver and check if the block does not exist
        blockReceiver = new BlockReceiver(block, in, 
            s.getInetAddress().toString(), isRecovery, client, srcDataNode);

        // get a connection back to the previous target
        replyOut = new DataOutputStream(
                       NetUtils.getOutputStream(s, socketWriteTimeout));

        //
        // Open network conn to backup machine, if 
        // appropriate
        //
        if (targets.length > 0) {
          InetSocketAddress mirrorTarget = null;
          // Connect to backup machine
          mirrorNode = targets[0].getName();
          mirrorTarget = NetUtils.createSocketAddr(mirrorNode);
          mirrorSock = newSocket();
          try {
            int timeoutValue = numTargets * socketTimeout;
            int writeTimeout = socketWriteTimeout + 
                               (WRITE_TIMEOUT_EXTENSION * numTargets);
            mirrorSock.connect(mirrorTarget, timeoutValue);
            mirrorSock.setSoTimeout(timeoutValue);
            mirrorSock.setSendBufferSize(DEFAULT_DATA_SOCKET_SIZE);
            mirrorOut = new DataOutputStream(
               new BufferedOutputStream(
                           NetUtils.getOutputStream(mirrorSock, writeTimeout),
                           SMALL_BUFFER_SIZE));
            mirrorIn = new DataInputStream(NetUtils.getInputStream(mirrorSock));

            // Write header: Copied from DFSClient.java!
            mirrorOut.writeShort( DATA_TRANSFER_VERSION );
            mirrorOut.write( OP_WRITE_BLOCK );
            mirrorOut.writeLong( block.getBlockId() );
            mirrorOut.writeLong( block.getGenerationStamp() );
            mirrorOut.writeInt( pipelineSize );
            mirrorOut.writeBoolean( isRecovery );
            Text.writeString( mirrorOut, client );
            mirrorOut.writeBoolean(hasSrcDataNode);
            if (hasSrcDataNode) { // pass src node information
              srcDataNode.write(mirrorOut);
            }
            mirrorOut.writeInt( targets.length - 1 );
            for ( int i = 1; i < targets.length; i++ ) {
              targets[i].write( mirrorOut );
            }

            blockReceiver.writeChecksumHeader(mirrorOut);
            mirrorOut.flush();

            // read connect ack (only for clients, not for replication req)
            if (client.length() != 0) {
              firstBadLink = Text.readString(mirrorIn);
              if (LOG.isDebugEnabled() || firstBadLink.length() > 0) {
                LOG.info("Datanode " + targets.length +
                         " got response for connect ack " +
                         " from downstream datanode with firstbadlink as " +
                         firstBadLink);
              }
            }

          } catch (IOException e) {
            if (client.length() != 0) {
              Text.writeString(replyOut, mirrorNode);
              replyOut.flush();
            }
            IOUtils.closeStream(mirrorOut);
            mirrorOut = null;
            IOUtils.closeStream(mirrorIn);
            mirrorIn = null;
            IOUtils.closeSocket(mirrorSock);
            mirrorSock = null;
            if (client.length() > 0) {
              throw e;
            } else {
              LOG.info(dnRegistration + ":Exception transfering block " +
                       block + " to mirror " + mirrorNode +
                       ". continuing without the mirror.\n" +
                       StringUtils.stringifyException(e));
            }
          }
        }

        // send connect ack back to source (only for clients)
        if (client.length() != 0) {
          if (LOG.isDebugEnabled() || firstBadLink.length() > 0) {
            LOG.info("Datanode " + targets.length +
                     " forwarding connect ack to upstream firstbadlink is " +
                     firstBadLink);
          }
          Text.writeString(replyOut, firstBadLink);
          replyOut.flush();
        }

        // receive the block and mirror to the next target
        String mirrorAddr = (mirrorSock == null) ? null : mirrorNode;
        blockReceiver.receiveBlock(mirrorOut, mirrorIn, replyOut,
                                   mirrorAddr, null, targets.length);

        // if this write is for a replication request (and not
        // from a client), then confirm block. For client-writes,
        // the block is finalized in the PacketResponder.
        if (client.length() == 0) {
          notifyNamenodeReceivedBlock(block, EMPTY_DEL_HINT);
          LOG.info("Received block " + block + 
                   " src: " + remoteAddress +
                   " dest: " + localAddress +
                   " of size " + block.getNumBytes());
        }

        if (blockScanner != null) {
          blockScanner.addBlock(block);
        }
        
      } catch (IOException ioe) {
        LOG.info("writeBlock " + block + " received exception " + ioe);
        throw ioe;
      } finally {
        // close all opened streams
        IOUtils.closeStream(mirrorOut);
        IOUtils.closeStream(mirrorIn);
        IOUtils.closeStream(replyOut);
        IOUtils.closeSocket(mirrorSock);
        IOUtils.closeStream(blockReceiver);
      }
    }

    /**
     * Reads the metadata and sends the data in one 'DATA_CHUNK'
     * @param in
     */
    void readMetadata(DataInputStream in) throws IOException {
      Block block = new Block( in.readLong(), 0 , in.readLong());
      MetaDataInputStream checksumIn = null;
      DataOutputStream out = null;
      
      try {

        checksumIn = data.getMetaDataInputStream(block);
        
        long fileSize = checksumIn.getLength();

        if (fileSize >= 1L<<31 || fileSize <= 0) {
            throw new IOException("Unexpected size for checksumFile of block" +
                    block);
        }

        byte [] buf = new byte[(int)fileSize];
        IOUtils.readFully(checksumIn, buf, 0, buf.length);
        
        out = new DataOutputStream(
                  NetUtils.getOutputStream(s, socketWriteTimeout));
        
        out.writeByte(OP_STATUS_SUCCESS);
        out.writeInt(buf.length);
        out.write(buf);
        
        //last DATA_CHUNK
        out.writeInt(0);
      } finally {
        IOUtils.closeStream(out);
        IOUtils.closeStream(checksumIn);
      }
    }
    
    /**
     * Read a block from the disk and then sends it to a destination
     * 
     * @param in
     *          The stream to read from
     * @throws IOException
     */
    private void copyBlock(DataInputStream in) throws IOException {
      // Read in the header
      long blockId = in.readLong(); // read block id
      Block block = new Block(blockId, 0, in.readLong());
      String source = Text.readString(in); // read del hint
      DatanodeInfo target = new DatanodeInfo(); // read target
      target.readFields(in);

      if (!balancingThrottler.acquire()) { // not able to start
        LOG.info("Not able to copy block " + blockId + " to "
            + s.getRemoteSocketAddress() + " because threads quota is exceeded.");
        sendResponse(s, (short)OP_STATUS_ERROR, socketWriteTimeout);
        return;
      }

      Socket targetSock = null;
      short opStatus = OP_STATUS_SUCCESS;
      BlockSender blockSender = null;
      DataOutputStream targetOut = null;
      try {
        // check if the block exists or not
        blockSender = new BlockSender(block, 0, -1, false, false, false);

        // get the output stream to the target
        InetSocketAddress targetAddr = NetUtils.createSocketAddr(target.getName());
        targetSock = newSocket();
        targetSock.connect(targetAddr, socketTimeout);
        targetSock.setSoTimeout(socketTimeout);

        OutputStream baseStream = NetUtils.getOutputStream(targetSock, 
                                                            socketWriteTimeout);
        targetOut = new DataOutputStream(
                       new BufferedOutputStream(baseStream, SMALL_BUFFER_SIZE));

        /* send request to the target */
        // fist write header info
        targetOut.writeShort(DATA_TRANSFER_VERSION); // transfer version
        targetOut.writeByte(OP_REPLACE_BLOCK); // op code
        targetOut.writeLong(block.getBlockId()); // block id
        targetOut.writeLong(block.getGenerationStamp()); // block id
        Text.writeString( targetOut, source); // del hint

        // then send data
        long read = blockSender.sendBlock(targetOut, baseStream, 
                                          balancingThrottler);

        myMetrics.bytesRead.inc((int) read);
        myMetrics.blocksRead.inc();
        
        // check the response from target
        receiveResponse(targetSock, 1);

        LOG.info("Copied block " + block + " to " + targetAddr);
      } catch (IOException ioe) {
        opStatus = OP_STATUS_ERROR;
        LOG.warn("Got exception while serving " + block + " to "
            + target.getName() + ": " + StringUtils.stringifyException(ioe));
        throw ioe;
      } finally {
        // now release the thread resource
        balancingThrottler.release();

        /* send response to the requester */
        try {
          sendResponse(s, opStatus, socketWriteTimeout);
        } catch (IOException replyE) {
          LOG.warn("Error writing the response back to "+
              s.getRemoteSocketAddress() + "\n" +
              StringUtils.stringifyException(replyE) );
        }
        IOUtils.closeStream(targetOut);
        IOUtils.closeStream(blockSender);
      }
    }

    /**
     * Receive a block and write it to disk, it then notifies the namenode to
     * remove the copy from the source
     * 
     * @param in
     *          The stream to read from
     * @throws IOException
     */
    private void replaceBlock(DataInputStream in) throws IOException {
      /* read header */
      long blockId = in.readLong();
      Block block = new Block(blockId, estimateBlockSize, in.readLong()); // block id & len
      String sourceID = Text.readString(in);

      if (!balancingThrottler.acquire()) { // not able to start
        LOG.warn("Not able to receive block " + blockId + " from "
              + s.getRemoteSocketAddress() + " because threads quota is exceeded.");
        return;
      }

      short opStatus = OP_STATUS_SUCCESS;
      BlockReceiver blockReceiver = null;
      try {
        // open a block receiver and check if the block does not exist
         blockReceiver = new BlockReceiver(
            block, in, s.getRemoteSocketAddress().toString(), false, "", null);

        // receive a block
        blockReceiver.receiveBlock(null, null, null, null, balancingThrottler, -1);
                      
        // notify name node
        notifyNamenodeReceivedBlock(block, sourceID);

        LOG.info("Moved block " + block + 
            " from " + s.getRemoteSocketAddress());
      } catch (IOException ioe) {
        opStatus = OP_STATUS_ERROR;
        throw ioe;
      } finally {
        balancingThrottler.release();

        // send response back
        try {
          sendResponse(s, opStatus, socketWriteTimeout);
        } catch (IOException ioe) {
          LOG.warn("Error writing reply back to " + s.getRemoteSocketAddress());
        }
        IOUtils.closeStream(blockReceiver);
      }
    }
  }
    
  /** a class to throttle the block transfers
   * This class is thread safe. It can be shared by multiple threads.
   * The parameter bandwidthPerSec specifies the total bandwidth shared by threads.
   */
  static class Throttler {
    private long period;          // period over which bw is imposed
    private long periodExtension; // Max period over which bw accumulates.
    private long bytesPerPeriod; // total number of bytes can be sent in each period
    private long curPeriodStart; // current period starting time
    private long curReserve;     // remaining bytes can be sent in the period
    private long bytesAlreadyUsed;

    /** Constructor 
     * @param bandwidthPerSec bandwidth allowed in bytes per second. 
     */
    Throttler(long bandwidthPerSec) {
      this(500, bandwidthPerSec);  // by default throttling period is 500ms 
    }

    /**
     * Constructor
     * @param period in milliseconds. Bandwidth is enforced over this
     *        period.
     * @param bandwidthPerSec bandwidth allowed in bytes per second. 
     */
    Throttler(long period, long bandwidthPerSec) {
      this.curPeriodStart = System.currentTimeMillis();
      this.period = period;
      this.curReserve = this.bytesPerPeriod = bandwidthPerSec*period/1000;
      this.periodExtension = period*3;
    }

    /**
     * @return current throttle bandwidth in bytes per second.
     */
    public synchronized long getBandwidth() {
      return bytesPerPeriod*1000/period;
    }
    
    /**
     * Sets throttle bandwidth. This takes affect latest by the end of current
     * period.
     * 
     * @param bytesPerSecond 
     */
    public synchronized void setBandwidth(long bytesPerSecond) {
      if ( bytesPerSecond <= 0 ) {
        throw new IllegalArgumentException("" + bytesPerSecond);
      }
      bytesPerPeriod = bytesPerSecond*period/1000;
    }
    
    /** Given the numOfBytes sent/received since last time throttle was called,
     * make the current thread sleep if I/O rate is too fast
     * compared to the given bandwidth
     *
     * @param numOfBytes
     *     number of bytes sent/received since last time throttle was called
     */
    public synchronized void throttle(long numOfBytes) {
      if ( numOfBytes <= 0 ) {
        return;
      }

      curReserve -= numOfBytes;
      bytesAlreadyUsed += numOfBytes;

      while (curReserve <= 0) {
        long now = System.currentTimeMillis();
        long curPeriodEnd = curPeriodStart + period;

        if ( now < curPeriodEnd ) {
          // Wait for next period so that curReserve can be increased.
          try {
            wait( curPeriodEnd - now );
          } catch (InterruptedException ignored) {}
        } else if ( now <  (curPeriodStart + periodExtension)) {
          curPeriodStart = curPeriodEnd;
          curReserve += bytesPerPeriod;
        } else {
          // discard the prev period. Throttler might not have
          // been used for a long time.
          curPeriodStart = now;
          curReserve = bytesPerPeriod - bytesAlreadyUsed;
        }
      }

      bytesAlreadyUsed -= numOfBytes;
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
  static final int PKT_HEADER_LEN = ( 4 + /* Packet payload length */
                                      8 + /* offset in block */
                                      8 + /* seqno */
                                      1   /* isLastPacketInBlock */);
  
  class BlockSender implements java.io.Closeable {
    private Block block; // the block to read from
    private InputStream blockIn; // data stream
    private long blockInPosition = -1; // updated while using transferTo().
    private DataInputStream checksumIn; // checksum datastream
    private DataChecksum checksum; // checksum stream
    private long offset; // starting position to read
    private long endOffset; // ending position
    private long blockLength;
    private int bytesPerChecksum; // chunk size
    private int checksumSize; // checksum size
    private boolean corruptChecksumOk; // if need to verify checksum
    private boolean chunkOffsetOK; // if need to send chunk offset
    private long seqno; // sequence number of packet

    private boolean blockReadFully; //set when the whole block is read
    private boolean verifyChecksum; //if true, check is verified while reading
    private Throttler throttler;
    
    BlockSender(Block block, long startOffset, long length,
                boolean corruptChecksumOk, boolean chunkOffsetOK,
                boolean verifyChecksum) throws IOException {

      try {
        this.block = block;
        this.chunkOffsetOK = chunkOffsetOK;
        this.corruptChecksumOk = corruptChecksumOk;
        this.verifyChecksum = verifyChecksum;
        this.blockLength = data.getLength(block);

        if ( !corruptChecksumOk || data.metaFileExists(block) ) {
          checksumIn = new DataInputStream(
                  new BufferedInputStream(data.getMetaDataInputStream(block),
                                          BUFFER_SIZE));

          // read and handle the common header here. For now just a version
         BlockMetadataHeader header = BlockMetadataHeader.readHeader(checksumIn);
         short version = header.getVersion();

          if (version != FSDataset.METADATA_VERSION) {
            LOG.warn("Wrong version (" + version + ") for metadata file for "
                + block + " ignoring ...");
          }
          checksum = header.getChecksum();
        } else {
          LOG.warn("Could not find metadata file for " + block);
          // This only decides the buffer size. Use BUFFER_SIZE?
          checksum = DataChecksum.newDataChecksum(DataChecksum.CHECKSUM_NULL,
              16 * 1024);
        }

        /* If bytesPerChecksum is very large, then the metadata file
         * is mostly corrupted. For now just truncate bytesPerchecksum to
         * blockLength.
         */        
        bytesPerChecksum = checksum.getBytesPerChecksum();
        if (bytesPerChecksum > 10*1024*1024 && bytesPerChecksum > blockLength){
          checksum = DataChecksum.newDataChecksum(checksum.getChecksumType(),
                                     Math.max((int)blockLength, 10*1024*1024));
          bytesPerChecksum = checksum.getBytesPerChecksum();        
        }
        checksumSize = checksum.getChecksumSize();

        if (length < 0) {
          length = blockLength;
        }

        endOffset = blockLength;
        if (startOffset < 0 || startOffset > endOffset
            || (length + startOffset) > endOffset) {
          String msg = " Offset " + startOffset + " and length " + length
          + " don't match block " + block + " ( blockLen " + endOffset + " )";
          LOG.warn(dnRegistration + ":sendBlock() : " + msg);
          throw new IOException(msg);
        }

        
        offset = (startOffset - (startOffset % bytesPerChecksum));
        if (length >= 0) {
          // Make sure endOffset points to end of a checksumed chunk.
          long tmpLen = startOffset + length + (startOffset - offset);
          if (tmpLen % bytesPerChecksum != 0) {
            tmpLen += (bytesPerChecksum - tmpLen % bytesPerChecksum);
          }
          if (tmpLen < endOffset) {
            endOffset = tmpLen;
          }
        }

        // seek to the right offsets
        if (offset > 0) {
          long checksumSkip = (offset / bytesPerChecksum) * checksumSize;
          // note blockInStream is  seeked when created below
          if (checksumSkip > 0) {
            // Should we use seek() for checksum file as well?
            IOUtils.skipFully(checksumIn, checksumSkip);
          }
        }
        seqno = 0;

        blockIn = data.getBlockInputStream(block, offset); // seek to offset
      } catch (IOException ioe) {
        IOUtils.closeStream(this);
        IOUtils.closeStream(blockIn);
        throw ioe;
      }
    }

    // close opened files
    public void close() throws IOException {
      IOException ioe = null;
      // close checksum file
      if(checksumIn!=null) {
        try {
          checksumIn.close();
        } catch (IOException e) {
          ioe = e;
        }
        checksumIn = null;
      }
      // close data file
      if(blockIn!=null) {
        try {
          blockIn.close();
        } catch (IOException e) {
          ioe = e;
        }
        blockIn = null;
      }
      // throw IOException if there is any
      if(ioe!= null) {
        throw ioe;
      }
    }

    /**
     * Sends upto maxChunks chunks of data.
     * 
     * When blockInPosition is >= 0, assumes 'out' is a 
     * {@link SocketOutputStream} and tries 
     * {@link SocketOutputStream#transferToFully(FileChannel, long, int)} to
     * send data (and updates blockInPosition).
     */
    private int sendChunks(ByteBuffer pkt, int maxChunks, OutputStream out) 
                           throws IOException {
      // Sends multiple chunks in one packet with a single write().

      int len = Math.min((int) (endOffset - offset),
                         bytesPerChecksum*maxChunks);
      if (len == 0) {
        return 0;
      }

      int numChunks = (len + bytesPerChecksum - 1)/bytesPerChecksum;
      int packetLen = len + numChunks*checksumSize + 4;
      pkt.clear();
      
      // write packet header
      pkt.putInt(packetLen);
      pkt.putLong(offset);
      pkt.putLong(seqno);
      pkt.put((byte)((offset + len >= endOffset) ? 1 : 0));
                 //why no ByteBuf.putBoolean()?
      pkt.putInt(len);
      
      int checksumOff = pkt.position();
      int checksumLen = numChunks * checksumSize;
      byte[] buf = pkt.array();
      
      if (checksumSize > 0 && checksumIn != null) {
        try {
          checksumIn.readFully(buf, checksumOff, checksumLen);
        } catch (IOException e) {
          LOG.warn(" Could not read or failed to veirfy checksum for data" +
                   " at offset " + offset + " for block " + block + " got : "
                   + StringUtils.stringifyException(e));
          IOUtils.closeStream(checksumIn);
          checksumIn = null;
          if (corruptChecksumOk) {
            if (checksumOff < checksumLen) {
              // Just fill the array with zeros.
              Arrays.fill(buf, checksumOff, checksumLen, (byte) 0);
            }
          } else {
            throw e;
          }
        }
      }
      
      int dataOff = checksumOff + checksumLen;
      
      if (blockInPosition < 0) {
        //normal transfer
        IOUtils.readFully(blockIn, buf, dataOff, len);

        if (verifyChecksum) {
          int dOff = dataOff;
          int cOff = checksumOff;
          int dLeft = len;

          for (int i=0; i<numChunks; i++) {
            checksum.reset();
            int dLen = Math.min(dLeft, bytesPerChecksum);
            checksum.update(buf, dOff, dLen);
            if (!checksum.compare(buf, cOff)) {
              throw new ChecksumException("Checksum failed at " + 
                                          (offset + len - dLeft), len);
            }
            dLeft -= dLen;
            dOff += dLen;
            cOff += checksumSize;
          }
        }
        //writing is done below (mainly to handle IOException)
      }
      
      try {
        if (blockInPosition >= 0) {
          //use transferTo(). Checks on out and blockIn are already done. 

          SocketOutputStream sockOut = (SocketOutputStream)out;
          //first write the packet
          sockOut.write(buf, 0, dataOff);
          // no need to flush. since we know out is not a buffered stream. 

          sockOut.transferToFully(((FileInputStream)blockIn).getChannel(), 
                                  blockInPosition, len);

          blockInPosition += len;
        } else {
          // normal transfer
          out.write(buf, 0, dataOff + len);
        }
        
      } catch (IOException e) {
        /* exception while writing to the client (well, with transferTo(),
         * it could also be while reading from the local file). Many times 
         * this error can be ignored. We will let the callers distinguish this 
         * from other exceptions if this is not a subclass of IOException. 
         */
        if (e.getClass().equals(IOException.class)) {
          // "se" could be a new class in stead of SocketException.
          IOException se = new SocketException("Original Exception : " + e);
          se.initCause(e);
          /* Cange the stacktrace so that original trace is not truncated
           * when printed.*/ 
          se.setStackTrace(e.getStackTrace());
          throw se;
        }
        throw e;
      }

      if (throttler != null) { // rebalancing so throttle
        throttler.throttle(packetLen);
      }

      return len;
    }

    /**
     * sendBlock() is used to read block and its metadata and stream the data to
     * either a client or to another datanode. 
     * 
     * @param out  stream to which the block is written to
     * @param baseStream optional. if non-null, <code>out</code> is assumed to 
     *        be a wrapper over this stream. This enables optimizations for
     *        sending the data, e.g. 
     *        {@link SocketOutputStream#transferToFully(FileChannel, 
     *        long, int)}.
     * @param throttler for sending data.
     * @return total bytes reads, including crc.
     */
    long sendBlock(DataOutputStream out, OutputStream baseStream, 
                   Throttler throttler) throws IOException {
      if( out == null ) {
        throw new IOException( "out stream is null" );
      }
      this.throttler = throttler;

      long initialOffset = offset;
      long totalRead = 0;
      OutputStream streamForSendChunks = out;
      
      try {
        checksum.writeHeader(out);
        if ( chunkOffsetOK ) {
          out.writeLong( offset );
        }
        out.flush();
        
        int maxChunksPerPacket;
        int pktSize = PKT_HEADER_LEN + SIZE_OF_INTEGER;
        
        if (transferToAllowed && !verifyChecksum && 
            baseStream instanceof SocketOutputStream && 
            blockIn instanceof FileInputStream) {
          
          FileChannel fileChannel = ((FileInputStream)blockIn).getChannel();
          
          // blockInPosition also indicates sendChunks() uses transferTo.
          blockInPosition = fileChannel.position();
          streamForSendChunks = baseStream;
          
          // assure a mininum buffer size.
          maxChunksPerPacket = (Math.max(BUFFER_SIZE, 
                                         MIN_BUFFER_WITH_TRANSFERTO)
                                + bytesPerChecksum - 1)/bytesPerChecksum;
          
          // allocate smaller buffer while using transferTo(). 
          pktSize += checksumSize * maxChunksPerPacket;
        } else {
          maxChunksPerPacket = Math.max(1,
                   (BUFFER_SIZE + bytesPerChecksum - 1)/bytesPerChecksum);
          pktSize += (bytesPerChecksum + checksumSize) * maxChunksPerPacket;
        }

        ByteBuffer pktBuf = ByteBuffer.allocate(pktSize);

        while (endOffset > offset) {
          long len = sendChunks(pktBuf, maxChunksPerPacket, 
                                streamForSendChunks);
          offset += len;
          totalRead += len + ((len + bytesPerChecksum - 1)/bytesPerChecksum*
                              checksumSize);
          seqno++;
        }
        out.writeInt(0); // mark the end of block        
        out.flush();
      } finally {
        close();
      }

      blockReadFully = (initialOffset == 0 && offset >= blockLength);

      return totalRead;
    }
    
    boolean isBlockReadFully() {
      return blockReadFully;
    }
  }

  // This information is cached by the Datanode in the ackQueue
  static private class Packet {
    long seqno;
    boolean lastPacketInBlock;

    Packet(long seqno, boolean lastPacketInBlock) {
      this.seqno = seqno;
      this.lastPacketInBlock = lastPacketInBlock;
    }
  }

  /**
   * Processed responses from downstream datanodes in the pipeline
   * and sends back replies to the originator.
   */
  class PacketResponder implements Runnable {
    private LinkedList<Packet> ackQueue = new LinkedList<Packet>(); // packet waiting for ack
    private volatile boolean running = true;
    private Block block;
    DataInputStream mirrorIn;   // input from downstream datanode
    DataOutputStream replyOut;  // output to upstream datanode
    private int numTargets;     // number of downstream datanodes including myself
    private String clientName;  // The name of the client (if any)
    private BlockReceiver receiver; // The owner of this responder.

    public String toString() {
      return "PacketResponder " + numTargets + " for Block " + this.block;
    }

    PacketResponder(BlockReceiver receiver, Block b, DataInputStream in, 
                    DataOutputStream out, int numTargets, String clientName) {
      this.receiver = receiver;
      this.block = b;
      mirrorIn = in;
      replyOut = out;
      this.numTargets = numTargets;
      this.clientName = clientName;
    }

    // enqueue the seqno that is still be to acked by the downstream datanode
    synchronized void enqueue(long seqno, boolean lastPacketInBlock) {
      if (running) {
        LOG.debug("PacketResponder " + numTargets + " adding seqno " + seqno +
                  " to ack queue.");
        ackQueue.addLast(new Packet(seqno, lastPacketInBlock));
        notifyAll();
      }
    }

    // wait for all pending packets to be acked. Then shutdown thread.
    synchronized void close() {
      while (running && ackQueue.size() != 0 && shouldRun) {
        try {
          wait();
        } catch (InterruptedException e) {
          running = false;
        }
      }
      LOG.debug("PacketResponder " + numTargets +
               " for block " + block + " Closing down.");
      running = false;
      notifyAll();
    }

    private synchronized void lastDataNodeRun() {
      long lastHeartbeat = System.currentTimeMillis();
      boolean lastPacket = false;

      while (running && shouldRun && !lastPacket) {
        long now = System.currentTimeMillis();
        try {

            // wait for a packet to be sent to downstream datanode
            while (running && shouldRun && ackQueue.size() == 0) {
              long idle = now - lastHeartbeat;
              long timeout = (socketTimeout/2) - idle;
              if (timeout <= 0) {
                timeout = 1000;
              }
              try {
                wait(timeout);
              } catch (InterruptedException e) {
                if (running) {
                  LOG.info("PacketResponder " + numTargets +
                           " for block " + block + " Interrupted.");
                  running = false;
                }
                break;
              }
          
              // send a heartbeat if it is time.
              now = System.currentTimeMillis();
              if (now - lastHeartbeat > socketTimeout/2) {
                replyOut.writeLong(-1); // send heartbeat
                replyOut.flush();
                lastHeartbeat = now;
              }
            }

            if (!running || !shouldRun) {
              break;
            }
            Packet pkt = ackQueue.removeFirst();
            long expected = pkt.seqno;
            notifyAll();
            LOG.debug("PacketResponder " + numTargets +
                      " for block " + block + 
                      " acking for packet " + expected);

            // If this is the last packet in block, then close block
            // file and finalize the block before responding success
            if (pkt.lastPacketInBlock) {
              if (!receiver.finalized) {
                receiver.close();
                block.setNumBytes(receiver.offsetInBlock);
                data.finalizeBlock(block);
                myMetrics.blocksWritten.inc();
                notifyNamenodeReceivedBlock(block, EMPTY_DEL_HINT);
                LOG.info("Received block " + block + 
                         " of size " + block.getNumBytes() + 
                         " from " + receiver.inAddr);
              }
              lastPacket = true;
            }

            replyOut.writeLong(expected);
            replyOut.writeShort(OP_STATUS_SUCCESS);
            replyOut.flush();
        } catch (Exception e) {
          if (running) {
            LOG.info("PacketResponder " + block + " " + numTargets + 
                     " Exception " + StringUtils.stringifyException(e));
            running = false;
          }
        }
      }
      LOG.info("PacketResponder " + numTargets + 
               " for block " + block + " terminating");
    }

    // Thread to process incoming acks
    public void run() {

      // If this is the last datanode in pipeline, then handle differently
      if (numTargets == 0) {
        lastDataNodeRun();
        return;
      }

      boolean lastPacketInBlock = false;
      while (running && shouldRun && !lastPacketInBlock) {

        try {
            short op = OP_STATUS_SUCCESS;
            boolean didRead = false;
            long expected = -2;
            try { 
              // read seqno from downstream datanode
              long seqno = mirrorIn.readLong();
              didRead = true;
              if (seqno == -1) {
                replyOut.writeLong(-1); // send keepalive
                replyOut.flush();
                LOG.debug("PacketResponder " + numTargets + " got -1");
                continue;
              } else if (seqno == -2) {
                LOG.debug("PacketResponder " + numTargets + " got -2");
              } else {
                LOG.debug("PacketResponder " + numTargets + " got seqno = " + seqno);
                Packet pkt = null;
                synchronized (this) {
                  while (running && shouldRun && ackQueue.size() == 0) {
                    if (LOG.isDebugEnabled()) {
                      LOG.debug("PacketResponder " + numTargets + 
                                " seqno = " + seqno +
                                " for block " + block +
                                " waiting for local datanode to finish write.");
                    }
                    wait();
                  }
                  pkt = ackQueue.removeFirst();
                  expected = pkt.seqno;
                  notifyAll();
                  LOG.debug("PacketResponder " + numTargets + " seqno = " + seqno);
                  if (seqno != expected) {
                    throw new IOException("PacketResponder " + numTargets +
                                          " for block " + block +
                                          " expected seqno:" + expected +
                                          " received:" + seqno);
                  }
                  lastPacketInBlock = pkt.lastPacketInBlock;
                }
              }
            } catch (Throwable e) {
              if (running) {
                LOG.info("PacketResponder " + block + " " + numTargets + 
                         " Exception " + StringUtils.stringifyException(e));
                running = false;
              }
            }

            if (Thread.interrupted()) {
              /* The receiver thread cancelled this thread. 
               * We could also check any other status updates from the 
               * receiver thread (e.g. if it is ok to write to replyOut). 
               */
              LOG.info("PacketResponder " + block +  " " + numTargets +
                       " : Thread is interrupted.");
              running = false;
            }
            
            if (!didRead) {
              op = OP_STATUS_ERROR;
            }
            
            // If this is the last packet in block, then close block
            // file and finalize the block before responding success
            if (lastPacketInBlock && !receiver.finalized) {
              receiver.close();
              block.setNumBytes(receiver.offsetInBlock);
              data.finalizeBlock(block);
              myMetrics.blocksWritten.inc();
              notifyNamenodeReceivedBlock(block, EMPTY_DEL_HINT);
              LOG.info("Received block " + block + 
                       " of size " + block.getNumBytes() + 
                       " from " + receiver.inAddr);
            }

            // send my status back to upstream datanode
            replyOut.writeLong(expected); // send seqno upstream
            replyOut.writeShort(OP_STATUS_SUCCESS);

            LOG.debug("PacketResponder " + numTargets + 
                      " for block " + block +
                      " responded my status " +
                      " for seqno " + expected);

            // forward responses from downstream datanodes.
            for (int i = 0; i < numTargets && shouldRun; i++) {
              try {
                if (op == OP_STATUS_SUCCESS) {
                  op = mirrorIn.readShort();
                  if (op != OP_STATUS_SUCCESS) {
                    LOG.debug("PacketResponder for block " + block +
                              ": error code received from downstream " +
                              " datanode[" + i + "] " + op);
                  }
                }
              } catch (Throwable e) {
                op = OP_STATUS_ERROR;
              }
              replyOut.writeShort(op);
            }
            replyOut.flush();
            LOG.debug("PacketResponder " + block + " " + numTargets + 
                      " responded other status " + " for seqno " + expected);

            // If we were unable to read the seqno from downstream, then stop.
            if (expected == -2) {
              running = false;
            }
            // If we forwarded an error response from a downstream datanode
            // and we are acting on behalf of a client, then we quit. The 
            // client will drive the recovery mechanism.
            if (op == OP_STATUS_ERROR && clientName.length() > 0) {
              running = false;
            }
        } catch (IOException e) {
          if (running) {
            LOG.info("PacketResponder " + block + " " + numTargets + 
                     " Exception " + StringUtils.stringifyException(e));
            running = false;
          }
        } catch (RuntimeException e) {
          if (running) {
            LOG.info("PacketResponder " + block + " " + numTargets + 
                     " Exception " + StringUtils.stringifyException(e));
            running = false;
          }
        }
      }
      LOG.info("PacketResponder " + numTargets + 
               " for block " + block + " terminating");
    }
  }

  /* A class that receives a block and wites to its own disk, meanwhile
   * may copies it to another site. If a throttler is provided,
   * streaming throttling is also supported. 
   * */
  private class BlockReceiver implements java.io.Closeable {
    private Block block; // the block to receive
    private boolean finalized;
    private DataInputStream in = null; // from where data are read
    private DataChecksum checksum; // from where chunks of a block can be read
    private OutputStream out = null; // to block file at local disk
    private DataOutputStream checksumOut = null; // to crc file at local disk
    private int bytesPerChecksum;
    private int checksumSize;
    private ByteBuffer buf; // contains one full packet.
    private int bufRead; //amount of valid data in the buf
    private int maxPacketReadLen;
    private long offsetInBlock;
    final private String inAddr;
    private String mirrorAddr;
    private DataOutputStream mirrorOut;
    private Daemon responder = null;
    private Throttler throttler;
    private FSDataset.BlockWriteStreams streams;
    private boolean isRecovery = false;
    private String clientName;
    DatanodeInfo srcDataNode = null;

    BlockReceiver(Block block, DataInputStream in, String inAddr,
                  boolean isRecovery, String clientName, 
                  DatanodeInfo srcDataNode) throws IOException {
      try{
        this.block = block;
        this.in = in;
        this.inAddr = inAddr;
        this.isRecovery = isRecovery;
        this.clientName = clientName;
        this.offsetInBlock = 0;
        this.srcDataNode = srcDataNode;
        this.checksum = DataChecksum.newDataChecksum(in);
        this.bytesPerChecksum = checksum.getBytesPerChecksum();
        this.checksumSize = checksum.getChecksumSize();
        //
        // Open local disk out
        //
        streams = data.writeToBlock(block, isRecovery);
        this.finalized = data.isValidBlock(block);
        if (streams != null) {
          this.out = streams.dataOut;
          this.checksumOut = new DataOutputStream(new BufferedOutputStream(
                                                    streams.checksumOut, 
                                                    SMALL_BUFFER_SIZE));
        }
      } catch(IOException ioe) {
        IOUtils.closeStream(this);
        removeBlock();

        // check if there is a disk error
        IOException cause = FSDataset.getCauseIfDiskError(ioe);
        if (cause != null) { // possible disk error
          ioe = cause;
          checkDiskError(ioe);
        }
        throw ioe;
      }
    }

    // close files
    public void close() throws IOException {

      IOException ioe = null;
      // close checksum file
      try {
        if (checksumOut != null) {
          checksumOut.close();
          checksumOut = null;
        }
      } catch(IOException e) {
        ioe = e;
      }
      // close block file
      try {
        if (out != null) {
          out.close();
          out = null;
        }
      } catch (IOException e) {
        ioe = e;
      }
      // disk check
      if(ioe != null) {
        checkDiskError(ioe);
        throw ioe;
      }
    }

    // flush block data and metadata files to disk.
    void flush() throws IOException {
      if (checksumOut != null) {
        checksumOut.flush();
      }
      if (out != null) {
        out.flush();
      }
    }

    /**
     * While writing to mirrorOut, failure to write to mirror should not
     * affect this datanode unless a client is writing the block.
     */
    private void handleMirrorOutError(IOException ioe) throws IOException {
      LOG.info(dnRegistration + ":Exception writing block " +
               block + " to mirror " + mirrorAddr + "\n" +
               StringUtils.stringifyException(ioe));
      mirrorOut = null;
      //
      // If stream-copy fails, continue
      // writing to disk for replication requests. For client
      // writes, return error so that the client can do error
      // recovery.
      //
      if (clientName.length() > 0) {
        throw ioe;
      }
    }
    
    /**
     * Verify multiple CRC chunks. 
     */
    private void verifyChunks( byte[] dataBuf, int dataOff, int len, 
                               byte[] checksumBuf, int checksumOff ) 
                               throws IOException {
      while (len > 0) {
        int chunkLen = Math.min(len, bytesPerChecksum);
        
        checksum.update(dataBuf, dataOff, chunkLen);

        if (!checksum.compare(checksumBuf, checksumOff)) {
          if (srcDataNode != null) {
            try {
              LOG.info("report corrupt block " + block + " from datanode " +
                        srcDataNode + " to namenode");
              LocatedBlock lb = new LocatedBlock(block, 
                                              new DatanodeInfo[] {srcDataNode});
              namenode.reportBadBlocks(new LocatedBlock[] {lb});
            } catch (IOException e) {
              LOG.warn("Failed to report bad block " + block + 
                        " from datanode " + srcDataNode + " to namenode");
            }
          }
          throw new IOException("Unexpected checksum mismatch " + 
                                "while writing " + block + " from " + inAddr);
        }

        checksum.reset();
        dataOff += chunkLen;
        checksumOff += checksumSize;
        len -= chunkLen;
      }
    }

    /**
     * Makes sure buf.position() is zero without modifying buf.remaining().
     * It moves the data if position needs to be changed.
     */
    private void shiftBufData() {
      if (bufRead != buf.limit()) {
        throw new IllegalStateException("bufRead should be same as " +
                                        "buf.limit()");
      }
      
      //shift the remaining data on buf to the front
      if (buf.position() > 0) {
        int dataLeft = buf.remaining();
        if (dataLeft > 0) {
          byte[] b = buf.array();
          System.arraycopy(b, buf.position(), b, 0, dataLeft);
        }
        buf.position(0);
        bufRead = dataLeft;
        buf.limit(bufRead);
      }
    }
    
    /**
     * reads upto toRead byte to buf at buf.limit() and increments the limit.
     * throws an IOException if read does not succeed.
     */
    private int readToBuf(int toRead) throws IOException {
      if (toRead < 0) {
        toRead = (maxPacketReadLen > 0 ? maxPacketReadLen : buf.capacity())
                 - buf.limit();
      }
      
      int nRead = in.read(buf.array(), buf.limit(), toRead);
      
      if (nRead < 0) {
        throw new EOFException("while trying to read " + toRead + " bytes");
      }
      bufRead = buf.limit() + nRead;
      buf.limit(bufRead);
      return nRead;
    }
    
    
    /**
     * Reads (at least) one packet and returns the packet length.
     * buf.position() points to the start of the packet and 
     * buf.limit() point to the end of the packet. There could 
     * be more data from next packet in buf.<br><br>
     * 
     * It tries to read a full packet with single read call.
     * Consecutinve packets are usually of the same length.
     */
    private int readNextPacket() throws IOException {
      /* This dances around buf a little bit, mainly to read 
       * full packet with single read and to accept arbitarary size  
       * for next packet at the same time.
       */
      if (buf == null) {
        /* initialize buffer to the best guess size:
         * 'chunksPerPacket' calculation here should match the same 
         * calculation in DFSClient to make the guess accurate.
         */
        int chunkSize = bytesPerChecksum + checksumSize;
        int chunksPerPacket = (writePacketSize - PKT_HEADER_LEN - 
                               SIZE_OF_INTEGER + chunkSize - 1)/chunkSize;
        buf = ByteBuffer.allocate(PKT_HEADER_LEN + SIZE_OF_INTEGER +
                                  Math.max(chunksPerPacket, 1) * chunkSize);
        buf.limit(0);
      }
      
      // See if there is data left in the buffer :
      if (bufRead > buf.limit()) {
        buf.limit(bufRead);
      }
      
      while (buf.remaining() < SIZE_OF_INTEGER) {
        if (buf.position() > 0) {
          shiftBufData();
        }
        readToBuf(-1);
      }
      
      /* We mostly have the full packet or at least enough for an int
       */
      buf.mark();
      int payloadLen = buf.getInt();
      buf.reset();
      
      if (payloadLen == 0) {
        //end of stream!
        buf.limit(buf.position() + SIZE_OF_INTEGER);
        return 0;
      }
      
      // check corrupt values for pktLen, 100MB upper limit should be ok?
      if (payloadLen < 0 || payloadLen > (100*1024*1024)) {
        throw new IOException("Incorrect value for packet payload : " +
                              payloadLen);
      }
      
      int pktSize = payloadLen + PKT_HEADER_LEN;
      
      if (buf.remaining() < pktSize) {
        //we need to read more data
        int toRead = pktSize - buf.remaining();
        
        // first make sure buf has enough space.        
        int spaceLeft = buf.capacity() - buf.limit();
        if (toRead > spaceLeft && buf.position() > 0) {
          shiftBufData();
          spaceLeft = buf.capacity() - buf.limit();
        }
        if (toRead > spaceLeft) {
          byte oldBuf[] = buf.array();
          int toCopy = buf.limit();
          buf = ByteBuffer.allocate(toCopy + toRead);
          System.arraycopy(oldBuf, 0, buf.array(), 0, toCopy);
          buf.limit(toCopy);
        }
        
        //now read:
        while (toRead > 0) {
          toRead -= readToBuf(toRead);
        }
      }
      
      if (buf.remaining() > pktSize) {
        buf.limit(buf.position() + pktSize);
      }
      
      if (pktSize > maxPacketReadLen) {
        maxPacketReadLen = pktSize;
      }
      
      return payloadLen;
    }
    
    /** 
     * Receives and processes a packet. It can contain many chunks.
     * returns size of the packet.
     */
    private int receivePacket() throws IOException {
      
      int payloadLen = readNextPacket();
      
      if (payloadLen <= 0) {
        return payloadLen;
      }
      
      buf.mark();
      //read the header
      buf.getInt(); // packet length
      offsetInBlock = buf.getLong(); // get offset of packet in block
      long seqno = buf.getLong();    // get seqno
      boolean lastPacketInBlock = (buf.get() != 0);
      
      int endOfHeader = buf.position();
      buf.reset();
      
      if (LOG.isDebugEnabled()){
        LOG.debug("Receiving one packet for block " + block +
                  " of length " + payloadLen +
                  " seqno " + seqno +
                  " offsetInBlock " + offsetInBlock +
                  " lastPacketInBlock " + lastPacketInBlock);
      }
      
      setBlockPosition(offsetInBlock);
      
      //First write the packet to the mirror:
      if (mirrorOut != null) {
        try {
          mirrorOut.write(buf.array(), buf.position(), buf.remaining());
          mirrorOut.flush();
        } catch (IOException e) {
          handleMirrorOutError(e);
        }
      }

      buf.position(endOfHeader);        
      int len = buf.getInt();
      
      if (len < 0) {
        throw new IOException("Got wrong length during writeBlock(" + block + 
                              ") from " + inAddr + " at offset " + 
                              offsetInBlock + ": " + len); 
      } 

      if (len == 0) {
        LOG.debug("Receiving empty packet for block " + block);
      } else {
        offsetInBlock += len;

        int checksumLen = ((len + bytesPerChecksum - 1)/bytesPerChecksum)*
                                                              checksumSize;

        if ( buf.remaining() != (checksumLen + len)) {
          throw new IOException("Data remaining in packet does not match " +
                                "sum of checksumLen and dataLen");
        }
        int checksumOff = buf.position();
        int dataOff = checksumOff + checksumLen;
        byte pktBuf[] = buf.array();

        buf.position(buf.limit()); // move to the end of the data.

        verifyChunks(pktBuf, dataOff, len, pktBuf, checksumOff);

        try {
          if (!finalized) {
            //finally write to the disk :
            out.write(pktBuf, dataOff, len);
            checksumOut.write(pktBuf, checksumOff, checksumLen);
            myMetrics.bytesWritten.inc(len);
          }
        } catch (IOException iex) {
          checkDiskError(iex);
          throw iex;
        }
      }

      /// flush entire packet before sending ack
      flush();

      // put in queue for pending acks
      if (responder != null) {
        ((PacketResponder)responder.getRunnable()).enqueue(seqno,
                                        lastPacketInBlock); 
      }
      
      if (throttler != null) { // throttle I/O
        throttler.throttle(payloadLen);
      }
      
      return payloadLen;
    }

    public void writeChecksumHeader(DataOutputStream mirrorOut) throws IOException {
      checksum.writeHeader(mirrorOut);
    }
   

    public void receiveBlock(
        DataOutputStream mirrOut, // output to next datanode
        DataInputStream mirrIn,   // input from next datanode
        DataOutputStream replyOut,  // output to previous datanode
        String mirrAddr, Throttler throttlerArg,
        int numTargets) throws IOException {

        mirrorOut = mirrOut;
        mirrorAddr = mirrAddr;
        throttler = throttlerArg;

      try {
        // write data chunk header
        if (!finalized) {
          BlockMetadataHeader.writeHeader(checksumOut, checksum);
        }
        if (clientName.length() > 0) {
          responder = new Daemon(threadGroup, 
                                 new PacketResponder(this, block, mirrIn, 
                                                     replyOut, numTargets,
                                                     clientName));
          responder.start(); // start thread to processes reponses
        }

        /* 
         * Receive until packet length is zero.
         */
        while (receivePacket() > 0) {}

        // flush the mirror out
        if (mirrorOut != null) {
          try {
            mirrorOut.writeInt(0); // mark the end of the block
            mirrorOut.flush();
          } catch (IOException e) {
            handleMirrorOutError(e);
          }
        }

        // wait for all outstanding packet responses. And then
        // indicate responder to gracefully shutdown.
        if (responder != null) {
          ((PacketResponder)responder.getRunnable()).close();
        }

        // if this write is for a replication request (and not
        // from a client), then finalize block. For client-writes, 
        // the block is finalized in the PacketResponder.
        if (clientName.length() == 0) {
          // close the block/crc files
          close();

          // Finalize the block. Does this fsync()?
          block.setNumBytes(offsetInBlock);
          data.finalizeBlock(block);
          myMetrics.blocksWritten.inc();
        }

      } catch (IOException ioe) {
        LOG.info("Exception in receiveBlock for block " + block + 
                 " " + ioe);
        IOUtils.closeStream(this);
        if (responder != null) {
          responder.interrupt();
        }
        removeBlock();
        throw ioe;
      } finally {
        if (responder != null) {
          try {
            responder.join();
          } catch (InterruptedException e) {
            throw new IOException("Interrupted receiveBlock");
          }
          responder = null;
        }
      }
    }

    /** Remove a partial block
     * if this write is for a replication request (and not from a client)
     */
    private void removeBlock() throws IOException {
      if (clientName.length() == 0) { // not client write
        data.unfinalizeBlock(block);
      }
    }

    /**
     * Sets the file pointer in the local block file to the specified value.
     */
    private void setBlockPosition(long offsetInBlock) throws IOException {
      if (finalized) {
        if (!isRecovery) {
          throw new IOException("Write to offset " + offsetInBlock +
                                " of block " + block +
                                " that is already finalized.");
        }
        if (offsetInBlock > data.getLength(block)) {
          throw new IOException("Write to offset " + offsetInBlock +
                                " of block " + block +
                                " that is already finalized and is of size " +
                                data.getLength(block));
        }
        return;
      }

      if (data.getChannelPosition(block, streams) == offsetInBlock) {
        return;                   // nothing to do 
      }
      if (offsetInBlock % bytesPerChecksum != 0) {
        throw new IOException("setBlockPosition trying to set position to " +
                              offsetInBlock +
                              " which is not a multiple of bytesPerChecksum " +
                               bytesPerChecksum);
      }
      long offsetInChecksum = BlockMetadataHeader.getHeaderSize() +
                              offsetInBlock / bytesPerChecksum * checksumSize;
      if (out != null) {
       out.flush();
      }
      if (checksumOut != null) {
        checksumOut.flush();
      }
      LOG.info("Changing block file offset of block " + block + " from " + 
               data.getChannelPosition(block, streams) +
               " to " + offsetInBlock +
               " meta file offset to " + offsetInChecksum);

      // set the position of the block file
      data.setChannelPosition(block, streams, offsetInBlock, offsetInChecksum);
    }
  }

  /**
   * Used for transferring a block of data.  This class
   * sends a piece of data to another DataNode.
   */
  class DataTransfer implements Runnable {
    DatanodeInfo targets[];
    Block b;

    /**
     * Connect to the first item in the target list.  Pass along the 
     * entire target list, the block, and the data.
     */
    public DataTransfer(DatanodeInfo targets[], Block b) throws IOException {
      this.targets = targets;
      this.b = b;
    }

    /**
     * Do the deed, write the bytes
     */
    public void run() {
      xmitsInProgress++;
      Socket sock = null;
      DataOutputStream out = null;
      BlockSender blockSender = null;
      
      try {
        InetSocketAddress curTarget = 
          NetUtils.createSocketAddr(targets[0].getName());
        sock = newSocket();
        sock.connect(curTarget, socketTimeout);
        sock.setSoTimeout(targets.length * socketTimeout);

        long writeTimeout = socketWriteTimeout + 
                            WRITE_TIMEOUT_EXTENSION * (targets.length-1);
        OutputStream baseStream = NetUtils.getOutputStream(sock, writeTimeout);
        out = new DataOutputStream(new BufferedOutputStream(baseStream, 
                                                            SMALL_BUFFER_SIZE));

        blockSender = new BlockSender(b, 0, -1, false, false, false);
        DatanodeInfo srcNode = new DatanodeInfo(dnRegistration);

        //
        // Header info
        //
        out.writeShort(DATA_TRANSFER_VERSION);
        out.writeByte(OP_WRITE_BLOCK);
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
        // send data & checksum
        blockSender.sendBlock(out, baseStream, null);

        // no response necessary
        LOG.info(dnRegistration + ":Transmitted block " + b + " to " + curTarget);

      } catch (IOException ie) {
        LOG.warn(dnRegistration + ":Failed to transfer " + b + " to " + targets[0].getName()
            + " got " + StringUtils.stringifyException(ie));
      } finally {
        IOUtils.closeStream(blockSender);
        IOUtils.closeStream(out);
        IOUtils.closeSocket(sock);
        xmitsInProgress--;
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
    dataXceiveServer.start();
        
    while (shouldRun) {
      try {
        startDistributedUpgradeIfNeeded();
        offerService();
      } catch (Exception ex) {
        LOG.error("Exception: " + StringUtils.stringifyException(ex));
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
  static void runDatanodeDaemon(DataNode dn) throws IOException {
    if (dn != null) {
      //register datanode
      dn.register();
      dn.dataNodeThread = new Thread(dn, dnThreadName);
      dn.dataNodeThread.setDaemon(true); // needed for JUnit testing
      dn.dataNodeThread.start();
    }
  }
  
  /** check if a datanode is up */
  static boolean isDatanodeUp(DataNode dn) {
    return dn.dataNodeThread != null && dn.dataNodeThread.isAlive();
  }

  /** Instantiate a single datanode object. This must be run by invoking
   *  {@link DataNode#runDatanodeDaemon(DataNode)} subsequently. 
   */
  static DataNode instantiateDataNode(String args[],
                                      Configuration conf) throws IOException {
    if (conf == null)
      conf = new Configuration();
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
  static DataNode createDataNode(String args[],
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
   * @throws IOException
   */
  static DataNode makeInstance(String[] dataDirs, Configuration conf)
    throws IOException {
    ArrayList<File> dirs = new ArrayList<File>();
    for (int i = 0; i < dataDirs.length; i++) {
      File data = new File(dataDirs[i]);
      try {
        DiskChecker.checkDir(data);
        dirs.add(data);
      } catch(DiskErrorException e) {
        LOG.warn("Invalid directory in dfs.data.dir: " + e.getMessage());
      }
    }
    if (dirs.size() > 0) 
      return new DataNode(conf, dirs);
    LOG.error("All directories in dfs.data.dir are invalid.");
    return null;
  }

  @Override
  public String toString() {
    return "DataNode{" +
      "data=" + data +
      ", localName='" + dnRegistration.getName() + "'" +
      ", storageID='" + dnRegistration.getStorageID() + "'" +
      ", xmitsInProgress=" + xmitsInProgress +
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
    Block stored = data.getStoredBlock(block.blkid);

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

  Daemon recoverBlocks(final Block[] blocks, final DatanodeInfo[][] targets) {
    Daemon d = new Daemon(threadGroup, new Runnable() {
      public void run() {
        LeaseManager.recoverBlocks(blocks, targets, DataNode.this, namenode, getConf());
      }
    });
    d.start();
    return d;
  }

  /** {@inheritDoc} */
  public void updateBlock(Block oldblock, Block newblock, boolean finalize) throws IOException {
    LOG.info("oldblock=" + oldblock + ", newblock=" + newblock
        + ", datanode=" + dnRegistration.getName());
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

  // ClientDataNodeProtocol implementation
  /** {@inheritDoc} */
  public Block recoverBlock(Block block, DatanodeInfo[] targets
      ) throws IOException {
    logRecoverBlock("Client", block, targets);
    return LeaseManager.recoverBlock(block, targets, this, namenode, 
                                     getConf(), false);
  }

  static void logRecoverBlock(String who, Block block, DatanodeID[] targets) {
    StringBuilder msg = new StringBuilder(targets[0].getName());
    for (int i = 1; i < targets.length; i++) {
      msg.append(", " + targets[i].getName());
    }
    LOG.info(who + " calls recoverBlock(block=" + block
        + ", targets=[" + msg + "])");
  }
}
