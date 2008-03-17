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
import org.apache.hadoop.net.SocketInputStream;
import org.apache.hadoop.net.SocketOutputStream;
import org.apache.hadoop.util.*;
import org.apache.hadoop.util.DiskChecker.DiskErrorException;
import org.apache.hadoop.util.DiskChecker.DiskOutOfSpaceException;
import org.apache.hadoop.mapred.StatusHttpServer;
import org.apache.hadoop.dfs.BlockCommand;
import org.apache.hadoop.dfs.DatanodeProtocol;
import org.apache.hadoop.dfs.FSDatasetInterface.MetaDataInputStream;
import org.apache.hadoop.dfs.datanode.metrics.DataNodeMetrics;

import java.io.*;
import java.net.*;
import java.nio.ByteBuffer;
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
public class DataNode implements FSConstants, Runnable {
  public static final Log LOG = LogFactory.getLog("org.apache.hadoop.dfs.DataNode");

  /**
   * Use {@link NetUtils#createSocketAddr(String)} instead.
   */
  @Deprecated
  public static InetSocketAddress createSocketAddr(String target
                                                   ) throws IOException {
    return NetUtils.createSocketAddr(target);
  }

  DatanodeProtocol namenode = null;
  FSDatasetInterface data = null;
  DatanodeRegistration dnRegistration = null;

  volatile boolean shouldRun = true;
  private LinkedList<Block> receivedBlockList = new LinkedList<Block>();
  private LinkedList<String> delHints = new LinkedList<String>();
  final private static String EMPTY_DEL_HINT = "";
  int xmitsInProgress = 0;
  Daemon dataXceiveServer = null;
  ThreadGroup threadGroup = null;
  long blockReportInterval;
  long lastBlockReport = 0;
  boolean resetBlockReportTime = true;
  long initialBlockReportDelay = BLOCKREPORT_INITIAL_DELAY * 1000L;
  boolean mustReportBlocks = false;
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
  int defaultBytesPerChecksum = 512;
  private int socketTimeout;
  
  private DataBlockScanner blockScanner;
  private Daemon blockScannerThread;

  /**
   * We need an estimate for block size to check if the disk partition has
   * enough space. For now we set it to be the default block size set
   * in the server side configuration, which is not ideal because the
   * default block size should be a client-size configuration. 
   * A better solution is to include in the header the estimated block size,
   * i.e. either the actual block size or the default block size.
   */
  private long estimateBlockSize;
  
  // The following three fields are to support balancing
  final static short MAX_BALANCING_THREADS = 5;
  private Semaphore balancingSem = new Semaphore(MAX_BALANCING_THREADS);
  long balanceBandwidth;
  private Throttler balancingThrottler;

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
    InetSocketAddress nameNodeAddr = NetUtils.createSocketAddr(
                                     conf.get("fs.default.name", "local"));
    
    this.defaultBytesPerChecksum = 
       Math.max(conf.getInt("io.bytes.per.checksum", 512), 1); 
    this.estimateBlockSize = conf.getLong("dfs.block.size", DEFAULT_BLOCK_SIZE);
    this.socketTimeout =  conf.getInt("dfs.socket.timeout",
                                      FSConstants.READ_TIMEOUT);
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
    ServerSocket ss = ServerSocketChannel.open().socket();
    Server.bind(ss, socAddr, 0);
    ss.setReceiveBufferSize(DEFAULT_DATA_SOCKET_SIZE); 
    // adjust machine name with the actual port
    tmpPort = ss.getLocalPort();
    selfAddr = new InetSocketAddress(ss.getInetAddress().getHostAddress(),
                                     tmpPort);
    this.dnRegistration.setName(machineName + ":" + tmpPort);
    LOG.info("Opened server at " + tmpPort);
      
    this.threadGroup = new ThreadGroup("dataXceiveServer");
    this.dataXceiveServer = new Daemon(threadGroup, new DataXceiveServer(ss));
    this.threadGroup.setDaemon(true); // auto destroy when empty

    long blockReportIntervalBasis =
      conf.getLong("dfs.blockreport.intervalMsec", BLOCKREPORT_INTERVAL);
    this.blockReportInterval =
      blockReportIntervalBasis - new Random().nextInt((int)(blockReportIntervalBasis/10));
    this.initialBlockReportDelay = conf.getLong("dfs.blockreport.initialDelay",
                                            BLOCKREPORT_INITIAL_DELAY)* 1000L; 
    if (this.initialBlockReportDelay >= blockReportIntervalBasis) {
      this.initialBlockReportDelay = 0;
      LOG.info("dfs.blockreport.initialDelay is greater than " +
        "dfs.blockreport.intervalMsec." + " Setting initial delay to 0 msec:");
    }
    this.heartBeatInterval = conf.getLong("dfs.heartbeat.interval", HEARTBEAT_INTERVAL) * 1000L;
    DataNode.nameNodeAddr = nameNodeAddr;

    //set up parameter for cluster balancing
    this.balanceBandwidth = conf.getLong("dfs.balance.bandwidthPerSec", 1024L*1024);
    LOG.info("Balancing bandwith is "+balanceBandwidth + " bytes/s");
    this.balancingThrottler = new Throttler(balanceBandwidth);

    //initialize periodic block scanner
    String reason = null;
    if (conf.getInt("dfs.datanode.scan.period.hours", 0) < 0) {
      reason = "verification is turned off by configuration";
    } else if ( !(data instanceof FSDataset) ) {
      reason = "verifcation is supported only with FSDataset";
    } 
    if ( reason == null ) {
      blockScanner = new DataBlockScanner(this, (FSDataset)data, conf);
      blockScannerThread = new Daemon(blockScanner);
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
    this.infoServer.addServlet(null, "/streamFile/*", StreamFile.class);
    this.infoServer.setAttribute("datanode.blockScanner", blockScanner);
    this.infoServer.addServlet(null, "/blockScannerReport", 
                               DataBlockScanner.Servlet.class);
    this.infoServer.start();
    // adjust info port
    this.dnRegistration.setInfoPort(this.infoServer.getPort());
    myMetrics = new DataNodeMetrics(conf, dnRegistration.getStorageID());
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
      "Data-node and name-node layout versions must be the same.";
    return nsInfo;
  }

  /** Return the DataNode object
   * 
   */
  public static DataNode getDataNode() {
    return datanodeObject;
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
      rand = (new Random()).nextInt(Integer.MAX_VALUE);
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
  }

  /**
   * Shut down this instance of the datanode.
   * Returns only after shutdown is complete.
   */
  public void shutdown() {
    if (infoServer != null) {
      try {
        infoServer.stop();
      } catch (Exception e) {
      }
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
          if (this.threadGroup.isDestroyed() ||
              this.threadGroup.activeCount() == 0) {
            break;
          }
          try {
            Thread.sleep(1000);
          } catch (InterruptedException e) {}
        }
      }
    }
    
    RPC.stopProxy(namenode); // stop the RPC threads
    
    if(upgradeManager != null)
      upgradeManager.shutdownUpgrade();
    if (blockScannerThread != null) {
      blockScanner.shutdown();
      blockScannerThread.interrupt();
    }
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
    if (e.getMessage().startsWith("No space left on device")) {
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
    try {
      namenode.errorReport(
                           dnRegistration, DatanodeProtocol.DISK_ERROR, errMsgr);
    } catch(IOException ignored) {              
    }
    shutdown();
  }
    
  private static class Count {
    int value = 0;
    Count(int init) { value = init; }
    synchronized void incr() { value++; }
    synchronized void decr() { value--; }
    @Override
    public String toString() { return Integer.toString(value); }
    public int getValue() { return value; }
  }
    
  Count xceiverCount = new Count(0);
    
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
          DatanodeCommand cmd = namenode.sendHeartbeat(dnRegistration,
                                                       data.getCapacity(),
                                                       data.getDfsUsed(),
                                                       data.getRemaining(),
                                                       xmitsInProgress,
                                                       xceiverCount.getValue());
          myMetrics.heartbeats.inc(now() - startTime);
          //LOG.info("Just sent heartbeat, with name " + localName);
          lastHeartbeat = startTime;
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
        if (mustReportBlocks || 
            startTime - lastBlockReport > blockReportInterval) {
          //
          // Send latest blockinfo report if timer has expired.
          // Get back a list of local block(s) that are obsolete
          // and can be safely GC'ed.
          //
          mustReportBlocks = false;
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
            lastBlockReport = startTime - new Random().nextInt((int)(blockReportInterval));
            resetBlockReportTime = false;
          } else {
            lastBlockReport = startTime;
          }
          processCommand(cmd);
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
            DisallowedDatanodeException.class.getName().equals(reClass)) {
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
    switch(cmd.getAction()) {
    case DatanodeProtocol.DNA_TRANSFER:
      //
      // Send a copy of a block to another datanode
      //
      BlockCommand bcmd = (BlockCommand)cmd;
      transferBlocks(bcmd.getBlocks(), bcmd.getTargets());
      break;
    case DatanodeProtocol.DNA_INVALIDATE:
      //
      // Some local block(s) are obsolete and can be 
      // safely garbage-collected.
      //
      Block toDelete[] = ((BlockCommand)cmd).getBlocks();
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
      register();
      // random short delay - helps scatter the BR from all DNs
      scheduleBlockReport(initialBlockReportDelay);
      break;
    case DatanodeProtocol.DNA_FINALIZE:
      storage.finalizeUpgrade();
      break;
    case UpgradeCommand.UC_ACTION_START_UPGRADE:
      // start distributed upgrade here
      processDistributedUpgradeCommand((UpgradeCommand)cmd);
      break;
    case DatanodeProtocol.DNA_BLOCKREPORT:
      try {
        if (initialBlockReportDelay != 0) {
          //sleep for a random time upto the heartbeat interval 
          //before sending the block report
          Thread.sleep((long)(new Random().nextDouble() * heartBeatInterval));
        }
        mustReportBlocks = true;
      } catch (InterruptedException ie) {}
      return false;
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
        new SocketInputStream(s), BUFFER_SIZE));
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
  private static void sendResponse(Socket s, short opStatus) throws IOException {
    DataOutputStream reply = 
      new DataOutputStream(new SocketOutputStream(s, WRITE_TIMEOUT));
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
      try {
        while (shouldRun) {
          Socket s = ss.accept();
          s.setTcpNoDelay(true);
          new Daemon(threadGroup, new DataXceiver(s)).start();
        }
        ss.close();
      } catch (IOException ie) {
        LOG.info(dnRegistration + ":Exiting DataXceiveServer due to " + ie.toString());
      }
    }
    public void kill() {
      assert shouldRun == false :
        "shoudRun should be set to false before killing";
      try {
        this.ss.close();
      } catch (IOException iex) {
      }

      // close all the sockets that were accepted earlier
      synchronized (childSockets) {
        for (Iterator it = childSockets.values().iterator();
             it.hasNext();) {
          Socket thissock = (Socket) it.next();
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
      LOG.debug("Number of active connections is: "+xceiverCount);
    }

    /**
     * Read/write data from/to the DataXceiveServer.
     */
    public void run() {
      DataInputStream in=null; 
      try {
        in = new DataInputStream(
            new BufferedInputStream(new SocketInputStream(s), BUFFER_SIZE));
        short version = in.readShort();
        if ( version != DATA_TRANSFER_VERSION ) {
          throw new IOException( "Version Mismatch" );
        }
        boolean local = s.getInetAddress().equals(s.getLocalAddress());
        byte op = in.readByte();
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
        LOG.debug(dnRegistration + ":Number of active connections is: "+xceiverCount);
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
      xceiverCount.incr();
      //
      // Read in the header
      //
      long blockId = in.readLong();          
      Block block = new Block( blockId, 0 );

      long startOffset = in.readLong();
      long length = in.readLong();

      // send the block
      DataOutputStream out = new DataOutputStream(
            new BufferedOutputStream(new SocketOutputStream(s, WRITE_TIMEOUT),
                                     SMALL_BUFFER_SIZE));
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
        long read = blockSender.sendBlock(out, null); // send data

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
        xceiverCount.decr();
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
      xceiverCount.incr();
      LOG.debug("writeBlock receive buf size " + s.getReceiveBufferSize() +
                " tcp no delay " + s.getTcpNoDelay());
      //
      // Read in the header
      //
      Block block = new Block(in.readLong(), estimateBlockSize);
      LOG.info("Receiving block " + block + 
               " src: " + remoteAddress +
               " dest: " + localAddress);
      int pipelineSize = in.readInt(); // num of datanodes in entire pipeline
      boolean isRecovery = in.readBoolean(); // is this part of recovery?
      String client = Text.readString(in); // working on behalf of this client
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
            s.getInetAddress().toString(), isRecovery, client);

        // get a connection back to the previous target
        replyOut = new DataOutputStream(
                       new SocketOutputStream(s, WRITE_TIMEOUT));

        //
        // Open network conn to backup machine, if 
        // appropriate
        //
        if (targets.length > 0) {
          InetSocketAddress mirrorTarget = null;
          // Connect to backup machine
          mirrorNode = targets[0].getName();
          mirrorTarget = NetUtils.createSocketAddr(mirrorNode);
          mirrorSock = SocketChannel.open().socket();
          try {
            int timeoutValue = numTargets * socketTimeout;
            int writeTimeout = WRITE_TIMEOUT + 
                               (WRITE_TIMEOUT_EXTENSION * numTargets);
            mirrorSock.connect(mirrorTarget, timeoutValue);
            mirrorSock.setSoTimeout(timeoutValue);
            mirrorSock.setSendBufferSize(DEFAULT_DATA_SOCKET_SIZE);
            mirrorOut = new DataOutputStream(
               new BufferedOutputStream(
                           new SocketOutputStream(mirrorSock, writeTimeout),
                           BUFFER_SIZE));
            mirrorIn = new DataInputStream(new SocketInputStream(mirrorSock));

            // Write header: Copied from DFSClient.java!
            mirrorOut.writeShort( DATA_TRANSFER_VERSION );
            mirrorOut.write( OP_WRITE_BLOCK );
            mirrorOut.writeLong( block.getBlockId() );
            mirrorOut.writeInt( pipelineSize );
            mirrorOut.writeBoolean( isRecovery );
            Text.writeString( mirrorOut, client );
            mirrorOut.writeInt( targets.length - 1 );
            for ( int i = 1; i < targets.length; i++ ) {
              targets[i].write( mirrorOut );
            }

            blockReceiver.writeChecksumHeader(mirrorOut);
            mirrorOut.flush();

            // read connect ack (only for clients, not for replication req)
            if (client.length() != 0) {
              firstBadLink = Text.readString(mirrorIn);
              LOG.info("Datanode " + targets.length +
                       " got response for connect ack " +
                       " from downstream datanode with firstbadlink as " +
                       firstBadLink);
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
            throw e;
          }
        }

        // send connect ack back to source (only for clients)
        if (client.length() != 0) {
          LOG.info("Datanode " + targets.length +
                   " forwarding connect ack to upstream firstbadlink is " +
                   firstBadLink);
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
        // decrement counter
        xceiverCount.decr();
      }
    }

    /**
     * Reads the metadata and sends the data in one 'DATA_CHUNK'
     * @param in
     */
    void readMetadata(DataInputStream in) throws IOException {
      xceiverCount.incr();

      Block block = new Block( in.readLong(), 0 );
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
        
        out = new DataOutputStream(new SocketOutputStream(s, WRITE_TIMEOUT));
        
        out.writeByte(OP_STATUS_SUCCESS);
        out.writeInt(buf.length);
        out.write(buf);
        
        //last DATA_CHUNK
        out.writeInt(0);
      } finally {
        xceiverCount.decr();
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
      Block block = new Block(blockId, 0);
      String source = Text.readString(in); // read del hint
      DatanodeInfo target = new DatanodeInfo(); // read target
      target.readFields(in);

      Socket targetSock = null;
      short opStatus = OP_STATUS_SUCCESS;
      BlockSender blockSender = null;
      DataOutputStream targetOut = null;
      try {
        balancingSem.acquireUninterruptibly();
        
        // check if the block exists or not
        blockSender = new BlockSender(block, 0, -1, false, false, false);

        // get the output stream to the target
        InetSocketAddress targetAddr = NetUtils.createSocketAddr(target.getName());
        targetSock = SocketChannel.open().socket();
        targetSock.connect(targetAddr, socketTimeout);
        targetSock.setSoTimeout(socketTimeout);

        targetOut = new DataOutputStream(new BufferedOutputStream(
                        new SocketOutputStream(targetSock, WRITE_TIMEOUT),
                        SMALL_BUFFER_SIZE));

        /* send request to the target */
        // fist write header info
        targetOut.writeShort(DATA_TRANSFER_VERSION); // transfer version
        targetOut.writeByte(OP_REPLACE_BLOCK); // op code
        targetOut.writeLong(block.getBlockId()); // block id
        Text.writeString( targetOut, source); // del hint

        // then send data
        long read = blockSender.sendBlock(targetOut, balancingThrottler);

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
        /* send response to the requester */
        try {
          sendResponse(s, opStatus);
        } catch (IOException replyE) {
          LOG.warn("Error writing the response back to "+
              s.getRemoteSocketAddress() + "\n" +
              StringUtils.stringifyException(replyE) );
        }
        IOUtils.closeStream(targetOut);
        IOUtils.closeStream(blockSender);
        balancingSem.release();
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
      balancingSem.acquireUninterruptibly();

      /* read header */
      Block block = new Block(in.readLong(), estimateBlockSize); // block id & len
      String sourceID = Text.readString(in);

      short opStatus = OP_STATUS_SUCCESS;
      BlockReceiver blockReceiver = null;
      try {
        // open a block receiver and check if the block does not exist
         blockReceiver = new BlockReceiver(
            block, in, s.getRemoteSocketAddress().toString(), false, "");

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
        // send response back
        try {
          sendResponse(s, opStatus);
        } catch (IOException ioe) {
          LOG.warn("Error writing reply back to " + s.getRemoteSocketAddress());
        }
        IOUtils.closeStream(blockReceiver);
        balancingSem.release();
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
     +-------------------------------------------------------+
     | 8 byte Block ID | 8 byte start offset | 8 byte length |
     +-------------------------------------------------------+
     
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
  
  class BlockSender implements java.io.Closeable {
    private Block block; // the block to read from
    private InputStream blockIn; // data stream
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
    private OutputStream out;
    
    static final int PKT_HEADER_LEN = ( 4 + /* PacketLen */
                                        8 + /* offset in block */
                                        8 + /* seqno */
                                        1 + /* isLastPacketInBlock */
                                        4   /* data len */ );
       
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
          short version = checksumIn.readShort();
          if (version != FSDataset.METADATA_VERSION) {
            LOG.warn("Wrong version (" + version + ") for metadata file for "
                + block + " ignoring ...");
          }
          checksum = DataChecksum.newDataChecksum(checksumIn);
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
     */
    private int sendChunks(ByteBuffer pkt, int maxChunks) throws IOException {
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
            // Just fill the array with zeros.
            Arrays.fill(buf, checksumOff, checksumLen, (byte) 0);
          } else {
            throw e;
          }
        }
      }
      
      int dataOff = checksumOff + checksumLen;
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

      out.write(buf, 0, dataOff + len);

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
     * returns total bytes reads, including crc.
     */
    long sendBlock(DataOutputStream out, Throttler throttler)
        throws IOException {
      if( out == null ) {
        throw new IOException( "out stream is null" );
      }
      this.out = out;
      this.throttler = throttler;

      long initialOffset = offset;
      long totalRead = 0;
      try {
        checksum.writeHeader(out);
        if ( chunkOffsetOK ) {
          out.writeLong( offset );
        }
        //set up sendBuf:
        int maxChunksPerPacket = Math.max(1,
                      (BUFFER_SIZE + bytesPerChecksum - 1)/bytesPerChecksum);
        ByteBuffer pktBuf = ByteBuffer.allocate(PKT_HEADER_LEN + 
                      (bytesPerChecksum + checksumSize) * maxChunksPerPacket);


        while (endOffset > offset) {
          long len = sendChunks(pktBuf, maxChunksPerPacket);
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
            } else {
              // flush packet to disk before sending ack
              if (!receiver.finalized) {
                receiver.flush();
              }
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
                if (!didRead) {
                  op = OP_STATUS_ERROR;
                }
              }
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
            else if (!lastPacketInBlock) {
              // flush packet to disk before sending ack
              if (!receiver.finalized) {
                receiver.flush();
              }
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

  // this class is a bufferoutputstream that exposes the number of
  // bytes in the buffer.
  static private class DFSBufferedOutputStream extends BufferedOutputStream {
    OutputStream out;
    DFSBufferedOutputStream(OutputStream out, int capacity) {
      super(out, capacity);
      this.out = out;
    }

    public synchronized void flush() throws IOException {
      super.flush();
    }

    /**
     * Returns true if the channel pointer is already set at the
     * specified offset. Otherwise returns false.
     */
    synchronized boolean samePosition(FSDatasetInterface data, 
                                      FSDataset.BlockWriteStreams streams,
                                      Block block,
                                      long offset) 
                                      throws IOException {
      if (data.getChannelPosition(block, streams) + count == offset) {
        return true;
      }
      LOG.debug("samePosition is false. " +
                " current position " + data.getChannelPosition(block, streams)+
                " buffered size " + count +
                " new offset " + offset);
      return false;
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
    private DataOutputStream out = null; // to block file at local disk
    private DataOutputStream checksumOut = null; // to crc file at local disk
    private DFSBufferedOutputStream bufStream = null;
    private int bytesPerChecksum;
    private int checksumSize;
    private byte buf[];
    private byte checksumBuf[];
    private long offsetInBlock;
    final private String inAddr;
    private String mirrorAddr;
    private DataOutputStream mirrorOut;
    private Daemon responder = null;
    private Throttler throttler;
    private FSDataset.BlockWriteStreams streams;
    private boolean isRecovery = false;
    private String clientName;
    private Object currentWriteLock;
    volatile private boolean currentWrite;

    BlockReceiver(Block block, DataInputStream in, String inAddr,
                  boolean isRecovery, String clientName)
        throws IOException {
      try{
        this.block = block;
        this.in = in;
        this.inAddr = inAddr;
        this.isRecovery = isRecovery;
        this.clientName = clientName;
        this.offsetInBlock = 0;
        this.currentWriteLock = new Object();
        this.currentWrite = false;
        this.checksum = DataChecksum.newDataChecksum(in);
        this.bytesPerChecksum = checksum.getBytesPerChecksum();
        this.checksumSize = checksum.getChecksumSize();
        this.buf = new byte[bytesPerChecksum + checksumSize];
        this.checksumBuf = new byte[checksumSize];
        //
        // Open local disk out
        //
        streams = data.writeToBlock(block, isRecovery);
        this.finalized = data.isValidBlock(block);
        if (streams != null) {
          this.bufStream = new DFSBufferedOutputStream(
                                          streams.dataOut, BUFFER_SIZE);
          this.out = new DataOutputStream(bufStream);
          this.checksumOut = new DataOutputStream(new BufferedOutputStream(
                                          streams.checksumOut, BUFFER_SIZE));
        }
      } catch(IOException ioe) {
        IOUtils.closeStream(this);
        throw ioe;
      }
    }

    // close files
    public void close() throws IOException {

      synchronized (currentWriteLock) {
        while (currentWrite) {
          try {
            LOG.info("BlockReceiver for block " + block +
                     " waiting for last write to drain.");
            currentWriteLock.wait();
          } catch (InterruptedException e) {
            throw new IOException("BlockReceiver for block " + block +
                                  " interrupted drain of last io.");
          }
        }
      }
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

    /* receive a chunk: write it to disk & mirror it to another stream */
    private void receiveChunk( int len, byte[] checksumBuf, int checksumOff ) 
                              throws IOException {
      if (len <= 0 || len > bytesPerChecksum) {
        throw new IOException("Got wrong length during writeBlock(" + block
            + ") from " + inAddr + " at offset " + offsetInBlock + ": " + len
            + " expected <= " + bytesPerChecksum);
      }

      in.readFully(buf, 0, len);

      /*
       * Verification is not included in the initial design. For now, it at
       * least catches some bugs. Later, we can include this after showing that
       * it does not affect performance much.
       */
      checksum.update(buf, 0, len);

      if (!checksum.compare(checksumBuf, checksumOff)) {
        throw new IOException("Unexpected checksum mismatch "
            + "while writing " + block + " from " + inAddr);
      }

      checksum.reset();

      // record the fact that the current write is still in progress
      synchronized (currentWriteLock) {
        currentWrite = true;
      }
      offsetInBlock += len;

      // First write to remote node before writing locally.
      if (mirrorOut != null) {
        try {
          mirrorOut.writeInt(len);
          mirrorOut.write(checksumBuf, checksumOff, checksumSize);
          mirrorOut.write(buf, 0, len);
        } catch (IOException ioe) {
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
            synchronized (currentWriteLock) {
              currentWrite = false;
              currentWriteLock.notifyAll();
            }
            throw ioe;
          }
        }
      }

      try {
        if (!finalized) {
          out.write(buf, 0, len);
          // Write checksum
          checksumOut.write(checksumBuf, checksumOff, checksumSize);
          myMetrics.bytesWritten.inc(len);
        }
      } catch (IOException iex) {
        checkDiskError(iex);
        throw iex;
      } finally {
        synchronized (currentWriteLock) {
          currentWrite = false;
          currentWriteLock.notifyAll();
        }
      }

      if (throttler != null) { // throttle I/O
        throttler.throttle(len + checksumSize + 4);
      }
    }

    /* 
     * Receive and process a packet. It contains many chunks.
     */
    private void receivePacket(int packetSize) throws IOException {
      /* TEMP: Currently this handles both interleaved 
       * and non-interleaved DATA_CHUNKs in side the packet.
       * non-interleaved is required for HADOOP-2758 and in future.
       * iterleaved will be removed once extra buffer copies are removed
       * in write path (HADOOP-1702).
       * 
       * Format of Non-interleaved data packets is described in the 
       * comment before BlockSender.
       */
      offsetInBlock = in.readLong(); // get offset of packet in block
      long seqno = in.readLong();    // get seqno
      boolean lastPacketInBlock = in.readBoolean();
      int curPacketSize = 0;         
      LOG.debug("Receiving one packet for block " + block +
                " of size " + packetSize +
                " seqno " + seqno +
                " offsetInBlock " + offsetInBlock +
                " lastPacketInBlock " + lastPacketInBlock);
      setBlockPosition(offsetInBlock);
      
      int len = in.readInt();
      curPacketSize += 4;            // read an integer in previous line

      // send packet header to next datanode in pipeline
      if (mirrorOut != null) {
        try {
          int mirrorPacketSize = packetSize;
          if (len > bytesPerChecksum) {
            /* 
             * This is a packet with non-interleaved checksum. 
             * But we are sending interleaving checksums to mirror, 
             * which changes packet len. Adjust the packet size for mirror.
             * 
             * As mentioned above, this is mismatch is 
             * temporary till HADOOP-1702.
             */
            
            //find out how many chunks are in this patcket :
            int chunksInPkt = (len + bytesPerChecksum - 1)/bytesPerChecksum;
            
            // we send 4 more bytes for for each of the extra 
            // checksum chunks. so :
            mirrorPacketSize += (chunksInPkt - 1) * 4;
          }
          mirrorOut.writeInt(mirrorPacketSize);
          mirrorOut.writeLong(offsetInBlock);
          mirrorOut.writeLong(seqno);
          mirrorOut.writeBoolean(lastPacketInBlock);
        } catch (IOException e) {
          LOG.info("Exception writing to mirror " + mirrorAddr + "\n"
              + StringUtils.stringifyException(e));
          mirrorOut = null;

          // If stream-copy fails, continue
          // writing to disk for replication requests. For client
          // writes, return error so that the client can do error
          // recovery.
          //
          if (clientName.length() > 0) {
            throw e;
          }
        }
        // first enqueue the ack packet to avoid a race with the response coming
        // from downstream datanode.
        if (responder != null) {
          ((PacketResponder)responder.getRunnable()).enqueue(seqno, 
                                          lastPacketInBlock); 
        }
      }

      if (len == 0) {
        LOG.info("Receiving empty packet for block " + block);
        if (mirrorOut != null) {
          mirrorOut.writeInt(len);
          mirrorOut.flush();
        }
      }

      while (len != 0) {
        int checksumOff = 0;    
        if (len > 0) {
          int checksumLen = (len + bytesPerChecksum - 1)/bytesPerChecksum*
                            checksumSize;
          if (checksumBuf.length < checksumLen) {
            checksumBuf = new byte[checksumLen];
          }
          // read the checksum
          in.readFully(checksumBuf, 0, checksumLen);
        }
        
        while (len != 0) {
          if (LOG.isDebugEnabled()) {
            LOG.debug("Receiving one chunk for block " + block +
                      " of size " + len);
          }
          
          int toRecv = Math.min(len, bytesPerChecksum);
          
          curPacketSize += (toRecv + checksumSize);
          if (curPacketSize > packetSize) {
            throw new IOException("Packet size for block " + block +
                                  " too long " + curPacketSize +
                                  " was expecting " + packetSize);
          } 
          
          receiveChunk(toRecv, checksumBuf, checksumOff);
          
          len -= toRecv;
          checksumOff += checksumSize;       
        }
        
        if (curPacketSize == packetSize) {
          if (mirrorOut != null) {
            mirrorOut.flush();
          }
          break;
        }
        len = in.readInt();
        curPacketSize += 4;
      }

      // put in queue for pending acks
      if (responder != null && mirrorOut == null) {
        ((PacketResponder)responder.getRunnable()).enqueue(seqno,
                                        lastPacketInBlock); 
      }
    }

    public void writeChecksumHeader(DataOutputStream mirrorOut) throws IOException {
      checksum.writeHeader(mirrorOut);
    }
   

    public void receiveBlock(
        DataOutputStream mirrorOut, // output to next datanode
        DataInputStream mirrorIn,   // input from next datanode
        DataOutputStream replyOut,  // output to previous datanode
        String mirrorAddr, Throttler throttler,
        int numTargets) throws IOException {

        this.mirrorOut = mirrorOut;
        this.mirrorAddr = mirrorAddr;
        this.throttler = throttler;

      try {
        // write data chunk header
        if (!finalized) {
          checksumOut.writeShort(FSDataset.METADATA_VERSION);
          checksum.writeHeader(checksumOut);
        }
        if (clientName.length() > 0) {
          responder = new Daemon(threadGroup, 
                                 new PacketResponder(this, block, mirrorIn, 
                                                     replyOut, numTargets,
                                                     clientName));
          responder.start(); // start thread to processes reponses
        }

        /* 
         * Skim packet headers. A response is needed for every packet.
         */
        int len = in.readInt(); // get packet size
        while (len != 0) {
          receivePacket(len);
          len = in.readInt(); // get packet size
        }

        // flush the mirror out
        if (mirrorOut != null) {
          mirrorOut.writeInt(0); // mark the end of the block
          mirrorOut.flush();
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
      if (bufStream.samePosition(data, streams, block, offsetInBlock)) {
        return;
      }
      if (offsetInBlock % bytesPerChecksum != 0) {
        throw new IOException("setBlockPosition trying to set position to " +
                              offsetInBlock +
                              " which is not a multiple of bytesPerChecksum " +
                               bytesPerChecksum);
      }
      long offsetInChecksum = checksum.getChecksumHeaderSize() + 
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
        sock = SocketChannel.open().socket();
        sock.connect(curTarget, socketTimeout);
        sock.setSoTimeout(targets.length * socketTimeout);

        long writeTimeout = WRITE_TIMEOUT + 
                            WRITE_TIMEOUT_EXTENSION * (targets.length-1);
        out = new DataOutputStream(new BufferedOutputStream(
               new SocketOutputStream(sock, writeTimeout), SMALL_BUFFER_SIZE));

        blockSender = new BlockSender(b, 0, -1, false, false, false);

        //
        // Header info
        //
        out.writeShort(DATA_TRANSFER_VERSION);
        out.writeByte(OP_WRITE_BLOCK);
        out.writeLong(b.getBlockId());
        out.writeInt(0);           // no pipelining
        out.writeBoolean(false);   // not part of recovery
        Text.writeString(out, ""); // client
        // write targets
        out.writeInt(targets.length - 1);
        for (int i = 1; i < targets.length; i++) {
          targets[i].write(out);
        }
        // send data & checksum
        blockSender.sendBlock(out, null);

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

    // start block scanner
    if (blockScannerThread != null) {
      blockScannerThread.start();
    }

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
        
    // wait for dataXceiveServer to terminate
    try {
      this.dataXceiveServer.join();
    } catch (InterruptedException ie) {
    }
        
    LOG.info(dnRegistration + ":Finishing DataNode in: "+data);
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
                            - ( blockReportInterval - new Random().nextInt((int)(delay)));
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
}
