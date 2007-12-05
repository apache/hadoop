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

import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.ipc.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.metrics.MetricsUtil;
import org.apache.hadoop.net.DNS;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.net.NodeBase;
import org.apache.hadoop.util.*;
import org.apache.hadoop.util.DiskChecker.DiskErrorException;
import org.apache.hadoop.util.DiskChecker.DiskOutOfSpaceException;
import org.apache.hadoop.mapred.StatusHttpServer;
import org.apache.hadoop.net.NetworkTopology;
import org.apache.hadoop.dfs.BlockCommand;
import org.apache.hadoop.dfs.DatanodeProtocol;
import org.apache.hadoop.dfs.FSDatasetInterface.MetaDataInputStream;

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.Semaphore;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;

import org.apache.hadoop.metrics.MetricsContext;
import org.apache.hadoop.metrics.MetricsRecord;
import org.apache.hadoop.metrics.Updater;
import org.apache.hadoop.metrics.jvm.JvmMetrics;

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
  private String networkLoc;
  volatile boolean shouldRun = true;
  private LinkedList<Block> receivedBlockList = new LinkedList<Block>();
  private LinkedList<String> delHints = new LinkedList<String>();
  final private static String EMPTY_DEL_HINT = "";
  int xmitsInProgress = 0;
  Daemon dataXceiveServer = null;
  long blockReportInterval;
  long lastBlockReport = 0;
  long lastHeartbeat = 0;
  long heartBeatInterval;
  private DataStorage storage = null;
  private StatusHttpServer infoServer = null;
  private DataNodeMetrics myMetrics;
  private static InetSocketAddress nameNodeAddr;
  private static DataNode datanodeObject = null;
  private Thread dataNodeThread = null;
  String machineName;
  int defaultBytesPerChecksum = 512;

  // The following three fields are to support balancing
  final static short MAX_BALANCING_THREADS = 5;
  private Semaphore balancingSem = new Semaphore(MAX_BALANCING_THREADS);
  long balanceBandwidth;
  private Throttler balancingThrottler;

  private static class DataNodeMetrics implements Updater {
    private final MetricsRecord metricsRecord;
    private int bytesWritten = 0;
    private int bytesRead = 0;
    private int blocksWritten = 0;
    private int blocksRead = 0;
    private int blocksReplicated = 0;
    private int blocksRemoved = 0;
      
    DataNodeMetrics(Configuration conf) {
      String sessionId = conf.get("session.id"); 
      // Initiate reporting of Java VM metrics
      JvmMetrics.init("DataNode", sessionId);
      // Create record for DataNode metrics
      MetricsContext context = MetricsUtil.getContext("dfs");
      metricsRecord = MetricsUtil.createRecord(context, "datanode");
      metricsRecord.setTag("sessionId", sessionId);
      context.registerUpdater(this);
    }
      
    /**
     * Since this object is a registered updater, this method will be called
     * periodically, e.g. every 5 seconds.
     */
    public void doUpdates(MetricsContext unused) {
      synchronized (this) {
        metricsRecord.incrMetric("bytes_read", bytesRead);
        metricsRecord.incrMetric("bytes_written", bytesWritten);
        metricsRecord.incrMetric("blocks_read", blocksRead);
        metricsRecord.incrMetric("blocks_written", blocksWritten);
        metricsRecord.incrMetric("blocks_replicated", blocksReplicated);
        metricsRecord.incrMetric("blocks_removed", blocksRemoved);
              
        bytesWritten = 0;
        bytesRead = 0;
        blocksWritten = 0;
        blocksRead = 0;
        blocksReplicated = 0;
        blocksRemoved = 0;
      }
      metricsRecord.update();
    }

    synchronized void readBytes(int nbytes) {
      bytesRead += nbytes;
    }
      
    synchronized void wroteBytes(int nbytes) {
      bytesWritten += nbytes;
    }
      
    synchronized void readBlocks(int nblocks) {
      blocksRead += nblocks;
    }
      
    synchronized void wroteBlocks(int nblocks) {
      blocksWritten += nblocks;
    }
      
    synchronized void replicatedBlocks(int nblocks) {
      blocksReplicated += nblocks;
    }
      
    synchronized void removedBlocks(int nblocks) {
      blocksRemoved += nblocks;
    }
  }
    
  /**
   * Create the DataNode given a configuration and an array of dataDirs.
   * 'dataDirs' is where the blocks are stored.
   */
  DataNode(Configuration conf, 
           AbstractList<File> dataDirs) throws IOException {
      
    myMetrics = new DataNodeMetrics(conf);
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
   * 		if conf's CONFIG_PROPERTY_SIMULATED property is set
   *  then a simulated storage based data node is created.
   * 
   * @param dataDirs - only for a non-simulated storage data node
   * @throws IOException
   */
  void startDataNode(Configuration conf, 
                     AbstractList<File> dataDirs
                     ) throws IOException {
    // use configured nameserver & interface to get local hostname
    machineName = DNS.getDefaultHost(
                                     conf.get("dfs.datanode.dns.interface","default"),
                                     conf.get("dfs.datanode.dns.nameserver","default"));
    InetSocketAddress nameNodeAddr = NetUtils.createSocketAddr(
                                     conf.get("fs.default.name", "local"));
    
    this.defaultBytesPerChecksum = 
       Math.max(conf.getInt("io.bytes.per.checksum", 512), 1); 
    
    String address = conf.get("dfs.datanode.bindAddress", "0.0.0.0:50010");
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
    ServerSocket ss = new ServerSocket(tmpPort, 0, socAddr.getAddress());
    // adjust machine name with the actual port
    tmpPort = ss.getLocalPort();
    this.dnRegistration.setName(machineName + ":" + tmpPort);
    LOG.info("Opened server at " + tmpPort);
      
    this.dataXceiveServer = new Daemon(new DataXceiveServer(ss));

    long blockReportIntervalBasis =
      conf.getLong("dfs.blockreport.intervalMsec", BLOCKREPORT_INTERVAL);
    this.blockReportInterval =
      blockReportIntervalBasis - new Random().nextInt((int)(blockReportIntervalBasis/10));
    this.heartBeatInterval = conf.getLong("dfs.heartbeat.interval", HEARTBEAT_INTERVAL) * 1000L;
    DataNode.nameNodeAddr = nameNodeAddr;

    //set up parameter for cluster balancing
    this.balanceBandwidth = conf.getLong("dfs.balance.bandwidthPerSec", 1024L*1024);
    LOG.info("Balancing bandwith is "+balanceBandwidth + " bytes/s");
    this.balancingThrottler = new Throttler(balanceBandwidth);

    //create a servlet to serve full-file content
    String infoAddr = conf.get("dfs.datanode.http.bindAddress", "0.0.0.0:50075");
    InetSocketAddress infoSocAddr = NetUtils.createSocketAddr(infoAddr);
    String infoHost = infoSocAddr.getHostName();
    int tmpInfoPort = infoSocAddr.getPort();
    this.infoServer = new StatusHttpServer("datanode", infoHost, tmpInfoPort, tmpInfoPort == 0);
    this.infoServer.addServlet(null, "/streamFile/*", StreamFile.class);
    this.infoServer.start();
    // adjust info port
    this.dnRegistration.setInfoPort(this.infoServer.getPort());
    // get network location
    this.networkLoc = conf.get("dfs.datanode.rack");
    if (networkLoc == null)  // exec network script or set the default rack
      networkLoc = getNetworkLoc(conf);
    // register datanode
    register();
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
        dnRegistration = namenode.register(dnRegistration, networkLoc);
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
    }
    if(upgradeManager != null)
      upgradeManager.shutdownUpgrade();
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
     
    LOG.info("using BLOCKREPORT_INTERVAL of " + blockReportInterval + "msec");

    //
    // Now loop for a long time....
    //

    while (shouldRun) {
      try {
        long now = System.currentTimeMillis();

        //
        // Every so often, send heartbeat or block-report
        //
        if (now - lastHeartbeat > heartBeatInterval) {
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
          //LOG.info("Just sent heartbeat, with name " + localName);
          lastHeartbeat = now;
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
        if (now - lastBlockReport > blockReportInterval) {
          //
          // Send latest blockinfo report if timer has expired.
          // Get back a list of local block(s) that are obsolete
          // and can be safely GC'ed.
          //
          DatanodeCommand cmd = namenode.blockReport(dnRegistration,
                                                     data.getBlockReport());
          //
          // If we have sent the first block report, then wait a random
          // time before we start the periodic block reports.
          //
          if (lastBlockReport == 0) {
            lastBlockReport = now - new Random().nextInt((int)(blockReportInterval));
          } else {
            lastBlockReport = now;
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
        data.invalidate(toDelete);
      } catch(IOException e) {
        checkDiskError();
        throw e;
      }
      myMetrics.removedBlocks(toDelete.length);
      break;
    case DatanodeProtocol.DNA_SHUTDOWN:
      // shut down the data node
      this.shutdown();
      return false;
    case DatanodeProtocol.DNA_REGISTER:
      // namenode requested a registration
      register();
      scheduleBlockReport();
      break;
    case DatanodeProtocol.DNA_FINALIZE:
      storage.finalizeUpgrade();
      break;
    case UpgradeCommand.UC_ACTION_START_UPGRADE:
      // start distributed upgrade here
      processDistributedUpgradeCommand((UpgradeCommand)cmd);
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
      if (xferTargets[i].length > 0) {
        LOG.info(dnRegistration + ":Starting thread to transfer block " + blocks[i] + " to " + xferTargets[i]);
        new Daemon(new DataTransfer(xferTargets[i], blocks[i])).start();
      }
    }
  }

  /* utility function for receiving a response */
  private static void receiveResponse(Socket s) throws IOException {
    // check the response
    DataInputStream reply = new DataInputStream(new BufferedInputStream(
        s.getInputStream(), BUFFER_SIZE));
    try {
      short opStatus = reply.readShort();
      if(opStatus != OP_STATUS_SUCCESS) {
        throw new IOException("operation failed at "+
            s.getInetAddress());
      } 
    } finally {
      IOUtils.closeStream(reply);
    }
  }

  /* utility function for sending a respose */
  private static void sendResponse(Socket s, short opStatus) throws IOException {
    DataOutputStream reply = new DataOutputStream(s.getOutputStream());
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
          new Daemon(new DataXceiver(s)).start();
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
    }
  }

  /**
   * Thread for processing incoming/outgoing data stream
   */
  class DataXceiver implements Runnable {
    Socket s;
    public DataXceiver(Socket s) {
      this.s = s;
      LOG.debug("Number of active connections is: "+xceiverCount);
    }

    /**
     * Read/write data from/to the DataXceiveServer.
     */
    public void run() {
      DataInputStream in=null; 
      try {
        in = new DataInputStream(
            new BufferedInputStream(s.getInputStream(), BUFFER_SIZE));
        short version = in.readShort();
        if ( version != DATA_TRANFER_VERSION ) {
          throw new IOException( "Version Mismatch" );
        }

        byte op = in.readByte();

        switch ( op ) {
        case OP_READ_BLOCK:
          readBlock( in );
          break;
        case OP_WRITE_BLOCK:
          writeBlock( in );
          break;
        case OP_READ_METADATA:
          readMetadata( in );
          break;
        case OP_REPLACE_BLOCK: // for balancing purpose; send to a destination
          replaceBlock(in);
          break;
        case OP_COPY_BLOCK: // for balancing purpose; send to a proxy source
          copyBlock(in);
          break;
        default:
          throw new IOException("Unknown opcode " + op + "in data stream");
        }
       } catch (Throwable t) {
        LOG.error(dnRegistration + ":DataXceiver: " + StringUtils.stringifyException(t));
      } finally {
        LOG.debug(dnRegistration + ":Number of active connections is: "+xceiverCount);
        IOUtils.closeStream(in);
        IOUtils.closeSocket(s);
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
          new BufferedOutputStream(s.getOutputStream(), BUFFER_SIZE));
      BlockSender blockSender = null;
      try {
        try {
          blockSender = new BlockSender(block, startOffset, length, true, true);
        } catch(IOException e) {
          out.writeShort(OP_STATUS_ERROR);
          throw e;
        }

        out.writeShort(DataNode.OP_STATUS_SUCCESS); // send op status
        long read = blockSender.sendBlock(out, null); // send data

        myMetrics.readBytes((int) read);
        myMetrics.readBlocks(1);
        LOG.info(dnRegistration + "Served block " + block + " to " + s.getInetAddress());
      } catch ( SocketException ignored ) {
        // Its ok for remote side to close the connection anytime.
        myMetrics.readBlocks(1);
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
      //
      // Read in the header
      //
      Block block = new Block(in.readLong(), 0);
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

      short opStatus = OP_STATUS_SUCCESS; // write operation status
      DataOutputStream mirrorOut = null;  // stream to next target
      Socket mirrorSock = null;           // socket to next target
      BlockReceiver blockReceiver = null; // responsible for data handling
      String mirrorNode = null;           // the name:port of next target
      try {
        // open a block receiver and check if the block does not exist
        blockReceiver = new BlockReceiver(block, in, 
            s.getInetAddress().toString());

        //
        // Open network conn to backup machine, if 
        // appropriate
        //
        if (targets.length > 0) {
          InetSocketAddress mirrorTarget = null;
          // Connect to backup machine
          mirrorNode = targets[0].getName();
          mirrorTarget = NetUtils.createSocketAddr(mirrorNode);
          mirrorSock = new Socket();
          try {
            mirrorSock.connect(mirrorTarget, READ_TIMEOUT);
            mirrorSock.setSoTimeout(numTargets*READ_TIMEOUT);
            mirrorOut = new DataOutputStream( 
                        new BufferedOutputStream(mirrorSock.getOutputStream(),
                                                 BUFFER_SIZE));
            // Write header: Copied from DFSClient.java!
            mirrorOut.writeShort( DATA_TRANFER_VERSION );
            mirrorOut.write( OP_WRITE_BLOCK );
            mirrorOut.writeLong( block.getBlockId() );
            mirrorOut.writeInt( targets.length - 1 );
            for ( int i = 1; i < targets.length; i++ ) {
              targets[i].write( mirrorOut );
            }
          } catch (IOException e) {
            IOUtils.closeStream(mirrorOut);
            mirrorOut = null;
            IOUtils.closeSocket(mirrorSock);
            mirrorSock = null;
          }
        }

        // receive the block and mirror to the next target
        String mirrorAddr = (mirrorSock == null) ? null : mirrorNode;

        blockReceiver.receiveBlock(mirrorOut, mirrorAddr, null);

        // notify name node
        notifyNamenodeReceivedBlock(block, EMPTY_DEL_HINT);

        String msg = "Received block " + block + " from " +
                     s.getInetAddress();

        /* read response from next target in the pipeline. 
         * ignore the response for now. Will fix it in HADOOP-1927
         */
        if( mirrorSock != null ) {
          try {
            receiveResponse(mirrorSock);
          } catch (IOException ignored) {
            msg += " and " +  ignored.getMessage();
          } 
        }

        LOG.info(msg);
      } catch (IOException ioe) {
        opStatus = OP_STATUS_ERROR;
        throw ioe;
      } finally {
        // send back reply
        try {
          sendResponse(s, opStatus);
        } catch (IOException ioe) {
          LOG.warn(dnRegistration +":Error writing reply of status " + opStatus +  " back to " + s.getInetAddress() +
              " for writing block " + block +"\n" +
              StringUtils.stringifyException(ioe));
        }
        // close all opened streams
        IOUtils.closeStream(mirrorOut);
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
        
        out = new DataOutputStream(s.getOutputStream());
        
        out.writeByte(OP_STATUS_SUCCESS);
        out.writeInt(buf.length);
        out.write(buf);
        
        //last DATA_CHUNK
        out.writeInt(0);
      } finally {
        xceiverCount.decr();
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
        blockSender = new BlockSender(block, 0, -1, false, false);

        // get the output stream to the target
        InetSocketAddress targetAddr = NetUtils.createSocketAddr(target.getName());
        targetSock = new Socket();
        targetSock.connect(targetAddr, READ_TIMEOUT);
        targetSock.setSoTimeout(READ_TIMEOUT);

        targetOut = new DataOutputStream(new BufferedOutputStream(
            targetSock.getOutputStream(), BUFFER_SIZE));

        /* send request to the target */
        // fist write header info
        targetOut.writeShort(DATA_TRANFER_VERSION); // transfer version
        targetOut.writeByte(OP_REPLACE_BLOCK); // op code
        targetOut.writeLong(block.getBlockId()); // block id
        Text.writeString( targetOut, source); // del hint

        // then send data
        long read = blockSender.sendBlock(targetOut, balancingThrottler);

        myMetrics.readBytes((int) read);
        myMetrics.readBlocks(1);
        
        // check the response from target
        receiveResponse(targetSock);

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
      Block block = new Block(in.readLong(), 0); // block id & len
      String sourceID = Text.readString(in);

      short opStatus = OP_STATUS_SUCCESS;
      BlockReceiver blockReceiver = null;
      try {
        // open a block receiver and check if the block does not exist
         blockReceiver = new BlockReceiver(
            block, in, s.getRemoteSocketAddress().toString());

        // receive a block
        blockReceiver.receiveBlock(null, null, balancingThrottler);
        
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

    /** Constructor */
    Throttler(long bandwidthPerSec) {
      this(500, bandwidthPerSec);  // by default throttling period is 500ms 
    }

    /** Constructor */
    Throttler(long period, long bandwidthPerSec) {
      this.curPeriodStart = System.currentTimeMillis();
      this.period = period;
      this.curReserve = this.bytesPerPeriod = bandwidthPerSec*period/1000;
      this.periodExtension = period*3;
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

  private class BlockSender implements java.io.Closeable {
    private Block block; // the block to read from
    private DataInputStream blockIn; // data strean
    private DataInputStream checksumIn; // checksum datastream
    private DataChecksum checksum; // checksum stream
    private long offset; // starting position to read
    private long endOffset; // ending position
    private byte buf[]; // buffer to store data read from the block file & crc
    private int bytesPerChecksum; // chunk size
    private int checksumSize; // checksum size
    private boolean corruptChecksumOk; // if need to verify checksum
    private boolean chunkOffsetOK; // if need to send chunk offset

    private Throttler throttler;
    private DataOutputStream out;

    BlockSender(Block block, long startOffset, long length,
        boolean corruptChecksumOk, boolean chunkOffsetOK) throws IOException {

      try {
        this.block = block;
        this.chunkOffsetOK = chunkOffsetOK;
        this.corruptChecksumOk = corruptChecksumOk;


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

        bytesPerChecksum = checksum.getBytesPerChecksum();
        checksumSize = checksum.getChecksumSize();

        if (length < 0) {
          length = data.getLength(block);
        }

        endOffset = data.getLength(block);
        if (startOffset < 0 || startOffset > endOffset
            || (length + startOffset) > endOffset) {
          String msg = " Offset " + startOffset + " and length " + length
          + " don't match block " + block + " ( blockLen " + endOffset + " )";
          LOG.warn(dnRegistration + ":sendBlock() : " + msg);
          throw new IOException(msg);
        }

        buf = new byte[bytesPerChecksum + checksumSize];
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
        InputStream blockInStream = data.getBlockInputStream(block, offset); // seek to offset
        blockIn = new DataInputStream(new BufferedInputStream(blockInStream, BUFFER_SIZE));
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
    
    private int sendChunk()
        throws IOException {
      int len = (int) Math.min(endOffset - offset, bytesPerChecksum);
      if (len == 0)
        return 0;
      blockIn.readFully(buf, 0, len);

      if (checksumSize > 0 && checksumIn != null) {
        try {
          checksumIn.readFully(buf, len, checksumSize);
        } catch (IOException e) {
          LOG.warn(" Could not read checksum for data at offset " + offset
              + " for block " + block + " got : "
              + StringUtils.stringifyException(e));
          IOUtils.closeStream(checksumIn);
          checksumIn = null;
          if (corruptChecksumOk) {
            // Just fill the array with zeros.
            Arrays.fill(buf, len, len + checksumSize, (byte) 0);
          } else {
            throw e;
          }
        }
      }

      out.writeInt(len);
      out.write(buf, 0, len + checksumSize);

      if (throttler != null) { // rebalancing so throttle
        throttler.throttle(len + checksumSize + 4);
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

      long totalRead = 0;
      try {
        checksum.writeHeader(out);
        if ( chunkOffsetOK ) {
          out.writeLong( offset );
        }

        while (endOffset > offset) {
          // Write one data chunk per loop.
          long len = sendChunk();
          offset += len;
          totalRead += len + checksumSize;
        }
        out.writeInt(0); // mark the end of block
        out.flush();
      } finally {
        close();
      }

      return totalRead;
    }
  }

  /* A class that receives a block and wites to its own disk, meanwhile
   * may copies it to another site. If a throttler is provided,
   * streaming throttling is also supported. 
   * */
  private class BlockReceiver implements java.io.Closeable {
    private Block block; // the block to receive
    private DataInputStream in = null; // from where data are read
    private DataChecksum checksum; // from where chunks of a block can be read
    private DataOutputStream out = null; // to block file at local disk
    private DataOutputStream checksumOut = null; // to crc file at local disk
    private int bytesPerChecksum;
    private int checksumSize;
    private byte buf[];
    private long offset;
    final private String inAddr;
    private String mirrorAddr;
    private DataOutputStream mirrorOut;
    private Throttler throttler;
    private int lastLen = -1;
    private int curLen = -1;

    BlockReceiver(Block block, DataInputStream in, String inAddr)
        throws IOException {
      try{
        this.block = block;
        this.in = in;
        this.inAddr = inAddr;
        this.checksum = DataChecksum.newDataChecksum(in);
        this.bytesPerChecksum = checksum.getBytesPerChecksum();
        this.checksumSize = checksum.getChecksumSize();
        this.buf = new byte[bytesPerChecksum + checksumSize];

        //
        // Open local disk out
        //
        FSDataset.BlockWriteStreams streams = data.writeToBlock(block);
        this.out = new DataOutputStream(new BufferedOutputStream(
          streams.dataOut, BUFFER_SIZE));
        this.checksumOut = new DataOutputStream(new BufferedOutputStream(
          streams.checksumOut, BUFFER_SIZE));
      } catch(IOException ioe) {
        IOUtils.closeStream(this);
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
    
    /* receive a chunk: write it to disk & mirror it to another stream */
    private void receiveChunk( int len ) throws IOException {
      if (len <= 0 || len > bytesPerChecksum) {
        throw new IOException("Got wrong length during writeBlock(" + block
            + ") from " + inAddr + " at offset " + offset + ": " + len
            + " expected <= " + bytesPerChecksum);
      }

      if (lastLen > 0 && lastLen != bytesPerChecksum) {
        throw new IOException("Got wrong length during receiveBlock(" + block
          + ") from " + inAddr + " : " + " got " + lastLen + " instead of "
          + bytesPerChecksum);
      }

      lastLen = curLen;
      curLen = len;

      in.readFully(buf, 0, len + checksumSize);

      /*
       * Verification is not included in the initial design. For now, it at
       * least catches some bugs. Later, we can include this after showing that
       * it does not affect performance much.
       */
      checksum.update(buf, 0, len);

      if (!checksum.compare(buf, len)) {
        throw new IOException("Unexpected checksum mismatch "
            + "while writing " + block + " from " + inAddr);
      }

      checksum.reset();

      // First write to remote node before writing locally.
      if (mirrorOut != null) {
        try {
          mirrorOut.writeInt(len);
          mirrorOut.write(buf, 0, len + checksumSize);
        } catch (IOException ioe) {
          LOG.info(dnRegistration + ":Exception writing to mirror " + mirrorAddr + "\n"
              + StringUtils.stringifyException(ioe));
          //
          // If stream-copy fails, continue
          // writing to disk. We shouldn't
          // interrupt client write.
          //
          mirrorOut = null;
        }
      }

      try {
        out.write(buf, 0, len);
        // Write checksum
        checksumOut.write(buf, len, checksumSize);
        myMetrics.wroteBytes(len);
      } catch (IOException iex) {
        checkDiskError(iex);
        throw iex;
      }

      if (throttler != null) { // throttle I/O
        throttler.throttle(len + checksumSize + 4);
      }
    }

    public void receiveBlock(DataOutputStream mirrorOut,
        String mirrorAddr, Throttler throttler) throws IOException {

        this.mirrorOut = mirrorOut;
        this.mirrorAddr = mirrorAddr;
        this.throttler = throttler;

      /*
       * We need an estimate for block size to check if the disk partition has
       * enough space. For now we just increment FSDataset.reserved by
       * configured dfs.block.size Other alternative is to include the block
       * size in the header sent by DFSClient.
       */

      try {
        // write data chunk header
        checksumOut.writeShort(FSDataset.METADATA_VERSION);
        checksum.writeHeader(checksumOut);
        if (mirrorOut != null) {
          checksum.writeHeader(mirrorOut);
          this.mirrorAddr = mirrorAddr;
        }

        int len = in.readInt();
        while (len != 0) {
          receiveChunk( len );
          offset += len;
          len = in.readInt();
        }

        // flush the mirror out
        if (mirrorOut != null) {
          mirrorOut.writeInt(0); // mark the end of the stream
          mirrorOut.flush();
        }

        // close the block/crc files
        close();

        // Finalize the block. Does this fsync()?
        block.setNumBytes(offset);
        data.finalizeBlock(block);
        myMetrics.wroteBlocks(1);
      } catch (IOException ioe) {
        IOUtils.closeStream(this);
        throw ioe;
      }
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
        sock = new Socket();  
        sock.connect(curTarget, READ_TIMEOUT);
        sock.setSoTimeout(targets.length*READ_TIMEOUT);

        out = new DataOutputStream(new BufferedOutputStream(
            sock.getOutputStream(), BUFFER_SIZE));
        blockSender = new BlockSender(b, 0, -1, false, false);

        //
        // Header info
        //
        out.writeShort(DATA_TRANFER_VERSION);
        out.writeByte(OP_WRITE_BLOCK);
        out.writeLong(b.getBlockId());
        // write targets
        out.writeInt(targets.length - 1);
        for (int i = 1; i < targets.length; i++) {
          targets[i].write(out);
        }
        // send data & checksum
        blockSender.sendBlock(out, null);

        
        // check the response
        receiveResponse(sock);
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
        
    // wait for dataXceiveServer to terminate
    try {
      this.dataXceiveServer.join();
    } catch (InterruptedException ie) {
    }
        
    LOG.info(dnRegistration + ":Finishing DataNode in: "+data);
  }
    
  /** Start datanode daemon.
   */
  public static DataNode run(Configuration conf) throws IOException {
    String[] dataDirs = conf.getStrings("dfs.data.dir");
    DataNode dn = makeInstance(dataDirs, conf);
    if (dn != null) {
      dn.dataNodeThread = new Thread(dn, "DataNode: [" +
                                  StringUtils.arrayToString(dataDirs) + "]");
      dn.dataNodeThread.setDaemon(true); // needed for JUnit testing
      dn.dataNodeThread.start();
    }
    return dn;
  }

  /** Start a single datanode daemon and wait for it to finish.
   *  If this thread is specifically interrupted, it will stop waiting.
   */
  static DataNode createDataNode(String args[],
                                 Configuration conf) throws IOException {
    if (conf == null)
      conf = new Configuration();
    if (!parseArguments(args, conf)) {
      printUsage();
      return null;
    }
    return run(conf);
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
    System.err.println("           [-r, --rack <network location>] |");
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
    String networkLoc = null;
    for(int i=0; i < argsLen; i++) {
      String cmd = args[i];
      if ("-r".equalsIgnoreCase(cmd) || "--rack".equalsIgnoreCase(cmd)) {
        if (i==args.length-1)
          return false;
        networkLoc = args[++i];
        if (networkLoc.startsWith("-"))
          return false;
      } else if ("-rollback".equalsIgnoreCase(cmd)) {
        startOpt = StartupOption.ROLLBACK;
      } else if ("-regular".equalsIgnoreCase(cmd)) {
        startOpt = StartupOption.REGULAR;
      } else
        return false;
    }
    if (networkLoc != null)
      conf.set("dfs.datanode.rack", NodeBase.normalize(networkLoc));
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

  /* Get the network location by running a script configured in conf */
  private static String getNetworkLoc(Configuration conf) 
    throws IOException {
    String locScript = conf.get("dfs.network.script");
    if (locScript == null) 
      return NetworkTopology.DEFAULT_RACK;

    LOG.info("Starting to run script to get datanode network location");
    Process p = Runtime.getRuntime().exec(locScript);
    StringBuffer networkLoc = new StringBuffer();
    final BufferedReader inR = new BufferedReader(
                                                  new InputStreamReader(p.getInputStream()));
    final BufferedReader errR = new BufferedReader(
                                                   new InputStreamReader(p.getErrorStream()));

    // read & log any error messages from the running script
    Thread errThread = new Thread() {
        @Override
        public void start() {
          try {
            String errLine = errR.readLine();
            while(errLine != null) {
              LOG.warn("Network script error: "+errLine);
              errLine = errR.readLine();
            }
          } catch(IOException e) {
                    
          }
        }
      };
    try {
      errThread.start();
            
      // fetch output from the process
      String line = inR.readLine();
      while(line != null) {
        networkLoc.append(line);
        line = inR.readLine();
      }
      try {
        // wait for the process to finish
        int returnVal = p.waitFor();
        // check the exit code
        if (returnVal != 0) {
          throw new IOException("Process exits with nonzero status: "+locScript);
        }
      } catch (InterruptedException e) {
        throw new IOException(e.getMessage());
      } finally {
        try {
          // make sure that the error thread exits
          errThread.join();
        } catch (InterruptedException je) {
          LOG.warn(StringUtils.stringifyException(je));
        }
      }
    } finally {
      // close in & error streams
      try {
        inR.close();
      } catch (IOException ine) {
        throw ine;
      } finally {
        errR.close();
      }
    }

    return networkLoc.toString();
  }
  
  /**
   * This methods  arranges for the data node to send the block report at the next heartbeat.
   */
  public void scheduleBlockReport() {
    lastHeartbeat=0;
    lastBlockReport=0;
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
