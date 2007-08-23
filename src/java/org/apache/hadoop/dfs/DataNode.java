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

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.ipc.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.metrics.MetricsUtil;
import org.apache.hadoop.net.DNS;
import org.apache.hadoop.net.NodeBase;
import org.apache.hadoop.util.*;
import org.apache.hadoop.util.DiskChecker.DiskErrorException;
import org.apache.hadoop.util.DiskChecker.DiskOutOfSpaceException;
import org.apache.hadoop.mapred.StatusHttpServer;
import org.apache.hadoop.net.NetworkTopology;
import org.apache.hadoop.dfs.BlockCommand;
import org.apache.hadoop.dfs.DatanodeProtocol;
import java.io.*;
import java.net.*;
import java.util.*;
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
   * Util method to build socket addr from either:
   *   <host>:<post>
   *   <fs>://<host>:<port>/<path>
   */
  public static InetSocketAddress createSocketAddr(String target
                                                   ) throws IOException {
    int colonIndex = target.indexOf(':');
    if (colonIndex < 0) {
      throw new RuntimeException("Not a host:port pair: " + target);
    }
    String hostname;
    int port;
    if (!target.contains("/")) {
      // must be the old style <host>:<port>
      hostname = target.substring(0, colonIndex);
      port = Integer.parseInt(target.substring(colonIndex + 1));
    } else {
      // a new uri
      URI addr = new Path(target).toUri();
      hostname = addr.getHost();
      port = addr.getPort();
    }

    return new InetSocketAddress(hostname, port);
  }

  DatanodeProtocol namenode = null;
  FSDataset data = null;
  DatanodeRegistration dnRegistration = null;
  private String networkLoc;
  volatile boolean shouldRun = true;
  LinkedList<Block> receivedBlockList = new LinkedList<Block>();
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
  private static Thread dataNodeThread = null;
  String machineName;
  int defaultBytesPerChecksum = 512;
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
    
  void startDataNode(Configuration conf, 
                     AbstractList<File> dataDirs
                     ) throws IOException {
    // use configured nameserver & interface to get local hostname
    machineName = DNS.getDefaultHost(
                                     conf.get("dfs.datanode.dns.interface","default"),
                                     conf.get("dfs.datanode.dns.nameserver","default"));
    InetSocketAddress nameNodeAddr = createSocketAddr(
                                                      conf.get("fs.default.name", "local"));
    
    this.defaultBytesPerChecksum = 
       Math.max(conf.getInt("io.bytes.per.checksum", 512), 1); 
    
    int tmpPort = conf.getInt("dfs.datanode.port", 50010);
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

    // read storage info, lock data dirs and transition fs state if necessary
    StartupOption startOpt = getStartupOption(conf);
    assert startOpt != null : "Startup option must be set.";
    storage.recoverTransitionRead(nsInfo, dataDirs, startOpt);
    // adjust
    this.dnRegistration.setStorageInfo(storage);
      
    // initialize data node internal structure
    this.data = new FSDataset(storage, conf);
      
    // find free port
    ServerSocket ss = null;
    String bindAddress = conf.get("dfs.datanode.bindAddress", "0.0.0.0");
    while (ss == null) {
      try {
        ss = new ServerSocket(tmpPort, 0, InetAddress.getByName(bindAddress));
        LOG.info("Opened server at " + tmpPort);
      } catch (IOException ie) {
        LOG.info("Could not open server at " + tmpPort + ", trying new port");
        tmpPort++;
      }
    }
    // adjust machine name with the actual port
    this.dnRegistration.setName(machineName + ":" + tmpPort);
      
    this.dataXceiveServer = new Daemon(new DataXceiveServer(ss));

    long blockReportIntervalBasis =
      conf.getLong("dfs.blockreport.intervalMsec", BLOCKREPORT_INTERVAL);
    this.blockReportInterval =
      blockReportIntervalBasis - new Random().nextInt((int)(blockReportIntervalBasis/10));
    this.heartBeatInterval = conf.getLong("dfs.heartbeat.interval", HEARTBEAT_INTERVAL) * 1000L;
    DataNode.nameNodeAddr = nameNodeAddr;

    //create a servlet to serve full-file content
    int infoServerPort = conf.getInt("dfs.datanode.info.port", 50075);
    String infoServerBindAddress = conf.get("dfs.datanode.info.bindAddress", "0.0.0.0");
    this.infoServer = new StatusHttpServer("datanode", infoServerBindAddress, infoServerPort, true);
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
        synchronized(receivedBlockList) {
          if (receivedBlockList.size() > 0) {
            //
            // Send newly-received blockids to namenode
            //
            blockArray = receivedBlockList.toArray(new Block[receivedBlockList.size()]);
          }
        }
        if (blockArray != null) {
          namenode.blockReceived(dnRegistration, blockArray);
          synchronized (receivedBlockList) {
            for(Block b: blockArray) {
              receivedBlockList.remove(b);
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
      lastHeartbeat=0;
      lastBlockReport=0;
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
        LOG.info("Starting thread to transfer block " + blocks[i] + " to " + xferTargets[i]);
        new Daemon(new DataTransfer(xferTargets[i], blocks[i])).start();
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
          //s.setSoTimeout(READ_TIMEOUT);
          xceiverCount.incr();
          new Daemon(new DataXceiver(s)).start();
        }
        ss.close();
      } catch (IOException ie) {
        LOG.info("Exiting DataXceiveServer due to " + ie.toString());
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
      try {
        DataInputStream in = new DataInputStream(
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
        default:
          throw new IOException("Unknown opcode " + op + "in data stream");
        }
       } catch (Throwable t) {
        LOG.error("DataXceiver: " + StringUtils.stringifyException(t));
      } finally {
        try {
          xceiverCount.decr();
          LOG.debug("Number of active connections is: "+xceiverCount);
          s.close();
        } catch (IOException ie2) {
        }
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
      Block block = new Block( blockId, 0 );

      long startOffset = in.readLong();
      long length = in.readLong();
      
      try {
        //XXX Buffered output stream?
        long read = sendBlock(s, block, startOffset, length, null );
        myMetrics.readBytes((int)read);
        myMetrics.readBlocks(1);
        LOG.info("Served block " + block + " to " + s.getInetAddress());
      } catch ( SocketException ignored ) {
        // Its ok for remote side to close the connection anytime.
        myMetrics.readBlocks(1);
      } catch ( IOException ioe ) {
        /* What exactly should we do here?
         * Earlier version shutdown() datanode if there is disk error.
         */
        LOG.warn( "Got exception while serving " + block + " to " +
                  s.getInetAddress() + ": " + 
                  StringUtils.stringifyException(ioe) );
        throw ioe;
      }
    }

    /**
     * Write a block to disk.
     * @param in The stream to read from
     * @throws IOException
     */
    private void writeBlock(DataInputStream in) throws IOException {
      //
      // Read in the header
      //
      DataOutputStream reply = new DataOutputStream(s.getOutputStream());
      DataOutputStream out = null;
      DataOutputStream checksumOut = null;
      Socket mirrorSock = null;
      DataOutputStream mirrorOut = null;
      DataInputStream mirrorIn = null;
      
      try {
        /* We need an estimate for block size to check if the 
         * disk partition has enough space. For now we just increment
         * FSDataset.reserved by configured dfs.block.size
         * Other alternative is to include the block size in the header
         * sent by DFSClient.
         */
        Block block = new Block( in.readLong(), 0 );
        int numTargets = in.readInt();
        if ( numTargets < 0 ) {
          throw new IOException("Mislabelled incoming datastream.");
        }
        DatanodeInfo targets[] = new DatanodeInfo[numTargets];
        for (int i = 0; i < targets.length; i++) {
          DatanodeInfo tmp = new DatanodeInfo();
          tmp.readFields(in);
          targets[i] = tmp;
        }
            
        DataChecksum checksum = DataChecksum.newDataChecksum( in );

        //
        // Open local disk out
        //
        FSDataset.BlockWriteStreams streams = data.writeToBlock( block );
        out = new DataOutputStream(
                  new BufferedOutputStream(streams.dataOut, BUFFER_SIZE));        
        checksumOut = new DataOutputStream(
                  new BufferedOutputStream(streams.checksumOut, BUFFER_SIZE));
        
        InetSocketAddress mirrorTarget = null;
        String mirrorNode = null;
        //
        // Open network conn to backup machine, if 
        // appropriate
        //
        if (targets.length > 0) {
          // Connect to backup machine
          mirrorNode = targets[0].getName();
          mirrorTarget = createSocketAddr(mirrorNode);
          try {
            mirrorSock = new Socket();
            mirrorSock.connect(mirrorTarget, READ_TIMEOUT);
            mirrorSock.setSoTimeout(READ_TIMEOUT);
            mirrorOut = new DataOutputStream( 
                        new BufferedOutputStream(mirrorSock.getOutputStream(),
                                                 BUFFER_SIZE));
            mirrorIn = new DataInputStream( mirrorSock.getInputStream() );
            //Copied from DFSClient.java!
            mirrorOut.writeShort( DATA_TRANFER_VERSION );
            mirrorOut.write( OP_WRITE_BLOCK );
            mirrorOut.writeLong( block.getBlockId() );
            mirrorOut.writeInt( targets.length - 1 );
            for ( int i = 1; i < targets.length; i++ ) {
              targets[i].write( mirrorOut );
            }
            checksum.writeHeader( mirrorOut );
            myMetrics.replicatedBlocks(1);
          } catch (IOException ie) {
            if (mirrorOut != null) {
              LOG.info("Exception connecting to mirror " + mirrorNode 
                       + "\n" + StringUtils.stringifyException(ie));
              mirrorOut = null;
            }
          }
        }
        
        // XXX The following code is similar on both sides...
        
        int bytesPerChecksum = checksum.getBytesPerChecksum();
        int checksumSize = checksum.getChecksumSize();
        byte buf[] = new byte[ bytesPerChecksum + checksumSize ];
        long blockLen = 0;
        long lastOffset = 0;
        long lastLen = 0;
        short status = -1;
        boolean headerWritten = false;
        
        while ( true ) {
          // Read one data chunk in each loop.
          
          long offset = lastOffset + lastLen;
          int len = (int) in.readInt();
          if ( len < 0 || len > bytesPerChecksum ) {
            LOG.warn( "Got wrong length during writeBlock(" +
                      block + ") from " + s.getRemoteSocketAddress() +
                      " at offset " + offset + ": " + len + 
                      " expected <= " + bytesPerChecksum );
            status = OP_STATUS_ERROR;
            break;
          }

          in.readFully( buf, 0, len + checksumSize );
          
          if ( len > 0 && checksumSize > 0 ) {
            /*
             * Verification is not included in the initial design.
             * For now, it at least catches some bugs. Later, we can 
             * include this after showing that it does not affect 
             * performance much.
             */
            checksum.update( buf, 0, len  );
            
            if ( ! checksum.compare( buf, len ) ) {
              throw new IOException( "Unexpected checksum mismatch " +
                                     "while writing " + block + 
                                     " from " +
                                     s.getRemoteSocketAddress() );
            }
            
            checksum.reset();
          }

          // First write to remote node before writing locally.
          if (mirrorOut != null) {
            try {
              mirrorOut.writeInt( len );
              mirrorOut.write( buf, 0, len + checksumSize );
              if (len == 0) {
                mirrorOut.flush();
              }
            } catch (IOException ioe) {
              LOG.info( "Exception writing to mirror " + mirrorNode + 
                        "\n" + StringUtils.stringifyException(ioe) );
              //
              // If stream-copy fails, continue 
              // writing to disk.  We shouldn't 
              // interrupt client write.
              //
              mirrorOut = null;
            }
          }

          try {
            if ( !headerWritten ) { 
              // First DATA_CHUNK. 
              // Write the header even if checksumSize is 0.
              checksumOut.writeShort( FSDataset.METADATA_VERSION );
              checksum.writeHeader( checksumOut );
              headerWritten = true;
            }
            
            if ( len > 0 ) {
              out.write( buf, 0, len );
              // Write checksum
              checksumOut.write( buf, len, checksumSize );
              myMetrics.wroteBytes( len );
            } else {
              /* Should we sync() files here? It can add many millisecs of
               * latency. We did not sync before HADOOP-1134 either.
               */ 
              out.close();
              out = null;
              checksumOut.close();
              checksumOut = null;
            }
            
          } catch (IOException iex) {
            checkDiskError(iex);
            throw iex;
          }
          
          if ( len == 0 ) {

            // We already have one successful write here. Should we
            // wait for response from next target? We will skip for now.

            block.setNumBytes( blockLen );
            
            //Does this fsync()?
            data.finalizeBlock( block );
            myMetrics.wroteBlocks(1);
            
            status = OP_STATUS_SUCCESS;
            
            break;
          }
          
          if ( lastLen > 0 && lastLen != bytesPerChecksum ) {
            LOG.warn( "Got wrong length during writeBlock(" +
                      block + ") from " + s.getRemoteSocketAddress() +
                      " : " + " got " + lastLen + " instead of " +
                      bytesPerChecksum );
            status = OP_STATUS_ERROR;
            break;
          }
          
          lastOffset = offset;
          lastLen = len;
          blockLen += len;
        }
        // done with reading the data.
        
        if ( status == OP_STATUS_SUCCESS ) {
          /* Informing the name node could take a long long time!
             Should we wait till namenode is informed before responding
             with success to the client? For now we don't.
          */
          synchronized ( receivedBlockList ) {
            receivedBlockList.add( block );
            receivedBlockList.notifyAll();
          }
          
          String msg = "Received block " + block + " from " + 
                       s.getInetAddress();
          
          if ( mirrorOut != null ) {
            //Wait for the remote reply
            mirrorOut.flush();
            short result = OP_STATUS_ERROR; 
            try {
              result = mirrorIn.readShort();
            } catch ( IOException ignored ) {}

            msg += " and " +  (( result != OP_STATUS_SUCCESS ) ? 
                                "failed to mirror to " : " mirrored to ") +
                   mirrorTarget;
            
            mirrorOut = null;
          }
          
          LOG.info(msg);
        }
            
        if ( status >= 0 ) {
          try {
            reply.writeShort( status );
            reply.flush();
          } catch ( IOException ignored ) {}
        }
        
      } finally {
        try {
          if ( out != null )
            out.close();
          if ( checksumOut != null )
            checksumOut.close();
          if ( mirrorSock != null )
            mirrorSock.close();
        } catch (IOException iex) {
          shutdown();
          throw iex;
        }
      }
    }
    
    /**
     * Reads the metadata and sends the data in one 'DATA_CHUNK'
     * @param in
     */
    void readMetadata(DataInputStream in) throws IOException {
      
      Block block = new Block( in.readLong(), 0 );
      InputStream checksumIn = null;
      DataOutputStream out = null;
      
      try {
        File blockFile = data.getBlockFile( block );
        File checksumFile = FSDataset.getMetaFile( blockFile );
        checksumIn = new FileInputStream(checksumFile);

        long fileSize = checksumFile.length();
        if (fileSize >= 1L<<31 || fileSize <= 0) {
          throw new IOException("Unexpected size for checksumFile " +
                                checksumFile);
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
        IOUtils.closeStream(checksumIn);
      }
    }
  }

  /** sendBlock() is used to read block and its metadata and stream
   * the data to either a client or to another datanode.
   * If argument targets is null, then it is assumed to be replying
   * to a client request (OP_BLOCK_READ). Otherwise, we are replicating
   * to another datanode.
   * 
   * returns total bytes reads, including crc.
   */
  long sendBlock(Socket sock, Block block,
                 long startOffset, long length, DatanodeInfo targets[] )
                 throws IOException {
    DataOutputStream out = new DataOutputStream(
                           new BufferedOutputStream(sock.getOutputStream(), 
                                                    BUFFER_SIZE));
    RandomAccessFile blockInFile = null;
    DataInputStream blockIn = null;
    DataInputStream checksumIn = null;
    long totalRead = 0;    

    /* XXX This will affect inter datanode transfers during 
     * a CRC upgrade. There should not be any replication
     * during crc upgrade since we are in safe mode, right?
     */    
    boolean corruptChecksumOk = targets == null; 

    try {
      File blockFile = data.getBlockFile( block );
      blockInFile = new RandomAccessFile(blockFile, "r");

      File checksumFile = FSDataset.getMetaFile( blockFile );
      DataChecksum checksum = null;

      if ( !corruptChecksumOk || checksumFile.exists() ) {
        checksumIn = new DataInputStream(
                     new BufferedInputStream(new FileInputStream(checksumFile),
                                             BUFFER_SIZE));
          
        //read and handle the common header here. For now just a version
        short version = checksumIn.readShort();
        if ( version != FSDataset.METADATA_VERSION ) {
          LOG.warn( "Wrong version (" + version + 
                    ") for metadata file for " + block + " ignoring ..." );
        }
        checksum = DataChecksum.newDataChecksum( checksumIn ) ;
      } else {
        LOG.warn( "Could not find metadata file for " + block );
        // This only decides the buffer size. Use BUFFER_SIZE?
        checksum = DataChecksum.newDataChecksum( DataChecksum.CHECKSUM_NULL,
                                                 16*1024 );
      }

      int bytesPerChecksum = checksum.getBytesPerChecksum();
      int checksumSize = checksum.getChecksumSize();
      
      if (length < 0) {
        length = data.getLength(block);
      }

      long endOffset = data.getLength( block );
      if ( startOffset < 0 || startOffset > endOffset ||
          (length + startOffset) > endOffset ) {
        String msg = " Offset " + startOffset + " and length " + length + 
                     " don't match block " + block +  " ( blockLen " + 
                     endOffset + " )"; 
        LOG.warn( "sendBlock() : " + msg );
        if ( targets != null ) {
          throw new IOException(msg);
        } else {
          out.writeShort( OP_STATUS_ERROR_INVALID );
          return totalRead;
        }
      }

      byte buf[] = new byte[ bytesPerChecksum + checksumSize ];
      long offset = (startOffset - (startOffset % bytesPerChecksum));
      if ( length >= 0 ) {
        // Make sure endOffset points to end of a checksumed chunk. 
        long tmpLen = startOffset + length + (startOffset - offset);
        if ( tmpLen % bytesPerChecksum != 0 ) { 
          tmpLen += ( bytesPerChecksum - tmpLen % bytesPerChecksum );
        }
        if ( tmpLen < endOffset ) {
          endOffset = tmpLen;
        }
      }

      // seek to the right offsets
      if ( offset > 0 ) {
        long checksumSkip = ( offset / bytesPerChecksum ) * checksumSize ;
        blockInFile.seek(offset);
        if (checksumSkip > 0) {
          //Should we use seek() for checksum file as well?
          IOUtils.skipFully(checksumIn, checksumSkip);
        }
      }
      
      blockIn = new DataInputStream(new BufferedInputStream(
                                      new FileInputStream(blockInFile.getFD()), 
                                      BUFFER_SIZE));
      
      if ( targets != null ) {
        //
        // Header info
        //
        out.writeShort( DATA_TRANFER_VERSION );
        out.writeByte( OP_WRITE_BLOCK );
        out.writeLong( block.getBlockId() );
        out.writeInt(targets.length-1);
        for (int i = 1; i < targets.length; i++) {
          targets[i].write( out );
        }
      } else {
        out.writeShort( OP_STATUS_SUCCESS );          
      }

      checksum.writeHeader( out );
      
      if ( targets == null ) {
        out.writeLong( offset );
      }
      
      while ( endOffset >= offset ) {
        // Write one data chunk per loop.
        int len = (int) Math.min( endOffset - offset, bytesPerChecksum );
        if ( len > 0 ) {
          blockIn.readFully( buf, 0, len );
          totalRead += len;
          
          if ( checksumSize > 0 && checksumIn != null ) {
            try {
              checksumIn.readFully( buf, len, checksumSize );
              totalRead += checksumSize;
            } catch ( IOException e ) {
              LOG.warn( " Could not read checksum for data at offset " +
                        offset + " for block " + block + " got : " + 
                        StringUtils.stringifyException(e) );
              IOUtils.closeStream( checksumIn );
              checksumIn = null;
              if ( corruptChecksumOk ) {
                // Just fill the array with zeros.
                Arrays.fill( buf, len, len + checksumSize, (byte)0 );
              } else {
                throw e;
              }
            }
          }
        }

        out.writeInt( len );
        out.write( buf, 0, len + checksumSize );
        
        if ( offset == endOffset ) {
          out.flush();
          // We are not waiting for response from target.
          break;
        }
        offset += len;
      }
    } finally {
      IOUtils.closeStream( blockInFile );
      IOUtils.closeStream( checksumIn );
      IOUtils.closeStream( blockIn );
      IOUtils.closeStream( out );
    }
    
    return totalRead;
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
      
      try {
        InetSocketAddress curTarget = 
          createSocketAddr(targets[0].getName());
        sock = new Socket();  
        sock.connect(curTarget, READ_TIMEOUT);
        sock.setSoTimeout(READ_TIMEOUT);
        sendBlock( sock, b, 0, -1, targets );
        LOG.info( "Transmitted block " + b + " to " + curTarget );

      } catch ( IOException ie ) {
        LOG.warn( "Failed to transfer " + b + " to " + 
                  targets[0].getName() + " got " + 
                  StringUtils.stringifyException( ie ) );
      } finally {
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
    LOG.info("In DataNode.run, data = " + data);

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
        
    LOG.info("Finishing DataNode in: "+data);
  }
    
  /** Start datanode daemon.
   */
  public static DataNode run(Configuration conf) throws IOException {
    String[] dataDirs = conf.getStrings("dfs.data.dir");
    DataNode dn = makeInstance(dataDirs, conf);
    if (dn != null) {
      dataNodeThread = new Thread(dn, "DataNode: [" +
                                  StringUtils.arrayToString(dataDirs) + "]");
      dataNodeThread.setDaemon(true); // needed for JUnit testing
      dataNodeThread.start();
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
