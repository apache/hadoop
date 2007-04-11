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

import java.io.*;
import java.net.*;
import java.util.*;
import org.apache.hadoop.metrics.MetricsContext;
import org.apache.hadoop.metrics.MetricsRecord;
import org.apache.hadoop.metrics.Updater;

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
 * @author Mike Cafarella
 **********************************************************/
public class DataNode implements FSConstants, Runnable {
    public static final Log LOG = LogFactory.getLog("org.apache.hadoop.dfs.DataNode");
    //
    // REMIND - mjc - I might bring "maxgigs" back so user can place 
    // artificial  limit on space
    //private static final long GIGABYTE = 1024 * 1024 * 1024;
    //private static long numGigs = Configuration.get().getLong("dfs.datanode.maxgigs", 100);
    //

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
    Vector receivedBlockList = new Vector();
    int xmitsInProgress = 0;
    Daemon dataXceiveServer = null;
    long blockReportInterval;
    long lastBlockReport = 0;
    long lastHeartbeat = 0;
    long heartBeatInterval;
    private DataStorage storage = null;
    private StatusHttpServer infoServer = null;
    private DataNodeMetrics myMetrics = new DataNodeMetrics();
    private static InetSocketAddress nameNodeAddr;
    private static DataNode datanodeObject = null;
    private static Thread dataNodeThread = null;
    String machineName;

    private class DataNodeMetrics implements Updater {
      private final MetricsRecord metricsRecord;
      private int bytesWritten = 0;
      private int bytesRead = 0;
      private int blocksWritten = 0;
      private int blocksRead = 0;
      private int blocksReplicated = 0;
      private int blocksRemoved = 0;
      
      DataNodeMetrics() {
        MetricsContext context = MetricsUtil.getContext("dfs");
        metricsRecord = MetricsUtil.createRecord(context, "datanode");
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
    DataNode( Configuration conf, 
              AbstractList<File> dataDirs ) throws IOException {
      try {
        startDataNode( conf, dataDirs );
      } catch (IOException ie) {
        shutdown();
        throw ie;
      }
    }
    
    void startDataNode( Configuration conf, 
                        AbstractList<File> dataDirs
                       ) throws IOException {
      // use configured nameserver & interface to get local hostname
      machineName = DNS.getDefaultHost(
          conf.get("dfs.datanode.dns.interface","default"),
          conf.get("dfs.datanode.dns.nameserver","default"));
      InetSocketAddress nameNodeAddr = createSocketAddr(
          conf.get("fs.default.name", "local"));

      // connect to name node
      this.namenode = (DatanodeProtocol) 
          RPC.waitForProxy(DatanodeProtocol.class,
                           DatanodeProtocol.versionID,
                           nameNodeAddr, 
                           conf);
      // get version and id info from the name-node
      NamespaceInfo nsInfo = handshake();

      // read storage info, lock data dirs and transition fs state if necessary
      StartupOption startOpt = (StartupOption)conf.get( "dfs.datanode.startup", 
                                                        StartupOption.REGULAR );
      assert startOpt != null : "Startup option must be set.";
      storage = new DataStorage();
      storage.recoverTransitionRead( nsInfo, dataDirs, startOpt );
      
      // initialize data node internal structure
      this.data = new FSDataset( storage, conf );
      
      // find free port
      ServerSocket ss = null;
      int tmpPort = conf.getInt("dfs.datanode.port", 50010);
      String bindAddress = conf.get("dfs.datanode.bindAddress", "0.0.0.0");
      while (ss == null) {
        try {
          ss = new ServerSocket(tmpPort,0,InetAddress.getByName(bindAddress));
          LOG.info("Opened server at " + tmpPort);
        } catch (IOException ie) {
          LOG.info("Could not open server at " + tmpPort + ", trying new port");
          tmpPort++;
        }
      }
      // construct registration
      this.dnRegistration = new DatanodeRegistration( 
                                    machineName + ":" + tmpPort, 
                                    -1,   // info port determined later
                                    storage );
      
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
      this.dnRegistration.infoPort = this.infoServer.getPort();
      // get network location
      this.networkLoc = conf.get( "dfs.datanode.rack" );
      if( networkLoc == null )  // exec network script or set the default rack
        networkLoc = getNetworkLoc( conf );
      // register datanode
      register();
      datanodeObject = this;
    }

    private NamespaceInfo handshake() throws IOException {
      NamespaceInfo nsInfo = new NamespaceInfo();
      while (shouldRun) {
        try {
          nsInfo = namenode.versionRequest();
          break;
        } catch( SocketTimeoutException e ) {  // namenode is busy
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
      while( shouldRun ) {
        try {
          // reset name to machineName. Mainly for web interface.
          dnRegistration.name = machineName + ":" + dnRegistration.getPort();
          dnRegistration = namenode.register( dnRegistration, networkLoc );
          break;
        } catch( SocketTimeoutException e ) {  // namenode is busy
          LOG.info("Problem connecting to server: " + getNameNodeAddr());
          try {
            Thread.sleep(1000);
          } catch (InterruptedException ie) {}
        }
      }
      if( storage.getStorageID().equals("") ) {
        storage.setStorageID( dnRegistration.getStorageID());
        storage.writeAll();
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

    void handleDiskError( String errMsgr ) {
        LOG.warn( "DataNode is shutting down.\n" + errMsgr );
        try {
            namenode.errorReport(
                    dnRegistration, DatanodeProtocol.DISK_ERROR, errMsgr);
        } catch( IOException ignored) {              
        }
        shutdown();
    }
    
    private static class Count {
        int value = 0;
        Count(int init) { value = init; }
        synchronized void incr() { value++; }
        synchronized void decr() { value--; }
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
            DatanodeCommand cmd = namenode.sendHeartbeat( dnRegistration, 
                                                      data.getCapacity(), 
                                                      data.getRemaining(), 
                                                      xmitsInProgress,
                                                      xceiverCount.getValue());
            //LOG.info("Just sent heartbeat, with name " + localName);
            lastHeartbeat = now;
            if( ! processCommand( cmd ) )
              continue;
          }

          // send block report
          if (now - lastBlockReport > blockReportInterval) {
            //
            // Send latest blockinfo report if timer has expired.
            // Get back a list of local block(s) that are obsolete
            // and can be safely GC'ed.
            //
            DatanodeCommand cmd = namenode.blockReport( dnRegistration,
                                                        data.getBlockReport());
            processCommand( cmd );
            lastBlockReport = now;
            continue;
          }
            
          // check if there are newly received blocks
          Block [] blockArray=null;
          synchronized( receivedBlockList ) {
            if (receivedBlockList.size() > 0) {
              //
              // Send newly-received blockids to namenode
              //
              blockArray = (Block[]) receivedBlockList.toArray(new Block[receivedBlockList.size()]);
            }
          }
          if( blockArray != null ) {
            namenode.blockReceived( dnRegistration, blockArray );
            synchronized (receivedBlockList) {
              for(Block b: blockArray) {
                receivedBlockList.remove(b);
              }
            }
          }
            
          //
          // There is no work to do;  sleep until hearbeat timer elapses, 
          // or work arrives, and then iterate again.
          //
          long waitTime = heartBeatInterval - (System.currentTimeMillis() - lastHeartbeat);
          synchronized( receivedBlockList ) {
            if (waitTime > 0 && receivedBlockList.size() == 0) {
              try {
                receivedBlockList.wait(waitTime);
              } catch (InterruptedException ie) {
              }
            }
          } // synchronized
        } catch(DiskErrorException e) {
          handleDiskError(e.getLocalizedMessage());
          return;
        } catch( RemoteException re ) {
          String reClass = re.getClassName();
          if( UnregisteredDatanodeException.class.getName().equals( reClass ) ||
              DisallowedDatanodeException.class.getName().equals( reClass )) {
            LOG.warn( "DataNode is shutting down: " + 
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
    private boolean processCommand( DatanodeCommand cmd ) throws IOException {
      if( cmd == null )
        return true;
      switch( cmd.action ) {
      case DNA_TRANSFER:
        //
        // Send a copy of a block to another datanode
        //
        BlockCommand bcmd = (BlockCommand)cmd;
        transferBlocks( bcmd.getBlocks(), bcmd.getTargets() );
        break;
      case DNA_INVALIDATE:
        //
        // Some local block(s) are obsolete and can be 
        // safely garbage-collected.
        //
        Block toDelete[] = ((BlockCommand)cmd).getBlocks();
        data.invalidate(toDelete);
        myMetrics.removedBlocks(toDelete.length);
        break;
      case DNA_SHUTDOWN:
        // shut down the data node
        this.shutdown();
        return false;
      case DNA_REGISTER:
        // namenode requested a registration
        register();
        lastHeartbeat=0;
        lastBlockReport=0;
        break;
      case DNA_FINALIZE:
        storage.finalizeUpgrade();
        break;
      default:
        LOG.warn( "Unknown DatanodeCommand action: " + cmd.action);
      }
      return true;
    }
    
    private void transferBlocks(  Block blocks[], 
                                  DatanodeInfo xferTargets[][] 
                                ) throws IOException {
      for (int i = 0; i < blocks.length; i++) {
        if (!data.isValidBlock(blocks[i])) {
          String errStr = "Can't send invalid block " + blocks[i];
          LOG.info(errStr);
          namenode.errorReport( dnRegistration, 
                                DatanodeProtocol.INVALID_BLOCK, 
                                errStr );
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
                DataInputStream in = new DataInputStream(new BufferedInputStream(s.getInputStream()));
                try {
                    byte op = (byte) in.read();
                    if (op == OP_WRITE_BLOCK) {
                        writeBlock(in);
                    } else if (op == OP_READ_BLOCK || op == OP_READSKIP_BLOCK ||
                        op == OP_READ_RANGE_BLOCK) {
                        readBlock(in, op);
                    } else {
                        while (op >= 0) {
                            System.out.println("Faulty op: " + op);
                            op = (byte) in.read();
                        }
                        throw new IOException("Unknown opcode for incoming data stream");
                    }
                } finally {
                    in.close();
                }
            } catch (Throwable t) {
              LOG.error("DataXCeiver", t);
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
         * @param op OP_READ_BLOCK or OP_READ_SKIPBLOCK
         * @throws IOException
         */
        private void readBlock(DataInputStream in, byte op) throws IOException {
          //
          // Read in the header
          //
          Block b = new Block();
          b.readFields(in);

          long toSkip = 0;
          long endOffset = -1;
          if (op == OP_READSKIP_BLOCK) {
              toSkip = in.readLong();
          } else if (op == OP_READ_RANGE_BLOCK) {
            toSkip = in.readLong();
            endOffset = in.readLong();
          }

          //
          // Open reply stream
          //
          DataOutputStream out = new DataOutputStream(new BufferedOutputStream(s.getOutputStream()));
          try {
              //
              // Write filelen of -1 if error
              //
              if (! data.isValidBlock(b)) {
                  out.writeLong(-1);
              } else {
                  //
                  // Get blockdata from disk
                  //
                  long len = data.getLength(b);
                  if (endOffset < 0) { endOffset = len; }
                  DataInputStream in2 = new DataInputStream(data.getBlockData(b));
                  out.writeLong(len);

                  long amtSkipped = 0;
                  if ((op == OP_READSKIP_BLOCK) || (op == OP_READ_RANGE_BLOCK)) {
                      if (toSkip > len) {
                          toSkip = len;
                      }
                      try {
                          amtSkipped = in2.skip(toSkip);
                      } catch (IOException iex) {
                          shutdown();
                          throw iex;
                      }
                      out.writeLong(amtSkipped);
                  }
                  if (op == OP_READ_RANGE_BLOCK) {
                      if (endOffset > len) {
                        endOffset = len;
                      }
                      out.writeLong(endOffset);
                  }

                  byte buf[] = new byte[BUFFER_SIZE];
                  try {
                    int toRead = (int) (endOffset - amtSkipped + 1);
                      int bytesRead = 0;
                      try {
                          bytesRead = in2.read(buf, 0, Math.min(BUFFER_SIZE, toRead));
                          myMetrics.readBytes(bytesRead);
                      } catch (IOException iex) {
                          shutdown();
                          throw iex;
                      }
                      while (toRead > 0 && bytesRead >= 0) {
                          out.write(buf, 0, bytesRead);
                          toRead -= bytesRead;
                          if (toRead > 0) {
                          try {
                              bytesRead = in2.read(buf, 0, Math.min(BUFFER_SIZE, toRead));
                              myMetrics.readBytes(bytesRead);
                          } catch (IOException iex) {
                              shutdown();
                              throw iex;
                          }
                          }
                      }
                  } catch (SocketException se) {
                      // This might be because the reader
                      // closed the stream early
                  } finally {
                      try {
                          in2.close();
                      } catch (IOException iex) {
                          shutdown();
                          throw iex;
                      }
                  }
              }
              myMetrics.readBlocks(1);
              LOG.info("Served block " + b + " to " + s.getInetAddress());
          } finally {
              out.close();
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
          DataOutputStream reply = 
            new DataOutputStream(new BufferedOutputStream(s.getOutputStream()));
          try {
            boolean shouldReportBlock = in.readBoolean();
            Block b = new Block();
            b.readFields(in);
            int numTargets = in.readInt();
            if (numTargets <= 0) {
              throw new IOException("Mislabelled incoming datastream.");
            }
            DatanodeInfo targets[] = new DatanodeInfo[numTargets];
            for (int i = 0; i < targets.length; i++) {
              DatanodeInfo tmp = new DatanodeInfo();
              tmp.readFields(in);
              targets[i] = tmp;
            }
            byte encodingType = (byte) in.read();
            long len = in.readLong();
            
            //
            // Make sure curTarget is equal to this machine
            //
            DatanodeInfo curTarget = targets[0];
            
            //
            // Track all the places we've successfully written the block
            //
            Vector mirrors = new Vector();
            
            //
            // Open local disk out
            //
            DataOutputStream out = new DataOutputStream(new BufferedOutputStream(data.writeToBlock(b)));
            InetSocketAddress mirrorTarget = null;
            String mirrorNode = null;
            try {
              //
              // Open network conn to backup machine, if 
              // appropriate
              //
              DataInputStream in2 = null;
              DataOutputStream out2 = null;
              if (targets.length > 1) {
                // Connect to backup machine
                mirrorNode = targets[1].getName();
                mirrorTarget = createSocketAddr(mirrorNode);
                try {
                  Socket s2 = new Socket();
                  s2.connect(mirrorTarget, READ_TIMEOUT);
                  s2.setSoTimeout(READ_TIMEOUT);
                  out2 = new DataOutputStream(new BufferedOutputStream(s2.getOutputStream()));
                  in2 = new DataInputStream(new BufferedInputStream(s2.getInputStream()));
                  
                  // Write connection header
                  out2.write(OP_WRITE_BLOCK);
                  out2.writeBoolean(shouldReportBlock);
                  b.write(out2);
                  out2.writeInt(targets.length - 1);
                  for (int i = 1; i < targets.length; i++) {
                    targets[i].write(out2);
                  }
                  out2.write(encodingType);
                  out2.writeLong(len);
                  myMetrics.replicatedBlocks(1);
                } catch (IOException ie) {
                  if (out2 != null) {
                    LOG.info("Exception connecting to mirror " + mirrorNode 
                             + "\n" + StringUtils.stringifyException(ie));
                    try {
                      out2.close();
                      in2.close();
                    } catch (IOException out2close) {
                    } finally {
                      out2 = null;
                      in2 = null;
                    }
                  }
                }
              }
              
              //
              // Process incoming data, copy to disk and
              // maybe to network.
              //
              boolean anotherChunk = len != 0;
              byte buf[] = new byte[BUFFER_SIZE];
              
              while (anotherChunk) {
                while (len > 0) {
                  int bytesRead = in.read(buf, 0, (int)Math.min(buf.length, len));
                  if (bytesRead < 0) {
                    throw new EOFException("EOF reading from "+s.toString());
                  }
                  if (bytesRead > 0) {
                    try {
                      out.write(buf, 0, bytesRead);
                      myMetrics.wroteBytes(bytesRead);
                    } catch (IOException iex) {
                      if (iex.getMessage().startsWith("No space left on device")) {
                    	  throw new DiskOutOfSpaceException("No space left on device");
                      } else {
                        shutdown();
                        throw iex;
                      }
                    }
                    if (out2 != null) {
                      try {
                        out2.write(buf, 0, bytesRead);
                      } catch (IOException out2e) {
                        LOG.info("Exception writing to mirror " + mirrorNode 
                            + "\n" + StringUtils.stringifyException(out2e));
                        //
                        // If stream-copy fails, continue 
                        // writing to disk.  We shouldn't 
                        // interrupt client write.
                        //
                        try {
                          out2.close();
                          in2.close();
                        } catch (IOException out2close) {
                        } finally {
                          out2 = null;
                          in2 = null;
                        }
                      }
                    }
                    len -= bytesRead;
                  }
                }
                
                if (encodingType == RUNLENGTH_ENCODING) {
                  anotherChunk = false;
                } else if (encodingType == CHUNKED_ENCODING) {
                  len = in.readLong();
                  if (out2 != null) {
                    try {
                      out2.writeLong(len);
                    } catch (IOException ie) {
                      LOG.info("Exception writing to mirror " + mirrorNode 
                          + "\n" + StringUtils.stringifyException(ie));
                      try {
                        out2.close();
                        in2.close();
                      } catch (IOException ie2) {
                        // NOTHING
                      } finally {
                        out2 = null;
                        in2 = null;
                      }
                    }
                  }
                  if (len == 0) {
                    anotherChunk = false;
                  }
                }
              }
              
              if (out2 != null) {
                try {
                  out2.flush();
                  long complete = in2.readLong();
                  if (complete != WRITE_COMPLETE) {
                    LOG.info("Conflicting value for WRITE_COMPLETE: " + complete);
                  }
                  LocatedBlock newLB = new LocatedBlock();
                  newLB.readFields(in2);
                  in2.close();
                  out2.close();
                  DatanodeInfo mirrorsSoFar[] = newLB.getLocations();
                  for (int k = 0; k < mirrorsSoFar.length; k++) {
                    mirrors.add(mirrorsSoFar[k]);
                  }
                } catch (IOException ie) {
                  LOG.info("Exception writing to mirror " + mirrorNode 
                      + "\n" + StringUtils.stringifyException(ie));
                  try {
                    out2.close();
                    in2.close();
                  } catch (IOException ie2) {
                    // NOTHING
                  } finally {
                    out2 = null;
                    in2 = null;
                  }
                }
              }
              if (out2 == null) {
                LOG.info("Received block " + b + " from " + 
                    s.getInetAddress());
              } else {
                LOG.info("Received block " + b + " from " + 
                    s.getInetAddress() + 
                    " and mirrored to " + mirrorTarget);
              }
            } finally {
              try {
                out.close();
              } catch (IOException iex) {
                shutdown();
                throw iex;
              }
            }
            data.finalizeBlock(b);
            myMetrics.wroteBlocks(1);
            
            // 
            // Tell the namenode that we've received this block 
            // in full, if we've been asked to.  This is done
            // during NameNode-directed block transfers, but not
            // client writes.
            //
            if (shouldReportBlock) {
              synchronized (receivedBlockList) {
                receivedBlockList.add(b);
                receivedBlockList.notifyAll();
              }
            }
            
            //
            // Tell client job is done, and reply with
            // the new LocatedBlock.
            //
            reply.writeLong(WRITE_COMPLETE);
            mirrors.add(curTarget);
            LocatedBlock newLB = new LocatedBlock(b, (DatanodeInfo[]) mirrors.toArray(new DatanodeInfo[mirrors.size()]));
            newLB.write(reply);
          } finally {
            reply.close();
          }
        }
    }

    /**
     * Used for transferring a block of data.  This class
     * sends a piece of data to another DataNode.
     */
    class DataTransfer implements Runnable {
        InetSocketAddress curTarget;
        DatanodeInfo targets[];
        Block b;
        byte buf[];

        /**
         * Connect to the first item in the target list.  Pass along the 
         * entire target list, the block, and the data.
         */
        public DataTransfer(DatanodeInfo targets[], Block b) throws IOException {
            this.curTarget = createSocketAddr(targets[0].getName());
            this.targets = targets;
            this.b = b;
            this.buf = new byte[BUFFER_SIZE];
        }

        /**
         * Do the deed, write the bytes
         */
        public void run() {
      xmitsInProgress++;
            try {
                Socket s = new Socket();
                s.connect(curTarget, READ_TIMEOUT);
                s.setSoTimeout(READ_TIMEOUT);
                DataOutputStream out = new DataOutputStream(new BufferedOutputStream(s.getOutputStream()));
                try {
                    long filelen = data.getLength(b);
                    DataInputStream in = new DataInputStream(new BufferedInputStream(data.getBlockData(b)));
                    try {
                        //
                        // Header info
                        //
                        out.write(OP_WRITE_BLOCK);
                        out.writeBoolean(true);
                        b.write(out);
                        out.writeInt(targets.length);
                        for (int i = 0; i < targets.length; i++) {
                            targets[i].write(out);
                        }
                        out.write(RUNLENGTH_ENCODING);
                        out.writeLong(filelen);

                        //
                        // Write the data
                        //
                        while (filelen > 0) {
                            int bytesRead = in.read(buf, 0, (int) Math.min(filelen, buf.length));
                            out.write(buf, 0, bytesRead);
                            filelen -= bytesRead;
                        }
                    } finally {
                        in.close();
                    }
                } finally {
                    out.close();
                }
                LOG.info("Transmitted block " + b + " to " + curTarget);
            } catch (IOException ie) {
              LOG.warn("Failed to transfer "+b+" to "+curTarget, ie);
            } finally {
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
        LOG.info("Starting DataNode in: "+data);
        
        // start dataXceiveServer
        dataXceiveServer.start();
        
        while (shouldRun) {
            try {
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
  static DataNode createDataNode( String args[],
                                  Configuration conf ) throws IOException {
    if( conf == null )
      conf = new Configuration();
    if( ! parseArguments( args, conf )) {
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
  static DataNode makeInstance( String[] dataDirs, Configuration conf )
  throws IOException {
    ArrayList<File> dirs = new ArrayList<File>();
    for (int i = 0; i < dataDirs.length; i++) {
      File data = new File(dataDirs[i]);
      try {
        DiskChecker.checkDir( data );
        dirs.add(data);
      } catch( DiskErrorException e ) {
        LOG.warn("Invalid directory in dfs.data.dir: " + e.getMessage() );
      }
    }
    if( dirs.size() > 0 ) 
      return new DataNode(conf, dirs);
    LOG.error("All directories in dfs.data.dir are invalid." );
    return null;
  }

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
                                        Configuration conf ) {
    int argsLen = (args == null) ? 0 : args.length;
    StartupOption startOpt = StartupOption.REGULAR;
    String networkLoc = null;
    for( int i=0; i < argsLen; i++ ) {
      String cmd = args[i];
      if( "-r".equalsIgnoreCase(cmd) || "--rack".equalsIgnoreCase(cmd) ) {
        if( i==args.length-1 )
          return false;
        networkLoc = args[++i];
        if( networkLoc.startsWith("-") )
          return false;
      } else if( "-rollback".equalsIgnoreCase(cmd) ) {
        startOpt = StartupOption.ROLLBACK;
      } else if( "-regular".equalsIgnoreCase(cmd) ) {
        startOpt = StartupOption.REGULAR;
      } else
        return false;
    }
    if( networkLoc != null )
      conf.set( "dfs.datanode.rack", NodeBase.normalize( networkLoc ));
    conf.setObject( "dfs.datanode.startup", startOpt );
    return true;
  }

    /* Get the network location by running a script configured in conf */
    private static String getNetworkLoc( Configuration conf ) 
                          throws IOException {
        String locScript = conf.get("dfs.network.script" );
        if( locScript == null ) 
          return NetworkTopology.DEFAULT_RACK;

        LOG.info( "Starting to run script to get datanode network location");
        Process p = Runtime.getRuntime().exec( locScript );
        StringBuffer networkLoc = new StringBuffer();
        final BufferedReader inR = new BufferedReader(
                new InputStreamReader(p.getInputStream() ) );
        final BufferedReader errR = new BufferedReader(
                new InputStreamReader( p.getErrorStream() ) );

        // read & log any error messages from the running script
        Thread errThread = new Thread() {
            public void start() {
                try {
                String errLine = errR.readLine();
                while(errLine != null) {
                    LOG.warn("Network script error: "+errLine);
                    errLine = errR.readLine();
                }
                } catch( IOException e) {
                    
                }
            }
        };
        try {
            errThread.start();
            
            // fetch output from the process
            String line = inR.readLine();
            while( line != null ) {
                networkLoc.append( line );
                line = inR.readLine();
            }
            try {
            // wait for the process to finish
            int returnVal = p.waitFor();
            // check the exit code
            if( returnVal != 0 ) {
                throw new IOException("Process exits with nonzero status: "+locScript);
            }
            } catch (InterruptedException e) {
                throw new IOException( e.getMessage() );
            } finally {
                try {
                    // make sure that the error thread exits
                    errThread.join();
                } catch (InterruptedException je) {
                    LOG.warn( StringUtils.stringifyException(je));
                }
            }
        } finally {
            // close in & error streams
            try {
                inR.close();
            } catch ( IOException ine ) {
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
        DataNode datanode = createDataNode( args, null );
        if( datanode != null )
          datanode.join();
      } catch ( Throwable e ) {
        LOG.error( StringUtils.stringifyException( e ) );
        System.exit(-1);
      }
    }

}
