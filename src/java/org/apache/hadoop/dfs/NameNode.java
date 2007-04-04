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
import org.apache.hadoop.fs.Trash;
import org.apache.hadoop.io.*;
import org.apache.hadoop.ipc.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.util.StringUtils;

import java.io.*;
import java.net.*;
import java.util.Collection;
import java.util.Iterator;

import org.apache.hadoop.metrics.MetricsRecord;
import org.apache.hadoop.metrics.MetricsUtil;
import org.apache.hadoop.metrics.MetricsContext;
import org.apache.hadoop.metrics.Updater;

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
 * with exposing the IPC interface to the outside world, plus some
 * configuration management.
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
 * @author Mike Cafarella
 **********************************************************/
public class NameNode implements ClientProtocol, DatanodeProtocol, FSConstants {
    public long getProtocolVersion(String protocol, 
                                   long clientVersion) throws IOException { 
      if (protocol.equals(ClientProtocol.class.getName())) {
        return ClientProtocol.versionID; 
      } else if (protocol.equals(DatanodeProtocol.class.getName())){
        return DatanodeProtocol.versionID;
      } else {
        throw new IOException("Unknown protocol to name node: " + protocol);
      }
    }
    
    public static final Log LOG = LogFactory.getLog("org.apache.hadoop.dfs.NameNode");
    public static final Log stateChangeLog = LogFactory.getLog( "org.apache.hadoop.dfs.StateChange");

    private FSNamesystem namesystem;
    private Server server;
    private Thread emptier;
    private int handlerCount = 2;
    
    private InetSocketAddress nameNodeAddress = null;
    
    /** only used for testing purposes  */
    private boolean stopRequested = false;

    /** Format a new filesystem.  Destroys any filesystem that may already
     * exist at this location.  **/
    public static void format(Configuration conf) throws IOException {
      format( conf, false );
    }

    private class NameNodeMetrics implements Updater {
      private final MetricsRecord metricsRecord;
      private int numFilesCreated = 0;
      private int numFilesOpened = 0;
      private int numFilesRenamed = 0;
      private int numFilesListed = 0;
      
      NameNodeMetrics() {
        MetricsContext metricsContext = MetricsUtil.getContext("dfs");
        metricsRecord = MetricsUtil.createRecord(metricsContext, "namenode");
        metricsContext.registerUpdater(this);
      }
      
      /**
       * Since this object is a registered updater, this method will be called
       * periodically, e.g. every 5 seconds.
       */
      public void doUpdates(MetricsContext unused) {
        synchronized (this) {
          metricsRecord.incrMetric("files_created", numFilesCreated);
          metricsRecord.incrMetric("files_opened", numFilesOpened);
          metricsRecord.incrMetric("files_renamed", numFilesRenamed);
          metricsRecord.incrMetric("files_listed", numFilesListed);
              
          numFilesCreated = 0;
          numFilesOpened = 0;
          numFilesRenamed = 0;
          numFilesListed = 0;
        }
        metricsRecord.update();
      }
      
      synchronized void createFile() {
        ++numFilesCreated;
      }
      
      synchronized void openFile() {
        ++numFilesOpened;
      }
      
      synchronized void renameFile() {
        ++numFilesRenamed;
      }
      
      synchronized void listFile(int nfiles) {
        numFilesListed += nfiles;
      }
    }
    
    private NameNodeMetrics myMetrics = new NameNodeMetrics();
    
    /**
     * Initialize the server
     * 
     * @param hostname which hostname to bind to
     * @param port the port number to bind to
     * @param conf the configuration
     */
    private void init(String hostname, int port, 
                      Configuration conf
                      ) throws IOException {
      this.handlerCount = conf.getInt("dfs.namenode.handler.count", 10);
      this.server = RPC.getServer(this, hostname, port, handlerCount, 
                                  false, conf);

      // The rpc-server port can be ephemeral... ensure we have the correct info
      this.nameNodeAddress = this.server.getListenerAddress(); 
      conf.set("fs.default.name", new String(nameNodeAddress.getHostName() + ":" + nameNodeAddress.getPort()));
      LOG.info("Namenode up at: " + this.nameNodeAddress);

      try {
        this.namesystem = new FSNamesystem(this.nameNodeAddress.getHostName(), this.nameNodeAddress.getPort(), this, conf);
        this.server.start();  //start RPC server   
  
        this.emptier = new Thread(new Trash(conf).getEmptier(), "Trash Emptier");
        this.emptier.setDaemon(true);
        this.emptier.start();
      } catch (IOException e) {
        this.server.stop();
        throw e;
      }
      
    }
    
    /**
     * Start NameNode.
     * <p>
     * The name-node can be started with one of the following startup options:
     * <ul> 
     * <li>{@link FSConstants.StartupOption#REGULAR REGULAR} - normal startup</li>
     * <li>{@link FSConstants.StartupOption#FORMAT FORMAT} - format name node</li>
     * <li>{@link FSConstants.StartupOption#UPGRADE UPGRADE} - start the cluster  
     * upgrade and create a snapshot of the current file system state</li> 
     * <li>{@link FSConstants.StartupOption#ROLLBACK ROLLBACK} - roll the  
     *            cluster back to the previous state</li>
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
      InetSocketAddress addr = 
        DataNode.createSocketAddr(conf.get("fs.default.name"));
      init( addr.getHostName(), addr.getPort(), conf );
    }

    /**
     * Create a NameNode at the specified location and start it.
     * 
     * The conf will be modified to reflect the actual ports on which 
     * the NameNode is up and running if the user passes the port as
     * <code>zero</code>.  
     */
    public NameNode(String bindAddress, int port, 
                    Configuration conf
                    ) throws IOException {
      init( bindAddress, port, conf );
    }

    /** Return the configured directories where name data is stored. 
     * @deprecated
     */
    static File[] getDirs(Configuration conf) {
      String[] dirNames = conf.getStrings("dfs.name.dir");
      if (dirNames == null) { dirNames = new String[] {"/tmp/hadoop/dfs/name"}; }
      File[] dirs = new File[dirNames.length];
      for (int idx = 0; idx < dirs.length; idx++) {
        dirs[idx] = new File(dirNames[idx]);
      }
      return dirs;
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
      if (! stopRequested) {
        stopRequested = true;
        namesystem.close();
        emptier.interrupt();
        server.stop();
      }
    }

    /////////////////////////////////////////////////////
    // ClientProtocol
    /////////////////////////////////////////////////////
    
    /**
     */
    public LocatedBlock[] open(String src) throws IOException {
        String clientMachine = Server.getRemoteAddress();
        if ( clientMachine == null ) {
            clientMachine = "";
        }
        Object openResults[] = namesystem.open(clientMachine, new UTF8(src));
        if (openResults == null) {
            throw new IOException("Cannot open filename " + src);
        } else {
            myMetrics.openFile();
            Block blocks[] = (Block[]) openResults[0];
            DatanodeInfo sets[][] = (DatanodeInfo[][]) openResults[1];
            LocatedBlock results[] = new LocatedBlock[blocks.length];
            for (int i = 0; i < blocks.length; i++) {
                results[i] = new LocatedBlock(blocks[i], sets[i]);
            }
            return results;
        }
    }

    /**
     */
    public LocatedBlock create(String src, 
                               String clientName, 
                               boolean overwrite,
                               short replication,
                               long blockSize
    ) throws IOException {
       String clientMachine = Server.getRemoteAddress();
       if ( clientMachine == null ) {
           clientMachine = "";
       }
       stateChangeLog.debug("*DIR* NameNode.create: file "
            +src+" for "+clientName+" at "+clientMachine);
       if (!checkPathLength(src)) {
           throw new IOException("create: Pathname too long.  Limit " 
               + MAX_PATH_LENGTH + " characters, " + MAX_PATH_DEPTH + " levels.");
       }
       Object results[] = namesystem.startFile(new UTF8(src), 
                                                new UTF8(clientName), 
                                                new UTF8(clientMachine), 
                                                overwrite,
                                                replication,
                                                blockSize);
       myMetrics.createFile();
        Block b = (Block) results[0];
        DatanodeInfo targets[] = (DatanodeInfo[]) results[1];
        return new LocatedBlock(b, targets);
    }

    public boolean setReplication( String src, 
                                short replication
                              ) throws IOException {
      return namesystem.setReplication( src, replication );
    }
    
    /**
     */
    public LocatedBlock addBlock(String src, 
                                 String clientName) throws IOException {
        stateChangeLog.debug("*BLOCK* NameNode.addBlock: file "
            +src+" for "+clientName);
        UTF8 src8 = new UTF8(src);
        UTF8 client8 = new UTF8(clientName);
        Object[] results = namesystem.getAdditionalBlock(src8, client8);
        Block b = (Block) results[0];
        DatanodeInfo targets[] = (DatanodeInfo[]) results[1];
        return new LocatedBlock(b, targets);            
    }

    /**
     * The client can report in a set written blocks that it wrote.
     * These blocks are reported via the client instead of the datanode
     * to prevent weird heartbeat race conditions.
     */
    public void reportWrittenBlock(LocatedBlock lb) throws IOException {
        Block b = lb.getBlock();        
        DatanodeInfo targets[] = lb.getLocations();
        stateChangeLog.debug("*BLOCK* NameNode.reportWrittenBlock"
                +": " + b.getBlockName() +" is written to "
                +targets.length + " locations" );

        for (int i = 0; i < targets.length; i++) {
            namesystem.blockReceived( targets[i], b );
        }
    }

    /**
     * The client needs to give up on the block.
     */
    public void abandonBlock(Block b, String src) throws IOException {
        stateChangeLog.debug("*BLOCK* NameNode.abandonBlock: "
                +b.getBlockName()+" of file "+src );
        if (! namesystem.abandonBlock(b, new UTF8(src))) {
            throw new IOException("Cannot abandon block during write to " + src);
        }
    }
    /**
     */
    public void abandonFileInProgress(String src, 
                                      String holder) throws IOException {
        stateChangeLog.debug("*DIR* NameNode.abandonFileInProgress:" + src );
        namesystem.abandonFileInProgress(new UTF8(src), new UTF8(holder));
    }
    /**
     */
    public boolean complete(String src, String clientName) throws IOException {
        stateChangeLog.debug("*DIR* NameNode.complete: " + src + " for " + clientName );
        int returnCode = namesystem.completeFile(new UTF8(src), new UTF8(clientName));
        if (returnCode == STILL_WAITING) {
            return false;
        } else if (returnCode == COMPLETE_SUCCESS) {
            return true;
        } else {
            throw new IOException("Could not complete write to file " + src + " by " + clientName);
        }
    }

    /**
     * The client has detected an error on the specified located blocks 
     * and is reporting them to the server.  For now, the namenode will 
     * delete the blocks from the datanodes.  In the future we might 
     * check the blocks are actually corrupt. 
     */
    public void reportBadBlocks(LocatedBlock[] blocks) throws IOException {
      stateChangeLog.debug("*DIR* NameNode.reportBadBlocks");
      for (int i = 0; i < blocks.length; i++) {
        Block blk = blocks[i].getBlock();
        DatanodeInfo[] nodes = blocks[i].getLocations();
        for (int j = 0; j < nodes.length; j++) {
          DatanodeInfo dn = nodes[j];
          namesystem.invalidateBlock(blk, dn);
        }
      }
    }

    /**
     */
    public String[][] getHints(String src, long start, long len) throws IOException {
      return namesystem.getDatanodeHints( src, start, len );
    }
    
    public long getBlockSize(String filename) throws IOException {
      return namesystem.getBlockSize(filename);
    }
    
    /**
     */
    public boolean rename(String src, String dst) throws IOException {
        stateChangeLog.debug("*DIR* NameNode.rename: " + src + " to " + dst );
        if (!checkPathLength(dst)) {
            throw new IOException("rename: Pathname too long.  Limit " 
                + MAX_PATH_LENGTH + " characters, " + MAX_PATH_DEPTH + " levels.");
        }
        boolean ret = namesystem.renameTo(new UTF8(src), new UTF8(dst));
        if (ret) {
            myMetrics.renameFile();
        }
        return ret;
    }

    /**
     */
    public boolean delete(String src) throws IOException {
        stateChangeLog.debug("*DIR* NameNode.delete: " + src );
        return namesystem.delete(new UTF8(src));
    }

    /**
     */
    public boolean exists(String src) throws IOException {
        return namesystem.exists(new UTF8(src));
    }

    /**
     */
    public boolean isDir(String src) throws IOException {
        return namesystem.isDir(new UTF8(src));
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
    
    /**
     */
    public boolean mkdirs(String src) throws IOException {
        stateChangeLog.debug("*DIR* NameNode.mkdirs: " + src );
        if (!checkPathLength(src)) {
            throw new IOException("mkdirs: Pathname too long.  Limit " 
                + MAX_PATH_LENGTH + " characters, " + MAX_PATH_DEPTH + " levels.");
        }
        return namesystem.mkdirs( src );
    }

    /** @deprecated */ @Deprecated
    public boolean obtainLock(String src, String clientName, boolean exclusive) throws IOException {
        int returnCode = namesystem.obtainLock(new UTF8(src), new UTF8(clientName), exclusive);
        if (returnCode == COMPLETE_SUCCESS) {
            return true;
        } else if (returnCode == STILL_WAITING) {
            return false;
        } else {
            throw new IOException("Failure when trying to obtain lock on " + src);
        }
    }

    /** @deprecated */ @Deprecated
    public boolean releaseLock(String src, String clientName) throws IOException {
        int returnCode = namesystem.releaseLock(new UTF8(src), new UTF8(clientName));
        if (returnCode == COMPLETE_SUCCESS) {
            return true;
        } else if (returnCode == STILL_WAITING) {
            return false;
        } else {
            throw new IOException("Failure when trying to release lock on " + src);
        }
    }

    /**
     */
    public void renewLease(String clientName) throws IOException {
        namesystem.renewLease(new UTF8(clientName));        
    }

    /**
     */
    public DFSFileInfo[] getListing(String src) throws IOException {
        DFSFileInfo[] files = namesystem.getListing(new UTF8(src));
        if (files != null) {
            myMetrics.listFile(files.length);
        }
        return files;
    }

    /**
     */
    public long[] getStats() throws IOException {
        long results[] = new long[2];
        long totalCapacity = namesystem.totalCapacity();
        results[0] = totalCapacity;
        results[1] = totalCapacity - namesystem.totalRemaining();
        return results;
    }

    /**
     */
    public DatanodeInfo[] getDatanodeReport() throws IOException {
        DatanodeInfo results[] = namesystem.datanodeReport();
        if (results == null || results.length == 0) {
            throw new IOException("Cannot find datanode report");
        }
        return results;
    }
    
    /**
     * @inheritDoc
     */
    public boolean setSafeMode( SafeModeAction action ) throws IOException {
      switch( action ) {
      case SAFEMODE_LEAVE: // leave safe mode
        namesystem.leaveSafeMode();
        break;
      case SAFEMODE_ENTER: // enter safe mode
        namesystem.enterSafeMode();
        break;
      case SAFEMODE_GET: // get safe mode
      }
      return namesystem.isInSafeMode();
    }

    /**
     * Is the cluster currently in safe mode?
     */
    boolean isInSafeMode() {
      return namesystem.isInSafeMode();
    }

    /*
     * Refresh the list of datanodes that the namenode should allow to  
     * connect.  Uses the files list in the configuration to update the list. 
     */
    public void refreshNodes() throws IOException {
      namesystem.refreshNodes();
    }

    /**
     * Returns the size of the current edit log.
     */
    public long getEditLogSize() throws IOException {
      return namesystem.getEditLogSize();
    }

    /**
     * Roll the edit log.
     */
    public void rollEditLog() throws IOException {
      namesystem.rollEditLog();
    }

    /**
     * Roll the image 
     */
    public void rollFsImage() throws IOException {
      namesystem.rollFSImage();
    }
    
    public void finalizeUpgrade() throws IOException {
      getFSImage().finalizeUpgrade();
    }

    /**
     * Dumps namenode state into specified file
     */
    public void metaSave(String filename) throws IOException {
      namesystem.metaSave(filename);
    }

    ////////////////////////////////////////////////////////////////
    // DatanodeProtocol
    ////////////////////////////////////////////////////////////////
    /** 
     */
    public DatanodeRegistration register( DatanodeRegistration nodeReg,
                                          String networkLocation
                                        ) throws IOException {
      verifyVersion( nodeReg.getVersion() );
      namesystem.registerDatanode( nodeReg, networkLocation );
      
      return nodeReg;
    }
    
    /**
     * Data node notify the name node that it is alive 
     * Return a block-oriented command for the datanode to execute.
     * This will be either a transfer or a delete operation.
     */
    public DatanodeCommand sendHeartbeat( DatanodeRegistration nodeReg,
                                          long capacity, 
                                          long remaining,
                                          int xmitsInProgress,
                                          int xceiverCount) throws IOException {
        Object xferResults[] = new Object[2];
        xferResults[0] = xferResults[1] = null;
        Object deleteList[] = new Object[1];
        deleteList[0] = null; 

        verifyRequest( nodeReg );
        if( namesystem.gotHeartbeat( nodeReg, capacity, remaining, 
                                     xceiverCount, 
                                     xmitsInProgress,
                                     xferResults,
                                     deleteList)) {
          // request block report from the datanode
          assert(xferResults[0] == null && deleteList[0] == null);
          return new DatanodeCommand( DataNodeAction.DNA_REGISTER );
        }
        
        //
        // Ask to perform pending transfers, if any
        //
        if (xferResults[0] != null) {
            assert(deleteList[0] == null);
            return new BlockCommand((Block[]) xferResults[0], (DatanodeInfo[][]) xferResults[1]);
        }

        //
        // If there are no transfers, check for recently-deleted blocks that
        // should be removed.  This is not a full-datanode sweep, as is done during
        // a block report.  This is just a small fast removal of blocks that have
        // just been removed.
        //
        if (deleteList[0] != null) {
            return new BlockCommand((Block[]) deleteList[0]);
        }
        return null;
    }

    public DatanodeCommand blockReport( DatanodeRegistration nodeReg,
                                        Block blocks[]) throws IOException {
        verifyRequest( nodeReg );
        stateChangeLog.debug("*BLOCK* NameNode.blockReport: "
                +"from "+nodeReg.getName()+" "+blocks.length+" blocks" );

        Block blocksToDelete[] = namesystem.processReport( nodeReg, blocks );
        if( blocksToDelete != null && blocksToDelete.length > 0 )
            return new BlockCommand( blocksToDelete );
        if( getFSImage().isUpgradeFinalized() )
          return new DatanodeCommand( DataNodeAction.DNA_FINALIZE );
        return null;
    }

    public void blockReceived(DatanodeRegistration nodeReg, 
                              Block blocks[]) throws IOException {
        verifyRequest( nodeReg );
        stateChangeLog.debug("*BLOCK* NameNode.blockReceived: "
                +"from "+nodeReg.getName()+" "+blocks.length+" blocks." );
        for (int i = 0; i < blocks.length; i++) {
            namesystem.blockReceived( nodeReg, blocks[i] );
        }
    }

    /**
     */
    public void errorReport(DatanodeRegistration nodeReg,
                            int errorCode, 
                            String msg) throws IOException {
      // Log error message from datanode
      LOG.info("Report from " + nodeReg.getName() + ": " + msg);
      if( errorCode == DatanodeProtocol.NOTIFY ) {
        return;
      }
      verifyRequest( nodeReg );
      if( errorCode == DatanodeProtocol.DISK_ERROR ) {
          namesystem.removeDatanode( nodeReg );            
      }
    }
    
    public NamespaceInfo versionRequest() throws IOException {
      return namesystem.getNamespaceInfo();
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
    public void verifyRequest( DatanodeRegistration nodeReg ) throws IOException {
      verifyVersion( nodeReg.getVersion() );
      if( ! namesystem.getRegistrationID().equals( nodeReg.getRegistrationID() ))
          throw new UnregisteredDatanodeException( nodeReg );
    }
    
    /**
     * Verify version.
     * 
     * @param version
     * @throws IOException
     */
    public void verifyVersion( int version ) throws IOException {
      if( version != LAYOUT_VERSION )
        throw new IncorrectVersionException( version, "data node" );
    }

    /**
     * Returns the name of the fsImage file
     */
    public File getFsImageName() throws IOException {
      return getFSImage().getFsImageName();
    }
    
    FSImage getFSImage() {
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
     * Returns the name of the edits file
     */
    public File getFsEditName() throws IOException {
      return namesystem.getFsEditName();
    }

    /**
     * Returns the address on which the NameNodes is listening to.
     * @return the address on which the NameNodes is listening to.
     */
    public InetSocketAddress getNameNodeAddress() {
      return nameNodeAddress;
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
                                  boolean isConfirmationNeeded
                                ) throws IOException {
      Collection<File> dirsToFormat = FSNamesystem.getNamespaceDirs( conf );
      for( Iterator<File> it = dirsToFormat.iterator(); it.hasNext(); ) {
        File curDir = it.next();
        if( ! curDir.exists() )
          continue;
        if( isConfirmationNeeded ) {
          System.err.print("Re-format filesystem in " + curDir +" ? (Y or N) ");
          if (!(System.in.read() == 'Y')) {
            System.err.println("Format aborted in "+ curDir);
            return true;
          }
          while( System.in.read() != '\n' ); // discard the enter-key
        }
      }

      FSNamesystem nsys = new FSNamesystem(new FSImage( dirsToFormat ));
      nsys.dir.fsImage.format();
      return false;
    }

    private static void printUsage() {
      System.err.println(
      "Usage: java NameNode [-format] | [-upgrade] | [-rollback]");
    }

    private static StartupOption parseArguments(String args[], 
                                                Configuration conf ) {
      int argsLen = (args == null) ? 0 : args.length;
      StartupOption startOpt = StartupOption.REGULAR;
      for( int i=0; i < argsLen; i++ ) {
        String cmd = args[i];
        if( "-format".equalsIgnoreCase(cmd) ) {
          startOpt = StartupOption.FORMAT;
        } else if( "-regular".equalsIgnoreCase(cmd) ) {
          startOpt = StartupOption.REGULAR;
        } else if( "-upgrade".equalsIgnoreCase(cmd) ) {
          startOpt = StartupOption.UPGRADE;
        } else if( "-rollback".equalsIgnoreCase(cmd) ) {
          startOpt = StartupOption.ROLLBACK;
        } else
          return null;
      }
      conf.setObject( "dfs.namenode.startup", startOpt );
      return startOpt;
    }

    static NameNode createNameNode( String argv[], 
                                    Configuration conf ) throws Exception {
      if( conf == null )
        conf = new Configuration();
      StartupOption startOpt = parseArguments( argv, conf );
      if( startOpt == null ) {
        printUsage();
        return null;
      }
      
      if( startOpt == StartupOption.FORMAT ) {
        boolean aborted = format( conf, true );
        System.exit(aborted ? 1 : 0);
      }
      
      NameNode namenode = new NameNode(conf);
      return namenode;
    }
    
    /**
     */
    public static void main(String argv[]) throws Exception {
      try {
        NameNode namenode = createNameNode( argv, null );
        if( namenode != null )
          namenode.join();
      } catch ( Throwable e ) {
        LOG.error( StringUtils.stringifyException( e ) );
        System.exit(-1);
      }
    }
}
