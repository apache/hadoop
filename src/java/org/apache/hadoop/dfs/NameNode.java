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
import org.apache.hadoop.io.*;
import org.apache.hadoop.ipc.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.util.StringUtils;

import java.io.*;

import org.apache.hadoop.metrics.MetricsRecord;
import org.apache.hadoop.metrics.Metrics;

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
    public long getProtocolVersion(String protocol, long clientVersion) { 
      if (protocol.equals(ClientProtocol.class.getName())) {
        return ClientProtocol.versionID; 
      } else {
        return DatanodeProtocol.versionID;
      }
    }
    public static final Log LOG = LogFactory.getLog("org.apache.hadoop.dfs.NameNode");
    public static final Log stateChangeLog = LogFactory.getLog( "org.apache.hadoop.dfs.StateChange");

    private FSNamesystem namesystem;
    private Server server;
    private int handlerCount = 2;
    
    /** only used for testing purposes  */
    private boolean stopRequested = false;

    /** Format a new filesystem.  Destroys any filesystem that may already
     * exist at this location.  **/
    public static void format(Configuration conf) throws IOException {
      File[] dirs = getDirs(conf);
      for (int idx = 0; idx < dirs.length; idx++) {
        FSImage.format(dirs[idx]);
      }
    }

    /** Format a new filesystem.  Destroys any filesystem that may already
     * exist at this location.  **/
    public static void format(File dir) throws IOException {
      FSImage.format(dir);
    }

    private class NameNodeMetrics {
      private MetricsRecord metricsRecord = null;
      
      private long numFilesCreated = 0L;
      private long numFilesOpened = 0L;
      private long numFilesRenamed = 0L;
      private long numFilesListed = 0L;
      
      NameNodeMetrics() {
        metricsRecord = Metrics.createRecord("dfs", "namenode");
      }
      
      synchronized void createFile() {
        Metrics.report(metricsRecord, "files-created", ++numFilesCreated);
      }
      
      synchronized void openFile() {
        Metrics.report(metricsRecord, "files-opened", ++numFilesOpened);
      }
      
      synchronized void renameFile() {
        Metrics.report(metricsRecord, "files-renamed", ++numFilesRenamed);
      }
      
      synchronized void listFile(int nfiles) {
        numFilesListed += nfiles;
        Metrics.report(metricsRecord, "files-listed", numFilesListed);
      }
    }
    
    private NameNodeMetrics myMetrics = null;
    
    /**
     * Create a NameNode at the default location
     */
    public NameNode(Configuration conf) throws IOException {
       this(getDirs(conf),DataNode.createSocketAddr(conf.get("fs.default.name", "local")).getHostName(),
                       DataNode.createSocketAddr(conf.get("fs.default.name", "local")).getPort(), conf);
    }

    /**
     * Create a NameNode at the specified location and start it.
     */
    public NameNode(File[] dirs, String bindAddress, int port, Configuration conf) throws IOException {
        this.namesystem = new FSNamesystem(dirs, this, conf);
        this.handlerCount = conf.getInt("dfs.namenode.handler.count", 10);
        this.server = RPC.getServer(this, bindAddress, port, handlerCount, false, conf);
        this.server.start();
        myMetrics = new NameNodeMetrics();
    }

    /** Return the configured directories where name data is stored. */
    private static File[] getDirs(Configuration conf) {
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
        server.stop();
        //this.join();
      }
    }

    /////////////////////////////////////////////////////
    // ClientProtocol
    /////////////////////////////////////////////////////
    
    /**
     */
    public LocatedBlock[] open(String src) throws IOException {
        Object openResults[] = namesystem.open(new UTF8(src));
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
                               String clientMachine, 
                               boolean overwrite,
                               short replication,
                               long blockSize
    ) throws IOException {
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

    /**
     */
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

    /**
     */
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
        results[0] = namesystem.totalCapacity();
        results[1] = namesystem.totalCapacity() - namesystem.totalRemaining();
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
    
    ////////////////////////////////////////////////////////////////
    // DatanodeProtocol
    ////////////////////////////////////////////////////////////////
    /** 
     */
    public DatanodeRegistration register( DatanodeRegistration nodeReg
                                        ) throws IOException {
      verifyVersion( nodeReg.getVersion() );
      namesystem.registerDatanode( nodeReg );
      return nodeReg;
    }
    
    /**
     * Data node notify the name node that it is alive 
     * Return a block-oriented command for the datanode to execute.
     * This will be either a transfer or a delete operation.
     */
    public BlockCommand sendHeartbeat(DatanodeRegistration nodeReg,
                                      long capacity, 
                                      long remaining,
                                      int xmitsInProgress,
                                      int xceiverCount) throws IOException {
        verifyRequest( nodeReg );
        if( namesystem.gotHeartbeat( nodeReg, capacity, remaining, xceiverCount )) {
          // request block report from the datanode
          return new BlockCommand( DataNodeAction.DNA_REPORT );
        }
        
        //
        // Ask to perform pending transfers, if any
        //
        Object xferResults[] = namesystem.pendingTransfers( nodeReg,
                                                            xmitsInProgress );
        if (xferResults != null) {
            return new BlockCommand((Block[]) xferResults[0], (DatanodeInfo[][]) xferResults[1]);
        }

        //
        // If there are no transfers, check for recently-deleted blocks that
        // should be removed.  This is not a full-datanode sweep, as is done during
        // a block report.  This is just a small fast removal of blocks that have
        // just been removed.
        //
        Block blocks[] = namesystem.blocksToInvalidate( nodeReg );
        if (blocks != null) {
            return new BlockCommand(blocks);
        }
        return null;
    }

    public Block[] blockReport( DatanodeRegistration nodeReg,
                                Block blocks[]) throws IOException {
        verifyRequest( nodeReg );
        stateChangeLog.debug("*BLOCK* NameNode.blockReport: "
                +"from "+nodeReg.getName()+" "+blocks.length+" blocks" );

        return namesystem.processReport( nodeReg, blocks );
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
      verifyRequest( nodeReg );
      LOG.warn("Report from " + nodeReg.getName() + ": " + msg);
      if( errorCode == DatanodeProtocol.DISK_ERROR ) {
          namesystem.removeDatanode( nodeReg );            
      }
    }

    /** 
     * Verify request.
     * 
     * Verifies correctness of the datanode version and registration ID.
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
      if( version != DFS_CURRENT_VERSION )
        throw new IncorrectVersionException( version, "data node" );
    }
    
    /**
     */
    public static void main(String argv[]) throws Exception {
      try {
        Configuration conf = new Configuration();

        if (argv.length == 1 && argv[0].equals("-format")) {
          boolean aborted = false;
          File[] dirs = getDirs(conf);
          for (int idx = 0; idx < dirs.length; idx++) {
            if (dirs[idx].exists()) {
              System.err.print("Re-format filesystem in " + dirs[idx] +" ? (Y or N) ");
              if (!(System.in.read() == 'Y')) {
                System.err.println("Format aborted in "+ dirs[idx]);
                aborted = true;
              } else {
                format(dirs[idx]);
                System.err.println("Formatted "+dirs[idx]);
              }
              System.in.read(); // discard the enter-key
            }else{
              format(dirs[idx]);
              System.err.println("Formatted "+dirs[idx]);
            }
          }
          System.exit(aborted ? 1 : 0);
        }
        
        NameNode namenode = new NameNode(conf);
        namenode.join();
        
      } catch ( Throwable e ) {
        LOG.error( StringUtils.stringifyException( e ) );
        System.exit(-1);
      }
    }
}
