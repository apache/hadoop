/**
 * Copyright 2005 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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

import org.apache.hadoop.io.*;
import org.apache.hadoop.ipc.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.util.*;

import java.io.*;

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
    public static final Log LOG = LogFactory.getLog("org.apache.hadoop.dfs.NameNode");
    public static final Log stateChangeLog = LogFactory.getLog( "org.apache.hadoop.dfs.StateChange");

    private FSNamesystem namesystem;
    private Server server;
    private int handlerCount = 2;
    private long datanodeStartupPeriod;
    private volatile long firstBlockReportTime;
    
    /** only used for testing purposes  */
    private boolean stopRequested = false;

    /** Format a new filesystem.  Destroys any filesystem that may already
     * exist at this location.  **/
    public static void format(Configuration conf) throws IOException {
      FSDirectory.format(getDir(conf), conf);
    }

    /**
     * Create a NameNode at the default location
     */
    public NameNode(Configuration conf) throws IOException {
        this(getDir(conf),
             DataNode.createSocketAddr
             (conf.get("fs.default.name", "local")).getPort(), conf);
    }

    /**
     * Create a NameNode at the specified location and start it.
     */
    public NameNode(File dir, int port, Configuration conf) throws IOException {     
        this.namesystem = new FSNamesystem(dir, conf);
        this.handlerCount = conf.getInt("dfs.namenode.handler.count", 10);
        this.server = RPC.getServer(this, port, handlerCount, false, conf);
        this.datanodeStartupPeriod =
            conf.getLong("dfs.datanode.startupMsec", DATANODE_STARTUP_PERIOD);
        this.server.start();
    }

    /** Return the configured directory where name data is stored. */
    private static File getDir(Configuration conf) {
      return new File(conf.get("dfs.name.dir", "/tmp/hadoop/dfs/name"));
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
     * Package-only access since this is intended for JUnit testing.
    */
    void stop() {
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
       Object results[] = namesystem.startFile(new UTF8(src), 
                                                new UTF8(clientName), 
                                                new UTF8(clientMachine), 
                                                overwrite,
                                                replication,
                                                blockSize);
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
        UTF8 hosts[][] = namesystem.getDatanodeHints(new UTF8(src), start, len);
        if (hosts == null) {
            return new String[0][];
        } else {
            String results[][] = new String[hosts.length][];
            for (int i = 0; i < hosts.length; i++) {
                results[i] = new String[hosts[i].length];
                for (int j = 0; j < results[i].length; j++) {
                    results[i][j] = hosts[i][j].toString();
                }
            }
            return results;
        }
    }
    
    public long getBlockSize(String filename) throws IOException {
      return namesystem.getBlockSize(filename);
    }
    
    /**
     */
    public boolean rename(String src, String dst) throws IOException {
        stateChangeLog.debug("*DIR* NameNode.rename: " + src + " to " + dst );
        return namesystem.renameTo(new UTF8(src), new UTF8(dst));
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
     */
    public boolean mkdirs(String src) throws IOException {
        stateChangeLog.debug("*DIR* NameNode.mkdirs: " + src );
        return namesystem.mkdirs(new UTF8(src));
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
        return namesystem.getListing(new UTF8(src));
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
                                      int xmitsInProgress) throws IOException {
        verifyRequest( nodeReg );
        namesystem.gotHeartbeat( nodeReg, capacity, remaining );
        
        //
        // Only ask datanodes to perform block operations (transfer, delete) 
        // after a startup quiet period.  The assumption is that all the
        // datanodes will be started together, but the namenode may
        // have been started some time before.  (This is esp. true in
        // the case of network interruptions.)  So, wait for some time
        // to pass from the time of connection to the first block-transfer.
        // Otherwise we transfer a lot of blocks unnecessarily.
        //
        // Hairong: Ideally in addition we also look at the history. For example,
        // we should wait until at least 98% of datanodes are connected to the server
        //
        if( firstBlockReportTime==0 ||
            System.currentTimeMillis()-firstBlockReportTime < datanodeStartupPeriod) {
            return null;
        }
        
        //
        // Ask to perform pending transfers, if any
        //
        Object xferResults[] = namesystem.pendingTransfers(
                       new DatanodeInfo( nodeReg ), xmitsInProgress );
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
        if( firstBlockReportTime==0)
              firstBlockReportTime=System.currentTimeMillis();

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
        Configuration conf = new Configuration();

        if (argv.length == 1 && argv[0].equals("-format")) {
          File dir = getDir(conf);
          if (dir.exists()) {
            System.err.print("Re-format filesystem in " + dir +" ? (Y or N) ");
            if (!(System.in.read() == 'Y')) {
              System.err.println("Format aborted.");
              System.exit(1);
            }
          }
          format(conf);
          System.err.println("Formatted "+dir);
          System.exit(0);
        }
        
        NameNode namenode = new NameNode(conf);
        namenode.join();
    }
}
