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

import org.apache.hadoop.io.*;
import org.apache.hadoop.ipc.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.util.LogFormatter;

import java.io.*;
import java.util.logging.*;

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
    public static final Logger LOG = LogFormatter.getLogger("org.apache.hadoop.dfs.NameNode");

    private FSNamesystem namesystem;
    private Server server;
    private int handlerCount = 2;

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
    public LocatedBlock create(String src, String clientName, String clientMachine, boolean overwrite) throws IOException {
        Object results[] = namesystem.startFile(new UTF8(src), new UTF8(clientName), new UTF8(clientMachine), overwrite);
        if (results == null) {
            throw new IOException("Cannot create file " + src + " on client " + clientName);
        } else {
            Block b = (Block) results[0];
            DatanodeInfo targets[] = (DatanodeInfo[]) results[1];
            return new LocatedBlock(b, targets);
        }
    }

    /**
     */
    public LocatedBlock addBlock(String src, String clientMachine) throws IOException {
        int retries = 5;
        Object results[] = namesystem.getAdditionalBlock(new UTF8(src), new UTF8(clientMachine));
        while (results != null && results[0] == null && retries > 0) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException ie) {
            }
            results = namesystem.getAdditionalBlock(new UTF8(src), new UTF8(clientMachine));
            retries--;
        }

        if (results == null) {
            throw new IOException("Cannot obtain additional block for file " + src);
        } else if (results[0] == null) {
            return null;
        } else {
            Block b = (Block) results[0];
            DatanodeInfo targets[] = (DatanodeInfo[]) results[1];
            return new LocatedBlock(b, targets);
        }
    }

    /**
     * The client can report in a set written blocks that it wrote.
     * These blocks are reported via the client instead of the datanode
     * to prevent weird heartbeat race conditions.
     */
    public void reportWrittenBlock(LocatedBlock lb) throws IOException {
        Block b = lb.getBlock();
        DatanodeInfo targets[] = lb.getLocations();
        for (int i = 0; i < targets.length; i++) {
            namesystem.blockReceived(b, targets[i].getName());
        }
    }

    /**
     * The client needs to give up on the block.
     */
    public void abandonBlock(Block b, String src) throws IOException {
        if (! namesystem.abandonBlock(b, new UTF8(src))) {
            throw new IOException("Cannot abandon block during write to " + src);
        }
    }
    /**
     */
    public void abandonFileInProgress(String src) throws IOException {
        namesystem.abandonFileInProgress(new UTF8(src));
    }
    /**
     */
    public boolean complete(String src, String clientName) throws IOException {
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
    /**
     */
    public boolean rename(String src, String dst) throws IOException {
        return namesystem.renameTo(new UTF8(src), new UTF8(dst));
    }

    /**
     */
    public boolean delete(String src) throws IOException {
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
    public void sendHeartbeat(String sender, long capacity, long remaining) {
        namesystem.gotHeartbeat(new UTF8(sender), capacity, remaining);
    }

    public Block[] blockReport(String sender, Block blocks[]) {
        LOG.info("Block report from "+sender+": "+blocks.length+" blocks.");
        return namesystem.processReport(blocks, new UTF8(sender));
    }

    public void blockReceived(String sender, Block blocks[]) {
        for (int i = 0; i < blocks.length; i++) {
            namesystem.blockReceived(blocks[i], new UTF8(sender));
        }
    }

    /**
     */
    public void errorReport(String sender, String msg) {
        // Log error message from datanode
        //LOG.info("Report from " + sender + ": " + msg);
    }

    /**
     * Return a block-oriented command for the datanode to execute.
     * This will be either a transfer or a delete operation.
     */
    public BlockCommand getBlockwork(String sender, int xmitsInProgress) {
        //
        // Ask to perform pending transfers, if any
        //
        Object xferResults[] = namesystem.pendingTransfers(new DatanodeInfo(new UTF8(sender)), xmitsInProgress);
        if (xferResults != null) {
            return new BlockCommand((Block[]) xferResults[0], (DatanodeInfo[][]) xferResults[1]);
        }

        //
        // If there are no transfers, check for recently-deleted blocks that
        // should be removed.  This is not a full-datanode sweep, as is done during
        // a block report.  This is just a small fast removal of blocks that have
        // just been removed.
        //
        Block blocks[] = namesystem.blocksToInvalidate(new UTF8(sender));
        if (blocks != null) {
            return new BlockCommand(blocks);
        }
        return null;
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
