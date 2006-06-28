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
import org.apache.hadoop.conf.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.mapred.StatusHttpServer;

import java.io.*;
import java.net.InetSocketAddress;
import java.util.*;

/***************************************************
 * FSNamesystem does the actual bookkeeping work for the
 * DataNode.
 *
 * It tracks several important tables.
 *
 * 1)  valid fsname --> blocklist  (kept on disk, logged)
 * 2)  Set of all valid blocks (inverted #1)
 * 3)  block --> machinelist (kept in memory, rebuilt dynamically from reports)
 * 4)  machine --> blocklist (inverted #2)
 * 5)  LRU cache of updated-heartbeat machines
 ***************************************************/
class FSNamesystem implements FSConstants {
    public static final Log LOG = LogFactory.getLog("org.apache.hadoop.fs.FSNamesystem");

    //
    // Stores the correct file name hierarchy
    //
    FSDirectory dir;

    //
    // Stores the block-->datanode(s) map.  Updated only in response
    // to client-sent information.
    //
    TreeMap blocksMap = new TreeMap();

    //
    // Stores the datanode-->block map.  Done by storing a 
    // set of datanode info objects, sorted by name.  Updated only in
    // response to client-sent information.
    //
    TreeMap datanodeMap = new TreeMap();

    
    //
    // Stores the set of dead datanodes
    TreeMap deaddatanodeMap = new TreeMap();
    
    //
    // Keeps a Vector for every named machine.  The Vector contains
    // blocks that have recently been invalidated and are thought to live
    // on the machine in question.
    //
    TreeMap recentInvalidateSets = new TreeMap();

    //
    // Keeps a TreeSet for every named node.  Each treeset contains
    // a list of the blocks that are "extra" at that location.  We'll
    // eventually remove these extras.
    //
    TreeMap excessReplicateMap = new TreeMap();

    //
    // Keeps track of files that are being created, plus the
    // blocks that make them up.
    //
    // Maps file names to FileUnderConstruction objects
    //
    TreeMap pendingCreates = new TreeMap();

    //
    // Keeps track of the blocks that are part of those pending creates
    //
    TreeSet pendingCreateBlocks = new TreeSet();

    //
    // Stats on overall usage
    //
    long totalCapacity = 0, totalRemaining = 0;

    
    //
    // For the HTTP browsing interface
    //
    StatusHttpServer infoServer;
    int infoPort;
    Date startTime;
        
    //
    Random r = new Random();

    //
    // Stores a set of datanode info objects, sorted by heartbeat
    //
    TreeSet heartbeats = new TreeSet(new Comparator() {
        public int compare(Object o1, Object o2) {
            DatanodeInfo d1 = (DatanodeInfo) o1;
            DatanodeInfo d2 = (DatanodeInfo) o2;            
            long lu1 = d1.lastUpdate();
            long lu2 = d2.lastUpdate();
            if (lu1 < lu2) {
                return -1;
            } else if (lu1 > lu2) {
                return 1;
            } else {
                return d1.getStorageID().compareTo(d2.getStorageID());
            }
        }
    });

    //
    // Store set of Blocks that need to be replicated 1 or more times.
    // We also store pending replication-orders.
    //
    private TreeSet neededReplications = new TreeSet();
    private TreeSet pendingReplications = new TreeSet();

    //
    // Used for handling lock-leases
    //
    private TreeMap leases = new TreeMap();
    private TreeSet sortedLeases = new TreeSet();

    //
    // Threaded object that checks to see if we have been
    // getting heartbeats from all clients. 
    //
    HeartbeatMonitor hbmon = null;
    LeaseMonitor lmon = null;
    Daemon hbthread = null, lmthread = null;
    boolean fsRunning = true;
    long systemStart = 0;

    //  The maximum number of replicates we should allow for a single block
    private int maxReplication;
    //  How many outgoing replication streams a given node should have at one time
    private int maxReplicationStreams;
    // MIN_REPLICATION is how many copies we need in place or else we disallow the write
    private int minReplication;
    // HEARTBEAT_RECHECK is how often a datanode sends its hearbeat
    private int heartBeatRecheck;

    public static FSNamesystem fsNamesystemObject;
    private String localMachine;
    private int port;

    /**
     * dir is where the filesystem directory state 
     * is stored
     */
    public FSNamesystem(File dir, Configuration conf) throws IOException {
        fsNamesystemObject = this;
        this.infoPort = conf.getInt("dfs.info.port", 50070);
        this.infoServer = new StatusHttpServer("dfs", infoPort, false);
        this.infoServer.start();
        InetSocketAddress addr = DataNode.createSocketAddr(conf.get("fs.default.name", "local"));
        this.localMachine = addr.getHostName();
        this.port = addr.getPort();
        this.dir = new FSDirectory(dir, conf);
        this.hbthread = new Daemon(new HeartbeatMonitor());
        this.lmthread = new Daemon(new LeaseMonitor());
        hbthread.start();
        lmthread.start();
        this.systemStart = System.currentTimeMillis();
        this.startTime = new Date(systemStart); 

        this.maxReplication = conf.getInt("dfs.replication.max", 512);
        this.minReplication = conf.getInt("dfs.replication.min", 1);
        if( maxReplication < minReplication )
          throw new IOException(
              "Unexpected configuration parameters: dfs.replication.min = " 
              + minReplication
              + " must be less than dfs.replication.max = " 
              + maxReplication );

        this.maxReplicationStreams = conf.getInt("dfs.max-repl-streams", 2);
        this.heartBeatRecheck= 1000;
    }
    /** Return the FSNamesystem object
     * 
     */
    public static FSNamesystem getFSNamesystem() {
        return fsNamesystemObject;
    } 

    /** Close down this filesystem manager.
     * Causes heartbeat and lease daemons to stop; waits briefly for
     * them to finish, but a short timeout returns control back to caller.
     */
    public void close() {
      synchronized (this) {
        fsRunning = false;
      }
        try {
            infoServer.stop();
            hbthread.join(3000);
        } catch (InterruptedException ie) {
        } finally {
          // using finally to ensure we also wait for lease daemon
          try {
            lmthread.join(3000);
          } catch (InterruptedException ie) {
          } finally {
              try {
                dir.close();
              } catch (IOException ex) {
                  // do nothing
              }
          }
        }
    }

    /////////////////////////////////////////////////////////
    //
    // These methods are called by HadoopFS clients
    //
    /////////////////////////////////////////////////////////
    /**
     * The client wants to open the given filename.  Return a
     * list of (block,machineArray) pairs.  The sequence of unique blocks
     * in the list indicates all the blocks that make up the filename.
     *
     * The client should choose one of the machines from the machineArray
     * at random.
     */
    public Object[] open(UTF8 src) {
        Object results[] = null;
        Block blocks[] = dir.getFile(src);
        if (blocks != null) {
            results = new Object[2];
            DatanodeInfo machineSets[][] = new DatanodeInfo[blocks.length][];

            for (int i = 0; i < blocks.length; i++) {
                TreeSet containingNodes = (TreeSet) blocksMap.get(blocks[i]);
                if (containingNodes == null) {
                    machineSets[i] = new DatanodeInfo[0];
                } else {
                    machineSets[i] = new DatanodeInfo[containingNodes.size()];
                    int j = 0;
                    for (Iterator it = containingNodes.iterator(); it.hasNext(); j++) {
                        machineSets[i][j] = (DatanodeInfo) it.next();
                    }
                }
            }

            results[0] = blocks;
            results[1] = machineSets;
        }
        return results;
    }

    /**
     * Set replication for an existing file.
     * 
     * The NameNode sets new replication and schedules either replication of 
     * under-replicated data blocks or removal of the eccessive block copies 
     * if the blocks are over-replicated.
     * 
     * @see ClientProtocol#setReplication(String, short)
     * @param src file name
     * @param replication new replication
     * @return true if successful; 
     *         false if file does not exist or is a directory
     * @author shv
     */
    public boolean setReplication(String src, 
                                  short replication
                                ) throws IOException {
      verifyReplication(src, replication, null );

      Vector oldReplication = new Vector();
      Block[] fileBlocks;
      fileBlocks = dir.setReplication( src, replication, oldReplication );
      if( fileBlocks == null )  // file not found or is a directory
        return false;
      int oldRepl = ((Integer)oldReplication.elementAt(0)).intValue();
      if( oldRepl == replication ) // the same replication
        return true;

      synchronized( neededReplications ) {
        if( oldRepl < replication ) { 
          // old replication < the new one; need to replicate
          LOG.info("Increasing replication for file " + src 
                    + ". New replication is " + replication );
          for( int idx = 0; idx < fileBlocks.length; idx++ )
            neededReplications.add( fileBlocks[idx] );
        } else {  
          // old replication > the new one; need to remove copies
          LOG.info("Reducing replication for file " + src 
                    + ". New replication is " + replication );
          for( int idx = 0; idx < fileBlocks.length; idx++ )
            proccessOverReplicatedBlock( fileBlocks[idx], replication );
        }
      }
      return true;
    }
    
    public long getBlockSize(String filename) throws IOException {
      return dir.getBlockSize(filename);
    }
    
    /**
     * Check whether the replication parameter is within the range
     * determined by system configuration.
     */
    private void verifyReplication( String src, 
                                    short replication, 
                                    UTF8 clientName 
                                  ) throws IOException {
      String text = "file " + src 
              + ((clientName != null) ? " on client " + clientName : "")
              + ".\n"
              + "Requested replication " + replication;

      if( replication > maxReplication )
        throw new IOException( text + " exceeds maximum " + maxReplication );
      
      if( replication < minReplication )
        throw new IOException(  
            text + " is less than the required minimum " + minReplication );
    }
    
    /**
     * The client would like to create a new block for the indicated
     * filename.  Return an array that consists of the block, plus a set 
     * of machines.  The first on this list should be where the client 
     * writes data.  Subsequent items in the list must be provided in
     * the connection to the first datanode.
     * @return Return an array that consists of the block, plus a set
     * of machines
     * @throws IOException if the filename is invalid
     *         {@link FSDirectory#isValidToCreate(UTF8)}.
     */
    public synchronized Object[] startFile( UTF8 src, 
                                            UTF8 holder, 
                                            UTF8 clientMachine, 
                                            boolean overwrite,
                                            short replication,
                                            long blockSize
                                          ) throws IOException {
      NameNode.stateChangeLog.debug("DIR* NameSystem.startFile: file "
            +src+" for "+holder+" at "+clientMachine);
      try {
        if (pendingCreates.get(src) != null) {
           throw new AlreadyBeingCreatedException(
                   "failed to create file " + src + " for " + holder +
                   " on client " + clientMachine + 
                   " because pendingCreates is non-null.");
        }

        try {
           verifyReplication(src.toString(), replication, clientMachine );
        } catch( IOException e) {
            throw new IOException( "failed to create "+e.getMessage());
        }
        if (!dir.isValidToCreate(src)) {
          if (overwrite) {
            delete(src);
          } else {
            throw new IOException("failed to create file " + src 
                    +" on client " + clientMachine
                    +" either because the filename is invalid or the file exists");
          }
        }

        // Get the array of replication targets 
        DatanodeInfo targets[] = chooseTargets(replication, null, 
                                               clientMachine, blockSize);
        if (targets.length < this.minReplication) {
            throw new IOException("failed to create file "+src
                    +" on client " + clientMachine
                    +" because target-length is " + targets.length 
                    +", below MIN_REPLICATION (" + minReplication+ ")");
       }

        // Reserve space for this pending file
        pendingCreates.put(src, 
                           new FileUnderConstruction(replication, 
                                                     blockSize,
                                                     holder,
                                                     clientMachine));
        NameNode.stateChangeLog.debug( "DIR* NameSystem.startFile: "
                   +"add "+src+" to pendingCreates for "+holder );
        synchronized (leases) {
            Lease lease = (Lease) leases.get(holder);
            if (lease == null) {
                lease = new Lease(holder);
                leases.put(holder, lease);
                sortedLeases.add(lease);
            } else {
                sortedLeases.remove(lease);
                lease.renew();
                sortedLeases.add(lease);
            }
            lease.startedCreate(src);
        }

        // Create next block
        Object results[] = new Object[2];
        results[0] = allocateBlock(src);
        results[1] = targets;
        return results;
      } catch (IOException ie) {
          NameNode.stateChangeLog.warn("DIR* NameSystem.startFile: "
                  +ie.getMessage());
        throw ie;
      }
    }

    /**
     * The client would like to obtain an additional block for the indicated
     * filename (which is being written-to).  Return an array that consists
     * of the block, plus a set of machines.  The first on this list should
     * be where the client writes data.  Subsequent items in the list must
     * be provided in the connection to the first datanode.
     *
     * Make sure the previous blocks have been reported by datanodes and
     * are replicated.  Will return an empty 2-elt array if we want the
     * client to "try again later".
     */
    public synchronized Object[] getAdditionalBlock(UTF8 src, 
                                                    UTF8 clientName
                                                    ) throws IOException {
        NameNode.stateChangeLog.debug("BLOCK* NameSystem.getAdditionalBlock: file "
            +src+" for "+clientName);
        FileUnderConstruction pendingFile = 
          (FileUnderConstruction) pendingCreates.get(src);
        // make sure that we still have the lease on this file
        if (pendingFile == null) {
          throw new LeaseExpiredException("No lease on " + src);
        }
        if (!pendingFile.getClientName().equals(clientName)) {
          throw new LeaseExpiredException("Lease mismatch on " + src + 
              " owned by " + pendingFile.getClientName() + 
              " and appended by " + clientName);
        }
        if (dir.getFile(src) != null) {
          throw new IOException("File " + src + " created during write");
        }

        //
        // If we fail this, bad things happen!
        //
        if (!checkFileProgress(src)) {
          throw new NotReplicatedYetException("Not replicated yet");
        }
        
        // Get the array of replication targets 
        DatanodeInfo targets[] = chooseTargets(pendingFile.getReplication(), 
            null, pendingFile.getClientMachine(), pendingFile.getBlockSize());
        if (targets.length < this.minReplication) {
          throw new IOException("File " + src + " could only be replicated to " +
                                targets.length + " nodes, instead of " +
                                minReplication);
        }
        
        // Create next block
        return new Object[]{allocateBlock(src), targets};
    }

    /**
     * The client would like to let go of the given block
     */
    public synchronized boolean abandonBlock(Block b, UTF8 src) {
        //
        // Remove the block from the pending creates list
        //
        NameNode.stateChangeLog.debug("BLOCK* NameSystem.abandonBlock: "
                +b.getBlockName()+"of file "+src );
        FileUnderConstruction pendingFile = 
          (FileUnderConstruction) pendingCreates.get(src);
        if (pendingFile != null) {
            Vector pendingVector = pendingFile.getBlocks();
            for (Iterator it = pendingVector.iterator(); it.hasNext(); ) {
                Block cur = (Block) it.next();
                if (cur.compareTo(b) == 0) {
                    pendingCreateBlocks.remove(cur);
                    it.remove();
                    NameNode.stateChangeLog.debug(
                             "BLOCK* NameSystem.abandonBlock: "
                            +b.getBlockName()
                            +" is removed from pendingCreateBlock and pendingCreates");
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * Abandon the entire file in progress
     */
    public synchronized void abandonFileInProgress(UTF8 src, 
                                                   UTF8 holder
                                                   ) throws IOException {
      NameNode.stateChangeLog.debug("DIR* NameSystem.abandonFileInProgress:" + src );
      synchronized (leases) {
        // find the lease
        Lease lease = (Lease) leases.get(holder);
        if (lease != null) {
          // remove the file from the lease
          if (lease.completedCreate(src)) {
            // if we found the file in the lease, remove it from pendingCreates
            internalReleaseCreate(src, holder);
          } else {
            LOG.info("Attempt by " + holder.toString() + 
                " to release someone else's create lock on " + 
                src.toString());
          }
        } else {
          LOG.info("Attempt to release a lock from an unknown lease holder "
              + holder.toString() + " for " + src.toString());
        }
      }
    }

    /**
     * Finalize the created file and make it world-accessible.  The
     * FSNamesystem will already know the blocks that make up the file.
     * Before we return, we make sure that all the file's blocks have 
     * been reported by datanodes and are replicated correctly.
     */
    public synchronized int completeFile(UTF8 src, UTF8 holder) {
        NameNode.stateChangeLog.debug("DIR* NameSystem.completeFile: " + src + " for " + holder );
        if (dir.getFile(src) != null || pendingCreates.get(src) == null) {
            NameNode.stateChangeLog.warn( "DIR* NameSystem.completeFile: "
                    + "failed to complete " + src
                    + " because dir.getFile()==" + dir.getFile(src) 
                    + " and " + pendingCreates.get(src));
            return OPERATION_FAILED;
        } else if (! checkFileProgress(src)) {
            return STILL_WAITING;
        }
        
        FileUnderConstruction pendingFile = 
            (FileUnderConstruction) pendingCreates.get(src);
        Vector blocks = pendingFile.getBlocks();
        int nrBlocks = blocks.size();
        Block pendingBlocks[] = (Block[]) blocks.toArray(new Block[nrBlocks]);

        //
        // We have the pending blocks, but they won't have
        // length info in them (as they were allocated before
        // data-write took place).  So we need to add the correct
        // length info to each
        //
        // REMIND - mjc - this is very inefficient!  We should
        // improve this!
        //
        for (int i = 0; i < nrBlocks; i++) {
            Block b = (Block)pendingBlocks[i];
            TreeSet containingNodes = (TreeSet) blocksMap.get(b);
            DatanodeInfo node = (DatanodeInfo) containingNodes.first();
            for (Iterator it = node.getBlockIterator(); it.hasNext(); ) {
                Block cur = (Block) it.next();
                if (b.getBlockId() == cur.getBlockId()) {
                    b.setNumBytes(cur.getNumBytes());
                    break;
                }
            }
        }
        
        //
        // Now we can add the (name,blocks) tuple to the filesystem
        //
        if ( ! dir.addFile(src, pendingBlocks, pendingFile.getReplication())) {
          return OPERATION_FAILED;
        }

        // The file is no longer pending
        pendingCreates.remove(src);
        NameNode.stateChangeLog.debug(
             "DIR* NameSystem.completeFile: " + src
           + " is removed from pendingCreates");
        for (int i = 0; i < nrBlocks; i++) {
            pendingCreateBlocks.remove(pendingBlocks[i]);
        }

        synchronized (leases) {
            Lease lease = (Lease) leases.get(holder);
            if (lease != null) {
                lease.completedCreate(src);
                if (! lease.hasLocks()) {
                    leases.remove(holder);
                    sortedLeases.remove(lease);
                }
            }
        }

        //
        // REMIND - mjc - this should be done only after we wait a few secs.
        // The namenode isn't giving datanodes enough time to report the
        // replicated blocks that are automatically done as part of a client
        // write.
        //

        // Now that the file is real, we need to be sure to replicate
        // the blocks.
        for (int i = 0; i < nrBlocks; i++) {
            TreeSet containingNodes = (TreeSet) blocksMap.get(pendingBlocks[i]);
            if (containingNodes.size() < pendingFile.getReplication()) {
                   NameNode.stateChangeLog.debug(
                          "DIR* NameSystem.completeFile:"
                        + pendingBlocks[i].getBlockName()+" has only "+containingNodes.size()
                        +" replicas so is added to neededReplications");           
                synchronized (neededReplications) {
                    neededReplications.add(pendingBlocks[i]);
                }
            }
        }
        return COMPLETE_SUCCESS;
    }

    static Random randBlockId = new Random();
    
    /**
     * Allocate a block at the given pending filename
     */
    synchronized Block allocateBlock(UTF8 src) {
        Block b = null;
        do {
            b = new Block(FSNamesystem.randBlockId.nextLong(), 0);
        } while (dir.isValidBlock(b));
        FileUnderConstruction v = 
          (FileUnderConstruction) pendingCreates.get(src);
        v.getBlocks().add(b);
        pendingCreateBlocks.add(b);
        NameNode.stateChangeLog.debug("BLOCK* NameSystem.allocateBlock: "
            +src+ ". "+b.getBlockName()+
            " is created and added to pendingCreates and pendingCreateBlocks" );      
        return b;
    }

    /**
     * Check that the indicated file's blocks are present and
     * replicated.  If not, return false.
     */
    synchronized boolean checkFileProgress(UTF8 src) {
        FileUnderConstruction v = 
          (FileUnderConstruction) pendingCreates.get(src);

        for (Iterator it = v.getBlocks().iterator(); it.hasNext(); ) {
            Block b = (Block) it.next();
            TreeSet containingNodes = (TreeSet) blocksMap.get(b);
            if (containingNodes == null || containingNodes.size() < this.minReplication) {
                return false;
            }
        }
        return true;
    }

    ////////////////////////////////////////////////////////////////
    // Here's how to handle block-copy failure during client write:
    // -- As usual, the client's write should result in a streaming
    // backup write to a k-machine sequence.
    // -- If one of the backup machines fails, no worries.  Fail silently.
    // -- Before client is allowed to close and finalize file, make sure
    // that the blocks are backed up.  Namenode may have to issue specific backup
    // commands to make up for earlier datanode failures.  Once all copies
    // are made, edit namespace and return to client.
    ////////////////////////////////////////////////////////////////

    /**
     * Change the indicated filename.
     */
    public boolean renameTo(UTF8 src, UTF8 dst) {
        NameNode.stateChangeLog.debug("DIR* NameSystem.renameTo: " + src + " to " + dst );
        return dir.renameTo(src, dst);
    }

    /**
     * Remove the indicated filename from the namespace.  This may
     * invalidate some blocks that make up the file.
     */
    public synchronized boolean delete(UTF8 src) {
        NameNode.stateChangeLog.debug("DIR* NameSystem.delete: " + src );
        Block deletedBlocks[] = (Block[]) dir.delete(src);
        if (deletedBlocks != null) {
            for (int i = 0; i < deletedBlocks.length; i++) {
                Block b = deletedBlocks[i];

                TreeSet containingNodes = (TreeSet) blocksMap.get(b);
                if (containingNodes != null) {
                    for (Iterator it = containingNodes.iterator(); it.hasNext(); ) {
                        DatanodeInfo node = (DatanodeInfo) it.next();
                        Vector invalidateSet = (Vector) recentInvalidateSets.get(node.getStorageID());
                        if (invalidateSet == null) {
                            invalidateSet = new Vector();
                            recentInvalidateSets.put(node.getStorageID(), invalidateSet);
                        }
                        invalidateSet.add(b);
                        NameNode.stateChangeLog.debug("BLOCK* NameSystem.delete: "
                            + b.getBlockName() + " is added to invalidSet of " + node.getName() );
                    }
                }
            }
        }

        return (deletedBlocks != null);
    }

    /**
     * Return whether the given filename exists
     */
    public boolean exists(UTF8 src) {
        if (dir.getFile(src) != null || dir.isDir(src)) {
            return true;
        } else {
            return false;
        }
    }

    /**
     * Whether the given name is a directory
     */
    public boolean isDir(UTF8 src) {
        return dir.isDir(src);
    }

    /**
     * Create all the necessary directories
     */
    public boolean mkdirs(UTF8 src) {
        NameNode.stateChangeLog.debug("DIR* NameSystem.mkdirs: " + src );
        return dir.mkdirs(src);
    }

    /**
     * Figure out a few hosts that are likely to contain the
     * block(s) referred to by the given (filename, start, len) tuple.
     */
    public UTF8[][] getDatanodeHints(UTF8 src, long start, long len) {
        if (start < 0 || len < 0) {
            return new UTF8[0][];
        }

        int startBlock = -1;
        int endBlock = -1;
        Block blocks[] = dir.getFile(src);

        if (blocks == null) {                     // no blocks
            return new UTF8[0][];
        }

        //
        // First, figure out where the range falls in
        // the blocklist.
        //
        long startpos = start;
        long endpos = start + len;
        for (int i = 0; i < blocks.length; i++) {
            if (startpos >= 0) {
                startpos -= blocks[i].getNumBytes();
                if (startpos <= 0) {
                    startBlock = i;
                }
            }
            if (endpos >= 0) {
                endpos -= blocks[i].getNumBytes();
                if (endpos <= 0) {
                    endBlock = i;
                    break;
                }
            }
        }

        //
        // Next, create an array of hosts where each block can
        // be found
        //
        if (startBlock < 0 || endBlock < 0) {
            return new UTF8[0][];
        } else {
            UTF8 hosts[][] = new UTF8[(endBlock - startBlock) + 1][];
            for (int i = startBlock; i <= endBlock; i++) {
                TreeSet containingNodes = (TreeSet) blocksMap.get(blocks[i]);
                Vector v = new Vector();
                if (containingNodes != null) {
                  for (Iterator it =containingNodes.iterator(); it.hasNext();) {
                    DatanodeInfo cur = (DatanodeInfo) it.next();
                    v.add(new UTF8( cur.getHost() ));
                  }
                }
                hosts[i-startBlock] = (UTF8[]) v.toArray(new UTF8[v.size()]);
            }
            return hosts;
        }
    }

    /************************************************************
     * A Lease governs all the locks held by a single client.
     * For each client there's a corresponding lease, whose
     * timestamp is updated when the client periodically
     * checks in.  If the client dies and allows its lease to
     * expire, all the corresponding locks can be released.
     *************************************************************/
    class Lease implements Comparable {
        public UTF8 holder;
        public long lastUpdate;
        private TreeSet locks = new TreeSet();
        private TreeSet creates = new TreeSet();

        public Lease(UTF8 holder) {
            this.holder = holder;
            renew();
        }
        public void renew() {
            this.lastUpdate = System.currentTimeMillis();
        }
        public boolean expired() {
            if (System.currentTimeMillis() - lastUpdate > LEASE_PERIOD) {
                return true;
            } else {
                return false;
            }
        }
        public void obtained(UTF8 src) {
            locks.add(src);
        }
        public void released(UTF8 src) {
            locks.remove(src);
        }
        public void startedCreate(UTF8 src) {
            creates.add(src);
        }
        public boolean completedCreate(UTF8 src) {
            return creates.remove(src);
        }
        public boolean hasLocks() {
            return (locks.size() + creates.size()) > 0;
        }
        public void releaseLocks() {
            for (Iterator it = locks.iterator(); it.hasNext(); ) {
                UTF8 src = (UTF8) it.next();
                internalReleaseLock(src, holder);
            }
            locks.clear();
            for (Iterator it = creates.iterator(); it.hasNext(); ) {
                UTF8 src = (UTF8) it.next();
                internalReleaseCreate(src, holder);
            }
            creates.clear();
        }

        /**
         */
        public String toString() {
            return "[Lease.  Holder: " + holder.toString() + ", heldlocks: " +
                   locks.size() + ", pendingcreates: " + creates.size() + "]";
        }

        /**
         */
        public int compareTo(Object o) {
            Lease l1 = (Lease) this;
            Lease l2 = (Lease) o;
            long lu1 = l1.lastUpdate;
            long lu2 = l2.lastUpdate;
            if (lu1 < lu2) {
                return -1;
            } else if (lu1 > lu2) {
                return 1;
            } else {
                return l1.holder.compareTo(l2.holder);
            }
        }
    }
    /******************************************************
     * LeaseMonitor checks for leases that have expired,
     * and disposes of them.
     ******************************************************/
    class LeaseMonitor implements Runnable {
        public void run() {
            while (fsRunning) {
                synchronized (FSNamesystem.this) {
                    synchronized (leases) {
                        Lease top;
                        while ((sortedLeases.size() > 0) &&
                               ((top = (Lease) sortedLeases.first()) != null)) {
                            if (top.expired()) {
                                top.releaseLocks();
                                leases.remove(top.holder);
                                LOG.info("Removing lease " + top + ", leases remaining: " + sortedLeases.size());
                                if (!sortedLeases.remove(top)) {
                                    LOG.info("Unknown failure trying to remove " + top + " from lease set.");
                                }
                            } else {
                                break;
                            }
                        }
                    }
                }
                try {
                    Thread.sleep(2000);
                } catch (InterruptedException ie) {
                }
            }
        }
    }

    /**
     * Get a lock (perhaps exclusive) on the given file
     */
    public synchronized int obtainLock(UTF8 src, UTF8 holder, boolean exclusive) {
        int result = dir.obtainLock(src, holder, exclusive);
        if (result == COMPLETE_SUCCESS) {
            synchronized (leases) {
                Lease lease = (Lease) leases.get(holder);
                if (lease == null) {
                    lease = new Lease(holder);
                    leases.put(holder, lease);
                    sortedLeases.add(lease);
                } else {
                    sortedLeases.remove(lease);
                    lease.renew();
                    sortedLeases.add(lease);
                }
                lease.obtained(src);
            }
        }
        return result;
    }

    /**
     * Release the lock on the given file
     */
    public synchronized int releaseLock(UTF8 src, UTF8 holder) {
        int result = internalReleaseLock(src, holder);
        if (result == COMPLETE_SUCCESS) {
            synchronized (leases) {
                Lease lease = (Lease) leases.get(holder);
                if (lease != null) {
                    lease.released(src);
                    if (! lease.hasLocks()) {
                        leases.remove(holder);
                        sortedLeases.remove(lease);
                    }
                }
            }
        }
        return result;
    }
    private int internalReleaseLock(UTF8 src, UTF8 holder) {
        return dir.releaseLock(src, holder);
    }

    /**
     * Release a pending file creation lock.
     * @param src The filename
     * @param holder The datanode that was creating the file
     */
    private void internalReleaseCreate(UTF8 src, UTF8 holder) {
      FileUnderConstruction v = 
        (FileUnderConstruction) pendingCreates.remove(src);
      if (v != null) {
         NameNode.stateChangeLog.debug(
                      "DIR* NameSystem.internalReleaseCreate: " + src
                    + " is removed from pendingCreates for "
                    + holder + " (failure)");
        for (Iterator it2 = v.getBlocks().iterator(); it2.hasNext(); ) {
          Block b = (Block) it2.next();
          pendingCreateBlocks.remove(b);
        }
      } else {
          NameNode.stateChangeLog.warn("DIR* NameSystem.internalReleaseCreate: "
                 + "attempt to release a create lock on "+ src.toString()
                 + " that was not in pedingCreates");
      }
    }

    /**
     * Renew the lease(s) held by the given client
     */
    public void renewLease(UTF8 holder) {
        synchronized (leases) {
            Lease lease = (Lease) leases.get(holder);
            if (lease != null) {
                sortedLeases.remove(lease);
                lease.renew();
                sortedLeases.add(lease);
            }
        }
    }

    /**
     * Get a listing of all files at 'src'.  The Object[] array
     * exists so we can return file attributes (soon to be implemented)
     */
    public DFSFileInfo[] getListing(UTF8 src) {
        return dir.getListing(src);
    }

    /////////////////////////////////////////////////////////
    //
    // These methods are called by datanodes
    //
    /////////////////////////////////////////////////////////
    /**
     * Register Datanode.
     * <p>
     * The purpose of registration is to identify whether the new datanode
     * serves a new data storage, and will report new data block copies,
     * which the namenode was not aware of; or the datanode is a replacement
     * node for the data storage that was previously served by a different
     * or the same (in terms of host:port) datanode.
     * The data storages are distinguished by their storageIDs. When a new
     * data storage is reported the namenode issues a new unique storageID.
     * <p>
     * Finally, the namenode returns its namespaceID as the registrationID
     * for the datanodes. 
     * namespaceID is a persistent attribute of the name space.
     * The registrationID is checked every time the datanode is communicating
     * with the namenode. 
     * Datanodes with inappropriate registrationID are rejected.
     * If the namenode stops, and then restarts it can restore its 
     * namespaceID and will continue serving the datanodes that has previously
     * registered with the namenode without restarting the whole cluster.
     * 
     * @see DataNode#register()
     * @author Konstantin Shvachko
     */
    public synchronized void registerDatanode( DatanodeRegistration nodeReg 
                                              ) throws IOException {
      NameNode.stateChangeLog.debug(
          "BLOCK* NameSystem.registerDatanode: "
          + "node registration from " + nodeReg.getName()
          + " storage " + nodeReg.getStorageID() );

      nodeReg.registrationID = getRegistrationID();
      DatanodeInfo nodeS = (DatanodeInfo)datanodeMap.get(nodeReg.getStorageID());
      DatanodeInfo nodeN = getDatanodeByName( nodeReg.getName() );
      
      if( nodeN != null && nodeS != null && nodeN == nodeS ) {
        // The same datanode has been just restarted to serve the same data 
        // storage. We do not need to remove old data blocks, the delta will  
        // be calculated on the next block report from the datanode
        NameNode.stateChangeLog.debug(
            "BLOCK* NameSystem.registerDatanode: "
            + "node restarted." );
        return;
      }
      
      if( nodeN != null ) {
        // nodeN previously served a different data storage, 
        // which is not served by anybody anymore.
        removeDatanode( nodeN );
        nodeN = null;
      }
      
      // nodeN is not found
      if( nodeS == null ) {
        // this is a new datanode serving a new data storage
        if( nodeReg.getStorageID().equals("") ) {
          // this data storage has never registered
          // it is either empty or was created by previous version of DFS
          nodeReg.storageID = newStorageID();
          NameNode.stateChangeLog.debug(
              "BLOCK* NameSystem.registerDatanode: "
              + "new storageID " + nodeReg.getStorageID() + " assigned." );
        }
        // register new datanode
        datanodeMap.put(nodeReg.getStorageID(), 
                        new DatanodeInfo( nodeReg ) ) ;
        NameNode.stateChangeLog.debug(
            "BLOCK* NameSystem.registerDatanode: "
            + "node registered." );
        return;
      }

      // nodeS is found
      // The registering datanode is a replacement node for the existing 
      // data storage, which from now on will be served by a new node.
      NameNode.stateChangeLog.debug(
          "BLOCK* NameSystem.registerDatanode: "
          + "node " + nodeS.name
          + " is replaced by " + nodeReg.getName() + "." );
      nodeS.name = nodeReg.getName();
      return;
    }
    
    /**
     * Get registrationID for datanodes based on the namespaceID.
     * 
     * @see #registerDatanode(DatanodeRegistration)
     * @see FSDirectory#newNamespaceID()
     * @return registration ID
     */
    public String getRegistrationID() {
      return "NS" + Integer.toString( dir.namespaceID );
    }
    
    /**
     * Generate new storage ID.
     * 
     * @return unique storage ID
     * 
     * Note: that collisions are still possible if somebody will try 
     * to bring in a data storage from a different cluster.
     */
    private String newStorageID() {
      String newID = null;
      while( newID == null ) {
        newID = "DS" + Integer.toString( r.nextInt() );
        if( datanodeMap.get( newID ) != null )
          newID = null;
      }
      return newID;
    }
    
    /**
     * The given node has reported in.  This method should:
     * 1) Record the heartbeat, so the datanode isn't timed out
     * 2) Adjust usage stats for future block allocation
     */
    public synchronized void gotHeartbeat(DatanodeID nodeID,
                                          long capacity, 
                                          long remaining,
                                          int xceiverCount) throws IOException {
      synchronized (heartbeats) {
        synchronized (datanodeMap) {
          long capacityDiff = 0;
          long remainingDiff = 0;
          DatanodeInfo nodeinfo = getDatanode( nodeID );
          deaddatanodeMap.remove(nodeID.getName());

          if (nodeinfo == null) {
            NameNode.stateChangeLog.debug("BLOCK* NameSystem.gotHeartbeat: "
                    +"brand-new heartbeat from "+nodeID.getName() );

            nodeinfo = new DatanodeInfo(nodeID, capacity, remaining, xceiverCount);
            datanodeMap.put(nodeinfo.getStorageID(), nodeinfo);
            capacityDiff = capacity;
            remainingDiff = remaining;
          } else {
            capacityDiff = capacity - nodeinfo.getCapacity();
            remainingDiff = remaining - nodeinfo.getRemaining();
            heartbeats.remove(nodeinfo);
            nodeinfo.updateHeartbeat(capacity, remaining, xceiverCount);
          }
          heartbeats.add(nodeinfo);
          totalCapacity += capacityDiff;
          totalRemaining += remainingDiff;
        }
      }
    }

    /**
     * Periodically calls heartbeatCheck().
     */
    class HeartbeatMonitor implements Runnable {
        /**
         */
        public void run() {
            while (fsRunning) {
                heartbeatCheck();
                try {
                    Thread.sleep(heartBeatRecheck);
                } catch (InterruptedException ie) {
                }
            }
        }
    }

    /**
     * remove a datanode info
     * @param name: datanode name
     * @author hairong
     */
    synchronized public void removeDatanode( DatanodeID nodeID ) 
    throws IOException {
      DatanodeInfo nodeInfo = getDatanode( nodeID );
      if (nodeInfo != null) {
        removeDatanode( nodeInfo );
      } else {
          NameNode.stateChangeLog.warn("BLOCK* NameSystem.removeDatanode: "
                  + nodeInfo.getName() + " does not exist");
      }
  }
  
  /**
   * remove a datanode info
   * @param nodeInfo: datanode info
   * @author hairong
   */
    private void removeDatanode( DatanodeInfo nodeInfo ) {
      heartbeats.remove(nodeInfo);
      datanodeMap.remove(nodeInfo.getStorageID());
      deaddatanodeMap.put(nodeInfo.getName(), nodeInfo);
      NameNode.stateChangeLog.debug("BLOCK* NameSystem.removeDatanode: "
              + nodeInfo.getName() + " is removed from datanodeMap");
      totalCapacity -= nodeInfo.getCapacity();
      totalRemaining -= nodeInfo.getRemaining();

      Block deadblocks[] = nodeInfo.getBlocks();
      if( deadblocks != null )
        for( int i = 0; i < deadblocks.length; i++ )
          removeStoredBlock(deadblocks[i], nodeInfo);
    }

    /**
     * Check if there are any expired heartbeats, and if so,
     * whether any blocks have to be re-replicated.
     */
    synchronized void heartbeatCheck() {
      synchronized (heartbeats) {
        DatanodeInfo nodeInfo = null;

        while ((heartbeats.size() > 0) &&
               ((nodeInfo = (DatanodeInfo) heartbeats.first()) != null) &&
               (nodeInfo.lastUpdate() < System.currentTimeMillis() - EXPIRE_INTERVAL)) {
          NameNode.stateChangeLog.info("BLOCK* NameSystem.heartbeatCheck: "
              + "lost heartbeat from " + nodeInfo.getName());
          removeDatanode( nodeInfo );
          if (heartbeats.size() > 0) {
              nodeInfo = (DatanodeInfo) heartbeats.first();
          }
        }
      }
    }
    
    /**
     * The given node is reporting all its blocks.  Use this info to 
     * update the (machine-->blocklist) and (block-->machinelist) tables.
     */
    public synchronized Block[] processReport(DatanodeID nodeID, 
                                              Block newReport[]
                                            ) throws IOException {
        NameNode.stateChangeLog.debug("BLOCK* NameSystem.processReport: "
          +"from "+nodeID.getName()+" "+newReport.length+" blocks" );
        DatanodeInfo node = getDatanode( nodeID );

        //
        // Modify the (block-->datanode) map, according to the difference
        // between the old and new block report.
        //
        int oldPos = 0, newPos = 0;
        Block oldReport[] = node.getBlocks();
        while (oldReport != null && newReport != null && oldPos < oldReport.length && newPos < newReport.length) {
            int cmp = oldReport[oldPos].compareTo(newReport[newPos]);
            
            if (cmp == 0) {
                // Do nothing, blocks are the same
                oldPos++;
                newPos++;
            } else if (cmp < 0) {
                // The old report has a block the new one does not
                removeStoredBlock(oldReport[oldPos], node);
                oldPos++;
            } else {
                // The new report has a block the old one does not
                addStoredBlock(newReport[newPos], node);
                newPos++;
            }
        }
        while (oldReport != null && oldPos < oldReport.length) {
            // The old report has a block the new one does not
            removeStoredBlock(oldReport[oldPos], node);
            oldPos++;
        }
        while (newReport != null && newPos < newReport.length) {
            // The new report has a block the old one does not
            addStoredBlock(newReport[newPos], node);
            newPos++;
        }

        //
        // Modify node so it has the new blockreport
        //
        node.updateBlocks(newReport);

        //
        // We've now completely updated the node's block report profile.
        // We now go through all its blocks and find which ones are invalid,
        // no longer pending, or over-replicated.
        //
        // (Note it's not enough to just invalidate blocks at lease expiry 
        // time; datanodes can go down before the client's lease on 
        // the failed file expires and miss the "expire" event.)
        //
        // This function considers every block on a datanode, and thus
        // should only be invoked infrequently.
        //
        Vector obsolete = new Vector();
        for (Iterator it = node.getBlockIterator(); it.hasNext(); ) {
            Block b = (Block) it.next();

            if (! dir.isValidBlock(b) && ! pendingCreateBlocks.contains(b)) {
                obsolete.add(b);
                NameNode.stateChangeLog.debug("BLOCK* NameSystem.processReport: "
                        +"ask "+nodeID.getName()+" to delete "+b.getBlockName() );
            }
        }
        return (Block[]) obsolete.toArray(new Block[obsolete.size()]);
    }

    /**
     * Modify (block-->datanode) map.  Remove block from set of 
     * needed replications if this takes care of the problem.
     */
    synchronized void addStoredBlock(Block block, DatanodeInfo node) {
        TreeSet containingNodes = (TreeSet) blocksMap.get(block);
        if (containingNodes == null) {
            containingNodes = new TreeSet();
            blocksMap.put(block, containingNodes);
        }
        if (! containingNodes.contains(node)) {
            containingNodes.add(node);
            // 
            // Hairong: I would prefer to set the level of next logrecord
            // to be debug.
            // But at startup time, because too many new blocks come in
            // they simply take up all the space in the log file 
            // So I set the level to be trace
            //
            NameNode.stateChangeLog.trace("BLOCK* NameSystem.addStoredBlock: "
                    +"blockMap updated: "+node.getName()+" is added to "+block.getBlockName() );
        } else {
            NameNode.stateChangeLog.warn("BLOCK* NameSystem.addStoredBlock: "
                    + "Redundant addStoredBlock request received for " 
                    + block.getBlockName() + " on " + node.getName());
        }

        synchronized (neededReplications) {
            FSDirectory.INode fileINode = dir.getFileByBlock(block);
            if( fileINode == null )  // block does not belong to any file
                return;
            short fileReplication = fileINode.getReplication();
            if (containingNodes.size() >= fileReplication ) {
                neededReplications.remove(block);
                pendingReplications.remove(block);
                NameNode.stateChangeLog.trace("BLOCK* NameSystem.addStoredBlock: "
                        +block.getBlockName()+" has "+containingNodes.size()
                        +" replicas so is removed from neededReplications and pendingReplications" );
            } else {// containingNodes.size() < fileReplication
                neededReplications.add(block);
                NameNode.stateChangeLog.debug("BLOCK* NameSystem.addStoredBlock: "
                    +block.getBlockName()+" has only "+containingNodes.size()
                    +" replicas so is added to neededReplications" );
            }

            proccessOverReplicatedBlock( block, fileReplication );
        }
    }
    
    /**
     * Find how many of the containing nodes are "extra", if any.
     * If there are any extras, call chooseExcessReplicates() to
     * mark them in the excessReplicateMap.
     */
    private void proccessOverReplicatedBlock( Block block, short replication ) {
      TreeSet containingNodes = (TreeSet) blocksMap.get(block);
      if( containingNodes == null )
        return;
      Vector nonExcess = new Vector();
      for (Iterator it = containingNodes.iterator(); it.hasNext(); ) {
          DatanodeInfo cur = (DatanodeInfo) it.next();
          TreeSet excessBlocks = (TreeSet) excessReplicateMap.get(cur.getStorageID());
          if (excessBlocks == null || ! excessBlocks.contains(block)) {
              nonExcess.add(cur);
          }
      }
      chooseExcessReplicates(nonExcess, block, replication);    
    }

    /**
     * We want "replication" replicates for the block, but we now have too many.  
     * In this method, copy enough nodes from 'srcNodes' into 'dstNodes' such that:
     *
     * srcNodes.size() - dstNodes.size() == replication
     *
     * For now, we choose nodes randomly.  In the future, we might enforce some
     * kind of policy (like making sure replicates are spread across racks).
     */
    void chooseExcessReplicates(Vector nonExcess, Block b, short replication) {
        while (nonExcess.size() - replication > 0) {
            int chosenNode = r.nextInt(nonExcess.size());
            DatanodeInfo cur = (DatanodeInfo) nonExcess.elementAt(chosenNode);
            nonExcess.removeElementAt(chosenNode);

            TreeSet excessBlocks = (TreeSet) excessReplicateMap.get(cur.getStorageID());
            if (excessBlocks == null) {
                excessBlocks = new TreeSet();
                excessReplicateMap.put(cur.getStorageID(), excessBlocks);
            }
            excessBlocks.add(b);
            NameNode.stateChangeLog.debug("BLOCK* NameSystem.chooseExcessReplicates: "
                    +"("+cur.getName()+", "+b.getBlockName()+") is added to excessReplicateMap" );

            //
            // The 'excessblocks' tracks blocks until we get confirmation
            // that the datanode has deleted them; the only way we remove them
            // is when we get a "removeBlock" message.  
            //
            // The 'invalidate' list is used to inform the datanode the block 
            // should be deleted.  Items are removed from the invalidate list
            // upon giving instructions to the namenode.
            //
            Vector invalidateSet = (Vector) recentInvalidateSets.get(cur.getStorageID());
            if (invalidateSet == null) {
                invalidateSet = new Vector();
                recentInvalidateSets.put(cur.getStorageID(), invalidateSet);
            }
            invalidateSet.add(b);
            NameNode.stateChangeLog.debug("BLOCK* NameSystem.chooseExcessReplicates: "
                    +"("+cur.getName()+", "+b.getBlockName()+") is added to recentInvalidateSets" );
        }
    }

    /**
     * Modify (block-->datanode) map.  Possibly generate 
     * replication tasks, if the removed block is still valid.
     */
    synchronized void removeStoredBlock(Block block, DatanodeInfo node) {
        NameNode.stateChangeLog.debug("BLOCK* NameSystem.removeStoredBlock: "
                +block.getBlockName() + " from "+node.getName() );
        TreeSet containingNodes = (TreeSet) blocksMap.get(block);
        if (containingNodes == null || ! containingNodes.contains(node)) {
            throw new IllegalArgumentException("No machine mapping found for block " + block + ", which should be at node " + node);
        }
        containingNodes.remove(node);
        //
        // It's possible that the block was removed because of a datanode
        // failure.  If the block is still valid, check if replication is
        // necessary.  In that case, put block on a possibly-will-
        // be-replicated list.
        //
        FSDirectory.INode fileINode = dir.getFileByBlock(block);
        if( fileINode != null && (containingNodes.size() < fileINode.getReplication())) {
            synchronized (neededReplications) {
                neededReplications.add(block);
            }
            NameNode.stateChangeLog.debug("BLOCK* NameSystem.removeStoredBlock: "
                    +block.getBlockName()+" has only "+containingNodes.size()
                    +" replicas so is added to neededReplications" );
        }

        //
        // We've removed a block from a node, so it's definitely no longer
        // in "excess" there.
        //
        TreeSet excessBlocks = (TreeSet) excessReplicateMap.get(node.getStorageID());
        if (excessBlocks != null) {
            excessBlocks.remove(block);
            NameNode.stateChangeLog.debug("BLOCK* NameSystem.removeStoredBlock: "
                    +block.getBlockName()+" is removed from excessBlocks" );
            if (excessBlocks.size() == 0) {
                excessReplicateMap.remove(node.getStorageID());
            }
        }
    }

    /**
     * The given node is reporting that it received a certain block.
     */
    public synchronized void blockReceived( DatanodeID nodeID,  
                                            Block block
                                          ) throws IOException {
        DatanodeInfo node = getDatanode( nodeID );
        if (node == null) {
            NameNode.stateChangeLog.warn("BLOCK* NameSystem.blockReceived: "
             + block.getBlockName() + " is received from an unrecorded node " 
             + nodeID.getName() );
            throw new IllegalArgumentException(
                "Unexpected exception.  Got blockReceived message from node " 
                + block.getBlockName() + ", but there is no info for it");
        }
        NameNode.stateChangeLog.debug("BLOCK* NameSystem.blockReceived: "
                +block.getBlockName()+" is received from " + nodeID.getName() );
        //
        // Modify the blocks->datanode map
        // 
        addStoredBlock(block, node);

        //
        // Supplement node's blockreport
        //
        node.addBlock(block);
    }

    /**
     * Total raw bytes
     */
    public long totalCapacity() {
        return totalCapacity;
    }

    /**
     * Total non-used raw bytes
     */
    public long totalRemaining() {
        return totalRemaining;
    }

    /**
     */
    public DatanodeInfo[] datanodeReport() {
        DatanodeInfo results[] = null;
        synchronized (heartbeats) {
            synchronized (datanodeMap) {
                results = new DatanodeInfo[datanodeMap.size()];
                int i = 0;
                for (Iterator it = datanodeMap.values().iterator(); it.hasNext(); ) {
                    DatanodeInfo cur = (DatanodeInfo) it.next();
                    results[i++] = cur;
                }
            }
        }
        return results;
    }

    
    /**
     */
    public void DFSNodesStatus(Vector live, Vector dead) {
        synchronized (heartbeats) {
            synchronized (datanodeMap) {
                live.addAll(datanodeMap.values());
                dead.addAll(deaddatanodeMap.values());
            }
        }
    }
    /** 
     */
    public DatanodeInfo getDataNodeInfo(String name) {
        UTF8 src = new UTF8(name);
        return (DatanodeInfo)datanodeMap.get(src);
    }
    /** 
     */
    public String getDFSNameNodeMachine() {
        return localMachine;
    }
    /**
     */ 
    public int getDFSNameNodePort() {
        return port;
    }
    /**
     */
    public Date getStartTime() {
        return startTime;
    }
    /////////////////////////////////////////////////////////
    //
    // These methods are called by the Namenode system, to see
    // if there is any work for a given datanode.
    //
    /////////////////////////////////////////////////////////

    /**
     * Check if there are any recently-deleted blocks a datanode should remove.
     */
    public synchronized Block[] blocksToInvalidate( DatanodeID nodeID ) {
        Vector invalidateSet = (Vector) recentInvalidateSets.remove( 
                                                      nodeID.getStorageID() );
 
        if (invalidateSet == null ) 
            return null;
        
        if(NameNode.stateChangeLog.isInfoEnabled()) {
            StringBuffer blockList = new StringBuffer();
            for( int i=0; i<invalidateSet.size(); i++ ) {
                blockList.append(' ');
                blockList.append(((Block)invalidateSet.elementAt(i)).getBlockName());
            }
            NameNode.stateChangeLog.debug("BLOCK* NameSystem.blockToInvalidate: "
                   +"ask "+nodeID.getName()+" to delete " + blockList );
        }
        return (Block[]) invalidateSet.toArray(new Block[invalidateSet.size()]);
    }

    /**
     * Return with a list of Block/DataNodeInfo sets, indicating
     * where various Blocks should be copied, ASAP.
     *
     * The Array that we return consists of two objects:
     * The 1st elt is an array of Blocks.
     * The 2nd elt is a 2D array of DatanodeInfo objs, identifying the
     *     target sequence for the Block at the appropriate index.
     *
     */
    public synchronized Object[] pendingTransfers(DatanodeInfo srcNode,
                                                  int xmitsInProgress) {
    synchronized (neededReplications) {
      Object results[] = null;
      int scheduledXfers = 0;

      if (neededReplications.size() > 0) {
        //
        // Go through all blocks that need replications. See if any
        // are present at the current node. If so, ask the node to
        // replicate them.
        //
        Vector replicateBlocks = new Vector();
        Vector replicateTargetSets = new Vector();
        for (Iterator it = neededReplications.iterator(); it.hasNext();) {
          //
          // We can only reply with 'maxXfers' or fewer blocks
          //
          if (scheduledXfers >= this.maxReplicationStreams - xmitsInProgress) {
            break;
          }

          Block block = (Block) it.next();
          long blockSize = block.getNumBytes();
          FSDirectory.INode fileINode = dir.getFileByBlock(block);
          if (fileINode == null) { // block does not belong to any file
            it.remove();
          } else {
            TreeSet containingNodes = (TreeSet) blocksMap.get(block);
            TreeSet excessBlocks = (TreeSet) excessReplicateMap.get( 
                                                      srcNode.getStorageID() );
            // srcNode must contain the block, and the block must
            // not be scheduled for removal on that node
            if (containingNodes.contains(srcNode)
                && (excessBlocks == null || ! excessBlocks.contains(block))) {
              DatanodeInfo targets[] = chooseTargets(
                  Math.min( fileINode.getReplication() - containingNodes.size(),
                            this.maxReplicationStreams - xmitsInProgress), 
                  containingNodes, null, blockSize);
              if (targets.length > 0) {
                // Build items to return
                replicateBlocks.add(block);
                replicateTargetSets.add(targets);
                scheduledXfers += targets.length;
              }
            }
          }
        }

        //
        // Move the block-replication into a "pending" state.
        // The reason we use 'pending' is so we can retry
        // replications that fail after an appropriate amount of time.
        // (REMIND - mjc - this timer is not yet implemented.)
        //
        if (replicateBlocks.size() > 0) {
          int i = 0;
          for (Iterator it = replicateBlocks.iterator(); it.hasNext(); i++) {
            Block block = (Block) it.next();
            DatanodeInfo targets[] = 
                      (DatanodeInfo[]) replicateTargetSets.elementAt(i);
            TreeSet containingNodes = (TreeSet) blocksMap.get(block);

            if (containingNodes.size() + targets.length >= 
                    dir.getFileByBlock( block).getReplication() ) {
              neededReplications.remove(block);
              pendingReplications.add(block);
              NameNode.stateChangeLog.debug(
                "BLOCK* NameSystem.pendingTransfer: "
                + block.getBlockName()
                + " is removed from neededReplications to pendingReplications");
            }

            if (NameNode.stateChangeLog.isInfoEnabled()) {
              StringBuffer targetList = new StringBuffer("datanode(s)");
              for (int k = 0; k < targets.length; k++) {
                targetList.append(' ');
                targetList.append(targets[k].getName());
              }
              NameNode.stateChangeLog.info(
                      "BLOCK* NameSystem.pendingTransfer: " + "ask "
                      + srcNode.getName() + " to replicate "
                      + block.getBlockName() + " to " + targetList);
            }
          }

          //
          // Build returned objects from above lists
          //
          DatanodeInfo targetMatrix[][] = 
                        new DatanodeInfo[replicateTargetSets.size()][];
          for (i = 0; i < targetMatrix.length; i++) {
            targetMatrix[i] = (DatanodeInfo[]) replicateTargetSets.elementAt(i);
          }

          results = new Object[2];
          results[0] = replicateBlocks.toArray(new Block[replicateBlocks.size()]);
          results[1] = targetMatrix;
        }
      }
      return results;
    }
  }

    /**
     * Get a certain number of targets, if possible.
     * If not, return as many as we can.
     * @param desiredReplicates
     *          number of duplicates wanted.
     * @param forbiddenNodes
     *          of DatanodeInfo instances that should not be considered targets.
     * @return array of DatanodeInfo instances uses as targets.
     */
    DatanodeInfo[] chooseTargets(int desiredReplicates, TreeSet forbiddenNodes,
                                 UTF8 clientMachine, long blockSize) {
        if (desiredReplicates > datanodeMap.size()) {
          LOG.warn("Replication requested of "+desiredReplicates
                      +" is larger than cluster size ("+datanodeMap.size()
                      +"). Using cluster size.");
          desiredReplicates  = datanodeMap.size();
        }

        TreeSet alreadyChosen = new TreeSet();
        Vector targets = new Vector();

        for (int i = 0; i < desiredReplicates; i++) {
            DatanodeInfo target = chooseTarget(forbiddenNodes, alreadyChosen, 
                                               clientMachine, blockSize);
            if (target == null)
              break; // calling chooseTarget again won't help
            targets.add(target);
            alreadyChosen.add(target);
        }
        return (DatanodeInfo[]) targets.toArray(new DatanodeInfo[targets.size()]);
    }

    /**
     * Choose a target from available machines, excepting the
     * given ones.
     *
     * Right now it chooses randomly from available boxes.  In future could 
     * choose according to capacity and load-balancing needs (or even 
     * network-topology, to avoid inter-switch traffic).
     * @param forbidden1 DatanodeInfo targets not allowed, null allowed.
     * @param forbidden2 DatanodeInfo targets not allowed, null allowed.
     * @return DatanodeInfo instance to use or null if something went wrong
     * (a log message is emitted if null is returned).
     */
    DatanodeInfo chooseTarget(TreeSet forbidden1, TreeSet forbidden2, 
                              UTF8 clientMachine, long blockSize) {
        //
        // Check if there are any available targets at all
        //
        int totalMachines = datanodeMap.size();
        if (totalMachines == 0) {
            LOG.warn("While choosing target, totalMachines is " + totalMachines);
            return null;
        }

        //
        // Build a map of forbidden hostnames from the two forbidden sets.
        //
        TreeSet forbiddenMachines = new TreeSet();
        if (forbidden1 != null) {
            for (Iterator it = forbidden1.iterator(); it.hasNext(); ) {
                DatanodeInfo cur = (DatanodeInfo) it.next();
                forbiddenMachines.add(cur.getHost());
            }
        }
        if (forbidden2 != null) {
            for (Iterator it = forbidden2.iterator(); it.hasNext(); ) {
                DatanodeInfo cur = (DatanodeInfo) it.next();
                forbiddenMachines.add(cur.getHost());
            }
        }

        double avgLoad = 0.0;
        //
        // Build list of machines we can actually choose from
        //
        Vector targetList = new Vector();
        for (Iterator it = datanodeMap.values().iterator(); it.hasNext(); ) {
            DatanodeInfo node = (DatanodeInfo) it.next();
            if (! forbiddenMachines.contains(node.getHost())) {
                targetList.add(node);
                avgLoad += node.getXceiverCount();
            }
        }
        if (targetList.size() > 0) { avgLoad = avgLoad/targetList.size(); }
        
        Collections.shuffle(targetList);
        
        //
        // Now pick one
        //
        if (targetList.size() > 0) {
            //
            // If the requester's machine is in the targetList, 
            // and it's got the capacity, pick it.
            //
            if (clientMachine != null && clientMachine.getLength() > 0) {
                for (Iterator it = targetList.iterator(); it.hasNext(); ) {
                    DatanodeInfo node = (DatanodeInfo) it.next();
                    if (clientMachine.equals(node.getHost())) {
                        if ((node.getRemaining() > blockSize * MIN_BLOCKS_FOR_WRITE) &&
                            (node.getXceiverCount() < (2.0 * avgLoad))) {
                            return node;
                        }
                    }
                }
            }

            //
            // Otherwise, choose node according to target capacity
            //
            for (Iterator it = targetList.iterator(); it.hasNext(); ) {
                DatanodeInfo node = (DatanodeInfo) it.next();
                if ((node.getRemaining() > blockSize * MIN_BLOCKS_FOR_WRITE) &&
                    (node.getXceiverCount() < (2.0 * avgLoad))) {
                    return node;
                }
            }

            //
            // That should do the trick.  But we might not be able
            // to pick any node if the target was out of bytes.  As
            // a last resort, pick the first valid one we can find.
            //
            for (Iterator it = targetList.iterator(); it.hasNext(); ) {
                DatanodeInfo node = (DatanodeInfo) it.next();
                if (node.getRemaining() > blockSize) {
                    return node;
                }
            }
            LOG.warn("Could not find any nodes with sufficient capacity");
            return null;
        } else {
            LOG.warn("Zero targets found, forbidden1.size=" +
                ( forbidden1 != null ? forbidden1.size() : 0 ) +
                " forbidden2.size()=" +
                ( forbidden2 != null ? forbidden2.size() : 0 ));
            return null;
        }
    }

    /**
     * Information about the file while it is being written to.
     * Note that at that time the file is not visible to the outside.
     * 
     * This class contains a <code>Vector</code> of {@link Block}s that has
     * been written into the file so far, and file replication. 
     * 
     * @author shv
     */
    private class FileUnderConstruction {
      private short blockReplication; // file replication
      private long blockSize;
      private Vector blocks;
      private UTF8 clientName;         // lease holder
      private UTF8 clientMachine;
      
      FileUnderConstruction(short replication,
                            long blockSize,
                            UTF8 clientName,
                            UTF8 clientMachine) throws IOException {
        this.blockReplication = replication;
        this.blockSize = blockSize;
        this.blocks = new Vector();
        this.clientName = clientName;
        this.clientMachine = clientMachine;
      }
      
      public short getReplication() {
        return this.blockReplication;
      }
      
      public long getBlockSize() {
        return blockSize;
      }
      
      public Vector getBlocks() {
        return blocks;
      }
      
      public UTF8 getClientName() {
        return clientName;
      }
      
      public UTF8 getClientMachine() {
        return clientMachine;
      }
    }

    /**
     * Get data node by storage ID.
     * 
     * @param nodeID
     * @return DatanodeInfo or null if the node is not found.
     * @throws IOException
     */
    public DatanodeInfo getDatanode( DatanodeID nodeID ) throws IOException {
      UnregisteredDatanodeException e = null;
      DatanodeInfo node = (DatanodeInfo) datanodeMap.get(nodeID.getStorageID());
      if (node == null) 
        return null;
      if (!node.getName().equals(nodeID.getName())) {
        e = new UnregisteredDatanodeException( nodeID, node );
        NameNode.stateChangeLog.fatal("BLOCK* NameSystem.getDatanode: "
            + e.getLocalizedMessage() );
        throw e;
      }
      return node;
    }
    
    /**
     * Find data node by its name.
     * 
     * This method is called when the node is registering.
     * Not performance critical.
     * Otherwise an additional tree-like structure will be required.
     * 
     * @param name
     * @return DatanodeInfo if found or null otherwise 
     * @throws IOException
     */
    public DatanodeInfo getDatanodeByName( String name ) throws IOException {
      for (Iterator it = datanodeMap.values().iterator(); it.hasNext(); ) {
        DatanodeInfo node = (DatanodeInfo) it.next();
        if( node.getName().equals(name) )
           return node;
      }
      return null;
    }
}
