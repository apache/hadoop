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

import org.apache.hadoop.io.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.mapred.StatusHttpServer;
import org.apache.hadoop.net.NetworkTopology;
import org.apache.hadoop.net.NodeBase;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.ipc.Server;

import java.io.*;
import java.util.*;

import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

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
    // Mapping: Block -> TreeSet<DatanodeDescriptor>
    //
    Map<Block, List<DatanodeDescriptor>> blocksMap = 
                              new HashMap<Block, List<DatanodeDescriptor>>();

    /**
     * Stores the datanode -> block map.  
     * <p>
     * Done by storing a set of {@link DatanodeDescriptor} objects, sorted by 
     * storage id. In order to keep the storage map consistent it tracks 
     * all storages ever registered with the namenode.
     * A descriptor corresponding to a specific storage id can be
     * <ul> 
     * <li>added to the map if it is a new storage id;</li>
     * <li>updated with a new datanode started as a replacement for the old one 
     * with the same storage id; and </li>
     * <li>removed if and only if an existing datanode is restarted to serve a
     * different storage id.</li>
     * </ul> <br>
     * The list of the {@link DatanodeDescriptor}s in the map is checkpointed
     * in the namespace image file. Only the {@link DatanodeInfo} part is 
     * persistent, the list of blocks is restored from the datanode block
     * reports. 
     * <p>
     * Mapping: StorageID -> DatanodeDescriptor
     */
    Map<String, DatanodeDescriptor> datanodeMap = 
                                      new TreeMap<String, DatanodeDescriptor>();

    //
    // Keeps a Collection for every named machine containing
    // blocks that have recently been invalidated and are thought to live
    // on the machine in question.
    // Mapping: StorageID -> ArrayList<Block>
    //
    private Map<String, Collection<Block>> recentInvalidateSets = 
                                      new TreeMap<String, Collection<Block>>();

    //
    // Keeps a TreeSet for every named node.  Each treeset contains
    // a list of the blocks that are "extra" at that location.  We'll
    // eventually remove these extras.
    // Mapping: StorageID -> TreeSet<Block>
    //
    private Map<String, Collection<Block>> excessReplicateMap = 
                                      new TreeMap<String, Collection<Block>>();

    //
    // Keeps track of files that are being created, plus the
    // blocks that make them up.
    // Mapping: fileName -> FileUnderConstruction
    //
    Map<UTF8, FileUnderConstruction> pendingCreates = 
                                  new TreeMap<UTF8, FileUnderConstruction>();

    //
    // Keeps track of the blocks that are part of those pending creates
    // Set of: Block
    //
    Collection<Block> pendingCreateBlocks = new TreeSet<Block>();

    //
    // Stats on overall usage
    //
    long totalCapacity = 0, totalRemaining = 0;

    // total number of connections per live datanode
    int totalLoad = 0;


    //
    // For the HTTP browsing interface
    //
    StatusHttpServer infoServer;
    int infoPort;
    String infoBindAddress;
    Date startTime;
    
    //
    Random r = new Random();

    /**
     * Stores a set of DatanodeDescriptor objects.
     * This is a subset of {@link #datanodeMap}, containing nodes that are 
     * considered alive.
     * The {@link HeartbeatMonitor} periodically checks for outdated entries,
     * and removes them from the list.
     */
    ArrayList<DatanodeDescriptor> heartbeats = new ArrayList<DatanodeDescriptor>();

    //
    // Store set of Blocks that need to be replicated 1 or more times.
    // We also store pending replication-orders.
    // Set of: Block
    //
    private UnderReplicationBlocks neededReplications = new UnderReplicationBlocks();
    private PendingReplicationBlocks pendingReplications;

    //
    // Used for handling lock-leases
    // Mapping: leaseHolder -> Lease
    //
    private Map<UTF8, Lease> leases = new TreeMap<UTF8, Lease>();
    // Set of: Lease
    private SortedSet<Lease> sortedLeases = new TreeSet<Lease>();

    //
    // Threaded object that checks to see if we have been
    // getting heartbeats from all clients. 
    //
    Daemon hbthread = null;   // HeartbeatMonitor thread
    Daemon lmthread = null;   // LeaseMonitor thread
    Daemon smmthread = null;  // SafeModeMonitor thread
    Daemon replthread = null;  // Replication thread
    boolean fsRunning = true;
    long systemStart = 0;

    //  The maximum number of replicates we should allow for a single block
    private int maxReplication;
    //  How many outgoing replication streams a given node should have at one time
    private int maxReplicationStreams;
    // MIN_REPLICATION is how many copies we need in place or else we disallow the write
    private int minReplication;
    // Default replication
    private int defaultReplication;
    // heartbeatRecheckInterval is how often namenode checks for expired datanodes
    private long heartbeatRecheckInterval;
    // heartbeatExpireInterval is how long namenode waits for datanode to report
    // heartbeat
    private long heartbeatExpireInterval;
    //replicationRecheckInterval is how often namenode checks for new replication work
    private long replicationRecheckInterval;
    static int replIndex = 0; // last datanode used for replication work
    static int REPL_WORK_PER_ITERATION = 32; // max percent datanodes per iteration

    public static FSNamesystem fsNamesystemObject;
    private String localMachine;
    private int port;
    private SafeModeInfo safeMode;  // safe mode information
    
    // datanode networktoplogy
    NetworkTopology clusterMap = new NetworkTopology();
    // for block replicas placement
    ReplicationTargetChooser replicator = new ReplicationTargetChooser();

    private HostsFileReader hostsReader; 
    private Daemon dnthread = null;

    /**
     * dirs is a list oif directories where the filesystem directory state 
     * is stored
     */
    public FSNamesystem(File[] dirs, 
                        String hostname,
                        int port,
                        NameNode nn, Configuration conf) throws IOException {
        fsNamesystemObject = this;
        this.defaultReplication = conf.getInt("dfs.replication", 3);
        this.maxReplication = conf.getInt("dfs.replication.max", 512);
        this.minReplication = conf.getInt("dfs.replication.min", 1);
        if( minReplication <= 0 )
          throw new IOException(
              "Unexpected configuration parameters: dfs.replication.min = " 
              + minReplication
              + " must be greater than 0" );
        if( maxReplication >= (int)Short.MAX_VALUE )
          throw new IOException(
              "Unexpected configuration parameters: dfs.replication.max = " 
              + maxReplication + " must be less than " + (Short.MAX_VALUE) );
        if( maxReplication < minReplication )
          throw new IOException(
              "Unexpected configuration parameters: dfs.replication.min = " 
              + minReplication
              + " must be less than dfs.replication.max = " 
              + maxReplication );
        this.maxReplicationStreams = conf.getInt("dfs.max-repl-streams", 2);
        long heartbeatInterval = conf.getLong("dfs.heartbeat.interval", 3) * 1000;
        this.heartbeatRecheckInterval = 5 * 60 * 1000; // 5 minutes
        this.heartbeatExpireInterval = 2 * heartbeatRecheckInterval +
            10 * heartbeatInterval;
        this.replicationRecheckInterval = 3 * 1000; //  3 second

        this.localMachine = hostname;
        this.port = port;
        this.dir = new FSDirectory(dirs);
        this.dir.loadFSImage( conf );
        this.safeMode = new SafeModeInfo( conf );
        setBlockTotal();
        pendingReplications = new PendingReplicationBlocks(LOG);
        this.hbthread = new Daemon(new HeartbeatMonitor());
        this.lmthread = new Daemon(new LeaseMonitor());
        this.replthread = new Daemon(new ReplicationMonitor());
        hbthread.start();
        lmthread.start();
        replthread.start();
        this.systemStart = now();
        this.startTime = new Date(systemStart); 
        
        this.hostsReader = new HostsFileReader(conf.get("dfs.hosts",""),
                                               conf.get("dfs.hosts.exclude",""));
        this.dnthread = new Daemon(new DecommissionedMonitor());
        dnthread.start();

        this.infoPort = conf.getInt("dfs.info.port", 50070);
        this.infoBindAddress = conf.get("dfs.info.bindAddress", "0.0.0.0");
        this.infoServer = new StatusHttpServer("dfs",infoBindAddress, infoPort, false);
        this.infoServer.setAttribute("name.system", this);
        this.infoServer.setAttribute("name.node", nn);
        this.infoServer.setAttribute("name.conf", conf);
        this.infoServer.addServlet("fsck", "/fsck", FsckServlet.class);
        this.infoServer.addServlet("getimage", "/getimage", GetImageServlet.class);
        this.infoServer.start();
    }

    /**
     * dirs is a list of directories where the filesystem directory state 
     * is stored
     */
    FSNamesystem(FSImage fsImage) throws IOException {
        fsNamesystemObject = this;
        this.dir = new FSDirectory(fsImage);
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
            pendingReplications.stop();
            infoServer.stop();
            hbthread.join(3000);
            replthread.join(3000);
            dnthread.join(3000);
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
    
    /* get replication factor of a block */
    private int getReplication( Block block ) {
        FSDirectory.INode fileINode = dir.getFileByBlock(block);
        if( fileINode == null ) { // block does not belong to any file
            return 0;
        } else {
            return fileINode.getReplication();
        }
    }

    /* Class for keeping track of under replication blocks
     * Blocks have replication priority, with priority 0 indicating the highest
     * Blocks have only one replicas has the highest
     */
    private class UnderReplicationBlocks {
        private static final int LEVEL = 3;
        TreeSet<Block>[] priorityQueues = new TreeSet[LEVEL];
        
        /* constructor */
        UnderReplicationBlocks() {
            for(int i=0; i<LEVEL; i++) {
                priorityQueues[i] = new TreeSet<Block>();
            }
        }
        
        /* Return the total number of under replication blocks */
        synchronized int size() {
            int size = 0;
            for( int i=0; i<LEVEL; i++ ) {
                size += priorityQueues[i].size();
            }
            return size;
        }
        
        /* Check if a block is in the neededReplication queue */
        synchronized boolean contains(Block block) {
            for(TreeSet<Block> set:priorityQueues) {
                if(set.contains(block)) return true;
            }
            return false;
        }
        
        /* Return the priority of a block
        * @param block a under replication block
        * @param curReplicas current number of replicas of the block
        * @param expectedReplicas expected number of replicas of the block
        */
        private int getPriority(Block block, 
                int curReplicas, int expectedReplicas) {
            if (curReplicas==0 || curReplicas>=expectedReplicas) {
                return LEVEL; // no need to replicate
            } else if(curReplicas==1) {
                return 0; // highest priority
            } else if(curReplicas*3<expectedReplicas) {
                return 1;
            } else {
                return 2;
            }
        }
        
        /* add a block to a under replication queue according to its priority
         * @param block a under replication block
         * @param curReplicas current number of replicas of the block
         * @param expectedReplicas expected number of replicas of the block
         */
        synchronized boolean add(
            Block block, int curReplicas, int expectedReplicas) {
            if(expectedReplicas <= curReplicas) {
                return false;
            }
            int priLevel = getPriority(block, curReplicas, expectedReplicas);
            if( priorityQueues[priLevel].add(block) ) {
                NameNode.stateChangeLog.debug(
                        "BLOCK* NameSystem.UnderReplicationBlock.add:"
                      + block.getBlockName()
                      + " has only "+curReplicas
                      + " replicas and need " + expectedReplicas
                      + " replicas so is added to neededReplications"
                      + " at priority level " + priLevel );
                return true;
            }
            return false;
        }

        /* add a block to a under replication queue */
        synchronized boolean add(Block block) {
            int curReplicas = countContainingNodes(blocksMap.get(block));
            int expectedReplicas = getReplication(block);
            return add(block, curReplicas, expectedReplicas);
        }
        
        /* remove a block from a under replication queue */
        synchronized boolean remove(Block block, 
                int oldReplicas, int oldExpectedReplicas) {
            if(oldExpectedReplicas <= oldReplicas) {
                return false;
            }
            int priLevel = getPriority(block, oldReplicas, oldExpectedReplicas);
            return remove(block, priLevel);
        }
        
        /* remove a block from a under replication queue given a priority*/
        private boolean remove(Block block, int priLevel ) {
            if( priorityQueues[priLevel].remove(block) ) {
                NameNode.stateChangeLog.debug(
                     "BLOCK* NameSystem.UnderReplicationBlock.remove: "
                   + "Removing block " + block.getBlockName()
                   + " from priority queue "+ priLevel );
                return true;
            } else {
                for(int i=0; i<LEVEL; i++) {
                    if( i!=priLevel && priorityQueues[i].remove(block) ) {
                        NameNode.stateChangeLog.debug(
                             "BLOCK* NameSystem.UnderReplicationBlock.remove: "
                           + "Removing block " + block.getBlockName()
                           + " from priority queue "+ i );
                        return true;
                    }
                }
            }
            return false;
        }
        
        /* remove a block from a under replication queue */
        synchronized boolean remove(Block block) {
            int curReplicas = countContainingNodes(blocksMap.get(block));
            int expectedReplicas = getReplication(block);
            return remove(block, curReplicas, expectedReplicas);
        }
        
        /* update the priority level of a block */
        synchronized void update(Block block,
                int curReplicasDelta, int expectedReplicasDelta) {
            int curReplicas = countContainingNodes(blocksMap.get(block));
            int curExpectedReplicas = getReplication(block);
            int oldReplicas = curReplicas-curReplicasDelta;
            int oldExpectedReplicas = curExpectedReplicas-expectedReplicasDelta;
            int curPri = getPriority(block, curReplicas, curExpectedReplicas);
            int oldPri = getPriority(block, oldReplicas, oldExpectedReplicas);
            NameNode.stateChangeLog.debug("UnderReplicationBlocks.update " + 
                               block +
                               " curReplicas " + curReplicas +
                               " curExpectedReplicas " + curExpectedReplicas +
                               " oldReplicas " + oldReplicas +
                               " oldExpectedReplicas  " + oldExpectedReplicas +
                               " curPri  " + curPri +
                               " oldPri  " + oldPri);
            if( oldPri != LEVEL && oldPri != curPri ) {
                remove(block, oldPri);
            }
            if( curPri != LEVEL && oldPri != curPri 
                    && priorityQueues[curPri].add(block)) {
                NameNode.stateChangeLog.debug(
                        "BLOCK* NameSystem.UnderReplicationBlock.update:"
                      + block.getBlockName()
                      + " has only "+curReplicas
                      + " replicas and need " + curExpectedReplicas
                      + " replicas so is added to neededReplications"
                      + " at priority level " + curPri );
            }
        }
        
        /* return a iterator of all the under replication blocks */
        synchronized Iterator<Block> iterator() {
            return new Iterator<Block>() {
                int level;
                Iterator<Block>[] iterator = new Iterator[LEVEL];
                
                {
                    level=0;
                    for(int i=0; i<LEVEL; i++) {
                        iterator[i] = priorityQueues[i].iterator();
                    }
                }
                
                private void update() {
                    while( level< LEVEL-1 && !iterator[level].hasNext()  ) {
                        level++;
                    }
                }
                
                public Block next() {
                    update();
                    return iterator[level].next();
                }
                
                public boolean hasNext() {
                    update();
                    return iterator[level].hasNext();
                }
                
                public void remove() {
                    iterator[level].remove();
                }
            };
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
    public Object[] open(String clientMachine, UTF8 src) {
        Object results[] = null;
        Block blocks[] = dir.getFile(src);
        if (blocks != null) {
            results = new Object[2];
            DatanodeDescriptor machineSets[][] = new DatanodeDescriptor[blocks.length][];

            for (int i = 0; i < blocks.length; i++) {
                Collection<DatanodeDescriptor> containingNodes = blocksMap.get(blocks[i]);
                if (containingNodes == null) {
                    machineSets[i] = new DatanodeDescriptor[0];
                } else {
                    machineSets[i] = new DatanodeDescriptor[containingNodes.size()];
                    ArrayList<DatanodeDescriptor> containingNodesList =
                      new ArrayList<DatanodeDescriptor>(containingNodes.size());
                    containingNodesList.addAll(containingNodes);
                    
                    machineSets[i] = replicator.sortByDistance(
                        getDatanodeByHost(clientMachine), containingNodesList);
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
    public synchronized boolean setReplication(String src, 
                                               short replication
                                              ) throws IOException {
      if( isInSafeMode() )
        throw new SafeModeException( "Cannot set replication for " + src, safeMode );
      verifyReplication(src, replication, null );

      Vector<Integer> oldReplication = new Vector<Integer>();
      Block[] fileBlocks;
      fileBlocks = dir.setReplication( src, replication, oldReplication );
      if( fileBlocks == null )  // file not found or is a directory
        return false;
      int oldRepl = oldReplication.elementAt(0).intValue();
      if( oldRepl == replication ) // the same replication
        return true;

      // update needReplication priority queues
      LOG.info("Increasing replication for file " + src 
              + ". New replication is " + replication );
      for( int idx = 0; idx < fileBlocks.length; idx++ )
          neededReplications.update( fileBlocks[idx], 0, replication-oldRepl );
      
      if( oldRepl > replication ) {  
        // old replication > the new one; need to remove copies
        LOG.info("Reducing replication for file " + src 
                + ". New replication is " + replication );
        for( int idx = 0; idx < fileBlocks.length; idx++ )
          proccessOverReplicatedBlock( fileBlocks[idx], replication );
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
      if( isInSafeMode() )
        throw new SafeModeException( "Cannot create file" + src, safeMode );
      if (!isValidName(src.toString())) {
        throw new IOException("Invalid file name: " + src);      	  
      }
      try {
        FileUnderConstruction pendingFile = pendingCreates.get(src);
        if (pendingFile != null) {
          //
          // If the file exists in pendingCreate, then it must be in our
          // leases. Find the appropriate lease record.
          //
          Lease lease = leases.get(holder);
          //
          // We found the lease for this file. And surprisingly the original
          // holder is trying to recreate this file. This should never occur.
          //
          if (lease != null) {
            throw new AlreadyBeingCreatedException(
                  "failed to create file " + src + " for " + holder +
                  " on client " + clientMachine + 
                  " because current leaseholder is trying to recreate file.");
          }
          //
          // Find the original holder.
          //
          UTF8 oldholder = pendingFile.getClientName();
          lease = leases.get(oldholder);
          if (lease == null) {
            throw new AlreadyBeingCreatedException(
                  "failed to create file " + src + " for " + holder +
                  " on client " + clientMachine + 
                  " because pendingCreates is non-null but no leases found.");
          }
          //
          // If the original holder has not renewed in the last SOFTLIMIT 
          // period, then reclaim all resources and allow this request 
          // to proceed. Otherwise, prevent this request from creating file.
          //
          if (lease.expiredSoftLimit()) {
            lease.releaseLocks();
            leases.remove(lease.holder);
            LOG.info("Removing lease " + lease + " ");
            if (!sortedLeases.remove(lease)) {
              LOG.error("Unknown failure trying to remove " + lease + 
                       " from lease set.");
            }
          } else  {
            throw new AlreadyBeingCreatedException(
                  "failed to create file " + src + " for " + holder +
                  " on client " + clientMachine + 
                  " because pendingCreates is non-null.");
          }
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
        DatanodeDescriptor targets[] = replicator.chooseTarget(replication,
            getDatanodeByHost(clientMachine.toString()), null, blockSize);
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
            Lease lease = leases.get(holder);
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
        if( isInSafeMode() )
          throw new SafeModeException( "Cannot add block to " + src, safeMode );
        FileUnderConstruction pendingFile = pendingCreates.get(src);
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
        String clientHost = pendingFile.getClientMachine().toString();
        DatanodeDescriptor targets[] = replicator.chooseTarget(
            (int)(pendingFile.getReplication()),
            getDatanodeByHost(clientHost),
            null,
            pendingFile.getBlockSize());
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
        FileUnderConstruction pendingFile = pendingCreates.get(src);
        if (pendingFile != null) {
            Collection<Block> pendingVector = pendingFile.getBlocks();
            for (Iterator<Block> it = pendingVector.iterator(); it.hasNext(); ) {
                Block cur = it.next();
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
        Lease lease = leases.get(holder);
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
    public synchronized int completeFile( UTF8 src, 
                                          UTF8 holder) throws IOException {
        NameNode.stateChangeLog.debug("DIR* NameSystem.completeFile: " + src + " for " + holder );
        if( isInSafeMode() )
          throw new SafeModeException( "Cannot complete file " + src, safeMode );
        if (dir.getFile(src) != null || pendingCreates.get(src) == null) {
            NameNode.stateChangeLog.warn( "DIR* NameSystem.completeFile: "
                    + "failed to complete " + src
                    + " because dir.getFile()==" + dir.getFile(src) 
                    + " and " + pendingCreates.get(src));
            return OPERATION_FAILED;
        } else if (! checkFileProgress(src)) {
            return STILL_WAITING;
        }
        
        FileUnderConstruction pendingFile = pendingCreates.get(src);
        Collection<Block> blocks = pendingFile.getBlocks();
        int nrBlocks = blocks.size();
        Block pendingBlocks[] = blocks.toArray(new Block[nrBlocks]);

        //
        // We have the pending blocks, but they won't have
        // length info in them (as they were allocated before
        // data-write took place). Find the block stored in
        // node descriptor.
        //
        for (int i = 0; i < nrBlocks; i++) {
            Block b = pendingBlocks[i];
            List<DatanodeDescriptor> containingNodes = blocksMap.get(b);
            Block storedBlock = 
                containingNodes.get(0).getBlock(b);
            if ( storedBlock != null ) {
                pendingBlocks[i] = storedBlock;
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
            Lease lease = leases.get(holder);
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
        int numExpectedReplicas = pendingFile.getReplication();
        for (int i = 0; i < nrBlocks; i++) {
          Collection<DatanodeDescriptor> containingNodes = blocksMap.get(pendingBlocks[i]);
          // filter out containingNodes that are marked for decommission.
          int numCurrentReplica = countContainingNodes(containingNodes);

            if (numCurrentReplica < numExpectedReplicas) {
                neededReplications.add(
                      pendingBlocks[i], numCurrentReplica, numExpectedReplicas);
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
        FileUnderConstruction v = pendingCreates.get(src);
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
        FileUnderConstruction v = pendingCreates.get(src);

        for (Iterator<Block> it = v.getBlocks().iterator(); it.hasNext(); ) {
            Block b = it.next();
            Collection<DatanodeDescriptor> containingNodes = blocksMap.get(b);
            if (containingNodes == null || containingNodes.size() < this.minReplication) {
                return false;
            }
        }
        return true;
    }

    /**
     * Adds block to list of blocks which will be invalidated on 
     * specified datanode.
     */
    private void addToInvalidates(Block b, DatanodeInfo n) {
      Collection<Block> invalidateSet = recentInvalidateSets.get(n.getStorageID());
      if (invalidateSet == null) {
        invalidateSet = new ArrayList<Block>();
        recentInvalidateSets.put(n.getStorageID(), invalidateSet);
      }
      invalidateSet.add(b);
    }

    /**
     * Invalidates the given block on the given datanode.
     */
    public synchronized void invalidateBlock(Block blk, DatanodeInfo dn)
        throws IOException {
      NameNode.stateChangeLog.info("DIR* NameSystem.invalidateBlock: " 
                                    + blk.getBlockName() + " on " 
                                    + dn.getName());
      if (isInSafeMode()) {
        throw new SafeModeException("Cannot invalidate block " + blk.getBlockName(), safeMode);
      }

      Collection<DatanodeDescriptor> containingNodes = blocksMap.get(blk);

      // Check how many copies we have of the block.  If we have at least one
      // copy on a live node, then we can delete it. 
      if (containingNodes != null ) {
        if ((countContainingNodes(containingNodes) > 1) || 
            ((countContainingNodes(containingNodes) == 1) &&
             (dn.isDecommissionInProgress() || dn.isDecommissioned()))) {
          addToInvalidates(blk, dn);
          removeStoredBlock(blk, getDatanode(dn));
          NameNode.stateChangeLog.info("BLOCK* NameSystem.invalidateBlocks: "
                                        + blk.getBlockName() + " on " 
                                        + dn.getName() + " listed for deletion.");
        } else {
          NameNode.stateChangeLog.info("BLOCK* NameSystem.invalidateBlocks: "
                                        + blk.getBlockName() + " on " 
                                        + dn.getName() + " is the only copy and was not deleted.");
        }
      }
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
    public synchronized boolean renameTo(UTF8 src, UTF8 dst) throws IOException {
        NameNode.stateChangeLog.debug("DIR* NameSystem.renameTo: " + src + " to " + dst );
        if( isInSafeMode() )
          throw new SafeModeException( "Cannot rename " + src, safeMode );
        if (!isValidName(dst.toString())) {
          throw new IOException("Invalid name: " + dst);
        }
        return dir.renameTo(src, dst);
    }

    /**
     * Remove the indicated filename from the namespace.  This may
     * invalidate some blocks that make up the file.
     */
    public synchronized boolean delete(UTF8 src) throws IOException {
        NameNode.stateChangeLog.debug("DIR* NameSystem.delete: " + src );
        if( isInSafeMode() )
          throw new SafeModeException( "Cannot delete " + src, safeMode );
        Block deletedBlocks[] = dir.delete(src);
        if (deletedBlocks != null) {
            for (int i = 0; i < deletedBlocks.length; i++) {
                Block b = deletedBlocks[i];

                Collection<DatanodeDescriptor> containingNodes = blocksMap.get(b);
                if (containingNodes != null) {
                    for (Iterator<DatanodeDescriptor> it = containingNodes.iterator(); it.hasNext(); ) {
                        DatanodeDescriptor node = it.next();
                        addToInvalidates(b, node);
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
     * Whether the pathname is valid.  Currently prohibits relative paths, 
     * and names which contain a ":" or "/" 
     */
    static boolean isValidName(String src) {
      
      // Path must be absolute.
      if (!src.startsWith(Path.SEPARATOR)) {
        return false;
      }
      
      // Check for ".." "." ":" "/"
      StringTokenizer tokens = new StringTokenizer(src, Path.SEPARATOR);
      while( tokens.hasMoreTokens()) {
        String element = tokens.nextToken();
        if (element.equals("..") || 
            element.equals(".")  ||
            (element.indexOf(":") >= 0)  ||
            (element.indexOf("/") >= 0)) {
          return false;
        }
      }
      return true;
    }
    
    /**
     * Create all the necessary directories
     */
    public synchronized boolean mkdirs( String src ) throws IOException {
        boolean    success;
        NameNode.stateChangeLog.debug("DIR* NameSystem.mkdirs: " + src );
        if( isInSafeMode() )
          throw new SafeModeException( "Cannot create directory " + src, safeMode );
        if (!isValidName(src)) {
          throw new IOException("Invalid directory name: " + src);
        }
        success = dir.mkdirs(src);
        if (!success) {
          throw new IOException("Invalid directory name: " + src);
        }
        return success;
    }

    /**
     * Figure out a few hosts that are likely to contain the
     * block(s) referred to by the given (filename, start, len) tuple.
     */
    public String[][] getDatanodeHints(String src, long start, long len) {
        if (start < 0 || len < 0) {
            return new String[0][];
        }

        int startBlock = -1;
        int endBlock = -1;
        Block blocks[] = dir.getFile( new UTF8( src ));

        if (blocks == null) {                     // no blocks
            return new String[0][];
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
            return new String[0][];
        } else {
          String hosts[][] = new String[(endBlock - startBlock) + 1][];
            for (int i = startBlock; i <= endBlock; i++) {
                Collection<DatanodeDescriptor> containingNodes = blocksMap.get(blocks[i]);
                Collection<String> v = new ArrayList<String>();
                if (containingNodes != null) {
                  for (Iterator<DatanodeDescriptor> it =containingNodes.iterator(); it.hasNext();) {
                    v.add( it.next().getHostName() );
                  }
                }
                hosts[i-startBlock] = v.toArray(new String[v.size()]);
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
    class Lease implements Comparable<Lease> {
        public UTF8 holder;
        public long lastUpdate;
        private Collection<UTF8> locks = new TreeSet<UTF8>();
        private Collection<UTF8> creates = new TreeSet<UTF8>();

        public Lease(UTF8 holder) {
            this.holder = holder;
            renew();
        }
        public void renew() {
            this.lastUpdate = now();
        }
        /**
         * Returns true if the Hard Limit Timer has expired
         */
        public boolean expiredHardLimit() {
            if (now() - lastUpdate > LEASE_HARDLIMIT_PERIOD) {
                return true;
            }
            return false;
        }
        /**
         * Returns true if the Soft Limit Timer has expired
         */
        public boolean expiredSoftLimit() {
            if (now() - lastUpdate > LEASE_SOFTLIMIT_PERIOD) {
                return true;
            }
            return false;
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
            for (Iterator<UTF8> it = locks.iterator(); it.hasNext(); )
                internalReleaseLock(it.next(), holder);
            locks.clear();
            for (Iterator<UTF8> it = creates.iterator(); it.hasNext(); )
                internalReleaseCreate(it.next(), holder);
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
        public int compareTo(Lease o) {
            Lease l1 = this;
            Lease l2 = o;
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
                               ((top = sortedLeases.first()) != null)) {
                            if (top.expiredHardLimit()) {
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
    /** @deprecated */ @Deprecated
    public synchronized int obtainLock( UTF8 src, 
                                        UTF8 holder, 
                                        boolean exclusive) throws IOException {
        if( isInSafeMode() )
          throw new SafeModeException( "Cannot lock file " + src, safeMode );
        int result = dir.obtainLock(src, holder, exclusive);
        if (result == COMPLETE_SUCCESS) {
            synchronized (leases) {
                Lease lease = leases.get(holder);
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
    /** @deprecated */ @Deprecated
    public synchronized int releaseLock(UTF8 src, UTF8 holder) {
        int result = internalReleaseLock(src, holder);
        if (result == COMPLETE_SUCCESS) {
            synchronized (leases) {
                Lease lease = leases.get(holder);
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
      FileUnderConstruction v = pendingCreates.remove(src);
      if (v != null) {
         NameNode.stateChangeLog.debug(
                      "DIR* NameSystem.internalReleaseCreate: " + src
                    + " is removed from pendingCreates for "
                    + holder + " (failure)");
        for (Iterator<Block> it2 = v.getBlocks().iterator(); it2.hasNext(); ) {
          Block b = it2.next();
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
    public void renewLease(UTF8 holder) throws IOException {
        synchronized (leases) {
            if( isInSafeMode() )
              throw new SafeModeException( "Cannot renew lease for " + holder, safeMode );
            Lease lease = leases.get(holder);
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
    public synchronized void registerDatanode( DatanodeRegistration nodeReg,
                                               String networkLocation
                                              ) throws IOException {

      if (!verifyNodeRegistration(nodeReg)) {
        throw new DisallowedDatanodeException( nodeReg );
      }

      String dnAddress = Server.getRemoteAddress();
      if ( dnAddress == null ) {
        //Mostly not called inside an RPC.
        throw new IOException( "Could not find remote address for " +
                               "registration from " + nodeReg.getName() );
      }      

      String hostName = nodeReg.getHost();
      
      // update the datanode's name with ip:port
      DatanodeID dnReg = new DatanodeID( dnAddress + ":" + nodeReg.getPort(),
                                         nodeReg.getStorageID(),
                                         nodeReg.getInfoPort() );
      nodeReg.updateRegInfo( dnReg );
      
      NameNode.stateChangeLog.info(
          "BLOCK* NameSystem.registerDatanode: "
          + "node registration from " + nodeReg.getName()
          + " storage " + nodeReg.getStorageID() );

      nodeReg.registrationID = getRegistrationID();
      DatanodeDescriptor nodeS = datanodeMap.get(nodeReg.getStorageID());
      DatanodeDescriptor nodeN = getDatanodeByName( nodeReg.getName() );
      
      if( nodeN != null && nodeN != nodeS ) {
          NameNode.LOG.info( "BLOCK* NameSystem.registerDatanode: "
                  + "node from name: " + nodeN.getName() );
        // nodeN previously served a different data storage, 
        // which is not served by anybody anymore.
        removeDatanode( nodeN );
        // physically remove node from datanodeMap
        wipeDatanode( nodeN );
        // and log removal
        getEditLog().logRemoveDatanode( nodeN );
        nodeN = null;
      }

      if ( nodeS != null ) {
        if( nodeN == nodeS ) {
          // The same datanode has been just restarted to serve the same data 
          // storage. We do not need to remove old data blocks, the delta will
          // be calculated on the next block report from the datanode
          NameNode.stateChangeLog.debug("BLOCK* NameSystem.registerDatanode: "
                                        + "node restarted." );
        } else {
          // nodeS is found
          // The registering datanode is a replacement node for the existing 
          // data storage, which from now on will be served by a new node.
          NameNode.stateChangeLog.debug(
            "BLOCK* NameSystem.registerDatanode: "
            + "node " + nodeS.getName()
            + " is replaced by " + nodeReg.getName() + "." );
        }
        getEditLog().logRemoveDatanode( nodeS );
        // update cluster map
        clusterMap.remove( nodeS );
        nodeS.updateRegInfo( nodeReg );
        nodeS.setNetworkLocation( networkLocation );
        clusterMap.add( nodeS );
        nodeS.setHostName( hostName );
        getEditLog().logAddDatanode( nodeS );
        
        // also treat the registration message as a heartbeat
        synchronized( heartbeats ) {
            heartbeats.add( nodeS );
            //update its timestamp
            nodeS.updateHeartbeat( 0L, 0L, 0);
            nodeS.isAlive = true;
        }
        return;
      } 

      // this is a new datanode serving a new data storage
      if( nodeReg.getStorageID().equals("") ) {
        // this data storage has never been registered
        // it is either empty or was created by pre-storageID version of DFS
        nodeReg.storageID = newStorageID();
        NameNode.stateChangeLog.debug(
            "BLOCK* NameSystem.registerDatanode: "
            + "new storageID " + nodeReg.getStorageID() + " assigned." );
      }
      // register new datanode
      DatanodeDescriptor nodeDescr 
              = new DatanodeDescriptor( nodeReg, networkLocation, hostName );
      unprotectedAddDatanode( nodeDescr );
      clusterMap.add(nodeDescr);
      getEditLog().logAddDatanode( nodeDescr );
      
      // also treat the registration message as a heartbeat
      synchronized( heartbeats ) {
          heartbeats.add( nodeDescr );
          nodeDescr.isAlive = true;
          // no need to update its timestamp
          // because its is done when the descriptor is created
      }
      return;
    }
    
    /**
     * Get registrationID for datanodes based on the namespaceID.
     * 
     * @see #registerDatanode(DatanodeRegistration)
     * @see FSImage#newNamespaceID()
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
    
    private boolean isDatanodeDead(DatanodeDescriptor node) {
      return (node.getLastUpdate() <
          (System.currentTimeMillis() - heartbeatExpireInterval));
    }
    
    void setDatanodeDead(DatanodeID nodeID) throws IOException {
        DatanodeDescriptor node = getDatanode(nodeID);
        node.setLastUpdate(0);
    }

    /**
     * The given node has reported in.  This method should:
     * 1) Record the heartbeat, so the datanode isn't timed out
     * 2) Adjust usage stats for future block allocation
     * 
     * If a substantial amount of time passed since the last datanode 
     * heartbeat then request an immediate block report.  
     * 
     * @return true if block report is required or false otherwise.
     * @throws IOException
     */
    public boolean gotHeartbeat( DatanodeID nodeID,
                                 long capacity, 
                                 long remaining,
                                 int xceiverCount,
                                 int xmitsInProgress,
                                 Object[] xferResults,
                                 Object deleteList[]
                                 ) throws IOException {
      synchronized (heartbeats) {
        synchronized (datanodeMap) {
          DatanodeDescriptor nodeinfo;
          try {
            nodeinfo = getDatanode( nodeID );
            if (nodeinfo == null ) {
                return true;
            }
          } catch(UnregisteredDatanodeException e) {
              return true;
          }
          
          // Check if this datanode should actually be shutdown instead. 
          if (shouldNodeShutdown(nodeinfo)) {
              setDatanodeDead(nodeinfo);
              throw new DisallowedDatanodeException(nodeinfo);
          }

          if( !nodeinfo.isAlive ) {
              return true;
          } else {
              updateStats(nodeinfo, false);
              nodeinfo.updateHeartbeat(capacity, remaining, xceiverCount);
              updateStats(nodeinfo, true);
              //
              // Extract pending replication work or block invalidation
              // work from the datanode descriptor
              //
              nodeinfo.getReplicationSets(this.maxReplicationStreams - 
                                          xmitsInProgress, xferResults); 
              if (xferResults[0] == null) {
                nodeinfo.getInvalidateBlocks(FSConstants.BLOCK_INVALIDATE_CHUNK,
                                             deleteList);
              }
              return false;
          }
        }
      }
    }

    private void updateStats(DatanodeDescriptor node, boolean isAdded) {
      //
      // The statistics are protected by the heartbeat lock
      //
      assert(Thread.holdsLock(heartbeats));
      if (isAdded) {
        totalCapacity += node.getCapacity();
        totalRemaining += node.getRemaining();
        totalLoad += node.getXceiverCount();
      } else {
        totalCapacity -= node.getCapacity();
        totalRemaining -= node.getRemaining();
        totalLoad -= node.getXceiverCount();
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
                    Thread.sleep(heartbeatRecheckInterval);
                } catch (InterruptedException ie) {
                }
            }
        }
    }

    /**
     * Periodically calls computeReplicationWork().
     */
    class ReplicationMonitor implements Runnable {
      public void run() {
        while (fsRunning) {
          try {
            computeDatanodeWork();
            processPendingReplications();
            Thread.sleep(replicationRecheckInterval);
          } catch (InterruptedException ie) {
          } catch (IOException ie) {
            LOG.warn("ReplicationMonitor thread received exception. " + ie);
          }
        }
      }
    }

    /**
     * Look at a few datanodes and compute any replication work that 
     * can be scheduled on them. The datanode will be infomed of this
     * work at the next heartbeat.
     */
    void computeDatanodeWork() throws IOException {
      int numiter = 0;
      int foundwork = 0;
      int hsize = 0;

      while (true) {
        DatanodeDescriptor node = null;

        //
        // pick the datanode that was the last one in the
        // previous invocation of this method.
        //
        synchronized (heartbeats) {
          hsize = heartbeats.size();
          if (numiter++ >= hsize) {
            break;
          }
          if (replIndex >= hsize) {
            replIndex = 0;
          }
          node = heartbeats.get(replIndex);
          replIndex++;
        }

        //
        // Is there replication work to be computed for this datanode?
        //
        int precomputed = node.getNumberOfBlocksToBeReplicated();
        int needed = this.maxReplicationStreams - precomputed;
        boolean doReplication = false;
        boolean doInvalidation = false;
        if (needed > 0) {
          //
          // Compute replication work and store work into the datanode
          //
          Object replsets[] = pendingTransfers(node, needed);
          if (replsets != null) {
            doReplication = true;
            addBlocksToBeReplicated(node, (Block[])replsets[0], 
                                   (DatanodeDescriptor[][])replsets[1]);
          }
        }
        if (!doReplication) {
          //
          // Determine if block deletion is pending for this datanode
          //
          Block blocklist[] = blocksToInvalidate(node);
          if (blocklist != null) {
            doInvalidation = true;
            addBlocksToBeInvalidated(node, blocklist);
          }
        }
        if (doReplication || doInvalidation) {
          //
          // If we have already computed work for a predefined
          // number of datanodes in this iteration, then relax
          //
          if (foundwork > ((hsize * REPL_WORK_PER_ITERATION)/100)) {
            break;
          }
          foundwork++;
        } else {
          //
          // See if the decommissioned node has finished moving all
          // its datablocks to another replica. This is a loose
          // heuristic to determine when a decommission is really over.
          //
          checkDecommissionState(node);
        }
      }
    }

    /**
     * If there were any replication requests that timed out, reap them
     * and put them back into the neededReplication queue
     */
    void processPendingReplications() {
      Block[] timedOutItems = pendingReplications.getTimedOutBlocks();
      if (timedOutItems != null) {
        synchronized (this) {
          for (int i = 0; i < timedOutItems.length; i++) {
            neededReplications.add(timedOutItems[i]);
          }
        }
      }
    }

    /**
     * Add more replication work for this datanode.
     */
    synchronized void addBlocksToBeReplicated(DatanodeDescriptor node, 
                                 Block[] blocklist,
                                 DatanodeDescriptor[][] targets) 
                                 throws IOException {
      //
      // Find the datanode with the FSNamesystem lock held.
      //
      DatanodeDescriptor n = getDatanode(node);
      if (n != null) {
        n.addBlocksToBeReplicated(blocklist, targets);
      }
    }

    /**
     * Add more block invalidation work for this datanode.
     */
    synchronized void addBlocksToBeInvalidated(DatanodeDescriptor node, 
                                 Block[] blocklist) throws IOException {
      //
      // Find the datanode with the FSNamesystem lock held.
      //
      DatanodeDescriptor n = getDatanode(node);
      if (n != null) {
        n.addBlocksToBeInvalidated(blocklist);
      }
    }

    /**
     * remove a datanode descriptor
     * @param nodeID datanode ID
     * @author hairong
     */
    synchronized public void removeDatanode( DatanodeID nodeID ) 
    throws IOException {
      DatanodeDescriptor nodeInfo = getDatanode( nodeID );
      if (nodeInfo != null) {
        removeDatanode( nodeInfo );
      } else {
          NameNode.stateChangeLog.warn("BLOCK* NameSystem.removeDatanode: "
                  + nodeInfo.getName() + " does not exist");
      }
  }
  
  /**
   * remove a datanode descriptor
   * @param nodeInfo datanode descriptor
   * @author hairong
   */
    private void removeDatanode( DatanodeDescriptor nodeInfo ) {
      if (nodeInfo.isAlive) {
        updateStats(nodeInfo, false);
        heartbeats.remove(nodeInfo);
        nodeInfo.isAlive = false;
      }

      for (Iterator<Block> it = nodeInfo.getBlockIterator(); it.hasNext(); ) {
          removeStoredBlock(it.next(), nodeInfo);
      }
      unprotectedRemoveDatanode(nodeInfo);
      clusterMap.remove(nodeInfo);
    }

    void unprotectedRemoveDatanode( DatanodeDescriptor nodeDescr ) {
      // datanodeMap.remove(nodeDescr.getStorageID());
      // deaddatanodeMap.put(nodeDescr.getName(), nodeDescr);
      nodeDescr.resetBlocks();
      NameNode.stateChangeLog.debug(
          "BLOCK* NameSystem.unprotectedRemoveDatanode: "
          + nodeDescr.getName() + " is out of service now.");
    }
    
    void unprotectedAddDatanode( DatanodeDescriptor nodeDescr ) {
      datanodeMap.put( nodeDescr.getStorageID(), nodeDescr );
      NameNode.stateChangeLog.debug(
          "BLOCK* NameSystem.unprotectedAddDatanode: "
          + "node " + nodeDescr.getName() + " is added to datanodeMap." );
    }

    
    /**
     * Physically remove node from datanodeMap.
     * 
     * @param nodeID node
     */
    void wipeDatanode( DatanodeID nodeID ) {
      String key = nodeID.getStorageID();
      datanodeMap.remove(key);
      NameNode.stateChangeLog.debug(
          "BLOCK* NameSystem.wipeDatanode: "
          + nodeID.getName() + " storage " + nodeID.getStorageID() 
          + " is removed from datanodeMap.");
    }
    
    private FSEditLog getEditLog() {
      return dir.fsImage.getEditLog();
    }

    /**
     * Check if there are any expired heartbeats, and if so,
     * whether any blocks have to be re-replicated.
     * While removing dead datanodes, make sure that only one datanode is marked
     * dead at a time within the synchronized section. Otherwise, a cascading
     * effect causes more datanodes to be declared dead.
     */
    void heartbeatCheck() {
      boolean allAlive = false;
      while (!allAlive) {
        boolean foundDead = false;
        DatanodeID nodeID = null;

        // locate the first dead node.
        synchronized(heartbeats) {
            for (Iterator<DatanodeDescriptor> it = heartbeats.iterator();
            it.hasNext();) {
              DatanodeDescriptor nodeInfo = it.next();
              if (isDatanodeDead(nodeInfo)) {
                foundDead = true;
                nodeID = nodeInfo;
                break;
              }
            }
        }

        // acquire the fsnamesystem lock, and then remove the dead node.
        if (foundDead) {
          synchronized (this) {
            synchronized(heartbeats) {
              synchronized (datanodeMap) {
                DatanodeDescriptor nodeInfo = null;
                try {
                  nodeInfo = getDatanode(nodeID);
                } catch (IOException e) {
                  nodeInfo = null;
                }
                if (nodeInfo != null && isDatanodeDead(nodeInfo)) {
                  NameNode.stateChangeLog.info("BLOCK* NameSystem.heartbeatCheck: "
                    + "lost heartbeat from " + nodeInfo.getName());
                  removeDatanode(nodeInfo);
                }
              }
            }
          }
        }
        allAlive = ! foundDead;
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
        DatanodeDescriptor node = getDatanode( nodeID );

        // Check if this datanode should actually be shutdown instead.
        if (shouldNodeShutdown(node)) {
          setDatanodeDead(node);
          throw new DisallowedDatanodeException(node);
        }

        //
        // Modify the (block-->datanode) map, according to the difference
        // between the old and new block report.
        //
        int newPos = 0;
        Iterator<Block> iter = node.getBlockIterator();
        Block oldblk = iter.hasNext() ? iter.next() : null;
        Block newblk = (newReport != null && newReport.length > 0) ? 
                        newReport[0]	: null;

        // common case is that most of the blocks from the datanode
        // matches blocks in datanode descriptor.                
        Collection<Block> toRemove = new LinkedList<Block>();
        Collection<Block> toAdd = new LinkedList<Block>();
        
        while (oldblk != null || newblk != null) {
           
            int cmp = (oldblk == null) ? 1 : 
                       ((newblk == null) ? -1 : oldblk.compareTo(newblk));

            if (cmp == 0) {
                // Do nothing, blocks are the same
                newPos++;
                oldblk = iter.hasNext() ? iter.next() : null;
                newblk = (newPos < newReport.length)
                         ? newReport[newPos] : null;
            } else if (cmp < 0) {
                // The old report has a block the new one does not
                toRemove.add(oldblk);
                oldblk = iter.hasNext() ? iter.next() : null;
            } else {
                // The new report has a block the old one does not
                toAdd.add(newblk);
                newPos++;
                newblk = (newPos < newReport.length)
                         ? newReport[newPos] : null;
            }
        }
        
        for ( Iterator<Block> i = toRemove.iterator(); i.hasNext(); ) {
            Block b = i.next();
            removeStoredBlock( b, node );
            node.removeBlock( b );
        }
        for ( Iterator<Block> i = toAdd.iterator(); i.hasNext(); ) {
            Block b = i.next();
            node.addBlock( addStoredBlock(b, node) );
        }
        
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
        Collection<Block> obsolete = new ArrayList<Block>();
        for (Iterator<Block> it = node.getBlockIterator(); it.hasNext(); ) {
            Block b = it.next();

            // 
            // A block report can only send BLOCK_INVALIDATE_CHUNK number of
            // blocks to be deleted. If there are more blocks to be deleted, 
            // they are added to recentInvalidateSets and will be sent out
            // thorugh succeeding heartbeat responses.
            //
            if (obsolete.size() > FSConstants.BLOCK_INVALIDATE_CHUNK) {
                addToInvalidates(b, node);
            } else {
                if (! dir.isValidBlock(b) && ! pendingCreateBlocks.contains(b)) {
                  obsolete.add(b);
                  NameNode.stateChangeLog.debug("BLOCK* NameSystem.processReport: "
                        +"ask "+nodeID.getName()+" to delete "+b.getBlockName() );
                }
            }
        }
        return (Block[]) obsolete.toArray(new Block[obsolete.size()]);
    }

    /**
     * Modify (block-->datanode) map.  Remove block from set of 
     * needed replications if this takes care of the problem.
     * @return the block that is stored in blockMap.
     */
    synchronized Block addStoredBlock(Block block, DatanodeDescriptor node) {
        List<DatanodeDescriptor> containingNodes = blocksMap.get(block);
        if (containingNodes == null) {
            //Create an arraylist with the current replication factor
            FSDirectory.INode inode = dir.getFileByBlock(block);
            int replication = (inode != null) ? 
                              inode.getReplication() : defaultReplication;
            containingNodes = new ArrayList<DatanodeDescriptor>(replication);
            blocksMap.put(block, containingNodes);
        } else {
            Block storedBlock = 
                containingNodes.get(0).getBlock(block);
            // update stored block's length.
            if ( storedBlock != null ) {
                if ( block.getNumBytes() > 0 ) {
                    storedBlock.setNumBytes( block.getNumBytes() );
                }
                block = storedBlock;
            }
        }
        int curReplicaDelta = 0;
        if (! containingNodes.contains(node)) {
            containingNodes.add(node);
            curReplicaDelta = 1;
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

        FSDirectory.INode fileINode = dir.getFileByBlock(block);
        if( fileINode == null )  // block does not belong to any file
            return block;
        
        // filter out containingNodes that are marked for decommission.
        int numCurrentReplica = countContainingNodes(containingNodes)
                              + pendingReplications.getNumReplicas(block);
        
        // check whether safe replication is reached for the block
        // only if it is a part of a files
        incrementSafeBlockCount( numCurrentReplica );
        
        // handle underReplication/overReplication
        short fileReplication = fileINode.getReplication();
        if(numCurrentReplica < fileReplication) {
          neededReplications.update(block, curReplicaDelta, 0);
        }
        proccessOverReplicatedBlock( block, fileReplication );
        return block;
    }
    
    /**
     * Find how many of the containing nodes are "extra", if any.
     * If there are any extras, call chooseExcessReplicates() to
     * mark them in the excessReplicateMap.
     */
    private void proccessOverReplicatedBlock( Block block, short replication ) {
      Collection<DatanodeDescriptor> containingNodes = blocksMap.get(block);
      if( containingNodes == null )
        return;
      Collection<DatanodeDescriptor> nonExcess = new ArrayList<DatanodeDescriptor>();
      for (Iterator<DatanodeDescriptor> it = containingNodes.iterator(); it.hasNext(); ) {
          DatanodeDescriptor cur = it.next();
          Collection<Block> excessBlocks = excessReplicateMap.get(cur.getStorageID());
          if (excessBlocks == null || ! excessBlocks.contains(block)) {
            if (!cur.isDecommissionInProgress() && !cur.isDecommissioned()) {
              nonExcess.add(cur);
            }
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
     * We pick node with least free space
     * In the future, we might enforce some kind of policy 
     * (like making sure replicates are spread across racks).
     */
    void chooseExcessReplicates(Collection<DatanodeDescriptor> nonExcess, 
                                Block b, short replication) {
        while (nonExcess.size() - replication > 0) {
            DatanodeInfo cur = null;
            long minSpace = Long.MAX_VALUE;
            
            for (Iterator<DatanodeDescriptor> iter = nonExcess.iterator(); iter.hasNext();) {
                DatanodeInfo node = iter.next();
                long free = node.getRemaining();
                
                if(minSpace > free) {
                    minSpace = free;
                    cur = node;
                }
            }
            
            nonExcess.remove(cur);

            Collection<Block> excessBlocks = excessReplicateMap.get(cur.getStorageID());
            if (excessBlocks == null) {
                excessBlocks = new TreeSet<Block>();
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
            Collection<Block> invalidateSet = recentInvalidateSets.get(cur.getStorageID());
            if (invalidateSet == null) {
                invalidateSet = new ArrayList<Block>();
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
    synchronized void removeStoredBlock(Block block, DatanodeDescriptor node) {
        NameNode.stateChangeLog.debug("BLOCK* NameSystem.removeStoredBlock: "
                +block.getBlockName() + " from "+node.getName() );
        Collection<DatanodeDescriptor> containingNodes = blocksMap.get(block);
        if (containingNodes == null || ! containingNodes.contains(node)) {
          NameNode.stateChangeLog.debug("BLOCK* NameSystem.removeStoredBlock: "
            +block.getBlockName()+" has already been removed from node "+node );
          return;
        }
        containingNodes.remove(node);
        
        // filter out containingNodes that are marked for decommission.
        int numCurrentReplica = countContainingNodes(containingNodes);

        decrementSafeBlockCount( numCurrentReplica );
        if( containingNodes.isEmpty() )
          blocksMap.remove(block);
        //
        // It's possible that the block was removed because of a datanode
        // failure.  If the block is still valid, check if replication is
        // necessary.  In that case, put block on a possibly-will-
        // be-replicated list.
        //
        FSDirectory.INode fileINode = dir.getFileByBlock(block);
        if( fileINode != null ) {
            neededReplications.update(block, -1, 0);
        }

        //
        // We've removed a block from a node, so it's definitely no longer
        // in "excess" there.
        //
        Collection<Block> excessBlocks = excessReplicateMap.get(node.getStorageID());
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
        DatanodeDescriptor node = getDatanode( nodeID );
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

        // Check if this datanode should actually be shutdown instead.
        if (shouldNodeShutdown(node)) {
            setDatanodeDead(node);
            throw new DisallowedDatanodeException(node);
        }

        //
        // Modify the blocks->datanode map and node's map.
        // 
        node.addBlock( addStoredBlock(block, node) );
        pendingReplications.remove(block);
    }

    /**
     * Total raw bytes.
     */
    public long totalCapacity() {

      synchronized (heartbeats) {
        return totalCapacity;
      }
    }

    /**
     * Total non-used raw bytes.
     */
    public long totalRemaining() {
      synchronized (heartbeats) {
        return totalRemaining;
      }
    }

    /**
     * Total number of connections.
     */
    public int totalLoad() {
      synchronized (heartbeats) {
        return totalLoad;
      }
    }

    public synchronized DatanodeInfo[] datanodeReport() {
      DatanodeInfo results[] = null;
        synchronized (datanodeMap) {
            results = new DatanodeInfo[datanodeMap.size()];
            int i = 0;
            for(Iterator<DatanodeDescriptor> it = datanodeMap.values().iterator(); it.hasNext(); )
              results[i++] = new DatanodeInfo( it.next() );
        }
        return results;
    }
    
    /**
     */
    public synchronized void DFSNodesStatus( ArrayList<DatanodeDescriptor> live, 
                                             ArrayList<DatanodeDescriptor> dead ) {
      synchronized (datanodeMap) {
          for(Iterator<DatanodeDescriptor> it = datanodeMap.values().iterator(); it.hasNext(); ) {
            DatanodeDescriptor node = it.next();
            if( isDatanodeDead(node))
              dead.add( node );
            else
              live.add( node );
          }
      }
    }


    /**
     * Start decommissioning the specified datanode. 
     */
    private void startDecommission (DatanodeDescriptor node) 
        throws IOException {

      if (!node.isDecommissionInProgress() && !node.isDecommissioned()) {
        LOG.info("Start Decommissioning node " + node.name);
        node.startDecommission();
        //
        // all the blocks that reside on this node have to be 
        // replicated.
        Block decommissionBlocks[] = node.getBlocks();
        for (int j = 0; j < decommissionBlocks.length; j++) {
            neededReplications.update(decommissionBlocks[j], -1, 0);
        }
      }
    }

    /**
     * Stop decommissioning the specified datanodes.
     */
    public void stopDecommission (DatanodeDescriptor node) 
                             throws IOException {
      LOG.info("Stop Decommissioning node " + node.name);
      node.stopDecommission();
    }

    /**
     * Return true if all specified nodes are decommissioned.
     * Otherwise return false.
     */
    public synchronized boolean checkDecommissioned (String[] nodes) 
                                   throws IOException {
      String badnodes = "";
      boolean isError = false;

      synchronized (datanodeMap) {
        for (int i = 0; i < nodes.length; i++) {
          boolean found = false;
          for (Iterator<DatanodeDescriptor> it = datanodeMap.values().iterator();
               it.hasNext(); ) {
            DatanodeDescriptor node = it.next();

            //
            // If this is a node that we are interested in, check its admin state.
            //
            if (node.getName().equals(nodes[i]) || 
                node.getHost().equals(nodes[i])) {
              found = true;
              boolean isDecommissioned = checkDecommissionStateInternal(node);
              if (!isDecommissioned) {
                return false;
              }
            }
          }
          if (!found) {
            badnodes += nodes[i] + " ";
            isError = true;
          }
        }
      }
      if (isError) {
        throw new IOException("Nodes " + badnodes + " not found");
      }
      return true;
    }

    /** 
     */
    public DatanodeInfo getDataNodeInfo(String name) {
        return datanodeMap.get(name);
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
        // Ask datanodes to perform block delete  
        // only if safe mode is off.
        if( isInSafeMode() )
          return null;
       
        Collection<Block> invalidateSet = recentInvalidateSets.remove( 
                                                      nodeID.getStorageID() );
 
        if (invalidateSet == null) {
            return null;
        }

        Iterator<Block> it = null;
        int sendNum = invalidateSet.size();
        int origSize = sendNum;
        ArrayList sendBlock = new ArrayList(sendNum);

        //
        // calculate the number of blocks that we send in one message
        //
        if (sendNum > FSConstants.BLOCK_INVALIDATE_CHUNK) {
            sendNum =  FSConstants.BLOCK_INVALIDATE_CHUNK;
        }
        //
        // Copy the first chunk into sendBlock
        //
        for (it = invalidateSet.iterator(); sendNum > 0; sendNum--) {
            assert(it.hasNext());
            sendBlock.add(it.next());
            it.remove();
        }

        //
        // If we could not send everything in this message, reinsert this item
        // into the collection.
        //
        if (it.hasNext()) {
            assert(origSize > FSConstants.BLOCK_INVALIDATE_CHUNK);
            recentInvalidateSets.put(nodeID.getStorageID(), invalidateSet);
        }
        
        if (NameNode.stateChangeLog.isInfoEnabled()) {
            StringBuffer blockList = new StringBuffer();
            for (int i = 0; i < sendBlock.size(); i++) {
                blockList.append(' ');
                Block block = (Block) sendBlock.get(i);
                blockList.append(block.getBlockName());
            }
            NameNode.stateChangeLog.debug("BLOCK* NameSystem.blockToInvalidate: "
                   +"ask "+nodeID.getName()+" to delete " + blockList );
        }
        return (Block[]) sendBlock.toArray(new Block[sendBlock.size()]);
    }

    /*
     * Counts the number of nodes in the given list. Skips over nodes
     * that are marked for decommission.
     */
    private int countContainingNodes(Collection<DatanodeDescriptor> nodelist) {
      if( nodelist == null ) return 0;
      int count = 0;
      for (Iterator<DatanodeDescriptor> it = nodelist.iterator(); 
           it.hasNext(); ) {
        DatanodeDescriptor node = it.next();
        if (!node.isDecommissionInProgress() && !node.isDecommissioned()) {
          count++;
        }
      }
      return count;
    }

    /*
     * Filter nodes that are marked for decommison in the given list. 
     * Return a list of non-decommissioned nodes
     */
    private List<DatanodeDescriptor> filterDecommissionedNodes(
        Collection<DatanodeDescriptor> nodelist) {
      List<DatanodeDescriptor> nonCommissionedNodeList =
        new ArrayList<DatanodeDescriptor>();
      for (Iterator<DatanodeDescriptor> it = nodelist.iterator(); 
           it.hasNext(); ) {
        DatanodeDescriptor node = it.next();
        if (!node.isDecommissionInProgress() && !node.isDecommissioned()) {
          nonCommissionedNodeList.add(node);
        }
      }
      return nonCommissionedNodeList;
    }
    /*
     * Return true if there are any blocks on this node that have not
     * yet reached their replication factor. Otherwise returns false.
     */
    private boolean isReplicationInProgress(DatanodeDescriptor srcNode) {
        Block decommissionBlocks[] = srcNode.getBlocks();
        for (int i = 0; i < decommissionBlocks.length; i++) {
            Block block = decommissionBlocks[i];
            FSDirectory.INode fileINode = dir.getFileByBlock(block);
            if (fileINode == null) {
                continue;
            }
            Collection<DatanodeDescriptor> containingNodes = blocksMap.get(block); 
            List<DatanodeDescriptor> nodes =
                filterDecommissionedNodes(containingNodes);
            int numCurrentReplica = nodes.size();
            if (fileINode.getReplication() > numCurrentReplica) {
              return true;
            }
        }
        return false;
    }

    /**
     * Change, if appropriate, the admin state of a datanode to 
     * decommission completed. Return true if decommission is complete.
     */
    private boolean checkDecommissionStateInternal(DatanodeDescriptor node) {
      //
      // Check to see if there are all blocks in this decommisioned
      // node has reached their target replication factor.
      //
      if (node.isDecommissionInProgress()) {
        if (!isReplicationInProgress(node)) {
          node.setDecommissioned();
          LOG.info("Decommission complete for node " + node.name);
        }
      }
      if (node.isDecommissioned()) {
        return true;
      }
      return false;
    }

    /**
     * Change, if appropriate, the admin state of a datanode to 
     * decommission completed.
     */
    public synchronized void checkDecommissionState(DatanodeID nodeReg) {
      DatanodeDescriptor node = datanodeMap.get(nodeReg.getStorageID());
      if (node == null) {
        return;
      }
      checkDecommissionStateInternal(node);
    }

    /**
     * Return with a list of Block/DataNodeInfo sets, indicating
     * where various Blocks should be copied, ASAP.
     *
     * The Array that we return consists of two objects:
     * The 1st elt is an array of Blocks.
     * The 2nd elt is a 2D array of DatanodeDescriptor objs, identifying the
     *     target sequence for the Block at the appropriate index.
     *
     */
    public synchronized Object[] pendingTransfers(DatanodeID srcNode,
                                                  int needed) {
    // Ask datanodes to perform block replication  
    // only if safe mode is off.
    if( isInSafeMode() )
      return null;
    
    synchronized (neededReplications) {
      Object results[] = null;

      if (neededReplications.size() > 0) {
        //
        // Go through all blocks that need replications. See if any
        // are present at the current node. If so, ask the node to
        // replicate them.
        //
        List<Block> replicateBlocks = new ArrayList<Block>();
        List<Integer> numCurrentReplicas = new ArrayList<Integer>();
        List<DatanodeDescriptor[]> replicateTargetSets;
        replicateTargetSets = new ArrayList<DatanodeDescriptor[]>();
        for (Iterator<Block> it = neededReplications.iterator(); it.hasNext();) {
          if (needed <= 0) {
            break;
          }
          Block block = it.next();
          long blockSize = block.getNumBytes();
          FSDirectory.INode fileINode = dir.getFileByBlock(block);
          if (fileINode == null) { // block does not belong to any file
            it.remove();
          } else {
            Collection<DatanodeDescriptor> containingNodes = blocksMap.get(block);
            Collection<Block> excessBlocks = excessReplicateMap.get( 
                                                      srcNode.getStorageID() );

            // srcNode must contain the block, and the block must
            // not be scheduled for removal on that node
            if (containingNodes != null && containingNodes.contains(srcNode)
                && (excessBlocks == null || ! excessBlocks.contains(block))) {
              // filter out containingNodes that are marked for decommission.
              List<DatanodeDescriptor> nodes = 
                  filterDecommissionedNodes(containingNodes);
              int numCurrentReplica = nodes.size() +
                                      pendingReplications.getNumReplicas(block);
              DatanodeDescriptor targets[] = replicator.chooseTarget(
                  Math.min( fileINode.getReplication() - numCurrentReplica,
                            needed),
                  datanodeMap.get(srcNode.getStorageID()),
                  nodes, null, blockSize);
              if (targets.length > 0) {
                // Build items to return
                replicateBlocks.add(block);
                numCurrentReplicas.add(new Integer(numCurrentReplica));
                replicateTargetSets.add(targets);
                needed -= targets.length;
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
          for (Iterator<Block> it = replicateBlocks.iterator(); it.hasNext(); i++) {
            Block block = it.next();
            DatanodeDescriptor targets[] = 
                      (DatanodeDescriptor[]) replicateTargetSets.get(i);
            int numCurrentReplica = numCurrentReplicas.get(i).intValue();
            int numExpectedReplica = dir.getFileByBlock( block).getReplication(); 
            if (numCurrentReplica + targets.length >= numExpectedReplica) {
              neededReplications.remove(
                      block, numCurrentReplica, numExpectedReplica);
              pendingReplications.add(block, targets.length);
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
              NameNode.stateChangeLog.debug(
                  "BLOCK* neededReplications = " + neededReplications.size()
                  + " pendingReplications = " + pendingReplications.size() );
            }
          }

          //
          // Build returned objects from above lists
          //
          DatanodeDescriptor targetMatrix[][] = 
                        new DatanodeDescriptor[replicateTargetSets.size()][];
          for (i = 0; i < targetMatrix.length; i++) {
            targetMatrix[i] = replicateTargetSets.get(i);
          }

          results = new Object[2];
          results[0] = replicateBlocks.toArray(new Block[replicateBlocks.size()]);
          results[1] = targetMatrix;
        }
      }
      return results;
    }
  }
  
    /** The class is responsible for choosing the desired number of targets
     * for placing block replicas.
     * The replica placement strategy is that if the writer is on a datanode,
     * the 1st replica is placed on the local machine, 
     * otherwise a random datanode. The 2nd replica is placed on a datanode
     * that is on a different rack. The 3rd replica is placed on a datanode
     * which is on the same rack as the first replca.
     * @author hairong
     *
     */
    class ReplicationTargetChooser {
      private class NotEnoughReplicasException extends Exception {
        NotEnoughReplicasException( String msg ) {
          super( msg );
        }
      }
      
      /**
       * choose <i>numOfReplicas</i> data nodes for <i>writer</i> to replicate
       * a block with size <i>blocksize</i> 
       * If not, return as many as we can.
       * 
       * @param numOfReplicas: number of replicas wanted.
       * @param writer: the writer's machine, null if not in the cluster.
       * @param excludedNodes: datanodesthat should not be considered targets.
       * @param blocksize: size of the data to be written.
       * @return array of DatanodeDescriptor instances chosen as targets
       * and sorted as a pipeline.
       */
      DatanodeDescriptor[] chooseTarget(int numOfReplicas,
          DatanodeDescriptor writer,
          List<DatanodeDescriptor> excludedNodes,
          long blocksize ) {
        if( excludedNodes == null) {
          excludedNodes = new ArrayList<DatanodeDescriptor>();
        }
        
        return chooseTarget(numOfReplicas, writer, 
            new ArrayList<DatanodeDescriptor>(), excludedNodes, blocksize);
      }
      
      /**
       * choose <i>numOfReplicas</i> data nodes for <i>writer</i> 
       * to re-replicate a block with size <i>blocksize</i> 
       * If not, return as many as we can.
       * 
       * @param numOfReplicas: additional number of replicas wanted.
       * @param writer: the writer's machine, null if not in the cluster.
       * @param choosenNodes: datanodes that have been choosen as targets.
       * @param excludedNodes: datanodesthat should not be considered targets.
       * @param blocksize: size of the data to be written.
       * @return array of DatanodeDescriptor instances chosen as target 
       * and sorted as a pipeline.
       */
      DatanodeDescriptor[] chooseTarget(int numOfReplicas,
          DatanodeDescriptor writer,
          List<DatanodeDescriptor> choosenNodes,
          List<DatanodeDescriptor> excludedNodes,
          long blocksize ) {
        if( numOfReplicas == 0 )
          return new DatanodeDescriptor[0];
        
        if( excludedNodes == null) {
          excludedNodes = new ArrayList<DatanodeDescriptor>();
        }
        
        int clusterSize = clusterMap.getNumOfLeaves();
        int totalNumOfReplicas = choosenNodes.size()+numOfReplicas;
        if( totalNumOfReplicas > clusterSize) {
          numOfReplicas -= (totalNumOfReplicas-clusterSize);
          totalNumOfReplicas = clusterSize;
        }
        
        int maxNodesPerRack = 
          (totalNumOfReplicas-1)/clusterMap.getNumOfRacks()+2;
        
        List<DatanodeDescriptor> results = 
          new ArrayList<DatanodeDescriptor>(choosenNodes);
        excludedNodes.addAll(choosenNodes);
        
        if(!clusterMap.contains(writer))
          writer=null;
        
        DatanodeDescriptor localNode = chooseTarget(numOfReplicas, writer, 
            excludedNodes, blocksize, maxNodesPerRack, results );
        
        results.removeAll(choosenNodes);
        
        // sorting nodes to form a pipeline
        return getPipeline((writer==null)?localNode:writer, results);
      }
      
      /* choose <i>numOfReplicas</i> from all data nodes */
      private DatanodeDescriptor chooseTarget(int numOfReplicas,
          DatanodeDescriptor writer,
          List<DatanodeDescriptor> excludedNodes,
          long blocksize,
          int maxNodesPerRack,
          List<DatanodeDescriptor> results) {
        
        if( numOfReplicas == 0 || clusterMap.getNumOfLeaves()==0 ) {
            return writer;
        }
        
        int numOfResults = results.size();
        if(writer == null && (numOfResults==1 || numOfResults==2) ) {
          writer = results.get(0);
        }
        
        try {
          switch( numOfResults ) {
          case 0:
            writer = chooseLocalNode(writer, excludedNodes, 
                blocksize, maxNodesPerRack, results);
            if(--numOfReplicas == 0) break;
          case 1:
            chooseRemoteRack(1, writer, excludedNodes, 
                blocksize, maxNodesPerRack, results);
            if(--numOfReplicas == 0) break;
          case 2:
            if(clusterMap.isOnSameRack(results.get(0), results.get(1))) {
              chooseRemoteRack(1, writer, excludedNodes,
                  blocksize, maxNodesPerRack, results);
            } else {
              chooseLocalRack(writer, excludedNodes, 
                  blocksize, maxNodesPerRack, results);
            }
            if(--numOfReplicas == 0) break;
          default:
            chooseRandom(numOfReplicas, NodeBase.ROOT, excludedNodes, 
                blocksize, maxNodesPerRack, results);
          }
        } catch (NotEnoughReplicasException e) {
          LOG.warn("Not able to place enough replicas, still in need of "
              + numOfReplicas );
        }
        return writer;
      }
      
      /* choose <i>localMachine</i> as the target.
       * if <i>localMachine</i> is not availabe, 
       * choose a node on the same rack
       * @return the choosen node
       */
      private DatanodeDescriptor chooseLocalNode(
          DatanodeDescriptor localMachine,
          List<DatanodeDescriptor> excludedNodes,
          long blocksize,
          int maxNodesPerRack,
          List<DatanodeDescriptor> results)
      throws NotEnoughReplicasException {
        // if no local machine, randomly choose one node
        if(localMachine == null)
          return chooseRandom(NodeBase.ROOT, excludedNodes, 
              blocksize, maxNodesPerRack, results);
        
        // otherwise try local machine first
        if(!excludedNodes.contains(localMachine)) {
          excludedNodes.add(localMachine);
          if( isGoodTarget(localMachine, blocksize,
                  maxNodesPerRack, false, results)) {
            results.add(localMachine);
            return localMachine;
          }
        } 
        
        // try a node on local rack
        return chooseLocalRack(localMachine, excludedNodes, 
            blocksize, maxNodesPerRack, results);
      }
      
      /* choose one node from the rack that <i>localMachine</i> is on.
       * if no such node is availabe, choose one node from the rack where
       * a second replica is on.
       * if still no such node is available, choose a random node 
       * in the cluster.
       * @return the choosen node
       */
      private DatanodeDescriptor chooseLocalRack(
          DatanodeDescriptor localMachine,
          List<DatanodeDescriptor> excludedNodes,
          long blocksize,
          int maxNodesPerRack,
          List<DatanodeDescriptor> results)
      throws NotEnoughReplicasException {
        // no local machine, so choose a random machine
        if( localMachine == null ) {
          return chooseRandom(NodeBase.ROOT, excludedNodes, 
              blocksize, maxNodesPerRack, results );
        }
        
        // choose one from the local rack
        try {
          return chooseRandom(
              localMachine.getNetworkLocation(),
              excludedNodes, blocksize, maxNodesPerRack, results);
        } catch (NotEnoughReplicasException e1) {
          // find the second replica
          DatanodeDescriptor newLocal=null;
          for(Iterator<DatanodeDescriptor> iter=results.iterator();
          iter.hasNext();) {
            DatanodeDescriptor nextNode = iter.next();
            if(nextNode != localMachine) {
              newLocal = nextNode;
              break;
            }
          }
          if( newLocal != null ) {
            try {
              return chooseRandom(
                  newLocal.getNetworkLocation(),
                  excludedNodes, blocksize, maxNodesPerRack, results);
            } catch( NotEnoughReplicasException e2 ) {
              //otherwise randomly choose one from the network
              return chooseRandom(NodeBase.ROOT, excludedNodes,
                  blocksize, maxNodesPerRack, results);
            }
          } else {
            //otherwise randomly choose one from the network
            return chooseRandom(NodeBase.ROOT, excludedNodes,
                blocksize, maxNodesPerRack, results);
          }
        }
      }
      
      /* choose <i>numOfReplicas</i> nodes from the racks 
       * that <i>localMachine</i> is NOT on.
       * if not enough nodes are availabe, choose the remaining ones 
       * from the local rack
       */
      
      private void chooseRemoteRack( int numOfReplicas,
          DatanodeDescriptor localMachine,
          List<DatanodeDescriptor> excludedNodes,
          long blocksize,
          int maxReplicasPerRack,
          List<DatanodeDescriptor> results)
      throws NotEnoughReplicasException {
        int oldNumOfReplicas = results.size();
        // randomly choose one node from remote racks
        try {
          chooseRandom( numOfReplicas, "~"+localMachine.getNetworkLocation(),
                  excludedNodes, blocksize, maxReplicasPerRack, results );
        } catch (NotEnoughReplicasException e) {
          chooseRandom( numOfReplicas-(results.size()-oldNumOfReplicas),
              localMachine.getNetworkLocation(), excludedNodes, blocksize, 
              maxReplicasPerRack, results);
        }
      }
      
      /* Randomly choose one target from <i>nodes</i>.
       * @return the choosen node
       */
      private DatanodeDescriptor chooseRandom(
          String nodes,
          List<DatanodeDescriptor> excludedNodes,
          long blocksize,
          int maxNodesPerRack,
          List<DatanodeDescriptor> results) 
      throws NotEnoughReplicasException {
        DatanodeDescriptor result;
        do {
          DatanodeDescriptor[] selectedNodes = 
            chooseRandom(1, nodes, excludedNodes);
          if(selectedNodes.length == 0 ) {
            throw new NotEnoughReplicasException( 
            "Not able to place enough replicas" );
          }
          result = (DatanodeDescriptor)(selectedNodes[0]);
        } while( !isGoodTarget( result, blocksize, maxNodesPerRack, results));
        results.add(result);
        return result;
      }
      
      /* Randomly choose <i>numOfReplicas</i> targets from <i>nodes</i>.
       */
      private void chooseRandom(int numOfReplicas,
          String nodes,
          List<DatanodeDescriptor> excludedNodes,
          long blocksize,
          int maxNodesPerRack,
          List<DatanodeDescriptor> results)
      throws NotEnoughReplicasException {
        boolean toContinue = true;
        do {
          DatanodeDescriptor[] selectedNodes = 
            chooseRandom(numOfReplicas, nodes, excludedNodes);
          if(selectedNodes.length < numOfReplicas) {
            toContinue = false;
          }
          for(int i=0; i<selectedNodes.length; i++) {
            DatanodeDescriptor result = (DatanodeDescriptor)(selectedNodes[i]);
            if( isGoodTarget( result, blocksize, maxNodesPerRack, results)) {
              numOfReplicas--;
              results.add(result);
            }
          } // end of for
        } while (numOfReplicas>0 && toContinue );
        
        if(numOfReplicas>0) {
          throw new NotEnoughReplicasException( 
          "Not able to place enough replicas");
        }
      }
      
      /* Randomly choose <i>numOfNodes</i> nodes from <i>scope</i>.
       * @return the choosen nodes
       */
      private DatanodeDescriptor[] chooseRandom(int numOfReplicas, 
          String nodes,
          List<DatanodeDescriptor> excludedNodes) {
        List<DatanodeDescriptor> results = 
          new ArrayList<DatanodeDescriptor>();
        int numOfAvailableNodes =
            clusterMap.countNumOfAvailableNodes(nodes, excludedNodes);
        numOfReplicas = (numOfAvailableNodes<numOfReplicas)?
            numOfAvailableNodes:numOfReplicas;
        while( numOfReplicas > 0 ) {
          DatanodeDescriptor choosenNode = clusterMap.chooseRandom(nodes);
          if(!excludedNodes.contains(choosenNode)) {
            results.add( choosenNode );
            excludedNodes.add(choosenNode);
            numOfReplicas--;
          }
        }
        return (DatanodeDescriptor[])results.toArray(
            new DatanodeDescriptor[results.size()]);    
      }
      
      /* judge if a node is a good target.
       * return true if <i>node</i> has enough space, 
       * does not have too much load, and the rack does not have too many nodes
       */
      private boolean isGoodTarget( DatanodeDescriptor node,
              long blockSize, int maxTargetPerLoc,
              List<DatanodeDescriptor> results) {
          return isGoodTarget(node, blockSize, maxTargetPerLoc, true, results);
      }
      
      private boolean isGoodTarget( DatanodeDescriptor node,
          long blockSize, int maxTargetPerLoc,
          boolean considerLoad,
          List<DatanodeDescriptor> results) {
        
        // check if the node is (being) decommissed
        if(node.isDecommissionInProgress() || node.isDecommissioned()) {
          LOG.debug("Node "+node.getPath()+
             " is not chosen because the node is (being) decommissioned");
          return false;
        }

        // check the remaining capacity of the target machine
        if(blockSize* FSConstants.MIN_BLOCKS_FOR_WRITE>node.getRemaining() ) {
            LOG.debug("Node "+node.getPath()+
               " is not chosen because the node does not have enough space");
          return false;
        }
        
        // check the communication traffic of the target machine
        if(considerLoad) {
          double avgLoad = 0;
          int size = clusterMap.getNumOfLeaves();
          if( size != 0 ) {
            avgLoad = (double)totalLoad()/size;
          }
          if(node.getXceiverCount() > (2.0 * avgLoad)) {
            LOG.debug("Node "+node.getPath()+
               " is not chosen because the node is too busy");
            return false;
          }
        }
        
        // check if the target rack has chosen too many nodes
        String rackname = node.getNetworkLocation();
        int counter=1;
        for( Iterator<DatanodeDescriptor> iter = results.iterator();
        iter.hasNext(); ) {
          DatanodeDescriptor result = iter.next();
          if(rackname.equals(result.getNetworkLocation())) {
            counter++;
          }
        }
        if(counter>maxTargetPerLoc) {
          LOG.debug("Node "+node.getPath()+
             " is not chosen because the rack has too many chosen nodes");
          return false;
        }
        return true;
      }
      
      /* Return a pipeline of nodes.
       * The pipeline is formed finding a shortest path that 
       * starts from the writer and tranverses all <i>nodes</i>
       * This is basically a traveling salesman problem.
       */
      private DatanodeDescriptor[] getPipeline(
          DatanodeDescriptor writer,
          List<DatanodeDescriptor> nodes ) {
        int numOfNodes = nodes.size();
        DatanodeDescriptor[] results = new DatanodeDescriptor[numOfNodes];
        if( numOfNodes==0 ) return results;
        
        synchronized( clusterMap ) {
          int index=0;
          if(writer == null || !clusterMap.contains(writer)) {
            writer = nodes.get(0);
          }
          for( ;index<numOfNodes; index++ ) {
            DatanodeDescriptor shortestNode = null;
            int shortestDistance = Integer.MAX_VALUE;
            int shortestIndex = index;
            for( int i=index; i<numOfNodes; i++ ) {
              DatanodeDescriptor currentNode = nodes.get(i);
              int currentDistance = clusterMap.getDistance( writer, currentNode );
              if(shortestDistance>currentDistance ) {
                shortestDistance = currentDistance;
                shortestNode = currentNode;
                shortestIndex = i;
              }
            }
            //switch position index & shortestIndex
            if( index != shortestIndex ) {
              nodes.set(shortestIndex, nodes.get(index));
              nodes.set(index, shortestNode);
            }
            writer = shortestNode;
          }
        }
        return nodes.toArray( results );
      }
      
      /** Return datanodes that sorted by their distances to <i>reader</i>
       */
      DatanodeDescriptor[] sortByDistance( 
          final DatanodeDescriptor reader,
          List<DatanodeDescriptor> nodes ) {
          synchronized(clusterMap) {
              if(reader != null && clusterMap.contains(reader)) {
                  java.util.Collections.sort(nodes, new Comparator<DatanodeDescriptor>() {
                      public int compare(DatanodeDescriptor n1, DatanodeDescriptor n2) {
                          return clusterMap.getDistance(reader, n1)
                          -clusterMap.getDistance(reader, n2);
                      }
                  });
              }
          }
          return (DatanodeDescriptor[])nodes.toArray(
                  new DatanodeDescriptor[nodes.size()]);
      }
      
    } //end of Replicator


    // Keeps track of which datanodes are allowed to connect to the namenode.
        
    private boolean inHostsList(DatanodeID node) {
      Set<String> hostsList = hostsReader.getHosts();
      return (hostsList.isEmpty() || 
              hostsList.contains(node.getName()) || 
              hostsList.contains(node.getHost()) ||
              ((node instanceof DatanodeInfo) && 
               hostsList.contains(((DatanodeInfo)node).getHostName())));
    }


    private boolean inExcludedHostsList(DatanodeID node) {
      Set<String> excludeList = hostsReader.getExcludedHosts();
      return (excludeList.contains(node.getName()) ||
              excludeList.contains(node.getHost()) ||
              ((node instanceof DatanodeInfo) && 
               excludeList.contains(((DatanodeInfo)node).getHostName())));
    }

    /**
     * Rereads the files to update the hosts and exclude lists.  It
     * checks if any of the hosts have changed states:
     * 1. Added to hosts  --> no further work needed here.
     * 2. Removed from hosts --> mark AdminState as decommissioned. 
     * 3. Added to exclude --> start decommission.
     * 4. Removed from exclude --> stop decommission.
     */
    void refreshNodes() throws IOException {
      hostsReader.refresh();
      synchronized (this) {
        for (Iterator<DatanodeDescriptor> it = datanodeMap.values().iterator();
             it.hasNext(); ) {
          DatanodeDescriptor node = it.next();
          // Check if not include.
          if (!inHostsList(node)) {
            node.setDecommissioned();  // case 2.
          } else {
            if (inExcludedHostsList(node)) {
              if (!node.isDecommissionInProgress() && 
                  !node.isDecommissioned()) {
                startDecommission(node);   // case 3.
              }
            } else {
              if (node.isDecommissionInProgress() || 
                  node.isDecommissioned()) {
                stopDecommission(node);   // case 4.
              } 
            }
          }
        }
      } 
      
    }
    

    /**
     * Checks if the node is not on the hosts list.  If it is not, then
     * it will be ignored.  If the node is in the hosts list, but is also 
     * on the exclude list, then it will be decommissioned.
     * Returns FALSE if node is rejected for registration. 
     * Returns TRUE if node is registered (including when it is on the 
     * exclude list and is being decommissioned). 
     */
    public synchronized boolean verifyNodeRegistration(DatanodeRegistration nodeReg) 
      throws IOException {
      if (!inHostsList(nodeReg)) {
        return false;    
      }
      if (inExcludedHostsList(nodeReg)) {
        DatanodeDescriptor node = getDatanode(nodeReg);
        if (!checkDecommissionStateInternal(node)) {
          startDecommission(node);
        }
      } 
      return true;
    }
    
    /**
     * Checks if the Admin state bit is DECOMMISSIONED.  If so, then 
     * we should shut it down. 
     * 
     * Returns true if the node should be shutdown.
     */
    private boolean shouldNodeShutdown(DatanodeDescriptor node) {
      return (node.isDecommissioned());
    }

    /**
     * Check if any of the nodes being decommissioned has finished 
     * moving all its datablocks to another replica. This is a loose
     * heuristic to determine when a decommission is really over.
     */
    public synchronized void decommissionedDatanodeCheck() {
      for (Iterator<DatanodeDescriptor> it = datanodeMap.values().iterator();
           it.hasNext(); ) {
        DatanodeDescriptor node = it.next();  
        checkDecommissionStateInternal(node);
      }
    }
    
    /**
     * Periodically calls decommissionedDatanodeCheck().
     */
    class DecommissionedMonitor implements Runnable {
        
      public void run() {
        while (fsRunning) {
          try {
            decommissionedDatanodeCheck();
          } catch (Exception e) {
            FSNamesystem.LOG.info(StringUtils.stringifyException(e));
          }
          try {
            Thread.sleep(1000 * 60 * 5);
          } catch (InterruptedException ie) {
          }
        }
      }
    }
    

    /**
     * Information about the file while it is being written to.
     * Note that at that time the file is not visible to the outside.
     * 
     * This class contains a <code>Collection</code> of {@link Block}s that has
     * been written into the file so far, and file replication. 
     * 
     * @author shv
     */
    private class FileUnderConstruction {
      private short blockReplication; // file replication
      private long blockSize;
      private Collection<Block> blocks;
      private UTF8 clientName;         // lease holder
      private UTF8 clientMachine;
      
      FileUnderConstruction(short replication,
                            long blockSize,
                            UTF8 clientName,
                            UTF8 clientMachine) throws IOException {
        this.blockReplication = replication;
        this.blockSize = blockSize;
        this.blocks = new ArrayList<Block>();
        this.clientName = clientName;
        this.clientMachine = clientMachine;
      }
      
      public short getReplication() {
        return this.blockReplication;
      }
      
      public long getBlockSize() {
        return blockSize;
      }
      
      public Collection<Block> getBlocks() {
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
     * @return DatanodeDescriptor or null if the node is not found.
     * @throws IOException
     */
    public DatanodeDescriptor getDatanode( DatanodeID nodeID ) throws IOException {
      UnregisteredDatanodeException e = null;
      DatanodeDescriptor node = datanodeMap.get(nodeID.getStorageID());
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
     * @return DatanodeDescriptor if found or null otherwise 
     * @throws IOException
     */
    public DatanodeDescriptor getDatanodeByName( String name ) throws IOException {
      for (Iterator<DatanodeDescriptor> it = datanodeMap.values().iterator(); it.hasNext(); ) {
        DatanodeDescriptor node = it.next();
        if( node.getName().equals(name) )
           return node;
      }
      return null;
    }
    
    /* Find data node by its host name. */
    private DatanodeDescriptor getDatanodeByHost( String name ) {
        for (Iterator<DatanodeDescriptor> it = datanodeMap.values().iterator(); 
        it.hasNext(); ) {
            DatanodeDescriptor node = it.next();
            if( node.getHost().equals(name) )
                return node;
        }
        return null;
    }
    
    /** Stop at and return the datanode at index (used for content browsing)*/
    private DatanodeInfo getDatanodeByIndex( int index ) {
      int i = 0;
      for (Iterator<DatanodeDescriptor> it = datanodeMap.values().iterator(); it.hasNext(); ) {
        DatanodeInfo node = it.next();
        if( i == index )
           return node;
        i++;
      }
      return null;
    }
    
    public String randomDataNode() {
      int size = datanodeMap.size();
      int index = 0;
      if (size != 0) {
        index = r.nextInt(size);
        DatanodeInfo d = getDatanodeByIndex(index);
        if (d != null) {
          return d.getHost() + ":" + d.getInfoPort();
        }
      }
      return null;
    }
    
    public int getNameNodeInfoPort() {
      return infoPort;
    }

    /**
     * SafeModeInfo contains information related to the safe mode.
     * <p>
     * An instance of {@link SafeModeInfo} is created when the name node
     * enters safe mode.
     * <p>
     * During name node startup {@link SafeModeInfo} counts the number of
     * <em>safe blocks</em>, those that have at least the minimal number of
     * replicas, and calculates the ratio of safe blocks to the total number
     * of blocks in the system, which is the size of
     * {@link FSDirectory#activeBlocks}. When the ratio reaches the
     * {@link #threshold} it starts the {@link SafeModeMonitor} daemon in order
     * to monitor whether the safe mode extension is passed. Then it leaves safe
     * mode and destroys itself.
     * <p>
     * If safe mode is turned on manually then the number of safe blocks is
     * not tracked because the name node is not intended to leave safe mode
     * automatically in the case.
     *
     * @see ClientProtocol#setSafeMode(FSConstants.SafeModeAction)
     * @see SafeModeMonitor
     * @author Konstantin Shvachko
     */
    class SafeModeInfo {
      // configuration fields
      /** Safe mode threshold condition %.*/
      private double threshold;
      /** Safe mode extension after the threshold. */
      private int extension;
      /** Min replication required by safe mode. */
      private int safeReplication;
      
      // internal fields
      /** Time when threshold was reached.
       * 
       * <br>-1 safe mode is off
       * <br> 0 safe mode is on, but threshold is not reached yet 
       */
      private long reached = -1;  
      /** Total number of blocks. */
      int blockTotal; 
      /** Number of safe blocks. */
      private int blockSafe;
      
      /**
       * Creates SafeModeInfo when the name node enters
       * automatic safe mode at startup.
       *  
       * @param conf configuration
       */
      SafeModeInfo( Configuration conf ) {
        this.threshold = conf.getFloat( "dfs.safemode.threshold.pct", 0.95f );
        this.extension = conf.getInt( "dfs.safemode.extension", 0 );
        this.safeReplication = conf.getInt( "dfs.replication.min", 1 );
        this.blockTotal = 0; 
        this.blockSafe = 0;
      }

      /**
       * Creates SafeModeInfo when safe mode is entered manually.
       *
       * The {@link #threshold} is set to 1.5 so that it could never be reached.
       * {@link #blockTotal} is set to -1 to indicate that safe mode is manual.
       * 
       * @see SafeModeInfo
       */
      private SafeModeInfo() {
        this.threshold = 1.5f;  // this threshold can never be riched
        this.extension = 0;
        this.safeReplication = Short.MAX_VALUE + 1; // more than maxReplication
        this.blockTotal = -1;
        this.blockSafe = -1;
        this.reached = -1;
        enter();
      }
      
      /**
       * Check if safe mode is on.
       * @return true if in safe mode
       */
      synchronized boolean isOn() {
        try {
          isConsistent();   // SHV this is an assert
        } catch( IOException e ) {
          System.err.print( StringUtils.stringifyException( e ));
        }
        return this.reached >= 0;
      }
      
      /**
       * Enter safe mode.
       */
      void enter() {
        if( reached != 0 )
          NameNode.stateChangeLog.info(
            "STATE* SafeModeInfo.enter: " + "Safe mode is ON.\n" 
            + getTurnOffTip() );
        this.reached = 0;
      }
      
      /**
       * Leave safe mode.
       */
      synchronized void leave() {
        if( reached >= 0 )
          NameNode.stateChangeLog.info(
            "STATE* SafeModeInfo.leave: " + "Safe mode is OFF." ); 
        reached = -1;
        safeMode = null;
        NameNode.stateChangeLog.info("STATE* Network topology has "
                +clusterMap.getNumOfRacks()+" racks and "
                +clusterMap.getNumOfLeaves()+ " datanodes");
      }
      
      /** 
       * Safe mode can be turned off iff 
       * the threshold is reached and 
       * the extension time have passed.
       * @return true if can leave or false otherwise.
       */
      synchronized boolean canLeave() {
        if( reached == 0 )
          return false;
        if( now() - reached < extension )
          return false;
        return ! needEnter();
      }
      
      /** 
       * There is no need to enter safe mode 
       * if DFS is empty or {@link #threshold} == 0
       */
      boolean needEnter() {
        return getSafeBlockRatio() < threshold;
      }
      
      /**
       * Ratio of the number of safe blocks to the total number of blocks 
       * to be compared with the threshold.
       */
      private float getSafeBlockRatio() {
        return ( blockTotal == 0 ? 1 : (float)blockSafe/blockTotal );
      }
      
      /**
       * Check and trigger safe mode if needed. 
       */
      private void checkMode() {
        if( needEnter() ) {
          enter();
          return;
        }
        // the threshold is reached
        if( ! isOn() ||                           // safe mode is off
            extension <= 0 || threshold <= 0 ) {  // don't need to wait
          this.leave();                           // just leave safe mode
          return;
        }
        if( reached > 0 )  // threshold has already been reached before
          return;
        // start monitor
        reached = now();
        smmthread = new Daemon(new SafeModeMonitor());
        smmthread.start();
      }
      
      /**
       * Set total number of blocks.
       */
      synchronized void setBlockTotal( int total) {
        this.blockTotal = total; 
        checkMode();
      }
      
      /**
       * Increment number of safe blocks if current block has 
       * reached minimal replication.
       * @param replication current replication 
       */
      synchronized void incrementSafeBlockCount( short replication ) {
        if( (int)replication == safeReplication )
          this.blockSafe++;
        checkMode();
      }
      
      /**
       * Decrement number of safe blocks if current block has 
       * fallen below minimal replication.
       * @param replication current replication 
       */
      synchronized void decrementSafeBlockCount( short replication ) {
        if( replication == safeReplication-1 )
          this.blockSafe--;
        checkMode();
      }
      
      /**
       * Check if safe mode was entered manually or at startup.
       */
      boolean isManual() {
        return blockTotal == -1;
      }
      
      /**
       * A tip on how safe mode is to be turned off: manually or automatically.
       */
      String getTurnOffTip() {
        return ( isManual() ? 
            "Use \"hadoop dfs -safemode leave\" to turn safe mode off." :
            "Safe mode will be turned off automatically." );
      }
      
      /**
       * Returns printable state of the class.
       */
      public String toString() {
        String resText = "Current safe block ratio = " 
          + getSafeBlockRatio() 
          + ". Target threshold = " + threshold
          + ". Minimal replication = " + safeReplication + ".";
        if( reached > 0 ) 
          resText += " Threshold was reached " + new Date(reached) + ".";
        return resText;
      }
      
      /**
       * Checks consistency of the class state.
       */
      void isConsistent() throws IOException {
        if( blockTotal == -1 && blockSafe == -1 ) {
          return; // manual safe mode
        }
        int activeBlocks = dir.activeBlocks.size();
        if( blockTotal != activeBlocks )
          throw new IOException( "blockTotal " + blockTotal 
              + " does not match all blocks count. " 
              + "activeBlocks = " + activeBlocks 
              + ". safeBlocks = " + blockSafe 
              + " safeMode is: " 
              + ((safeMode == null) ? "null" : safeMode.toString()) ); 
        if( blockSafe < 0 || blockSafe > blockTotal )
          throw new IOException( "blockSafe " + blockSafe 
              + " is out of range [0," + blockTotal + "]. " 
              + "activeBlocks = " + activeBlocks 
              + " safeMode is: " 
              + ((safeMode == null) ? "null" : safeMode.toString()) ); 
      } 
    }
    
    /**
     * Periodically check whether it is time to leave safe mode.
     * This thread starts when the threshold level is reached.
     *
     * @author Konstantin Shvachko
     */
    class SafeModeMonitor implements Runnable {
      /** interval in msec for checking safe mode: {@value} */
      private static final long recheckInterval = 1000;
      
      /**
       */
      public void run() {
        while( ! safeMode.canLeave() ) {
          try {
            Thread.sleep(recheckInterval);
          } catch (InterruptedException ie) {
          }
        }
        // leave safe mode an stop the monitor
        safeMode.leave();
        smmthread = null;
      }
    }
    
    /**
     * Current system time.
     * @return current time in msec.
     */
    static long now() {
      return System.currentTimeMillis();
    }
    
    /**
     * Check whether the name node is in safe mode.
     * @return true if safe mode is ON, false otherwise
     */
    boolean isInSafeMode() {
      if( safeMode == null )
        return false;
      return safeMode.isOn();
    }
    
    /**
     * Increment number of blocks that reached minimal replication.
     * @param replication current replication 
     */
    void incrementSafeBlockCount( int replication ) {
      if( safeMode == null )
        return;
      safeMode.incrementSafeBlockCount( (short)replication );
    }

    /**
     * Decrement number of blocks that reached minimal replication.
     * @param replication current replication
     */
    void decrementSafeBlockCount( int replication ) {
      if( safeMode == null )
        return;
      safeMode.decrementSafeBlockCount( (short)replication );
    }

    /**
     * Set the total number of blocks in the system. 
     */
    void setBlockTotal() {
      if( safeMode == null )
        return;
      safeMode.setBlockTotal( dir.activeBlocks.size() );
    }

    /**
     * Enter safe mode manually.
     * @throws IOException
     */
    synchronized void enterSafeMode() throws IOException {
      if( isInSafeMode() ) {
        NameNode.stateChangeLog.info(
            "STATE* FSNamesystem.enterSafeMode: " + "Safe mode is already ON."); 
        return;
      }
      safeMode = new SafeModeInfo();
    }
    
    /**
     * Leave safe mode.
     * @throws IOException
     */
    synchronized void leaveSafeMode() throws IOException {
      if( ! isInSafeMode() ) {
        NameNode.stateChangeLog.info(
            "STATE* FSNamesystem.leaveSafeMode: " + "Safe mode is already OFF."); 
        return;
      }
      safeMode.leave();
    }
    
    String getSafeModeTip() {
      if( ! isInSafeMode() )
        return "";
      return safeMode.getTurnOffTip();
    }

    long getEditLogSize() throws IOException {
      return getEditLog().getEditLogSize();
    }

    synchronized void rollEditLog() throws IOException {
      if (isInSafeMode()) {
        throw new SafeModeException("Checkpoint not created",
                                     safeMode);
      }
      LOG.info("Roll Edit Log");
      getEditLog().rollEditLog();
    }

    synchronized void rollFSImage() throws IOException {
      LOG.info("Roll FSImage");
      if (isInSafeMode()) {
        throw new SafeModeException("Checkpoint not created",
                                    safeMode);
      }
      dir.fsImage.rollFSImage();
    }

    File getFsImageName() throws IOException {
      return dir.fsImage.getFsImageName();
    }

    File[] getFsImageNameCheckpoint() throws IOException {
      return dir.fsImage.getFsImageNameCheckpoint();
    }

    File getFsEditName() throws IOException {
      return getEditLog().getFsEditName();
    }
    
    /**
     * This class is used in Namesystem's jetty to do fsck on namenode
     * @author Milind Bhandarkar
     */
    public static class FsckServlet extends HttpServlet {
      public void doGet(HttpServletRequest request,
          HttpServletResponse response
          ) throws ServletException, IOException {
        Map<String,String[]> pmap = request.getParameterMap();
        try {
          ServletContext context = getServletContext();
          NameNode nn = (NameNode) context.getAttribute("name.node");
          Configuration conf = (Configuration) context.getAttribute("name.conf");
          NamenodeFsck fscker = new NamenodeFsck(conf, nn, pmap, response);
          fscker.fsck();
        } catch (IOException ie) {
          StringUtils.stringifyException(ie);
          LOG.warn(ie);
          String errMsg = "Fsck on path " + pmap.get("path") + " failed.";
          response.sendError(HttpServletResponse.SC_GONE, errMsg);
          throw ie;
        }
      }
    }

    /**
     * This class is used in Namesystem's jetty to retrieve a file.
     * Typically used by the Secondary NameNode to retrieve image and
     * edit file for periodic checkpointing.
     * @author Dhruba Borthakur
     */
    public static class GetImageServlet extends HttpServlet {
      public void doGet(HttpServletRequest request,
          HttpServletResponse response
          ) throws ServletException, IOException {
        Map<String,String[]> pmap = request.getParameterMap();
        try {
          ServletContext context = getServletContext();
          NameNode nn = (NameNode) context.getAttribute("name.node");
          Configuration conf = (Configuration) context.getAttribute("name.conf");
          TransferFsImage ff = new TransferFsImage(pmap, request, response);
          if (ff.getImage()) {
            // send fsImage to Secondary
            TransferFsImage.getFileServer(response.getOutputStream(),
                                          nn.getFsImageName()); 
          } else if (ff.getEdit()) {
            // send old edits to Secondary
            TransferFsImage.getFileServer(response.getOutputStream(),
                                          nn.getFsEditName());
          } else if (ff.putImage()) {
            // issue a HTTP get request to download the new fsimage 
            TransferFsImage.getFileClient(ff.getInfoServer(), "getimage=1", 
                                          nn.getFsImageNameCheckpoint());
          }
        } catch (IOException ie) {
          StringUtils.stringifyException(ie);
          LOG.warn(ie);
          String errMsg = "GetImage failed.";
          response.sendError(HttpServletResponse.SC_GONE, errMsg);
          throw ie;
        }
      }
    }
}
