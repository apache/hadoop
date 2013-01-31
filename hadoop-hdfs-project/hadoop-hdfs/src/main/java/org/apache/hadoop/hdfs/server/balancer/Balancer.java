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
package org.apache.hadoop.hdfs.server.balancer;

import static com.google.common.base.Preconditions.checkArgument;

import static org.apache.hadoop.hdfs.protocolPB.PBHelper.vintPrefixed;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintStream;
import java.net.Socket;
import java.net.URI;
import java.text.DateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.Formatter;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.DatanodeReportType;
import org.apache.hadoop.hdfs.protocol.datatransfer.DataTransferEncryptor;
import org.apache.hadoop.hdfs.protocol.datatransfer.IOStreamPair;
import org.apache.hadoop.hdfs.protocol.datatransfer.Sender;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.BlockOpResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.Status;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenIdentifier;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockPlacementPolicy;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockPlacementPolicyDefault;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.namenode.UnsupportedActionException;
import org.apache.hadoop.hdfs.server.protocol.BlocksWithLocations.BlockWithLocations;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.net.NetworkTopology;
import org.apache.hadoop.net.Node;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/** <p>The balancer is a tool that balances disk space usage on an HDFS cluster
 * when some datanodes become full or when new empty nodes join the cluster.
 * The tool is deployed as an application program that can be run by the 
 * cluster administrator on a live HDFS cluster while applications
 * adding and deleting files.
 * 
 * <p>SYNOPSIS
 * <pre>
 * To start:
 *      bin/start-balancer.sh [-threshold <threshold>]
 *      Example: bin/ start-balancer.sh 
 *                     start the balancer with a default threshold of 10%
 *               bin/ start-balancer.sh -threshold 5
 *                     start the balancer with a threshold of 5%
 * To stop:
 *      bin/ stop-balancer.sh
 * </pre>
 * 
 * <p>DESCRIPTION
 * <p>The threshold parameter is a fraction in the range of (1%, 100%) with a 
 * default value of 10%. The threshold sets a target for whether the cluster 
 * is balanced. A cluster is balanced if for each datanode, the utilization 
 * of the node (ratio of used space at the node to total capacity of the node) 
 * differs from the utilization of the (ratio of used space in the cluster 
 * to total capacity of the cluster) by no more than the threshold value. 
 * The smaller the threshold, the more balanced a cluster will become. 
 * It takes more time to run the balancer for small threshold values. 
 * Also for a very small threshold the cluster may not be able to reach the 
 * balanced state when applications write and delete files concurrently.
 * 
 * <p>The tool moves blocks from highly utilized datanodes to poorly 
 * utilized datanodes iteratively. In each iteration a datanode moves or 
 * receives no more than the lesser of 10G bytes or the threshold fraction 
 * of its capacity. Each iteration runs no more than 20 minutes.
 * At the end of each iteration, the balancer obtains updated datanodes
 * information from the namenode.
 * 
 * <p>A system property that limits the balancer's use of bandwidth is 
 * defined in the default configuration file:
 * <pre>
 * <property>
 *   <name>dfs.balance.bandwidthPerSec</name>
 *   <value>1048576</value>
 * <description>  Specifies the maximum bandwidth that each datanode 
 * can utilize for the balancing purpose in term of the number of bytes 
 * per second. </description>
 * </property>
 * </pre>
 * 
 * <p>This property determines the maximum speed at which a block will be 
 * moved from one datanode to another. The default value is 1MB/s. The higher 
 * the bandwidth, the faster a cluster can reach the balanced state, 
 * but with greater competition with application processes. If an 
 * administrator changes the value of this property in the configuration 
 * file, the change is observed when HDFS is next restarted.
 * 
 * <p>MONITERING BALANCER PROGRESS
 * <p>After the balancer is started, an output file name where the balancer 
 * progress will be recorded is printed on the screen.  The administrator 
 * can monitor the running of the balancer by reading the output file. 
 * The output shows the balancer's status iteration by iteration. In each 
 * iteration it prints the starting time, the iteration number, the total 
 * number of bytes that have been moved in the previous iterations, 
 * the total number of bytes that are left to move in order for the cluster 
 * to be balanced, and the number of bytes that are being moved in this 
 * iteration. Normally "Bytes Already Moved" is increasing while "Bytes Left 
 * To Move" is decreasing.
 * 
 * <p>Running multiple instances of the balancer in an HDFS cluster is 
 * prohibited by the tool.
 * 
 * <p>The balancer automatically exits when any of the following five 
 * conditions is satisfied:
 * <ol>
 * <li>The cluster is balanced;
 * <li>No block can be moved;
 * <li>No block has been moved for five consecutive iterations;
 * <li>An IOException occurs while communicating with the namenode;
 * <li>Another balancer is running.
 * </ol>
 * 
 * <p>Upon exit, a balancer returns an exit code and prints one of the 
 * following messages to the output file in corresponding to the above exit 
 * reasons:
 * <ol>
 * <li>The cluster is balanced. Exiting
 * <li>No block can be moved. Exiting...
 * <li>No block has been moved for 5 iterations. Exiting...
 * <li>Received an IO exception: failure reason. Exiting...
 * <li>Another balancer is running. Exiting...
 * </ol>
 * 
 * <p>The administrator can interrupt the execution of the balancer at any 
 * time by running the command "stop-balancer.sh" on the machine where the 
 * balancer is running.
 */

@InterfaceAudience.Private
public class Balancer {
  static final Log LOG = LogFactory.getLog(Balancer.class);
  final private static long MAX_BLOCKS_SIZE_TO_FETCH = 2*1024*1024*1024L; //2GB
  private static long WIN_WIDTH = 5400*1000L; // 1.5 hour

  /** The maximum number of concurrent blocks moves for 
   * balancing purpose at a datanode
   */
  public static final int MAX_NUM_CONCURRENT_MOVES = 5;
  private static final int MAX_NO_PENDING_BLOCK_ITERATIONS = 5;
  
  private static final String USAGE = "Usage: java "
      + Balancer.class.getSimpleName()
      + "\n\t[-policy <policy>]\tthe balancing policy: "
      + BalancingPolicy.Node.INSTANCE.getName() + " or "
      + BalancingPolicy.Pool.INSTANCE.getName()
      + "\n\t[-threshold <threshold>]\tPercentage of disk capacity";
  
  private final NameNodeConnector nnc;
  private final BalancingPolicy policy;
  private final double threshold;
  
  // all data node lists
  private Collection<Source> overUtilizedDatanodes
                               = new LinkedList<Source>();
  private Collection<Source> aboveAvgUtilizedDatanodes
                               = new LinkedList<Source>();
  private Collection<BalancerDatanode> belowAvgUtilizedDatanodes
                               = new LinkedList<BalancerDatanode>();
  private Collection<BalancerDatanode> underUtilizedDatanodes
                               = new LinkedList<BalancerDatanode>();
  
  private Collection<Source> sources
                               = new HashSet<Source>();
  private Collection<BalancerDatanode> targets
                               = new HashSet<BalancerDatanode>();
  
  private Map<Block, BalancerBlock> globalBlockList
                 = new HashMap<Block, BalancerBlock>();
  private MovedBlocks movedBlocks = new MovedBlocks();
  // Map storage IDs to BalancerDatanodes
  private Map<String, BalancerDatanode> datanodes
                 = new HashMap<String, BalancerDatanode>();
  
  private NetworkTopology cluster;
  final static private int MOVER_THREAD_POOL_SIZE = 1000;
  final private ExecutorService moverExecutor = 
    Executors.newFixedThreadPool(MOVER_THREAD_POOL_SIZE);
  final static private int DISPATCHER_THREAD_POOL_SIZE = 200;
  final private ExecutorService dispatcherExecutor =
    Executors.newFixedThreadPool(DISPATCHER_THREAD_POOL_SIZE);
  

  /* This class keeps track of a scheduled block move */
  private class PendingBlockMove {
    private BalancerBlock block;
    private Source source;
    private BalancerDatanode proxySource;
    private BalancerDatanode target;
    
    /** constructor */
    private PendingBlockMove() {
    }
    
    /* choose a block & a proxy source for this pendingMove 
     * whose source & target have already been chosen.
     * 
     * Return true if a block and its proxy are chosen; false otherwise
     */
    private boolean chooseBlockAndProxy() {
      // iterate all source's blocks until find a good one
      for (Iterator<BalancerBlock> blocks=
        source.getBlockIterator(); blocks.hasNext();) {
        if (markMovedIfGoodBlock(blocks.next())) {
          blocks.remove();
          return true;
        }
      }
      return false;
    }
    
    /* Return true if the given block is good for the tentative move;
     * If it is good, add it to the moved list to marked as "Moved".
     * A block is good if
     * 1. it is a good candidate; see isGoodBlockCandidate
     * 2. can find a proxy source that's not busy for this move
     */
    private boolean markMovedIfGoodBlock(BalancerBlock block) {
      synchronized(block) {
        synchronized(movedBlocks) {
          if (isGoodBlockCandidate(source, target, block)) {
            this.block = block;
            if ( chooseProxySource() ) {
              movedBlocks.add(block);
              if (LOG.isDebugEnabled()) {
                LOG.debug("Decided to move block "+ block.getBlockId()
                    +" with a length of "+StringUtils.byteDesc(block.getNumBytes())
                    + " bytes from " + source.getDisplayName()
                    + " to " + target.getDisplayName()
                    + " using proxy source " + proxySource.getDisplayName() );
              }
              return true;
            }
          }
        }
      }
      return false;
    }
    
    /* Now we find out source, target, and block, we need to find a proxy
     * 
     * @return true if a proxy is found; otherwise false
     */
    private boolean chooseProxySource() {
      final DatanodeInfo targetDN = target.getDatanode();
      boolean find = false;
      for (BalancerDatanode loc : block.getLocations()) {
        // check if there is replica which is on the same rack with the target
        if (cluster.isOnSameRack(loc.getDatanode(), targetDN) && addTo(loc)) {
          find = true;
          // if cluster is not nodegroup aware or the proxy is on the same 
          // nodegroup with target, then we already find the nearest proxy
          if (!cluster.isNodeGroupAware() 
              || cluster.isOnSameNodeGroup(loc.getDatanode(), targetDN)) {
            return true;
          }
        }
        
        if (!find) {
          // find out a non-busy replica out of rack of target
          find = addTo(loc);
        }
      }
      
      return find;
    }
    
    // add a BalancerDatanode as proxy source for specific block movement
    private boolean addTo(BalancerDatanode bdn) {
      if (bdn.addPendingBlock(this)) {
        proxySource = bdn;
        return true;
      }
      return false;
    }
    
    /* Dispatch the block move task to the proxy source & wait for the response
     */
    private void dispatch() {
      Socket sock = new Socket();
      DataOutputStream out = null;
      DataInputStream in = null;
      try {
        sock.connect(
            NetUtils.createSocketAddr(target.datanode.getXferAddr()),
            HdfsServerConstants.READ_TIMEOUT);
        sock.setKeepAlive(true);
        
        OutputStream unbufOut = sock.getOutputStream();
        InputStream unbufIn = sock.getInputStream();
        if (nnc.getDataEncryptionKey() != null) {
          IOStreamPair encryptedStreams =
              DataTransferEncryptor.getEncryptedStreams(
                  unbufOut, unbufIn, nnc.getDataEncryptionKey());
          unbufOut = encryptedStreams.out;
          unbufIn = encryptedStreams.in;
        }
        out = new DataOutputStream(new BufferedOutputStream(unbufOut,
            HdfsConstants.IO_FILE_BUFFER_SIZE));
        in = new DataInputStream(new BufferedInputStream(unbufIn,
            HdfsConstants.IO_FILE_BUFFER_SIZE));
        
        sendRequest(out);
        receiveResponse(in);
        bytesMoved.inc(block.getNumBytes());
        LOG.info( "Moving block " + block.getBlock().getBlockId() +
              " from "+ source.getDisplayName() + " to " +
              target.getDisplayName() + " through " +
              proxySource.getDisplayName() +
              " is succeeded." );
      } catch (IOException e) {
        LOG.warn("Error moving block "+block.getBlockId()+
            " from " + source.getDisplayName() + " to " +
            target.getDisplayName() + " through " +
            proxySource.getDisplayName() +
            ": "+e.getMessage());
      } finally {
        IOUtils.closeStream(out);
        IOUtils.closeStream(in);
        IOUtils.closeSocket(sock);
        
        proxySource.removePendingBlock(this);
        target.removePendingBlock(this);

        synchronized (this ) {
          reset();
        }
        synchronized (Balancer.this) {
          Balancer.this.notifyAll();
        }
      }
    }
    
    /* Send a block replace request to the output stream*/
    private void sendRequest(DataOutputStream out) throws IOException {
      final ExtendedBlock eb = new ExtendedBlock(nnc.blockpoolID, block.getBlock());
      final Token<BlockTokenIdentifier> accessToken = nnc.getAccessToken(eb);
      new Sender(out).replaceBlock(eb, accessToken,
          source.getStorageID(), proxySource.getDatanode());
    }
    
    /* Receive a block copy response from the input stream */ 
    private void receiveResponse(DataInputStream in) throws IOException {
      BlockOpResponseProto response = BlockOpResponseProto.parseFrom(
          vintPrefixed(in));
      if (response.getStatus() != Status.SUCCESS) {
        if (response.getStatus() == Status.ERROR_ACCESS_TOKEN)
          throw new IOException("block move failed due to access token error");
        throw new IOException("block move is failed: " +
            response.getMessage());
      }
    }

    /* reset the object */
    private void reset() {
      block = null;
      source = null;
      proxySource = null;
      target = null;
    }
    
    /* start a thread to dispatch the block move */
    private void scheduleBlockMove() {
      moverExecutor.execute(new Runnable() {
        @Override
        public void run() {
          if (LOG.isDebugEnabled()) {
            LOG.debug("Starting moving "+ block.getBlockId() +
                " from " + proxySource.getDisplayName() + " to " +
                target.getDisplayName());
          }
          dispatch();
        }
      });
    }
  }
  
  /* A class for keeping track of blocks in the Balancer */
  static private class BalancerBlock {
    private Block block; // the block
    private List<BalancerDatanode> locations
            = new ArrayList<BalancerDatanode>(3); // its locations
    
    /* Constructor */
    private BalancerBlock(Block block) {
      this.block = block;
    }
    
    /* clean block locations */
    private synchronized void clearLocations() {
      locations.clear();
    }
    
    /* add a location */
    private synchronized void addLocation(BalancerDatanode datanode) {
      if (!locations.contains(datanode)) {
        locations.add(datanode);
      }
    }
    
    /* Return if the block is located on <code>datanode</code> */
    private synchronized boolean isLocatedOnDatanode(
        BalancerDatanode datanode) {
      return locations.contains(datanode);
    }
    
    /* Return its locations */
    private synchronized List<BalancerDatanode> getLocations() {
      return locations;
    }
    
    /* Return the block */
    private Block getBlock() {
      return block;
    }
    
    /* Return the block id */
    private long getBlockId() {
      return block.getBlockId();
    }
    
    /* Return the length of the block */
    private long getNumBytes() {
      return block.getNumBytes();
    }
  }
  
  /* The class represents a desired move of bytes between two nodes 
   * and the target.
   * An object of this class is stored in a source node. 
   */
  static private class NodeTask {
    private BalancerDatanode datanode; //target node
    private long size;  //bytes scheduled to move
    
    /* constructor */
    private NodeTask(BalancerDatanode datanode, long size) {
      this.datanode = datanode;
      this.size = size;
    }
    
    /* Get the node */
    private BalancerDatanode getDatanode() {
      return datanode;
    }
    
    /* Get the number of bytes that need to be moved */
    private long getSize() {
      return size;
    }
  }
  
  
  /* A class that keeps track of a datanode in Balancer */
  private static class BalancerDatanode {
    final private static long MAX_SIZE_TO_MOVE = 10*1024*1024*1024L; //10GB
    final DatanodeInfo datanode;
    final double utilization;
    final long maxSize2Move;
    protected long scheduledSize = 0L;
    //  blocks being moved but not confirmed yet
    private List<PendingBlockMove> pendingBlocks = 
      new ArrayList<PendingBlockMove>(MAX_NUM_CONCURRENT_MOVES); 
    
    @Override
    public String toString() {
      return getClass().getSimpleName() + "[" + datanode
          + ", utilization=" + utilization + "]";
    }

    /* Constructor 
     * Depending on avgutil & threshold, calculate maximum bytes to move 
     */
    private BalancerDatanode(DatanodeInfo node, BalancingPolicy policy, double threshold) {
      datanode = node;
      utilization = policy.getUtilization(node);
      final double avgUtil = policy.getAvgUtilization();
      long maxSizeToMove;

      if (utilization >= avgUtil+threshold
          || utilization <= avgUtil-threshold) { 
        maxSizeToMove = (long)(threshold*datanode.getCapacity()/100);
      } else {
        maxSizeToMove = 
          (long)(Math.abs(avgUtil-utilization)*datanode.getCapacity()/100);
      }
      if (utilization < avgUtil ) {
        maxSizeToMove = Math.min(datanode.getRemaining(), maxSizeToMove);
      }
      this.maxSize2Move = Math.min(MAX_SIZE_TO_MOVE, maxSizeToMove);
    }
    
    /** Get the datanode */
    protected DatanodeInfo getDatanode() {
      return datanode;
    }
    
    /** Get the name of the datanode */
    protected String getDisplayName() {
      return datanode.toString();
    }
    
    /* Get the storage id of the datanode */
    protected String getStorageID() {
      return datanode.getStorageID();
    }
    
    /** Decide if still need to move more bytes */
    protected boolean hasSpaceForScheduling() {
      return scheduledSize<maxSize2Move;
    }

    /** Return the total number of bytes that need to be moved */
    protected long availableSizeToMove() {
      return maxSize2Move-scheduledSize;
    }
    
    /* increment scheduled size */
    protected void incScheduledSize(long size) {
      scheduledSize += size;
    }
    
    /* Check if the node can schedule more blocks to move */
    synchronized private boolean isPendingQNotFull() {
      if ( pendingBlocks.size() < MAX_NUM_CONCURRENT_MOVES ) {
        return true;
      }
      return false;
    }
    
    /* Check if all the dispatched moves are done */
    synchronized private boolean isPendingQEmpty() {
      return pendingBlocks.isEmpty();
    }
    
    /* Add a scheduled block move to the node */
    private synchronized boolean addPendingBlock(
        PendingBlockMove pendingBlock) {
      if (isPendingQNotFull()) {
        return pendingBlocks.add(pendingBlock);
      }
      return false;
    }
    
    /* Remove a scheduled block move from the node */
    private synchronized boolean  removePendingBlock(
        PendingBlockMove pendingBlock) {
      return pendingBlocks.remove(pendingBlock);
    }
  }
  
  /** A node that can be the sources of a block move */
  private class Source extends BalancerDatanode {
    
    /* A thread that initiates a block move 
     * and waits for block move to complete */
    private class BlockMoveDispatcher implements Runnable {
      @Override
      public void run() {
        dispatchBlocks();
      }
    }
    
    private ArrayList<NodeTask> nodeTasks = new ArrayList<NodeTask>(2);
    private long blocksToReceive = 0L;
    /* source blocks point to balancerBlocks in the global list because
     * we want to keep one copy of a block in balancer and be aware that
     * the locations are changing over time.
     */
    private List<BalancerBlock> srcBlockList
            = new ArrayList<BalancerBlock>();
    
    /* constructor */
    private Source(DatanodeInfo node, BalancingPolicy policy, double threshold) {
      super(node, policy, threshold);
    }
    
    /** Add a node task */
    private void addNodeTask(NodeTask task) {
      assert (task.datanode != this) :
        "Source and target are the same " + datanode;
      incScheduledSize(task.getSize());
      nodeTasks.add(task);
    }
    
    /* Return an iterator to this source's blocks */
    private Iterator<BalancerBlock> getBlockIterator() {
      return srcBlockList.iterator();
    }
    
    /* fetch new blocks of this source from namenode and
     * update this source's block list & the global block list
     * Return the total size of the received blocks in the number of bytes.
     */
    private long getBlockList() throws IOException {
      BlockWithLocations[] newBlocks = nnc.namenode.getBlocks(datanode, 
        Math.min(MAX_BLOCKS_SIZE_TO_FETCH, blocksToReceive)).getBlocks();
      long bytesReceived = 0;
      for (BlockWithLocations blk : newBlocks) {
        bytesReceived += blk.getBlock().getNumBytes();
        BalancerBlock block;
        synchronized(globalBlockList) {
          block = globalBlockList.get(blk.getBlock());
          if (block==null) {
            block = new BalancerBlock(blk.getBlock());
            globalBlockList.put(blk.getBlock(), block);
          } else {
            block.clearLocations();
          }
        
          synchronized (block) {
            // update locations
            for ( String storageID : blk.getStorageIDs() ) {
              BalancerDatanode datanode = datanodes.get(storageID);
              if (datanode != null) { // not an unknown datanode
                block.addLocation(datanode);
              }
            }
          }
          if (!srcBlockList.contains(block) && isGoodBlockCandidate(block)) {
            // filter bad candidates
            srcBlockList.add(block);
          }
        }
      }
      return bytesReceived;
    }

    /* Decide if the given block is a good candidate to move or not */
    private boolean isGoodBlockCandidate(BalancerBlock block) {
      for (NodeTask nodeTask : nodeTasks) {
        if (Balancer.this.isGoodBlockCandidate(this, nodeTask.datanode, block)) {
          return true;
        }
      }
      return false;
    }

    /* Return a block that's good for the source thread to dispatch immediately
     * The block's source, target, and proxy source are determined too.
     * When choosing proxy and target, source & target throttling
     * has been considered. They are chosen only when they have the capacity
     * to support this block move.
     * The block should be dispatched immediately after this method is returned.
     */
    private PendingBlockMove chooseNextBlockToMove() {
      for ( Iterator<NodeTask> tasks=nodeTasks.iterator(); tasks.hasNext(); ) {
        NodeTask task = tasks.next();
        BalancerDatanode target = task.getDatanode();
        PendingBlockMove pendingBlock = new PendingBlockMove();
        if (target.addPendingBlock(pendingBlock)) { 
          // target is not busy, so do a tentative block allocation
          pendingBlock.source = this;
          pendingBlock.target = target;
          if ( pendingBlock.chooseBlockAndProxy() ) {
            long blockSize = pendingBlock.block.getNumBytes(); 
            scheduledSize -= blockSize;
            task.size -= blockSize;
            if (task.size == 0) {
              tasks.remove();
            }
            return pendingBlock;
          } else {
            // cancel the tentative move
            target.removePendingBlock(pendingBlock);
          }
        }
      }
      return null;
    }

    /* iterate all source's blocks to remove moved ones */    
    private void filterMovedBlocks() {
      for (Iterator<BalancerBlock> blocks=getBlockIterator();
            blocks.hasNext();) {
        if (movedBlocks.contains(blocks.next())) {
          blocks.remove();
        }
      }
    }
    
    private static final int SOURCE_BLOCK_LIST_MIN_SIZE=5;
    /* Return if should fetch more blocks from namenode */
    private boolean shouldFetchMoreBlocks() {
      return srcBlockList.size()<SOURCE_BLOCK_LIST_MIN_SIZE &&
                 blocksToReceive>0;
    }
    
    /* This method iteratively does the following:
     * it first selects a block to move,
     * then sends a request to the proxy source to start the block move
     * when the source's block list falls below a threshold, it asks
     * the namenode for more blocks.
     * It terminates when it has dispatch enough block move tasks or
     * it has received enough blocks from the namenode, or 
     * the elapsed time of the iteration has exceeded the max time limit.
     */ 
    private static final long MAX_ITERATION_TIME = 20*60*1000L; //20 mins
    private void dispatchBlocks() {
      long startTime = Time.now();
      this.blocksToReceive = 2*scheduledSize;
      boolean isTimeUp = false;
      int noPendingBlockIteration = 0;
      while(!isTimeUp && scheduledSize>0 &&
          (!srcBlockList.isEmpty() || blocksToReceive>0)) {
        PendingBlockMove pendingBlock = chooseNextBlockToMove();
        if (pendingBlock != null) {
          // move the block
          pendingBlock.scheduleBlockMove();
          continue;
        }
        
        /* Since we can not schedule any block to move,
         * filter any moved blocks from the source block list and
         * check if we should fetch more blocks from the namenode
         */
        filterMovedBlocks(); // filter already moved blocks
        if (shouldFetchMoreBlocks()) {
          // fetch new blocks
          try {
            blocksToReceive -= getBlockList();
            continue;
          } catch (IOException e) {
            LOG.warn("Exception while getting block list", e);
            return;
          }
        } else {
          // source node cannot find a pendingBlockToMove, iteration +1
          noPendingBlockIteration++;
          // in case no blocks can be moved for source node's task,
          // jump out of while-loop after 5 iterations.
          if (noPendingBlockIteration >= MAX_NO_PENDING_BLOCK_ITERATIONS) {
            scheduledSize = 0;
          }
        }
        
        // check if time is up or not
        if (Time.now()-startTime > MAX_ITERATION_TIME) {
          isTimeUp = true;
          continue;
        }
        
        /* Now we can not schedule any block to move and there are
         * no new blocks added to the source block list, so we wait. 
         */
        try {
          synchronized(Balancer.this) {
            Balancer.this.wait(1000);  // wait for targets/sources to be idle
          }
        } catch (InterruptedException ignored) {
        }
      }
    }
  }

  /* Check that this Balancer is compatible with the Block Placement Policy
   * used by the Namenode.
   */
  private static void checkReplicationPolicyCompatibility(Configuration conf
      ) throws UnsupportedActionException {
    if (!(BlockPlacementPolicy.getInstance(conf, null, null) instanceof 
        BlockPlacementPolicyDefault)) {
      throw new UnsupportedActionException(
          "Balancer without BlockPlacementPolicyDefault");
    }
  }

  /**
   * Construct a balancer.
   * Initialize balancer. It sets the value of the threshold, and 
   * builds the communication proxies to
   * namenode as a client and a secondary namenode and retry proxies
   * when connection fails.
   */
  Balancer(NameNodeConnector theblockpool, Parameters p, Configuration conf) {
    this.threshold = p.threshold;
    this.policy = p.policy;
    this.nnc = theblockpool;
    cluster = NetworkTopology.getInstance(conf);
  }
  
  /* Shuffle datanode array */
  static private void shuffleArray(DatanodeInfo[] datanodes) {
    for (int i=datanodes.length; i>1; i--) {
      int randomIndex = DFSUtil.getRandom().nextInt(i);
      DatanodeInfo tmp = datanodes[randomIndex];
      datanodes[randomIndex] = datanodes[i-1];
      datanodes[i-1] = tmp;
    }
  }
  
  /* Given a data node set, build a network topology and decide
   * over-utilized datanodes, above average utilized datanodes, 
   * below average utilized datanodes, and underutilized datanodes. 
   * The input data node set is shuffled before the datanodes 
   * are put into the over-utilized datanodes, above average utilized
   * datanodes, below average utilized datanodes, and
   * underutilized datanodes lists. This will add some randomness
   * to the node matching later on.
   * 
   * @return the total number of bytes that are 
   *                needed to move to make the cluster balanced.
   * @param datanodes a set of datanodes
   */
  private long initNodes(DatanodeInfo[] datanodes) {
    // compute average utilization
    for (DatanodeInfo datanode : datanodes) {
      if (datanode.isDecommissioned() || datanode.isDecommissionInProgress()) {
        continue; // ignore decommissioning or decommissioned nodes
      }
      policy.accumulateSpaces(datanode);
    }
    policy.initAvgUtilization();

    /*create network topology and all data node lists: 
     * overloaded, above-average, below-average, and underloaded
     * we alternates the accessing of the given datanodes array either by
     * an increasing order or a decreasing order.
     */  
    long overLoadedBytes = 0L, underLoadedBytes = 0L;
    shuffleArray(datanodes);
    for (DatanodeInfo datanode : datanodes) {
      if (datanode.isDecommissioned() || datanode.isDecommissionInProgress()) {
        continue; // ignore decommissioning or decommissioned nodes
      }
      cluster.add(datanode);
      BalancerDatanode datanodeS;
      final double avg = policy.getAvgUtilization();
      if (policy.getUtilization(datanode) >= avg) {
        datanodeS = new Source(datanode, policy, threshold);
        if (isAboveAvgUtilized(datanodeS)) {
          this.aboveAvgUtilizedDatanodes.add((Source)datanodeS);
        } else {
          assert(isOverUtilized(datanodeS)) :
            datanodeS.getDisplayName()+ "is not an overUtilized node";
          this.overUtilizedDatanodes.add((Source)datanodeS);
          overLoadedBytes += (long)((datanodeS.utilization-avg
              -threshold)*datanodeS.datanode.getCapacity()/100.0);
        }
      } else {
        datanodeS = new BalancerDatanode(datanode, policy, threshold);
        if ( isBelowOrEqualAvgUtilized(datanodeS)) {
          this.belowAvgUtilizedDatanodes.add(datanodeS);
        } else {
          assert isUnderUtilized(datanodeS) : "isUnderUtilized("
              + datanodeS.getDisplayName() + ")=" + isUnderUtilized(datanodeS)
              + ", utilization=" + datanodeS.utilization; 
          this.underUtilizedDatanodes.add(datanodeS);
          underLoadedBytes += (long)((avg-threshold-
              datanodeS.utilization)*datanodeS.datanode.getCapacity()/100.0);
        }
      }
      this.datanodes.put(datanode.getStorageID(), datanodeS);
    }

    //logging
    logNodes();
    
    assert (this.datanodes.size() == 
      overUtilizedDatanodes.size()+underUtilizedDatanodes.size()+
      aboveAvgUtilizedDatanodes.size()+belowAvgUtilizedDatanodes.size())
      : "Mismatched number of datanodes";
    
    // return number of bytes to be moved in order to make the cluster balanced
    return Math.max(overLoadedBytes, underLoadedBytes);
  }

  /* log the over utilized & under utilized nodes */
  private void logNodes() {
    logNodes("over-utilized", overUtilizedDatanodes);
    if (LOG.isTraceEnabled()) {
      logNodes("above-average", aboveAvgUtilizedDatanodes);
      logNodes("below-average", belowAvgUtilizedDatanodes);
    }
    logNodes("underutilized", underUtilizedDatanodes);
  }

  private static <T extends BalancerDatanode> void logNodes(
      String name, Collection<T> nodes) {
    LOG.info(nodes.size() + " " + name + ": " + nodes);
  }

  /** A matcher interface for matching nodes. */
  private interface Matcher {
    /** Given the cluster topology, does the left node match the right node? */
    boolean match(NetworkTopology cluster, Node left,  Node right);
  }

  /** Match datanodes in the same node group. */
  static final Matcher SAME_NODE_GROUP = new Matcher() {
    @Override
    public boolean match(NetworkTopology cluster, Node left, Node right) {
      return cluster.isOnSameNodeGroup(left, right);
    }
  };

  /** Match datanodes in the same rack. */
  static final Matcher SAME_RACK = new Matcher() {
    @Override
    public boolean match(NetworkTopology cluster, Node left, Node right) {
      return cluster.isOnSameRack(left, right);
    }
  };

  /** Match any datanode with any other datanode. */
  static final Matcher ANY_OTHER = new Matcher() {
    @Override
    public boolean match(NetworkTopology cluster, Node left, Node right) {
      return left != right;
    }
  };

  /**
   * Decide all <source, target> pairs and
   * the number of bytes to move from a source to a target
   * Maximum bytes to be moved per node is
   * Min(1 Band worth of bytes,  MAX_SIZE_TO_MOVE).
   * Return total number of bytes to move in this iteration
   */
  private long chooseNodes() {
    // First, match nodes on the same node group if cluster is node group aware
    if (cluster.isNodeGroupAware()) {
      chooseNodes(SAME_NODE_GROUP);
    }
    
    // Then, match nodes on the same rack
    chooseNodes(SAME_RACK);
    // At last, match all remaining nodes
    chooseNodes(ANY_OTHER);
    
    assert (datanodes.size() >= sources.size()+targets.size())
      : "Mismatched number of datanodes (" +
      datanodes.size() + " total, " +
      sources.size() + " sources, " +
      targets.size() + " targets)";

    long bytesToMove = 0L;
    for (Source src : sources) {
      bytesToMove += src.scheduledSize;
    }
    return bytesToMove;
  }

  /** Decide all <source, target> pairs according to the matcher. */
  private void chooseNodes(final Matcher matcher) {
    /* first step: match each overUtilized datanode (source) to
     * one or more underUtilized datanodes (targets).
     */
    chooseDatanodes(overUtilizedDatanodes, underUtilizedDatanodes, matcher);
    
    /* match each remaining overutilized datanode (source) to 
     * below average utilized datanodes (targets).
     * Note only overutilized datanodes that haven't had that max bytes to move
     * satisfied in step 1 are selected
     */
    chooseDatanodes(overUtilizedDatanodes, belowAvgUtilizedDatanodes, matcher);

    /* match each remaining underutilized datanode (target) to 
     * above average utilized datanodes (source).
     * Note only underutilized datanodes that have not had that max bytes to
     * move satisfied in step 1 are selected.
     */
    chooseDatanodes(underUtilizedDatanodes, aboveAvgUtilizedDatanodes, matcher);
  }

  /**
   * For each datanode, choose matching nodes from the candidates. Either the
   * datanodes or the candidates are source nodes with (utilization > Avg), and
   * the others are target nodes with (utilization < Avg).
   */
  private <D extends BalancerDatanode, C extends BalancerDatanode> void 
      chooseDatanodes(Collection<D> datanodes, Collection<C> candidates,
          Matcher matcher) {
    for (Iterator<D> i = datanodes.iterator(); i.hasNext();) {
      final D datanode = i.next();
      for(; chooseForOneDatanode(datanode, candidates, matcher); );
      if (!datanode.hasSpaceForScheduling()) {
        i.remove();
      }
    }
  }

  /**
   * For the given datanode, choose a candidate and then schedule it.
   * @return true if a candidate is chosen; false if no candidates is chosen.
   */
  private <C extends BalancerDatanode> boolean chooseForOneDatanode(
      BalancerDatanode dn, Collection<C> candidates, Matcher matcher) {
    final Iterator<C> i = candidates.iterator();
    final C chosen = chooseCandidate(dn, i, matcher);

    if (chosen == null) {
      return false;
    }
    if (dn instanceof Source) {
      matchSourceWithTargetToMove((Source)dn, chosen);
    } else {
      matchSourceWithTargetToMove((Source)chosen, dn);
    }
    if (!chosen.hasSpaceForScheduling()) {
      i.remove();
    }
    return true;
  }
  
  private void matchSourceWithTargetToMove(
      Source source, BalancerDatanode target) {
    long size = Math.min(source.availableSizeToMove(), target.availableSizeToMove());
    NodeTask nodeTask = new NodeTask(target, size);
    source.addNodeTask(nodeTask);
    target.incScheduledSize(nodeTask.getSize());
    sources.add(source);
    targets.add(target);
    LOG.info("Decided to move "+StringUtils.byteDesc(size)+" bytes from "
        +source.datanode.getName() + " to " + target.datanode.getName());
  }
  
  /** Choose a candidate for the given datanode. */
  private <D extends BalancerDatanode, C extends BalancerDatanode>
      C chooseCandidate(D dn, Iterator<C> candidates, Matcher matcher) {
    if (dn.hasSpaceForScheduling()) {
      for(; candidates.hasNext(); ) {
        final C c = candidates.next();
        if (!c.hasSpaceForScheduling()) {
          candidates.remove();
        } else if (matcher.match(cluster, dn.getDatanode(), c.getDatanode())) {
          return c;
        }
      }
    }
    return null;
  }

  private static class BytesMoved {
    private long bytesMoved = 0L;;
    private synchronized void inc( long bytes ) {
      bytesMoved += bytes;
    }

    private long get() {
      return bytesMoved;
    }
  };
  private BytesMoved bytesMoved = new BytesMoved();
  
  /* Start a thread to dispatch block moves for each source. 
   * The thread selects blocks to move & sends request to proxy source to
   * initiate block move. The process is flow controlled. Block selection is
   * blocked if there are too many un-confirmed block moves.
   * Return the total number of bytes successfully moved in this iteration.
   */
  private long dispatchBlockMoves() throws InterruptedException {
    long bytesLastMoved = bytesMoved.get();
    Future<?>[] futures = new Future<?>[sources.size()];
    int i=0;
    for (Source source : sources) {
      futures[i++] = dispatcherExecutor.submit(source.new BlockMoveDispatcher());
    }
    
    // wait for all dispatcher threads to finish
    for (Future<?> future : futures) {
      try {
        future.get();
      } catch (ExecutionException e) {
        LOG.warn("Dispatcher thread failed", e.getCause());
      }
    }
    
    // wait for all block moving to be done
    waitForMoveCompletion();
    
    return bytesMoved.get()-bytesLastMoved;
  }
  
  // The sleeping period before checking if block move is completed again
  static private long blockMoveWaitTime = 30000L;
  
  /** set the sleeping period for block move completion check */
  static void setBlockMoveWaitTime(long time) {
    blockMoveWaitTime = time;
  }
  
  /* wait for all block move confirmations 
   * by checking each target's pendingMove queue 
   */
  private void waitForMoveCompletion() {
    boolean shouldWait;
    do {
      shouldWait = false;
      for (BalancerDatanode target : targets) {
        if (!target.isPendingQEmpty()) {
          shouldWait = true;
        }
      }
      if (shouldWait) {
        try {
          Thread.sleep(blockMoveWaitTime);
        } catch (InterruptedException ignored) {
        }
      }
    } while (shouldWait);
  }

  /** This window makes sure to keep blocks that have been moved within 1.5 hour.
   * Old window has blocks that are older;
   * Current window has blocks that are more recent;
   * Cleanup method triggers the check if blocks in the old window are
   * more than 1.5 hour old. If yes, purge the old window and then
   * move blocks in current window to old window.
   */ 
  private static class MovedBlocks {
    private long lastCleanupTime = Time.now();
    final private static int CUR_WIN = 0;
    final private static int OLD_WIN = 1;
    final private static int NUM_WINS = 2;
    final private List<HashMap<Block, BalancerBlock>> movedBlocks = 
      new ArrayList<HashMap<Block, BalancerBlock>>(NUM_WINS);
    
    /* initialize the moved blocks collection */
    private MovedBlocks() {
      movedBlocks.add(new HashMap<Block,BalancerBlock>());
      movedBlocks.add(new HashMap<Block,BalancerBlock>());
    }

    /* add a block thus marking a block to be moved */
    synchronized private void add(BalancerBlock block) {
      movedBlocks.get(CUR_WIN).put(block.getBlock(), block);
    }

    /* check if a block is marked as moved */
    synchronized private boolean contains(BalancerBlock block) {
      return contains(block.getBlock());
    }

    /* check if a block is marked as moved */
    synchronized private boolean contains(Block block) {
      return movedBlocks.get(CUR_WIN).containsKey(block) ||
        movedBlocks.get(OLD_WIN).containsKey(block);
    }

    /* remove old blocks */
    synchronized private void cleanup() {
      long curTime = Time.now();
      // check if old win is older than winWidth
      if (lastCleanupTime + WIN_WIDTH <= curTime) {
        // purge the old window
        movedBlocks.set(OLD_WIN, movedBlocks.get(CUR_WIN));
        movedBlocks.set(CUR_WIN, new HashMap<Block, BalancerBlock>());
        lastCleanupTime = curTime;
      }
    }
  }

  /* Decide if it is OK to move the given block from source to target
   * A block is a good candidate if
   * 1. the block is not in the process of being moved/has not been moved;
   * 2. the block does not have a replica on the target;
   * 3. doing the move does not reduce the number of racks that the block has
   */
  private boolean isGoodBlockCandidate(Source source, 
      BalancerDatanode target, BalancerBlock block) {
    // check if the block is moved or not
    if (movedBlocks.contains(block)) {
        return false;
    }
    if (block.isLocatedOnDatanode(target)) {
      return false;
    }
    if (cluster.isNodeGroupAware() && 
        isOnSameNodeGroupWithReplicas(target, block, source)) {
      return false;
    }

    boolean goodBlock = false;
    if (cluster.isOnSameRack(source.getDatanode(), target.getDatanode())) {
      // good if source and target are on the same rack
      goodBlock = true;
    } else {
      boolean notOnSameRack = true;
      synchronized (block) {
        for (BalancerDatanode loc : block.locations) {
          if (cluster.isOnSameRack(loc.datanode, target.datanode)) {
            notOnSameRack = false;
            break;
          }
        }
      }
      if (notOnSameRack) {
        // good if target is target is not on the same rack as any replica
        goodBlock = true;
      } else {
        // good if source is on the same rack as on of the replicas
        for (BalancerDatanode loc : block.locations) {
          if (loc != source && 
              cluster.isOnSameRack(loc.datanode, source.datanode)) {
            goodBlock = true;
            break;
          }
        }
      }
    }
    return goodBlock;
  }

  /**
   * Check if there are any replica (other than source) on the same node group
   * with target. If true, then target is not a good candidate for placing 
   * specific block replica as we don't want 2 replicas under the same nodegroup 
   * after balance.
   * @param target targetDataNode
   * @param block dataBlock
   * @param source sourceDataNode
   * @return true if there are any replica (other than source) on the same node
   * group with target
   */
  private boolean isOnSameNodeGroupWithReplicas(BalancerDatanode target,
      BalancerBlock block, Source source) {
    for (BalancerDatanode loc : block.locations) {
      if (loc != source && 
        cluster.isOnSameNodeGroup(loc.getDatanode(), target.getDatanode())) {
          return true;
        }
      }
    return false;
  }

  /* reset all fields in a balancer preparing for the next iteration */
  private void resetData(Configuration conf) {
    this.cluster = NetworkTopology.getInstance(conf);
    this.overUtilizedDatanodes.clear();
    this.aboveAvgUtilizedDatanodes.clear();
    this.belowAvgUtilizedDatanodes.clear();
    this.underUtilizedDatanodes.clear();
    this.datanodes.clear();
    this.sources.clear();
    this.targets.clear();  
    this.policy.reset();
    cleanGlobalBlockList();
    this.movedBlocks.cleanup();
  }
  
  /* Remove all blocks from the global block list except for the ones in the
   * moved list.
   */
  private void cleanGlobalBlockList() {
    for (Iterator<Block> globalBlockListIterator=globalBlockList.keySet().iterator();
    globalBlockListIterator.hasNext();) {
      Block block = globalBlockListIterator.next();
      if(!movedBlocks.contains(block)) {
        globalBlockListIterator.remove();
      }
    }
  }
  
  /* Return true if the given datanode is overUtilized */
  private boolean isOverUtilized(BalancerDatanode datanode) {
    return datanode.utilization > (policy.getAvgUtilization()+threshold);
  }
  
  /* Return true if the given datanode is above or equal to average utilized
   * but not overUtilized */
  private boolean isAboveAvgUtilized(BalancerDatanode datanode) {
    final double avg = policy.getAvgUtilization();
    return (datanode.utilization <= (avg+threshold))
        && (datanode.utilization >= avg);
  }
  
  /* Return true if the given datanode is underUtilized */
  private boolean isUnderUtilized(BalancerDatanode datanode) {
    return datanode.utilization < (policy.getAvgUtilization()-threshold);
  }

  /* Return true if the given datanode is below average utilized 
   * but not underUtilized */
  private boolean isBelowOrEqualAvgUtilized(BalancerDatanode datanode) {
    final double avg = policy.getAvgUtilization();
    return (datanode.utilization >= (avg-threshold))
             && (datanode.utilization <= avg);
  }

  // Exit status
  enum ReturnStatus {
    // These int values will map directly to the balancer process's exit code.
    SUCCESS(0),
    IN_PROGRESS(1),
    ALREADY_RUNNING(-1),
    NO_MOVE_BLOCK(-2),
    NO_MOVE_PROGRESS(-3),
    IO_EXCEPTION(-4),
    ILLEGAL_ARGS(-5),
    INTERRUPTED(-6);

    final int code;

    ReturnStatus(int code) {
      this.code = code;
    }
  }

  /** Run an iteration for all datanodes. */
  private ReturnStatus run(int iteration, Formatter formatter,
      Configuration conf) {
    try {
      /* get all live datanodes of a cluster and their disk usage
       * decide the number of bytes need to be moved
       */
      final long bytesLeftToMove = initNodes(nnc.client.getDatanodeReport(DatanodeReportType.LIVE));
      if (bytesLeftToMove == 0) {
        System.out.println("The cluster is balanced. Exiting...");
        return ReturnStatus.SUCCESS;
      } else {
        LOG.info( "Need to move "+ StringUtils.byteDesc(bytesLeftToMove)
            + " to make the cluster balanced." );
      }
      
      /* Decide all the nodes that will participate in the block move and
       * the number of bytes that need to be moved from one node to another
       * in this iteration. Maximum bytes to be moved per node is
       * Min(1 Band worth of bytes,  MAX_SIZE_TO_MOVE).
       */
      final long bytesToMove = chooseNodes();
      if (bytesToMove == 0) {
        System.out.println("No block can be moved. Exiting...");
        return ReturnStatus.NO_MOVE_BLOCK;
      } else {
        LOG.info( "Will move " + StringUtils.byteDesc(bytesToMove) +
            " in this iteration");
      }

      formatter.format("%-24s %10d  %19s  %18s  %17s%n",
          DateFormat.getDateTimeInstance().format(new Date()),
          iteration,
          StringUtils.byteDesc(bytesMoved.get()),
          StringUtils.byteDesc(bytesLeftToMove),
          StringUtils.byteDesc(bytesToMove)
          );
      
      /* For each pair of <source, target>, start a thread that repeatedly 
       * decide a block to be moved and its proxy source, 
       * then initiates the move until all bytes are moved or no more block
       * available to move.
       * Exit no byte has been moved for 5 consecutive iterations.
       */
      if (!this.nnc.shouldContinue(dispatchBlockMoves())) {
        return ReturnStatus.NO_MOVE_PROGRESS;
      }

      return ReturnStatus.IN_PROGRESS;
    } catch (IllegalArgumentException e) {
      System.out.println(e + ".  Exiting ...");
      return ReturnStatus.ILLEGAL_ARGS;
    } catch (IOException e) {
      System.out.println(e + ".  Exiting ...");
      return ReturnStatus.IO_EXCEPTION;
    } catch (InterruptedException e) {
      System.out.println(e + ".  Exiting ...");
      return ReturnStatus.INTERRUPTED;
    } finally {
      // shutdown thread pools
      dispatcherExecutor.shutdownNow();
      moverExecutor.shutdownNow();
    }
  }

  /**
   * Balance all namenodes.
   * For each iteration,
   * for each namenode,
   * execute a {@link Balancer} to work through all datanodes once.  
   */
  static int run(Collection<URI> namenodes, final Parameters p,
      Configuration conf) throws IOException, InterruptedException {
    final long sleeptime = 2000*conf.getLong(
        DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_KEY,
        DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_DEFAULT);
    LOG.info("namenodes = " + namenodes);
    LOG.info("p         = " + p);
    
    final Formatter formatter = new Formatter(System.out);
    System.out.println("Time Stamp               Iteration#  Bytes Already Moved  Bytes Left To Move  Bytes Being Moved");
    
    final List<NameNodeConnector> connectors
        = new ArrayList<NameNodeConnector>(namenodes.size());
    try {
      for (URI uri : namenodes) {
        connectors.add(new NameNodeConnector(uri, conf));
      }
    
      boolean done = false;
      for(int iteration = 0; !done; iteration++) {
        done = true;
        Collections.shuffle(connectors);
        for(NameNodeConnector nnc : connectors) {
          final Balancer b = new Balancer(nnc, p, conf);
          final ReturnStatus r = b.run(iteration, formatter, conf);
          // clean all lists
          b.resetData(conf);
          if (r == ReturnStatus.IN_PROGRESS) {
            done = false;
          } else if (r != ReturnStatus.SUCCESS) {
            //must be an error statue, return.
            return r.code;
          }
        }

        if (!done) {
          Thread.sleep(sleeptime);
        }
      }
    } finally {
      for(NameNodeConnector nnc : connectors) {
        nnc.close();
      }
    }
    return ReturnStatus.SUCCESS.code;
  }

  /* Given elaspedTime in ms, return a printable string */
  private static String time2Str(long elapsedTime) {
    String unit;
    double time = elapsedTime;
    if (elapsedTime < 1000) {
      unit = "milliseconds";
    } else if (elapsedTime < 60*1000) {
      unit = "seconds";
      time = time/1000;
    } else if (elapsedTime < 3600*1000) {
      unit = "minutes";
      time = time/(60*1000);
    } else {
      unit = "hours";
      time = time/(3600*1000);
    }

    return time+" "+unit;
  }

  static class Parameters {
    static final Parameters DEFALUT = new Parameters(
        BalancingPolicy.Node.INSTANCE, 10.0);

    final BalancingPolicy policy;
    final double threshold;

    Parameters(BalancingPolicy policy, double threshold) {
      this.policy = policy;
      this.threshold = threshold;
    }

    @Override
    public String toString() {
      return Balancer.class.getSimpleName() + "." + getClass().getSimpleName()
          + "[" + policy + ", threshold=" + threshold + "]";
    }
  }

  static class Cli extends Configured implements Tool {
    /**
     * Parse arguments and then run Balancer.
     * 
     * @param args command specific arguments.
     * @return exit code. 0 indicates success, non-zero indicates failure.
     */
    @Override
    public int run(String[] args) {
      final long startTime = Time.now();
      final Configuration conf = getConf();
      WIN_WIDTH = conf.getLong(
          DFSConfigKeys.DFS_BALANCER_MOVEDWINWIDTH_KEY, 
          DFSConfigKeys.DFS_BALANCER_MOVEDWINWIDTH_DEFAULT);

      try {
        checkReplicationPolicyCompatibility(conf);

        final Collection<URI> namenodes = DFSUtil.getNsServiceRpcUris(conf);
        return Balancer.run(namenodes, parse(args), conf);
      } catch (IOException e) {
        System.out.println(e + ".  Exiting ...");
        return ReturnStatus.IO_EXCEPTION.code;
      } catch (InterruptedException e) {
        System.out.println(e + ".  Exiting ...");
        return ReturnStatus.INTERRUPTED.code;
      } finally {
        System.out.println("Balancing took " + time2Str(Time.now()-startTime));
      }
    }

    /** parse command line arguments */
    static Parameters parse(String[] args) {
      BalancingPolicy policy = Parameters.DEFALUT.policy;
      double threshold = Parameters.DEFALUT.threshold;

      if (args != null) {
        try {
          for(int i = 0; i < args.length; i++) {
            checkArgument(args.length >= 2, "args = " + Arrays.toString(args));
            if ("-threshold".equalsIgnoreCase(args[i])) {
              i++;
              try {
                threshold = Double.parseDouble(args[i]);
                if (threshold < 1 || threshold > 100) {
                  throw new IllegalArgumentException(
                      "Number out of range: threshold = " + threshold);
                }
                LOG.info( "Using a threshold of " + threshold );
              } catch(IllegalArgumentException e) {
                System.err.println(
                    "Expecting a number in the range of [1.0, 100.0]: "
                    + args[i]);
                throw e;
              }
            } else if ("-policy".equalsIgnoreCase(args[i])) {
              i++;
              try {
                policy = BalancingPolicy.parse(args[i]);
              } catch(IllegalArgumentException e) {
                System.err.println("Illegal policy name: " + args[i]);
                throw e;
              }
            } else {
              throw new IllegalArgumentException("args = "
                  + Arrays.toString(args));
            }
          }
        } catch(RuntimeException e) {
          printUsage(System.err);
          throw e;
        }
      }
      
      return new Parameters(policy, threshold);
    }

    private static void printUsage(PrintStream out) {
      out.println(USAGE + "\n");
    }
  }

  /**
   * Run a balancer
   * @param args Command line arguments
   */
  public static void main(String[] args) {
    if (DFSUtil.parseHelpArgument(args, USAGE, System.out, true)) {
      System.exit(0);
    }

    try {
      System.exit(ToolRunner.run(new HdfsConfiguration(), new Cli(), args));
    } catch (Throwable e) {
      LOG.error("Exiting balancer due an exception", e);
      System.exit(-1);
    }
  }
}
