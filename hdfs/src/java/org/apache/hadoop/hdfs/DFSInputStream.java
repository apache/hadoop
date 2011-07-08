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
package org.apache.hadoop.hdfs;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.fs.ChecksumException;
import org.apache.hadoop.fs.FSInputStream;
import org.apache.hadoop.fs.UnresolvedLinkException;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.ClientDatanodeProtocol;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.security.token.block.InvalidBlockTokenException;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenIdentifier;
import org.apache.hadoop.hdfs.server.datanode.ReplicaNotFoundException;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.StringUtils;

/****************************************************************
 * DFSInputStream provides bytes from a named file.  It handles 
 * negotiation of the namenode and various datanodes as necessary.
 ****************************************************************/
@InterfaceAudience.Private
public class DFSInputStream extends FSInputStream {
  private final SocketCache socketCache;

  private final DFSClient dfsClient;
  private boolean closed = false;

  private final String src;
  private long prefetchSize;
  private BlockReader blockReader = null;
  private boolean verifyChecksum;
  private LocatedBlocks locatedBlocks = null;
  private long lastBlockBeingWrittenLength = 0;
  private DatanodeInfo currentNode = null;
  private LocatedBlock currentLocatedBlock = null;
  private long pos = 0;
  private long blockEnd = -1;

  /**
   * This variable tracks the number of failures since the start of the
   * most recent user-facing operation. That is to say, it should be reset
   * whenever the user makes a call on this stream, and if at any point
   * during the retry logic, the failure count exceeds a threshold,
   * the errors will be thrown back to the operation.
   *
   * Specifically this counts the number of times the client has gone
   * back to the namenode to get a new list of block locations, and is
   * capped at maxBlockAcquireFailures
   */
  private int failures = 0;
  private int timeWindow; 

  /* XXX Use of CocurrentHashMap is temp fix. Need to fix 
   * parallel accesses to DFSInputStream (through ptreads) properly */
  private ConcurrentHashMap<DatanodeInfo, DatanodeInfo> deadNodes = 
             new ConcurrentHashMap<DatanodeInfo, DatanodeInfo>();
  private int buffersize = 1;
  
  private byte[] oneByteBuf = new byte[1]; // used for 'int read()'

  private int nCachedConnRetry;

  void addToDeadNodes(DatanodeInfo dnInfo) {
    deadNodes.put(dnInfo, dnInfo);
  }
  
  DFSInputStream(DFSClient dfsClient, String src, int buffersize, boolean verifyChecksum
                 ) throws IOException, UnresolvedLinkException {
    this.dfsClient = dfsClient;
    this.verifyChecksum = verifyChecksum;
    this.buffersize = buffersize;
    this.src = src;
    this.socketCache = dfsClient.socketCache;
    prefetchSize = dfsClient.getConf().prefetchSize;
    timeWindow = dfsClient.getConf().timeWindow;
    nCachedConnRetry = dfsClient.getConf().nCachedConnRetry;
    openInfo();
  }

  /**
   * Grab the open-file info from namenode
   */
  synchronized void openInfo() throws IOException, UnresolvedLinkException {
    LocatedBlocks newInfo = DFSClient.callGetBlockLocations(dfsClient.namenode, src, 0, prefetchSize);
    if (DFSClient.LOG.isDebugEnabled()) {
      DFSClient.LOG.debug("newInfo = " + newInfo);
    }
    if (newInfo == null) {
      throw new IOException("Cannot open filename " + src);
    }

    if (locatedBlocks != null) {
      Iterator<LocatedBlock> oldIter = locatedBlocks.getLocatedBlocks().iterator();
      Iterator<LocatedBlock> newIter = newInfo.getLocatedBlocks().iterator();
      while (oldIter.hasNext() && newIter.hasNext()) {
        if (! oldIter.next().getBlock().equals(newIter.next().getBlock())) {
          throw new IOException("Blocklist for " + src + " has changed!");
        }
      }
    }
    locatedBlocks = newInfo;
    lastBlockBeingWrittenLength = 0;
    if (!locatedBlocks.isLastBlockComplete()) {
      final LocatedBlock last = locatedBlocks.getLastLocatedBlock();
      if (last != null) {
        final long len = readBlockLength(last);
        last.getBlock().setNumBytes(len);
        lastBlockBeingWrittenLength = len; 
      }
    }

    currentNode = null;
  }

  /** Read the block length from one of the datanodes. */
  private long readBlockLength(LocatedBlock locatedblock) throws IOException {
    if (locatedblock == null || locatedblock.getLocations().length == 0) {
      return 0;
    }
    int replicaNotFoundCount = locatedblock.getLocations().length;
    
    for(DatanodeInfo datanode : locatedblock.getLocations()) {
      ClientDatanodeProtocol cdp = null;
      
      try {
        cdp = DFSClient.createClientDatanodeProtocolProxy(
        datanode, dfsClient.conf, dfsClient.getConf().socketTimeout, locatedblock);
        
        final long n = cdp.getReplicaVisibleLength(locatedblock.getBlock());
        
        if (n >= 0) {
          return n;
        }
      }
      catch(IOException ioe) {
        if (ioe instanceof RemoteException &&
          (((RemoteException) ioe).unwrapRemoteException() instanceof
            ReplicaNotFoundException)) {
          // special case : replica might not be on the DN, treat as 0 length
          replicaNotFoundCount--;
        }
        
        if (DFSClient.LOG.isDebugEnabled()) {
          DFSClient.LOG.debug("Failed to getReplicaVisibleLength from datanode "
              + datanode + " for block " + locatedblock.getBlock(), ioe);
        }
      } finally {
        if (cdp != null) {
          RPC.stopProxy(cdp);
        }
      }
    }

    // Namenode told us about these locations, but none know about the replica
    // means that we hit the race between pipeline creation start and end.
    // we require all 3 because some other exception could have happened
    // on a DN that has it.  we want to report that error
    if (replicaNotFoundCount == 0) {
      return 0;
    }

    throw new IOException("Cannot obtain block length for " + locatedblock);
  }
  
  public synchronized long getFileLength() {
    return locatedBlocks == null? 0:
        locatedBlocks.getFileLength() + lastBlockBeingWrittenLength;
  }

  /**
   * Returns the datanode from which the stream is currently reading.
   */
  public DatanodeInfo getCurrentDatanode() {
    return currentNode;
  }

  /**
   * Returns the block containing the target position. 
   */
  synchronized public ExtendedBlock getCurrentBlock() {
    if (currentLocatedBlock == null){
      return null;
    }
    return currentLocatedBlock.getBlock();
  }

  /**
   * Return collection of blocks that has already been located.
   */
  synchronized List<LocatedBlock> getAllBlocks() throws IOException {
    return getBlockRange(0, getFileLength());
  }

  /**
   * Get block at the specified position.
   * Fetch it from the namenode if not cached.
   * 
   * @param offset
   * @param updatePosition whether to update current position
   * @return located block
   * @throws IOException
   */
  private synchronized LocatedBlock getBlockAt(long offset,
      boolean updatePosition) throws IOException {
    assert (locatedBlocks != null) : "locatedBlocks is null";

    final LocatedBlock blk;

    //check offset
    if (offset < 0 || offset >= getFileLength()) {
      throw new IOException("offset < 0 || offset > getFileLength(), offset="
          + offset
          + ", updatePosition=" + updatePosition
          + ", locatedBlocks=" + locatedBlocks);
    }
    else if (offset >= locatedBlocks.getFileLength()) {
      // offset to the portion of the last block,
      // which is not known to the name-node yet;
      // getting the last block 
      blk = locatedBlocks.getLastLocatedBlock();
    }
    else {
      // search cached blocks first
      int targetBlockIdx = locatedBlocks.findBlock(offset);
      if (targetBlockIdx < 0) { // block is not cached
        targetBlockIdx = LocatedBlocks.getInsertIndex(targetBlockIdx);
        // fetch more blocks
        LocatedBlocks newBlocks;
        newBlocks = DFSClient.callGetBlockLocations(dfsClient.namenode, src, offset, prefetchSize);
        assert (newBlocks != null) : "Could not find target position " + offset;
        locatedBlocks.insertRange(targetBlockIdx, newBlocks.getLocatedBlocks());
      }
      blk = locatedBlocks.get(targetBlockIdx);
    }

    // update current position
    if (updatePosition) {
      pos = offset;
      blockEnd = blk.getStartOffset() + blk.getBlockSize() - 1;
      currentLocatedBlock = blk;
    }
    return blk;
  }

  /** Fetch a block from namenode and cache it */
  private synchronized void fetchBlockAt(long offset) throws IOException {
    int targetBlockIdx = locatedBlocks.findBlock(offset);
    if (targetBlockIdx < 0) { // block is not cached
      targetBlockIdx = LocatedBlocks.getInsertIndex(targetBlockIdx);
    }
    // fetch blocks
    LocatedBlocks newBlocks;
    newBlocks = DFSClient.callGetBlockLocations(dfsClient.namenode, src, offset, prefetchSize);
    if (newBlocks == null) {
      throw new IOException("Could not find target position " + offset);
    }
    locatedBlocks.insertRange(targetBlockIdx, newBlocks.getLocatedBlocks());
  }

  /**
   * Get blocks in the specified range.
   * Fetch them from the namenode if not cached. This function
   * will not get a read request beyond the EOF.
   * @param offset
   * @param length
   * @return consequent segment of located blocks
   * @throws IOException
   */
  private synchronized List<LocatedBlock> getBlockRange(long offset, 
                                                        long length) 
                                                      throws IOException {
    // getFileLength(): returns total file length
    // locatedBlocks.getFileLength(): returns length of completed blocks
    if (offset >= getFileLength()) {
      throw new IOException("Offset: " + offset +
        " exceeds file length: " + getFileLength());
    }

    final List<LocatedBlock> blocks;
    final long lengthOfCompleteBlk = locatedBlocks.getFileLength();
    final boolean readOffsetWithinCompleteBlk = offset < lengthOfCompleteBlk;
    final boolean readLengthPastCompleteBlk = offset + length > lengthOfCompleteBlk;

    if (readOffsetWithinCompleteBlk) {
      //get the blocks of finalized (completed) block range
      blocks = getFinalizedBlockRange(offset, 
        Math.min(length, lengthOfCompleteBlk - offset));
    } else {
      blocks = new ArrayList<LocatedBlock>(1);
    }

    // get the blocks from incomplete block range
    if (readLengthPastCompleteBlk) {
       blocks.add(locatedBlocks.getLastLocatedBlock());
    }

    return blocks;
  }

  /**
   * Get blocks in the specified range.
   * Includes only the complete blocks.
   * Fetch them from the namenode if not cached.
   */
  private synchronized List<LocatedBlock> getFinalizedBlockRange(
      long offset, long length) throws IOException {
    assert (locatedBlocks != null) : "locatedBlocks is null";
    List<LocatedBlock> blockRange = new ArrayList<LocatedBlock>();
    // search cached blocks first
    int blockIdx = locatedBlocks.findBlock(offset);
    if (blockIdx < 0) { // block is not cached
      blockIdx = LocatedBlocks.getInsertIndex(blockIdx);
    }
    long remaining = length;
    long curOff = offset;
    while(remaining > 0) {
      LocatedBlock blk = null;
      if(blockIdx < locatedBlocks.locatedBlockCount())
        blk = locatedBlocks.get(blockIdx);
      if (blk == null || curOff < blk.getStartOffset()) {
        LocatedBlocks newBlocks;
        newBlocks = DFSClient.callGetBlockLocations(dfsClient.namenode, src, curOff, remaining);
        locatedBlocks.insertRange(blockIdx, newBlocks.getLocatedBlocks());
        continue;
      }
      assert curOff >= blk.getStartOffset() : "Block not found";
      blockRange.add(blk);
      long bytesRead = blk.getStartOffset() + blk.getBlockSize() - curOff;
      remaining -= bytesRead;
      curOff += bytesRead;
      blockIdx++;
    }
    return blockRange;
  }

  /**
   * Open a DataInputStream to a DataNode so that it can be read from.
   * We get block ID and the IDs of the destinations at startup, from the namenode.
   */
  private synchronized DatanodeInfo blockSeekTo(long target) throws IOException {
    if (target >= getFileLength()) {
      throw new IOException("Attempted to read past end of file");
    }

    // Will be getting a new BlockReader.
    if (blockReader != null) {
      closeBlockReader(blockReader);
      blockReader = null;
    }

    //
    // Connect to best DataNode for desired Block, with potential offset
    //
    DatanodeInfo chosenNode = null;
    int refetchToken = 1; // only need to get a new access token once
    
    while (true) {
      //
      // Compute desired block
      //
      LocatedBlock targetBlock = getBlockAt(target, true);
      assert (target==pos) : "Wrong postion " + pos + " expect " + target;
      long offsetIntoBlock = target - targetBlock.getStartOffset();

      DNAddrPair retval = chooseDataNode(targetBlock);
      chosenNode = retval.info;
      InetSocketAddress targetAddr = retval.addr;

      try {
        ExtendedBlock blk = targetBlock.getBlock();
        Token<BlockTokenIdentifier> accessToken = targetBlock.getBlockToken();
        
        blockReader = getBlockReader(
            targetAddr, src, blk,
            accessToken,
            offsetIntoBlock, blk.getNumBytes() - offsetIntoBlock,
            buffersize, verifyChecksum, dfsClient.clientName);
        return chosenNode;
      } catch (IOException ex) {
        if (ex instanceof InvalidBlockTokenException && refetchToken > 0) {
          DFSClient.LOG.info("Will fetch a new access token and retry, " 
              + "access token was invalid when connecting to " + targetAddr
              + " : " + ex);
          /*
           * Get a new access token and retry. Retry is needed in 2 cases. 1)
           * When both NN and DN re-started while DFSClient holding a cached
           * access token. 2) In the case that NN fails to update its
           * access key at pre-set interval (by a wide margin) and
           * subsequently restarts. In this case, DN re-registers itself with
           * NN and receives a new access key, but DN will delete the old
           * access key from its memory since it's considered expired based on
           * the estimated expiration date.
           */
          refetchToken--;
          fetchBlockAt(target);
        } else {
          DFSClient.LOG.warn("Failed to connect to " + targetAddr
              + ", add to deadNodes and continue " + ex);
          if (DFSClient.LOG.isDebugEnabled()) {
            DFSClient.LOG.debug("Connection failure ", ex);
          }
          // Put chosen node into dead list, continue
          addToDeadNodes(chosenNode);
        }
      }
    }
  }

  /**
   * Close it down!
   */
  @Override
  public synchronized void close() throws IOException {
    if (closed) {
      return;
    }
    dfsClient.checkOpen();

    if (blockReader != null) {
      closeBlockReader(blockReader);
      blockReader = null;
    }
    super.close();
    closed = true;
  }

  @Override
  public synchronized int read() throws IOException {
    int ret = read( oneByteBuf, 0, 1 );
    return ( ret <= 0 ) ? -1 : (oneByteBuf[0] & 0xff);
  }

  /* This is a used by regular read() and handles ChecksumExceptions.
   * name readBuffer() is chosen to imply similarity to readBuffer() in
   * ChecksumFileSystem
   */ 
  private synchronized int readBuffer(byte buf[], int off, int len,
      Map<ExtendedBlock, Set<DatanodeInfo>> corruptedBlockMap)
      throws IOException {
    IOException ioe;
    
    /* we retry current node only once. So this is set to true only here.
     * Intention is to handle one common case of an error that is not a
     * failure on datanode or client : when DataNode closes the connection
     * since client is idle. If there are other cases of "non-errors" then
     * then a datanode might be retried by setting this to true again.
     */
    boolean retryCurrentNode = true;

    while (true) {
      // retry as many times as seekToNewSource allows.
      try {
        return blockReader.read(buf, off, len);
      } catch ( ChecksumException ce ) {
        DFSClient.LOG.warn("Found Checksum error for "
            + getCurrentBlock() + " from " + currentNode.getName()
            + " at " + ce.getPos());        
        ioe = ce;
        retryCurrentNode = false;
        // we want to remember which block replicas we have tried
        addIntoCorruptedBlockMap(getCurrentBlock(), currentNode,
            corruptedBlockMap);
      } catch ( IOException e ) {
        if (!retryCurrentNode) {
          DFSClient.LOG.warn("Exception while reading from "
              + getCurrentBlock() + " of " + src + " from "
              + currentNode + ": " + StringUtils.stringifyException(e));
        }
        ioe = e;
      }
      boolean sourceFound = false;
      if (retryCurrentNode) {
        /* possibly retry the same node so that transient errors don't
         * result in application level failures (e.g. Datanode could have
         * closed the connection because the client is idle for too long).
         */ 
        sourceFound = seekToBlockSource(pos);
      } else {
        addToDeadNodes(currentNode);
        sourceFound = seekToNewSource(pos);
      }
      if (!sourceFound) {
        throw ioe;
      }
      retryCurrentNode = false;
    }
  }

  /**
   * Read the entire buffer.
   */
  @Override
  public synchronized int read(byte buf[], int off, int len) throws IOException {
    dfsClient.checkOpen();
    if (closed) {
      throw new IOException("Stream closed");
    }
    Map<ExtendedBlock,Set<DatanodeInfo>> corruptedBlockMap 
      = new HashMap<ExtendedBlock, Set<DatanodeInfo>>();
    failures = 0;
    if (pos < getFileLength()) {
      int retries = 2;
      while (retries > 0) {
        try {
          if (pos > blockEnd) {
            currentNode = blockSeekTo(pos);
          }
          int realLen = (int) Math.min((long) len, (blockEnd - pos + 1L));
          int result = readBuffer(buf, off, realLen, corruptedBlockMap);
          
          if (result >= 0) {
            pos += result;
          } else {
            // got a EOS from reader though we expect more data on it.
            throw new IOException("Unexpected EOS from the reader");
          }
          if (dfsClient.stats != null && result != -1) {
            dfsClient.stats.incrementBytesRead(result);
          }
          return result;
        } catch (ChecksumException ce) {
          throw ce;            
        } catch (IOException e) {
          if (retries == 1) {
            DFSClient.LOG.warn("DFS Read: " + StringUtils.stringifyException(e));
          }
          blockEnd = -1;
          if (currentNode != null) { addToDeadNodes(currentNode); }
          if (--retries == 0) {
            throw e;
          }
        } finally {
          // Check if need to report block replicas corruption either read
          // was successful or ChecksumException occured.
          reportCheckSumFailure(corruptedBlockMap, 
              currentLocatedBlock.getLocations().length);
        }
      }
    }
    return -1;
  }

  /**
   * Add corrupted block replica into map.
   * @param corruptedBlockMap 
   */
  private void addIntoCorruptedBlockMap(ExtendedBlock blk, DatanodeInfo node, 
      Map<ExtendedBlock, Set<DatanodeInfo>> corruptedBlockMap) {
    Set<DatanodeInfo> dnSet = null;
    if((corruptedBlockMap.containsKey(blk))) {
      dnSet = corruptedBlockMap.get(blk);
    }else {
      dnSet = new HashSet<DatanodeInfo>();
    }
    if (!dnSet.contains(node)) {
      dnSet.add(node);
      corruptedBlockMap.put(blk, dnSet);
    }
  }
      
  private DNAddrPair chooseDataNode(LocatedBlock block)
    throws IOException {
    while (true) {
      DatanodeInfo[] nodes = block.getLocations();
      try {
        DatanodeInfo chosenNode = bestNode(nodes, deadNodes);
        InetSocketAddress targetAddr = 
                          NetUtils.createSocketAddr(chosenNode.getName());
        return new DNAddrPair(chosenNode, targetAddr);
      } catch (IOException ie) {
        String blockInfo = block.getBlock() + " file=" + src;
        if (failures >= dfsClient.getMaxBlockAcquireFailures()) {
          throw new BlockMissingException(src, "Could not obtain block: " + blockInfo,
                                          block.getStartOffset());
        }
        
        if (nodes == null || nodes.length == 0) {
          DFSClient.LOG.info("No node available for block: " + blockInfo);
        }
        DFSClient.LOG.info("Could not obtain block " + block.getBlock()
            + " from any node: " + ie
            + ". Will get new block locations from namenode and retry...");
        try {
          // Introducing a random factor to the wait time before another retry.
          // The wait time is dependent on # of failures and a random factor.
          // At the first time of getting a BlockMissingException, the wait time
          // is a random number between 0..3000 ms. If the first retry
          // still fails, we will wait 3000 ms grace period before the 2nd retry.
          // Also at the second retry, the waiting window is expanded to 6000 ms
          // alleviating the request rate from the server. Similarly the 3rd retry
          // will wait 6000ms grace period before retry and the waiting window is
          // expanded to 9000ms. 
          double waitTime = timeWindow * failures +       // grace period for the last round of attempt
            timeWindow * (failures + 1) * dfsClient.r.nextDouble(); // expanding time window for each failure
          DFSClient.LOG.warn("DFS chooseDataNode: got # " + (failures + 1) + " IOException, will wait for " + waitTime + " msec.");
          Thread.sleep((long)waitTime);
        } catch (InterruptedException iex) {
        }
        deadNodes.clear(); //2nd option is to remove only nodes[blockId]
        openInfo();
        block = getBlockAt(block.getStartOffset(), false);
        failures++;
        continue;
      }
    }
  } 
      
  private void fetchBlockByteRange(LocatedBlock block, long start, long end,
      byte[] buf, int offset,
      Map<ExtendedBlock, Set<DatanodeInfo>> corruptedBlockMap)
      throws IOException {
    //
    // Connect to best DataNode for desired Block, with potential offset
    //
    int refetchToken = 1; // only need to get a new access token once
    
    while (true) {
      // cached block locations may have been updated by chooseDataNode()
      // or fetchBlockAt(). Always get the latest list of locations at the 
      // start of the loop.
      block = getBlockAt(block.getStartOffset(), false);
      DNAddrPair retval = chooseDataNode(block);
      DatanodeInfo chosenNode = retval.info;
      InetSocketAddress targetAddr = retval.addr;
      BlockReader reader = null;
          
      try {
        Token<BlockTokenIdentifier> blockToken = block.getBlockToken();
            
        int len = (int) (end - start + 1);

        reader = getBlockReader(targetAddr, src,
                                block.getBlock(),
                                blockToken,
                                start, len, buffersize,
                                verifyChecksum, dfsClient.clientName);
        int nread = reader.readAll(buf, offset, len);
        if (nread != len) {
          throw new IOException("truncated return from reader.read(): " +
                                "excpected " + len + ", got " + nread);
        }
        return;
      } catch (ChecksumException e) {
        DFSClient.LOG.warn("fetchBlockByteRange(). Got a checksum exception for " +
                 src + " at " + block.getBlock() + ":" + 
                 e.getPos() + " from " + chosenNode.getName());
        // we want to remember what we have tried
        addIntoCorruptedBlockMap(block.getBlock(), chosenNode, corruptedBlockMap);
      } catch (IOException e) {
        if (e instanceof InvalidBlockTokenException && refetchToken > 0) {
          DFSClient.LOG.info("Will get a new access token and retry, "
              + "access token was invalid when connecting to " + targetAddr
              + " : " + e);
          refetchToken--;
          fetchBlockAt(block.getStartOffset());
          continue;
        } else {
          DFSClient.LOG.warn("Failed to connect to " + targetAddr + 
              " for file " + src + " for block " + block.getBlock() + ":" + e);
          if (DFSClient.LOG.isDebugEnabled()) {
            DFSClient.LOG.debug("Connection failure ", e);
          }
        }
      } finally {
        if (reader != null) {
          closeBlockReader(reader);
        }
      }
      // Put chosen node into dead list, continue
      addToDeadNodes(chosenNode);
    }
  }

  /**
   * Close the given BlockReader and cache its socket.
   */
  private void closeBlockReader(BlockReader reader) throws IOException {
    if (reader.hasSentStatusCode()) {
      Socket oldSock = reader.takeSocket();
      socketCache.put(oldSock);
    }
    reader.close();
  }

  /**
   * Retrieve a BlockReader suitable for reading.
   * This method will reuse the cached connection to the DN if appropriate.
   * Otherwise, it will create a new connection.
   *
   * @param dnAddr  Address of the datanode
   * @param file  File location
   * @param block  The Block object
   * @param blockToken  The access token for security
   * @param startOffset  The read offset, relative to block head
   * @param len  The number of bytes to read
   * @param bufferSize  The IO buffer size (not the client buffer size)
   * @param verifyChecksum  Whether to verify checksum
   * @param clientName  Client name
   * @return New BlockReader instance
   */
  protected BlockReader getBlockReader(InetSocketAddress dnAddr,
                                       String file,
                                       ExtendedBlock block,
                                       Token<BlockTokenIdentifier> blockToken,
                                       long startOffset,
                                       long len,
                                       int bufferSize,
                                       boolean verifyChecksum,
                                       String clientName)
      throws IOException {
    IOException err = null;
    boolean fromCache = true;

    // Allow retry since there is no way of knowing whether the cached socket
    // is good until we actually use it.
    for (int retries = 0; retries <= nCachedConnRetry && fromCache; ++retries) {
      Socket sock = socketCache.get(dnAddr);
      if (sock == null) {
        fromCache = false;

        sock = dfsClient.socketFactory.createSocket();
        
        // TCP_NODELAY is crucial here because of bad interactions between
        // Nagle's Algorithm and Delayed ACKs. With connection keepalive
        // between the client and DN, the conversation looks like:
        //   1. Client -> DN: Read block X
        //   2. DN -> Client: data for block X
        //   3. Client -> DN: Status OK (successful read)
        //   4. Client -> DN: Read block Y
        // The fact that step #3 and #4 are both in the client->DN direction
        // triggers Nagling. If the DN is using delayed ACKs, this results
        // in a delay of 40ms or more.
        //
        // TCP_NODELAY disables nagling and thus avoids this performance
        // disaster.
        sock.setTcpNoDelay(true);

        NetUtils.connect(sock, dnAddr, dfsClient.getConf().socketTimeout);
        sock.setSoTimeout(dfsClient.getConf().socketTimeout);
      }

      try {
        // The OP_READ_BLOCK request is sent as we make the BlockReader
        BlockReader reader =
            BlockReader.newBlockReader(sock, file, block,
                                       blockToken,
                                       startOffset, len,
                                       bufferSize, verifyChecksum,
                                       clientName);
        return reader;
      } catch (IOException ex) {
        // Our socket is no good.
        DFSClient.LOG.debug("Error making BlockReader. Closing stale " + sock, ex);
        sock.close();
        err = ex;
      }
    }

    throw err;
  }


  /**
   * Read bytes starting from the specified position.
   * 
   * @param position start read from this position
   * @param buffer read buffer
   * @param offset offset into buffer
   * @param length number of bytes to read
   * 
   * @return actual number of bytes read
   */
  @Override
  public int read(long position, byte[] buffer, int offset, int length)
    throws IOException {
    // sanity checks
    dfsClient.checkOpen();
    if (closed) {
      throw new IOException("Stream closed");
    }
    failures = 0;
    long filelen = getFileLength();
    if ((position < 0) || (position >= filelen)) {
      return -1;
    }
    int realLen = length;
    if ((position + length) > filelen) {
      realLen = (int)(filelen - position);
    }
    
    // determine the block and byte range within the block
    // corresponding to position and realLen
    List<LocatedBlock> blockRange = getBlockRange(position, realLen);
    int remaining = realLen;
    Map<ExtendedBlock,Set<DatanodeInfo>> corruptedBlockMap 
      = new HashMap<ExtendedBlock, Set<DatanodeInfo>>();
    for (LocatedBlock blk : blockRange) {
      long targetStart = position - blk.getStartOffset();
      long bytesToRead = Math.min(remaining, blk.getBlockSize() - targetStart);
      try {
        fetchBlockByteRange(blk, targetStart, 
            targetStart + bytesToRead - 1, buffer, offset, corruptedBlockMap);
      } finally {
        // Check and report if any block replicas are corrupted.
        // BlockMissingException may be caught if all block replicas are
        // corrupted.
        reportCheckSumFailure(corruptedBlockMap, blk.getLocations().length);
      }

      remaining -= bytesToRead;
      position += bytesToRead;
      offset += bytesToRead;
    }
    assert remaining == 0 : "Wrong number of bytes read.";
    if (dfsClient.stats != null) {
      dfsClient.stats.incrementBytesRead(realLen);
    }
    return realLen;
  }
  
  /**
   * DFSInputStream reports checksum failure.
   * Case I : client has tried multiple data nodes and at least one of the
   * attempts has succeeded. We report the other failures as corrupted block to
   * namenode. 
   * Case II: client has tried out all data nodes, but all failed. We
   * only report if the total number of replica is 1. We do not
   * report otherwise since this maybe due to the client is a handicapped client
   * (who can not read).
   * @param corruptedBlockMap, map of corrupted blocks
   * @param dataNodeCount, number of data nodes who contains the block replicas
   */
  private void reportCheckSumFailure(
      Map<ExtendedBlock, Set<DatanodeInfo>> corruptedBlockMap, 
      int dataNodeCount) {
    if (corruptedBlockMap.isEmpty()) {
      return;
    }
    Iterator<Entry<ExtendedBlock, Set<DatanodeInfo>>> it = corruptedBlockMap
        .entrySet().iterator();
    Entry<ExtendedBlock, Set<DatanodeInfo>> entry = it.next();
    ExtendedBlock blk = entry.getKey();
    Set<DatanodeInfo> dnSet = entry.getValue();
    if (((dnSet.size() < dataNodeCount) && (dnSet.size() > 0))
        || ((dataNodeCount == 1) && (dnSet.size() == dataNodeCount))) {
      DatanodeInfo[] locs = new DatanodeInfo[dnSet.size()];
      int i = 0;
      for (DatanodeInfo dn:dnSet) {
        locs[i++] = dn;
      }
      LocatedBlock [] lblocks = { new LocatedBlock(blk, locs) };
      dfsClient.reportChecksumFailure(src, lblocks);
    }
    corruptedBlockMap.clear();
  }

  @Override
  public long skip(long n) throws IOException {
    if ( n > 0 ) {
      long curPos = getPos();
      long fileLen = getFileLength();
      if( n+curPos > fileLen ) {
        n = fileLen - curPos;
      }
      seek(curPos+n);
      return n;
    }
    return n < 0 ? -1 : 0;
  }

  /**
   * Seek to a new arbitrary location
   */
  @Override
  public synchronized void seek(long targetPos) throws IOException {
    if (targetPos > getFileLength()) {
      throw new IOException("Cannot seek after EOF");
    }
    if (closed) {
      throw new IOException("Stream is closed!");
    }
    boolean done = false;
    if (pos <= targetPos && targetPos <= blockEnd) {
      //
      // If this seek is to a positive position in the current
      // block, and this piece of data might already be lying in
      // the TCP buffer, then just eat up the intervening data.
      //
      int diff = (int)(targetPos - pos);
      if (diff <= DFSClient.TCP_WINDOW_SIZE) {
        try {
          pos += blockReader.skip(diff);
          if (pos == targetPos) {
            done = true;
          }
        } catch (IOException e) {//make following read to retry
          if(DFSClient.LOG.isDebugEnabled()) {
            DFSClient.LOG.debug("Exception while seek to " + targetPos
                + " from " + getCurrentBlock() + " of " + src
                + " from " + currentNode + ": "
                + StringUtils.stringifyException(e));
          }
        }
      }
    }
    if (!done) {
      pos = targetPos;
      blockEnd = -1;
    }
  }

  /**
   * Same as {@link #seekToNewSource(long)} except that it does not exclude
   * the current datanode and might connect to the same node.
   */
  private synchronized boolean seekToBlockSource(long targetPos)
                                                 throws IOException {
    currentNode = blockSeekTo(targetPos);
    return true;
  }
  
  /**
   * Seek to given position on a node other than the current node.  If
   * a node other than the current node is found, then returns true. 
   * If another node could not be found, then returns false.
   */
  @Override
  public synchronized boolean seekToNewSource(long targetPos) throws IOException {
    boolean markedDead = deadNodes.containsKey(currentNode);
    addToDeadNodes(currentNode);
    DatanodeInfo oldNode = currentNode;
    DatanodeInfo newNode = blockSeekTo(targetPos);
    if (!markedDead) {
      /* remove it from deadNodes. blockSeekTo could have cleared 
       * deadNodes and added currentNode again. Thats ok. */
      deadNodes.remove(oldNode);
    }
    if (!oldNode.getStorageID().equals(newNode.getStorageID())) {
      currentNode = newNode;
      return true;
    } else {
      return false;
    }
  }
      
  /**
   */
  @Override
  public synchronized long getPos() throws IOException {
    return pos;
  }

  /** Return the size of the remaining available bytes
   * if the size is less than or equal to {@link Integer#MAX_VALUE},
   * otherwise, return {@link Integer#MAX_VALUE}.
   */
  @Override
  public synchronized int available() throws IOException {
    if (closed) {
      throw new IOException("Stream closed");
    }

    final long remaining = getFileLength() - pos;
    return remaining <= Integer.MAX_VALUE? (int)remaining: Integer.MAX_VALUE;
  }

  /**
   * We definitely don't support marks
   */
  @Override
  public boolean markSupported() {
    return false;
  }
  @Override
  public void mark(int readLimit) {
  }
  @Override
  public void reset() throws IOException {
    throw new IOException("Mark/reset not supported");
  }

  /**
   * Pick the best node from which to stream the data.
   * Entries in <i>nodes</i> are already in the priority order
   */
  static DatanodeInfo bestNode(DatanodeInfo nodes[], 
                               AbstractMap<DatanodeInfo, DatanodeInfo> deadNodes)
                               throws IOException {
    if (nodes != null) { 
      for (int i = 0; i < nodes.length; i++) {
        if (!deadNodes.containsKey(nodes[i])) {
          return nodes[i];
        }
      }
    }
    throw new IOException("No live nodes contain current block");
  }

  /** Utility class to encapsulate data node info and its ip address. */
  static class DNAddrPair {
    DatanodeInfo info;
    InetSocketAddress addr;
    DNAddrPair(DatanodeInfo info, InetSocketAddress addr) {
      this.info = info;
      this.addr = addr;
    }
  }

}
