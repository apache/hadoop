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

import org.apache.hadoop.io.*;
import org.apache.hadoop.io.retry.RetryPolicies;
import org.apache.hadoop.io.retry.RetryPolicy;
import org.apache.hadoop.io.retry.RetryProxy;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.ipc.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.util.*;

import org.apache.commons.logging.*;

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.TimeUnit;

/********************************************************
 * DFSClient can connect to a Hadoop Filesystem and 
 * perform basic file tasks.  It uses the ClientProtocol
 * to communicate with a NameNode daemon, and connects 
 * directly to DataNodes to read/write block data.
 *
 * Hadoop DFS users should obtain an instance of 
 * DistributedFileSystem, which uses DFSClient to handle
 * filesystem tasks.
 *
 * @author Mike Cafarella, Tessa MacDuff
 ********************************************************/
class DFSClient implements FSConstants {
  public static final Log LOG = LogFactory.getLog("org.apache.hadoop.fs.DFSClient");
  static final int MAX_BLOCK_ACQUIRE_FAILURES = 3;
  private static final int TCP_WINDOW_SIZE = 128 * 1024; // 128 KB
  private static final long DEFAULT_BLOCK_SIZE = 64 * 1024 * 1024;
  ClientProtocol namenode;
  boolean running = true;
  Random r = new Random();
  String clientName;
  Daemon leaseChecker;
  private Configuration conf;
  private long defaultBlockSize;
  private short defaultReplication;
  private LocalDirAllocator dirAllocator;
    
  /**
   * A map from name -> DFSOutputStream of files that are currently being
   * written by this client.
   */
  private TreeMap<String, OutputStream> pendingCreates =
    new TreeMap<String, OutputStream>();
    
  /**
   * A class to track the list of DFS clients, so that they can be closed
   * on exit.
   * @author Owen O'Malley
   */
  private static class ClientFinalizer extends Thread {
    private List<DFSClient> clients = new ArrayList<DFSClient>();

    public synchronized void addClient(DFSClient client) {
      clients.add(client);
    }

    public synchronized void run() {
      for (DFSClient client : clients) {
        if (client.running) {
          try {
            client.close();
          } catch (IOException ie) {
            System.err.println("Error closing client");
            ie.printStackTrace();
          }
        }
      }
    }
  }

  // add a cleanup thread
  private static ClientFinalizer clientFinalizer = new ClientFinalizer();
  static {
    Runtime.getRuntime().addShutdownHook(clientFinalizer);
  }

  private static ClientProtocol createNamenode(
      InetSocketAddress nameNodeAddr, Configuration conf)
    throws IOException {
    RetryPolicy timeoutPolicy = RetryPolicies.exponentialBackoffRetry(
        5, 200, TimeUnit.MILLISECONDS);
    RetryPolicy createPolicy = RetryPolicies.retryUpToMaximumCountWithFixedSleep(
        5, LEASE_SOFTLIMIT_PERIOD, TimeUnit.MILLISECONDS);
    
    Map<Class<? extends Exception>,RetryPolicy> remoteExceptionToPolicyMap =
      new HashMap<Class<? extends Exception>, RetryPolicy>();
    remoteExceptionToPolicyMap.put(AlreadyBeingCreatedException.class, createPolicy);

    Map<Class<? extends Exception>,RetryPolicy> exceptionToPolicyMap =
      new HashMap<Class<? extends Exception>, RetryPolicy>();
    exceptionToPolicyMap.put(RemoteException.class, 
        RetryPolicies.retryByRemoteException(
            RetryPolicies.TRY_ONCE_THEN_FAIL, remoteExceptionToPolicyMap));
    exceptionToPolicyMap.put(SocketTimeoutException.class, timeoutPolicy);
    RetryPolicy methodPolicy = RetryPolicies.retryByException(
        RetryPolicies.TRY_ONCE_THEN_FAIL, exceptionToPolicyMap);
    Map<String,RetryPolicy> methodNameToPolicyMap = new HashMap<String,RetryPolicy>();
    
    methodNameToPolicyMap.put("open", methodPolicy);
    methodNameToPolicyMap.put("setReplication", methodPolicy);
    methodNameToPolicyMap.put("abandonBlock", methodPolicy);
    methodNameToPolicyMap.put("abandonFileInProgress", methodPolicy);
    methodNameToPolicyMap.put("reportBadBlocks", methodPolicy);
    methodNameToPolicyMap.put("exists", methodPolicy);
    methodNameToPolicyMap.put("isDir", methodPolicy);
    methodNameToPolicyMap.put("getListing", methodPolicy);
    methodNameToPolicyMap.put("getHints", methodPolicy);
    methodNameToPolicyMap.put("renewLease", methodPolicy);
    methodNameToPolicyMap.put("getStats", methodPolicy);
    methodNameToPolicyMap.put("getDatanodeReport", methodPolicy);
    methodNameToPolicyMap.put("getBlockSize", methodPolicy);
    methodNameToPolicyMap.put("getEditLogSize", methodPolicy);
    methodNameToPolicyMap.put("complete", methodPolicy);
    methodNameToPolicyMap.put("getEditLogSize", methodPolicy);
    methodNameToPolicyMap.put("create", methodPolicy);

    return (ClientProtocol) RetryProxy.create(ClientProtocol.class,
        RPC.getProxy(ClientProtocol.class,
            ClientProtocol.versionID, nameNodeAddr, conf),
        methodNameToPolicyMap);
  }
        
  /** 
   * Create a new DFSClient connected to the given namenode server.
   */
  public DFSClient(InetSocketAddress nameNodeAddr, Configuration conf)
    throws IOException {
    this.conf = conf;
    this.namenode = createNamenode(nameNodeAddr, conf);
    String taskId = conf.get("mapred.task.id");
    if (taskId != null) {
      this.clientName = "DFSClient_" + taskId; 
    } else {
      this.clientName = "DFSClient_" + r.nextInt();
    }
    defaultBlockSize = conf.getLong("dfs.block.size", DEFAULT_BLOCK_SIZE);
    defaultReplication = (short) conf.getInt("dfs.replication", 3);
    dirAllocator = new LocalDirAllocator("dfs.client.buffer.dir");
    this.leaseChecker = new Daemon(new LeaseChecker());
    this.leaseChecker.start();
  }

  private void checkOpen() throws IOException {
    if (!running) {
      IOException result = new IOException("Filesystem closed");
      throw result;
    }
  }
    
  /**
   * Close the file system, abadoning all of the leases and files being
   * created.
   */
  public void close() throws IOException {
    // synchronize in here so that we don't need to change the API
    synchronized (this) {
      checkOpen();
      synchronized (pendingCreates) {
        Iterator file_itr = pendingCreates.keySet().iterator();
        while (file_itr.hasNext()) {
          String name = (String) file_itr.next();
          try {
            namenode.abandonFileInProgress(name, clientName);
          } catch (IOException ie) {
            System.err.println("Exception abandoning create lock on " + name);
            ie.printStackTrace();
          }
        }
        pendingCreates.clear();
      }
      this.running = false;
      try {
        leaseChecker.join();
      } catch (InterruptedException ie) {
      }
    }
  }

  /**
   * Get the default block size for this cluster
   * @return the default block size in bytes
   */
  public long getDefaultBlockSize() {
    return defaultBlockSize;
  }
    
  public long getBlockSize(UTF8 f) throws IOException {
    try {
      return namenode.getBlockSize(f.toString());
    } catch (IOException ie) {
      LOG.warn("Problem getting block size: " + 
          StringUtils.stringifyException(ie));
      throw ie;
    }
  }

  /**
   * Report corrupt blocks that were discovered by the client.
   */
  public void reportBadBlocks(LocatedBlock[] blocks) throws IOException {
    namenode.reportBadBlocks(blocks);
  }
  
  public short getDefaultReplication() {
    return defaultReplication;
  }
    
  /**
   * Get hints about the location of the indicated block(s).
   * 
   * getHints() returns a list of hostnames that store data for
   * a specific file region.  It returns a set of hostnames for 
   * every block within the indicated region.
   *
   * This function is very useful when writing code that considers
   * data-placement when performing operations.  For example, the
   * MapReduce system tries to schedule tasks on the same machines
   * as the data-block the task processes. 
   */
  public String[][] getHints(String src, long start, long length) 
    throws IOException {
    LocatedBlocks blocks = namenode.getBlockLocations(src, start, length);
    if (blocks == null) {
      return new String[0][];
    }
    int nrBlocks = blocks.locatedBlockCount();
    String[][] hints = new String[nrBlocks][];
    int idx = 0;
    for (LocatedBlock blk : blocks.getLocatedBlocks()) {
      assert idx < nrBlocks : "Incorrect index";
      DatanodeInfo[] locations = blk.getLocations();
      hints[idx] = new String[locations.length];
      for (int hCnt = 0; hCnt < locations.length; hCnt++) {
        hints[idx][hCnt] = locations[hCnt].getHostName();
      }
      idx++;
    }
    return hints;
  }

  /**
   * Create an input stream that obtains a nodelist from the
   * namenode, and then reads from all the right places.  Creates
   * inner subclass of InputStream that does the right out-of-band
   * work.
   */
  public DFSInputStream open(UTF8 src) throws IOException {
    checkOpen();
    //    Get block info from namenode
    return new DFSInputStream(src.toString());
  }

  /**
   * Create a new dfs file and return an output stream for writing into it. 
   * 
   * @param src stream name
   * @param overwrite do not check for file existence if true
   * @return output stream
   * @throws IOException
   */
  public OutputStream create(UTF8 src, 
                             boolean overwrite
                             ) throws IOException {
    return create(src, overwrite, defaultReplication, defaultBlockSize, null);
  }
    
  /**
   * Create a new dfs file and return an output stream for writing into it
   * with write-progress reporting. 
   * 
   * @param src stream name
   * @param overwrite do not check for file existence if true
   * @return output stream
   * @throws IOException
   */
  public OutputStream create(UTF8 src, 
                             boolean overwrite,
                             Progressable progress
                             ) throws IOException {
    return create(src, overwrite, defaultReplication, defaultBlockSize, null);
  }
    
  /**
   * Create a new dfs file with the specified block replication 
   * and return an output stream for writing into the file.  
   * 
   * @param src stream name
   * @param overwrite do not check for file existence if true
   * @param replication block replication
   * @return output stream
   * @throws IOException
   */
  public OutputStream create(UTF8 src, 
                             boolean overwrite, 
                             short replication,
                             long blockSize
                             ) throws IOException {
    return create(src, overwrite, replication, blockSize, null);
  }

  /**
   * Create a new dfs file with the specified block replication 
   * with write-progress reporting and return an output stream for writing
   * into the file.  
   * 
   * @param src stream name
   * @param overwrite do not check for file existence if true
   * @param replication block replication
   * @return output stream
   * @throws IOException
   */
  public OutputStream create(UTF8 src, 
                             boolean overwrite, 
                             short replication,
                             long blockSize,
                             Progressable progress
                             ) throws IOException {
    checkOpen();
    OutputStream result = new DFSOutputStream(src, overwrite, 
                                              replication, blockSize, progress);
    synchronized (pendingCreates) {
      pendingCreates.put(src.toString(), result);
    }
    return result;
  }
  /**
   * Set replication for an existing file.
   * 
   * @see ClientProtocol#setReplication(String, short)
   * @param replication
   * @throws IOException
   * @return true is successful or false if file does not exist 
   * @author shv
   */
  public boolean setReplication(UTF8 src, 
                                short replication
                                ) throws IOException {
    return namenode.setReplication(src.toString(), replication);
  }

  /**
   * Make a direct connection to namenode and manipulate structures
   * there.
   */
  public boolean rename(UTF8 src, UTF8 dst) throws IOException {
    checkOpen();
    return namenode.rename(src.toString(), dst.toString());
  }

  /**
   * Make a direct connection to namenode and manipulate structures
   * there.
   */
  public boolean delete(UTF8 src) throws IOException {
    checkOpen();
    return namenode.delete(src.toString());
  }

  /**
   */
  public boolean exists(UTF8 src) throws IOException {
    checkOpen();
    return namenode.exists(src.toString());
  }

  /**
   */
  public boolean isDirectory(UTF8 src) throws IOException {
    checkOpen();
    return namenode.isDir(src.toString());
  }

  /**
   */
  public DFSFileInfo[] listPaths(UTF8 src) throws IOException {
    checkOpen();
    return namenode.getListing(src.toString());
  }

  /**
   */
  public long totalRawCapacity() throws IOException {
    long rawNums[] = namenode.getStats();
    return rawNums[0];
  }

  /**
   */
  public long totalRawUsed() throws IOException {
    long rawNums[] = namenode.getStats();
    return rawNums[1];
  }

  public DatanodeInfo[] datanodeReport() throws IOException {
    return namenode.getDatanodeReport();
  }
    
  /**
   * Enter, leave or get safe mode.
   * See {@link ClientProtocol#setSafeMode(FSConstants.SafeModeAction)} 
   * for more details.
   * 
   * @see ClientProtocol#setSafeMode(FSConstants.SafeModeAction)
   */
  public boolean setSafeMode(SafeModeAction action) throws IOException {
    return namenode.setSafeMode(action);
  }

  /**
   * Refresh the hosts and exclude files.  (Rereads them.)
   * See {@link ClientProtocol#refreshNodes()} 
   * for more details.
   * 
   * @see ClientProtocol#refreshNodes()
   */
  public void refreshNodes() throws IOException {
    namenode.refreshNodes();
  }

  /**
   * Dumps DFS data structures into specified file.
   * See {@link ClientProtocol#metaSave(String)} 
   * for more details.
   * 
   * @see ClientProtocol#metaSave(String)
   */
  public void metaSave(String pathname) throws IOException {
    namenode.metaSave(pathname);
  }
    
  /**
   * @see ClientProtocol#finalizeUpgrade()
   */
  public void finalizeUpgrade() throws IOException {
    namenode.finalizeUpgrade();
  }

  /**
   */
  public boolean mkdirs(UTF8 src) throws IOException {
    checkOpen();
    return namenode.mkdirs(src.toString());
  }

  /**
   */
  public void lock(UTF8 src, boolean exclusive) throws IOException {
    long start = System.currentTimeMillis();
    boolean hasLock = false;
    while (!hasLock) {
      hasLock = namenode.obtainLock(src.toString(), clientName, exclusive);
      if (!hasLock) {
        try {
          Thread.sleep(400);
          if (System.currentTimeMillis() - start > 5000) {
            LOG.info("Waiting to retry lock for " + (System.currentTimeMillis() - start) + " ms.");
            Thread.sleep(2000);
          }
        } catch (InterruptedException ie) {
        }
      }
    }
  }

  /**
   *
   */
  public void release(UTF8 src) throws IOException {
    boolean hasReleased = false;
    while (!hasReleased) {
      hasReleased = namenode.releaseLock(src.toString(), clientName);
      if (!hasReleased) {
        LOG.info("Could not release.  Retrying...");
        try {
          Thread.sleep(2000);
        } catch (InterruptedException ie) {
        }
      }
    }
  }

  /**
   * Pick the best node from which to stream the data.
   * Entries in <i>nodes</i> are already in the priority order
   */
  private DatanodeInfo bestNode(DatanodeInfo nodes[], TreeSet deadNodes) throws IOException {
    if (nodes != null) { 
      for (int i = 0; i < nodes.length; i++) {
        if (!deadNodes.contains(nodes[i])) {
          return nodes[i];
        }
      }
    }
    throw new IOException("No live nodes contain current block");
  }

  /***************************************************************
   * Periodically check in with the namenode and renew all the leases
   * when the lease period is half over.
   ***************************************************************/
  class LeaseChecker implements Runnable {
    /**
     */
    public void run() {
      long lastRenewed = 0;
      while (running) {
        if (System.currentTimeMillis() - lastRenewed > (LEASE_SOFTLIMIT_PERIOD / 2)) {
          try {
            if (pendingCreates.size() > 0)
              namenode.renewLease(clientName);
            lastRenewed = System.currentTimeMillis();
          } catch (IOException ie) {
            String err = StringUtils.stringifyException(ie);
            LOG.warn("Problem renewing lease for " + clientName +
                     ": " + err);
          }
        }
        try {
          Thread.sleep(1000);
        } catch (InterruptedException ie) {
        }
      }
    }
  }

  /** Utility class to encapsulate data node info and its ip address. */
  private static class DNAddrPair {
    DatanodeInfo info;
    InetSocketAddress addr;
    DNAddrPair(DatanodeInfo info, InetSocketAddress addr) {
      this.info = info;
      this.addr = addr;
    }
  }

  /****************************************************************
   * DFSInputStream provides bytes from a named file.  It handles 
   * negotiation of the namenode and various datanodes as necessary.
   ****************************************************************/
  class DFSInputStream extends FSInputStream {
    private Socket s = null;
    private boolean closed = false;

    private String src;
    private long prefetchSize = 10 * defaultBlockSize;
    private DataInputStream blockStream;
    private LocatedBlocks locatedBlocks = null;
    private DatanodeInfo currentNode = null;
    private Block currentBlock = null;
    private long pos = 0;
    private long blockEnd = -1;
    private TreeSet<DatanodeInfo> deadNodes = new TreeSet<DatanodeInfo>();
        
    /**
     */
    public DFSInputStream(String src) throws IOException {
      this.src = src;
      prefetchSize = conf.getLong("dfs.read.prefetch.size", prefetchSize);
      openInfo();
      this.blockStream = null;
    }

    /**
     * Grab the open-file info from namenode
     */
    synchronized void openInfo() throws IOException {
      LocatedBlocks newInfo = namenode.open(src, 0, prefetchSize);

      if (locatedBlocks != null) {
        Iterator<LocatedBlock> oldIter = locatedBlocks.getLocatedBlocks().iterator();
        Iterator<LocatedBlock> newIter = newInfo.getLocatedBlocks().iterator();
        while (oldIter.hasNext() && newIter.hasNext()) {
          if (! oldIter.next().getBlock().equals(newIter.next().getBlock())) {
            throw new IOException("Blocklist for " + src + " has changed!");
          }
        }
      }
      this.locatedBlocks = newInfo;
      this.currentNode = null;
    }
    
    public long getFileLength() {
      return (locatedBlocks == null) ? 0 : locatedBlocks.getFileLength();
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
    public Block getCurrentBlock() {
      return currentBlock;
    }

    /**
     * Return collection of blocks that has already been located.
     */
    synchronized List<LocatedBlock> getAllBlocks() throws IOException {
      return getBlockRange(0, this.getFileLength());
    }

    /**
     * Get block at the specified position.
     * Fetch it from the namenode if not cached.
     * 
     * @param offset
     * @return
     * @throws IOException
     */
    private LocatedBlock getBlockAt(long offset) throws IOException {
      assert (locatedBlocks != null) : "locatedBlocks is null";
      // search cached blocks first
      int targetBlockIdx = locatedBlocks.findBlock(offset);
      if (targetBlockIdx < 0) { // block is not cached
        targetBlockIdx = LocatedBlocks.getInsertIndex(targetBlockIdx);
        // fetch more blocks
        LocatedBlocks newBlocks;
        newBlocks = namenode.getBlockLocations(src, offset, prefetchSize);
        assert (newBlocks != null) : "Could not find target position " + offset;
        locatedBlocks.insertRange(targetBlockIdx, newBlocks.getLocatedBlocks());
      }
      LocatedBlock blk = locatedBlocks.get(targetBlockIdx);
      // update current position
      this.pos = offset;
      this.blockEnd = blk.getStartOffset() + blk.getBlockSize() - 1;
      this.currentBlock = blk.getBlock();
      return blk;
    }

    /**
     * Get blocks in the specified range.
     * Fetch them from the namenode if not cached.
     * 
     * @param offset
     * @param length
     * @return
     * @throws IOException
     */
    private List<LocatedBlock> getBlockRange(long offset, long length) 
                                                        throws IOException {
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
          newBlocks = namenode.getBlockLocations(src, curOff, remaining);
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

      if (s != null) {
        s.close();
        s = null;
      }

      //
      // Compute desired block
      //
      LocatedBlock targetBlock = getBlockAt(target);
      assert (target==this.pos) : "Wrong postion " + pos + " expect " + target;
      long offsetIntoBlock = target - targetBlock.getStartOffset();

      //
      // Connect to best DataNode for desired Block, with potential offset
      //
      DatanodeInfo chosenNode = null;
      while (s == null) {
        DNAddrPair retval = chooseDataNode(targetBlock);
        chosenNode = retval.info;
        InetSocketAddress targetAddr = retval.addr;

        try {
          s = new Socket();
          s.connect(targetAddr, READ_TIMEOUT);
          s.setSoTimeout(READ_TIMEOUT);

          //
          // Xmit header info to datanode
          //
          Block block = targetBlock.getBlock();
          DataOutputStream out = new DataOutputStream(new BufferedOutputStream(s.getOutputStream()));
          out.write(OP_READSKIP_BLOCK);
          block.write(out);
          out.writeLong(offsetIntoBlock);
          out.flush();

          //
          // Get bytes in block, set streams
          //
          DataInputStream in = new DataInputStream(new BufferedInputStream(s.getInputStream()));
          long curBlockSize = in.readLong();
          long amtSkipped = in.readLong();
          if (curBlockSize != block.getNumBytes()) {
            throw new IOException("Recorded block size is " + block.getNumBytes() + ", but datanode reports size of " + curBlockSize);
          }
          if (amtSkipped != offsetIntoBlock) {
            throw new IOException("Asked for offset of " + offsetIntoBlock + ", but only received offset of " + amtSkipped);
          }

          this.blockStream = in;
          return chosenNode;
        } catch (IOException ex) {
          // Put chosen node into dead list, continue
          LOG.debug("Failed to connect to " + targetAddr + ":" 
                    + StringUtils.stringifyException(ex));
          deadNodes.add(chosenNode);
          if (s != null) {
            try {
              s.close();
            } catch (IOException iex) {
            }                        
          }
          s = null;
        }
      }
      return chosenNode;
    }

    /**
     * Close it down!
     */
    public synchronized void close() throws IOException {
      checkOpen();
      if (closed) {
        throw new IOException("Stream closed");
      }

      if (s != null) {
        blockStream.close();
        s.close();
        s = null;
      }
      super.close();
      closed = true;
    }

    /**
     * Basic read()
     */
    public synchronized int read() throws IOException {
      checkOpen();
      if (closed) {
        throw new IOException("Stream closed");
      }
      int result = -1;
      if (pos < getFileLength()) {
        if (pos > blockEnd) {
          currentNode = blockSeekTo(pos);
        }
        result = blockStream.read();
        if (result >= 0) {
          pos++;
        }
      }
      return result;
    }

    /**
     * Read the entire buffer.
     */
    public synchronized int read(byte buf[], int off, int len) throws IOException {
      checkOpen();
      if (closed) {
        throw new IOException("Stream closed");
      }
      if (pos < getFileLength()) {
        int retries = 2;
        while (retries > 0) {
          try {
            if (pos > blockEnd) {
              currentNode = blockSeekTo(pos);
            }
            int realLen = Math.min(len, (int) (blockEnd - pos + 1));
            int result = blockStream.read(buf, off, realLen);
            if (result >= 0) {
              pos += result;
            }
            return result;
          } catch (IOException e) {
            if (retries == 1) {
              LOG.warn("DFS Read: " + StringUtils.stringifyException(e));
            }
            blockEnd = -1;
            if (currentNode != null) { deadNodes.add(currentNode); }
            if (--retries == 0) {
              throw e;
            }
          }
        }
      }
      return -1;
    }

        
    private DNAddrPair chooseDataNode(LocatedBlock block)
      throws IOException {
      int failures = 0;
      while (true) {
        DatanodeInfo[] nodes = block.getLocations();
        try {
          DatanodeInfo chosenNode = bestNode(nodes, deadNodes);
          InetSocketAddress targetAddr = DataNode.createSocketAddr(chosenNode.getName());
          return new DNAddrPair(chosenNode, targetAddr);
        } catch (IOException ie) {
          String blockInfo = block.getBlock() + " file=" + src;
          if (failures >= MAX_BLOCK_ACQUIRE_FAILURES) {
            throw new IOException("Could not obtain block: " + blockInfo);
          }
          
          if (nodes == null || nodes.length == 0) {
            LOG.info("No node available for block: " + blockInfo);
          }
          LOG.info("Could not obtain block " + block.getBlock() + " from any node:  " + ie);
          try {
            Thread.sleep(3000);
          } catch (InterruptedException iex) {
          }
          deadNodes.clear(); //2nd option is to remove only nodes[blockId]
          openInfo();
          failures++;
          continue;
        }
      }
    } 
        
    private void fetchBlockByteRange(LocatedBlock block, long start,
                                     long end, byte[] buf, int offset) throws IOException {
      //
      // Connect to best DataNode for desired Block, with potential offset
      //
      Socket dn = null;
      while (dn == null) {
        DNAddrPair retval = chooseDataNode(block);
        DatanodeInfo chosenNode = retval.info;
        InetSocketAddress targetAddr = retval.addr;
            
        try {
          dn = new Socket();
          dn.connect(targetAddr, READ_TIMEOUT);
          dn.setSoTimeout(READ_TIMEOUT);
              
          //
          // Xmit header info to datanode
          //
          DataOutputStream out = new DataOutputStream(new BufferedOutputStream(dn.getOutputStream()));
          out.write(OP_READ_RANGE_BLOCK);
          block.getBlock().write(out);
          out.writeLong(start);
          out.writeLong(end);
          out.flush();
              
          //
          // Get bytes in block, set streams
          //
          DataInputStream in = new DataInputStream(new BufferedInputStream(dn.getInputStream()));
          long curBlockSize = in.readLong();
          long actualStart = in.readLong();
          long actualEnd = in.readLong();
          if (curBlockSize != block.getBlockSize()) {
            throw new IOException("Recorded block size is " +
                                  block.getBlockSize() + 
                                  ", but datanode reports size of " +
                                  curBlockSize);
          }
          if ((actualStart != start) || (actualEnd != end)) {
            throw new IOException("Asked for byte range  " + start +
                                  "-" + end + ", but only received range " + actualStart +
                                  "-" + actualEnd);
          }
          int nread = in.read(buf, offset, (int)(end - start + 1));
          assert nread == (int)(end - start + 1) : 
            "Incorrect number of bytes read " + nread
            + ". Expacted " + (int)(end - start + 1);
        } catch (IOException ex) {
          // Put chosen node into dead list, continue
          LOG.debug("Failed to connect to " + targetAddr + ":" 
                    + StringUtils.stringifyException(ex));
          deadNodes.add(chosenNode);
          if (dn != null) {
            try {
              dn.close();
            } catch (IOException iex) {
            }
          }
          dn = null;
        }
      }
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
    public int read(long position, byte[] buffer, int offset, int length)
      throws IOException {
      // sanity checks
      checkOpen();
      if (closed) {
        throw new IOException("Stream closed");
      }
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
      for (LocatedBlock blk : blockRange) {
        long targetStart = position - blk.getStartOffset();
        long bytesToRead = Math.min(remaining, blk.getBlockSize() - targetStart);
        fetchBlockByteRange(blk, targetStart, 
                            targetStart + bytesToRead - 1, buffer, offset);
        remaining -= bytesToRead;
        position += bytesToRead;
        offset += bytesToRead;
      }
      assert remaining == 0 : "Wrong number of bytes read.";
      return realLen;
    }
        
    /**
     * Seek to a new arbitrary location
     */
    public synchronized void seek(long targetPos) throws IOException {
      if (targetPos > getFileLength()) {
        throw new IOException("Cannot seek after EOF");
      }
      boolean done = false;
      if (pos <= targetPos && targetPos <= blockEnd) {
        //
        // If this seek is to a positive position in the current
        // block, and this piece of data might already be lying in
        // the TCP buffer, then just eat up the intervening data.
        //
        int diff = (int)(targetPos - pos);
        if (diff <= TCP_WINDOW_SIZE) {
          int adiff = blockStream.skipBytes(diff);
          pos += adiff;
          if (pos == targetPos) {
            done = true;
          }
        }
      }
      if (!done) {
        pos = targetPos;
        blockEnd = -1;
      }
    }

    /**
     * Seek to given position on a node other than the current node.  If
     * a node other than the current node is found, then returns true. 
     * If another node could not be found, then returns false.
     */
    public synchronized boolean seekToNewSource(long targetPos) throws IOException {
      boolean markedDead = deadNodes.contains(currentNode);
      deadNodes.add(currentNode);
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
    public synchronized long getPos() throws IOException {
      return pos;
    }

    /**
     */
    public synchronized int available() throws IOException {
      if (closed) {
        throw new IOException("Stream closed");
      }
      return (int) (getFileLength() - pos);
    }

    /**
     * We definitely don't support marks
     */
    public boolean markSupported() {
      return false;
    }
    public void mark(int readLimit) {
    }
    public void reset() throws IOException {
      throw new IOException("Mark not supported");
    }
  }
    
  static class DFSDataInputStream extends FSDataInputStream {
    DFSDataInputStream(DFSInputStream in, Configuration conf)
      throws IOException {
      super(in, conf);
    }
      
    DFSDataInputStream(DFSInputStream in, int bufferSize) throws IOException {
      super(in, bufferSize);
    }
      
    /**
     * Returns the datanode from which the stream is currently reading.
     */
    public DatanodeInfo getCurrentDatanode() {
      return ((DFSInputStream)inStream).getCurrentDatanode();
    }
      
    /**
     * Returns the block containing the target position. 
     */
    public Block getCurrentBlock() {
      return ((DFSInputStream)inStream).getCurrentBlock();
    }

    /**
     * Return collection of blocks that has already been located.
     */
    synchronized List<LocatedBlock> getAllBlocks() throws IOException {
      return ((DFSInputStream)inStream).getAllBlocks();
    }

  }

  /****************************************************************
   * DFSOutputStream creates files from a stream of bytes.
   ****************************************************************/
  class DFSOutputStream extends OutputStream {
    private Socket s;
    boolean closed = false;

    private byte outBuf[] = new byte[BUFFER_SIZE];
    private int pos = 0;

    private UTF8 src;
    private boolean overwrite;
    private short replication;
    private boolean firstTime = true;
    private DataOutputStream blockStream;
    private DataInputStream blockReplyStream;
    private File backupFile;
    private OutputStream backupStream;
    private Block block;
    private long filePos = 0;
    private int bytesWrittenToBlock = 0;
    private String datanodeName;
    private long blockSize;

    private Progressable progress;
    /**
     * Create a new output stream to the given DataNode.
     */
    public DFSOutputStream(UTF8 src, boolean overwrite, 
                           short replication, long blockSize,
                           Progressable progress
                           ) throws IOException {
      this.src = src;
      this.overwrite = overwrite;
      this.replication = replication;
      this.backupFile = newBackupFile();
      this.blockSize = blockSize;
      this.backupStream = new FileOutputStream(backupFile);
      this.progress = progress;
      if (progress != null) {
        LOG.debug("Set non-null progress callback on DFSOutputStream "+src);
      }
    }

    /* Wrapper for closing backupStream. This sets backupStream to null so
     * that we do not attempt to write to backupStream that could be
     * invalid in subsequent writes. Otherwise we might end trying to write
     * filedescriptor that we don't own.
     */
    private void closeBackupStream() throws IOException {
      if (backupStream != null) {
        OutputStream stream = backupStream;
        backupStream = null;
        stream.close();
      }   
    }
    /* Similar to closeBackupStream(). Theoritically deleting a file
     * twice could result in deleting a file that we should not.
     */
    private void deleteBackupFile() {
      if (backupFile != null) {
        File file = backupFile;
        backupFile = null;
        file.delete();
      }
    }
        
    private File newBackupFile() throws IOException {
      String name = "tmp" + File.separator +
                     "client-" + Math.abs(r.nextLong());
      File result = dirAllocator.createTmpFileForWrite(name, 
                                                       2 * blockSize, 
                                                       conf);
      return result;
    }

    /**
     * Open a DataOutputStream to a DataNode so that it can be written to.
     * This happens when a file is created and each time a new block is allocated.
     * Must get block ID and the IDs of the destinations from the namenode.
     */
    private synchronized void nextBlockOutputStream() throws IOException {
      boolean retry = false;
      long startTime = System.currentTimeMillis();
      do {
        retry = false;
                
        LocatedBlock lb;
        if (firstTime) {
          lb = locateNewBlock();
        } else {
          lb = locateFollowingBlock(startTime);
        }

        block = lb.getBlock();
        if (block.getNumBytes() < bytesWrittenToBlock) {
          block.setNumBytes(bytesWrittenToBlock);
        }
        DatanodeInfo nodes[] = lb.getLocations();

        //
        // Connect to first DataNode in the list.  Abort if this fails.
        //
        InetSocketAddress target = DataNode.createSocketAddr(nodes[0].getName());
        try {
          s = new Socket();
          s.connect(target, READ_TIMEOUT);
          s.setSoTimeout(replication * READ_TIMEOUT);
          datanodeName = nodes[0].getName();
        } catch (IOException ie) {
          // Connection failed.  Let's wait a little bit and retry
          try {
            if (System.currentTimeMillis() - startTime > 5000) {
              LOG.info("Waiting to find target node: " + target);
            }
            Thread.sleep(6000);
          } catch (InterruptedException iex) {
          }
          if (firstTime) {
            namenode.abandonFileInProgress(src.toString(), 
                                           clientName);
          } else {
            namenode.abandonBlock(block, src.toString());
          }
          retry = true;
          continue;
        }

        //
        // Xmit header info to datanode
        //
        DataOutputStream out = new DataOutputStream(new BufferedOutputStream(s.getOutputStream()));
        out.write(OP_WRITE_BLOCK);
        out.writeBoolean(true);
        block.write(out);
        out.writeInt(nodes.length);
        for (int i = 0; i < nodes.length; i++) {
          nodes[i].write(out);
        }
        out.write(CHUNKED_ENCODING);
        blockStream = out;
        blockReplyStream = new DataInputStream(new BufferedInputStream(s.getInputStream()));
      } while (retry);
      firstTime = false;
    }

    private LocatedBlock locateNewBlock() throws IOException {     
      return namenode.create(src.toString(), clientName,
          overwrite, replication, blockSize);
    }
        
    private LocatedBlock locateFollowingBlock(long start
                                              ) throws IOException {     
      int retries = 5;
      long sleeptime = 400;
      while (true) {
        long localstart = System.currentTimeMillis();
        while (true) {
          try {
            return namenode.addBlock(src.toString(), clientName);
          } catch (RemoteException e) {
            if (--retries == 0 || 
                !NotReplicatedYetException.class.getName().
                equals(e.getClassName())) {
              throw e;
            } else {
              LOG.info(StringUtils.stringifyException(e));
              if (System.currentTimeMillis() - localstart > 5000) {
                LOG.info("Waiting for replication for " + 
                         (System.currentTimeMillis() - localstart)/1000 + 
                         " seconds");
              }
              try {
                LOG.warn("NotReplicatedYetException sleeping " + src +
                          " retries left " + retries);
                Thread.sleep(sleeptime);
                sleeptime *= 2;
              } catch (InterruptedException ie) {
              }
            }                
          }
        }
      } 
    }

    /**
     * We're referring to the file pos here
     */
    public synchronized long getPos() throws IOException {
      return filePos;
    }
			
    /**
     * Writes the specified byte to this output stream.
     */
    public synchronized void write(int b) throws IOException {
      checkOpen();
      if (closed) {
        throw new IOException("Stream closed");
      }

      if ((bytesWrittenToBlock + pos == blockSize) ||
          (pos >= BUFFER_SIZE)) {
        flush();
      }
      outBuf[pos++] = (byte) b;
      filePos++;
    }

    /**
     * Writes the specified bytes to this output stream.
     */
    public synchronized void write(byte b[], int off, int len)
      throws IOException {
      checkOpen();
      if (closed) {
        throw new IOException("Stream closed");
      }
      while (len > 0) {
        int remaining = Math.min(BUFFER_SIZE - pos,
                                 (int)((blockSize - bytesWrittenToBlock) - pos));
        int toWrite = Math.min(remaining, len);
        System.arraycopy(b, off, outBuf, pos, toWrite);
        pos += toWrite;
        off += toWrite;
        len -= toWrite;
        filePos += toWrite;

        if ((bytesWrittenToBlock + pos >= blockSize) ||
            (pos == BUFFER_SIZE)) {
          flush();
        }
      }
    }

    /**
     * Flush the buffer, getting a stream to a new block if necessary.
     */
    public synchronized void flush() throws IOException {
      checkOpen();
      if (closed) {
        throw new IOException("Stream closed");
      }

      if (bytesWrittenToBlock + pos >= blockSize) {
        flushData((int) blockSize - bytesWrittenToBlock);
      }
      if (bytesWrittenToBlock == blockSize) {
        endBlock();
      }
      flushData(pos);
    }

    /**
     * Actually flush the accumulated bytes to the remote node,
     * but no more bytes than the indicated number.
     */
    private synchronized void flushData(int maxPos) throws IOException {
      int workingPos = Math.min(pos, maxPos);
            
      if (workingPos > 0) {
        if (backupStream == null) {
          throw new IOException("Trying to write to backupStream " +
                                "but it already closed or not open");
        }
        //
        // To the local block backup, write just the bytes
        //
        backupStream.write(outBuf, 0, workingPos);

        //
        // Track position
        //
        bytesWrittenToBlock += workingPos;
        System.arraycopy(outBuf, workingPos, outBuf, 0, pos - workingPos);
        pos -= workingPos;
      }
    }

    /**
     * We're done writing to the current block.
     */
    private synchronized void endBlock() throws IOException {
      long sleeptime = 400;
      //
      // Done with local copy
      //
      closeBackupStream();

      //
      // Send it to datanode
      //
      boolean sentOk = false;
      int remainingAttempts = 
        conf.getInt("dfs.client.block.write.retries", 3);
      while (!sentOk) {
        nextBlockOutputStream();
        InputStream in = new FileInputStream(backupFile);
        try {
          byte buf[] = new byte[BUFFER_SIZE];
          int bytesRead = in.read(buf);
          while (bytesRead > 0) {
            blockStream.writeLong((long) bytesRead);
            blockStream.write(buf, 0, bytesRead);
            if (progress != null) { progress.progress(); }
            bytesRead = in.read(buf);
          }
          internalClose();
          sentOk = true;
        } catch (IOException ie) {
          handleSocketException(ie);
          remainingAttempts -= 1;
          if (remainingAttempts == 0) {
            throw ie;
          }
          try {
            Thread.sleep(sleeptime);
          } catch (InterruptedException e) {
          }
        } finally {
          in.close();
        }
      }

      bytesWrittenToBlock = 0;
      //
      // Delete local backup, start new one
      //
      deleteBackupFile();
      File tmpFile = newBackupFile();
      bytesWrittenToBlock = 0;
      backupStream = new FileOutputStream(tmpFile);
      backupFile = tmpFile;
    }

    /**
     * Close down stream to remote datanode.
     */
    private synchronized void internalClose() throws IOException {
      try {
        blockStream.writeLong(0);
        blockStream.flush();

        long complete = blockReplyStream.readLong();
        if (complete != WRITE_COMPLETE) {
          LOG.info("Did not receive WRITE_COMPLETE flag: " + complete);
          throw new IOException("Did not receive WRITE_COMPLETE_FLAG: " + complete);
        }
      } catch (IOException ie) {
        throw (IOException)
          new IOException("failure closing block of file " +
                          src.toString() + " to node " +
                          (datanodeName == null ? "?" : datanodeName)
                          ).initCause(ie);
      }
                    
      LocatedBlock lb = new LocatedBlock();
      lb.readFields(blockReplyStream);

      s.close();
      s = null;
    }

    private void handleSocketException(IOException ie) throws IOException {
      LOG.warn("Error while writing.", ie);
      try {
        if (s != null) {
          s.close();
          s = null;
        }
      } catch (IOException ie2) {
        LOG.warn("Error closing socket.", ie2);
      }
      namenode.abandonBlock(block, src.toString());
    }

    /**
     * Closes this output stream and releases any system 
     * resources associated with this stream.
     */
    public synchronized void close() throws IOException {
      checkOpen();
      if (closed) {
        throw new IOException("Stream closed");
      }
          
      try {
        flush();
        if (filePos == 0 || bytesWrittenToBlock != 0) {
          try {
            endBlock();
          } catch (IOException e) {
            namenode.abandonFileInProgress(src.toString(), clientName);
            throw e;
          }
        }
            
        closeBackupStream();
        deleteBackupFile();

        if (s != null) {
          s.close();
          s = null;
        }
        super.close();

        long localstart = System.currentTimeMillis();
        boolean fileComplete = false;
        while (!fileComplete) {
          fileComplete = namenode.complete(src.toString(), clientName);
          if (!fileComplete) {
            try {
              Thread.sleep(400);
              if (System.currentTimeMillis() - localstart > 5000) {
                LOG.info("Could not complete file, retrying...");
              }
            } catch (InterruptedException ie) {
            }
          }
        }
        closed = true;
      } finally {
        synchronized (pendingCreates) {
          pendingCreates.remove(src.toString());
        }
      }
    }
  }
}
