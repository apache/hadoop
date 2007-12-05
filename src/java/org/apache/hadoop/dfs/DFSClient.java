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
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.dfs.DistributedFileSystem.DiskStatus;
import org.apache.hadoop.security.UnixUserGroupInformation;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.*;

import org.apache.commons.logging.*;

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.zip.CRC32;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.ConcurrentHashMap;

import javax.net.SocketFactory;
import javax.security.auth.login.LoginException;

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
 ********************************************************/
class DFSClient implements FSConstants {
  public static final Log LOG = LogFactory.getLog("org.apache.hadoop.fs.DFSClient");
  static final int MAX_BLOCK_ACQUIRE_FAILURES = 3;
  private static final int TCP_WINDOW_SIZE = 128 * 1024; // 128 KB
  ClientProtocol namenode;
  boolean running = true;
  Random r = new Random();
  String clientName;
  Daemon leaseChecker;
  private Configuration conf;
  private long defaultBlockSize;
  private short defaultReplication;
  private LocalDirAllocator dirAllocator;
  private SocketFactory socketFactory;
    
  /**
   * A map from name -> DFSOutputStream of files that are currently being
   * written by this client.
   */
  private TreeMap<String, OutputStream> pendingCreates =
    new TreeMap<String, OutputStream>();
    
  static ClientProtocol createNamenode(
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
    methodNameToPolicyMap.put("getPreferredBlockSize", methodPolicy);
    methodNameToPolicyMap.put("getEditLogSize", methodPolicy);
    methodNameToPolicyMap.put("complete", methodPolicy);
    methodNameToPolicyMap.put("getEditLogSize", methodPolicy);
    methodNameToPolicyMap.put("create", methodPolicy);

    UserGroupInformation userInfo;
    try {
      userInfo = UnixUserGroupInformation.login(conf);
    } catch (LoginException e) {
      throw new IOException(e.getMessage());
    }

    return (ClientProtocol) RetryProxy.create(ClientProtocol.class,
        RPC.getProxy(ClientProtocol.class,
            ClientProtocol.versionID, nameNodeAddr, userInfo, conf,
            NetUtils.getSocketFactory(conf, ClientProtocol.class)),
        methodNameToPolicyMap);
  }
        
  /** 
   * Create a new DFSClient connected to the given namenode server.
   */
  public DFSClient(InetSocketAddress nameNodeAddr, Configuration conf)
    throws IOException {
    this.conf = conf;
    this.socketFactory = NetUtils.getSocketFactory(conf, ClientProtocol.class);
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
    
  public long getBlockSize(String f) throws IOException {
    try {
      return namenode.getPreferredBlockSize(f);
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

  public DFSInputStream open(String src) throws IOException {
    return open(src, conf.getInt("io.file.buffer.size", 4096));
  }
  /**
   * Create an input stream that obtains a nodelist from the
   * namenode, and then reads from all the right places.  Creates
   * inner subclass of InputStream that does the right out-of-band
   * work.
   */
  public DFSInputStream open(String src, int buffersize) throws IOException {
    checkOpen();
    //    Get block info from namenode
    return new DFSInputStream(src, buffersize);
  }

  /**
   * Create a new dfs file and return an output stream for writing into it. 
   * 
   * @param src stream name
   * @param overwrite do not check for file existence if true
   * @return output stream
   * @throws IOException
   */
  public OutputStream create(String src, 
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
  public OutputStream create(String src, 
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
  public OutputStream create(String src, 
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
  public OutputStream create(String src, 
                             boolean overwrite, 
                             short replication,
                             long blockSize,
                             Progressable progress
                             ) throws IOException {
    return create(src, overwrite, replication, blockSize, progress,
        conf.getInt("io.file.buffer.size", 4096));
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
  public OutputStream create(String src, 
                             boolean overwrite, 
                             short replication,
                             long blockSize,
                             Progressable progress,
                             int buffersize
                             ) throws IOException {
    checkOpen();
    OutputStream result = new DFSOutputStream(
        src, overwrite, replication, blockSize, progress, buffersize);
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
   */
  public boolean setReplication(String src, 
                                short replication
                                ) throws IOException {
    return namenode.setReplication(src, replication);
  }

  /**
   * Make a direct connection to namenode and manipulate structures
   * there.
   */
  public boolean rename(String src, String dst) throws IOException {
    checkOpen();
    return namenode.rename(src, dst);
  }

  /**
   * Make a direct connection to namenode and manipulate structures
   * there.
   */
  public boolean delete(String src) throws IOException {
    checkOpen();
    return namenode.delete(src);
  }

  /**
   */
  public boolean exists(String src) throws IOException {
    checkOpen();
    return namenode.exists(src);
  }

  /**
   */
  public boolean isDirectory(String src) throws IOException {
    checkOpen();
    return namenode.isDir(src);
  }

  /**
   */
  public DFSFileInfo[] listPaths(String src) throws IOException {
    checkOpen();
    return namenode.getListing(src);
  }

  public DFSFileInfo getFileInfo(String src) throws IOException {
    checkOpen();
    return namenode.getFileInfo(src);
  }

  public DiskStatus getDiskStatus() throws IOException {
    long rawNums[] = namenode.getStats();
    return new DiskStatus(rawNums[0], rawNums[1], rawNums[2]);
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

  public DatanodeInfo[] datanodeReport(DatanodeReportType type)
  throws IOException {
    return namenode.getDatanodeReport(type);
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
   * @see ClientProtocol#distributedUpgradeProgress(FSConstants.UpgradeAction)
   */
  public UpgradeStatusReport distributedUpgradeProgress(UpgradeAction action
                                                        ) throws IOException {
    return namenode.distributedUpgradeProgress(action);
  }

  /**
   */
  public boolean mkdirs(String src) throws IOException {
    checkOpen();
    return namenode.mkdirs(src);
  }

  /**
   * Retrieves the total size of all files and directories under
   * the specified path.
   * 
   * @param src
   * @throws IOException
   * @return the number of bytes in the subtree rooted at src
   */
  public long getContentLength(String src
                               ) throws IOException {
    return namenode.getContentLength(src);
  }

  /**
   * Pick the best node from which to stream the data.
   * Entries in <i>nodes</i> are already in the priority order
   */
  private DatanodeInfo bestNode(DatanodeInfo nodes[], 
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

  /** This is a wrapper around connection to datadone
   * and understands checksum, offset etc
   */
  static class BlockReader extends FSInputChecker {

    private DataInputStream in;
    private DataChecksum checksum;
    private long lastChunkOffset = -1;
    private long lastChunkLen = -1;

    private long startOffset;
    private long firstChunkOffset;
    private int bytesPerChecksum;
    private int checksumSize;
    private boolean gotEOS = false;
    
    byte[] skipBuf = null;
    
    /* FSInputChecker interface */
    
    /* same interface as inputStream java.io.InputStream#read()
     * used by DFSInputStream#read()
     * This violates one rule when there is a checksum error:
     * "Read should not modify user buffer before successful read"
     * because it first reads the data to user buffer and then checks
     * the checksum.
     */
    @Override
    public synchronized int read(byte[] buf, int off, int len) 
                                 throws IOException {
      
      //for the first read, skip the extra bytes at the front.
      if (lastChunkLen < 0 && startOffset > firstChunkOffset && len > 0) {
        // Skip these bytes. But don't call this.skip()!
        int toSkip = (int)(startOffset - firstChunkOffset);
        if ( skipBuf == null ) {
          skipBuf = new byte[bytesPerChecksum];
        }
        if ( super.read(skipBuf, 0, toSkip) != toSkip ) {
          // should never happen
          throw new IOException("Could not skip required number of bytes");
        }
      }
      
      return super.read(buf, off, len);
    }

    @Override
    public synchronized long skip(long n) throws IOException {
      /* How can we make sure we don't throw a ChecksumException, at least
       * in majority of the cases?. This one throws. */  
      if ( skipBuf == null ) {
        skipBuf = new byte[bytesPerChecksum]; 
      }

      long nSkipped = 0;
      while ( nSkipped < n ) {
        int toSkip = (int)Math.min(n-nSkipped, skipBuf.length);
        int ret = read(skipBuf, 0, toSkip);
        if ( ret <= 0 ) {
          return nSkipped;
        }
        nSkipped += ret;
      }
      return nSkipped;
    }

    @Override
    public int read() throws IOException {
      throw new IOException("read() is not expected to be invoked. " +
                            "Use read(buf, off, len) instead.");
    }
    
    @Override
    public boolean seekToNewSource(long targetPos) throws IOException {
      /* Checksum errors are handled outside the BlockReader. 
       * DFSInputStream does not always call 'seekToNewSource'. In the 
       * case of pread(), it just tries a different replica without seeking.
       */ 
      return false;
    }
    
    @Override
    public void seek(long pos) throws IOException {
      throw new IOException("Seek() is not supported in BlockInputChecker");
    }

    @Override
    protected long getChunkPosition(long pos) {
      throw new RuntimeException("getChunkPosition() is not supported, " +
                                 "since seek is not required");
    }
    
    @Override
    protected synchronized int readChunk(long pos, byte[] buf, int offset, 
                                         int len, byte[] checksumBuf) 
                                         throws IOException {
      // Read one chunk.
      
      if ( gotEOS ) {
        if ( startOffset < 0 ) {
          //This is mainly for debugging. can be removed.
          throw new IOException( "BlockRead: already got EOS or an error" );
        }
        startOffset = -1;
        return -1;
      }
      
      // Read one DATA_CHUNK.
      long chunkOffset = lastChunkOffset;
      if ( lastChunkLen > 0 ) {
        chunkOffset += lastChunkLen;
      }
      
      if ( (pos + firstChunkOffset) != chunkOffset ) {
        throw new IOException("Mismatch in pos : " + pos + " + " + 
                              firstChunkOffset + " != " + chunkOffset);
      }
      
      int chunkLen = in.readInt();
      
      // Sanity check the lengths
      if ( chunkLen < 0 || chunkLen > bytesPerChecksum ||
          ( lastChunkLen >= 0 && // prev packet exists
              ( (chunkLen > 0 && lastChunkLen != bytesPerChecksum) ||
                  chunkOffset != (lastChunkOffset + lastChunkLen) ) ) ) {
        throw new IOException("BlockReader: error in chunk's offset " +
                              "or length (" + chunkOffset + ":" +
                              chunkLen + ")");
      }

      if ( chunkLen > 0 ) {
        // len should be >= chunkLen
        IOUtils.readFully(in, buf, offset, chunkLen);
      }
      
      if ( checksumSize > 0 ) {
        IOUtils.readFully(in, checksumBuf, 0, checksumSize);
      }

      lastChunkOffset = chunkOffset;
      lastChunkLen = chunkLen;
      
      if ( chunkLen == 0 ) {
        gotEOS = true;
        return -1;
      }
      
      return chunkLen;
    }
    
    private BlockReader( String file, long blockId, DataInputStream in, 
                         DataChecksum checksum, long startOffset,
                         long firstChunkOffset ) {
      super(new Path("/blk_" + blockId + ":of:" + file)/*too non path-like?*/,
            1, (checksum.getChecksumSize() > 0) ? checksum : null, 
            checksum.getBytesPerChecksum(),
            checksum.getChecksumSize());
      
      this.in = in;
      this.checksum = checksum;
      this.startOffset = Math.max( startOffset, 0 );

      this.firstChunkOffset = firstChunkOffset;
      lastChunkOffset = firstChunkOffset;
      lastChunkLen = -1;

      bytesPerChecksum = this.checksum.getBytesPerChecksum();
      checksumSize = this.checksum.getChecksumSize();
    }

    /** Java Doc required */
    static BlockReader newBlockReader( Socket sock, String file, long blockId, 
                                       long startOffset, long len,
                                       int bufferSize)
                                       throws IOException {
      
      // in and out will be closed when sock is closed (by the caller)
      DataOutputStream out = new DataOutputStream(
                       new BufferedOutputStream(sock.getOutputStream()));

      //write the header.
      out.writeShort( DATA_TRANFER_VERSION );
      out.write( OP_READ_BLOCK );
      out.writeLong( blockId );
      out.writeLong( startOffset );
      out.writeLong( len );
      out.flush();

      //
      // Get bytes in block, set streams
      //

      DataInputStream in = new DataInputStream(
                   new BufferedInputStream(sock.getInputStream(), bufferSize));
      
      if ( in.readShort() != OP_STATUS_SUCCESS ) {
        throw new IOException("Got error in response to OP_READ_BLOCK");
      }
      DataChecksum checksum = DataChecksum.newDataChecksum( in );
      //Warning when we get CHECKSUM_NULL?
      
      // Read the first chunk offset.
      long firstChunkOffset = in.readLong();
      
      if ( firstChunkOffset < 0 || firstChunkOffset > startOffset ||
          firstChunkOffset >= (startOffset + checksum.getBytesPerChecksum())) {
        throw new IOException("BlockReader: error in first chunk offset (" +
                              firstChunkOffset + ") startOffset is " + 
                              startOffset + "for file XXX");
      }

      return new BlockReader( file, blockId, in, checksum,
                              startOffset, firstChunkOffset );
    }

    @Override
    public synchronized void close() throws IOException {
      startOffset = -1;
      checksum = null;
      // in will be closed when its Socket is closed.
    }
    
    /** kind of like readFully(). Only reads as much as possible.
     * And allows use of protected readFully().
     */
    int readAll(byte[] buf, int offset, int len) throws IOException {
      return readFully(this, buf, offset, len);
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
    private BlockReader blockReader;
    private LocatedBlocks locatedBlocks = null;
    private DatanodeInfo currentNode = null;
    private Block currentBlock = null;
    private long pos = 0;
    private long blockEnd = -1;
    /* XXX Use of CocurrentHashMap is temp fix. Need to fix 
     * parallel accesses to DFSInputStream (through ptreads) properly */
    private ConcurrentHashMap<DatanodeInfo, DatanodeInfo> deadNodes = 
               new ConcurrentHashMap<DatanodeInfo, DatanodeInfo>();
    private int buffersize = 1;
    
    private byte[] oneByteBuf = new byte[1]; // used for 'int read()'
    
    void addToDeadNodes(DatanodeInfo dnInfo) {
      deadNodes.put(dnInfo, dnInfo);
    }
    
    /**
     */
    public DFSInputStream(String src, int buffersize) throws IOException {
      this.buffersize = buffersize;
      this.src = src;
      prefetchSize = conf.getLong("dfs.read.prefetch.size", prefetchSize);
      openInfo();
      blockReader = null;
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
    
    public synchronized long getFileLength() {
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
     * @return located block
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
     * @return consequent segment of located blocks
     * @throws IOException
     */
    private synchronized List<LocatedBlock> getBlockRange(long offset, 
                                                          long length) 
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

      if ( blockReader != null ) {
        blockReader.close(); 
        blockReader = null;
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
          s = socketFactory.createSocket();
          s.connect(targetAddr, READ_TIMEOUT);
          s.setSoTimeout(READ_TIMEOUT);
          Block blk = targetBlock.getBlock();
          
          blockReader = BlockReader.newBlockReader(s, src, blk.getBlockId(), 
                                                   offsetIntoBlock,
                                                   (blk.getNumBytes() - 
                                                    offsetIntoBlock),
                                                   buffersize);
          return chosenNode;
        } catch (IOException ex) {
          // Put chosen node into dead list, continue
          LOG.debug("Failed to connect to " + targetAddr + ":" 
                    + StringUtils.stringifyException(ex));
          addToDeadNodes(chosenNode);
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
    @Override
    public synchronized void close() throws IOException {
      checkOpen();
      if (closed) {
        throw new IOException("Stream closed");
      }

      if ( blockReader != null ) {
        blockReader.close();
        blockReader = null;
      }
      
      if (s != null) {
        s.close();
        s = null;
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
     * ChecksuFileSystem
     */ 
    private synchronized int readBuffer(byte buf[], int off, int len) 
                                                    throws IOException {
      IOException ioe;
 
      while (true) {
        // retry as many times as seekToNewSource allows.
        try {
          return blockReader.read(buf, off, len);
        } catch ( ChecksumException ce ) {
          LOG.warn("Found Checksum error for " + currentBlock + " from " +
                   currentNode.getName() + " at " + ce.getPos());          
          reportChecksumFailure(src, currentBlock, currentNode);
          ioe = ce;
        } catch ( IOException e ) {
          LOG.warn("Exception while reading from " + currentBlock +
                   " of " + src + " from " + currentNode + ": " +
                   StringUtils.stringifyException(e));
          ioe = e;
        }
        addToDeadNodes(currentNode);
        if (!seekToNewSource(pos)) {
            throw ioe;
        }
      }
    }

    /**
     * Read the entire buffer.
     */
    @Override
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
            int result = readBuffer(buf, off, realLen);
            
            if (result >= 0) {
              pos += result;
            } else {
              // got a EOS from reader though we expect more data on it.
              throw new IOException("Unexpected EOS from the reader");
            }
            return result;
          } catch (ChecksumException ce) {
            throw ce;            
          } catch (IOException e) {
            if (retries == 1) {
              LOG.warn("DFS Read: " + StringUtils.stringifyException(e));
            }
            blockEnd = -1;
            if (currentNode != null) { addToDeadNodes(currentNode); }
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
          InetSocketAddress targetAddr = 
                            NetUtils.createSocketAddr(chosenNode.getName());
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
      int numAttempts = block.getLocations().length;
      IOException ioe = null;
      
      while (dn == null && numAttempts-- > 0 ) {
        DNAddrPair retval = chooseDataNode(block);
        DatanodeInfo chosenNode = retval.info;
        InetSocketAddress targetAddr = retval.addr;
            
        try {
          dn = socketFactory.createSocket();
          dn.connect(targetAddr, READ_TIMEOUT);
          dn.setSoTimeout(READ_TIMEOUT);
              
          int len = (int) (end - start + 1);
              
          BlockReader reader = 
            BlockReader.newBlockReader(dn, src, block.getBlock().getBlockId(),
                                       start, len, buffersize);
          int nread = reader.readAll(buf, offset, len);
          if (nread != len) {
            throw new IOException("truncated return from reader.read(): " +
                                  "excpected " + len + ", got " + nread);
          }
          return;
        } catch (ChecksumException e) {
          ioe = e;
          LOG.warn("fetchBlockByteRange(). Got a checksum exception for " +
                   src + " at " + block.getBlock() + ":" + 
                   e.getPos() + " from " + chosenNode.getName());
          reportChecksumFailure(src, block.getBlock(), chosenNode);
        } catch (IOException e) {
          ioe = e;
          LOG.warn("Failed to connect to " + targetAddr + ":" 
                    + StringUtils.stringifyException(e));
        } 
        // Put chosen node into dead list, continue
        addToDeadNodes(chosenNode);
        if (dn != null) {
          try {
            dn.close();
          } catch (IOException iex) {
          }
          dn = null;
        }
      }
      throw (ioe == null) ? new IOException("Could not read data") : ioe;
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
      boolean done = false;
      if (pos <= targetPos && targetPos <= blockEnd) {
        //
        // If this seek is to a positive position in the current
        // block, and this piece of data might already be lying in
        // the TCP buffer, then just eat up the intervening data.
        //
        int diff = (int)(targetPos - pos);
        if (diff <= TCP_WINDOW_SIZE) {
          pos += blockReader.skip(diff);
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

    /**
     */
    @Override
    public synchronized int available() throws IOException {
      if (closed) {
        throw new IOException("Stream closed");
      }
      return (int) (getFileLength() - pos);
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
  }
    
  static class DFSDataInputStream extends FSDataInputStream {
    DFSDataInputStream(DFSInputStream in)
      throws IOException {
      super(in);
    }
      
    /**
     * Returns the datanode from which the stream is currently reading.
     */
    public DatanodeInfo getCurrentDatanode() {
      return ((DFSInputStream)in).getCurrentDatanode();
    }
      
    /**
     * Returns the block containing the target position. 
     */
    public Block getCurrentBlock() {
      return ((DFSInputStream)in).getCurrentBlock();
    }

    /**
     * Return collection of blocks that has already been located.
     */
    synchronized List<LocatedBlock> getAllBlocks() throws IOException {
      return ((DFSInputStream)in).getAllBlocks();
    }

  }

  /****************************************************************
   * DFSOutputStream creates files from a stream of bytes.
   ****************************************************************/
  class DFSOutputStream extends FSOutputSummer {
    private Socket s;
    boolean closed = false;

    private String src;
    private short replication;
    private DataOutputStream blockStream;
    private DataInputStream blockReplyStream;
    private File backupFile;
    private OutputStream backupStream;
    private Block block;
    private long filePos = 0;
    private long bytesWrittenToBlock = 0;
    private long blockSize;
    private int buffersize;
    private DataChecksum checksum;

    private Progressable progress;
    /**
     * Create a new output stream to the given DataNode.
     */
    public DFSOutputStream(String src, boolean overwrite, 
                           short replication, long blockSize,
                           Progressable progress,
                           int buffersize
                           ) throws IOException {
      super(new CRC32(), conf.getInt("io.bytes.per.checksum", 512), 4);
      this.src = src;
      this.replication = replication;
      this.blockSize = blockSize;
      this.buffersize = buffersize;
      this.progress = progress;
      if (progress != null) {
        LOG.debug("Set non-null progress callback on DFSOutputStream "+src);
      }
      
      int bytesPerChecksum = conf.getInt( "io.bytes.per.checksum", 512); 
      if ( bytesPerChecksum < 1 || blockSize % bytesPerChecksum != 0) {
        throw new IOException("io.bytes.per.checksum(" + bytesPerChecksum +
                              ") and blockSize(" + blockSize + 
                              ") do not match. " + "blockSize should be a " +
                              "multiple of io.bytes.per.checksum");
                              
      }
      
      checksum = DataChecksum.newDataChecksum(DataChecksum.CHECKSUM_CRC32, 
                                              bytesPerChecksum);
      namenode.create(
          src.toString(), clientName, overwrite, replication, blockSize);
    }

    private void openBackupStream() throws IOException {
      File tmpFile = newBackupFile();
      backupStream = new BufferedOutputStream(new FileOutputStream(tmpFile),
                                              buffersize);
      backupFile = tmpFile;
    }
    
    /* Wrapper for closing backupStream. This sets backupStream to null so
     * that we do not attempt to write to backupStream that could be
     * invalid in subsequent writes. Otherwise we might end trying to write
     * filedescriptor that we don't own.
     */
    private void closeBackupStream() throws IOException {
      if (backupStream != null) {
        backupStream.flush();
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
                
        LocatedBlock lb = locateFollowingBlock(startTime);
        block = lb.getBlock();
        if (block.getNumBytes() < bytesWrittenToBlock) {
          block.setNumBytes(bytesWrittenToBlock);
        }
        DatanodeInfo nodes[] = lb.getLocations();

        //
        // Connect to first DataNode in the list.  Abort if this fails.
        //
        InetSocketAddress target = NetUtils.createSocketAddr(nodes[0].getName());
        try {
          s = socketFactory.createSocket();
          s.connect(target, READ_TIMEOUT);
          s.setSoTimeout(replication * READ_TIMEOUT);
        } catch (IOException ie) {
          // Connection failed.  Let's wait a little bit and retry
          try {
            if (System.currentTimeMillis() - startTime > 5000) {
              LOG.info("Waiting to find target node: " + target);
            }
            Thread.sleep(6000);
          } catch (InterruptedException iex) {
          }
          namenode.abandonBlock(block, src.toString());
          retry = true;
          continue;
        }

        //
        // Xmit header info to datanode
        //
        DataOutputStream out = new DataOutputStream(new BufferedOutputStream(s.getOutputStream(), buffersize));
        out.writeShort( DATA_TRANFER_VERSION );
        out.write( OP_WRITE_BLOCK );
        out.writeLong( block.getBlockId() );
        out.writeInt( nodes.length - 1 );
        for (int i = 1; i < nodes.length; i++) {
          nodes[i].write(out);
        }
        checksum.writeHeader( out );
        blockStream = out;
        blockReplyStream = new DataInputStream(s.getInputStream());
      } while (retry);
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

    // @see FSOutputSummer#writeChunk()
    @Override
    protected void writeChunk(byte[] b, int offset, int len, byte[] checksum) 
                                                          throws IOException {
      checkOpen();
      int bytesPerChecksum = this.checksum.getBytesPerChecksum(); 
      if (len > bytesPerChecksum || (len + bytesWrittenToBlock) > blockSize) {
        // should never happen
        throw new IOException("Mismatch in writeChunk() args");
      }
      
      if ( backupFile == null ) {
        openBackupStream();
      }
      
      backupStream.write(b, offset, len);
      backupStream.write(checksum);
      
      bytesWrittenToBlock += len;
      filePos += len;
      
      if ( bytesWrittenToBlock >= blockSize ) {
        endBlock();
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
      int numSuccessfulWrites = 0;
            
      while (!sentOk) {
        nextBlockOutputStream();

        long bytesLeft = bytesWrittenToBlock;
        int bytesPerChecksum = checksum.getBytesPerChecksum();
        int checksumSize = checksum.getChecksumSize(); 
        byte buf[] = new byte[ bytesPerChecksum + checksumSize ];

        InputStream in = null;
        if ( bytesLeft > 0 ) { 
          in = new BufferedInputStream(new FileInputStream(backupFile),
                                       buffersize);
        }
        
        try {

          while ( bytesLeft > 0 ) {
            int len = (int) Math.min( bytesLeft, bytesPerChecksum );
            IOUtils.readFully( in, buf, 0, len + checksumSize);

            blockStream.writeInt( len );
            blockStream.write( buf, 0, len + checksumSize );

            bytesLeft -= len;

            if (progress != null) { progress.progress(); }
          }

          // write 0 to mark the end of a block
          blockStream.writeInt(0);
          blockStream.flush();
          
          numSuccessfulWrites++;

          //We should wait for response from the receiver.
          int reply = blockReplyStream.readShort();
          if ( reply == OP_STATUS_SUCCESS ||
              ( reply == OP_STATUS_ERROR_EXISTS &&
                  numSuccessfulWrites > 1 ) ) {
            s.close();
            s = null;
            sentOk = true;
          } else {
            throw new IOException( "Got error reply " + reply +
                                   " while writting the block " 
                                   + block );
          }

        } catch (IOException ie) {
          /*
           * The error could be OP_STATUS_ERROR_EXISTS.
           * We are not handling it properly here yet.
           * We should try to read a byte from blockReplyStream
           * wihtout blocking. 
           */
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
          if (in != null) {
            in.close();
          }
        }
      }

      bytesWrittenToBlock = 0;
      //
      // Delete local backup.
      //
      deleteBackupFile();
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
      //XXX Why are we abondoning the block? There could be retries left.
      namenode.abandonBlock(block, src.toString());
    }

    private void internalClose() throws IOException {
      // Clean up any resources that might be held.
      closed = true;
      
      synchronized (pendingCreates) {
        pendingCreates.remove(src);
      }
      
      if (s != null) {
        s.close();
        s = null;
      }
      
      closeBackupStream();
    }
    
    /**
     * Closes this output stream and releases any system 
     * resources associated with this stream.
     */
    @Override
    public synchronized void close() throws IOException {
      checkOpen();
      if (closed) {
        throw new IOException("Stream closed");
      }
      
      try {
        flushBuffer();
        
        if (filePos == 0 || bytesWrittenToBlock != 0) {
          try {
            endBlock();
          } catch (IOException e) {
            namenode.abandonFileInProgress(src.toString(), clientName);
            throw e;
          }
        }

        if (s != null) {
          s.close();
          s = null;
        }

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
      } finally {
        internalClose();
      }
    }
  }
  
  void reportChecksumFailure(String file, Block blk, DatanodeInfo dn) {
    DatanodeInfo [] dnArr = { dn };
    LocatedBlock [] lblocks = { new LocatedBlock(blk, dnArr) };
    reportChecksumFailure(file, lblocks);
  }
  
  // just reports checksum failure and ignores any exception during the report.
  void reportChecksumFailure(String file, LocatedBlock lblocks[]) {
    try {
      reportBadBlocks(lblocks);
    } catch (IOException ie) {
      LOG.info("Found corruption while reading " + file 
               + ".  Error repairing corrupt blocks.  Bad blocks remain. " 
               + StringUtils.stringifyException(ie));
    }
  }
}
