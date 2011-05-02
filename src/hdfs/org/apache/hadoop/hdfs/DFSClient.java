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

import org.apache.hadoop.io.*;
import org.apache.hadoop.io.retry.RetryPolicies;
import org.apache.hadoop.io.retry.RetryPolicy;
import org.apache.hadoop.io.retry.RetryProxy;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.ipc.*;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.net.NodeBase;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.hdfs.DistributedFileSystem.DiskStatus;
import org.apache.hadoop.hdfs.protocol.*;
import org.apache.hadoop.hdfs.protocol.DataTransferProtocol.PipelineAck;
import org.apache.hadoop.hdfs.security.token.block.InvalidBlockTokenException;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenIdentifier;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.hdfs.server.common.HdfsConstants;
import org.apache.hadoop.hdfs.server.common.UpgradeStatusReport;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.namenode.NotReplicatedYetException;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.SecretManager.InvalidToken;
import org.apache.hadoop.util.*;

import org.apache.commons.logging.*;

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.zip.CRC32;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.ConcurrentHashMap;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;

import javax.net.SocketFactory;

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
public class DFSClient implements FSConstants, java.io.Closeable {
  public static final Log LOG = LogFactory.getLog(DFSClient.class);
  public static final int MAX_BLOCK_ACQUIRE_FAILURES = 3;
  private static final int TCP_WINDOW_SIZE = 128 * 1024; // 128 KB
  public final ClientProtocol namenode;
  private final ClientProtocol rpcNamenode;
  final UserGroupInformation ugi;
  volatile boolean clientRunning = true;
  Random r = new Random();
  final String clientName;
  final LeaseChecker leasechecker = new LeaseChecker();
  private Configuration conf;
  private long defaultBlockSize;
  private short defaultReplication;
  private SocketFactory socketFactory;
  private int socketTimeout;
  private int datanodeWriteTimeout;
  final int writePacketSize;
  private final FileSystem.Statistics stats;
  private int maxBlockAcquireFailures;
    
 
  public static ClientProtocol createNamenode(Configuration conf) throws IOException {
    return createNamenode(NameNode.getAddress(conf), conf);
  }

  public static ClientProtocol createNamenode( InetSocketAddress nameNodeAddr,
      Configuration conf) throws IOException {
    return createNamenode(createRPCNamenode(nameNodeAddr, conf,
      UserGroupInformation.getCurrentUser()));
  }

  private static ClientProtocol createRPCNamenode(InetSocketAddress nameNodeAddr,
      Configuration conf, UserGroupInformation ugi) 
    throws IOException {
    return (ClientProtocol)RPC.getProxy(ClientProtocol.class,
        ClientProtocol.versionID, nameNodeAddr, ugi, conf,
        NetUtils.getSocketFactory(conf, ClientProtocol.class));
  }

  private static ClientProtocol createNamenode(ClientProtocol rpcNamenode)
    throws IOException {
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
    RetryPolicy methodPolicy = RetryPolicies.retryByException(
        RetryPolicies.TRY_ONCE_THEN_FAIL, exceptionToPolicyMap);
    Map<String,RetryPolicy> methodNameToPolicyMap = new HashMap<String,RetryPolicy>();
    
    methodNameToPolicyMap.put("create", methodPolicy);

    return (ClientProtocol) RetryProxy.create(ClientProtocol.class,
        rpcNamenode, methodNameToPolicyMap);
  }

  static ClientDatanodeProtocol createClientDatanodeProtocolProxy (
      DatanodeID datanodeid, Configuration conf, 
      Block block, Token<BlockTokenIdentifier> token) throws IOException {
    InetSocketAddress addr = NetUtils.createSocketAddr(
      datanodeid.getHost() + ":" + datanodeid.getIpcPort());
    if (ClientDatanodeProtocol.LOG.isDebugEnabled()) {
      ClientDatanodeProtocol.LOG.info("ClientDatanodeProtocol addr=" + addr);
    }
    UserGroupInformation ticket = UserGroupInformation
        .createRemoteUser(block.toString());
    ticket.addToken(token);
    return (ClientDatanodeProtocol)RPC.getProxy(ClientDatanodeProtocol.class,
        ClientDatanodeProtocol.versionID, addr, ticket, conf, NetUtils
        .getDefaultSocketFactory(conf));
  }
        
  /**
   * Same as this(NameNode.getAddress(conf), conf);
   * @see #DFSClient(InetSocketAddress, Configuration)
   */
  public DFSClient(Configuration conf) throws IOException {
    this(NameNode.getAddress(conf), conf);
  }

  /**
   * Same as this(nameNodeAddr, conf, null);
   * @see #DFSClient(InetSocketAddress, Configuration, org.apache.hadoop.fs.FileSystem.Statistics)
   */
  public DFSClient(InetSocketAddress nameNodeAddr, Configuration conf
      ) throws IOException {
    this(nameNodeAddr, conf, null);
  }

  /**
   * Same as this(nameNodeAddr, null, conf, stats);
   * @see #DFSClient(InetSocketAddress, ClientProtocol, Configuration, org.apache.hadoop.fs.FileSystem.Statistics) 
   */
  public DFSClient(InetSocketAddress nameNodeAddr, Configuration conf,
                   FileSystem.Statistics stats)
    throws IOException {
    this(nameNodeAddr, null, conf, stats);
  }

  /** 
   * Create a new DFSClient connected to the given nameNodeAddr or rpcNamenode.
   * Exactly one of nameNodeAddr or rpcNamenode must be null.
   */
  DFSClient(InetSocketAddress nameNodeAddr, ClientProtocol rpcNamenode,
      Configuration conf, FileSystem.Statistics stats)
    throws IOException {
    this.conf = conf;
    this.stats = stats;
    this.socketTimeout = conf.getInt("dfs.socket.timeout", 
                                     HdfsConstants.READ_TIMEOUT);
    this.datanodeWriteTimeout = conf.getInt("dfs.datanode.socket.write.timeout",
                                            HdfsConstants.WRITE_TIMEOUT);
    this.socketFactory = NetUtils.getSocketFactory(conf, ClientProtocol.class);
    // dfs.write.packet.size is an internal config variable
    this.writePacketSize = conf.getInt("dfs.write.packet.size", 64*1024);
    this.maxBlockAcquireFailures = getMaxBlockAcquireFailures(conf);
    
    ugi = UserGroupInformation.getCurrentUser();

    String taskId = conf.get("mapred.task.id");
    if (taskId != null) {
      this.clientName = "DFSClient_" + taskId; 
    } else {
      this.clientName = "DFSClient_" + r.nextInt();
    }
    defaultBlockSize = conf.getLong("dfs.block.size", DEFAULT_BLOCK_SIZE);
    defaultReplication = (short) conf.getInt("dfs.replication", 3);

    if (nameNodeAddr != null && rpcNamenode == null) {
      this.rpcNamenode = createRPCNamenode(nameNodeAddr, conf, ugi);
      this.namenode = createNamenode(this.rpcNamenode);
    } else if (nameNodeAddr == null && rpcNamenode != null) {
      //This case is used for testing.
      this.namenode = this.rpcNamenode = rpcNamenode;
    } else {
      throw new IllegalArgumentException(
          "Expecting exactly one of nameNodeAddr and rpcNamenode being null: "
          + "nameNodeAddr=" + nameNodeAddr + ", rpcNamenode=" + rpcNamenode);
    }
  }

  static int getMaxBlockAcquireFailures(Configuration conf) {
    return conf.getInt("dfs.client.max.block.acquire.failures",
                       MAX_BLOCK_ACQUIRE_FAILURES);
  }

  private void checkOpen() throws IOException {
    if (!clientRunning) {
      IOException result = new IOException("Filesystem closed");
      throw result;
    }
  }
    
  /**
   * Close the file system, abandoning all of the leases and files being
   * created and close connections to the namenode.
   */
  public synchronized void close() throws IOException {
    if(clientRunning) {
      leasechecker.close();
      clientRunning = false;
      try {
        leasechecker.interruptAndJoin();
      } catch (InterruptedException ie) {
      }
  
      // close connections to the namenode
      RPC.stopProxy(rpcNamenode);
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

  /** A test method for printing out tokens */
  public static String stringifyToken(Token<DelegationTokenIdentifier> token
                                      ) throws IOException {
    DelegationTokenIdentifier ident = new DelegationTokenIdentifier();
    ByteArrayInputStream buf = new ByteArrayInputStream(token.getIdentifier());
    DataInputStream in = new DataInputStream(buf);  
    ident.readFields(in);
    String str = ident.getKind() + " token " + ident.getSequenceNumber() + 
                 " for " + ident.getUser().getShortUserName();
    if (token.getService().getLength() > 0) {
      return (str + " on " + token.getService());
    } else {
      return str;
    }
  }

  public Token<DelegationTokenIdentifier> getDelegationToken(Text renewer)
      throws IOException {
    Token<DelegationTokenIdentifier> result =
      namenode.getDelegationToken(renewer);
    LOG.info("Created " + stringifyToken(result));
    return result;
  }

  public long renewDelegationToken(Token<DelegationTokenIdentifier> token)
      throws InvalidToken, IOException {
    try {
      LOG.info("Renewing " + stringifyToken(token));
      return namenode.renewDelegationToken(token);
    } catch (RemoteException re) {
      throw re.unwrapRemoteException(InvalidToken.class,
                                     AccessControlException.class);
    }
  }

  public void cancelDelegationToken(Token<DelegationTokenIdentifier> token)
      throws InvalidToken, IOException {
    try {
      LOG.info("Cancelling " + stringifyToken(token));
      namenode.cancelDelegationToken(token);
    } catch (RemoteException re) {
      throw re.unwrapRemoteException(InvalidToken.class,
                                     AccessControlException.class);
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
   *  @deprecated Use getBlockLocations instead
   *
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
  @Deprecated
  public String[][] getHints(String src, long start, long length) 
    throws IOException {
    BlockLocation[] blkLocations = getBlockLocations(src, start, length);
    if ((blkLocations == null) || (blkLocations.length == 0)) {
      return new String[0][];
    }
    int blkCount = blkLocations.length;
    String[][]hints = new String[blkCount][];
    for (int i=0; i < blkCount ; i++) {
      String[] hosts = blkLocations[i].getHosts();
      hints[i] = new String[hosts.length];
      hints[i] = hosts;
    }
    return hints;
  }

  private static LocatedBlocks callGetBlockLocations(ClientProtocol namenode,
      String src, long start, long length) throws IOException {
    try {
      return namenode.getBlockLocations(src, start, length);
    } catch(RemoteException re) {
      throw re.unwrapRemoteException(AccessControlException.class,
                                    FileNotFoundException.class);
    }
  }

  /**
   * Get block location info about file
   * 
   * getBlockLocations() returns a list of hostnames that store 
   * data for a specific file region.  It returns a set of hostnames
   * for every block within the indicated region.
   *
   * This function is very useful when writing code that considers
   * data-placement when performing operations.  For example, the
   * MapReduce system tries to schedule tasks on the same machines
   * as the data-block the task processes. 
   */
  public BlockLocation[] getBlockLocations(String src, long start, 
    long length) throws IOException {
    LocatedBlocks blocks = callGetBlockLocations(namenode, src, start, length);
    if (blocks == null) {
      return new BlockLocation[0];
    }
    int nrBlocks = blocks.locatedBlockCount();
    BlockLocation[] blkLocations = new BlockLocation[nrBlocks];
    int idx = 0;
    for (LocatedBlock blk : blocks.getLocatedBlocks()) {
      assert idx < nrBlocks : "Incorrect index";
      DatanodeInfo[] locations = blk.getLocations();
      String[] hosts = new String[locations.length];
      String[] names = new String[locations.length];
      String[] racks = new String[locations.length];
      for (int hCnt = 0; hCnt < locations.length; hCnt++) {
        hosts[hCnt] = locations[hCnt].getHostName();
        names[hCnt] = locations[hCnt].getName();
        NodeBase node = new NodeBase(names[hCnt], 
                                     locations[hCnt].getNetworkLocation());
        racks[hCnt] = node.toString();
      }
      blkLocations[idx] = new BlockLocation(names, hosts, racks,
                                            blk.getStartOffset(),
                                            blk.getBlockSize());
      idx++;
    }
    return blkLocations;
  }

  public DFSInputStream open(String src) throws IOException {
    return open(src, conf.getInt("io.file.buffer.size", 4096), true, null);
  }

  /**
   * Create an input stream that obtains a nodelist from the
   * namenode, and then reads from all the right places.  Creates
   * inner subclass of InputStream that does the right out-of-band
   * work.
   */
  DFSInputStream open(String src, int buffersize, boolean verifyChecksum,
                      FileSystem.Statistics stats
      ) throws IOException {
    checkOpen();
    //    Get block info from namenode
    return new DFSInputStream(src, buffersize, verifyChecksum);
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
   * Call
   * {@link #create(String,FsPermission,boolean,short,long,Progressable,int)}
   * with default permission.
   * @see FsPermission#getDefault()
   */
  public OutputStream create(String src,
      boolean overwrite,
      short replication,
      long blockSize,
      Progressable progress,
      int buffersize
      ) throws IOException {
    return create(src, FsPermission.getDefault(),
        overwrite, replication, blockSize, progress, buffersize);
  }
  /**
   * Create a new dfs file with the specified block replication 
   * with write-progress reporting and return an output stream for writing
   * into the file.  
   * 
   * @param src stream name
   * @param permission The permission of the directory being created.
   * If permission == null, use {@link FsPermission#getDefault()}.
   * @param overwrite do not check for file existence if true
   * @param replication block replication
   * @return output stream
   * @throws IOException
   * @see ClientProtocol#create(String, FsPermission, String, boolean, short, long)
   */
  public OutputStream create(String src, 
                             FsPermission permission,
                             boolean overwrite, 
                             short replication,
                             long blockSize,
                             Progressable progress,
                             int buffersize
                             ) throws IOException {
    checkOpen();
    if (permission == null) {
      permission = FsPermission.getDefault();
    }
    FsPermission masked = permission.applyUMask(FsPermission.getUMask(conf));
    LOG.debug(src + ": masked=" + masked);
    OutputStream result = new DFSOutputStream(src, masked,
        overwrite, replication, blockSize, progress, buffersize,
        conf.getInt("io.bytes.per.checksum", 512));
    leasechecker.put(src, result);
    return result;
  }

  /**
   * Append to an existing HDFS file.  
   * 
   * @param src file name
   * @param buffersize buffer size
   * @param progress for reporting write-progress
   * @return an output stream for writing into the file
   * @throws IOException
   * @see ClientProtocol#append(String, String)
   */
  OutputStream append(String src, int buffersize, Progressable progress
      ) throws IOException {
    checkOpen();
    HdfsFileStatus stat = null;
    LocatedBlock lastBlock = null;
    try {
      stat = getFileInfo(src);
      lastBlock = namenode.append(src, clientName);
    } catch(RemoteException re) {
      throw re.unwrapRemoteException(FileNotFoundException.class,
                                     AccessControlException.class,
                                     NSQuotaExceededException.class,
                                     DSQuotaExceededException.class);
    }
    OutputStream result = new DFSOutputStream(src, buffersize, progress,
        lastBlock, stat, conf.getInt("io.bytes.per.checksum", 512));
    leasechecker.put(src, result);
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
    try {
      return namenode.setReplication(src, replication);
    } catch(RemoteException re) {
      throw re.unwrapRemoteException(AccessControlException.class,
                                     NSQuotaExceededException.class,
                                     DSQuotaExceededException.class);
    }
  }

  /**
   * Rename file or directory.
   * See {@link ClientProtocol#rename(String, String)}. 
   */
  public boolean rename(String src, String dst) throws IOException {
    checkOpen();
    try {
      return namenode.rename(src, dst);
    } catch(RemoteException re) {
      throw re.unwrapRemoteException(AccessControlException.class,
                                     NSQuotaExceededException.class,
                                     DSQuotaExceededException.class);
    }
  }

  /**
   * Delete file or directory.
   * See {@link ClientProtocol#delete(String)}. 
   */
  @Deprecated
  public boolean delete(String src) throws IOException {
    checkOpen();
    return namenode.delete(src, true);
  }

  /**
   * delete file or directory.
   * delete contents of the directory if non empty and recursive 
   * set to true
   */
  public boolean delete(String src, boolean recursive) throws IOException {
    checkOpen();
    try {
      return namenode.delete(src, recursive);
    } catch(RemoteException re) {
      throw re.unwrapRemoteException(AccessControlException.class);
    }
  }
  
  /** Implemented using getFileInfo(src)
   */
  public boolean exists(String src) throws IOException {
    checkOpen();
    return getFileInfo(src) != null;
  }

  /** @deprecated Use getHdfsFileStatus() instead */
  @Deprecated
  public boolean isDirectory(String src) throws IOException {
    HdfsFileStatus fs = getFileInfo(src);
    if (fs != null)
      return fs.isDir();
    else
      throw new FileNotFoundException("File does not exist: " + src);
  }

  /**
   * Get a partial listing of the indicated directory
   * 
   * Recommend to use HdfsFileStatus.EMPTY_NAME as startAfter 
   * if the application wants to fetch a listing starting from 
   * the first entry in the directory
   * 
   * @param src the directory name
   * @param startAfter the name to start listing after
   * @return a partial listing starting after startAfter 
   */
  public DirectoryListing listPaths(String src, byte[] startAfter)
  throws IOException {
    checkOpen();
    try {
      return namenode.getListing(src, startAfter);
    } catch(RemoteException re) {
      throw re.unwrapRemoteException(AccessControlException.class);
    }
  }

  public HdfsFileStatus getFileInfo(String src) throws IOException {
    checkOpen();
    try {
      return namenode.getFileInfo(src);
    } catch(RemoteException re) {
      throw re.unwrapRemoteException(AccessControlException.class);
    }
  }

  /**
   * Get the checksum of a file.
   * @param src The file path
   * @return The checksum 
   * @see DistributedFileSystem#getFileChecksum(Path)
   */
  MD5MD5CRC32FileChecksum getFileChecksum(String src) throws IOException {
    checkOpen();
    return getFileChecksum(src, namenode, socketFactory, socketTimeout);    
  }

  /**
   * Get the checksum of a file.
   * @param src The file path
   * @return The checksum 
   */
  public static MD5MD5CRC32FileChecksum getFileChecksum(String src,
      ClientProtocol namenode, SocketFactory socketFactory, int socketTimeout
      ) throws IOException {
    //get all block locations
    List<LocatedBlock> locatedblocks
        = callGetBlockLocations(namenode, src, 0, Long.MAX_VALUE).getLocatedBlocks();
    final DataOutputBuffer md5out = new DataOutputBuffer();
    int bytesPerCRC = 0;
    long crcPerBlock = 0;
    boolean refetchBlocks = false;
    int lastRetriedIndex = -1;

    //get block checksum for each block
    for(int i = 0; i < locatedblocks.size(); i++) {
      if (refetchBlocks) {  // refetch to get fresh tokens
        locatedblocks = callGetBlockLocations(namenode, src, 0, Long.MAX_VALUE)
            .getLocatedBlocks();
        refetchBlocks = false;
      }
      LocatedBlock lb = locatedblocks.get(i);
      final Block block = lb.getBlock();
      final DatanodeInfo[] datanodes = lb.getLocations();
      
      //try each datanode location of the block
      final int timeout = 3000 * datanodes.length + socketTimeout;
      boolean done = false;
      for(int j = 0; !done && j < datanodes.length; j++) {
        //connect to a datanode
        final Socket sock = socketFactory.createSocket();
        NetUtils.connect(sock, 
                         NetUtils.createSocketAddr(datanodes[j].getName()),
                         timeout);
        sock.setSoTimeout(timeout);

        DataOutputStream out = new DataOutputStream(
            new BufferedOutputStream(NetUtils.getOutputStream(sock), 
                                     DataNode.SMALL_BUFFER_SIZE));
        DataInputStream in = new DataInputStream(NetUtils.getInputStream(sock));

        // get block MD5
        try {
          if (LOG.isDebugEnabled()) {
            LOG.debug("write to " + datanodes[j].getName() + ": "
                + DataTransferProtocol.OP_BLOCK_CHECKSUM +
                ", block=" + block);
          }
          out.writeShort(DataTransferProtocol.DATA_TRANSFER_VERSION);
          out.write(DataTransferProtocol.OP_BLOCK_CHECKSUM);
          out.writeLong(block.getBlockId());
          out.writeLong(block.getGenerationStamp());
          lb.getBlockToken().write(out);
          out.flush();
         
          final short reply = in.readShort();
          if (reply != DataTransferProtocol.OP_STATUS_SUCCESS) {
            if (reply == DataTransferProtocol.OP_STATUS_ERROR_ACCESS_TOKEN
                && i > lastRetriedIndex) {
              if (LOG.isDebugEnabled()) {
                LOG.debug("Got access token error in response to OP_BLOCK_CHECKSUM "
                    + "for file " + src + " for block " + block
                    + " from datanode " + datanodes[j].getName()
                    + ". Will retry the block once.");
              }
              lastRetriedIndex = i;
              done = true; // actually it's not done; but we'll retry
              i--; // repeat at i-th block
              refetchBlocks = true;
              break;
            } else {
              throw new IOException("Bad response " + reply + " for block "
                  + block + " from datanode " + datanodes[j].getName());
            }
          }

          //read byte-per-checksum
          final int bpc = in.readInt(); 
          if (i == 0) { //first block
            bytesPerCRC = bpc;
          }
          else if (bpc != bytesPerCRC) {
            throw new IOException("Byte-per-checksum not matched: bpc=" + bpc
                + " but bytesPerCRC=" + bytesPerCRC);
          }
          
          //read crc-per-block
          final long cpb = in.readLong();
          if (locatedblocks.size() > 1 && i == 0) {
            crcPerBlock = cpb;
          }

          //read md5
          final MD5Hash md5 = MD5Hash.read(in);
          md5.write(md5out);
          
          done = true;

          if (LOG.isDebugEnabled()) {
            if (i == 0) {
              LOG.debug("set bytesPerCRC=" + bytesPerCRC
                  + ", crcPerBlock=" + crcPerBlock);
            }
            LOG.debug("got reply from " + datanodes[j].getName()
                + ": md5=" + md5);
          }
        } catch (IOException ie) {
          LOG.warn("src=" + src + ", datanodes[" + j + "].getName()="
              + datanodes[j].getName(), ie);
        } finally {
          IOUtils.closeStream(in);
          IOUtils.closeStream(out);
          IOUtils.closeSocket(sock);        
        }
      }

      if (!done) {
        throw new IOException("Fail to get block MD5 for " + block);
      }
    }

    //compute file MD5
    final MD5Hash fileMD5 = MD5Hash.digest(md5out.getData()); 
    return new MD5MD5CRC32FileChecksum(bytesPerCRC, crcPerBlock, fileMD5);
  }

  /**
   * Set permissions to a file or directory.
   * @param src path name.
   * @param permission
   * @throws <code>FileNotFoundException</code> is file does not exist.
   */
  public void setPermission(String src, FsPermission permission
                            ) throws IOException {
    checkOpen();
    try {
      namenode.setPermission(src, permission);
    } catch(RemoteException re) {
      throw re.unwrapRemoteException(AccessControlException.class,
                                     FileNotFoundException.class);
    }
  }

  /**
   * Set file or directory owner.
   * @param src path name.
   * @param username user id.
   * @param groupname user group.
   * @throws <code>FileNotFoundException</code> is file does not exist.
   */
  public void setOwner(String src, String username, String groupname
                      ) throws IOException {
    checkOpen();
    try {
      namenode.setOwner(src, username, groupname);
    } catch(RemoteException re) {
      throw re.unwrapRemoteException(AccessControlException.class,
                                     FileNotFoundException.class);
    }
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

  /**
   * Returns count of blocks with no good replicas left. Normally should be 
   * zero.
   * @throws IOException
   */ 
  public long getMissingBlocksCount() throws IOException {
    return namenode.getStats()[ClientProtocol.GET_STATS_MISSING_BLOCKS_IDX];
  }
  
  /**
   * Returns count of blocks with one of more replica missing.
   * @throws IOException
   */ 
  public long getUnderReplicatedBlocksCount() throws IOException {
    return namenode.getStats()[ClientProtocol.GET_STATS_UNDER_REPLICATED_IDX];
  }
  
  /**
   * Returns count of blocks with at least one replica marked corrupt. 
   * @throws IOException
   */ 
  public long getCorruptBlocksCount() throws IOException {
    return namenode.getStats()[ClientProtocol.GET_STATS_CORRUPT_BLOCKS_IDX];
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
   * Save namespace image.
   * See {@link ClientProtocol#saveNamespace()} 
   * for more details.
   * 
   * @see ClientProtocol#saveNamespace()
   */
  void saveNamespace() throws AccessControlException, IOException {
    try {
      namenode.saveNamespace();
    } catch(RemoteException re) {
      throw re.unwrapRemoteException(AccessControlException.class);
    }
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
    return mkdirs(src, null);
  }

  /**
   * Create a directory (or hierarchy of directories) with the given
   * name and permission.
   *
   * @param src The path of the directory being created
   * @param permission The permission of the directory being created.
   * If permission == null, use {@link FsPermission#getDefault()}.
   * @return True if the operation success.
   * @see ClientProtocol#mkdirs(String, FsPermission)
   */
  public boolean mkdirs(String src, FsPermission permission)throws IOException{
    checkOpen();
    if (permission == null) {
      permission = FsPermission.getDefault();
    }
    FsPermission masked = permission.applyUMask(FsPermission.getUMask(conf));
    LOG.debug(src + ": masked=" + masked);
    try {
      return namenode.mkdirs(src, masked);
    } catch(RemoteException re) {
      throw re.unwrapRemoteException(AccessControlException.class,
                                     NSQuotaExceededException.class,
                                     DSQuotaExceededException.class);
    }
  }

  ContentSummary getContentSummary(String src) throws IOException {
    try {
      return namenode.getContentSummary(src);
    } catch(RemoteException re) {
      throw re.unwrapRemoteException(AccessControlException.class,
                                     FileNotFoundException.class);
    }
  }

  /**
   * Sets or resets quotas for a directory.
   * @see org.apache.hadoop.hdfs.protocol.ClientProtocol#setQuota(String, long, long)
   */
  void setQuota(String src, long namespaceQuota, long diskspaceQuota) 
                                                 throws IOException {
    // sanity check
    if ((namespaceQuota <= 0 && namespaceQuota != FSConstants.QUOTA_DONT_SET &&
         namespaceQuota != FSConstants.QUOTA_RESET) ||
        (diskspaceQuota <= 0 && diskspaceQuota != FSConstants.QUOTA_DONT_SET &&
         diskspaceQuota != FSConstants.QUOTA_RESET)) {
      throw new IllegalArgumentException("Invalid values for quota : " +
                                         namespaceQuota + " and " + 
                                         diskspaceQuota);
                                         
    }
    
    try {
      namenode.setQuota(src, namespaceQuota, diskspaceQuota);
    } catch(RemoteException re) {
      throw re.unwrapRemoteException(AccessControlException.class,
                                     FileNotFoundException.class,
                                     NSQuotaExceededException.class,
                                     DSQuotaExceededException.class);
    }
  }

  /**
   * set the modification and access time of a file
   * @throws FileNotFoundException if the path is not a file
   */
  public void setTimes(String src, long mtime, long atime) throws IOException {
    checkOpen();
    try {
      namenode.setTimes(src, mtime, atime);
    } catch(RemoteException re) {
      throw re.unwrapRemoteException(AccessControlException.class,
                                     FileNotFoundException.class);
    }
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

  boolean isLeaseCheckerStarted() {
    return leasechecker.daemon != null;
  }

  /** Lease management*/
  class LeaseChecker implements Runnable {
    /** A map from src -> DFSOutputStream of files that are currently being
     * written by this client.
     */
    private final SortedMap<String, OutputStream> pendingCreates
        = new TreeMap<String, OutputStream>();

    private Daemon daemon = null;
    
    synchronized void put(String src, OutputStream out) {
      if (clientRunning) {
        if (daemon == null) {
          daemon = new Daemon(this);
          daemon.start();
        }
        pendingCreates.put(src, out);
      }
    }
    
    synchronized void remove(String src) {
      pendingCreates.remove(src);
    }
    
    void interruptAndJoin() throws InterruptedException {
      Daemon daemonCopy = null;
      synchronized (this) {
        if (daemon != null) {
          daemon.interrupt();
          daemonCopy = daemon;
        }
      }
     
      if (daemonCopy != null) {
        LOG.debug("Wait for lease checker to terminate");
        daemonCopy.join();
      }
    }

    void close() {
      while (true) {
        String src;
        OutputStream out;
        synchronized (this) {
          if (pendingCreates.isEmpty()) {
            return;
          }
          src = pendingCreates.firstKey();
          out = pendingCreates.remove(src);
        }
        if (out != null) {
          try {
            out.close();
          } catch (IOException ie) {
            LOG.error("Exception closing file " + src+ " : " + ie, ie);
          }
        }
      }
    }

    private void renew() throws IOException {
      synchronized(this) {
        if (pendingCreates.isEmpty()) {
          return;
        }
      }
      namenode.renewLease(clientName);
    }

    /**
     * Periodically check in with the namenode and renew all the leases
     * when the lease period is half over.
     */
    public void run() {
      long lastRenewed = 0;
      while (clientRunning && !Thread.interrupted()) {
        if (System.currentTimeMillis() - lastRenewed > (LEASE_SOFTLIMIT_PERIOD / 2)) {
          try {
            renew();
            lastRenewed = System.currentTimeMillis();
          } catch (IOException ie) {
            LOG.warn("Problem renewing lease for " + clientName, ie);
          }
        }

        try {
          Thread.sleep(1000);
        } catch (InterruptedException ie) {
          if (LOG.isDebugEnabled()) {
            LOG.debug(this + " is interrupted.", ie);
          }
          return;
        }
      }
    }

    /** {@inheritDoc} */
    public String toString() {
      String s = getClass().getSimpleName();
      if (LOG.isTraceEnabled()) {
        return s + "@" + DFSClient.this + ": "
               + StringUtils.stringifyException(new Throwable("for testing"));
      }
      return s;
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
  public static class BlockReader extends FSInputChecker {

    private Socket dnSock; //for now just sending checksumOk.
    private DataInputStream in;
    private DataChecksum checksum;
    private long lastChunkOffset = -1;
    private long lastChunkLen = -1;
    private long lastSeqNo = -1;

    private long startOffset;
    private long firstChunkOffset;
    private int bytesPerChecksum;
    private int checksumSize;
    private boolean gotEOS = false;
    
    byte[] skipBuf = null;
    ByteBuffer checksumBytes = null;
    int dataLeft = 0;
    boolean isLastPacket = false;
    
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
      
      boolean eosBefore = gotEOS;
      int nRead = super.read(buf, off, len);
      
      // if gotEOS was set in the previous read and checksum is enabled :
      if (gotEOS && !eosBefore && nRead >= 0 && needChecksum()) {
        //checksum is verified and there are no errors.
        checksumOk(dnSock);
      }
      return nRead;
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
    
    /**
     * Makes sure that checksumBytes has enough capacity 
     * and limit is set to the number of checksum bytes needed 
     * to be read.
     */
    private void adjustChecksumBytes(int dataLen) {
      int requiredSize = 
        ((dataLen + bytesPerChecksum - 1)/bytesPerChecksum)*checksumSize;
      if (checksumBytes == null || requiredSize > checksumBytes.capacity()) {
        checksumBytes =  ByteBuffer.wrap(new byte[requiredSize]);
      } else {
        checksumBytes.clear();
      }
      checksumBytes.limit(requiredSize);
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

      // Read next packet if the previous packet has been read completely.
      if (dataLeft <= 0) {
        //Read packet headers.
        int packetLen = in.readInt();
        long offsetInBlock = in.readLong();
        long seqno = in.readLong();
        boolean lastPacketInBlock = in.readBoolean();
      
        if (LOG.isDebugEnabled()) {
          LOG.debug("DFSClient readChunk got seqno " + seqno +
                    " offsetInBlock " + offsetInBlock +
                    " lastPacketInBlock " + lastPacketInBlock +
                    " packetLen " + packetLen);
        }
        
        int dataLen = in.readInt();
      
        // Sanity check the lengths
        if ( dataLen < 0 || 
             ( (dataLen % bytesPerChecksum) != 0 && !lastPacketInBlock ) ||
             (seqno != (lastSeqNo + 1)) ) {
             throw new IOException("BlockReader: error in packet header" +
                                   "(chunkOffset : " + chunkOffset + 
                                   ", dataLen : " + dataLen +
                                   ", seqno : " + seqno + 
                                   " (last: " + lastSeqNo + "))");
        }
        
        lastSeqNo = seqno;
        isLastPacket = lastPacketInBlock;
        dataLeft = dataLen;
        adjustChecksumBytes(dataLen);
        if (dataLen > 0) {
          IOUtils.readFully(in, checksumBytes.array(), 0,
                            checksumBytes.limit());
        }
      }

      int chunkLen = Math.min(dataLeft, bytesPerChecksum);
      
      if ( chunkLen > 0 ) {
        // len should be >= chunkLen
        IOUtils.readFully(in, buf, offset, chunkLen);
        checksumBytes.get(checksumBuf, 0, checksumSize);
      }
      
      dataLeft -= chunkLen;
      lastChunkOffset = chunkOffset;
      lastChunkLen = chunkLen;
      
      if ((dataLeft == 0 && isLastPacket) || chunkLen == 0) {
        gotEOS = true;
      }
      if ( chunkLen == 0 ) {
        return -1;
      }
      
      return chunkLen;
    }
    
    private BlockReader( String file, long blockId, DataInputStream in, 
                         DataChecksum checksum, boolean verifyChecksum,
                         long startOffset, long firstChunkOffset, 
                         Socket dnSock ) {
      super(new Path("/blk_" + blockId + ":of:" + file)/*too non path-like?*/,
            1, verifyChecksum,
            checksum.getChecksumSize() > 0? checksum : null, 
            checksum.getBytesPerChecksum(),
            checksum.getChecksumSize());
      
      this.dnSock = dnSock;
      this.in = in;
      this.checksum = checksum;
      this.startOffset = Math.max( startOffset, 0 );

      this.firstChunkOffset = firstChunkOffset;
      lastChunkOffset = firstChunkOffset;
      lastChunkLen = -1;

      bytesPerChecksum = this.checksum.getBytesPerChecksum();
      checksumSize = this.checksum.getChecksumSize();
    }

    public static BlockReader newBlockReader(Socket sock, String file, long blockId, Token<BlockTokenIdentifier> accessToken, 
        long genStamp, long startOffset, long len, int bufferSize) throws IOException {
      return newBlockReader(sock, file, blockId, accessToken, genStamp, startOffset, len, bufferSize,
          true);
    }

    /** Java Doc required */
    public static BlockReader newBlockReader( Socket sock, String file, long blockId, 
                                       Token<BlockTokenIdentifier> accessToken,
                                       long genStamp,
                                       long startOffset, long len,
                                       int bufferSize, boolean verifyChecksum)
                                       throws IOException {
      return newBlockReader(sock, file, blockId, accessToken, genStamp, startOffset,
                            len, bufferSize, verifyChecksum, "");
    }

    public static BlockReader newBlockReader( Socket sock, String file,
                                       long blockId, 
                                       Token<BlockTokenIdentifier> accessToken,
                                       long genStamp,
                                       long startOffset, long len,
                                       int bufferSize, boolean verifyChecksum,
                                       String clientName)
                                       throws IOException {
      // in and out will be closed when sock is closed (by the caller)
      DataOutputStream out = new DataOutputStream(
        new BufferedOutputStream(NetUtils.getOutputStream(sock,HdfsConstants.WRITE_TIMEOUT)));

      //write the header.
      out.writeShort( DataTransferProtocol.DATA_TRANSFER_VERSION );
      out.write( DataTransferProtocol.OP_READ_BLOCK );
      out.writeLong( blockId );
      out.writeLong( genStamp );
      out.writeLong( startOffset );
      out.writeLong( len );
      Text.writeString(out, clientName);
      accessToken.write(out);
      out.flush();
      
      //
      // Get bytes in block, set streams
      //

      DataInputStream in = new DataInputStream(
          new BufferedInputStream(NetUtils.getInputStream(sock), 
                                  bufferSize));
      
      short status = in.readShort();
      if (status != DataTransferProtocol.OP_STATUS_SUCCESS) {
        if (status == DataTransferProtocol.OP_STATUS_ERROR_ACCESS_TOKEN) {
          throw new InvalidBlockTokenException(
              "Got access token error for OP_READ_BLOCK, self="
                  + sock.getLocalSocketAddress() + ", remote="
                  + sock.getRemoteSocketAddress() + ", for file " + file
                  + ", for block " + blockId + "_" + genStamp);
        } else {
          throw new IOException("Got error for OP_READ_BLOCK, self="
              + sock.getLocalSocketAddress() + ", remote="
              + sock.getRemoteSocketAddress() + ", for file " + file
              + ", for block " + blockId + "_" + genStamp);
        }
      }
      DataChecksum checksum = DataChecksum.newDataChecksum( in );
      //Warning when we get CHECKSUM_NULL?
      
      // Read the first chunk offset.
      long firstChunkOffset = in.readLong();
      
      if ( firstChunkOffset < 0 || firstChunkOffset > startOffset ||
          firstChunkOffset >= (startOffset + checksum.getBytesPerChecksum())) {
        throw new IOException("BlockReader: error in first chunk offset (" +
                              firstChunkOffset + ") startOffset is " + 
                              startOffset + " for file " + file);
      }

      return new BlockReader( file, blockId, in, checksum, verifyChecksum,
                              startOffset, firstChunkOffset, sock );
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
    public int readAll(byte[] buf, int offset, int len) throws IOException {
      return readFully(this, buf, offset, len);
    }
    
    /* When the reader reaches end of a block and there are no checksum
     * errors, we send OP_STATUS_CHECKSUM_OK to datanode to inform that 
     * checksum was verified and there was no error.
     */ 
    private void checksumOk(Socket sock) {
      try {
        OutputStream out = NetUtils.getOutputStream(sock, HdfsConstants.WRITE_TIMEOUT);
        byte buf[] = { (DataTransferProtocol.OP_STATUS_CHECKSUM_OK >>> 8) & 0xff,
                       (DataTransferProtocol.OP_STATUS_CHECKSUM_OK) & 0xff };
        out.write(buf);
        out.flush();
      } catch (IOException e) {
        // its ok not to be able to send this.
        LOG.debug("Could not write to datanode " + sock.getInetAddress() +
                  ": " + e.getMessage());
      }
    }
  }
    
  /****************************************************************
   * DFSInputStream provides bytes from a named file.  It handles 
   * negotiation of the namenode and various datanodes as necessary.
   ****************************************************************/
  public class DFSInputStream extends FSInputStream {
    private Socket s = null;
    private boolean closed = false;

    private String src;
    private long prefetchSize = 10 * defaultBlockSize;
    private BlockReader blockReader = null;
    private boolean verifyChecksum;
    private LocatedBlocks locatedBlocks = null;
    private DatanodeInfo currentNode = null;
    private Block currentBlock = null;
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

    /* XXX Use of CocurrentHashMap is temp fix. Need to fix 
     * parallel accesses to DFSInputStream (through ptreads) properly */
    private ConcurrentHashMap<DatanodeInfo, DatanodeInfo> deadNodes = 
               new ConcurrentHashMap<DatanodeInfo, DatanodeInfo>();
    private int buffersize = 1;
    
    private byte[] oneByteBuf = new byte[1]; // used for 'int read()'
    
    void addToDeadNodes(DatanodeInfo dnInfo) {
      deadNodes.put(dnInfo, dnInfo);
    }
    
    DFSInputStream(String src, int buffersize, boolean verifyChecksum
                   ) throws IOException {
      this.verifyChecksum = verifyChecksum;
      this.buffersize = buffersize;
      this.src = src;
      prefetchSize = conf.getLong("dfs.read.prefetch.size", prefetchSize);
      openInfo();
    }

    /**
     * Grab the open-file info from namenode
     */
    synchronized void openInfo() throws IOException {
      LocatedBlocks newInfo = callGetBlockLocations(namenode, src, 0, prefetchSize);
      if (newInfo == null) {
        throw new FileNotFoundException("File does not exist: " + src);
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
     * @param updatePosition whether to update current position
     * @return located block
     * @throws IOException
     */
    private synchronized LocatedBlock getBlockAt(long offset,
        boolean updatePosition) throws IOException {
      assert (locatedBlocks != null) : "locatedBlocks is null";
      // search cached blocks first
      int targetBlockIdx = locatedBlocks.findBlock(offset);
      if (targetBlockIdx < 0) { // block is not cached
        targetBlockIdx = LocatedBlocks.getInsertIndex(targetBlockIdx);
        // fetch more blocks
        LocatedBlocks newBlocks;
        newBlocks = callGetBlockLocations(namenode, src, offset, prefetchSize);
        assert (newBlocks != null) : "Could not find target position " + offset;
        locatedBlocks.insertRange(targetBlockIdx, newBlocks.getLocatedBlocks());
      }
      LocatedBlock blk = locatedBlocks.get(targetBlockIdx);
      // update current position
      if (updatePosition) {
        this.pos = offset;
        this.blockEnd = blk.getStartOffset() + blk.getBlockSize() - 1;
        this.currentBlock = blk.getBlock();
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
      newBlocks = callGetBlockLocations(namenode, src, offset, prefetchSize);
      if (newBlocks == null) {
        throw new IOException("Could not find target position " + offset);
      }
      locatedBlocks.insertRange(targetBlockIdx, newBlocks.getLocatedBlocks());
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
          newBlocks = callGetBlockLocations(namenode, src, curOff, remaining);
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
      // Connect to best DataNode for desired Block, with potential offset
      //
      DatanodeInfo chosenNode = null;
      int refetchToken = 1; // only need to get a new access token once
      while (true) {
        //
        // Compute desired block
        //
        LocatedBlock targetBlock = getBlockAt(target, true);
        assert (target==this.pos) : "Wrong postion " + pos + " expect " + target;
        long offsetIntoBlock = target - targetBlock.getStartOffset();

        DNAddrPair retval = chooseDataNode(targetBlock);
        chosenNode = retval.info;
        InetSocketAddress targetAddr = retval.addr;

        try {
          s = socketFactory.createSocket();
          NetUtils.connect(s, targetAddr, socketTimeout);
          s.setSoTimeout(socketTimeout);
          Block blk = targetBlock.getBlock();
          Token<BlockTokenIdentifier> accessToken = targetBlock.getBlockToken();
          
          blockReader = BlockReader.newBlockReader(s, src, blk.getBlockId(), 
              accessToken, 
              blk.getGenerationStamp(),
              offsetIntoBlock, blk.getNumBytes() - offsetIntoBlock,
              buffersize, verifyChecksum, clientName);
          return chosenNode;
        } catch (IOException ex) {
          if (ex instanceof InvalidBlockTokenException && refetchToken > 0) {
            LOG.info("Will fetch a new access token and retry, " 
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
            LOG.info("Failed to connect to " + targetAddr
                + ", add to deadNodes and continue", ex);
            // Put chosen node into dead list, continue
            addToDeadNodes(chosenNode);
          }
          if (s != null) {
            try {
              s.close();
            } catch (IOException iex) {
            }                        
          }
          s = null;
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
      checkOpen();
      
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
          LOG.warn("Found Checksum error for " + currentBlock + " from " +
                   currentNode.getName() + " at " + ce.getPos());          
          reportChecksumFailure(src, currentBlock, currentNode);
          ioe = ce;
          retryCurrentNode = false;
        } catch ( IOException e ) {
          if (!retryCurrentNode) {
            LOG.warn("Exception while reading from " + currentBlock +
                     " of " + src + " from " + currentNode + ": " +
                     StringUtils.stringifyException(e));
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
      checkOpen();
      if (closed) {
        throw new IOException("Stream closed");
      }
      failures = 0;

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
            if (stats != null && result != -1) {
              stats.incrementBytesRead(result);
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
      while (true) {
        DatanodeInfo[] nodes = block.getLocations();
        try {
          DatanodeInfo chosenNode = bestNode(nodes, deadNodes);
          InetSocketAddress targetAddr = 
                            NetUtils.createSocketAddr(chosenNode.getName());
          return new DNAddrPair(chosenNode, targetAddr);
        } catch (IOException ie) {
          String blockInfo = block.getBlock() + " file=" + src;
          if (failures >= maxBlockAcquireFailures) {
            throw new IOException("Could not obtain block: " + blockInfo);
          }
          
          if (nodes == null || nodes.length == 0) {
            LOG.info("No node available for block: " + blockInfo);
          }
          LOG.info("Could not obtain block " + block.getBlock()
              + " from any node: " + ie
              + ". Will get new block locations from namenode and retry...");
          try {
            Thread.sleep(3000);
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
        
    private void fetchBlockByteRange(LocatedBlock block, long start,
                                     long end, byte[] buf, int offset) throws IOException {
      //
      // Connect to best DataNode for desired Block, with potential offset
      //
      Socket dn = null;
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
          dn = socketFactory.createSocket();
          NetUtils.connect(dn, targetAddr, socketTimeout);
          dn.setSoTimeout(socketTimeout);
          Token<BlockTokenIdentifier> accessToken = block.getBlockToken();
              
          int len = (int) (end - start + 1);
              
          reader = BlockReader.newBlockReader(dn, src, 
                                              block.getBlock().getBlockId(),
                                              accessToken,
                                              block.getBlock().getGenerationStamp(),
                                              start, len, buffersize, 
                                              verifyChecksum, clientName);
          int nread = reader.readAll(buf, offset, len);
          if (nread != len) {
            throw new IOException("truncated return from reader.read(): " +
                                  "excpected " + len + ", got " + nread);
          }
          return;
        } catch (ChecksumException e) {
          LOG.warn("fetchBlockByteRange(). Got a checksum exception for " +
                   src + " at " + block.getBlock() + ":" + 
                   e.getPos() + " from " + chosenNode.getName());
          reportChecksumFailure(src, block.getBlock(), chosenNode);
        } catch (IOException e) {
          if (e instanceof InvalidBlockTokenException && refetchToken > 0) {
            LOG.info("Will get a new access token and retry, "
                + "access token was invalid when connecting to " + targetAddr
                + " : " + e);
            refetchToken--;
            fetchBlockAt(block.getStartOffset());
            continue;
          } else {
            LOG.warn("Failed to connect to " + targetAddr + " for file " + src
                + " for block " + block.getBlock() + ":"
                + StringUtils.stringifyException(e));
          }
        } finally {
          IOUtils.closeStream(reader);
          IOUtils.closeSocket(dn);
        }
        // Put chosen node into dead list, continue
        addToDeadNodes(chosenNode);
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
    @Override
    public int read(long position, byte[] buffer, int offset, int length)
      throws IOException {
      // sanity checks
      checkOpen();
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
      if (stats != null) {
        stats.incrementBytesRead(realLen);
      }
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
          try {
            pos += blockReader.skip(diff);
            if (pos == targetPos) {
              done = true;
            }
          } catch (IOException e) {//make following read to retry
            LOG.debug("Exception while seek to " + targetPos + " from "
                      + currentBlock +" of " + src + " from " + currentNode + 
                      ": " + StringUtils.stringifyException(e));
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

  /**
   * The Hdfs implementation of {@link FSDataInputStream}
   */
  public static class DFSDataInputStream extends FSDataInputStream {
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

    /**
     * @return The visible length of the file.
     */
    public long getVisibleLength() throws IOException {
      return ((DFSInputStream)in).getFileLength();
    }
  }

  /****************************************************************
   * DFSOutputStream creates files from a stream of bytes.
   *
   * The client application writes data that is cached internally by
   * this stream. Data is broken up into packets, each packet is
   * typically 64K in size. A packet comprises of chunks. Each chunk
   * is typically 512 bytes and has an associated checksum with it.
   *
   * When a client application fills up the currentPacket, it is
   * enqueued into dataQueue.  The DataStreamer thread picks up
   * packets from the dataQueue, sends it to the first datanode in
   * the pipeline and moves it from the dataQueue to the ackQueue.
   * The ResponseProcessor receives acks from the datanodes. When an
   * successful ack for a packet is received from all datanodes, the
   * ResponseProcessor removes the corresponding packet from the
   * ackQueue.
   *
   * In case of error, all outstanding packets and moved from
   * ackQueue. A new pipeline is setup by eliminating the bad
   * datanode from the original pipeline. The DataStreamer now
   * starts sending packets from the dataQueue.
  ****************************************************************/
  class DFSOutputStream extends FSOutputSummer implements Syncable {
    private Socket s;
    boolean closed = false;
  
    private String src;
    private DataOutputStream blockStream;
    private DataInputStream blockReplyStream;
    private Block block;
    private Token<BlockTokenIdentifier> accessToken;
    final private long blockSize;
    private DataChecksum checksum;
    private LinkedList<Packet> dataQueue = new LinkedList<Packet>();
    private LinkedList<Packet> ackQueue = new LinkedList<Packet>();
    private Packet currentPacket = null;
    private int maxPackets = 80; // each packet 64K, total 5MB
    // private int maxPackets = 1000; // each packet 64K, total 64MB
    private DataStreamer streamer = new DataStreamer();;
    private ResponseProcessor response = null;
    private long currentSeqno = 0;
    private long bytesCurBlock = 0; // bytes writen in current block
    private int packetSize = 0; // write packet size, including the header.
    private int chunksPerPacket = 0;
    private DatanodeInfo[] nodes = null; // list of targets for current block
    private volatile boolean hasError = false;
    private volatile int errorIndex = 0;
    private volatile IOException lastException = null;
    private long artificialSlowdown = 0;
    private long lastFlushOffset = -1; // offset when flush was invoked
    private boolean persistBlocks = false; // persist blocks on namenode
    private int recoveryErrorCount = 0; // number of times block recovery failed
    private int maxRecoveryErrorCount = 5; // try block recovery 5 times
    private volatile boolean appendChunk = false;   // appending to existing partial block
    private long initialFileSize = 0; // at time of file open

    Token<BlockTokenIdentifier> getAccessToken() {
      return accessToken;
    }

    private void setLastException(IOException e) {
      if (lastException == null) {
        lastException = e;
      }
    }
    
    private class Packet {
      ByteBuffer buffer;           // only one of buf and buffer is non-null
      byte[]  buf;
      long    seqno;               // sequencenumber of buffer in block
      long    offsetInBlock;       // offset in block
      boolean lastPacketInBlock;   // is this the last packet in block?
      int     numChunks;           // number of chunks currently in packet
      int     maxChunks;           // max chunks in packet
      int     dataStart;
      int     dataPos;
      int     checksumStart;
      int     checksumPos;      
  
      // create a new packet
      Packet(int pktSize, int chunksPerPkt, long offsetInBlock) {
        this.lastPacketInBlock = false;
        this.numChunks = 0;
        this.offsetInBlock = offsetInBlock;
        this.seqno = currentSeqno;
        currentSeqno++;
        
        buffer = null;
        buf = new byte[pktSize];
        
        checksumStart = DataNode.PKT_HEADER_LEN + SIZE_OF_INTEGER;
        checksumPos = checksumStart;
        dataStart = checksumStart + chunksPerPkt * checksum.getChecksumSize();
        dataPos = dataStart;
        maxChunks = chunksPerPkt;
      }

      void writeData(byte[] inarray, int off, int len) {
        if ( dataPos + len > buf.length) {
          throw new BufferOverflowException();
        }
        System.arraycopy(inarray, off, buf, dataPos, len);
        dataPos += len;
      }
  
      void  writeChecksum(byte[] inarray, int off, int len) {
        if (checksumPos + len > dataStart) {
          throw new BufferOverflowException();
        }
        System.arraycopy(inarray, off, buf, checksumPos, len);
        checksumPos += len;
      }
      
      /**
       * Returns ByteBuffer that contains one full packet, including header.
       */
      ByteBuffer getBuffer() {
        /* Once this is called, no more data can be added to the packet.
         * setting 'buf' to null ensures that.
         * This is called only when the packet is ready to be sent.
         */
        if (buffer != null) {
          return buffer;
        }
        
        //prepare the header and close any gap between checksum and data.
        
        int dataLen = dataPos - dataStart;
        int checksumLen = checksumPos - checksumStart;
        
        if (checksumPos != dataStart) {
          /* move the checksum to cover the gap.
           * This can happen for the last packet.
           */
          System.arraycopy(buf, checksumStart, buf, 
                           dataStart - checksumLen , checksumLen); 
        }
        
        int pktLen = SIZE_OF_INTEGER + dataLen + checksumLen;
        
        //normally dataStart == checksumPos, i.e., offset is zero.
        buffer = ByteBuffer.wrap(buf, dataStart - checksumPos,
                                 DataNode.PKT_HEADER_LEN + pktLen);
        buf = null;
        buffer.mark();
        
        /* write the header and data length.
         * The format is described in comment before DataNode.BlockSender
         */
        buffer.putInt(pktLen);  // pktSize
        buffer.putLong(offsetInBlock); 
        buffer.putLong(seqno);
        buffer.put((byte) ((lastPacketInBlock) ? 1 : 0));
        //end of pkt header
        buffer.putInt(dataLen); // actual data length, excluding checksum.
        
        buffer.reset();
        return buffer;
      }
    }
  
    //
    // The DataStreamer class is responsible for sending data packets to the
    // datanodes in the pipeline. It retrieves a new blockid and block locations
    // from the namenode, and starts streaming packets to the pipeline of
    // Datanodes. Every packet has a sequence number associated with
    // it. When all the packets for a block are sent out and acks for each
    // if them are received, the DataStreamer closes the current block.
    //
    private class DataStreamer extends Daemon {

      private volatile boolean closed = false;
  
      public void run() {
        while (!closed && clientRunning) {

          // if the Responder encountered an error, shutdown Responder
          if (hasError && response != null) {
            try {
              response.close();
              response.join();
              response = null;
            } catch (InterruptedException  e) {
            }
          }

          Packet one = null;
          synchronized (dataQueue) {

            // process IO errors if any
            boolean doSleep = processDatanodeError(hasError, false);

            // wait for a packet to be sent.
            while ((!closed && !hasError && clientRunning 
                   && dataQueue.size() == 0) || doSleep) {
              try {
                dataQueue.wait(1000);
              } catch (InterruptedException  e) {
              }
              doSleep = false;
            }
            if (closed || hasError || dataQueue.size() == 0 || !clientRunning) {
              continue;
            }

            try {
              // get packet to be sent.
              one = dataQueue.getFirst();
              long offsetInBlock = one.offsetInBlock;
  
              // get new block from namenode.
              if (blockStream == null) {
                LOG.debug("Allocating new block");
                nodes = nextBlockOutputStream(src); 
                this.setName("DataStreamer for file " + src +
                             " block " + block);
                response = new ResponseProcessor(nodes);
                response.start();
              }

              if (offsetInBlock >= blockSize) {
                throw new IOException("BlockSize " + blockSize +
                                      " is smaller than data size. " +
                                      " Offset of packet in block " + 
                                      offsetInBlock +
                                      " Aborting file " + src);
              }

              ByteBuffer buf = one.getBuffer();
              
              // move packet from dataQueue to ackQueue
              dataQueue.removeFirst();
              dataQueue.notifyAll();
              synchronized (ackQueue) {
                ackQueue.addLast(one);
                ackQueue.notifyAll();
              } 
              
              // write out data to remote datanode
              blockStream.write(buf.array(), buf.position(), buf.remaining());
              
              if (one.lastPacketInBlock) {
                blockStream.writeInt(0); // indicate end-of-block 
              }
              blockStream.flush();
              if (LOG.isDebugEnabled()) {
                LOG.debug("DataStreamer block " + block +
                          " wrote packet seqno:" + one.seqno +
                          " size:" + buf.remaining() +
                          " offsetInBlock:" + one.offsetInBlock + 
                          " lastPacketInBlock:" + one.lastPacketInBlock);
              }
            } catch (Throwable e) {
              LOG.warn("DataStreamer Exception: " + 
                       StringUtils.stringifyException(e));
              if (e instanceof IOException) {
                setLastException((IOException)e);
              }
              hasError = true;
            }
          }

          if (closed || hasError || !clientRunning) {
            continue;
          }

          // Is this block full?
          if (one.lastPacketInBlock) {
            synchronized (ackQueue) {
              while (!hasError && ackQueue.size() != 0 && clientRunning) {
                try {
                  ackQueue.wait();   // wait for acks to arrive from datanodes
                } catch (InterruptedException  e) {
                }
              }
            }
            LOG.debug("Closing old block " + block);
            this.setName("DataStreamer for file " + src);

            response.close();        // ignore all errors in Response
            try {
              response.join();
              response = null;
            } catch (InterruptedException  e) {
            }

            if (closed || hasError || !clientRunning) {
              continue;
            }

            synchronized (dataQueue) {
              try {
                blockStream.close();
                blockReplyStream.close();
              } catch (IOException e) {
              }
              nodes = null;
              response = null;
              blockStream = null;
              blockReplyStream = null;
            }
          }
          if (progress != null) { progress.progress(); }

          // This is used by unit test to trigger race conditions.
          if (artificialSlowdown != 0 && clientRunning) {
            try { 
              Thread.sleep(artificialSlowdown); 
            } catch (InterruptedException e) {}
          }
        }
      }

      // shutdown thread
      void close() {
        closed = true;
        synchronized (dataQueue) {
          dataQueue.notifyAll();
        }
        synchronized (ackQueue) {
          ackQueue.notifyAll();
        }
        this.interrupt();
      }
    }
                  
    //
    // Processes reponses from the datanodes.  A packet is removed 
    // from the ackQueue when its response arrives.
    //
    private class ResponseProcessor extends Thread {

      private volatile boolean closed = false;
      private DatanodeInfo[] targets = null;
      private boolean lastPacketInBlock = false;

      ResponseProcessor (DatanodeInfo[] targets) {
        this.targets = targets;
      }

      public void run() {

        this.setName("ResponseProcessor for block " + block);
        PipelineAck ack = new PipelineAck();
  
        while (!closed && clientRunning && !lastPacketInBlock) {
          // process responses from datanodes.
          try {
            // read an ack from the pipeline
            ack.readFields(blockReplyStream);
            if (LOG.isDebugEnabled()) {
              LOG.debug("DFSClient for block " + block + " " + ack);
            }
            long seqno = ack.getSeqno();
            if (seqno == PipelineAck.HEART_BEAT.getSeqno()) {
              continue;
            } else if (seqno == -2) {
              // no nothing
            } else {
              Packet one = null;
              synchronized (ackQueue) {
                one = ackQueue.getFirst();
              }
              if (one.seqno != seqno) {
                throw new IOException("Responseprocessor: Expecting seqno " + 
                                      " for block " + block + " " +
                                      one.seqno + " but received " + seqno);
              }
              lastPacketInBlock = one.lastPacketInBlock;
            }

            // processes response status from all datanodes.
            for (int i = ack.getNumOfReplies()-1; i >=0  && clientRunning; i--) {
              short reply = ack.getReply(i);
              if (reply != DataTransferProtocol.OP_STATUS_SUCCESS) {
                errorIndex = i; // first bad datanode
                throw new IOException("Bad response " + reply +
                                      " for block " + block +
                                      " from datanode " + 
                                      targets[i].getName());
              }
            }

            synchronized (ackQueue) {
              ackQueue.removeFirst();
              ackQueue.notifyAll();
            }
          } catch (Exception e) {
            if (!closed) {
              hasError = true;
              if (e instanceof IOException) {
                setLastException((IOException)e);
              }
              LOG.warn("DFSOutputStream ResponseProcessor exception " + 
                       " for block " + block +
                        StringUtils.stringifyException(e));
              closed = true;
            }
          }

          synchronized (dataQueue) {
            dataQueue.notifyAll();
          }
          synchronized (ackQueue) {
            ackQueue.notifyAll();
          }
        }
      }

      void close() {
        closed = true;
        this.interrupt();
      }
    }

    // If this stream has encountered any errors so far, shutdown 
    // threads and mark stream as closed. Returns true if we should
    // sleep for a while after returning from this call.
    //
    private boolean processDatanodeError(boolean hasError, boolean isAppend) {
      if (!hasError) {
        return false;
      }
      if (response != null) {
        LOG.info("Error Recovery for block " + block +
                 " waiting for responder to exit. ");
        return true;
      }
      if (errorIndex >= 0) {
        LOG.warn("Error Recovery for block " + block
            + " bad datanode[" + errorIndex + "] "
            + (nodes == null? "nodes == null": nodes[errorIndex].getName()));
      }

      if (blockStream != null) {
        try {
          blockStream.close();
          blockReplyStream.close();
        } catch (IOException e) {
        }
      }
      blockStream = null;
      blockReplyStream = null;

      // move packets from ack queue to front of the data queue
      synchronized (ackQueue) {
        dataQueue.addAll(0, ackQueue);
        ackQueue.clear();
      }

      boolean success = false;
      while (!success && clientRunning) {
        DatanodeInfo[] newnodes = null;
        if (nodes == null) {
          String msg = "Could not get block locations. " +
                                          "Source file \"" + src
                                          + "\" - Aborting...";
          LOG.warn(msg);
          setLastException(new IOException(msg));
          closed = true;
          if (streamer != null) streamer.close();
          return false;
        }
        StringBuilder pipelineMsg = new StringBuilder();
        for (int j = 0; j < nodes.length; j++) {
          pipelineMsg.append(nodes[j].getName());
          if (j < nodes.length - 1) {
            pipelineMsg.append(", ");
          }
        }
        // remove bad datanode from list of datanodes.
        // If errorIndex was not set (i.e. appends), then do not remove 
        // any datanodes
        // 
        if (errorIndex < 0) {
          newnodes = nodes;
        } else {
          if (nodes.length <= 1) {
            lastException = new IOException("All datanodes " + pipelineMsg + 
                                            " are bad. Aborting...");
            closed = true;
            if (streamer != null) streamer.close();
            return false;
          }
          LOG.warn("Error Recovery for block " + block +
                   " in pipeline " + pipelineMsg + 
                   ": bad datanode " + nodes[errorIndex].getName());
          newnodes =  new DatanodeInfo[nodes.length-1];
          System.arraycopy(nodes, 0, newnodes, 0, errorIndex);
          System.arraycopy(nodes, errorIndex+1, newnodes, errorIndex,
              newnodes.length-errorIndex);
        }

        // Tell the primary datanode to do error recovery 
        // by stamping appropriate generation stamps.
        //
        LocatedBlock newBlock = null;
        ClientDatanodeProtocol primary =  null;
        DatanodeInfo primaryNode = null;
        try {
          // Pick the "least" datanode as the primary datanode to avoid deadlock.
          primaryNode = Collections.min(Arrays.asList(newnodes));
          primary = createClientDatanodeProtocolProxy(primaryNode, conf, block, accessToken);
          newBlock = primary.recoverBlock(block, isAppend, newnodes);
        } catch (IOException e) {
          recoveryErrorCount++;
          if (recoveryErrorCount > maxRecoveryErrorCount) {
            if (nodes.length > 1) {
              // if the primary datanode failed, remove it from the list.
              // The original bad datanode is left in the list because it is
              // conservative to remove only one datanode in one iteration.
              for (int j = 0; j < nodes.length; j++) {
                if (nodes[j].equals(primaryNode)) {
                  errorIndex = j; // forget original bad node.
                }
              }
              // remove primary node from list
              newnodes =  new DatanodeInfo[nodes.length-1];
              System.arraycopy(nodes, 0, newnodes, 0, errorIndex);
              System.arraycopy(nodes, errorIndex+1, newnodes, errorIndex,
                               newnodes.length-errorIndex);
              nodes = newnodes;
              LOG.warn("Error Recovery for block " + block + " failed " +
                       " because recovery from primary datanode " +
                       primaryNode + " failed " + recoveryErrorCount +
                       " times. " + " Pipeline was " + pipelineMsg +
                       ". Marking primary datanode as bad.");
              recoveryErrorCount = 0; 
              errorIndex = -1;
              return true;          // sleep when we return from here
            }
            String emsg = "Error Recovery for block " + block + " failed " +
                          " because recovery from primary datanode " +
                          primaryNode + " failed " + recoveryErrorCount + 
                          " times. "  + " Pipeline was " + pipelineMsg +
                          ". Aborting...";
            LOG.warn(emsg);
            lastException = new IOException(emsg);
            closed = true;
            if (streamer != null) streamer.close();
            return false;       // abort with IOexception
          } 
          LOG.warn("Error Recovery for block " + block + " failed " +
                   " because recovery from primary datanode " +
                   primaryNode + " failed " + recoveryErrorCount +
                   " times. "  + " Pipeline was " + pipelineMsg +
                   ". Will retry...");
          return true;          // sleep when we return from here
        } finally {
          RPC.stopProxy(primary);
        }
        recoveryErrorCount = 0; // block recovery successful

        // If the block recovery generated a new generation stamp, use that
        // from now on.  Also, setup new pipeline
        // newBlock should never be null and it should contain a newly
        // generated access token.
        block = newBlock.getBlock();
        accessToken = newBlock.getBlockToken();
        nodes = newBlock.getLocations();

        this.hasError = false;
        lastException = null;
        errorIndex = 0;
        success = createBlockOutputStream(nodes, clientName, true);
      }

      response = new ResponseProcessor(nodes);
      response.start();
      return false; // do not sleep, continue processing
    }

    private void isClosed() throws IOException {
      if (closed && lastException != null) {
          throw lastException;
      }
    }

    //
    // returns the list of targets, if any, that is being currently used.
    //
    DatanodeInfo[] getPipeline() {
      synchronized (dataQueue) {
        if (nodes == null) {
          return null;
        }
        DatanodeInfo[] value = new DatanodeInfo[nodes.length];
        for (int i = 0; i < nodes.length; i++) {
          value[i] = nodes[i];
        }
        return value;
      }
    }

    private Progressable progress;

    private DFSOutputStream(String src, long blockSize, Progressable progress,
        int bytesPerChecksum) throws IOException {
      super(new CRC32(), bytesPerChecksum, 4);
      this.src = src;
      this.blockSize = blockSize;
      this.progress = progress;
      if (progress != null) {
        LOG.debug("Set non-null progress callback on DFSOutputStream "+src);
      }
      
      if ( bytesPerChecksum < 1 || blockSize % bytesPerChecksum != 0) {
        throw new IOException("io.bytes.per.checksum(" + bytesPerChecksum +
                              ") and blockSize(" + blockSize + 
                              ") do not match. " + "blockSize should be a " +
                              "multiple of io.bytes.per.checksum");
                              
      }
      checksum = DataChecksum.newDataChecksum(DataChecksum.CHECKSUM_CRC32, 
                                              bytesPerChecksum);
    }

    /**
     * Create a new output stream to the given DataNode.
     * @see ClientProtocol#create(String, FsPermission, String, boolean, short, long)
     */
    DFSOutputStream(String src, FsPermission masked, boolean overwrite,
        short replication, long blockSize, Progressable progress,
        int buffersize, int bytesPerChecksum) throws IOException {
      this(src, blockSize, progress, bytesPerChecksum);

      computePacketChunkSize(writePacketSize, bytesPerChecksum);

      try {
        namenode.create(
            src, masked, clientName, overwrite, replication, blockSize);
      } catch(RemoteException re) {
        throw re.unwrapRemoteException(AccessControlException.class,
                                       NSQuotaExceededException.class,
                                       DSQuotaExceededException.class);
      }
      streamer.start();
    }
  
    /**
     * Create a new output stream to the given DataNode.
     * @see ClientProtocol#create(String, FsPermission, String, boolean, short, long)
     */
    DFSOutputStream(String src, int buffersize, Progressable progress,
        LocatedBlock lastBlock, HdfsFileStatus stat,
        int bytesPerChecksum) throws IOException {
      this(src, stat.getBlockSize(), progress, bytesPerChecksum);
      initialFileSize = stat.getLen(); // length of file when opened

      //
      // The last partial block of the file has to be filled.
      //
      if (lastBlock != null) {
        block = lastBlock.getBlock();
        accessToken = lastBlock.getBlockToken();
        long usedInLastBlock = stat.getLen() % blockSize;
        int freeInLastBlock = (int)(blockSize - usedInLastBlock);

        // calculate the amount of free space in the pre-existing 
        // last crc chunk
        int usedInCksum = (int)(stat.getLen() % bytesPerChecksum);
        int freeInCksum = bytesPerChecksum - usedInCksum;

        // if there is space in the last block, then we have to 
        // append to that block
        if (freeInLastBlock > blockSize) {
          throw new IOException("The last block for file " + 
                                src + " is full.");
        }

        // indicate that we are appending to an existing block
        bytesCurBlock = lastBlock.getBlockSize();

        if (usedInCksum > 0 && freeInCksum > 0) {
          // if there is space in the last partial chunk, then 
          // setup in such a way that the next packet will have only 
          // one chunk that fills up the partial chunk.
          //
          computePacketChunkSize(0, freeInCksum);
          resetChecksumChunk(freeInCksum);
          this.appendChunk = true;
        } else {
          // if the remaining space in the block is smaller than 
          // that expected size of of a packet, then create 
          // smaller size packet.
          //
          computePacketChunkSize(Math.min(writePacketSize, freeInLastBlock), 
                                 bytesPerChecksum);
        }

        // setup pipeline to append to the last block XXX retries??
        nodes = lastBlock.getLocations();
        errorIndex = -1;   // no errors yet.
        if (nodes.length < 1) {
          throw new IOException("Unable to retrieve blocks locations " +
                                " for last block " + block +
                                "of file " + src);
                        
        }
        processDatanodeError(true, true);
        streamer.start();
      }
      else {
        computePacketChunkSize(writePacketSize, bytesPerChecksum);
        streamer.start();
      }
    }

    private void computePacketChunkSize(int psize, int csize) {
      int chunkSize = csize + checksum.getChecksumSize();
      int n = DataNode.PKT_HEADER_LEN + SIZE_OF_INTEGER;
      chunksPerPacket = Math.max((psize - n + chunkSize-1)/chunkSize, 1);
      packetSize = n + chunkSize*chunksPerPacket;
      if (LOG.isDebugEnabled()) {
        LOG.debug("computePacketChunkSize: src=" + src +
                  ", chunkSize=" + chunkSize +
                  ", chunksPerPacket=" + chunksPerPacket +
                  ", packetSize=" + packetSize);
      }
    }

    /**
     * Open a DataOutputStream to a DataNode so that it can be written to.
     * This happens when a file is created and each time a new block is allocated.
     * Must get block ID and the IDs of the destinations from the namenode.
     * Returns the list of target datanodes.
     */
    private DatanodeInfo[] nextBlockOutputStream(String client) throws IOException {
      LocatedBlock lb = null;
      boolean retry = false;
      DatanodeInfo[] nodes;
      int count = conf.getInt("dfs.client.block.write.retries", 3);
      boolean success;
      do {
        hasError = false;
        lastException = null;
        errorIndex = 0;
        retry = false;
        nodes = null;
        success = false;
                
        long startTime = System.currentTimeMillis();
        lb = locateFollowingBlock(startTime);
        block = lb.getBlock();
        accessToken = lb.getBlockToken();
        nodes = lb.getLocations();
  
        //
        // Connect to first DataNode in the list.
        //
        success = createBlockOutputStream(nodes, clientName, false);

        if (!success) {
          LOG.info("Abandoning block " + block);
          namenode.abandonBlock(block, src, clientName);

          // Connection failed.  Let's wait a little bit and retry
          retry = true;
          try {
            if (System.currentTimeMillis() - startTime > 5000) {
              LOG.info("Waiting to find target node: " + nodes[0].getName());
            }
            Thread.sleep(6000);
          } catch (InterruptedException iex) {
          }
        }
      } while (retry && --count >= 0);

      if (!success) {
        throw new IOException("Unable to create new block.");
      }
      return nodes;
    }

    // connects to the first datanode in the pipeline
    // Returns true if success, otherwise return failure.
    //
    private boolean createBlockOutputStream(DatanodeInfo[] nodes, String client,
                    boolean recoveryFlag) {
      short pipelineStatus = (short)DataTransferProtocol.OP_STATUS_SUCCESS;
      String firstBadLink = "";
      if (LOG.isDebugEnabled()) {
        for (int i = 0; i < nodes.length; i++) {
          LOG.debug("pipeline = " + nodes[i].getName());
        }
      }

      // persist blocks on namenode on next flush
      persistBlocks = true;

      try {
        LOG.debug("Connecting to " + nodes[0].getName());
        InetSocketAddress target = NetUtils.createSocketAddr(nodes[0].getName());
        s = socketFactory.createSocket();
        int timeoutValue = 3000 * nodes.length + socketTimeout;
        NetUtils.connect(s, target, timeoutValue);
        s.setSoTimeout(timeoutValue);
        s.setSendBufferSize(DEFAULT_DATA_SOCKET_SIZE);
        LOG.debug("Send buf size " + s.getSendBufferSize());
        long writeTimeout = HdfsConstants.WRITE_TIMEOUT_EXTENSION * nodes.length +
                            datanodeWriteTimeout;

        //
        // Xmit header info to datanode
        //
        DataOutputStream out = new DataOutputStream(
            new BufferedOutputStream(NetUtils.getOutputStream(s, writeTimeout), 
                                     DataNode.SMALL_BUFFER_SIZE));
        blockReplyStream = new DataInputStream(NetUtils.getInputStream(s));

        out.writeShort( DataTransferProtocol.DATA_TRANSFER_VERSION );
        out.write( DataTransferProtocol.OP_WRITE_BLOCK );
        out.writeLong( block.getBlockId() );
        out.writeLong( block.getGenerationStamp() );
        out.writeInt( nodes.length );
        out.writeBoolean( recoveryFlag );       // recovery flag
        Text.writeString( out, client );
        out.writeBoolean(false); // Not sending src node information
        out.writeInt( nodes.length - 1 );
        for (int i = 1; i < nodes.length; i++) {
          nodes[i].write(out);
        }
        accessToken.write(out);
        checksum.writeHeader( out );
        out.flush();

        // receive ack for connect
        pipelineStatus = blockReplyStream.readShort();
        firstBadLink = Text.readString(blockReplyStream);
        if (pipelineStatus != DataTransferProtocol.OP_STATUS_SUCCESS) {
          if (pipelineStatus == DataTransferProtocol.OP_STATUS_ERROR_ACCESS_TOKEN) {
            throw new InvalidBlockTokenException(
                "Got access token error for connect ack with firstBadLink as "
                    + firstBadLink);
          } else {
            throw new IOException("Bad connect ack with firstBadLink as "
                + firstBadLink);
          }
        }

        blockStream = out;
        return true;     // success

      } catch (IOException ie) {

        LOG.info("Exception in createBlockOutputStream " + ie);

        // find the datanode that matches
        if (firstBadLink.length() != 0) {
          for (int i = 0; i < nodes.length; i++) {
            if (nodes[i].getName().equals(firstBadLink)) {
              errorIndex = i;
              break;
            }
          }
        }
        hasError = true;
        setLastException(ie);
        blockReplyStream = null;
        return false;  // error
      }
    }
  
    private LocatedBlock locateFollowingBlock(long start
                                              ) throws IOException {     
      int retries = conf.getInt("dfs.client.block.write.locateFollowingBlock.retries", 5);
      long sleeptime = 400;
      while (true) {
        long localstart = System.currentTimeMillis();
        while (true) {
          try {
            return namenode.addBlock(src, clientName);
          } catch (RemoteException e) {
            IOException ue = 
              e.unwrapRemoteException(FileNotFoundException.class,
                                      AccessControlException.class,
                                      NSQuotaExceededException.class,
                                      DSQuotaExceededException.class);
            if (ue != e) { 
              throw ue; // no need to retry these exceptions
            }
            
            if (NotReplicatedYetException.class.getName().
                equals(e.getClassName())) {

                if (retries == 0) { 
                  throw e;
                } else {
                  --retries;
                  LOG.info(StringUtils.stringifyException(e));
                  if (System.currentTimeMillis() - localstart > 5000) {
                    LOG.info("Waiting for replication for "
                        + (System.currentTimeMillis() - localstart) / 1000
                        + " seconds");
                  }
                  try {
                    LOG.warn("NotReplicatedYetException sleeping " + src
                        + " retries left " + retries);
                    Thread.sleep(sleeptime);
                    sleeptime *= 2;
                  } catch (InterruptedException ie) {
                  }
                }
            } else {
              throw e;
            }
          }
        }
      } 
    }
  
    // @see FSOutputSummer#writeChunk()
    @Override
    protected synchronized void writeChunk(byte[] b, int offset, int len, byte[] checksum) 
                                                          throws IOException {
      checkOpen();
      isClosed();
  
      int cklen = checksum.length;
      int bytesPerChecksum = this.checksum.getBytesPerChecksum(); 
      if (len > bytesPerChecksum) {
        throw new IOException("writeChunk() buffer size is " + len +
                              " is larger than supported  bytesPerChecksum " +
                              bytesPerChecksum);
      }
      if (checksum.length != this.checksum.getChecksumSize()) {
        throw new IOException("writeChunk() checksum size is supposed to be " +
                              this.checksum.getChecksumSize() + 
                              " but found to be " + checksum.length);
      }

      synchronized (dataQueue) {
  
        // If queue is full, then wait till we can create  enough space
        while (!closed && dataQueue.size() + ackQueue.size()  > maxPackets) {
          try {
            dataQueue.wait();
          } catch (InterruptedException  e) {
          }
        }
        isClosed();
  
        if (currentPacket == null) {
          currentPacket = new Packet(packetSize, chunksPerPacket, 
                                     bytesCurBlock);
          if (LOG.isDebugEnabled()) {
            LOG.debug("DFSClient writeChunk allocating new packet seqno=" + 
                      currentPacket.seqno +
                      ", src=" + src +
                      ", packetSize=" + packetSize +
                      ", chunksPerPacket=" + chunksPerPacket +
                      ", bytesCurBlock=" + bytesCurBlock);
          }
        }

        currentPacket.writeChecksum(checksum, 0, cklen);
        currentPacket.writeData(b, offset, len);
        currentPacket.numChunks++;
        bytesCurBlock += len;

        // If packet is full, enqueue it for transmission
        //
        if (currentPacket.numChunks == currentPacket.maxChunks ||
            bytesCurBlock == blockSize) {
          if (LOG.isDebugEnabled()) {
            LOG.debug("DFSClient writeChunk packet full seqno=" +
                      currentPacket.seqno +
                      ", src=" + src +
                      ", bytesCurBlock=" + bytesCurBlock +
                      ", blockSize=" + blockSize +
                      ", appendChunk=" + appendChunk);
          }
          //
          // if we allocated a new packet because we encountered a block
          // boundary, reset bytesCurBlock.
          //
          if (bytesCurBlock == blockSize) {
            currentPacket.lastPacketInBlock = true;
            bytesCurBlock = 0;
            lastFlushOffset = -1;
          }
          dataQueue.addLast(currentPacket);
          dataQueue.notifyAll();
          currentPacket = null;
 
          // If this was the first write after reopening a file, then the above
          // write filled up any partial chunk. Tell the summer to generate full 
          // crc chunks from now on.
          if (appendChunk) {
            appendChunk = false;
            resetChecksumChunk(bytesPerChecksum);
          }
          int psize = Math.min((int)(blockSize-bytesCurBlock), writePacketSize);
          computePacketChunkSize(psize, bytesPerChecksum);
        }
      }
      //LOG.debug("DFSClient writeChunk done length " + len +
      //          " checksum length " + cklen);
    }
  
    /**
     * All data is written out to datanodes. It is not guaranteed 
     * that data has been flushed to persistent store on the 
     * datanode. Block allocations are persisted on namenode.
     */
    public synchronized void sync() throws IOException {
      try {
        /* Record current blockOffset. This might be changed inside
         * flushBuffer() where a partial checksum chunk might be flushed.
         * After the flush, reset the bytesCurBlock back to its previous value,
         * any partial checksum chunk will be sent now and in next packet.
         */
        long saveOffset = bytesCurBlock;

        // flush checksum buffer, but keep checksum buffer intact
        flushBuffer(true);

        LOG.debug("DFSClient flush() : saveOffset " + saveOffset +  
                  " bytesCurBlock " + bytesCurBlock +
                  " lastFlushOffset " + lastFlushOffset);
        
        // Flush only if we haven't already flushed till this offset.
        if (lastFlushOffset != bytesCurBlock) {

          // record the valid offset of this flush
          lastFlushOffset = bytesCurBlock;

          // wait for all packets to be sent and acknowledged
          flushInternal();
        } else {
          // just discard the current packet since it is already been sent.
          currentPacket = null;
        }
        
        // Restore state of stream. Record the last flush offset 
        // of the last full chunk that was flushed.
        //
        bytesCurBlock = saveOffset;

        // If any new blocks were allocated since the last flush, 
        // then persist block locations on namenode. 
        //
        if (persistBlocks) {
          namenode.fsync(src, clientName);
          persistBlocks = false;
        }
      } catch (IOException e) {
          lastException = new IOException("IOException flush:" + e);
          closed = true;
          closeThreads();
          throw e;
      }
    }

    /**
     * Waits till all existing data is flushed and confirmations 
     * received from datanodes. 
     */
    private synchronized void flushInternal() throws IOException {
      checkOpen();
      isClosed();

      while (!closed) {
        synchronized (dataQueue) {
          isClosed();
          //
          // If there is data in the current buffer, send it across
          //
          if (currentPacket != null) {
            dataQueue.addLast(currentPacket);
            dataQueue.notifyAll();
            currentPacket = null;
          }

          // wait for all buffers to be flushed to datanodes
          if (!closed && dataQueue.size() != 0) {
            try {
              dataQueue.wait();
            } catch (InterruptedException e) {
            }
            continue;
          }
        }

        // wait for all acks to be received back from datanodes
        synchronized (ackQueue) {
          if (!closed && ackQueue.size() != 0) {
            try {
              ackQueue.wait();
            } catch (InterruptedException e) {
            }
            continue;
          }
        }

        // acquire both the locks and verify that we are
        // *really done*. In the case of error recovery, 
        // packets might move back from ackQueue to dataQueue.
        //
        synchronized (dataQueue) {
          synchronized (ackQueue) {
            if (dataQueue.size() + ackQueue.size() == 0) {
              break;       // we are done
            }
          }
        }
      }
    }
  
    /**
     * Closes this output stream and releases any system 
     * resources associated with this stream.
     */
    @Override
    public void close() throws IOException {
      if (closed) {
        IOException e = lastException;
        if (e == null)
          return;
        else
          throw e;
      }
      closeInternal();
      leasechecker.remove(src);
      
      if (s != null) {
        s.close();
        s = null;
      }
    }
 
    // shutdown datastreamer and responseprocessor threads.
    private void closeThreads() throws IOException {
      try {
        streamer.close();
        streamer.join();
        
        // shutdown response after streamer has exited.
        if (response != null) {
          response.close();
          response.join();
          response = null;
        }
      } catch (InterruptedException e) {
        throw new IOException("Failed to shutdown response thread");
      }
    }
    
    /**
     * Closes this output stream and releases any system 
     * resources associated with this stream.
     */
    private synchronized void closeInternal() throws IOException {
      checkOpen();
      isClosed();

      try {
          flushBuffer();       // flush from all upper layers
      
          // Mark that this packet is the last packet in block.
          // If there are no outstanding packets and the last packet
          // was not the last one in the current block, then create a
          // packet with empty payload.
          synchronized (dataQueue) {
            if (currentPacket == null && bytesCurBlock != 0) {
              currentPacket = new Packet(packetSize, chunksPerPacket,
                                         bytesCurBlock);
            }
            if (currentPacket != null) { 
              currentPacket.lastPacketInBlock = true;
            }
          }

        flushInternal();             // flush all data to Datanodes
        isClosed(); // check to see if flushInternal had any exceptions
        closed = true; // allow closeThreads() to showdown threads

        closeThreads();
        
        synchronized (dataQueue) {
          if (blockStream != null) {
            blockStream.writeInt(0); // indicate end-of-block to datanode
            blockStream.close();
            blockReplyStream.close();
          }
          if (s != null) {
            s.close();
            s = null;
          }
        }

        streamer = null;
        blockStream = null;
        blockReplyStream = null;

        long localstart = System.currentTimeMillis();
        boolean fileComplete = false;
        while (!fileComplete) {
          fileComplete = namenode.complete(src, clientName);
          if (!fileComplete) {
            try {
              Thread.sleep(400);
              if (System.currentTimeMillis() - localstart > 5000) {
                LOG.info("Could not complete file " + src + " retrying...");
              }
            } catch (InterruptedException ie) {
            }
          }
        }
      } finally {
        closed = true;
      }
    }

    void setArtificialSlowdown(long period) {
      artificialSlowdown = period;
    }

    synchronized void setChunksPerPacket(int value) {
      chunksPerPacket = Math.min(chunksPerPacket, value);
      packetSize = DataNode.PKT_HEADER_LEN + SIZE_OF_INTEGER +
                   (checksum.getBytesPerChecksum() + 
                    checksum.getChecksumSize()) * chunksPerPacket;
    }

    synchronized void setTestFilename(String newname) {
      src = newname;
    }

    /**
     * Returns the size of a file as it was when this stream was opened
     */
    long getInitialLen() {
      return initialFileSize;
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

  /** {@inheritDoc} */
  public String toString() {
    return getClass().getSimpleName() + "[clientName=" + clientName
        + ", ugi=" + ugi + "]"; 
  }
}
