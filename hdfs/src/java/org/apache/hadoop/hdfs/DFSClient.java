
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

import static org.apache.hadoop.hdfs.protocol.DataTransferProtocol.Op.BLOCK_CHECKSUM;
import static org.apache.hadoop.hdfs.protocol.DataTransferProtocol.Status.ERROR_ACCESS_TOKEN;
import static org.apache.hadoop.hdfs.protocol.DataTransferProtocol.Status.SUCCESS;

import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;

import javax.net.SocketFactory;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsServerDefaults;
import org.apache.hadoop.fs.FsStatus;
import org.apache.hadoop.fs.MD5MD5CRC32FileChecksum;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.Options;
import org.apache.hadoop.fs.UnresolvedLinkException;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.protocol.AlreadyBeingCreatedException;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.ClientDatanodeProtocol;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.protocol.DSQuotaExceededException;
import org.apache.hadoop.hdfs.protocol.DataTransferProtocol;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.DirectoryListing;
import org.apache.hadoop.hdfs.protocol.FSConstants;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.protocol.NSQuotaExceededException;
import org.apache.hadoop.hdfs.protocol.UnresolvedPathException;
import org.apache.hadoop.hdfs.server.common.HdfsConstants;
import org.apache.hadoop.hdfs.server.common.UpgradeStatusReport;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.EnumSetWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.MD5Hash;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.retry.RetryPolicies;
import org.apache.hadoop.io.retry.RetryPolicy;
import org.apache.hadoop.io.retry.RetryProxy;
import org.apache.hadoop.ipc.Client;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.net.NodeBase;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.SecretManager.InvalidToken;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.util.Daemon;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.util.StringUtils;

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
@InterfaceAudience.Private
public class DFSClient implements FSConstants, java.io.Closeable {
  public static final Log LOG = LogFactory.getLog(DFSClient.class);
  public static final long SERVER_DEFAULTS_VALIDITY_PERIOD = 60 * 60 * 1000L; // 1 hour
  public static final int MAX_BLOCK_ACQUIRE_FAILURES = 3;
  static final int TCP_WINDOW_SIZE = 128 * 1024; // 128 KB
  final ClientProtocol namenode;
  private final ClientProtocol rpcNamenode;
  final UserGroupInformation ugi;
  volatile boolean clientRunning = true;
  private volatile FsServerDefaults serverDefaults;
  private volatile long serverDefaultsLastUpdate;
  Random r = new Random();
  final String clientName;
  final LeaseChecker leasechecker = new LeaseChecker();
  Configuration conf;
  long defaultBlockSize;
  private short defaultReplication;
  SocketFactory socketFactory;
  int socketTimeout;
  final int writePacketSize;
  final FileSystem.Statistics stats;
  final int hdfsTimeout;    // timeout value for a DFS operation.

  /**
   * The locking hierarchy is to first acquire lock on DFSClient object, followed by 
   * lock on leasechecker, followed by lock on an individual DFSOutputStream.
   */
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
      DatanodeID datanodeid, Configuration conf) throws IOException {
    InetSocketAddress addr = NetUtils.createSocketAddr(
      datanodeid.getHost() + ":" + datanodeid.getIpcPort());
    if (ClientDatanodeProtocol.LOG.isDebugEnabled()) {
      ClientDatanodeProtocol.LOG.info("ClientDatanodeProtocol addr=" + addr);
    }
    return (ClientDatanodeProtocol)RPC.getProxy(ClientDatanodeProtocol.class,
        ClientDatanodeProtocol.versionID, addr, conf);
  }
        
  /**
   * Same as this(NameNode.getAddress(conf), conf);
   * @see #DFSClient(InetSocketAddress, Configuration)
   * @deprecated Deprecated at 0.21
   */
  @Deprecated
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
    this.socketTimeout = conf.getInt(DFSConfigKeys.DFS_CLIENT_SOCKET_TIMEOUT_KEY, 
                                     HdfsConstants.READ_TIMEOUT);
    this.socketFactory = NetUtils.getSocketFactory(conf, ClientProtocol.class);
    // dfs.write.packet.size is an internal config variable
    this.writePacketSize = conf.getInt(DFSConfigKeys.DFS_CLIENT_WRITE_PACKET_SIZE_KEY, 
		                       DFSConfigKeys.DFS_CLIENT_WRITE_PACKET_SIZE_DEFAULT);
    // The hdfsTimeout is currently the same as the ipc timeout 
    this.hdfsTimeout = Client.getTimeout(conf);

    this.ugi = UserGroupInformation.getCurrentUser();
    
    String taskId = conf.get("mapred.task.id");
    if (taskId != null) {
      this.clientName = "DFSClient_" + taskId; 
    } else {
      this.clientName = "DFSClient_" + r.nextInt();
    }
    defaultBlockSize = conf.getLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, DEFAULT_BLOCK_SIZE);
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

  /**
   * Return the number of times the client should go back to the namenode
   * to retrieve block locations when reading.
   */
  int getMaxBlockAcquireFailures() {
    return conf.getInt("dfs.client.max.block.acquire.failures",
                       MAX_BLOCK_ACQUIRE_FAILURES);
  }

  /**
   * Return the timeout that clients should use when writing to datanodes.
   * @param numNodes the number of nodes in the pipeline.
   */
  int getDatanodeWriteTimeout(int numNodes) {
    int confTime =
        conf.getInt("dfs.datanode.socket.write.timeout",
                    HdfsConstants.WRITE_TIMEOUT);

    return (confTime > 0) ?
      (confTime + HdfsConstants.WRITE_TIMEOUT_EXTENSION * numNodes) : 0;
  }

  int getDatanodeReadTimeout(int numNodes) {
    return socketTimeout > 0 ?
        (HdfsConstants.READ_TIMEOUT_EXTENSION * numNodes +
        socketTimeout) : 0;
  }

  void checkOpen() throws IOException {
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

  /**
   * Get server default values for a number of configuration params.
   */
  public FsServerDefaults getServerDefaults() throws IOException {
    long now = System.currentTimeMillis();
    if (now - serverDefaultsLastUpdate > SERVER_DEFAULTS_VALIDITY_PERIOD) {
      serverDefaults = namenode.getServerDefaults();
      serverDefaultsLastUpdate = now;
    }
    return serverDefaults;
  }

  public Token<DelegationTokenIdentifier> getDelegationToken(Text renewer)
      throws IOException {
    return namenode.getDelegationToken(renewer);
  }

  public long renewDelegationToken(Token<DelegationTokenIdentifier> token)
      throws InvalidToken, IOException {
    try {
      return namenode.renewDelegationToken(token);
    } catch (RemoteException re) {
      throw re.unwrapRemoteException(InvalidToken.class,
                                     AccessControlException.class);
    }
  }

  public void cancelDelegationToken(Token<DelegationTokenIdentifier> token)
      throws InvalidToken, IOException {
    try {
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

  static LocatedBlocks callGetBlockLocations(ClientProtocol namenode,
      String src, long start, long length) 
      throws IOException, UnresolvedLinkException {
    try {
      return namenode.getBlockLocations(src, start, length);
    } catch(RemoteException re) {
      throw re.unwrapRemoteException(AccessControlException.class,
                                     FileNotFoundException.class,
                                     UnresolvedPathException.class);
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
    long length) throws IOException, UnresolvedLinkException {
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

  public DFSInputStream open(String src) 
      throws IOException, UnresolvedLinkException {
    return open(src, conf.getInt("io.file.buffer.size", 4096), true, null);
  }

  /**
   * Create an input stream that obtains a nodelist from the
   * namenode, and then reads from all the right places.  Creates
   * inner subclass of InputStream that does the right out-of-band
   * work.
   * @deprecated Use {@link #open(String, int, boolean)} instead.
   */
  @Deprecated
  public DFSInputStream open(String src, int buffersize, boolean verifyChecksum,
                             FileSystem.Statistics stats)
      throws IOException, UnresolvedLinkException {
    return open(src, buffersize, verifyChecksum);
  }
  

  /**
   * Create an input stream that obtains a nodelist from the
   * namenode, and then reads from all the right places.  Creates
   * inner subclass of InputStream that does the right out-of-band
   * work.
   */
  public DFSInputStream open(String src, int buffersize, boolean verifyChecksum)
      throws IOException, UnresolvedLinkException {
    checkOpen();
    //    Get block info from namenode
    return new DFSInputStream(this, src, buffersize, verifyChecksum);
  }

  /**
   * Create a new dfs file and return an output stream for writing into it. 
   * 
   * @param src stream name
   * @param overwrite do not check for file existence if true
   * @return output stream
   * @throws UnresolvedLinkException if a symlink is encountered in src.
   * @throws IOException
   */
  public OutputStream create(String src, boolean overwrite) 
      throws IOException, UnresolvedLinkException {
    return create(src, overwrite, defaultReplication, defaultBlockSize, null);
  }
    
  /**
   * Create a new dfs file and return an output stream for writing into it
   * with write-progress reporting. 
   * 
   * @param src stream name
   * @param overwrite do not check for file existence if true
   * @return output stream
   * @throws UnresolvedLinkException if a symlink is encountered in src.
   * @throws IOException
   */
  public OutputStream create(String src, 
                             boolean overwrite,
                             Progressable progress)
      throws IOException, UnresolvedLinkException {
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
   * @throws UnresolvedLinkException if a symlink is encountered in src.
   * @throws IOException
   */
  public OutputStream create(String src, 
                             boolean overwrite, 
                             short replication,
                             long blockSize)
      throws IOException, UnresolvedLinkException {
    return create(src, overwrite, replication, blockSize, null);
  }

  /**
   * Get the namenode associated with this DFSClient object
   * @return the namenode associated with this DFSClient object
   */
  public ClientProtocol getNamenode() {
    return namenode;
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
   * @throws UnresolvedLinkException if a symlink is encountered in src.
   * @throws IOException
   */
  public OutputStream create(String src, 
                             boolean overwrite, 
                             short replication,
                             long blockSize,
                             Progressable progress)
      throws IOException, UnresolvedLinkException {
    return create(src, overwrite, replication, blockSize, progress,
        conf.getInt("io.file.buffer.size", 4096));
  }
  /**
   * Call
   * {@link #create(String,FsPermission,EnumSet,short,long,Progressable,int)}
   * with default permission.
   * @see FsPermission#getDefault()
   */
  public OutputStream create(String src,
      boolean overwrite,
      short replication,
      long blockSize,
      Progressable progress,
      int buffersize)
      throws IOException, UnresolvedLinkException {
    return create(src, FsPermission.getDefault(),
        overwrite ? EnumSet.of(CreateFlag.OVERWRITE) : EnumSet.of(CreateFlag.CREATE), 
        replication, blockSize, progress, buffersize);
  }

  /**
   * Call
   * {@link #create(String,FsPermission,EnumSet,boolean,short,long,Progressable,int)}
   * with createParent set to true.
   */
  public OutputStream create(String src, 
      FsPermission permission,
      EnumSet<CreateFlag> flag, 
      short replication,
      long blockSize,
      Progressable progress,
      int buffersize)
      throws IOException, UnresolvedLinkException {
    return create(src, permission, flag, true,
        replication, blockSize, progress, buffersize);
  }

  /**
   * Create a new dfs file with the specified block replication 
   * with write-progress reporting and return an output stream for writing
   * into the file.  
   * 
   * @param src stream name
   * @param permission The permission of the directory being created.
   * If permission == null, use {@link FsPermission#getDefault()}.
   * @param flag do not check for file existence if true
   * @param createParent create missing parent directory if true
   * @param replication block replication
   * @return output stream
   * @throws IOException
   * @throws UnresolvedLinkException if src contains a symlink. 
   * @see ClientProtocol#create(String, FsPermission, String, EnumSetWritable, boolean, short, long)
   */
  public OutputStream create(String src, 
                             FsPermission permission,
                             EnumSet<CreateFlag> flag, 
                             boolean createParent,
                             short replication,
                             long blockSize,
                             Progressable progress,
                             int buffersize)
    throws IOException, UnresolvedLinkException {
    checkOpen();
    if (permission == null) {
      permission = FsPermission.getDefault();
    }
    FsPermission masked = permission.applyUMask(FsPermission.getUMask(conf));
    LOG.debug(src + ": masked=" + masked);
    OutputStream result = new DFSOutputStream(this, src, masked,
        flag, createParent, replication, blockSize, progress, buffersize,
        conf.getInt(DFSConfigKeys.DFS_BYTES_PER_CHECKSUM_KEY, 
                    DFSConfigKeys.DFS_BYTES_PER_CHECKSUM_DEFAULT));
    leasechecker.put(src, result);
    return result;
  }
  
  /**
   * Same as {{@link #create(String, FsPermission, EnumSet, short, long,
   *  Progressable, int)}   except that the permission
   *   is absolute (ie has already been masked with umask.
   */
  public OutputStream primitiveCreate(String src, 
                             FsPermission absPermission,
                             EnumSet<CreateFlag> flag,
                             boolean createParent,
                             short replication,
                             long blockSize,
                             Progressable progress,
                             int buffersize,
                             int bytesPerChecksum)
      throws IOException, UnresolvedLinkException {
    checkOpen();
    OutputStream result = new DFSOutputStream(this, src, absPermission,
        flag, createParent, replication, blockSize, progress, buffersize,
        bytesPerChecksum);
    leasechecker.put(src, result);
    return result;
  }
  
  /**
   * Creates a symbolic link.
   * 
   * @see ClientProtocol#createSymlink(String, String,FsPermission, boolean) 
   */
  public void createSymlink(String target, String link, boolean createParent)
      throws IOException, UnresolvedLinkException {
    try {
      FsPermission dirPerm = 
          FsPermission.getDefault().applyUMask(FsPermission.getUMask(conf)); 
      namenode.createSymlink(target, link, dirPerm, createParent);
    } catch (RemoteException re) {
      throw re.unwrapRemoteException(AccessControlException.class,
                                     NSQuotaExceededException.class, 
                                     DSQuotaExceededException.class,
                                     FileAlreadyExistsException.class, 
                                     UnresolvedPathException.class);
    }
  }

  /**
   * Resolve the *first* symlink, if any, in the path.
   * 
   * @see ClientProtocol#getLinkTarget(String)
   */
  public String getLinkTarget(String path) throws IOException { 
    checkOpen();
    try {
      return namenode.getLinkTarget(path);
    } catch (RemoteException re) {
      throw re.unwrapRemoteException(AccessControlException.class,
                                     FileNotFoundException.class);
    }
  }

  /**
   * Append to an existing HDFS file.  
   * 
   * @param src file name
   * @param buffersize buffer size
   * @param progress for reporting write-progress
   * @return an output stream for writing into the file
   * @throws IOException
   * @throws UnresolvedLinkException if the path contains a symlink.
   * @see ClientProtocol#append(String, String)
   */
  OutputStream append(String src, int buffersize, Progressable progress)
      throws IOException, UnresolvedLinkException {
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
                                     DSQuotaExceededException.class,
                                     UnresolvedPathException.class);
    }
    OutputStream result = new DFSOutputStream(this, src, buffersize, progress,
        lastBlock, stat, conf.getInt(DFSConfigKeys.DFS_BYTES_PER_CHECKSUM_KEY, 
                                     DFSConfigKeys.DFS_BYTES_PER_CHECKSUM_DEFAULT));
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
                                short replication)
      throws IOException, UnresolvedLinkException {
    try {
      return namenode.setReplication(src, replication);
    } catch(RemoteException re) {
      throw re.unwrapRemoteException(AccessControlException.class,
                                     NSQuotaExceededException.class,
                                     DSQuotaExceededException.class,
                                     UnresolvedPathException.class);
    }
  }

  /**
   * Rename file or directory.
   * See {@link ClientProtocol#rename(String, String)}.
   * @deprecated Use {@link #rename(String, String, Options.Rename...)} instead.
   */
  @Deprecated
  public boolean rename(String src, String dst) 
      throws IOException, UnresolvedLinkException {
    checkOpen();
    try {
      return namenode.rename(src, dst);
    } catch(RemoteException re) {
      throw re.unwrapRemoteException(AccessControlException.class,
                                     NSQuotaExceededException.class,
                                     DSQuotaExceededException.class,
                                     UnresolvedPathException.class);
    }
  }

  /**
   * Move blocks from src to trg and delete src
   * See {@link ClientProtocol#concat(String, String [])}. 
   */
  public void concat(String trg, String [] srcs) 
      throws IOException, UnresolvedLinkException {
    checkOpen();
    try {
      namenode.concat(trg, srcs);
    } catch(RemoteException re) {
      throw re.unwrapRemoteException(AccessControlException.class,
                                     NSQuotaExceededException.class,
                                     DSQuotaExceededException.class,
                                     UnresolvedPathException.class);
    }
  }
  /**
   * Rename file or directory.
   * See {@link ClientProtocol#rename(String, String, Options.Rename...)}
   */
  public void rename(String src, String dst, Options.Rename... options) 
      throws IOException, UnresolvedLinkException {
    checkOpen();
    try {
      namenode.rename(src, dst, options);
    } catch(RemoteException re) {
      throw re.unwrapRemoteException(AccessControlException.class,
                                     NSQuotaExceededException.class,
                                     DSQuotaExceededException.class,
                                     UnresolvedPathException.class);
    }
  }
  /**
   * Delete file or directory.
   * See {@link ClientProtocol#delete(String)}. 
   */
  @Deprecated
  public boolean delete(String src) 
      throws IOException, UnresolvedLinkException {
    checkOpen();
    return namenode.delete(src, true);
  }

  /**
   * delete file or directory.
   * delete contents of the directory if non empty and recursive 
   * set to true
   */
  public boolean delete(String src, boolean recursive) 
      throws IOException, UnresolvedLinkException {
    checkOpen();
    try {
      return namenode.delete(src, recursive);
    } catch(RemoteException re) {
      throw re.unwrapRemoteException(AccessControlException.class,
                                     UnresolvedPathException.class);
    }
  }
  
  /** Implemented using getFileInfo(src)
   */
  public boolean exists(String src) throws IOException {
    checkOpen();
    return getFileInfo(src) != null;
  }

  /**
   * Get a partial listing of the indicated directory
   *
   * Recommend to use HdfsFileStatus.EMPTY_NAME as startAfter
   * if the application wants to fetch a listing starting from
   * the first entry in the directory
   *
   * @param src the directory name
   * @param startAfter the name to start listing after encoded in java UTF8
   * @return a partial listing starting after startAfter
   */
  public DirectoryListing listPaths(String src,  byte[] startAfter) 
    throws IOException, UnresolvedLinkException {
    checkOpen();
    try {
      return namenode.getListing(src, startAfter);
    } catch(RemoteException re) {
      throw re.unwrapRemoteException(AccessControlException.class,
                                     UnresolvedPathException.class);
    }
  }

  public HdfsFileStatus getFileInfo(String src) 
      throws IOException, UnresolvedLinkException {
    checkOpen();
    try {
      return namenode.getFileInfo(src);
    } catch(RemoteException re) {
      throw re.unwrapRemoteException(AccessControlException.class,
                                     UnresolvedPathException.class);
    }
  }

  /**
   * Get the file info for a specific file or directory. If src
   * refers to a symlink then the FileStatus of the link is returned.
   * @param src path to a file or directory.
   * @throws IOException
   * @throws UnresolvedLinkException if the path contains symlinks
   * @return FileStatus describing src.
   */
  public HdfsFileStatus getFileLinkInfo(String src) 
      throws IOException, UnresolvedLinkException {
    checkOpen();
    try {
      return namenode.getFileLinkInfo(src);
    } catch(RemoteException re) {
      throw re.unwrapRemoteException(AccessControlException.class,
                                     UnresolvedPathException.class);
     }
   }

  /**
   * Get the checksum of a file.
   * @param src The file path
   * @return The checksum 
   * @see DistributedFileSystem#getFileChecksum(Path)
   */
  public MD5MD5CRC32FileChecksum getFileChecksum(String src) throws IOException {
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
        Socket sock = null;
        DataOutputStream out = null;
        DataInputStream in = null;
        
        try {
          //connect to a datanode
          sock = socketFactory.createSocket();
          NetUtils.connect(sock,
              NetUtils.createSocketAddr(datanodes[j].getName()), timeout);
          sock.setSoTimeout(timeout);

          out = new DataOutputStream(
              new BufferedOutputStream(NetUtils.getOutputStream(sock), 
                                       DataNode.SMALL_BUFFER_SIZE));
          in = new DataInputStream(NetUtils.getInputStream(sock));

          if (LOG.isDebugEnabled()) {
            LOG.debug("write to " + datanodes[j].getName() + ": "
                + BLOCK_CHECKSUM + ", block=" + block);
          }
          // get block MD5
          DataTransferProtocol.Sender.opBlockChecksum(out, block.getBlockId(),
              block.getGenerationStamp(), lb.getAccessToken());

          final DataTransferProtocol.Status reply = DataTransferProtocol.Status.read(in);
          if (reply != SUCCESS) {
            if (reply == ERROR_ACCESS_TOKEN
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
   * @throws UnresolvedLinkException if the path contains a symlink.
   */
  public void setPermission(String src, FsPermission permission)
      throws IOException, UnresolvedLinkException {
    checkOpen();
    try {
      namenode.setPermission(src, permission);
    } catch(RemoteException re) {
      throw re.unwrapRemoteException(AccessControlException.class,
                                     FileNotFoundException.class,
                                     UnresolvedPathException.class);
    }
  }

  /**
   * Set file or directory owner.
   * @param src path name.
   * @param username user id.
   * @param groupname user group.
   * @throws <code>FileNotFoundException</code> is file does not exist.
   * @throws UnresolvedLinkException if the path contains a symlink.
   */
  public void setOwner(String src, String username, String groupname)
      throws IOException, UnresolvedLinkException {
    checkOpen();
    try {
      namenode.setOwner(src, username, groupname);
    } catch(RemoteException re) {
      throw re.unwrapRemoteException(AccessControlException.class,
                                     FileNotFoundException.class,
                                     UnresolvedPathException.class);
    }
  }

  public FsStatus getDiskStatus() throws IOException {
    long rawNums[] = namenode.getStats();
    return new FsStatus(rawNums[0], rawNums[1], rawNums[2]);
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
   * enable/disable restore failed storage.
   * See {@link ClientProtocol#restoreFailedStorage(String arg)} 
   * for more details.
   * 
   */
  boolean restoreFailedStorage(String arg) throws AccessControlException {
    return namenode.restoreFailedStorage(arg);
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
  @Deprecated
  public boolean mkdirs(String src) throws IOException {
    return mkdirs(src, null, true);
  }

  /**
   * Create a directory (or hierarchy of directories) with the given
   * name and permission.
   *
   * @param src The path of the directory being created
   * @param permission The permission of the directory being created.
   * If permission == null, use {@link FsPermission#getDefault()}.
   * @param createParent create missing parent directory if true
   * @return True if the operation success.
   * @throws UnresolvedLinkException if the path contains a symlink.
   * @see ClientProtocol#mkdirs(String, FsPermission, boolean)
   */
  public boolean mkdirs(String src, FsPermission permission, boolean createParent)
      throws IOException, UnresolvedLinkException {
    checkOpen();
    if (permission == null) {
      permission = FsPermission.getDefault();
    }
    FsPermission masked = permission.applyUMask(FsPermission.getUMask(conf));
    LOG.debug(src + ": masked=" + masked);
    try {
      return namenode.mkdirs(src, masked, createParent);
    } catch(RemoteException re) {
      throw re.unwrapRemoteException(AccessControlException.class,
                                     NSQuotaExceededException.class,
                                     DSQuotaExceededException.class,
                                     FileNotFoundException.class,
                                     FileAlreadyExistsException.class,
                                     UnresolvedPathException.class);
    }
  }
  
  /**
   * Same {{@link #mkdirs(String, FsPermission, boolean)} except
   * that the permissions has already been masked against umask.
   * @throws UnresolvedLinkException if the path contains a symlink.
   */
  public boolean primitiveMkdir(String src, FsPermission absPermission)
    throws IOException, UnresolvedLinkException {
    checkOpen();
    if (absPermission == null) {
      absPermission = 
        FsPermission.getDefault().applyUMask(FsPermission.getUMask(conf));
    } 

    LOG.debug(src + ": masked=" + absPermission);
    try {
      return namenode.mkdirs(src, absPermission, true);
    } catch(RemoteException re) {
      throw re.unwrapRemoteException(AccessControlException.class,
                                     NSQuotaExceededException.class,
                                     DSQuotaExceededException.class,
                                     UnresolvedPathException.class);
    }
  }

  ContentSummary getContentSummary(String src) throws IOException {
    try {
      return namenode.getContentSummary(src);
    } catch(RemoteException re) {
      throw re.unwrapRemoteException(AccessControlException.class,
                                     FileNotFoundException.class,
                                     UnresolvedPathException.class);
    }
  }

  /**
   * Sets or resets quotas for a directory.
   * @see org.apache.hadoop.hdfs.protocol.ClientProtocol#setQuota(String, long, long)
   */
  void setQuota(String src, long namespaceQuota, long diskspaceQuota) 
      throws IOException, UnresolvedLinkException {
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
                                     DSQuotaExceededException.class,
                                     UnresolvedPathException.class);
    }
  }

  /**
   * set the modification and access time of a file
   * @throws FileNotFoundException if the path is not a file
   */
  public void setTimes(String src, long mtime, long atime) 
      throws IOException, UnresolvedLinkException {
    checkOpen();
    try {
      namenode.setTimes(src, mtime, atime);
    } catch(RemoteException re) {
      throw re.unwrapRemoteException(AccessControlException.class,
                                     FileNotFoundException.class,
                                     UnresolvedPathException.class);
    }
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

    /**
     * Abort all open files. Release resources held. Ignore all errors.
     */
    synchronized void abort() {
      clientRunning = false;
      while (!pendingCreates.isEmpty()) {
        String src = pendingCreates.firstKey();
        DFSOutputStream out = (DFSOutputStream)pendingCreates.remove(src);
        if (out != null) {
          try {
            out.abort();
          } catch (IOException ie) {
            LOG.error("Exception aborting file " + src+ ": ", ie);
          }
        }
      }
      RPC.stopProxy(rpcNamenode); // close connections to the namenode
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
      int renewal = (int)(LEASE_SOFTLIMIT_PERIOD / 2);
      if (hdfsTimeout > 0) {
        renewal = Math.min(renewal, hdfsTimeout/2);
      }
      while (clientRunning && !Thread.interrupted()) {
        if (System.currentTimeMillis() - lastRenewed > renewal) {
          try {
            renew();
            lastRenewed = System.currentTimeMillis();
          } catch (SocketTimeoutException ie) {
            LOG.warn("Problem renewing lease for " + clientName +
                     " for a period of " + (hdfsTimeout/1000) +
                     " seconds. Shutting down HDFS client...", ie);
            abort();
            break;
          } catch (IOException ie) {
            LOG.warn("Problem renewing lease for " + clientName +
                     " for a period of " + (hdfsTimeout/1000) +
                     " seconds. Will retry shortly...", ie);
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

  /**
   * The Hdfs implementation of {@link FSDataInputStream}
   */
  @InterfaceAudience.Private
  public static class DFSDataInputStream extends FSDataInputStream {
    public DFSDataInputStream(DFSInputStream in)
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
