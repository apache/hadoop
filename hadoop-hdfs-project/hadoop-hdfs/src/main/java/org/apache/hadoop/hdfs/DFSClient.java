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

import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.URI;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.net.SocketFactory;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsServerDefaults;
import org.apache.hadoop.fs.FsStatus;
import org.apache.hadoop.fs.InvalidPathException;
import org.apache.hadoop.fs.MD5MD5CRC32FileChecksum;
import org.apache.hadoop.fs.Options;
import org.apache.hadoop.fs.ParentNotDirectoryException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.UnresolvedLinkException;
import org.apache.hadoop.fs.permission.FsPermission;
import static org.apache.hadoop.hdfs.DFSConfigKeys.*;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.protocol.CorruptFileBlocks;
import org.apache.hadoop.hdfs.protocol.DSQuotaExceededException;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.DirectoryListing;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.DatanodeReportType;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.SafeModeAction;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.UpgradeAction;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.protocol.HdfsProtoUtil;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.protocol.NSQuotaExceededException;
import org.apache.hadoop.hdfs.protocol.UnresolvedPathException;
import org.apache.hadoop.hdfs.protocol.datatransfer.Op;
import org.apache.hadoop.hdfs.protocol.datatransfer.ReplaceDatanodeOnFailure;
import org.apache.hadoop.hdfs.protocol.datatransfer.Sender;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.BlockOpResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.OpBlockChecksumResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.Status;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.common.UpgradeStatusReport;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.namenode.SafeModeException;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.EnumSetWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.MD5Hash;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.retry.FailoverProxyProvider;
import org.apache.hadoop.io.retry.RetryPolicies;
import org.apache.hadoop.io.retry.RetryProxy;
import org.apache.hadoop.ipc.Client;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.SecretManager.InvalidToken;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.util.ReflectionUtils;

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
public class DFSClient implements java.io.Closeable {
  public static final Log LOG = LogFactory.getLog(DFSClient.class);
  public static final long SERVER_DEFAULTS_VALIDITY_PERIOD = 60 * 60 * 1000L; // 1 hour
  static final int TCP_WINDOW_SIZE = 128 * 1024; // 128 KB
  final ClientProtocol namenode;
  final UserGroupInformation ugi;
  volatile boolean clientRunning = true;
  private volatile FsServerDefaults serverDefaults;
  private volatile long serverDefaultsLastUpdate;
  final String clientName;
  Configuration conf;
  SocketFactory socketFactory;
  final ReplaceDatanodeOnFailure dtpReplaceDatanodeOnFailure;
  final FileSystem.Statistics stats;
  final int hdfsTimeout;    // timeout value for a DFS operation.
  final LeaseRenewer leaserenewer;
  final SocketCache socketCache;
  final Conf dfsClientConf;

  /**
   * DFSClient configuration 
   */
  static class Conf {
    final int maxBlockAcquireFailures;
    final int confTime;
    final int ioBufferSize;
    final int bytesPerChecksum;
    final int writePacketSize;
    final int socketTimeout;
    final int socketCacheCapacity;
    /** Wait time window (in msec) if BlockMissingException is caught */
    final int timeWindow;
    final int nCachedConnRetry;
    final int nBlockWriteRetry;
    final int nBlockWriteLocateFollowingRetry;
    final long defaultBlockSize;
    final long prefetchSize;
    final short defaultReplication;
    final String taskId;
    final FsPermission uMask;

    Conf(Configuration conf) {
      maxBlockAcquireFailures = conf.getInt(
          DFS_CLIENT_MAX_BLOCK_ACQUIRE_FAILURES_KEY,
          DFS_CLIENT_MAX_BLOCK_ACQUIRE_FAILURES_DEFAULT);
      confTime = conf.getInt(DFS_DATANODE_SOCKET_WRITE_TIMEOUT_KEY,
          HdfsServerConstants.WRITE_TIMEOUT);
      ioBufferSize = conf.getInt(
          CommonConfigurationKeysPublic.IO_FILE_BUFFER_SIZE_KEY,
          CommonConfigurationKeysPublic.IO_FILE_BUFFER_SIZE_DEFAULT);
      bytesPerChecksum = conf.getInt(DFS_BYTES_PER_CHECKSUM_KEY,
          DFS_BYTES_PER_CHECKSUM_DEFAULT);
      socketTimeout = conf.getInt(DFS_CLIENT_SOCKET_TIMEOUT_KEY,
          HdfsServerConstants.READ_TIMEOUT);
      /** dfs.write.packet.size is an internal config variable */
      writePacketSize = conf.getInt(DFS_CLIENT_WRITE_PACKET_SIZE_KEY,
          DFS_CLIENT_WRITE_PACKET_SIZE_DEFAULT);
      defaultBlockSize = conf.getLong(DFS_BLOCK_SIZE_KEY,
          DFS_BLOCK_SIZE_DEFAULT);
      defaultReplication = (short) conf.getInt(
          DFS_REPLICATION_KEY, DFS_REPLICATION_DEFAULT);
      taskId = conf.get("mapreduce.task.attempt.id", "NONMAPREDUCE");
      socketCacheCapacity = conf.getInt(DFS_CLIENT_SOCKET_CACHE_CAPACITY_KEY,
          DFS_CLIENT_SOCKET_CACHE_CAPACITY_DEFAULT);
      prefetchSize = conf.getLong(DFS_CLIENT_READ_PREFETCH_SIZE_KEY,
          10 * defaultBlockSize);
      timeWindow = conf
          .getInt(DFS_CLIENT_RETRY_WINDOW_BASE, 3000);
      nCachedConnRetry = conf.getInt(DFS_CLIENT_CACHED_CONN_RETRY_KEY,
          DFS_CLIENT_CACHED_CONN_RETRY_DEFAULT);
      nBlockWriteRetry = conf.getInt(DFS_CLIENT_BLOCK_WRITE_RETRIES_KEY,
          DFS_CLIENT_BLOCK_WRITE_RETRIES_DEFAULT);
      nBlockWriteLocateFollowingRetry = conf
          .getInt(DFS_CLIENT_BLOCK_WRITE_LOCATEFOLLOWINGBLOCK_RETRIES_KEY,
              DFS_CLIENT_BLOCK_WRITE_LOCATEFOLLOWINGBLOCK_RETRIES_DEFAULT);
      uMask = FsPermission.getUMask(conf);
    }
  }
 
  Conf getConf() {
    return dfsClientConf;
  }
  
  /**
   * A map from file names to {@link DFSOutputStream} objects
   * that are currently being written by this client.
   * Note that a file can only be written by a single client.
   */
  private final Map<String, DFSOutputStream> filesBeingWritten
      = new HashMap<String, DFSOutputStream>();

  /**
   * Same as this(NameNode.getAddress(conf), conf);
   * @see #DFSClient(InetSocketAddress, Configuration)
   * @deprecated Deprecated at 0.21
   */
  @Deprecated
  public DFSClient(Configuration conf) throws IOException {
    this(NameNode.getAddress(conf), conf);
  }
  
  public DFSClient(InetSocketAddress address, Configuration conf) throws IOException {
    this(NameNode.getUri(address), conf);
  }

  /**
   * Same as this(nameNodeAddr, conf, null);
   * @see #DFSClient(InetSocketAddress, Configuration, org.apache.hadoop.fs.FileSystem.Statistics)
   */
  public DFSClient(URI nameNodeAddr, Configuration conf
      ) throws IOException {
    this(nameNodeAddr, conf, null);
  }

  /**
   * Same as this(nameNodeAddr, null, conf, stats);
   * @see #DFSClient(InetSocketAddress, ClientProtocol, Configuration, org.apache.hadoop.fs.FileSystem.Statistics) 
   */
  public DFSClient(URI nameNodeAddr, Configuration conf,
                   FileSystem.Statistics stats)
    throws IOException {
    this(nameNodeAddr, null, conf, stats);
  }
  
  /** 
   * Create a new DFSClient connected to the given nameNodeAddr or rpcNamenode.
   * Exactly one of nameNodeAddr or rpcNamenode must be null.
   */
  DFSClient(URI nameNodeUri, ClientProtocol rpcNamenode,
      Configuration conf, FileSystem.Statistics stats)
    throws IOException {
    // Copy only the required DFSClient configuration
    this.dfsClientConf = new Conf(conf);
    this.conf = conf;
    this.stats = stats;
    this.socketFactory = NetUtils.getSocketFactory(conf, ClientProtocol.class);
    this.dtpReplaceDatanodeOnFailure = ReplaceDatanodeOnFailure.get(conf);

    // The hdfsTimeout is currently the same as the ipc timeout 
    this.hdfsTimeout = Client.getTimeout(conf);
    this.ugi = UserGroupInformation.getCurrentUser();
    
    final String authority = nameNodeUri == null? "null": nameNodeUri.getAuthority();
    this.leaserenewer = LeaseRenewer.getInstance(authority, ugi, this);
    this.clientName = leaserenewer.getClientName(dfsClientConf.taskId);
    
    this.socketCache = new SocketCache(dfsClientConf.socketCacheCapacity);
    
    Class<?> failoverProxyProviderClass = getFailoverProxyProviderClass(authority, conf);
    
    if (nameNodeUri != null && failoverProxyProviderClass != null) {
      FailoverProxyProvider failoverProxyProvider = (FailoverProxyProvider)
          ReflectionUtils.newInstance(failoverProxyProviderClass, conf);
      this.namenode = (ClientProtocol)RetryProxy.create(ClientProtocol.class,
          failoverProxyProvider, RetryPolicies.failoverOnNetworkException(1));
    } else if (nameNodeUri != null && rpcNamenode == null) {
      this.namenode = DFSUtil.createNamenode(NameNode.getAddress(nameNodeUri), conf);
    } else if (nameNodeUri == null && rpcNamenode != null) {
      //This case is used for testing.
      this.namenode = rpcNamenode;
    } else {
      throw new IllegalArgumentException(
          "Expecting exactly one of nameNodeAddr and rpcNamenode being null: "
          + "nameNodeAddr=" + nameNodeUri + ", rpcNamenode=" + rpcNamenode);
    }
  }
  
  private Class<?> getFailoverProxyProviderClass(String authority, Configuration conf)
      throws IOException {
    String configKey = DFS_CLIENT_FAILOVER_PROXY_PROVIDER_KEY_PREFIX + "." + authority;
    try {
      return conf.getClass(configKey, null);
    } catch (RuntimeException e) {
      if (e.getCause() instanceof ClassNotFoundException) {
        throw new IOException("Could not load failover proxy provider class "
            + conf.get(configKey) + " which is configured for authority " + authority,
            e);
      } else {
        throw e;
      }
    }
  }

  /**
   * Return the number of times the client should go back to the namenode
   * to retrieve block locations when reading.
   */
  int getMaxBlockAcquireFailures() {
    return dfsClientConf.maxBlockAcquireFailures;
  }

  /**
   * Return the timeout that clients should use when writing to datanodes.
   * @param numNodes the number of nodes in the pipeline.
   */
  int getDatanodeWriteTimeout(int numNodes) {
    return (dfsClientConf.confTime > 0) ?
      (dfsClientConf.confTime + HdfsServerConstants.WRITE_TIMEOUT_EXTENSION * numNodes) : 0;
  }

  int getDatanodeReadTimeout(int numNodes) {
    return dfsClientConf.socketTimeout > 0 ?
        (HdfsServerConstants.READ_TIMEOUT_EXTENSION * numNodes +
            dfsClientConf.socketTimeout) : 0;
  }
  
  int getHdfsTimeout() {
    return hdfsTimeout;
  }
  
  String getClientName() {
    return clientName;
  }

  void checkOpen() throws IOException {
    if (!clientRunning) {
      IOException result = new IOException("Filesystem closed");
      throw result;
    }
  }

  /** Put a file. */
  void putFileBeingWritten(final String src, final DFSOutputStream out) {
    synchronized(filesBeingWritten) {
      filesBeingWritten.put(src, out);
    }
  }

  /** Remove a file. */
  void removeFileBeingWritten(final String src) {
    synchronized(filesBeingWritten) {
      filesBeingWritten.remove(src);
    }
  }

  /** Is file-being-written map empty? */
  boolean isFilesBeingWrittenEmpty() {
    synchronized(filesBeingWritten) {
      return filesBeingWritten.isEmpty();
    }
  }
  
  /** @return true if the client is running */
  boolean isClientRunning() {
    return clientRunning;
  }

  /** Renew leases */
  void renewLease() throws IOException {
    if (clientRunning && !isFilesBeingWrittenEmpty()) {
      namenode.renewLease(clientName);
    }
  }

  /** Abort and release resources held.  Ignore all errors. */
  void abort() {
    clientRunning = false;
    closeAllFilesBeingWritten(true);
    RPC.stopProxy(namenode); // close connections to the namenode
  }

  /** Close/abort all files being written. */
  private void closeAllFilesBeingWritten(final boolean abort) {
    for(;;) {
      final String src;
      final DFSOutputStream out;
      synchronized(filesBeingWritten) {
        if (filesBeingWritten.isEmpty()) {
          return;
        }
        src = filesBeingWritten.keySet().iterator().next();
        out = filesBeingWritten.remove(src);
      }
      if (out != null) {
        try {
          if (abort) {
            out.abort();
          } else {
            out.close();
          }
        } catch(IOException ie) {
          LOG.error("Failed to " + (abort? "abort": "close") + " file " + src,
              ie);
        }
      }
    }
  }

  /**
   * Close the file system, abandoning all of the leases and files being
   * created and close connections to the namenode.
   */
  public synchronized void close() throws IOException {
    if(clientRunning) {
      closeAllFilesBeingWritten(false);
      clientRunning = false;
      leaserenewer.closeClient(this);
      // close connections to the namenode
      RPC.stopProxy(namenode);
    }
  }

  /**
   * Get the default block size for this cluster
   * @return the default block size in bytes
   */
  public long getDefaultBlockSize() {
    return dfsClientConf.defaultBlockSize;
  }
    
  /**
   * @see ClientProtocol#getPreferredBlockSize(String)
   */
  public long getBlockSize(String f) throws IOException {
    try {
      return namenode.getPreferredBlockSize(f);
    } catch (IOException ie) {
      LOG.warn("Problem getting block size", ie);
      throw ie;
    }
  }

  /**
   * Get server default values for a number of configuration params.
   * @see ClientProtocol#getServerDefaults()
   */
  public FsServerDefaults getServerDefaults() throws IOException {
    long now = System.currentTimeMillis();
    if (now - serverDefaultsLastUpdate > SERVER_DEFAULTS_VALIDITY_PERIOD) {
      serverDefaults = namenode.getServerDefaults();
      serverDefaultsLastUpdate = now;
    }
    return serverDefaults;
  }
  
  /**
   * @see ClientProtocol#getDelegationToken(Text)
   */
  public Token<DelegationTokenIdentifier> getDelegationToken(Text renewer)
      throws IOException {
    Token<DelegationTokenIdentifier> result =
      namenode.getDelegationToken(renewer);
    LOG.info("Created " + DelegationTokenIdentifier.stringifyToken(result));
    return result;
  }

  /**
   * @see ClientProtocol#renewDelegationToken(Token)
   */
  public long renewDelegationToken(Token<DelegationTokenIdentifier> token)
      throws InvalidToken, IOException {
    LOG.info("Renewing " + DelegationTokenIdentifier.stringifyToken(token));
    try {
      return namenode.renewDelegationToken(token);
    } catch (RemoteException re) {
      throw re.unwrapRemoteException(InvalidToken.class,
                                     AccessControlException.class);
    }
  }

  /**
   * @see ClientProtocol#cancelDelegationToken(Token)
   */
  public void cancelDelegationToken(Token<DelegationTokenIdentifier> token)
      throws InvalidToken, IOException {
    LOG.info("Cancelling " + DelegationTokenIdentifier.stringifyToken(token));
    try {
      namenode.cancelDelegationToken(token);
    } catch (RemoteException re) {
      throw re.unwrapRemoteException(InvalidToken.class,
                                     AccessControlException.class);
    }
  }
  
  /**
   * Report corrupt blocks that were discovered by the client.
   * @see ClientProtocol#reportBadBlocks(LocatedBlock[])
   */
  public void reportBadBlocks(LocatedBlock[] blocks) throws IOException {
    namenode.reportBadBlocks(blocks);
  }
  
  public short getDefaultReplication() {
    return dfsClientConf.defaultReplication;
  }

  /**
   * @see ClientProtocol#getBlockLocations(String, long, long)
   */
  static LocatedBlocks callGetBlockLocations(ClientProtocol namenode,
      String src, long start, long length) 
      throws IOException {
    try {
      return namenode.getBlockLocations(src, start, length);
    } catch(RemoteException re) {
      throw re.unwrapRemoteException(AccessControlException.class,
                                     FileNotFoundException.class,
                                     UnresolvedPathException.class);
    }
  }

  /**
   * Recover a file's lease
   * @param src a file's path
   * @return true if the file is already closed
   * @throws IOException
   */
  boolean recoverLease(String src) throws IOException {
    checkOpen();

    try {
      return namenode.recoverLease(src, clientName);
    } catch (RemoteException re) {
      throw re.unwrapRemoteException(FileNotFoundException.class,
                                     AccessControlException.class);
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
    return DFSUtil.locatedBlocks2Locations(blocks);
  }
  
  public DFSInputStream open(String src) 
      throws IOException, UnresolvedLinkException {
    return open(src, dfsClientConf.ioBufferSize, true, null);
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
   * Get the namenode associated with this DFSClient object
   * @return the namenode associated with this DFSClient object
   */
  public ClientProtocol getNamenode() {
    return namenode;
  }
  
  /**
   * Call {@link #create(String, boolean, short, long, Progressable)} with
   * default <code>replication</code> and <code>blockSize<code> and null <code>
   * progress</code>.
   */
  public OutputStream create(String src, boolean overwrite) 
      throws IOException {
    return create(src, overwrite, dfsClientConf.defaultReplication,
        dfsClientConf.defaultBlockSize, null);
  }
    
  /**
   * Call {@link #create(String, boolean, short, long, Progressable)} with
   * default <code>replication</code> and <code>blockSize<code>.
   */
  public OutputStream create(String src, 
                             boolean overwrite,
                             Progressable progress) throws IOException {
    return create(src, overwrite, dfsClientConf.defaultReplication,
        dfsClientConf.defaultBlockSize, progress);
  }
    
  /**
   * Call {@link #create(String, boolean, short, long, Progressable)} with
   * null <code>progress</code>.
   */
  public OutputStream create(String src, 
                             boolean overwrite, 
                             short replication,
                             long blockSize) throws IOException {
    return create(src, overwrite, replication, blockSize, null);
  }

  /**
   * Call {@link #create(String, boolean, short, long, Progressable, int)}
   * with default bufferSize.
   */
  public OutputStream create(String src, boolean overwrite, short replication,
      long blockSize, Progressable progress) throws IOException {
    return create(src, overwrite, replication, blockSize, progress,
        dfsClientConf.ioBufferSize);
  }

  /**
   * Call {@link #create(String, FsPermission, EnumSet, short, long, 
   * Progressable, int)} with default <code>permission</code>
   * {@link FsPermission#getDefault()}.
   * 
   * @param src File name
   * @param overwrite overwrite an existing file if true
   * @param replication replication factor for the file
   * @param blockSize maximum block size
   * @param progress interface for reporting client progress
   * @param buffersize underlying buffersize
   * 
   * @return output stream
   */
  public OutputStream create(String src,
                             boolean overwrite,
                             short replication,
                             long blockSize,
                             Progressable progress,
                             int buffersize)
      throws IOException {
    return create(src, FsPermission.getDefault(),
        overwrite ? EnumSet.of(CreateFlag.CREATE, CreateFlag.OVERWRITE)
            : EnumSet.of(CreateFlag.CREATE), replication, blockSize, progress,
        buffersize);
  }

  /**
   * Call {@link #create(String, FsPermission, EnumSet, boolean, short, 
   * long, Progressable, int)} with <code>createParent</code> set to true.
   */
  public OutputStream create(String src, 
                             FsPermission permission,
                             EnumSet<CreateFlag> flag, 
                             short replication,
                             long blockSize,
                             Progressable progress,
                             int buffersize)
      throws IOException {
    return create(src, permission, flag, true,
        replication, blockSize, progress, buffersize);
  }

  /**
   * Create a new dfs file with the specified block replication 
   * with write-progress reporting and return an output stream for writing
   * into the file.  
   * 
   * @param src File name
   * @param permission The permission of the directory being created.
   *          If null, use default permission {@link FsPermission#getDefault()}
   * @param flag indicates create a new file or create/overwrite an
   *          existing file or append to an existing file
   * @param createParent create missing parent directory if true
   * @param replication block replication
   * @param blockSize maximum block size
   * @param progress interface for reporting client progress
   * @param buffersize underlying buffer size 
   * 
   * @return output stream
   * 
   * @see ClientProtocol#create(String, FsPermission, String, EnumSetWritable,
   * boolean, short, long) for detailed description of exceptions thrown
   */
  public OutputStream create(String src, 
                             FsPermission permission,
                             EnumSet<CreateFlag> flag, 
                             boolean createParent,
                             short replication,
                             long blockSize,
                             Progressable progress,
                             int buffersize)
    throws IOException {
    checkOpen();
    if (permission == null) {
      permission = FsPermission.getDefault();
    }
    FsPermission masked = permission.applyUMask(dfsClientConf.uMask);
    if(LOG.isDebugEnabled()) {
      LOG.debug(src + ": masked=" + masked);
    }
    final DFSOutputStream result = new DFSOutputStream(this, src, masked, flag,
        createParent, replication, blockSize, progress, buffersize,
        dfsClientConf.bytesPerChecksum);
    leaserenewer.put(src, result, this);
    return result;
  }
  
  /**
   * Append to an existing file if {@link CreateFlag#APPEND} is present
   */
  private DFSOutputStream primitiveAppend(String src, EnumSet<CreateFlag> flag,
      int buffersize, Progressable progress) throws IOException {
    if (flag.contains(CreateFlag.APPEND)) {
      HdfsFileStatus stat = getFileInfo(src);
      if (stat == null) { // No file to append to
        // New file needs to be created if create option is present
        if (!flag.contains(CreateFlag.CREATE)) {
          throw new FileNotFoundException("failed to append to non-existent file "
              + src + " on client " + clientName);
        }
        return null;
      }
      return callAppend(stat, src, buffersize, progress);
    }
    return null;
  }
  
  /**
   * Same as {{@link #create(String, FsPermission, EnumSet, short, long,
   *  Progressable, int)} except that the permission
   *  is absolute (ie has already been masked with umask.
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
    CreateFlag.validate(flag);
    DFSOutputStream result = primitiveAppend(src, flag, buffersize, progress);
    if (result == null) {
      result = new DFSOutputStream(this, src, absPermission,
          flag, createParent, replication, blockSize, progress, buffersize,
          bytesPerChecksum);
    }
    leaserenewer.put(src, result, this);
    return result;
  }
  
  /**
   * Creates a symbolic link.
   * 
   * @see ClientProtocol#createSymlink(String, String,FsPermission, boolean) 
   */
  public void createSymlink(String target, String link, boolean createParent)
      throws IOException {
    try {
      FsPermission dirPerm = 
          FsPermission.getDefault().applyUMask(dfsClientConf.uMask); 
      namenode.createSymlink(target, link, dirPerm, createParent);
    } catch (RemoteException re) {
      throw re.unwrapRemoteException(AccessControlException.class,
                                     FileAlreadyExistsException.class, 
                                     FileNotFoundException.class,
                                     ParentNotDirectoryException.class,
                                     NSQuotaExceededException.class, 
                                     DSQuotaExceededException.class,
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

  /** Method to get stream returned by append call */
  private DFSOutputStream callAppend(HdfsFileStatus stat, String src,
      int buffersize, Progressable progress) throws IOException {
    LocatedBlock lastBlock = null;
    try {
      lastBlock = namenode.append(src, clientName);
    } catch(RemoteException re) {
      throw re.unwrapRemoteException(AccessControlException.class,
                                     FileNotFoundException.class,
                                     SafeModeException.class,
                                     DSQuotaExceededException.class,
                                     UnsupportedOperationException.class,
                                     UnresolvedPathException.class);
    }
    return new DFSOutputStream(this, src, buffersize, progress,
        lastBlock, stat, dfsClientConf.bytesPerChecksum);
  }
  
  /**
   * Append to an existing HDFS file.  
   * 
   * @param src file name
   * @param buffersize buffer size
   * @param progress for reporting write-progress; null is acceptable.
   * @param statistics file system statistics; null is acceptable.
   * @return an output stream for writing into the file
   * 
   * @see ClientProtocol#append(String, String) 
   */
  public FSDataOutputStream append(final String src, final int buffersize,
      final Progressable progress, final FileSystem.Statistics statistics
      ) throws IOException {
    final DFSOutputStream out = append(src, buffersize, progress);
    return new FSDataOutputStream(out, statistics, out.getInitialLen());
  }

  private DFSOutputStream append(String src, int buffersize, Progressable progress) 
      throws IOException {
    checkOpen();
    HdfsFileStatus stat = getFileInfo(src);
    if (stat == null) { // No file found
      throw new FileNotFoundException("failed to append to non-existent file "
          + src + " on client " + clientName);
    }
    final DFSOutputStream result = callAppend(stat, src, buffersize, progress);
    leaserenewer.put(src, result, this);
    return result;
  }

  /**
   * Set replication for an existing file.
   * @param src file name
   * @param replication
   * 
   * @see ClientProtocol#setReplication(String, short)
   */
  public boolean setReplication(String src, short replication)
      throws IOException {
    try {
      return namenode.setReplication(src, replication);
    } catch(RemoteException re) {
      throw re.unwrapRemoteException(AccessControlException.class,
                                     FileNotFoundException.class,
                                     SafeModeException.class,
                                     DSQuotaExceededException.class,
                                     UnresolvedPathException.class);
    }
  }

  /**
   * Rename file or directory.
   * @see ClientProtocol#rename(String, String)
   * @deprecated Use {@link #rename(String, String, Options.Rename...)} instead.
   */
  @Deprecated
  public boolean rename(String src, String dst) throws IOException {
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
  public void concat(String trg, String [] srcs) throws IOException {
    checkOpen();
    try {
      namenode.concat(trg, srcs);
    } catch(RemoteException re) {
      throw re.unwrapRemoteException(AccessControlException.class,
                                     UnresolvedPathException.class);
    }
  }
  /**
   * Rename file or directory.
   * @see ClientProtocol#rename2(String, String, Options.Rename...)
   */
  public void rename(String src, String dst, Options.Rename... options)
      throws IOException {
    checkOpen();
    try {
      namenode.rename2(src, dst, options);
    } catch(RemoteException re) {
      throw re.unwrapRemoteException(AccessControlException.class,
                                     DSQuotaExceededException.class,
                                     FileAlreadyExistsException.class,
                                     FileNotFoundException.class,
                                     ParentNotDirectoryException.class,
                                     SafeModeException.class,
                                     NSQuotaExceededException.class,
                                     UnresolvedPathException.class);
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
   *
   * @see ClientProtocol#delete(String, boolean)
   */
  public boolean delete(String src, boolean recursive) throws IOException {
    checkOpen();
    try {
      return namenode.delete(src, recursive);
    } catch(RemoteException re) {
      throw re.unwrapRemoteException(AccessControlException.class,
                                     FileNotFoundException.class,
                                     SafeModeException.class,
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
   * No block locations need to be fetched
   */
  public DirectoryListing listPaths(String src,  byte[] startAfter)
    throws IOException {
    return listPaths(src, startAfter, false);
  }
  
  /**
   * Get a partial listing of the indicated directory
   *
   * Recommend to use HdfsFileStatus.EMPTY_NAME as startAfter
   * if the application wants to fetch a listing starting from
   * the first entry in the directory
   *
   * @see ClientProtocol#getListing(String, byte[], boolean)
   */
  public DirectoryListing listPaths(String src,  byte[] startAfter,
      boolean needLocation) 
    throws IOException {
    checkOpen();
    try {
      return namenode.getListing(src, startAfter, needLocation);
    } catch(RemoteException re) {
      throw re.unwrapRemoteException(AccessControlException.class,
                                     FileNotFoundException.class,
                                     UnresolvedPathException.class);
    }
  }

  /**
   * Get the file info for a specific file or directory.
   * @param src The string representation of the path to the file
   * @return object containing information regarding the file
   *         or null if file not found
   *         
   * @see ClientProtocol#getFileInfo(String) for description of exceptions
   */
  public HdfsFileStatus getFileInfo(String src) throws IOException {
    checkOpen();
    try {
      return namenode.getFileInfo(src);
    } catch(RemoteException re) {
      throw re.unwrapRemoteException(AccessControlException.class,
                                     FileNotFoundException.class,
                                     UnresolvedPathException.class);
    }
  }

  /**
   * Get the file info for a specific file or directory. If src
   * refers to a symlink then the FileStatus of the link is returned.
   * @param src path to a file or directory.
   * 
   * For description of exceptions thrown 
   * @see ClientProtocol#getFileLinkInfo(String)
   */
  public HdfsFileStatus getFileLinkInfo(String src) throws IOException {
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
    return getFileChecksum(src, namenode, socketFactory, dfsClientConf.socketTimeout);    
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
      final ExtendedBlock block = lb.getBlock();
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
                                       HdfsConstants.SMALL_BUFFER_SIZE));
          in = new DataInputStream(NetUtils.getInputStream(sock));

          if (LOG.isDebugEnabled()) {
            LOG.debug("write to " + datanodes[j].getName() + ": "
                + Op.BLOCK_CHECKSUM + ", block=" + block);
          }
          // get block MD5
          new Sender(out).blockChecksum(block, lb.getBlockToken());

          final BlockOpResponseProto reply =
            BlockOpResponseProto.parseFrom(HdfsProtoUtil.vintPrefixed(in));

          if (reply.getStatus() != Status.SUCCESS) {
            if (reply.getStatus() == Status.ERROR_ACCESS_TOKEN
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
          
          OpBlockChecksumResponseProto checksumData =
            reply.getChecksumResponse();

          //read byte-per-checksum
          final int bpc = checksumData.getBytesPerCrc();
          if (i == 0) { //first block
            bytesPerCRC = bpc;
          }
          else if (bpc != bytesPerCRC) {
            throw new IOException("Byte-per-checksum not matched: bpc=" + bpc
                + " but bytesPerCRC=" + bytesPerCRC);
          }
          
          //read crc-per-block
          final long cpb = checksumData.getCrcPerBlock();
          if (locatedblocks.size() > 1 && i == 0) {
            crcPerBlock = cpb;
          }

          //read md5
          final MD5Hash md5 = new MD5Hash(
              checksumData.getMd5().toByteArray());
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
   * 
   * @see ClientProtocol#setPermission(String, FsPermission)
   */
  public void setPermission(String src, FsPermission permission)
      throws IOException {
    checkOpen();
    try {
      namenode.setPermission(src, permission);
    } catch(RemoteException re) {
      throw re.unwrapRemoteException(AccessControlException.class,
                                     FileNotFoundException.class,
                                     SafeModeException.class,
                                     UnresolvedPathException.class);
    }
  }

  /**
   * Set file or directory owner.
   * @param src path name.
   * @param username user id.
   * @param groupname user group.
   * 
   * @see ClientProtocol#setOwner(String, String, String)
   */
  public void setOwner(String src, String username, String groupname)
      throws IOException {
    checkOpen();
    try {
      namenode.setOwner(src, username, groupname);
    } catch(RemoteException re) {
      throw re.unwrapRemoteException(AccessControlException.class,
                                     FileNotFoundException.class,
                                     SafeModeException.class,
                                     UnresolvedPathException.class);                                   
    }
  }

  /**
   * @see ClientProtocol#getStats()
   */
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
  
  /**
   * @return a list in which each entry describes a corrupt file/block
   * @throws IOException
   */
  public CorruptFileBlocks listCorruptFileBlocks(String path,
                                                 String cookie)
    throws IOException {
    return namenode.listCorruptFileBlocks(path, cookie);
  }

  public DatanodeInfo[] datanodeReport(DatanodeReportType type)
  throws IOException {
    return namenode.getDatanodeReport(type);
  }
    
  /**
   * Enter, leave or get safe mode.
   * 
   * @see ClientProtocol#setSafeMode(HdfsConstants.SafeModeAction)
   */
  public boolean setSafeMode(SafeModeAction action) throws IOException {
    return namenode.setSafeMode(action);
  }

  /**
   * Save namespace image.
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
   * 
   * @see ClientProtocol#restoreFailedStorage(String arg)
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
   * 
   * @see ClientProtocol#metaSave(String)
   */
  public void metaSave(String pathname) throws IOException {
    namenode.metaSave(pathname);
  }

  /**
   * Requests the namenode to tell all datanodes to use a new, non-persistent
   * bandwidth value for dfs.balance.bandwidthPerSec.
   * See {@link ClientProtocol#setBalancerBandwidth(long)} 
   * for more details.
   * 
   * @see ClientProtocol#setBalancerBandwidth(long)
   */
  public void setBalancerBandwidth(long bandwidth) throws IOException {
    namenode.setBalancerBandwidth(bandwidth);
  }
    
  /**
   * @see ClientProtocol#finalizeUpgrade()
   */
  public void finalizeUpgrade() throws IOException {
    namenode.finalizeUpgrade();
  }

  /**
   * @see ClientProtocol#distributedUpgradeProgress(HdfsConstants.UpgradeAction)
   */
  public UpgradeStatusReport distributedUpgradeProgress(UpgradeAction action)
      throws IOException {
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
   * 
   * @return True if the operation success.
   * 
   * @see ClientProtocol#mkdirs(String, FsPermission, boolean)
   */
  public boolean mkdirs(String src, FsPermission permission,
      boolean createParent) throws IOException {
    checkOpen();
    if (permission == null) {
      permission = FsPermission.getDefault();
    }
    FsPermission masked = permission.applyUMask(dfsClientConf.uMask);
    if(LOG.isDebugEnabled()) {
      LOG.debug(src + ": masked=" + masked);
    }
    try {
      return namenode.mkdirs(src, masked, createParent);
    } catch(RemoteException re) {
      throw re.unwrapRemoteException(AccessControlException.class,
                                     InvalidPathException.class,
                                     FileAlreadyExistsException.class,
                                     FileNotFoundException.class,
                                     ParentNotDirectoryException.class,
                                     SafeModeException.class,
                                     NSQuotaExceededException.class,
                                     UnresolvedPathException.class);
    }
  }
  
  /**
   * Same {{@link #mkdirs(String, FsPermission, boolean)} except
   * that the permissions has already been masked against umask.
   */
  public boolean primitiveMkdir(String src, FsPermission absPermission)
    throws IOException {
    checkOpen();
    if (absPermission == null) {
      absPermission = 
        FsPermission.getDefault().applyUMask(dfsClientConf.uMask);
    } 

    if(LOG.isDebugEnabled()) {
      LOG.debug(src + ": masked=" + absPermission);
    }
    try {
      return namenode.mkdirs(src, absPermission, true);
    } catch(RemoteException re) {
      throw re.unwrapRemoteException(AccessControlException.class,
                                     NSQuotaExceededException.class,
                                     DSQuotaExceededException.class,
                                     UnresolvedPathException.class);
    }
  }

  /**
   * Get {@link ContentSummary} rooted at the specified directory.
   * @param path The string representation of the path
   * 
   * @see ClientProtocol#getContentSummary(String)
   */
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
   * @see ClientProtocol#setQuota(String, long, long)
   */
  void setQuota(String src, long namespaceQuota, long diskspaceQuota) 
      throws IOException {
    // sanity check
    if ((namespaceQuota <= 0 && namespaceQuota != HdfsConstants.QUOTA_DONT_SET &&
         namespaceQuota != HdfsConstants.QUOTA_RESET) ||
        (diskspaceQuota <= 0 && diskspaceQuota != HdfsConstants.QUOTA_DONT_SET &&
         diskspaceQuota != HdfsConstants.QUOTA_RESET)) {
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
   * 
   * @see ClientProtocol#setTimes(String, long, long)
   */
  public void setTimes(String src, long mtime, long atime) throws IOException {
    checkOpen();
    try {
      namenode.setTimes(src, mtime, atime);
    } catch(RemoteException re) {
      throw re.unwrapRemoteException(AccessControlException.class,
                                     FileNotFoundException.class,
                                     UnresolvedPathException.class);
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
    public ExtendedBlock getCurrentBlock() {
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

  void reportChecksumFailure(String file, ExtendedBlock blk, DatanodeInfo dn) {
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
          + ".  Error repairing corrupt blocks.  Bad blocks remain.", ie);
    }
  }

  /** {@inheritDoc} */
  public String toString() {
    return getClass().getSimpleName() + "[clientName=" + clientName
        + ", ugi=" + ugi + "]"; 
  }
}
