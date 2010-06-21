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

import java.io.*;
import java.net.*;
import java.util.ArrayList;
import java.util.EnumSet;

import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.FSConstants;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.DirectoryListing;
import org.apache.hadoop.hdfs.protocol.FSConstants.DatanodeReportType;
import org.apache.hadoop.hdfs.protocol.FSConstants.UpgradeAction;
import org.apache.hadoop.hdfs.server.common.UpgradeStatusReport;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.DFSClient.DFSDataInputStream;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.SecretManager.InvalidToken;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.fs.Options;


/****************************************************************
 * Implementation of the abstract FileSystem for the DFS system.
 * This object is the way end-user code interacts with a Hadoop
 * DistributedFileSystem.
 *
 *****************************************************************/
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class DistributedFileSystem extends FileSystem {
  private Path workingDir;
  private URI uri;

  DFSClient dfs;
  private boolean verifyChecksum = true;
  
  static{
    Configuration.addDefaultResource("hdfs-default.xml");
    Configuration.addDefaultResource("hdfs-site.xml");
  }

  public DistributedFileSystem() {
  }

  @Deprecated
  public DistributedFileSystem(InetSocketAddress namenode,
    Configuration conf) throws IOException {
    initialize(NameNode.getUri(namenode), conf);
  }

  @Override
  public URI getUri() { return uri; }

  @Override
  public void initialize(URI uri, Configuration conf) throws IOException {
    super.initialize(uri, conf);
    setConf(conf);

    String host = uri.getHost();
    if (host == null) {
      throw new IOException("Incomplete HDFS URI, no host: "+ uri);
    }

    InetSocketAddress namenode = NameNode.getAddress(uri.getAuthority());
    this.dfs = new DFSClient(namenode, conf, statistics);
    this.uri = URI.create(FSConstants.HDFS_URI_SCHEME + "://" + uri.getAuthority());
    this.workingDir = getHomeDirectory();
  }

  /** Permit paths which explicitly specify the default port. */
  @Override
  protected void checkPath(Path path) {
    URI thisUri = this.getUri();
    URI thatUri = path.toUri();
    String thatAuthority = thatUri.getAuthority();
    if (thatUri.getScheme() != null
        && thatUri.getScheme().equalsIgnoreCase(thisUri.getScheme())
        && thatUri.getPort() == NameNode.DEFAULT_PORT
        && (thisUri.getPort() == -1 || 
            thisUri.getPort() == NameNode.DEFAULT_PORT)
        && thatAuthority.substring(0,thatAuthority.indexOf(":"))
        .equalsIgnoreCase(thisUri.getAuthority()))
      return;
    super.checkPath(path);
  }

  /** Normalize paths that explicitly specify the default port. */
  @Override
  public Path makeQualified(Path path) {
    URI thisUri = this.getUri();
    URI thatUri = path.toUri();
    String thatAuthority = thatUri.getAuthority();
    if (thatUri.getScheme() != null
        && thatUri.getScheme().equalsIgnoreCase(thisUri.getScheme())
        && thatUri.getPort() == NameNode.DEFAULT_PORT
        && thisUri.getPort() == -1
        && thatAuthority.substring(0,thatAuthority.indexOf(":"))
        .equalsIgnoreCase(thisUri.getAuthority())) {
      path = new Path(thisUri.getScheme(), thisUri.getAuthority(),
                      thatUri.getPath());
    }
    return super.makeQualified(path);
  }

  @Override
  public Path getWorkingDirectory() {
    return workingDir;
  }

  @Override
  public long getDefaultBlockSize() {
    return dfs.getDefaultBlockSize();
  }

  @Override
  public short getDefaultReplication() {
    return dfs.getDefaultReplication();
  }

  private Path makeAbsolute(Path f) {
    if (f.isAbsolute()) {
      return f;
    } else {
      return new Path(workingDir, f);
    }
  }

  @Override
  public void setWorkingDirectory(Path dir) {
    String result = makeAbsolute(dir).toUri().getPath();
    if (!DFSUtil.isValidName(result)) {
      throw new IllegalArgumentException("Invalid DFS directory name " + 
                                         result);
    }
    workingDir = makeAbsolute(dir);
  }

  /** {@inheritDoc} */
  @Override
  public Path getHomeDirectory() {
    return new Path("/user/" + dfs.ugi.getShortUserName()).makeQualified(this);
  }

  private String getPathName(Path file) {
    checkPath(file);
    String result = makeAbsolute(file).toUri().getPath();
    if (!DFSUtil.isValidName(result)) {
      throw new IllegalArgumentException("Pathname " + result + " from " +
                                         file+" is not a valid DFS filename.");
    }
    return result;
  }
  
  @Override
  public BlockLocation[] getFileBlockLocations(FileStatus file, long start,
      long len) throws IOException {
    if (file == null) {
      return null;
    }
    return dfs.getBlockLocations(getPathName(file.getPath()), start, len);
  }
  
  @Override
  public BlockLocation[] getFileBlockLocations(Path p, 
      long start, long len) throws IOException {
    return dfs.getBlockLocations(getPathName(p), start, len);

  }

  @Override
  public void setVerifyChecksum(boolean verifyChecksum) {
    this.verifyChecksum = verifyChecksum;
  }

  @SuppressWarnings("deprecation")
  @Override
  public FSDataInputStream open(Path f, int bufferSize) throws IOException {
    return new DFSClient.DFSDataInputStream(
          dfs.open(getPathName(f), bufferSize, verifyChecksum, statistics));
  }

  /** This optional operation is not yet supported. */
  @Override
  public FSDataOutputStream append(Path f, int bufferSize,
      Progressable progress) throws IOException {

    DFSOutputStream op = (DFSOutputStream)dfs.append(getPathName(f), bufferSize, progress);
    return new FSDataOutputStream(op, statistics, op.getInitialLen());
  }

  @Override
  public FSDataOutputStream create(Path f, FsPermission permission,
    boolean overwrite, int bufferSize, short replication, long blockSize,
    Progressable progress) throws IOException {

    return new FSDataOutputStream(dfs.create(getPathName(f), permission,
        overwrite ? EnumSet.of(CreateFlag.OVERWRITE) : EnumSet.of(CreateFlag.CREATE),
        replication, blockSize, progress, bufferSize),
        statistics);
  }
  
  @SuppressWarnings("deprecation")
  @Override
  protected FSDataOutputStream primitiveCreate(Path f,
    FsPermission absolutePermission, EnumSet<CreateFlag> flag, int bufferSize,
    short replication, long blockSize, Progressable progress,
    int bytesPerChecksum) throws IOException {
    return new FSDataOutputStream(dfs.primitiveCreate(getPathName(f),
        absolutePermission, flag, true, replication, blockSize,
        progress, bufferSize, bytesPerChecksum),statistics);
   } 

  /**
   * Same as create(), except fails if parent directory doesn't already exist.
   */
  public FSDataOutputStream createNonRecursive(Path f, FsPermission permission,
      EnumSet<CreateFlag> flag, int bufferSize, short replication,
      long blockSize, Progressable progress) throws IOException {

    return new FSDataOutputStream(dfs.create(getPathName(f), permission, flag,
        false, replication, blockSize, progress, bufferSize), statistics);
  }

  @Override
  public boolean setReplication(Path src, 
                                short replication
                               ) throws IOException {
    return dfs.setReplication(getPathName(src), replication);
  }
  
  /**
   * THIS IS DFS only operations, it is not part of FileSystem
   * move blocks from srcs to trg
   * and delete srcs afterwards
   * all blocks should be the same size
   * @param trg existing file to append to
   * @param psrcs list of files (same block size, same replication)
   * @throws IOException
   */
  public void concat(Path trg, Path [] psrcs) throws IOException {
    String [] srcs = new String [psrcs.length];
    for(int i=0; i<psrcs.length; i++) {
      srcs[i] = getPathName(psrcs[i]);
    }
    dfs.concat(getPathName(trg), srcs);
  }

  /** {@inheritDoc} */
  @SuppressWarnings("deprecation")
  @Override
  public boolean rename(Path src, Path dst) throws IOException {
    return dfs.rename(getPathName(src), getPathName(dst));
  }

  /** 
   * {@inheritDoc}
   * This rename operation is guaranteed to be atomic.
   */
  @SuppressWarnings("deprecation")
  @Override
  public void rename(Path src, Path dst, Options.Rename... options) throws IOException {
    dfs.rename(getPathName(src), getPathName(dst), options);
  }
  
  @Override
  public boolean delete(Path f, boolean recursive) throws IOException {
   return dfs.delete(getPathName(f), recursive);
  }
  
  /** {@inheritDoc} */
  @Override
  public ContentSummary getContentSummary(Path f) throws IOException {
    return dfs.getContentSummary(getPathName(f));
  }

  /** Set a directory's quotas
   * @see org.apache.hadoop.hdfs.protocol.ClientProtocol#setQuota(String, long, long) 
   */
  public void setQuota(Path src, long namespaceQuota, long diskspaceQuota) 
                       throws IOException {
    dfs.setQuota(getPathName(src), namespaceQuota, diskspaceQuota);
  }
  
  private FileStatus makeQualified(HdfsFileStatus f, Path parent) {
    return new FileStatus(f.getLen(), f.isDir(), f.getReplication(),
        f.getBlockSize(), f.getModificationTime(),
        f.getAccessTime(),
        f.getPermission(), f.getOwner(), f.getGroup(),
        (f.getFullPath(parent)).makeQualified(
            getUri(), getWorkingDirectory())); // fully-qualify path
  }

  /**
   * List all the entries of a directory
   *
   * Note that this operation is not atomic for a large directory.
   * The entries of a directory may be fetched from NameNode multiple times.
   * It only guarantees that  each name occurs once if a directory
   * undergoes changes between the calls.
   */
  @Override
  public FileStatus[] listStatus(Path p) throws IOException {
    String src = getPathName(p);

    // fetch the first batch of entries in the directory
    DirectoryListing thisListing = dfs.listPaths(
        src, HdfsFileStatus.EMPTY_NAME);

    if (thisListing == null) { // the directory does not exist
      throw new FileNotFoundException("File " + p + " does not exist.");
    }
    
    HdfsFileStatus[] partialListing = thisListing.getPartialListing();
    if (!thisListing.hasMore()) { // got all entries of the directory
      FileStatus[] stats = new FileStatus[partialListing.length];
      for (int i = 0; i < partialListing.length; i++) {
        stats[i] = makeQualified(partialListing[i], p);
      }
      return stats;
    }

    // The directory size is too big that it needs to fetch more
    // estimate the total number of entries in the directory
    int totalNumEntries =
      partialListing.length + thisListing.getRemainingEntries();
    ArrayList<FileStatus> listing =
      new ArrayList<FileStatus>(totalNumEntries);
    // add the first batch of entries to the array list
    for (HdfsFileStatus fileStatus : partialListing) {
      listing.add(makeQualified(fileStatus, p));
    }
 
    // now fetch more entries
    do {
      thisListing = dfs.listPaths(src, thisListing.getLastName());
 
      if (thisListing == null) { // the directory is deleted
        throw new FileNotFoundException("File " + p + " does not exist.");
      }
 
      partialListing = thisListing.getPartialListing();
      for (HdfsFileStatus fileStatus : partialListing) {
        listing.add(makeQualified(fileStatus, p));
      }
    } while (thisListing.hasMore());
 
    return listing.toArray(new FileStatus[listing.size()]);
  }

  /**
   * Create a directory with given name and permission, only when
   * parent directory exists.
   */
  public boolean mkdir(Path f, FsPermission permission) throws IOException {
    return dfs.mkdirs(getPathName(f), permission, false);
  }

  @Override
  public boolean mkdirs(Path f, FsPermission permission) throws IOException {
    return dfs.mkdirs(getPathName(f), permission, true);
  }

  @SuppressWarnings("deprecation")
  @Override
  protected boolean primitiveMkdir(Path f, FsPermission absolutePermission)
    throws IOException {
    return dfs.primitiveMkdir(getPathName(f), absolutePermission);
  }

  /** {@inheritDoc} */
  @Override
  public void close() throws IOException {
    try {
      super.processDeleteOnExit();
      dfs.close();
    } finally {
      super.close();
    }
  }

  @Override
  public String toString() {
    return "DFS[" + dfs + "]";
  }

  public DFSClient getClient() {
    return dfs;
  }        
  
  /** @deprecated Use {@link org.apache.hadoop.fs.FsStatus} instead */
  @InterfaceAudience.Private
  @Deprecated
  public static class DiskStatus extends FsStatus {
    public DiskStatus(FsStatus stats) {
      super(stats.getCapacity(), stats.getUsed(), stats.getRemaining());
    }

    public DiskStatus(long capacity, long dfsUsed, long remaining) {
      super(capacity, dfsUsed, remaining);
    }

    public long getDfsUsed() {
      return super.getUsed();
    }
  }
  
  /** {@inheritDoc} */
  @Override
  public FsStatus getStatus(Path p) throws IOException {
    return dfs.getDiskStatus();
  }

  /** Return the disk usage of the filesystem, including total capacity,
   * used space, and remaining space 
   * @deprecated Use {@link org.apache.hadoop.fs.FileSystem#getStatus()} 
   * instead */
   @Deprecated
  public DiskStatus getDiskStatus() throws IOException {
    return new DiskStatus(dfs.getDiskStatus());
  }
  
  /** Return the total raw capacity of the filesystem, disregarding
   * replication.
   * @deprecated Use {@link org.apache.hadoop.fs.FileSystem#getStatus()} 
   * instead */
   @Deprecated
  public long getRawCapacity() throws IOException{
    return dfs.getDiskStatus().getCapacity();
  }

  /** Return the total raw used space in the filesystem, disregarding
   * replication.
   * @deprecated Use {@link org.apache.hadoop.fs.FileSystem#getStatus()} 
   * instead */
   @Deprecated
  public long getRawUsed() throws IOException{
    return dfs.getDiskStatus().getUsed();
  }
   
  /**
   * Returns count of blocks with no good replicas left. Normally should be
   * zero.
   * 
   * @throws IOException
   */
  public long getMissingBlocksCount() throws IOException {
    return dfs.getMissingBlocksCount();
  }

  /**
   * Returns count of blocks with one of more replica missing.
   * 
   * @throws IOException
   */
  public long getUnderReplicatedBlocksCount() throws IOException {
    return dfs.getUnderReplicatedBlocksCount();
  }

  /**
   * Returns count of blocks with at least one replica marked corrupt.
   * 
   * @throws IOException
   */
  public long getCorruptBlocksCount() throws IOException {
    return dfs.getCorruptBlocksCount();
  }

  /** Return statistics for each datanode. */
  public DatanodeInfo[] getDataNodeStats() throws IOException {
    return dfs.datanodeReport(DatanodeReportType.ALL);
  }

  /**
   * Enter, leave or get safe mode.
   *  
   * @see org.apache.hadoop.hdfs.protocol.ClientProtocol#setSafeMode(
   *    FSConstants.SafeModeAction)
   */
  public boolean setSafeMode(FSConstants.SafeModeAction action) 
  throws IOException {
    return dfs.setSafeMode(action);
  }

  /**
   * Save namespace image.
   * 
   * @see org.apache.hadoop.hdfs.protocol.ClientProtocol#saveNamespace()
   */
  public void saveNamespace() throws AccessControlException, IOException {
    dfs.saveNamespace();
  }

  /**
   * enable/disable/check restoreFaileStorage
   * 
   * @see org.apache.hadoop.hdfs.protocol.ClientProtocol#restoreFailedStorage(String arg)
   */
  public boolean restoreFailedStorage(String arg) throws AccessControlException {
    return dfs.restoreFailedStorage(arg);
  }
  

  /**
   * Refreshes the list of hosts and excluded hosts from the configured 
   * files.  
   */
  public void refreshNodes() throws IOException {
    dfs.refreshNodes();
  }

  /**
   * Finalize previously upgraded files system state.
   * @throws IOException
   */
  public void finalizeUpgrade() throws IOException {
    dfs.finalizeUpgrade();
  }

  public UpgradeStatusReport distributedUpgradeProgress(UpgradeAction action
  ) throws IOException {
    return dfs.distributedUpgradeProgress(action);
  }

  /*
   * Requests the namenode to dump data strcutures into specified 
   * file.
   */
  public void metaSave(String pathname) throws IOException {
    dfs.metaSave(pathname);
  }

  /** {@inheritDoc} */
  @Override
  public FsServerDefaults getServerDefaults() throws IOException {
    return dfs.getServerDefaults();
  }

  /**
   * We need to find the blocks that didn't match.  Likely only one 
   * is corrupt but we will report both to the namenode.  In the future,
   * we can consider figuring out exactly which block is corrupt.
   */
  public boolean reportChecksumFailure(Path f, 
    FSDataInputStream in, long inPos, 
    FSDataInputStream sums, long sumsPos) {
    
    if(!(in instanceof DFSDataInputStream && sums instanceof DFSDataInputStream))
      throw new IllegalArgumentException("Input streams must be types " +
                                         "of DFSDataInputStream");
    
    LocatedBlock lblocks[] = new LocatedBlock[2];

    // Find block in data stream.
    DFSClient.DFSDataInputStream dfsIn = (DFSClient.DFSDataInputStream) in;
    Block dataBlock = dfsIn.getCurrentBlock();
    if (dataBlock == null) {
      LOG.error("Error: Current block in data stream is null! ");
      return false;
    }
    DatanodeInfo[] dataNode = {dfsIn.getCurrentDatanode()}; 
    lblocks[0] = new LocatedBlock(dataBlock, dataNode);
    LOG.info("Found checksum error in data stream at block="
        + dataBlock + " on datanode="
        + dataNode[0].getName());

    // Find block in checksum stream
    DFSClient.DFSDataInputStream dfsSums = (DFSClient.DFSDataInputStream) sums;
    Block sumsBlock = dfsSums.getCurrentBlock();
    if (sumsBlock == null) {
      LOG.error("Error: Current block in checksum stream is null! ");
      return false;
    }
    DatanodeInfo[] sumsNode = {dfsSums.getCurrentDatanode()}; 
    lblocks[1] = new LocatedBlock(sumsBlock, sumsNode);
    LOG.info("Found checksum error in checksum stream at block="
        + sumsBlock + " on datanode="
        + sumsNode[0].getName());

    // Ask client to delete blocks.
    dfs.reportChecksumFailure(f.toString(), lblocks);

    return true;
  }

  /**
   * Returns the stat information about the file.
   * @throws FileNotFoundException if the file does not exist.
   */
  @Override
  public FileStatus getFileStatus(Path f) throws IOException {
    HdfsFileStatus fi = dfs.getFileInfo(getPathName(f));
    if (fi != null) {
      return makeQualified(fi, f);
    } else {
      throw new FileNotFoundException("File does not exist: " + f);
    }
  }

  /** {@inheritDoc} */
  @Override
  public MD5MD5CRC32FileChecksum getFileChecksum(Path f) throws IOException {
    return dfs.getFileChecksum(getPathName(f));
  }

  /** {@inheritDoc }*/
  @Override
  public void setPermission(Path p, FsPermission permission
      ) throws IOException {
    dfs.setPermission(getPathName(p), permission);
  }

  /** {@inheritDoc }*/
  @Override
  public void setOwner(Path p, String username, String groupname
      ) throws IOException {
    if (username == null && groupname == null) {
      throw new IOException("username == null && groupname == null");
    }
    dfs.setOwner(getPathName(p), username, groupname);
  }

  /** {@inheritDoc }*/
  @Override
  public void setTimes(Path p, long mtime, long atime
      ) throws IOException {
    dfs.setTimes(getPathName(p), mtime, atime);
  }
  
  /** 
   * Delegation Token Operations
   * These are DFS only operations.
   */
  
  /**
   * Get a valid Delegation Token.
   * 
   * @param renewer Name of the designated renewer for the token
   * @return Token<DelegationTokenIdentifier>
   * @throws IOException
   */
  public Token<DelegationTokenIdentifier> getDelegationToken(Text renewer)
      throws IOException {
    return dfs.getDelegationToken(renewer);
  }

  /**
   * Renew an existing delegation token.
   * 
   * @param token delegation token obtained earlier
   * @return the new expiration time
   * @throws IOException
   */
  public long renewDelegationToken(Token<DelegationTokenIdentifier> token)
      throws InvalidToken, IOException {
    return dfs.renewDelegationToken(token);
  }

  /**
   * Cancel an existing delegation token.
   * 
   * @param token delegation token
   * @throws IOException
   */
  public void cancelDelegationToken(Token<DelegationTokenIdentifier> token)
      throws IOException {
    dfs.cancelDelegationToken(token);
  }
}
