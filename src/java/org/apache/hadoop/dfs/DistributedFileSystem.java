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

import java.io.*;
import java.net.*;

import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.dfs.FSConstants.DatanodeReportType;
import org.apache.hadoop.dfs.FSConstants.UpgradeAction;
import org.apache.hadoop.util.*;

/****************************************************************
 * Implementation of the abstract FileSystem for the DFS system.
 * This object is the way end-user code interacts with a Hadoop
 * DistributedFileSystem.
 *
 *****************************************************************/
public class DistributedFileSystem extends FileSystem {
  private Path workingDir;
  private URI uri;

  DFSClient dfs;
  private boolean verifyChecksum = true;

  public DistributedFileSystem() {
  }

  /** @deprecated */
  public DistributedFileSystem(InetSocketAddress namenode,
    Configuration conf) throws IOException {
    initialize(URI.create("hdfs://"+
                          namenode.getHostName()+":"+
                          namenode.getPort()),
                          conf);
  }

  /** @deprecated */
  public String getName() { return uri.getAuthority(); }

  public URI getUri() { return uri; }

  public void initialize(URI uri, Configuration conf) throws IOException {
    setConf(conf);
    String host = uri.getHost();
    int port = uri.getPort();
    if (host == null || port == -1) {
      throw new IOException("Incomplete HDFS URI, no host/port: "+ uri);
    }
    this.dfs = new DFSClient(new InetSocketAddress(host, port), conf,
                             statistics);
    this.uri = URI.create("hdfs://"+host+":"+port);
    this.workingDir = getHomeDirectory();
  }

  public Path getWorkingDirectory() {
    return workingDir;
  }

  public long getDefaultBlockSize() {
    return dfs.getDefaultBlockSize();
  }

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

  public void setWorkingDirectory(Path dir) {
    String result = makeAbsolute(dir).toUri().getPath();
    if (!FSNamesystem.isValidName(result)) {
      throw new IllegalArgumentException("Invalid DFS directory name " + 
                                         result);
    }
    workingDir = makeAbsolute(dir);
  }

  /** {@inheritDoc} */
  public Path getHomeDirectory() {
    return new Path("/user/" + dfs.ugi.getUserName()).makeQualified(this);
  }

  private String getPathName(Path file) {
    checkPath(file);
    String result = makeAbsolute(file).toUri().getPath();
    if (!FSNamesystem.isValidName(result)) {
      throw new IllegalArgumentException("Pathname " + result + " from " +
                                         file+" is not a valid DFS filename.");
    }
    return result;
  }
  

  public BlockLocation[] getFileBlockLocations(Path f, long start,
      long len) throws IOException {
    return dfs.getBlockLocations(getPathName(f), start, len);
  }

  public void setVerifyChecksum(boolean verifyChecksum) {
    this.verifyChecksum = verifyChecksum;
  }

  public FSDataInputStream open(Path f, int bufferSize) throws IOException {
    try {
      return new DFSClient.DFSDataInputStream(
          dfs.open(getPathName(f), bufferSize, verifyChecksum, statistics));
    } catch(RemoteException e) {
      if (IOException.class.getName().equals(e.getClassName()) &&
          e.getMessage().startsWith(
              "java.io.IOException: Cannot open filename")) {
          // non-existent path
          FileNotFoundException ne = new FileNotFoundException("File " + f + " does not exist.");
          throw (FileNotFoundException) ne.initCause(e);
      } else {
        throw e;      // unexpected exception
      }
    }
  }

  public FSDataOutputStream create(Path f, FsPermission permission,
    boolean overwrite,
    int bufferSize, short replication, long blockSize,
    Progressable progress) throws IOException {

    return new FSDataOutputStream
       (dfs.create(getPathName(f), permission,
                   overwrite, replication, blockSize, progress, bufferSize),
        statistics);
  }

  public boolean setReplication(Path src, 
                                short replication
                               ) throws IOException {
    return dfs.setReplication(getPathName(src), replication);
  }

  /**
   * Rename files/dirs
   */
  public boolean rename(Path src, Path dst) throws IOException {
    return dfs.rename(getPathName(src), getPathName(dst));
  }

  /**
   * Get rid of Path f, whether a true file or dir.
   */
  @Deprecated
  public boolean delete(Path f) throws IOException {
    return dfs.delete(getPathName(f));
  }
  
  /**
   * requires a boolean check to delete a non 
   * empty directory recursively.
   */
  public boolean delete(Path f, boolean recursive) throws IOException {
   return dfs.delete(getPathName(f), recursive);
  }
  
  /** {@inheritDoc} */
  @Deprecated
  public long getContentLength(Path f) throws IOException {
    // If it is a directory, then issue a getContentLength
    // RPC to find the size of the entire subtree in one call.
    //
    if (f instanceof DfsPath) {
      DfsPath dfspath = (DfsPath)f;
      if (!dfspath.isDirectory()) {
        return dfspath.getContentsLength();
      }
    }
    return getContentSummary(f).getLength();
  }

  /** {@inheritDoc} */
  public ContentSummary getContentSummary(Path f) throws IOException {
    return dfs.getContentSummary(getPathName(f));
  }

  public FileStatus[] listStatus(Path p) throws IOException {
    DFSFileInfo[] infos = dfs.listPaths(getPathName(p));
    if (infos == null) return null;
    FileStatus[] stats = new FileStatus[infos.length];
    for (int i = 0; i < infos.length; i++) {
      DFSFileInfo f = (DFSFileInfo)infos[i];
      stats[i] = new FileStatus(f.getLen(), f.isDir(), f.getReplication(),
                                f.getBlockSize(), f.getModificationTime(),
                                f.getPermission(), f.getOwner(), f.getGroup(),
                                new DfsPath(f, this)); // fully-qualify path
    }
    return stats;
  }

  public boolean mkdirs(Path f, FsPermission permission) throws IOException {
    return dfs.mkdirs(getPathName(f), permission);
  }

  /** {@inheritDoc} */
  public void close() throws IOException {
    try {
      dfs.close();
    } finally {
      super.close();
    }
  }

  public String toString() {
    return "DFS[" + dfs + "]";
  }

  DFSClient getClient() {
    return dfs;
  }        
  
  public static class DiskStatus {
    private long capacity;
    private long dfsUsed;
    private long remaining;
    public DiskStatus(long capacity, long dfsUsed, long remaining) {
      this.capacity = capacity;
      this.dfsUsed = dfsUsed;
      this.remaining = remaining;
    }
    
    public long getCapacity() {
      return capacity;
    }
    public long getDfsUsed() {
      return dfsUsed;
    }
    public long getRemaining() {
      return remaining;
    }
  }
  

  /** Return the disk usage of the filesystem, including total capacity,
   * used space, and remaining space */
  public DiskStatus getDiskStatus() throws IOException {
    return dfs.getDiskStatus();
  }
  
  /** Return the total raw capacity of the filesystem, disregarding
   * replication .*/
  public long getRawCapacity() throws IOException{
    return dfs.totalRawCapacity();
  }

  /** Return the total raw used space in the filesystem, disregarding
   * replication .*/
  public long getRawUsed() throws IOException{
    return dfs.totalRawUsed();
  }

  /** Return statistics for each datanode. */
  public DatanodeInfo[] getDataNodeStats() throws IOException {
    return dfs.datanodeReport(DatanodeReportType.ALL);
  }

  /**
   * Enter, leave or get safe mode.
   *  
   * @see org.apache.hadoop.dfs.ClientProtocol#setSafeMode(
   *    FSConstants.SafeModeAction)
   */
  public boolean setSafeMode(FSConstants.SafeModeAction action) 
  throws IOException {
    return dfs.setSafeMode(action);
  }

  /*
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

  /**
   * We need to find the blocks that didn't match.  Likely only one 
   * is corrupt but we will report both to the namenode.  In the future,
   * we can consider figuring out exactly which block is corrupt.
   */
  public boolean reportChecksumFailure(Path f, 
    FSDataInputStream in, long inPos, 
    FSDataInputStream sums, long sumsPos) {

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
        + dataBlock.getBlockName() + " on datanode="
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
        + sumsBlock.getBlockName() + " on datanode="
        + sumsNode[0].getName());

    // Ask client to delete blocks.
    dfs.reportChecksumFailure(f.toString(), lblocks);

    return true;
  }

  /**
   * Returns the stat information about the file.
   * @throws FileNotFoundException if the file does not exist.
   */
  public FileStatus getFileStatus(Path f) throws IOException {
    if (f instanceof DfsPath) {
      DfsPath p = (DfsPath) f;
      return p.info;
    }
    
    try {
      DFSFileInfo p = dfs.getFileInfo(getPathName(f));
      return p;
    } catch (RemoteException e) {
      if (IOException.class.getName().equals(e.getClassName()) &&
          e.getMessage().startsWith(
              "java.io.IOException: File does not exist: ")) {
        // non-existent path
        FileNotFoundException fe = new FileNotFoundException("File " + f + " does not exist.");
        throw (FileNotFoundException) fe.initCause(e); 
      } else {
        throw e;      // unexpected exception
      }
    }
  }

  /** {@inheritDoc }*/
  public void setPermission(Path p, FsPermission permission
      ) throws IOException {
    try {
      dfs.namenode.setPermission(getPathName(p), permission);
    } catch(RemoteException re) {
      if(FileNotFoundException.class.getName().equals(re.getClassName())) {
        throw new FileNotFoundException("File does not exist: " + p);
      }
      throw re;
    }
  }

  /** {@inheritDoc }*/
  public void setOwner(Path p, String username, String groupname
      ) throws IOException {
    if (username == null && groupname == null) {
      throw new IOException("username == null && groupname == null");
    }
    try {
      dfs.namenode.setOwner(getPathName(p), username, groupname);
    } catch(RemoteException re) {
      if(FileNotFoundException.class.getName().equals(re.getClassName())) {
        throw new FileNotFoundException("File does not exist: " + p);
      }
      throw re;
    }
  }
}
