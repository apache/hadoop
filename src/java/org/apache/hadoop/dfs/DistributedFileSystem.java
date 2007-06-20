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

import org.apache.hadoop.io.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.*;

/****************************************************************
 * Implementation of the abstract FileSystem for the DFS system.
 * This object is the way end-user code interacts with a Hadoop
 * DistributedFileSystem.
 *
 *****************************************************************/
public class DistributedFileSystem extends ChecksumFileSystem {
  private static class RawDistributedFileSystem extends FileSystem {
    private Path workingDir =
      new Path("/user", System.getProperty("user.name")); 
    private URI uri;
    private FileSystem localFs;

    DFSClient dfs;

    public RawDistributedFileSystem() {
    }


    /** @deprecated */
    public RawDistributedFileSystem(InetSocketAddress namenode,
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
      this.dfs = new DFSClient(new InetSocketAddress(host, port), conf);
      this.uri = URI.create("hdfs://"+host+":"+port);
      this.localFs = getNamed("file:///", conf);
    }

    public Path getWorkingDirectory() {
      return workingDir;
    }
    
    public long getDefaultBlockSize() {
      return dfs.getDefaultBlockSize();
    }
    
    public long getBlockSize(Path f) throws IOException {
      // if we already know the answer, use it.
      if (f instanceof DfsPath) {
        return ((DfsPath) f).getBlockSize();
      }
      return dfs.getBlockSize(getPath(f));
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
      Path result = makeAbsolute(dir);
      if (!FSNamesystem.isValidName(result.toString())) {
        throw new IllegalArgumentException("Invalid DFS directory name " + 
                                           result);
      }
      workingDir = makeAbsolute(dir);
    }
    
    /**
     * @deprecated use {@link #getPathName(Path)} instead.
     */
    private UTF8 getPath(Path file) {
      return new UTF8(getPathName(file));
    }

    private String getPathName(Path file) {
      checkPath(file);
      String result = makeAbsolute(file).toUri().getPath();
      if (!FSNamesystem.isValidName(result)) {
        throw new IllegalArgumentException("Pathname " + result + " from " +
                                           file +
                                           " is not a valid DFS filename.");
      }
      return result;
    }

    public String[][] getFileCacheHints(Path f, long start, long len) throws IOException {
      return dfs.getHints(getPathName(f), start, len);
    }

    public FSDataInputStream open(Path f, int bufferSize) throws IOException {
      return new DFSClient.DFSDataInputStream(dfs.open(getPath(f)), bufferSize);
    }

    public FSDataOutputStream create(Path f, boolean overwrite,
                                     int bufferSize, short replication, long blockSize,
                                     Progressable progress) throws IOException {
      if (exists(f) && !overwrite) {
        throw new IOException("File already exists:"+f);
      }
      Path parent = f.getParent();
      if (parent != null && !mkdirs(parent)) {
        throw new IOException("Mkdirs failed to create " + parent);
      }
      
      return new FSDataOutputStream(
                                    dfs.create(getPath(f), overwrite,
                                               replication, blockSize, progress),
                                    bufferSize);
    }
    
    public boolean setReplication(Path src, 
                                  short replication
                                  ) throws IOException {
      return dfs.setReplication(getPath(src), replication);
    }
    
    /**
     * Rename files/dirs
     */
    public boolean rename(Path src, Path dst) throws IOException {
      return dfs.rename(getPath(src), getPath(dst));
    }

    /**
     * Get rid of Path f, whether a true file or dir.
     */
    public boolean delete(Path f) throws IOException {
      return dfs.delete(getPath(f));
    }

    public boolean exists(Path f) throws IOException {
      return dfs.exists(getPath(f));
    }

    public boolean isDirectory(Path f) throws IOException {
      if (f instanceof DfsPath) {
        return ((DfsPath)f).isDirectory();
      }
      return dfs.isDirectory(getPath(f));
    }

    public long getLength(Path f) throws IOException {
      if (f instanceof DfsPath) {
        return ((DfsPath)f).length();
      }

      DFSFileInfo info[] = dfs.listPaths(getPath(f));
      return (info == null) ? 0 : info[0].getLen();
    }

    public long getContentLength(Path f) throws IOException {
      if (f instanceof DfsPath) {
        return ((DfsPath)f).getContentsLength();
      }

      DFSFileInfo info[] = dfs.listPaths(getPath(f));
      return (info == null) ? 0 : info[0].getLen();
    }

    public short getReplication(Path f) throws IOException {
      if (f instanceof DfsPath) {
        return ((DfsPath)f).getReplication();
      }

      DFSFileInfo info[] = dfs.listPaths(getPath(f));
      return info[0].getReplication();
    }

    public Path[] listPaths(Path f) throws IOException {
      DFSFileInfo info[] = dfs.listPaths(getPath(f));
      if (info == null) {
        return new Path[0];
      } else {
        Path results[] = new DfsPath[info.length];
        for (int i = 0; i < info.length; i++) {
          results[i] = new DfsPath(info[i], this);
        }
        return results;
      }
    }

    public boolean mkdirs(Path f) throws IOException {
      return dfs.mkdirs(getPath(f));
    }

    /** @deprecated */ @Deprecated
      public void lock(Path f, boolean shared) throws IOException {
      dfs.lock(getPath(f), !shared);
    }

    /** @deprecated */ @Deprecated
      public void release(Path f) throws IOException {
      dfs.release(getPath(f));
    }

    @Override
    public void copyFromLocalFile(boolean delSrc, Path src, Path dst)
      throws IOException {
      FileUtil.copy(localFs, src, this, dst, delSrc, getConf());
    }

    @Override
    public void copyToLocalFile(boolean delSrc, Path src, Path dst)
      throws IOException {
      FileUtil.copy(this, src, localFs, dst, delSrc, getConf());
    }

    public Path startLocalOutput(Path fsOutputFile, Path tmpLocalFile)
      throws IOException {
      return tmpLocalFile;
    }

    /**
     * Move completed local data to DFS destination
     */
    public void completeLocalOutput(Path fsOutputFile, Path tmpLocalFile)
      throws IOException {
      moveFromLocalFile(tmpLocalFile, fsOutputFile);
    }

    public void close() throws IOException {
      super.close();
      dfs.close();
    }

    public String toString() {
      return "DFS[" + dfs + "]";
    }

    DFSClient getClient() {
      return dfs;
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
      return dfs.datanodeReport();
    }
    
    /**
     * Enter, leave or get safe mode.
     *  
     * @see org.apache.hadoop.dfs.ClientProtocol#setSafeMode(FSConstants.SafeModeAction)
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

      try {
        // Find block in data stream.
        DFSClient.DFSDataInputStream dfsIn = (DFSClient.DFSDataInputStream) in;
        Block dataBlock = dfsIn.getCurrentBlock();
        if (dataBlock == null) {
          throw new IOException("Error: Current block in data stream is null! ");
        }
        DatanodeInfo[] dataNode = {dfsIn.getCurrentDatanode()}; 
        lblocks[0] = new LocatedBlock(dataBlock, dataNode);
        LOG.info("Found checksum error in data stream at block=" + dataBlock.getBlockName() + 
                 " on datanode=" + dataNode[0].getName());

        // Find block in checksum stream
        DFSClient.DFSDataInputStream dfsSums = (DFSClient.DFSDataInputStream) sums;
        Block sumsBlock = dfsSums.getCurrentBlock();
        if (sumsBlock == null) {
          throw new IOException("Error: Current block in checksum stream is null! ");
        }
        DatanodeInfo[] sumsNode = {dfsSums.getCurrentDatanode()}; 
        lblocks[1] = new LocatedBlock(sumsBlock, sumsNode);
        LOG.info("Found checksum error in checksum stream at block=" + sumsBlock.getBlockName() + 
                 " on datanode=" + sumsNode[0].getName());

        // Ask client to delete blocks.
        dfs.reportBadBlocks(lblocks);

      } catch (IOException ie) {
        LOG.info("Found corruption while reading "
                 + f.toString() 
                 + ".  Error repairing corrupt blocks.  Bad blocks remain. " 
                 + StringUtils.stringifyException(ie));
      }

      return true;
    }
  }

  public DistributedFileSystem() {
    super(new RawDistributedFileSystem());
  }

  /** @deprecated */
  public DistributedFileSystem(InetSocketAddress namenode,
                               Configuration conf) throws IOException {
    super(new RawDistributedFileSystem(namenode, conf));
  }

  @Override
  public long getContentLength(Path f) throws IOException {
    return fs.getContentLength(f);
  }

  /** Return the total raw capacity of the filesystem, disregarding
   * replication .*/
  public long getRawCapacity() throws IOException{
    return ((RawDistributedFileSystem)fs).getRawCapacity();
  }

  /** Return the total raw used space in the filesystem, disregarding
   * replication .*/
  public long getRawUsed() throws IOException{
    return ((RawDistributedFileSystem)fs).getRawUsed();
  }

  /** Return statistics for each datanode. */
  public DatanodeInfo[] getDataNodeStats() throws IOException {
    return ((RawDistributedFileSystem)fs).getDataNodeStats();
  }
    
  /**
   * Enter, leave or get safe mode.
   *  
   * @see org.apache.hadoop.dfs.ClientProtocol#setSafeMode(FSConstants.SafeModeAction)
   */
  public boolean setSafeMode(FSConstants.SafeModeAction action) 
    throws IOException {
    return ((RawDistributedFileSystem)fs).setSafeMode(action);
  }

  /*
   * Refreshes the list of hosts and excluded hosts from the configured 
   * files.  
   */
  public void refreshNodes() throws IOException {
    ((RawDistributedFileSystem)fs).refreshNodes();
  }

  /**
   * Finalize previously upgraded files system state.
   */
  public void finalizeUpgrade() throws IOException {
    ((RawDistributedFileSystem)fs).finalizeUpgrade();
  }

  /*
   * Dumps dfs data structures into specified file.
   */
  public void metaSave(String pathname) throws IOException {
    ((RawDistributedFileSystem)fs).metaSave(pathname);
  }

  /**
   * We need to find the blocks that didn't match.  Likely only one 
   * is corrupt but we will report both to the namenode.  In the future,
   * we can consider figuring out exactly which block is corrupt.
   */
  public boolean reportChecksumFailure(Path f, 
                                       FSDataInputStream in, long inPos, 
                                       FSDataInputStream sums, long sumsPos) {
    return ((RawDistributedFileSystem)fs).reportChecksumFailure(
                                                                f, in, inPos, sums, sumsPos);
  }
}
