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
import org.apache.hadoop.util.Progressable;

/****************************************************************
 * Implementation of the abstract FileSystem for the DFS system.
 * This object is the way end-user code interacts with a Hadoop
 * DistributedFileSystem.
 *
 * @author Mike Cafarella
 *****************************************************************/
public class DistributedFileSystem extends FileSystem {
    private Path workingDir = 
      new Path("/user", System.getProperty("user.name"));

    private String name;
    private FileSystem localFs;

    DFSClient dfs;

    /** Construct a client for the filesystem at <code>namenode</code>.
     */
    public DistributedFileSystem(InetSocketAddress namenode, Configuration conf) throws IOException {
      super(conf);
      this.dfs = new DFSClient(namenode, conf);
      this.name = namenode.getHostName() + ":" + namenode.getPort();
      this.localFs = getNamed("local", conf);
    }

    public String getName() { return name; }

    public Path getWorkingDirectory() {
      return workingDir;
    }
    
    public long getDefaultBlockSize() {
      return dfs.getDefaultBlockSize();
    }
    
    public long getBlockSize(Path f) throws IOException {
      return dfs.getBlockSize(makeAbsolute(f));
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
      workingDir = makeAbsolute(dir);
    }
    
    private UTF8 getPath(Path file) {
      return new UTF8(makeAbsolute(file).toString());
    }

    public String[][] getFileCacheHints(Path f, long start, long len) throws IOException {
      return dfs.getHints(getPath(f), start, len);
    }

    public FSInputStream openRaw(Path f) throws IOException {
      return dfs.open(getPath(f));
    }

    public FSOutputStream createRaw(Path f, boolean overwrite, 
                                    short replication, long blockSize)
      throws IOException {
      return dfs.create(getPath(f), overwrite, replication, blockSize);
    }

    public FSOutputStream createRaw(Path f, boolean overwrite, 
                                    short replication, long blockSize,
                                    Progressable progress)
      throws IOException {
      return dfs.create(getPath(f), overwrite, replication, blockSize, progress);
    }
    
    public boolean setReplicationRaw( Path src, 
                                      short replication
                                    ) throws IOException {
      return dfs.setReplication(getPath(src), replication);
    }
    
    /**
     * Rename files/dirs
     */
    public boolean renameRaw(Path src, Path dst) throws IOException {
      return dfs.rename(getPath(src), getPath(dst));
    }

    /**
     * Get rid of Path f, whether a true file or dir.
     */
    public boolean deleteRaw(Path f) throws IOException {
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

    public short getReplication(Path f) throws IOException {
      if (f instanceof DfsPath) {
        return ((DfsPath)f).getReplication();
      }

      DFSFileInfo info[] = dfs.listPaths(getPath(f));
      return info[0].getReplication();
  }

    public Path[] listPathsRaw(Path f) throws IOException {
        DFSFileInfo info[] = dfs.listPaths(getPath(f));
        if (info == null) {
            return new Path[0];
        } else {
            Path results[] = new DfsPath[info.length];
            for (int i = 0; i < info.length; i++) {
                results[i] = new DfsPath(info[i]);
            }
            return results;
        }
    }

    public boolean mkdirs(Path f) throws IOException {
        return dfs.mkdirs(getPath(f));
    }

    public void lock(Path f, boolean shared) throws IOException {
        dfs.lock(getPath(f), ! shared);
    }

    public void release(Path f) throws IOException {
        dfs.release(getPath(f));
    }

    public void moveFromLocalFile(Path src, Path dst) throws IOException {
      FileUtil.copy(localFs, src, this, dst, true, getConf());
    }

    public void copyFromLocalFile(Path src, Path dst) throws IOException {
      FileUtil.copy(localFs, src, this, dst, false, getConf());
    }

    public void copyToLocalFile(Path src, Path dst) throws IOException {
      FileUtil.copy(this, src, localFs, dst, false, getConf());
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
    
    public void reportChecksumFailure(Path f, FSInputStream in,
                                      long start, long length, int crc) {
      
      // ignore for now, causing task to fail, and hope that when task is
      // retried it gets a different copy of the block that is not corrupt.

      // FIXME: we should move the bad block(s) involved to a bad block
      // directory on their datanode, and then re-replicate the blocks, so that
      // no data is lost. a task may fail, but on retry it should succeed.
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

    /** Return the total size of all files in the filesystem.*/
    public long getUsed()throws IOException{
        long used = 0;
        DFSFileInfo dfsFiles[] = dfs.listPaths(getPath(new Path("/")));
        for(int i=0;i<dfsFiles.length;i++){
            used += dfsFiles[i].getContentsLen();
        }
        return used;
    }

    /** Return statistics for each datanode.*/
    public DatanodeInfo[] getDataNodeStats() throws IOException {
      return dfs.datanodeReport();
    }
    
    /**
     * Enter, leave or get safe mode.
     *  
     * @see org.apache.hadoop.dfs.ClientProtocol#setSafeMode(FSConstants.SafeModeAction)
     */
    public boolean setSafeMode( FSConstants.SafeModeAction action ) 
    throws IOException {
      return dfs.setSafeMode( action );
    }
}
