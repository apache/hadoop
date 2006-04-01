/**
 * Copyright 2005 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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
import java.util.*;

import org.apache.hadoop.io.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.Configuration;

/****************************************************************
 * Implementation of the abstract FileSystem for the DFS system.
 * This object is the way end-user code interacts with a Hadoop
 * DistributedFileSystem.
 *
 * @author Mike Cafarella
 *****************************************************************/
public class DistributedFileSystem extends FileSystem {
    private File workingDir = 
      new File("/user", System.getProperty("user.name")).getAbsoluteFile();

    private String name;

    DFSClient dfs;

    /** Construct a client for the filesystem at <code>namenode</code>.
     */
    public DistributedFileSystem(InetSocketAddress namenode, Configuration conf) throws IOException {
      super(conf);
      this.dfs = new DFSClient(namenode, conf);
      this.name = namenode.getHostName() + ":" + namenode.getPort();
    }

    public String getName() { return name; }

    public File getWorkingDirectory() {
      return workingDir;
    }
    
    private File makeAbsolute(File f) {
      if (isAbsolute(f)) {
        return f;
      } else {
        return new File(workingDir, f.getPath());
      }
    }
    
    public void setWorkingDirectory(File dir) {
      workingDir = makeAbsolute(dir);
    }
    
    private UTF8 getPath(File file) {
      String path = getDFSPath(makeAbsolute(file));
      return new UTF8(path);
    }

    public String[][] getFileCacheHints(File f, long start, long len) throws IOException {
      return dfs.getHints(getPath(f), start, len);
    }

    public FSInputStream openRaw(File f) throws IOException {
      return dfs.open(getPath(f));
    }

    public FSOutputStream createRaw(File f, boolean overwrite)
      throws IOException {
      return dfs.create(getPath(f), overwrite);
    }

    /**
     * Rename files/dirs
     */
    public boolean renameRaw(File src, File dst) throws IOException {
      return dfs.rename(getPath(src), getPath(dst));
    }

    /**
     * Get rid of File f, whether a true file or dir.
     */
    public boolean deleteRaw(File f) throws IOException {
        return dfs.delete(getPath(f));
    }

    public boolean exists(File f) throws IOException {
        return dfs.exists(getPath(f));
    }

    public boolean isDirectory(File f) throws IOException {
        if (f instanceof DFSFile) {
          return ((DFSFile)f).isDirectory();
        }
        return dfs.isDirectory(getPath(f));
    }

    public boolean isAbsolute(File f) {
      return f.isAbsolute() ||
        f.getPath().startsWith("/") ||
        f.getPath().startsWith("\\");
    }

    public long getLength(File f) throws IOException {
        if (f instanceof DFSFile) {
          return ((DFSFile)f).length();
        }

        DFSFileInfo info[] = dfs.listFiles(getPath(f));
        return info[0].getLen();
    }

    public File[] listFilesRaw(File f) throws IOException {
        DFSFileInfo info[] = dfs.listFiles(getPath(f));
        if (info == null) {
            return new File[0];
        } else {
            File results[] = new DFSFile[info.length];
            for (int i = 0; i < info.length; i++) {
                results[i] = new DFSFile(info[i]);
            }
            return results;
        }
    }

    public void mkdirs(File f) throws IOException {
        dfs.mkdirs(getPath(f));
    }

    public void lock(File f, boolean shared) throws IOException {
        dfs.lock(getPath(f), ! shared);
    }

    public void release(File f) throws IOException {
        dfs.release(getPath(f));
    }

    public void moveFromLocalFile(File src, File dst) throws IOException {
        doFromLocalFile(src, dst, true);
    }

    public void copyFromLocalFile(File src, File dst) throws IOException {
        doFromLocalFile(src, dst, false);
    }

    private void doFromLocalFile(File src, File dst, boolean deleteSource) throws IOException {
        if (exists(dst)) {
            if (! isDirectory(dst)) {
                throw new IOException("Target " + dst + " already exists");
            } else {
                dst = new File(dst, src.getName());
                if (exists(dst)) {
                    throw new IOException("Target " + dst + " already exists");
                }
            }
        }

        FileSystem localFs = getNamed("local", getConf());

        if (localFs.isDirectory(src)) {
            mkdirs(dst);
            File contents[] = localFs.listFiles(src);
            for (int i = 0; i < contents.length; i++) {
                doFromLocalFile(contents[i], new File(dst, contents[i].getName()), deleteSource);
            }
        } else {
            byte buf[] = new byte[getConf().getInt("io.file.buffer.size", 4096)];
            InputStream in = localFs.open(src);
            try {
                OutputStream out = create(dst);
                try {
                    int bytesRead = in.read(buf);
                    while (bytesRead >= 0) {
                        out.write(buf, 0, bytesRead);
                        bytesRead = in.read(buf);
                    }
                } finally {
                    out.close();
                }
            } finally {
                in.close();
            } 
        }
        if (deleteSource)
            localFs.delete(src);
    }

    public void copyToLocalFile(File src, File dst) throws IOException {
        if (dst.exists()) {
            if (! dst.isDirectory()) {
                throw new IOException("Target " + dst + " already exists");
            } else {
                dst = new File(dst, src.getName());
                if (dst.exists()) {
                    throw new IOException("Target " + dst + " already exists");
                }
            }
        }
        dst = dst.getCanonicalFile();

        FileSystem localFs = getNamed("local", getConf());

        if (isDirectory(src)) {
            localFs.mkdirs(dst);
            File contents[] = listFiles(src);
            for (int i = 0; i < contents.length; i++) {
                copyToLocalFile(contents[i], new File(dst, contents[i].getName()));
            }
        } else {
            byte buf[] = new byte[getConf().getInt("io.file.buffer.size", 4096)];
            InputStream in = open(src);
            try {
                OutputStream out = localFs.create(dst);
                try {
                    int bytesRead = in.read(buf);
                    while (bytesRead >= 0) {
                        out.write(buf, 0, bytesRead);
                        bytesRead = in.read(buf);
                    }
                } finally {
                    out.close();
                }
            } finally {
                in.close();
            } 
        }
    }

    public File startLocalOutput(File fsOutputFile, File tmpLocalFile) throws IOException {
        if (exists(fsOutputFile)) {
            copyToLocalFile(fsOutputFile, tmpLocalFile);
        }
        return tmpLocalFile;
    }

    /**
     * Move completed local data to DFS destination
     */
    public void completeLocalOutput(File fsOutputFile, File tmpLocalFile) throws IOException {
        moveFromLocalFile(tmpLocalFile, fsOutputFile);
    }

    /**
     * Fetch remote DFS file, place at tmpLocalFile
     */
    public File startLocalInput(File fsInputFile, File tmpLocalFile) throws IOException {
        copyToLocalFile(fsInputFile, tmpLocalFile);
        return tmpLocalFile;
    }

    /**
     * We're done with the local stuff, so delete it
     */
    public void completeLocalInput(File localFile) throws IOException {
        // Get rid of the local copy - we don't need it anymore.
        FileUtil.fullyDelete(localFile, getConf());
    }

    public void close() throws IOException {
        dfs.close();
    }

    public String toString() {
        return "DFS[" + dfs + "]";
    }

    DFSClient getClient() {
        return dfs;
    }
    
    private String getDFSPath(File f) {
      List l = new ArrayList();
      l.add(f.getName());
      File parent = f.getParentFile();
      while (parent != null) {
        l.add(parent.getName());
        parent = parent.getParentFile();
      }
      StringBuffer path = new StringBuffer();
      path.append(l.get(l.size() - 1));
      for (int i = l.size() - 2; i >= 0; i--) {
        path.append(DFSFile.DFS_FILE_SEPARATOR);
        path.append(l.get(i));
      }
      if (isAbsolute(f) && path.length() == 0) {
        path.append(DFSFile.DFS_FILE_SEPARATOR);
      }
      return path.toString();
    }

    public void reportChecksumFailure(File f, FSInputStream in,
                                      long start, long length, int crc) {
      
      // ignore for now, causing task to fail, and hope that when task is
      // retried it gets a different copy of the block that is not corrupt.

      // FIXME: we should move the bad block(s) involved to a bad block
      // directory on their datanode, and then re-replicate the blocks, so that
      // no data is lost. a task may fail, but on retry it should succeed.
    }

    public long getBlockSize() {
      return dfs.BLOCK_SIZE;
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
        DFSFileInfo dfsFiles[] = dfs.listFiles(getPath(new File("/")));
        for(int i=0;i<dfsFiles.length;i++){
            used += dfsFiles[i].getContentsLen();
        }
        return used;
    }

    /** Return statistics for each datanode.*/
    public DataNodeReport[] getDataNodeStats() throws IOException {
      DatanodeInfo[]  dnReport = dfs.datanodeReport();
      DataNodeReport[] reports = new DataNodeReport[dnReport.length];

      for (int i = 0; i < dnReport.length; i++) {
        reports[i] = new DataNodeReport();
        reports[i].name = dnReport[i].getName().toString();
        reports[i].host = dnReport[i].getHost().toString();
        reports[i].capacity = dnReport[i].getCapacity();
        reports[i].remaining = dnReport[i].getRemaining();
        reports[i].lastUpdate = dnReport[i].lastUpdate();
      }
      return reports;
    }
}
