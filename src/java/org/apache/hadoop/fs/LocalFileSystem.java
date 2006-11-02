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

package org.apache.hadoop.fs;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.*;
import java.nio.channels.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Progressable;

/****************************************************************
 * Implement the FileSystem API for the native filesystem.
 *
 * @author Mike Cafarella
 *****************************************************************/
public class LocalFileSystem extends FileSystem {
    private Path workingDir =
      new Path(System.getProperty("user.dir"));
    TreeMap sharedLockDataSet = new TreeMap();
    TreeMap nonsharedLockDataSet = new TreeMap();
    TreeMap lockObjSet = new TreeMap();
    // by default use copy/delete instead of rename
    boolean useCopyForRename = true;
    
    /** Construct a local filesystem client. */
    public LocalFileSystem(Configuration conf) throws IOException {
        super(conf);
        // if you find an OS which reliably supports non-POSIX
        // rename(2) across filesystems / volumes, you can
        // uncomment this.
        // String os = System.getProperty("os.name");
        // if (os.toLowerCase().indexOf("os-with-super-rename") != -1)
        //     useCopyForRename = false;
    }

    /**
     * Return 1x1 'localhost' cell if the file exists.
     * Return null if otherwise.
     */
    public String[][] getFileCacheHints(Path f, long start, long len) throws IOException {
        if (! exists(f)) {
            return null;
        } else {
            String result[][] = new String[1][];
            result[0] = new String[1];
            result[0][0] = "localhost";
            return result;
        }
    }

    public String getName() { return "local"; }

    /** Convert a path to a File. */
    public File pathToFile(Path path) {
      if (!path.isAbsolute()) {
        path = new Path(workingDir, path);
      }
      return new File(path.toString());
    }

    /*******************************************************
     * For open()'s FSInputStream
     *******************************************************/
    class LocalFSFileInputStream extends FSInputStream {
        FileInputStream fis;

        public LocalFSFileInputStream(Path f) throws IOException {
          this.fis = new FileInputStream(pathToFile(f));
        }

        public void seek(long pos) throws IOException {
          fis.getChannel().position(pos);
        }

        public long getPos() throws IOException {
          return fis.getChannel().position();
        }

        /*
         * Just forward to the fis
         */
        public int available() throws IOException { return fis.available(); }
        public void close() throws IOException { fis.close(); }
        public boolean markSupport() { return false; }

        public int read() throws IOException {
          try {
            return fis.read();
          } catch (IOException e) {               // unexpected exception
            throw new FSError(e);                 // assume native fs error
          }
        }

        public int read(byte[] b, int off, int len) throws IOException {
          try {
            return fis.read(b, off, len);
          } catch (IOException e) {               // unexpected exception
            throw new FSError(e);                 // assume native fs error
          }
        }

        public int read(long position, byte[] b, int off, int len)
        throws IOException {
          ByteBuffer bb = ByteBuffer.wrap(b, off, len);
          try {
            return fis.getChannel().read(bb, position);
          } catch (IOException e) {
            throw new FSError(e);
          }
        }
        
        public long skip(long n) throws IOException { return fis.skip(n); }
    }
    
    public FSInputStream openRaw(Path f) throws IOException {
        if (! exists(f)) {
            throw new FileNotFoundException(f.toString());
        }
        return new LocalFSFileInputStream(f);
    }

    /*********************************************************
     * For create()'s FSOutputStream.
     *********************************************************/
    class LocalFSFileOutputStream extends FSOutputStream {
      FileOutputStream fos;

      public LocalFSFileOutputStream(Path f) throws IOException {
        this.fos = new FileOutputStream(pathToFile(f));
      }

      public long getPos() throws IOException {
        return fos.getChannel().position();
      }

      /*
       * Just forward to the fos
       */
      public void close() throws IOException { fos.close(); }
      public void flush() throws IOException { fos.flush(); }

      public void write(byte[] b, int off, int len) throws IOException {
        try {
          fos.write(b, off, len);
        } catch (IOException e) {               // unexpected exception
          throw new FSError(e);                 // assume native fs error
        }
      }
      public void write(int b) throws IOException {
        try {
          fos.write(b);
        } catch (IOException e) {               // unexpected exception
          throw new FSError(e);                 // assume native fs error
        }
      }
    }

    public FSOutputStream createRaw(Path f, boolean overwrite, 
                                    short replication, long blockSize)
      throws IOException {
        if (exists(f) && ! overwrite) {
            throw new IOException("File already exists:"+f);
        }
        Path parent = f.getParent();
        if (parent != null) {
          if (!mkdirs(parent)) {
            throw new IOException("Mkdirs failed to create " + parent.toString());
          }
        }
        return new LocalFSFileOutputStream(f);
    }

    public FSOutputStream createRaw(Path f, boolean overwrite, 
                                    short replication, long blockSize,
                                    Progressable progress)
      throws IOException {
        // ignore write-progress reporter for local files
        return createRaw(f, overwrite, replication, blockSize);
    }
    /**
     * Replication is not supported for the local file system.
     */
    public short getReplication(Path f) throws IOException {
      return 1;
    }
    
    public boolean setReplicationRaw( Path src, 
                                      short replication
                                    ) throws IOException {
      return true;
    }

    public boolean renameRaw(Path src, Path dst) throws IOException {
        if (useCopyForRename) {
          return FileUtil.copy(this, src, this, dst, true, getConf());
        } else return pathToFile(src).renameTo(pathToFile(dst));
    }

    public boolean deleteRaw(Path p) throws IOException {
        File f = pathToFile(p);
        if (f.isFile()) {
            return f.delete();
        } else return FileUtil.fullyDelete(f);
    }

    public boolean exists(Path f) throws IOException {
        return pathToFile(f).exists();
    }

    public boolean isDirectory(Path f) throws IOException {
        return pathToFile(f).isDirectory();
    }

    public long getLength(Path f) throws IOException {
        return pathToFile(f).length();
    }

    public Path[] listPathsRaw(Path f) throws IOException {
        File localf = pathToFile(f);
        Path[] results;

        if(!localf.exists())
          return null;
        else if(localf.isFile()) {
          results = new Path[1];
          results[0] = f;
          return results;
        } else { //directory
          String[] names = localf.list();
          if (names == null) {
            return null;
          }
          results = new Path[names.length];
          for (int i = 0; i < names.length; i++) {
            results[i] = new Path(f, names[i]);
          }
          return results;
        }
    }
    
    /**
     * Creates the specified directory hierarchy. Does not
     * treat existence as an error.
     */
    public boolean mkdirs(Path f) throws IOException {
      Path parent = f.getParent();
      File p2f = pathToFile(f);
      return (parent == null || mkdirs(parent)) &&
             (p2f.mkdir() || p2f.isDirectory());
    }
    
    /**
     * Set the working directory to the given directory.
     */
    public void setWorkingDirectory(Path newDir) {
      workingDir = newDir;
    }
    
    public Path getWorkingDirectory() {
      return workingDir;
    }
    
    public void lock(Path p, boolean shared) throws IOException {
      File f = pathToFile(p);
      f.createNewFile();

      if (shared) {
        FileInputStream lockData = new FileInputStream(f);
        FileLock lockObj =
          lockData.getChannel().lock(0L, Long.MAX_VALUE, shared);
        synchronized (this) {
          sharedLockDataSet.put(f, lockData);
          lockObjSet.put(f, lockObj);
        }
      } else {
        FileOutputStream lockData = new FileOutputStream(f);
        FileLock lockObj = lockData.getChannel().lock(0L, Long.MAX_VALUE, shared);
        synchronized (this) {
          nonsharedLockDataSet.put(f, lockData);
          lockObjSet.put(f, lockObj);
        }
      }
    }

    public void release(Path p) throws IOException {
      File f = pathToFile(p);

      FileLock lockObj;
      FileInputStream sharedLockData;
      FileOutputStream nonsharedLockData;
      synchronized (this) {
        lockObj = (FileLock) lockObjSet.remove(f);
        sharedLockData = (FileInputStream) sharedLockDataSet.remove(f);
        nonsharedLockData = (FileOutputStream) nonsharedLockDataSet.remove(f);
      }
 
      if (lockObj == null) {
        throw new IOException("Given target not held as lock");
      }
      if (sharedLockData == null && nonsharedLockData == null) {
        throw new IOException("Given target not held as lock");
      }

      lockObj.release();

      if (sharedLockData != null) {
        sharedLockData.close();
      } else {
        nonsharedLockData.close();
      }
    }

    // In the case of the local filesystem, we can just rename the file.
    public void moveFromLocalFile(Path src, Path dst) throws IOException {
      rename(src, dst);
    }

    // Similar to moveFromLocalFile(), except the source is kept intact.
    public void copyFromLocalFile(Path src, Path dst) throws IOException {
      FileUtil.copy(this, src, this, dst, false, getConf());
    }

    // We can't delete the src file in this case.  Too bad.
    public void copyToLocalFile(Path src, Path dst) throws IOException {
      FileUtil.copy(this, src, this, dst, false, getConf());
    }

    // We can write output directly to the final location
    public Path startLocalOutput(Path fsOutputFile, Path tmpLocalFile)
      throws IOException {
      return fsOutputFile;
    }

    // It's in the right place - nothing to do.
    public void completeLocalOutput(Path fsWorkingFile, Path tmpLocalFile)
      throws IOException {
    }

    public void close() throws IOException {
        super.close();
    }

    public String toString() {
        return "LocalFS";
    }
    

    /** Moves files to a bad file directory on the same device, so that their
     * storage will not be reused. */
    public void reportChecksumFailure(Path p, FSInputStream in,
                                      long start, long length, int crc) {
      try {
        // canonicalize f   
        File f = pathToFile(p).getCanonicalFile();
      
        // find highest writable parent dir of f on the same device
        String device = new DF(f, getConf()).getMount();
        File parent = f.getParentFile();
        File dir;
        do {
          dir = parent;
          parent = parent.getParentFile();
        } while (parent.canWrite() && parent.toString().startsWith(device));

        // move the file there
        File badDir = new File(dir, "bad_files");
        if (!badDir.mkdirs()) {
          if (!badDir.isDirectory()) {
            throw new IOException("Mkdirs failed to create " + badDir.toString());
          }
        }
        String suffix = "." + new Random().nextInt();
        File badFile = new File(badDir,f.getName()+suffix);
        LOG.warn("Moving bad file " + f + " to " + badFile);
        in.close();                               // close it first
        f.renameTo(badFile);                      // rename it

        // move checksum file too
        File checkFile = pathToFile(getChecksumFile(p));
        checkFile.renameTo(new File(badDir, checkFile.getName()+suffix));

      } catch (IOException e) {
        LOG.warn("Error moving bad file " + p + ": " + e);
      }
    }

    public long getDefaultBlockSize() {
      // default to 32MB: large enough to minimize the impact of seeks
      return getConf().getLong("fs.local.block.size", 32 * 1024 * 1024);
    }

    public long getBlockSize(Path filename) {
      // local doesn't really do blocks, so just use the global number
      return getDefaultBlockSize();
    }
    
    public short getDefaultReplication() {
      return 1;
    }


}
