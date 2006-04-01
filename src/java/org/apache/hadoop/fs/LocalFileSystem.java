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

package org.apache.hadoop.fs;

import java.io.*;
import java.util.*;
import java.nio.channels.*;

import org.apache.hadoop.fs.DF;
import org.apache.hadoop.conf.Configuration;

/****************************************************************
 * Implement the FileSystem API for the native filesystem.
 *
 * @author Mike Cafarella
 *****************************************************************/
public class LocalFileSystem extends FileSystem {
    private File workingDir = new File(System.getProperty("user.dir"));
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
    public String[][] getFileCacheHints(File f, long start, long len) throws IOException {
        if (! f.exists()) {
            return null;
        } else {
            String result[][] = new String[1][];
            result[0] = new String[1];
            result[0][0] = "localhost";
            return result;
        }
    }

    public String getName() { return "local"; }

    /*******************************************************
     * For open()'s FSInputStream
     *******************************************************/
    class LocalFSFileInputStream extends FSInputStream {
        FileInputStream fis;

        public LocalFSFileInputStream(File f) throws IOException {
          this.fis = new FileInputStream(f);
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

        public long skip(long n) throws IOException { return fis.skip(n); }
    }
    
    public FSInputStream openRaw(File f) throws IOException {
        f = makeAbsolute(f);
        if (! f.exists()) {
            throw new FileNotFoundException(f.toString());
        }
        return new LocalFSFileInputStream(f);
    }

    /*********************************************************
     * For create()'s FSOutputStream.
     *********************************************************/
    class LocalFSFileOutputStream extends FSOutputStream {
      FileOutputStream fos;

      public LocalFSFileOutputStream(File f) throws IOException {
        this.fos = new FileOutputStream(f);
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

    private File makeAbsolute(File f) {
      if (isAbsolute(f)) {
        return f;
      } else {
        return new File(workingDir, f.toString());
      }
    }
    
    public FSOutputStream createRaw(File f, boolean overwrite)
      throws IOException {
        f = makeAbsolute(f);
        if (f.exists() && ! overwrite) {
            throw new IOException("File already exists:"+f);
        }
        File parent = f.getParentFile();
        if (parent != null)
          parent.mkdirs();

        return new LocalFSFileOutputStream(f);
    }

    public boolean renameRaw(File src, File dst) throws IOException {
        src = makeAbsolute(src);
        dst = makeAbsolute(dst);
        if (useCopyForRename) {
            FileUtil.copyContents(this, src, dst, true, getConf());
            return fullyDelete(src);
        } else return src.renameTo(dst);
    }

    public boolean deleteRaw(File f) throws IOException {
        f = makeAbsolute(f);
        if (f.isFile()) {
            return f.delete();
        } else return fullyDelete(f);
    }

    public boolean exists(File f) throws IOException {
        f = makeAbsolute(f);
        return f.exists();
    }

    public boolean isDirectory(File f) throws IOException {
        f = makeAbsolute(f);
        return f.isDirectory();
    }

    public boolean isAbsolute(File f) {
      return f.isAbsolute();
    }

    public long getLength(File f) throws IOException {
        f = makeAbsolute(f);
        return f.length();
    }

    public File[] listFilesRaw(File f) throws IOException {
        f = makeAbsolute(f);
        return f.listFiles();
    }

    public void mkdirs(File f) throws IOException {
        f = makeAbsolute(f);
        f.mkdirs();
    }

    /**
     * Set the working directory to the given directory.
     * Sets both a local variable and the system property.
     * Note that the system property is only used if the application explictly
     * calls java.io.File.getAbsolutePath().
     */
    public void setWorkingDirectory(File new_dir) {
      workingDir = makeAbsolute(new_dir);
      System.setProperty("user.dir", workingDir.toString());
    }
    
    public File getWorkingDirectory() {
      return workingDir;
    }
    
    public synchronized void lock(File f, boolean shared) throws IOException {
        f = makeAbsolute(f);
        f.createNewFile();

        FileLock lockObj = null;
        if (shared) {
            FileInputStream lockData = new FileInputStream(f);
            lockObj = lockData.getChannel().lock(0L, Long.MAX_VALUE, shared);
            sharedLockDataSet.put(f, lockData);
        } else {
            FileOutputStream lockData = new FileOutputStream(f);
            lockObj = lockData.getChannel().lock(0L, Long.MAX_VALUE, shared);
            nonsharedLockDataSet.put(f, lockData);
        }
        lockObjSet.put(f, lockObj);
    }

    public synchronized void release(File f) throws IOException {
        f = makeAbsolute(f);
        FileLock lockObj = (FileLock) lockObjSet.get(f);
        FileInputStream sharedLockData = (FileInputStream) sharedLockDataSet.get(f);
        FileOutputStream nonsharedLockData = (FileOutputStream) nonsharedLockDataSet.get(f);

        if (lockObj == null) {
            throw new IOException("Given target not held as lock");
        }
        if (sharedLockData == null && nonsharedLockData == null) {
            throw new IOException("Given target not held as lock");
        }

        lockObj.release();
        lockObjSet.remove(f);
        if (sharedLockData != null) {
            sharedLockData.close();
            sharedLockDataSet.remove(f);
        } else {
            nonsharedLockData.close();
            nonsharedLockDataSet.remove(f);
        }
    }

    // In the case of the local filesystem, we can just rename the file.
    public void moveFromLocalFile(File src, File dst) throws IOException {
        if (! src.equals(dst)) {
            src = makeAbsolute(src);
            dst = makeAbsolute(dst);
            if (useCopyForRename) {
                FileUtil.copyContents(this, src, dst, true, getConf());
                fullyDelete(src);
            } else src.renameTo(dst);
        }
    }

    // Similar to moveFromLocalFile(), except the source is kept intact.
    public void copyFromLocalFile(File src, File dst) throws IOException {
        if (! src.equals(dst)) {
            src = makeAbsolute(src);
            dst = makeAbsolute(dst);
            FileUtil.copyContents(this, src, dst, true, getConf());
        }
    }

    // We can't delete the src file in this case.  Too bad.
    public void copyToLocalFile(File src, File dst) throws IOException {
        if (! src.equals(dst)) {
            src = makeAbsolute(src);
            dst = makeAbsolute(dst);
            FileUtil.copyContents(this, src, dst, true, getConf());
        }
    }

    // We can write output directly to the final location
    public File startLocalOutput(File fsOutputFile, File tmpLocalFile) throws IOException {
        return makeAbsolute(fsOutputFile);
    }

    // It's in the right place - nothing to do.
    public void completeLocalOutput(File fsWorkingFile, File tmpLocalFile) throws IOException {
    }

    // We can read directly from the real local fs.
    public File startLocalInput(File fsInputFile, File tmpLocalFile) throws IOException {
        return makeAbsolute(fsInputFile);
    }

    // We're done reading.  Nothing to clean up.
    public void completeLocalInput(File localFile) throws IOException {
        // Ignore the file, it's at the right destination!
    }

    public void close() throws IOException {}

    public String toString() {
        return "LocalFS";
    }
    
    /**
     * Implement our own version instead of using the one in FileUtil,
     * to avoid infinite recursion.
     * @param dir
     * @return
     * @throws IOException
     */
    private boolean fullyDelete(File dir) throws IOException {
        dir = makeAbsolute(dir);
        File contents[] = dir.listFiles();
        if (contents != null) {
            for (int i = 0; i < contents.length; i++) {
                if (contents[i].isFile()) {
                    if (! contents[i].delete()) {
                        return false;
                    }
                } else {
                    if (! fullyDelete(contents[i])) {
                        return false;
                    }
                }
            }
        }
        return dir.delete();
    }

    /** Moves files to a bad file directory on the same device, so that their
     * storage will not be reused. */
    public void reportChecksumFailure(File f, FSInputStream in,
                                      long start, long length, int crc) {
      try {
        // canonicalize f   
        f = makeAbsolute(f).getCanonicalFile();
      
        // find highest writable parent dir of f on the same device
        String device = new DF(f.toString(), getConf()).getMount();
        File parent = f.getParentFile();
        File dir;
        do {
          dir = parent;
          parent = parent.getParentFile();
        } while (parent.canWrite() && parent.toString().startsWith(device));

        // move the file there
        File badDir = new File(dir, "bad_files");
        badDir.mkdirs();
        String suffix = "." + new Random().nextInt();
        File badFile = new File(badDir,f.getName()+suffix);
        LOG.warning("Moving bad file " + f + " to " + badFile);
        in.close();                               // close it first
        f.renameTo(badFile);                      // rename it

        // move checksum file too
        File checkFile = getChecksumFile(f);
        checkFile.renameTo(new File(badDir, checkFile.getName()+suffix));

      } catch (IOException e) {
        LOG.warning("Error moving bad file " + f + ": " + e);
      }
    }

    public long getBlockSize() {
      // default to 32MB: large enough to minimize the impact of seeks
      return getConf().getLong("fs.local.block.size", 32 * 1024 * 1024);
    }


}
