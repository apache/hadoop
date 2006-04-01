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
import java.net.*;
import java.util.*;
import java.util.logging.*;

import org.apache.hadoop.dfs.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.util.LogFormatter;

/****************************************************************
 * An abstract base class for a fairly generic filesystem.  It
 * may be implemented as a distributed filesystem, or as a "local"
 * one that reflects the locally-connected disk.  The local version
 * exists for small Hadopp instances and for testing.
 *
 * <p>
 *
 * All user code that may potentially use the Hadoop Distributed
 * File System should be written to use a FileSystem object.  The
 * Hadoop DFS is a multi-machine system that appears as a single
 * disk.  It's useful because of its fault tolerance and potentially
 * very large capacity.
 * 
 * <p>
 * The local implementation is {@link LocalFileSystem} and distributed
 * implementation is {@link DistributedFileSystem}.
 * @author Mike Cafarella
 *****************************************************************/
public abstract class FileSystem extends Configured {
    public static final Logger LOG = LogFormatter.getLogger("org.apache.hadoop.dfs.DistributedFileSystem");

    private static final HashMap NAME_TO_FS = new HashMap();
    /**
     * Parse the cmd-line args, starting at i.  Remove consumed args
     * from array.  We expect param in the form:
     * '-local | -dfs <namenode:port>'
     *
     * @deprecated use fs.default.name config option instead
     */
    public static FileSystem parseArgs(String argv[], int i, Configuration conf) throws IOException {
        /**
        if (argv.length - i < 1) {
            throw new IOException("Must indicate filesystem type for DFS");
        }
        */
        int orig = i;
        FileSystem fs = null;
        String cmd = argv[i];
        if ("-dfs".equals(cmd)) {
            i++;
            InetSocketAddress addr = DataNode.createSocketAddr(argv[i++]);
            fs = new DistributedFileSystem(addr, conf);
        } else if ("-local".equals(cmd)) {
            i++;
            fs = new LocalFileSystem(conf);
        } else {
            fs = get(conf);                          // using default
            LOG.info("No FS indicated, using default:"+fs.getName());

        }
        System.arraycopy(argv, i, argv, orig, argv.length - i);
        for (int j = argv.length - i; j < argv.length; j++) {
            argv[j] = null;
        }
        return fs;
    }

    /** Returns the configured filesystem implementation.*/
    public static FileSystem get(Configuration conf) throws IOException {
      return getNamed(conf.get("fs.default.name", "local"), conf);
    }

    /** Returns a name for this filesystem, suitable to pass to {@link
     * FileSystem#getNamed(String,Configuration)}.*/
    public abstract String getName();
  
    /** Returns a named filesystem.  Names are either the string "local" or a
     * host:port pair, naming an DFS name server.*/
    public static FileSystem getNamed(String name, Configuration conf) throws IOException {
      FileSystem fs = (FileSystem)NAME_TO_FS.get(name);
      if (fs == null) {
        if ("local".equals(name)) {
          fs = new LocalFileSystem(conf);
        } else {
          fs = new DistributedFileSystem(DataNode.createSocketAddr(name), conf);
        }
        NAME_TO_FS.put(name, fs);
      }
      return fs;
    }

    /** Return the name of the checksum file associated with a file.*/
    public static File getChecksumFile(File file) {
      return new File(file.getParentFile(), "."+file.getName()+".crc");
    }

    /** Return true iff file is a checksum file name.*/
    public static boolean isChecksumFile(File file) {
      String name = file.getName();
      return name.startsWith(".") && name.endsWith(".crc");
    }

    ///////////////////////////////////////////////////////////////
    // FileSystem
    ///////////////////////////////////////////////////////////////

    protected FileSystem(Configuration conf) {
      super(conf);
    }

    /**
     * Return a 2D array of size 1x1 or greater, containing hostnames 
     * where portions of the given file can be found.  For a nonexistent 
     * file or regions, null will be returned.
     *
     * This call is most helpful with DFS, where it returns 
     * hostnames of machines that contain the given file.
     *
     * The FileSystem will simply return an elt containing 'localhost'.
     */
    public abstract String[][] getFileCacheHints(File f, long start, long len) throws IOException;

    /**
     * Opens an FSDataInputStream at the indicated File.
     * @param f the file name to open
     * @param bufferSize the size of the buffer to be used.
     */
    public FSDataInputStream open(File f, int bufferSize) throws IOException {
      return new FSDataInputStream(this, f, bufferSize, getConf());
    }
    
    /**
     * Opens an FSDataInputStream at the indicated File.
     * @param f the file to open
     */
    public FSDataInputStream open(File f) throws IOException {
      return new FSDataInputStream(this, f, getConf());
    }

    /**
     * Opens an InputStream for the indicated File, whether local
     * or via DFS.
     */
    public abstract FSInputStream openRaw(File f) throws IOException;

    /**
     * Opens an FSDataOutputStream at the indicated File.
     * Files are overwritten by default.
     */
    public FSDataOutputStream create(File f) throws IOException {
      return create(f, true, getConf().getInt("io.file.buffer.size", 4096));
    }

    /**
     * Opens an FSDataOutputStream at the indicated File.
     * @param f the file name to open
     * @param overwrite if a file with this name already exists, then if true,
     *   the file will be overwritten, and if false an error will be thrown.
     * @param bufferSize the size of the buffer to be used.
     */
    public FSDataOutputStream create(File f, boolean overwrite,
                                      int bufferSize) throws IOException {
      return new FSDataOutputStream(this, f, overwrite, getConf(), bufferSize);
    }

    /** Opens an OutputStream at the indicated File.
     * @param f the file name to open
     * @param overwrite if a file with this name already exists, then if true,
     *   the file will be overwritten, and if false an error will be thrown.
     */
    public abstract FSOutputStream createRaw(File f, boolean overwrite)
      throws IOException;

    /**
     * Creates the given File as a brand-new zero-length file.  If
     * create fails, or if it already existed, return false.
     */
    public boolean createNewFile(File f) throws IOException {
        if (exists(f)) {
            return false;
        } else {
            OutputStream out = createRaw(f, false);
            try {
            } finally {
              out.close();
            }
            return true;
        }
    }

    /**
     * Renames File src to File dst.  Can take place on local fs
     * or remote DFS.
     */
    public boolean rename(File src, File dst) throws IOException {
      if (isDirectory(src)) {
        return renameRaw(src, dst);
      } else {

        boolean value = renameRaw(src, dst);

        File checkFile = getChecksumFile(src);
        if (exists(checkFile))
          renameRaw(checkFile, getChecksumFile(dst)); // try to rename checksum

        return value;
      }
      
    }

    /**
     * Renames File src to File dst.  Can take place on local fs
     * or remote DFS.
     */
    public abstract boolean renameRaw(File src, File dst) throws IOException;

    /**
     * Deletes File
     */
    public boolean delete(File f) throws IOException {
      if (isDirectory(f)) {
        return deleteRaw(f);
      } else {
        deleteRaw(getChecksumFile(f));            // try to delete checksum
        return deleteRaw(f);
      }
    }

    /**
     * Deletes File
     */
    public abstract boolean deleteRaw(File f) throws IOException;

    /**
     * Check if exists
     */
    public abstract boolean exists(File f) throws IOException;

    /** True iff the named path is a directory. */
    public abstract boolean isDirectory(File f) throws IOException;

    /** True iff the named path is a regular file. */
    public boolean isFile(File f) throws IOException {
        if (exists(f) && ! isDirectory(f)) {
            return true;
        } else {
            return false;
        }
    }
    
    /** True iff the named path is absolute. */
    public abstract boolean isAbsolute(File f);

    /** The number of bytes in a file. */
    public abstract long getLength(File f) throws IOException;

    /** List files in a directory. */
    public File[] listFiles(File f) throws IOException {
      return listFiles(f, new FileFilter() {
          public boolean accept(File file) {
            return !isChecksumFile(file);
          }
        });
    }

    /** List files in a directory. */
    public abstract File[] listFilesRaw(File f) throws IOException;

    /** Filter files in a directory. */
    public File[] listFiles(File f, FileFilter filter) throws IOException {
        Vector results = new Vector();
        File listing[] = listFilesRaw(f);
        if (listing != null) {
          for (int i = 0; i < listing.length; i++) {
            if (filter.accept(listing[i])) {
              results.add(listing[i]);
            }
          }
        }
        return (File[]) results.toArray(new File[results.size()]);
    }

    /**
     * Set the current working directory for the given file system.
     * All relative paths will be resolved relative to it.
     * @param new_dir
     */
    public abstract void setWorkingDirectory(File new_dir);
    
    /**
     * Get the current working directory for the given file system
     * @return the directory pathname
     */
    public abstract File getWorkingDirectory();
    
    /**
     * Make the given file and all non-existent parents into
     * directories.
     */
    public abstract void mkdirs(File f) throws IOException;

    /**
     * Obtain a lock on the given File
     */
    public abstract void lock(File f, boolean shared) throws IOException;

    /**
     * Release the lock
     */
    public abstract void release(File f) throws IOException;

    /**
     * The src file is on the local disk.  Add it to FS at
     * the given dst name and the source is kept intact afterwards
     */
    public abstract void copyFromLocalFile(File src, File dst) throws IOException;

    /**
     * The src file is on the local disk.  Add it to FS at
     * the given dst name, removing the source afterwards.
     */
    public abstract void moveFromLocalFile(File src, File dst) throws IOException;

    /**
     * The src file is under FS2, and the dst is on the local disk.
     * Copy it from FS control to the local dst name.
     */
    public abstract void copyToLocalFile(File src, File dst) throws IOException;

    /**
     * the same as copyToLocalFile(File src, File dst), except that
     * the source is removed afterward.
     */
    // not implemented yet
    //public abstract void moveToLocalFile(File src, File dst) throws IOException;

    /**
     * Returns a local File that the user can write output to.  The caller
     * provides both the eventual FS target name and the local working
     * file.  If the FS is local, we write directly into the target.  If
     * the FS is remote, we write into the tmp local area.
     */
    public abstract File startLocalOutput(File fsOutputFile, File tmpLocalFile) throws IOException;

    /**
     * Called when we're all done writing to the target.  A local FS will
     * do nothing, because we've written to exactly the right place.  A remote
     * FS will copy the contents of tmpLocalFile to the correct target at
     * fsOutputFile.
     */
    public abstract void completeLocalOutput(File fsOutputFile, File tmpLocalFile) throws IOException;

    /**
     * Returns a local File that the user can read from.  The caller 
     * provides both the eventual FS target name and the local working
     * file.  If the FS is local, we read directly from the source.  If
     * the FS is remote, we write data into the tmp local area.
     */
    public abstract File startLocalInput(File fsInputFile, File tmpLocalFile) throws IOException;

    /**
     * Called when we're all done writing to the target.  A local FS will
     * do nothing, because we've written to exactly the right place.  A remote
     * FS will copy the contents of tmpLocalFile to the correct target at
     * fsOutputFile.
     */
    public abstract void completeLocalInput(File localFile) throws IOException;

    /**
     * No more filesystem operations are needed.  Will
     * release any held locks.
     */
    public abstract void close() throws IOException;

    /**
     * Report a checksum error to the file system.
     * @param f the file name containing the error
     * @param in the stream open on the file
     * @param start the position of the beginning of the bad data in the file
     * @param length the length of the bad data in the file
     * @param crc the expected CRC32 of the data
     */
    public abstract void reportChecksumFailure(File f, FSInputStream in,
                                               long start, long length,
                                               int crc);

    /** Return the number of bytes that large input files should be optimally
     * be split into to minimize i/o time. */
    public abstract long getBlockSize();

}
