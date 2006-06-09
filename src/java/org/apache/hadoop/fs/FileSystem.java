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

import org.apache.commons.logging.*;

import org.apache.hadoop.dfs.*;
import org.apache.hadoop.conf.*;

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
    public static final Log LOG = LogFactory.getLog("org.apache.hadoop.dfs.DistributedFileSystem");

    private static final HashMap NAME_TO_FS = new HashMap();
    /**
     * Parse the cmd-line args, starting at i.  Remove consumed args
     * from array.  We expect param in the form:
     * '-local | -dfs <namenode:port>'
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
    public static Path getChecksumFile(Path file) {
      return new Path(file.getParent(), "."+file.getName()+".crc");
    }

    /** Return true iff file is a checksum file name.*/
    public static boolean isChecksumFile(Path file) {
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
    public abstract String[][] getFileCacheHints(Path f, long start, long len) throws IOException;

    /** @deprecated Call {@link #open(Path)} instead. */
    public FSDataInputStream open(File f) throws IOException {
      return open(new Path(f.toString()));
    }

    /**
     * Opens an FSDataInputStream at the indicated Path.
     * @param f the file name to open
     * @param bufferSize the size of the buffer to be used.
     */
    public FSDataInputStream open(Path f, int bufferSize) throws IOException {
      return new FSDataInputStream(this, f, bufferSize, getConf());
    }
    
    /**
     * Opens an FSDataInputStream at the indicated Path.
     * @param f the file to open
     */
    public FSDataInputStream open(Path f) throws IOException {
      return new FSDataInputStream(this, f, getConf());
    }

    /**
     * Opens an InputStream for the indicated Path, whether local
     * or via DFS.
     */
    public abstract FSInputStream openRaw(Path f) throws IOException;

    /** @deprecated Call {@link #create(Path)} instead. */
    public FSDataOutputStream create(File f) throws IOException {
      return create(new Path(f.toString()));
    }

    /**
     * Opens an FSDataOutputStream at the indicated Path.
     * Files are overwritten by default.
     */
    public FSDataOutputStream create(Path f) throws IOException {
      return create(f, true, 
                    getConf().getInt("io.file.buffer.size", 4096),
                    getDefaultReplication(),
                    getDefaultBlockSize());
    }

    /**
     * Opens an FSDataOutputStream at the indicated Path.
     * Files are overwritten by default.
     */
    public FSDataOutputStream create(Path f, short replication)
      throws IOException {
      return create(f, true, 
                    getConf().getInt("io.file.buffer.size", 4096),
                    replication,
                    getDefaultBlockSize());
    }

    /**
     * Opens an FSDataOutputStream at the indicated Path.
     * @param f the file name to open
     * @param overwrite if a file with this name already exists, then if true,
     *   the file will be overwritten, and if false an error will be thrown.
     * @param bufferSize the size of the buffer to be used.
     */
    public FSDataOutputStream create( Path f, 
                                      boolean overwrite,
                                      int bufferSize
                                    ) throws IOException {
      return create( f, overwrite, bufferSize, 
                     getDefaultReplication(),
                     getDefaultBlockSize());
    }
    
    /**
     * Opens an FSDataOutputStream at the indicated Path.
     * @param f the file name to open
     * @param overwrite if a file with this name already exists, then if true,
     *   the file will be overwritten, and if false an error will be thrown.
     * @param bufferSize the size of the buffer to be used.
     * @param replication required block replication for the file. 
     */
    public FSDataOutputStream create( Path f, 
                                      boolean overwrite,
                                      int bufferSize,
                                      short replication,
                                      long blockSize
                                    ) throws IOException {
      return new FSDataOutputStream(this, f, overwrite, getConf(), 
                                    bufferSize, replication, blockSize );
    }

    /** Opens an OutputStream at the indicated Path.
     * @param f the file name to open
     * @param overwrite if a file with this name already exists, then if true,
     *   the file will be overwritten, and if false an error will be thrown.
     * @param replication required block replication for the file. 
     */
    public abstract FSOutputStream createRaw(Path f, boolean overwrite, 
                                             short replication,
                                             long blockSize)
      throws IOException;

    /** @deprecated Call {@link #createNewFile(Path)} instead. */
    public boolean createNewFile(File f) throws IOException {
      return createNewFile(new Path(f.toString()));
    }

    /**
     * Creates the given Path as a brand-new zero-length file.  If
     * create fails, or if it already existed, return false.
     */
    public boolean createNewFile(Path f) throws IOException {
        if (exists(f)) {
            return false;
        } else {
          create(f,false,getConf().getInt("io.file.buffer.size", 4096)).close();
          return true;
        }
    }

    /**
     * Set replication for an existing file.
     * 
     * @param src file name
     * @param replication new replication
     * @throws IOException
     * @return true if successful;
     *         false if file does not exist or is a directory
     */
    public boolean setReplication(Path src, short replication) throws IOException {
      boolean value = setReplicationRaw(src, replication);
      if( ! value )
        return false;

      Path checkFile = getChecksumFile(src);
      if (exists(checkFile))
        setReplicationRaw(checkFile, replication);

      return true;
    }

    /**
     * Get replication.
     * 
     * @param src file name
     * @return file replication
     * @throws IOException
     */
    public abstract short getReplication(Path src) throws IOException;

    /**
     * Set replication for an existing file.
     * 
     * @param src file name
     * @param replication new replication
     * @throws IOException
     * @return true if successful;
     *         false if file does not exist or is a directory
     */
    public abstract boolean setReplicationRaw(Path src, short replication) throws IOException;

    /** @deprecated Call {@link #rename(Path, Path)} instead. */
    public boolean rename(File src, File dst) throws IOException {
      return rename(new Path(src.toString()), new Path(dst.toString()));
    }

    /**
     * Renames Path src to Path dst.  Can take place on local fs
     * or remote DFS.
     */
    public boolean rename(Path src, Path dst) throws IOException {
      if (isDirectory(src)) {
        return renameRaw(src, dst);
      } else {

        boolean value = renameRaw(src, dst);

        Path checkFile = getChecksumFile(src);
        if (exists(checkFile))
          renameRaw(checkFile, getChecksumFile(dst)); // try to rename checksum

        return value;
      }
      
    }

    /**
     * Renames Path src to Path dst.  Can take place on local fs
     * or remote DFS.
     */
    public abstract boolean renameRaw(Path src, Path dst) throws IOException;

    /** @deprecated Call {@link #delete(Path)} instead. */
    public boolean delete(File f) throws IOException {
      return delete(new Path(f.toString()));
    }

    /** Delete a file. */
    public boolean delete(Path f) throws IOException {
      if (isDirectory(f)) {
        return deleteRaw(f);
      } else {
        deleteRaw(getChecksumFile(f));            // try to delete checksum
        return deleteRaw(f);
      }
    }

    /**
     * Deletes Path
     */
    public abstract boolean deleteRaw(Path f) throws IOException;

    /** @deprecated call {@link #exists(Path)} instead */
    public boolean exists(File f) throws IOException {
      return exists(new Path(f.toString()));
    }

    /** Check if exists. */
    public abstract boolean exists(Path f) throws IOException;

    /** @deprecated Call {@link #isDirectory(Path)} instead. */
    public boolean isDirectory(File f) throws IOException {
      return isDirectory(new Path(f.toString()));
    }

    /** True iff the named path is a directory. */
    public abstract boolean isDirectory(Path f) throws IOException;

    /** @deprecated Call {@link #isFile(Path)} instead. */
    public boolean isFile(File f) throws IOException {
      return isFile(new Path(f.toString()));
    }

    /** True iff the named path is a regular file. */
    public boolean isFile(Path f) throws IOException {
        if (exists(f) && ! isDirectory(f)) {
            return true;
        } else {
            return false;
        }
    }
    
    /** @deprecated Call {@link #getLength(Path)} instead. */
    public long getLength(File f) throws IOException {
      return getLength(new Path(f.toString()));
    }

    /** The number of bytes in a file. */
    public abstract long getLength(Path f) throws IOException;

    /** @deprecated Call {@link #listPaths(Path)} instead. */
    public File[] listFiles(File f) throws IOException {
      Path[] paths = listPaths(new Path(f.toString()));
      if (paths == null)
        return null;
      File[] result = new File[paths.length];
      for (int i = 0 ; i < paths.length; i++) {
        result[i] = new File(paths[i].toString());
      }
      return result;
    }

    /** List files in a directory. */
    public Path[] listPaths(Path f) throws IOException {
      return listPaths(f, new PathFilter() {
          public boolean accept(Path file) {
            return !isChecksumFile(file);
          }
        });
    }

    /** List files in a directory. */
    public abstract Path[] listPathsRaw(Path f) throws IOException;

    /** @deprecated Call {@link #listPaths(Path)} instead. */
    public File[] listFiles(File f, final FileFilter filt) throws IOException {
      Path[] paths = listPaths(new Path(f.toString()),
                               new PathFilter() {
                                 public boolean accept(Path p) {
                                   return filt.accept(new File(p.toString()));
                                 }
                               });
      if (paths == null)
        return null;
      File[] result = new File[paths.length];
      for (int i = 0 ; i < paths.length; i++) {
        result[i] = new File(paths[i].toString());
      }
      return result;
    }

    /** Filter files in a directory. */
    public Path[] listPaths(Path f, PathFilter filter) throws IOException {
        Vector results = new Vector();
        Path listing[] = listPathsRaw(f);
        if (listing != null) {
          for (int i = 0; i < listing.length; i++) {
            if (filter.accept(listing[i])) {
              results.add(listing[i]);
            }
          }
        }
        return (Path[]) results.toArray(new Path[results.size()]);
    }

    /**
     * Set the current working directory for the given file system.
     * All relative paths will be resolved relative to it.
     * @param new_dir
     */
    public abstract void setWorkingDirectory(Path new_dir);
    
    /**
     * Get the current working directory for the given file system
     * @return the directory pathname
     */
    public abstract Path getWorkingDirectory();
    
    /** @deprecated Call {@link #mkdirs(Path)} instead. */
    public boolean mkdirs(File f) throws IOException {
      return mkdirs(new Path(f.toString()));
    }

    /**
     * Make the given file and all non-existent parents into
     * directories. Has the semantics of Unix 'mkdir -p'.
     * Existence of the directory hierarchy is not an error.
     */
    public abstract boolean mkdirs(Path f) throws IOException;

    /** @deprecated Call {@link #lock(Path,boolean)} instead. */
    public void lock(File f, boolean shared) throws IOException {
      lock(new Path(f.toString()), shared);
    }

    /**
     * Obtain a lock on the given Path
     */
    public abstract void lock(Path f, boolean shared) throws IOException;

    /** @deprecated Call {@link #release(Path)} instead. */
    public void release(File f) throws IOException {
      release(new Path(f.toString()));
    }

    /**
     * Release the lock
     */
    public abstract void release(Path f) throws IOException;

    /**
     * The src file is on the local disk.  Add it to FS at
     * the given dst name and the source is kept intact afterwards
     */
    public abstract void copyFromLocalFile(Path src, Path dst) throws IOException;

    /**
     * The src file is on the local disk.  Add it to FS at
     * the given dst name, removing the source afterwards.
     */
    public abstract void moveFromLocalFile(Path src, Path dst) throws IOException;

    /**
     * The src file is under FS, and the dst is on the local disk.
     * Copy it from FS control to the local dst name.
     */
    public abstract void copyToLocalFile(Path src, Path dst) throws IOException;

    /**
     * the same as copyToLocalFile(Path src, File dst), except that
     * the source is removed afterward.
     */
    // not implemented yet
    //public abstract void moveToLocalFile(Path src, File dst) throws IOException;

    /** @deprecated Call {@link #startLocalOutput(Path, Path)} instead. */
    public File startLocalOutput(File src, File dst) throws IOException {
      return new File(startLocalOutput(new Path(src.toString()),
                                       new Path(dst.toString())).toString());
    }

    /**
     * Returns a local File that the user can write output to.  The caller
     * provides both the eventual FS target name and the local working
     * file.  If the FS is local, we write directly into the target.  If
     * the FS is remote, we write into the tmp local area.
     */
    public abstract Path startLocalOutput(Path fsOutputFile, Path tmpLocalFile) throws IOException;

    /** @deprecated Call {@link #completeLocalOutput(Path, Path)} instead. */
    public void completeLocalOutput(File src, File dst) throws IOException {
      completeLocalOutput(new Path(src.toString()), new Path(dst.toString()));
    }

    /**
     * Called when we're all done writing to the target.  A local FS will
     * do nothing, because we've written to exactly the right place.  A remote
     * FS will copy the contents of tmpLocalFile to the correct target at
     * fsOutputFile.
     */
    public abstract void completeLocalOutput(Path fsOutputFile, Path tmpLocalFile) throws IOException;

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
    public abstract void reportChecksumFailure(Path f, FSInputStream in,
                                               long start, long length,
                                               int crc);

    /**
     * Get the size for a particular file.
     * @param f the filename
     * @return the number of bytes in a block
     */
    public abstract long getBlockSize(Path f) throws IOException;
    
    /** Return the number of bytes that large input files should be optimally
     * be split into to minimize i/o time. */
    public abstract long getDefaultBlockSize();
    
    /**
     * Get the default replication.
     */
    public abstract short getDefaultReplication();

}
