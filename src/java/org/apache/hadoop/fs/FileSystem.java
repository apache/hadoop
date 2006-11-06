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
import java.net.*;
import java.util.*;
import java.util.regex.Pattern;

import org.apache.commons.logging.*;

import org.apache.hadoop.dfs.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.util.Progressable;

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
     * Create an FSDataOutputStream at the indicated Path with write-progress
     * reporting.
     * Files are overwritten by default.
     */
    public FSDataOutputStream create(Path f, Progressable progress) throws IOException {
      return create(f, true, 
                    getConf().getInt("io.file.buffer.size", 4096),
                    getDefaultReplication(),
                    getDefaultBlockSize(), progress);
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
     * Opens an FSDataOutputStream at the indicated Path with write-progress
     * reporting.
     * Files are overwritten by default.
     */
    public FSDataOutputStream create(Path f, short replication, Progressable progress)
      throws IOException {
      return create(f, true, 
                    getConf().getInt("io.file.buffer.size", 4096),
                    replication,
                    getDefaultBlockSize(), progress);
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
     * Opens an FSDataOutputStream at the indicated Path with write-progress
     * reporting.
     * @param f the file name to open
     * @param overwrite if a file with this name already exists, then if true,
     *   the file will be overwritten, and if false an error will be thrown.
     * @param bufferSize the size of the buffer to be used.
     */
    public FSDataOutputStream create( Path f, 
                                      boolean overwrite,
                                      int bufferSize,
                                      Progressable progress
                                    ) throws IOException {
      return create( f, overwrite, bufferSize, 
                     getDefaultReplication(),
                     getDefaultBlockSize(), progress);
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

    /**
     * Opens an FSDataOutputStream at the indicated Path with write-progress
     * reporting.
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
                                      long blockSize,
                                      Progressable progress
                                    ) throws IOException {
      return new FSDataOutputStream(this, f, overwrite, getConf(), 
                                    bufferSize, replication, blockSize, progress );
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

    /** Opens an OutputStream at the indicated Path with write-progress
     * reporting.
     * @param f the file name to open
     * @param overwrite if a file with this name already exists, then if true,
     *   the file will be overwritten, and if false an error will be thrown.
     * @param replication required block replication for the file. 
     */
    public abstract FSOutputStream createRaw(Path f, boolean overwrite, 
                                             short replication,
                                             long blockSize, Progressable progress)
      throws IOException;
    
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

    /**
     * Renames Path src to Path dst.  Can take place on local fs
     * or remote DFS.
     */
    public boolean rename(Path src, Path dst) throws IOException {
      if (isDirectory(src)) {
        return renameRaw(src, dst);
      } else {

        boolean value = renameRaw(src, dst);
        if (!value)
          return false;

        Path checkFile = getChecksumFile(src);
        if (exists(checkFile)) { //try to rename checksum
          if(isDirectory(dst)) {
            renameRaw(checkFile, dst);
          } else {
            renameRaw(checkFile, getChecksumFile(dst)); 
          }
        }

        return value;
      }
      
    }

    /**
     * Renames Path src to Path dst.  Can take place on local fs
     * or remote DFS.
     */
    public abstract boolean renameRaw(Path src, Path dst) throws IOException;

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

    /** Check if exists. */
    public abstract boolean exists(Path f) throws IOException;

    /** True iff the named path is a directory. */
    public abstract boolean isDirectory(Path f) throws IOException;

    /** True iff the named path is a regular file. */
    public boolean isFile(Path f) throws IOException {
        if (exists(f) && ! isDirectory(f)) {
            return true;
        } else {
            return false;
        }
    }
    
    /** The number of bytes in a file. */
    public abstract long getLength(Path f) throws IOException;

    final private static PathFilter DEFAULT_FILTER = new PathFilter() {
      public boolean accept(Path file) {
        return !isChecksumFile(file);
      }     
    };
  
    /** List files in a directory. */
    public Path[] listPaths(Path f) throws IOException {
      return listPaths(f, DEFAULT_FILTER);
    }
    
    /** List files in a directory. */
    public abstract Path[] listPathsRaw(Path f) throws IOException;

    /** Filter raw files in a directory. */
    private void listPaths(ArrayList<Path> results, Path f, PathFilter filter)
      throws IOException {
      Path listing[] = listPathsRaw(f);
      if (listing != null) {
        for (int i = 0; i < listing.length; i++) {
          if (filter.accept(listing[i])) {
            results.add(listing[i]);
          }
        }
      }      
    }
    
    /** Filter raw files in a directory. */
    public Path[] listPaths(Path f, PathFilter filter) throws IOException {
        ArrayList<Path> results = new ArrayList<Path>();
        listPaths(results, f, filter);
        return (Path[]) results.toArray(new Path[results.size()]);
    }

    /** 
     * Filter raw files in a list directories using the default checksum filter. 
     * @param files a list of paths
     * @return a list of files under the source paths
     * @exception IOException
     */
    public Path[] listPaths(Path[] files ) throws IOException {
      return listPaths( files, DEFAULT_FILTER );
    }
    
    /** 
     * Filter raw files in a list directories using user-supplied path filter. 
     * @param files a list of paths
     * @return a list of files under the source paths
     * @exception IOException
     */
    public Path[] listPaths(Path[] files, PathFilter filter)
    throws IOException {
      ArrayList<Path> results = new ArrayList<Path>();
      for(int i=0; i<files.length; i++) {
        listPaths(results, files[i], filter);
      }
      return (Path[]) results.toArray(new Path[results.size()]);
    }
    
    /**
     * <p>Return all the files that match filePattern and are not checksum
     * files. Results are sorted by their names.
     * 
     * <p>
     * A filename pattern is composed of <i>regular</i> characters and
     * <i>special pattern matching</i> characters, which are:
     *
     * <dl>
     *  <dd>
     *   <dl>
     *    <p>
     *    <dt> <tt> ? </tt>
     *    <dd> Matches any single character.
     *
     *    <p>
     *    <dt> <tt> * </tt>
     *    <dd> Matches zero or more characters.
     *
     *    <p>
     *    <dt> <tt> [<i>abc</i>] </tt>
     *    <dd> Matches a single character from character set
     *     <tt>{<i>a,b,c</i>}</tt>.
     *
     *    <p>
     *    <dt> <tt> [<i>a</i>-<i>b</i>] </tt>
     *    <dd> Matches a single character from the character range
     *     <tt>{<i>a...b</i>}</tt>.  Note that character <tt><i>a</i></tt> must be
     *     lexicographically less than or equal to character <tt><i>b</i></tt>.
     *
     *    <p>
     *    <dt> <tt> [^<i>a</i>] </tt>
     *    <dd> Matches a single character that is not from character set or range
     *     <tt>{<i>a</i>}</tt>.  Note that the <tt>^</tt> character must occur
     *     immediately to the right of the opening bracket.
     *
     *    <p>
     *    <dt> <tt> \<i>c</i> </tt>
     *    <dd> Removes (escapes) any special meaning of character <i>c</i>.
     *
     *   </dl>
     *  </dd>
     * </dl>
     *
     * @param filePattern a regular expression specifying file pattern

     * @return an array of paths that match the file pattern
     * @throws IOException
     */
    public Path[] globPaths(Path filePattern) throws IOException {
      return globPaths(filePattern, DEFAULT_FILTER);
    }
    
    /** glob all the file names that matches filePattern
     * and is accepted by filter.
     */
    public Path[] globPaths(Path filePattern, PathFilter filter) 
        throws IOException {
      Path [] parents = new Path[1];
      int level = 0;
      
      String filename = filePattern.toString();
      if("".equals(filename) || Path.SEPARATOR.equals(filename)) {
        parents[0] = filePattern;
        return parents;
      }
      
      String [] components = filename.split(Path.SEPARATOR);
      if(filePattern.isAbsolute()) {
        parents[0] = new Path(Path.SEPARATOR);
        level = 1;
      } else {
        parents[0] = new Path( "" );
      }
      
      Path[] results = globPathsLevel(parents, components, level, filter);
      Arrays.sort(results);
      return results;
    }
    
    private Path[] globPathsLevel(Path[] parents,
        String [] filePattern, int level, PathFilter filter) throws IOException {
      if (level == filePattern.length)
        return parents;
      GlobFilter fp = new GlobFilter(filePattern[level], filter);
      if( fp.hasPattern()) {
        parents = listPaths(parents, fp);
      } else {
        for(int i=0; i<parents.length; i++) {
          parents[i] = new Path(parents[i], filePattern[level]);
        }
      }
      return globPathsLevel(parents, filePattern, level+1, filter);      
    }
 
    private static class GlobFilter implements PathFilter {
      private PathFilter userFilter = DEFAULT_FILTER;
      private Pattern regex;
      private boolean hasPattern = false;
      
      /** Default pattern character: Escape any special meaning. */
      private static final char  PAT_ESCAPE =  '\\';
      /** Default pattern character: Any single character. */
      private static final char  PAT_ANY = '.';
      /** Default pattern character: Character set close. */
      private static final char  PAT_SET_CLOSE = ']';
      
      GlobFilter() {
      }
      
      GlobFilter(String filePattern) throws IOException {
        setRegex(filePattern);
      }
      
      GlobFilter(String filePattern, PathFilter filter) throws IOException {
        userFilter = filter;
        setRegex(filePattern);
      }
      
      void setRegex(String filePattern) throws IOException {
        int len;
        int setOpen;
        boolean setRange;
        StringBuffer fileRegex = new StringBuffer();

        // Validate the pattern
        len = filePattern.length();
        if (len == 0)
            return;

        setOpen =  0;
        setRange = false;

        for (int i = 0;  i < len;  i++)
        {
            char  pCh;

            // Examine a single pattern character
            pCh = filePattern.charAt(i);            
            if( pCh == PAT_ESCAPE ) {
              fileRegex.append( pCh );
              i++;
              if (i >= len)
                  error( "An escaped character does not present",
                      filePattern, i);
              pCh = filePattern.charAt(i);
            } else if( pCh == '.' ) {
              fileRegex.append( PAT_ESCAPE );
            } else if( pCh == '*' ) {
                fileRegex.append( PAT_ANY );
                hasPattern = true;
            } else if( pCh == '?' ) {
                pCh = PAT_ANY ;
                hasPattern = true;
            } else if( pCh == '[' && setOpen == 0 ) {
                setOpen++;
                hasPattern = true;
            } else if( pCh == '^' && setOpen > 0) {
            } else if (pCh == '-'  &&  setOpen > 0) {
                // Character set range
                setRange = true;
            } else if (pCh == PAT_SET_CLOSE  &&  setRange) {
                // Incomplete character set range
                error("Incomplete character set range", filePattern, i);
            } else if (pCh == PAT_SET_CLOSE  &&  setOpen > 0) {
                // End of a character set
                if (setOpen < 2)
                    error("Unexpected end of set", filePattern, i);
                setOpen = 0;
            } else if (setOpen > 0) {
                // Normal character, or the end of a character set range
                setOpen++;
                setRange = false;
            }
            fileRegex.append( pCh );
        }

        // Check for a well-formed pattern
        if (setOpen > 0  ||  setRange)
        {
            // Incomplete character set or character range
            error("Expecting set closure character or end of range", filePattern, len);
        }
        regex = Pattern.compile(fileRegex.toString());
      }
      
      boolean hasPattern() {
        return hasPattern;
      }
      
      public boolean accept(Path path) {
        return regex.matcher(path.getName()).matches() && userFilter.accept(path);
      }
      
      private void error(String s, String pattern, int pos) throws IOException {
        throw new IOException("Illegal file pattern: "
                                 +s+" for glob "+pattern + " at " + pos);
      }
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
    
    /**
     * Make the given file and all non-existent parents into
     * directories. Has the semantics of Unix 'mkdir -p'.
     * Existence of the directory hierarchy is not an error.
     */
    public abstract boolean mkdirs(Path f) throws IOException;

    /**
     * Obtain a lock on the given Path
     */
    public abstract void lock(Path f, boolean shared) throws IOException;

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
     * Returns a local File that the user can write output to.  The caller
     * provides both the eventual FS target name and the local working
     * file.  If the FS is local, we write directly into the target.  If
     * the FS is remote, we write into the tmp local area.
     */
    public abstract Path startLocalOutput(Path fsOutputFile, Path tmpLocalFile) throws IOException;

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
    public void close() throws IOException {
        NAME_TO_FS.remove(getName());
    }

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
