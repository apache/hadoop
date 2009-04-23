/**
 * Copyright 2007 The Apache Software Foundation
 *
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
package org.apache.hadoop.hbase.util;

import java.io.DataInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.RemoteExceptionHandler;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hdfs.DistributedFileSystem;

/**
 * Utility methods for interacting with the underlying file system.
 */
public class FSUtils {
  private static final Log LOG = LogFactory.getLog(FSUtils.class);

  /**
   * Not instantiable
   */
  private FSUtils() {
    super();
  }

  /**
   * Delete if exists.
   * @param fs
   * @param dir
   * @return True if deleted <code>dir</code>
   * @throws IOException
   */
  public static boolean deleteDirectory(final FileSystem fs, final Path dir)
  throws IOException {
    return fs.exists(dir)? fs.delete(dir, true): false;
  }

  /**
   * Check if directory exists.  If it does not, create it.
   * @param fs 
   * @param dir
   * @return Path
   * @throws IOException
   */
  public Path checkdir(final FileSystem fs, final Path dir) throws IOException {
    if (!fs.exists(dir)) {
      fs.mkdirs(dir);
    }
    return dir;
  }

  /**
   * Create file.
   * @param fs
   * @param p
   * @return Path
   * @throws IOException
   */
  public static Path create(final FileSystem fs, final Path p)
  throws IOException {
    if (fs.exists(p)) {
      throw new IOException("File already exists " + p.toString());
    }
    if (!fs.createNewFile(p)) {
      throw new IOException("Failed create of " + p);
    }
    return p;
  }

  /**
   * Checks to see if the specified file system is available
   * 
   * @param fs
   * @throws IOException
   */
  public static void checkFileSystemAvailable(final FileSystem fs) 
  throws IOException {
    if (!(fs instanceof DistributedFileSystem)) {
      return;
    }
    IOException exception = null;
    DistributedFileSystem dfs = (DistributedFileSystem) fs;
    try {
      if (dfs.exists(new Path("/"))) {
        return;
      }
    } catch (IOException e) {
      exception = RemoteExceptionHandler.checkIOException(e);
    }
    try {
      fs.close();
    } catch (Exception e) {
        LOG.error("file system close failed: ", e);
    }
    IOException io = new IOException("File system is not available");
    io.initCause(exception);
    throw io;
  }
  
  /**
   * Verifies current version of file system
   * 
   * @param fs
   * @param rootdir
   * @return null if no version file exists, version string otherwise.
   * @throws IOException
   */
  public static String getVersion(FileSystem fs, Path rootdir)
  throws IOException {
    Path versionFile = new Path(rootdir, HConstants.VERSION_FILE_NAME);
    String version = null;
    if (fs.exists(versionFile)) {
      FSDataInputStream s =
        fs.open(versionFile);
      try {
        version = DataInputStream.readUTF(s);
      } finally {
        s.close();
      }
    }
    return version;
  }
  
  /**
   * Verifies current version of file system
   * 
   * @param fs file system
   * @param rootdir root directory of HBase installation
   * @param message if true, issues a message on System.out 
   * 
   * @throws IOException
   */
  public static void checkVersion(FileSystem fs, Path rootdir, 
      boolean message) throws IOException {
    String version = getVersion(fs, rootdir);
    
    if (version == null) {
      if (!rootRegionExists(fs, rootdir)) {
        // rootDir is empty (no version file and no root region)
        // just create new version file (HBASE-1195)
        FSUtils.setVersion(fs, rootdir);
        return;
      }
    } else if (version.compareTo(HConstants.FILE_SYSTEM_VERSION) == 0)
        return;
    
    // version is deprecated require migration
    // Output on stdout so user sees it in terminal.
    String msg = "File system needs to be upgraded. Run " +
      "the '${HBASE_HOME}/bin/hbase migrate' script.";
    if (message) {
      System.out.println("WARNING! " + msg);
    }
    throw new FileSystemVersionException(msg);
  }
  
  /**
   * Sets version of file system
   * 
   * @param fs
   * @param rootdir
   * @throws IOException
   */
  public static void setVersion(FileSystem fs, Path rootdir) 
  throws IOException {
    FSDataOutputStream s =
      fs.create(new Path(rootdir, HConstants.VERSION_FILE_NAME));
    s.writeUTF(HConstants.FILE_SYSTEM_VERSION);
    s.close();
    LOG.debug("Created version file to: " + rootdir.toString());
  }

  /**
   * Verifies root directory path is a valid URI with a scheme
   * 
   * @param root root directory path
   * @throws IOException if not a valid URI with a scheme
   */
  public static void validateRootPath(Path root) throws IOException {
    try {
      URI rootURI = new URI(root.toString());
      String scheme = rootURI.getScheme();
      if (scheme == null) {
        throw new IOException("Root directory does not contain a scheme");
      }
    } catch (URISyntaxException e) {
      IOException io = new IOException("Root directory path is not a valid URI");
      io.initCause(e);
      throw io;
    }
  }
  
  /**
   * Return the 'path' component of a Path.  In Hadoop, Path is an URI.  This
   * method returns the 'path' component of a Path's URI: e.g. If a Path is
   * <code>hdfs://example.org:9000/hbase_trunk/TestTable/compaction.dir</code>,
   * this method returns <code>/hbase_trunk/TestTable/compaction.dir</code>.
   * This method is useful if you want to print out a Path without qualifying
   * Filesystem instance.
   * @param p Filesystem Path whose 'path' component we are to return.
   * @return Path portion of the Filesystem 
   */
  public static String getPath(Path p) {
    return p.toUri().getPath();
  }

  /**
   * @param c
   * @return Path to hbase root directory: i.e. <code>hbase.rootdir</code> as a
   * Path.
   * @throws IOException 
   */
  public static Path getRootDir(final HBaseConfiguration c) throws IOException {
    FileSystem fs = FileSystem.get(c);
    // Get root directory of HBase installation
    Path rootdir = fs.makeQualified(new Path(c.get(HConstants.HBASE_DIR)));
    if (!fs.exists(rootdir)) {
      String message = "HBase root directory " + rootdir.toString() +
        " does not exist.";
      LOG.error(message);
      throw new FileNotFoundException(message);
    }
    return rootdir;
  }
  
  /**
   * Checks if root region exists
   * 
   * @param fs file system
   * @param rootdir root directory of HBase installation
   * @return true if exists
   * @throws IOException
   */
  public static boolean rootRegionExists(FileSystem fs, Path rootdir)
  throws IOException {
    Path rootRegionDir =
      HRegion.getRegionDir(rootdir, HRegionInfo.ROOT_REGIONINFO);
    return fs.exists(rootRegionDir);
  }
}