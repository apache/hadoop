/**
 * Copyright 2010 The Apache Software Foundation
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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.RemoteExceptionHandler;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.AlreadyBeingCreatedException;
import org.apache.hadoop.hdfs.protocol.FSConstants;
import org.apache.hadoop.io.SequenceFile;

import java.io.DataInputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;

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
   * @param fs filesystem object
   * @param dir directory to delete
   * @return True if deleted <code>dir</code>
   * @throws IOException e
   */
  public static boolean deleteDirectory(final FileSystem fs, final Path dir)
  throws IOException {
    return fs.exists(dir) && fs.delete(dir, true);
  }

  /**
   * Check if directory exists.  If it does not, create it.
   * @param fs filesystem object
   * @param dir path to check
   * @return Path
   * @throws IOException e
   */
  public Path checkdir(final FileSystem fs, final Path dir) throws IOException {
    if (!fs.exists(dir)) {
      fs.mkdirs(dir);
    }
    return dir;
  }

  /**
   * Create file.
   * @param fs filesystem object
   * @param p path to create
   * @return Path
   * @throws IOException e
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
   * @param fs filesystem
   * @throws IOException e
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
   * @param fs filesystem object
   * @param rootdir root hbase directory
   * @return null if no version file exists, version string otherwise.
   * @throws IOException e
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
   * @throws IOException e
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
    String msg = "File system needs to be upgraded."
      + "  You have version " + version
      + " and I want version " + HConstants.FILE_SYSTEM_VERSION
      + ".  Run the '${HBASE_HOME}/bin/hbase migrate' script.";
    if (message) {
      System.out.println("WARNING! " + msg);
    }
    throw new FileSystemVersionException(msg);
  }

  /**
   * Sets version of file system
   *
   * @param fs filesystem object
   * @param rootdir hbase root
   * @throws IOException e
   */
  public static void setVersion(FileSystem fs, Path rootdir)
  throws IOException {
    setVersion(fs, rootdir, HConstants.FILE_SYSTEM_VERSION);
  }

  /**
   * Sets version of file system
   *
   * @param fs filesystem object
   * @param rootdir hbase root directory
   * @param version version to set
   * @throws IOException e
   */
  public static void setVersion(FileSystem fs, Path rootdir, String version)
  throws IOException {
    FSDataOutputStream s =
      fs.create(new Path(rootdir, HConstants.VERSION_FILE_NAME));
    s.writeUTF(version);
    s.close();
    LOG.debug("Created version file at " + rootdir.toString() + " set its version at:" + version);
  }

  /**
   * Verifies root directory path is a valid URI with a scheme
   *
   * @param root root directory path
   * @return Passed <code>root</code> argument.
   * @throws IOException if not a valid URI with a scheme
   */
  public static Path validateRootPath(Path root) throws IOException {
    try {
      URI rootURI = new URI(root.toString());
      String scheme = rootURI.getScheme();
      if (scheme == null) {
        throw new IOException("Root directory does not have a scheme");
      }
      return root;
    } catch (URISyntaxException e) {
      IOException io = new IOException("Root directory path is not a valid " +
        "URI -- check your " + HConstants.HBASE_DIR + " configuration");
      io.initCause(e);
      throw io;
    }
  }

  /**
   * If DFS, check safe mode and if so, wait until we clear it.
   * @param conf configuration
   * @param wait Sleep between retries
   * @throws IOException e
   */
  public static void waitOnSafeMode(final Configuration conf,
    final long wait)
  throws IOException {
    FileSystem fs = FileSystem.get(conf);
    if (!(fs instanceof DistributedFileSystem)) return;
    DistributedFileSystem dfs = (DistributedFileSystem)fs;
    // Are there any data nodes up yet?
    // Currently the safe mode check falls through if the namenode is up but no
    // datanodes have reported in yet.
    try {
      while (dfs.getDataNodeStats().length == 0) {
        LOG.info("Waiting for dfs to come up...");
        try {
          Thread.sleep(wait);
        } catch (InterruptedException e) {
          //continue
        }
      }
    } catch (IOException e) {
      // getDataNodeStats can fail if superuser privilege is required to run
      // the datanode report, just ignore it
    }
    // Make sure dfs is not in safe mode
    while (dfs.setSafeMode(FSConstants.SafeModeAction.SAFEMODE_GET)) {
      LOG.info("Waiting for dfs to exit safe mode...");
      try {
        Thread.sleep(wait);
      } catch (InterruptedException e) {
        //continue
      }
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
   * @param c configuration
   * @return Path to hbase root directory: i.e. <code>hbase.rootdir</code> from
   * configuration as a Path.
   * @throws IOException e
   */
  public static Path getRootDir(final Configuration c) throws IOException {
    return new Path(c.get(HConstants.HBASE_DIR));
  }

  /**
   * Checks if root region exists
   *
   * @param fs file system
   * @param rootdir root directory of HBase installation
   * @return true if exists
   * @throws IOException e
   */
  public static boolean rootRegionExists(FileSystem fs, Path rootdir)
  throws IOException {
    Path rootRegionDir =
      HRegion.getRegionDir(rootdir, HRegionInfo.ROOT_REGIONINFO);
    return fs.exists(rootRegionDir);
  }

  /**
   * Runs through the hbase rootdir and checks all stores have only
   * one file in them -- that is, they've been major compacted.  Looks
   * at root and meta tables too.
   * @param fs filesystem
   * @param hbaseRootDir hbase root directory
   * @return True if this hbase install is major compacted.
   * @throws IOException e
   */
  public static boolean isMajorCompacted(final FileSystem fs,
      final Path hbaseRootDir)
  throws IOException {
    // Presumes any directory under hbase.rootdir is a table.
    FileStatus [] tableDirs = fs.listStatus(hbaseRootDir, new DirFilter(fs));
    for (FileStatus tableDir : tableDirs) {
      // Skip the .log directory.  All others should be tables.  Inside a table,
      // there are compaction.dir directories to skip.  Otherwise, all else
      // should be regions.  Then in each region, should only be family
      // directories.  Under each of these, should be one file only.
      Path d = tableDir.getPath();
      if (d.getName().equals(HConstants.HREGION_LOGDIR_NAME)) {
        continue;
      }
      FileStatus[] regionDirs = fs.listStatus(d, new DirFilter(fs));
      for (FileStatus regionDir : regionDirs) {
        Path dd = regionDir.getPath();
        if (dd.getName().equals(HConstants.HREGION_COMPACTIONDIR_NAME)) {
          continue;
        }
        // Else its a region name.  Now look in region for families.
        FileStatus[] familyDirs = fs.listStatus(dd, new DirFilter(fs));
        for (FileStatus familyDir : familyDirs) {
          Path family = familyDir.getPath();
          // Now in family make sure only one file.
          FileStatus[] familyStatus = fs.listStatus(family);
          if (familyStatus.length > 1) {
            LOG.debug(family.toString() + " has " + familyStatus.length +
                " files.");
            return false;
          }
        }
      }
    }
    return true;
  }

  // TODO move this method OUT of FSUtils. No dependencies to HMaster
  /**
   * Returns the total overall fragmentation percentage. Includes .META. and
   * -ROOT- as well.
   *
   * @param master  The master defining the HBase root and file system.
   * @return A map for each table and its percentage.
   * @throws IOException When scanning the directory fails.
   */
  public static int getTotalTableFragmentation(final HMaster master)
  throws IOException {
    Map<String, Integer> map = getTableFragmentation(master);
    return map != null && map.size() > 0 ? map.get("-TOTAL-") : -1;
  }

  /**
   * Runs through the HBase rootdir and checks how many stores for each table
   * have more than one file in them. Checks -ROOT- and .META. too. The total
   * percentage across all tables is stored under the special key "-TOTAL-".
   *
   * @param master  The master defining the HBase root and file system.
   * @return A map for each table and its percentage.
   * @throws IOException When scanning the directory fails.
   */
  public static Map<String, Integer> getTableFragmentation(
    final HMaster master)
  throws IOException {
    Path path = master.getRootDir();
    // since HMaster.getFileSystem() is package private
    FileSystem fs = path.getFileSystem(master.getConfiguration());
    return getTableFragmentation(fs, path);
  }

  /**
   * Runs through the HBase rootdir and checks how many stores for each table
   * have more than one file in them. Checks -ROOT- and .META. too. The total
   * percentage across all tables is stored under the special key "-TOTAL-".
   *
   * @param fs  The file system to use.
   * @param hbaseRootDir  The root directory to scan.
   * @return A map for each table and its percentage.
   * @throws IOException When scanning the directory fails.
   */
  public static Map<String, Integer> getTableFragmentation(
    final FileSystem fs, final Path hbaseRootDir)
  throws IOException {
    Map<String, Integer> frags = new HashMap<String, Integer>();
    int cfCountTotal = 0;
    int cfFragTotal = 0;
    DirFilter df = new DirFilter(fs);
    // presumes any directory under hbase.rootdir is a table
    FileStatus [] tableDirs = fs.listStatus(hbaseRootDir, df);
    for (FileStatus tableDir : tableDirs) {
      // Skip the .log directory.  All others should be tables.  Inside a table,
      // there are compaction.dir directories to skip.  Otherwise, all else
      // should be regions.  Then in each region, should only be family
      // directories.  Under each of these, should be one file only.
      Path d = tableDir.getPath();
      if (d.getName().equals(HConstants.HREGION_LOGDIR_NAME)) {
        continue;
      }
      int cfCount = 0;
      int cfFrag = 0;
      FileStatus[] regionDirs = fs.listStatus(d, df);
      for (FileStatus regionDir : regionDirs) {
        Path dd = regionDir.getPath();
        if (dd.getName().equals(HConstants.HREGION_COMPACTIONDIR_NAME)) {
          continue;
        }
        // else its a region name, now look in region for families
        FileStatus[] familyDirs = fs.listStatus(dd, df);
        for (FileStatus familyDir : familyDirs) {
          cfCount++;
          cfCountTotal++;
          Path family = familyDir.getPath();
          // now in family make sure only one file
          FileStatus[] familyStatus = fs.listStatus(family);
          if (familyStatus.length > 1) {
            cfFrag++;
            cfFragTotal++;
          }
        }
      }
      // compute percentage per table and store in result list
      frags.put(d.getName(), Math.round((float) cfFrag / cfCount * 100));
    }
    // set overall percentage for all tables
    frags.put("-TOTAL-", Math.round((float) cfFragTotal / cfCountTotal * 100));
    return frags;
  }

  /**
   * Expects to find -ROOT- directory.
   * @param fs filesystem
   * @param hbaseRootDir hbase root directory
   * @return True if this a pre020 layout.
   * @throws IOException e
   */
  public static boolean isPre020FileLayout(final FileSystem fs,
    final Path hbaseRootDir)
  throws IOException {
    Path mapfiles = new Path(new Path(new Path(new Path(hbaseRootDir, "-ROOT-"),
      "70236052"), "info"), "mapfiles");
    return fs.exists(mapfiles);
  }

  /**
   * Runs through the hbase rootdir and checks all stores have only
   * one file in them -- that is, they've been major compacted.  Looks
   * at root and meta tables too.  This version differs from
   * {@link #isMajorCompacted(FileSystem, Path)} in that it expects a
   * pre-0.20.0 hbase layout on the filesystem.  Used migrating.
   * @param fs filesystem
   * @param hbaseRootDir hbase root directory
   * @return True if this hbase install is major compacted.
   * @throws IOException e
   */
  public static boolean isMajorCompactedPre020(final FileSystem fs,
      final Path hbaseRootDir)
  throws IOException {
    // Presumes any directory under hbase.rootdir is a table.
    FileStatus [] tableDirs = fs.listStatus(hbaseRootDir, new DirFilter(fs));
    for (FileStatus tableDir : tableDirs) {
      // Inside a table, there are compaction.dir directories to skip.
      // Otherwise, all else should be regions.  Then in each region, should
      // only be family directories.  Under each of these, should be a mapfile
      // and info directory and in these only one file.
      Path d = tableDir.getPath();
      if (d.getName().equals(HConstants.HREGION_LOGDIR_NAME)) {
        continue;
      }
      FileStatus[] regionDirs = fs.listStatus(d, new DirFilter(fs));
      for (FileStatus regionDir : regionDirs) {
        Path dd = regionDir.getPath();
        if (dd.getName().equals(HConstants.HREGION_COMPACTIONDIR_NAME)) {
          continue;
        }
        // Else its a region name.  Now look in region for families.
        FileStatus[] familyDirs = fs.listStatus(dd, new DirFilter(fs));
        for (FileStatus familyDir : familyDirs) {
          Path family = familyDir.getPath();
          FileStatus[] infoAndMapfile = fs.listStatus(family);
          // Assert that only info and mapfile in family dir.
          if (infoAndMapfile.length != 0 && infoAndMapfile.length != 2) {
            LOG.debug(family.toString() +
                " has more than just info and mapfile: " + infoAndMapfile.length);
            return false;
          }
          // Make sure directory named info or mapfile.
          for (int ll = 0; ll < 2; ll++) {
            if (infoAndMapfile[ll].getPath().getName().equals("info") ||
                infoAndMapfile[ll].getPath().getName().equals("mapfiles"))
              continue;
            LOG.debug("Unexpected directory name: " +
                infoAndMapfile[ll].getPath());
            return false;
          }
          // Now in family, there are 'mapfile' and 'info' subdirs.  Just
          // look in the 'mapfile' subdir.
          FileStatus[] familyStatus =
              fs.listStatus(new Path(family, "mapfiles"));
          if (familyStatus.length > 1) {
            LOG.debug(family.toString() + " has " + familyStatus.length +
                " files.");
            return false;
          }
        }
      }
    }
    return true;
  }

  /**
   * A {@link PathFilter} that returns directories.
   */
  public static class DirFilter implements PathFilter {
    private final FileSystem fs;

    public DirFilter(final FileSystem fs) {
      this.fs = fs;
    }

    public boolean accept(Path p) {
      boolean isdir = false;
      try {
        isdir = this.fs.getFileStatus(p).isDir();
      } catch (IOException e) {
        e.printStackTrace();
      }
      return isdir;
    }
  }

  /**
   * Heuristic to determine whether is safe or not to open a file for append
   * Looks both for dfs.support.append and use reflection to search
   * for SequenceFile.Writer.syncFs() or FSDataOutputStream.hflush()
   * @param conf
   * @return True if append support
   */
  public static boolean isAppendSupported(final Configuration conf) {
    boolean append = conf.getBoolean("dfs.support.append", false);
    if (append) {
      try {
        // TODO: The implementation that comes back when we do a createWriter
        // may not be using SequenceFile so the below is not a definitive test.
        // Will do for now (hdfs-200).
        SequenceFile.Writer.class.getMethod("syncFs", new Class<?> []{});
        append = true;
      } catch (SecurityException e) {
      } catch (NoSuchMethodException e) {
        append = false;
      }
    } else {
      try {
        FSDataOutputStream.class.getMethod("hflush", new Class<?> []{});
      } catch (NoSuchMethodException e) {
        append = false;
      }
    }
    return append;
  }


  /*
   * Recover file lease. Used when a file might be suspect to be had been left open by another process. <code>p</code>
   * @param fs
   * @param p
   * @param append True if append supported
   * @throws IOException
   */
  public static void recoverFileLease(final FileSystem fs, final Path p, Configuration conf)
  throws IOException{
    if (!isAppendSupported(conf)) {
      return;
    }
    // lease recovery not needed for local file system case.
    // currently, local file system doesn't implement append either.
    if (!(fs instanceof DistributedFileSystem)) {
      return;
    }
    LOG.info("Recovering file" + p);
    long startWaiting = System.currentTimeMillis();

    // Trying recovery
    boolean recovered = false;
    while (!recovered) {
      try {
        FSDataOutputStream out = fs.append(p);
        out.close();
        recovered = true;
      } catch (IOException e) {
        e = RemoteExceptionHandler.checkIOException(e);
        if (e instanceof AlreadyBeingCreatedException) {
          // We expect that we'll get this message while the lease is still
          // within its soft limit, but if we get it past that, it means
          // that the RS is holding onto the file even though it lost its
          // znode. We could potentially abort after some time here.
          long waitedFor = System.currentTimeMillis() - startWaiting;
          if (waitedFor > FSConstants.LEASE_SOFTLIMIT_PERIOD) {
            LOG.warn("Waited " + waitedFor + "ms for lease recovery on " + p +
              ":" + e.getMessage());
          }
          try {
            Thread.sleep(1000);
          } catch (InterruptedException ex) {
            // ignore it and try again
          }
        } else {
          throw new IOException("Failed to open " + p + " for append", e);
        }
      }
    }
    LOG.info("Finished lease recover attempt for " + p);
  }

}