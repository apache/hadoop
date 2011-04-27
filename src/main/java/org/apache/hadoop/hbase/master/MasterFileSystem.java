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
package org.apache.hadoop.hbase.master;

import java.io.IOException;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.RemoteExceptionHandler;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.master.metrics.MasterMetrics;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.Store;
import org.apache.hadoop.hbase.regionserver.wal.HLog;
import org.apache.hadoop.hbase.regionserver.wal.HLogSplitter;
import org.apache.hadoop.hbase.regionserver.wal.OrphanHLogAfterSplitException;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.FSUtils;

/**
 * This class abstracts a bunch of operations the HMaster needs to interact with
 * the underlying file system, including splitting log files, checking file
 * system status, etc.
 */
public class MasterFileSystem {
  private static final Log LOG = LogFactory.getLog(MasterFileSystem.class.getName());
  // HBase configuration
  Configuration conf;
  // master status
  Server master;
  // metrics for master
  MasterMetrics metrics;
  // Persisted unique cluster ID
  private String clusterId;
  // Keep around for convenience.
  private final FileSystem fs;
  // Is the fileystem ok?
  private volatile boolean fsOk = true;
  // The Path to the old logs dir
  private final Path oldLogDir;
  // root hbase directory on the FS
  private final Path rootdir;
  // create the split log lock
  final Lock splitLogLock = new ReentrantLock();
  final boolean distributedLogSplitting;
  final SplitLogManager splitLogManager;

  public MasterFileSystem(Server master, MasterMetrics metrics)
  throws IOException {
    this.conf = master.getConfiguration();
    this.master = master;
    this.metrics = metrics;
    // Set filesystem to be that of this.rootdir else we get complaints about
    // mismatched filesystems if hbase.rootdir is hdfs and fs.defaultFS is
    // default localfs.  Presumption is that rootdir is fully-qualified before
    // we get to here with appropriate fs scheme.
    this.rootdir = FSUtils.getRootDir(conf);
    // Cover both bases, the old way of setting default fs and the new.
    // We're supposed to run on 0.20 and 0.21 anyways.
    this.fs = this.rootdir.getFileSystem(conf);
    String fsUri = this.fs.getUri().toString();
    conf.set("fs.default.name", fsUri);
    conf.set("fs.defaultFS", fsUri);
    this.distributedLogSplitting =
      conf.getBoolean("hbase.master.distributed.log.splitting", true);
    if (this.distributedLogSplitting) {
      this.splitLogManager = new SplitLogManager(master.getZooKeeper(),
          master.getConfiguration(), master, master.getServerName().toString());
      this.splitLogManager.finishInitialization();
    } else {
      this.splitLogManager = null;
    }
    // setup the filesystem variable
    // set up the archived logs path
    this.oldLogDir = new Path(this.rootdir, HConstants.HREGION_OLDLOGDIR_NAME);
    createInitialFileSystemLayout();
  }

  /**
   * Create initial layout in filesystem.
   * <ol>
   * <li>Check if the root region exists and is readable, if not create it.
   * Create hbase.version and the -ROOT- directory if not one.
   * </li>
   * <li>Create a log archive directory for RS to put archived logs</li>
   * </ol>
   * Idempotent.
   */
  private void createInitialFileSystemLayout() throws IOException {
    // check if the root directory exists
    checkRootDir(this.rootdir, conf, this.fs);

    // Make sure the region servers can archive their old logs
    if(!this.fs.exists(this.oldLogDir)) {
      this.fs.mkdirs(this.oldLogDir);
    }
  }

  public FileSystem getFileSystem() {
    return this.fs;
  }

  /**
   * Get the directory where old logs go
   * @return the dir
   */
  public Path getOldLogDir() {
    return this.oldLogDir;
  }

  /**
   * Checks to see if the file system is still accessible.
   * If not, sets closed
   * @return false if file system is not available
   */
  public boolean checkFileSystem() {
    if (this.fsOk) {
      try {
        FSUtils.checkFileSystemAvailable(this.fs);
      } catch (IOException e) {
        master.abort("Shutting down HBase cluster: file system not available", e);
        this.fsOk = false;
      }
    }
    return this.fsOk;
  }

  /**
   * @return HBase root dir.
   * @throws IOException
   */
  public Path getRootDir() {
    return this.rootdir;
  }

  /**
   * Returns the unique identifier generated for this cluster
   * @return
   */
  public String getClusterId() {
    return clusterId;
  }

  /**
   * Inspect the log directory to recover any log file without
   * an active region server.
   * @param onlineServers Map of online servers keyed by
   * {@link ServerName}
   */
  void splitLogAfterStartup(final Set<ServerName> onlineServers) {
    Path logsDirPath = new Path(this.rootdir, HConstants.HREGION_LOGDIR_NAME);
    try {
      if (!this.fs.exists(logsDirPath)) {
        return;
      }
    } catch (IOException e) {
      throw new RuntimeException("Failed exists test on " + logsDirPath, e);
    }
    FileStatus[] logFolders;
    try {
      logFolders = this.fs.listStatus(logsDirPath);
    } catch (IOException e) {
      throw new RuntimeException("Failed listing " + logsDirPath.toString(), e);
    }
    if (logFolders == null || logFolders.length == 0) {
      LOG.debug("No log files to split, proceeding...");
      return;
    }
    for (FileStatus status : logFolders) {
      ServerName serverName = new ServerName(status.getPath().getName());
      if (!onlineServers.contains(serverName)) {
        LOG.info("Log folder " + status.getPath() + " doesn't belong " +
          "to a known region server, splitting");
        splitLog(serverName);
      } else {
        LOG.info("Log folder " + status.getPath() +
          " belongs to an existing region server");
      }
    }
  }

  public void splitLog(final ServerName serverName) {
    long splitTime = 0, splitLogSize = 0;
    Path logDir = new Path(this.rootdir, HLog.getHLogDirectoryName(serverName.toString()));
    if (distributedLogSplitting) {
      splitTime = EnvironmentEdgeManager.currentTimeMillis();
      try {
        try {
          splitLogSize = splitLogManager.splitLogDistributed(logDir);
        } catch (OrphanHLogAfterSplitException e) {
          LOG.warn("Retrying distributed splitting for " +
              serverName + "because of:", e);
          splitLogManager.splitLogDistributed(logDir);
        }
      } catch (IOException e) {
        LOG.error("Failed distributed splitting " + serverName, e);
      }
      splitTime = EnvironmentEdgeManager.currentTimeMillis() - splitTime;
    } else {
      // splitLogLock ensures that dead region servers' logs are processed
      // one at a time
      this.splitLogLock.lock();

      try {
        HLogSplitter splitter = HLogSplitter.createLogSplitter(
            conf, rootdir, logDir, oldLogDir, this.fs);
        try {
          splitter.splitLog();
        } catch (OrphanHLogAfterSplitException e) {
          LOG.warn("Retrying splitting because of:", e);
          // An HLogSplitter instance can only be used once.  Get new instance.
          splitter = HLogSplitter.createLogSplitter(conf, rootdir, logDir,
              oldLogDir, this.fs);
          splitter.splitLog();
        }
        splitTime = splitter.getTime();
        splitLogSize = splitter.getSize();
      } catch (IOException e) {
        LOG.error("Failed splitting " + logDir.toString(), e);
      } finally {
        this.splitLogLock.unlock();
      }
    }
    if (this.metrics != null) {
      this.metrics.addSplit(splitTime, splitLogSize);
    }
  }

  /**
   * Get the rootdir.  Make sure its wholesome and exists before returning.
   * @param rd
   * @param conf
   * @param fs
   * @return hbase.rootdir (after checks for existence and bootstrapping if
   * needed populating the directory with necessary bootup files).
   * @throws IOException
   */
  private Path checkRootDir(final Path rd, final Configuration c,
    final FileSystem fs)
  throws IOException {
    // If FS is in safe mode wait till out of it.
    FSUtils.waitOnSafeMode(c, c.getInt(HConstants.THREAD_WAKE_FREQUENCY,
        10 * 1000));
    // Filesystem is good. Go ahead and check for hbase.rootdir.
    if (!fs.exists(rd)) {
      fs.mkdirs(rd);
      // DFS leaves safe mode with 0 DNs when there are 0 blocks.
      // We used to handle this by checking the current DN count and waiting until
      // it is nonzero. With security, the check for datanode count doesn't work --
      // it is a privileged op. So instead we adopt the strategy of the jobtracker
      // and simply retry file creation during bootstrap indefinitely. As soon as
      // there is one datanode it will succeed. Permission problems should have
      // already been caught by mkdirs above.
      FSUtils.setVersion(fs, rd, c.getInt(HConstants.THREAD_WAKE_FREQUENCY,
        10 * 1000));
    } else {
      // as above
      FSUtils.checkVersion(fs, rd, true, c.getInt(HConstants.THREAD_WAKE_FREQUENCY,
        10 * 1000));
    }
    // Make sure cluster ID exists
    if (!FSUtils.checkClusterIdExists(fs, rd, c.getInt(
        HConstants.THREAD_WAKE_FREQUENCY, 10 * 1000))) {
      FSUtils.setClusterId(fs, rd, UUID.randomUUID().toString(), c.getInt(
          HConstants.THREAD_WAKE_FREQUENCY, 10 * 1000));
    }
    clusterId = FSUtils.getClusterId(fs, rd);

    // Make sure the root region directory exists!
    if (!FSUtils.rootRegionExists(fs, rd)) {
      bootstrap(rd, c);
    }
    return rd;
  }

  private static void bootstrap(final Path rd, final Configuration c)
  throws IOException {
    LOG.info("BOOTSTRAP: creating ROOT and first META regions");
    try {
      // Bootstrapping, make sure blockcache is off.  Else, one will be
      // created here in bootstap and it'll need to be cleaned up.  Better to
      // not make it in first place.  Turn off block caching for bootstrap.
      // Enable after.
      HRegionInfo rootHRI = new HRegionInfo(HRegionInfo.ROOT_REGIONINFO);
      setInfoFamilyCaching(rootHRI, false);
      HRegionInfo metaHRI = new HRegionInfo(HRegionInfo.FIRST_META_REGIONINFO);
      setInfoFamilyCaching(metaHRI, false);
      HRegion root = HRegion.createHRegion(rootHRI, rd, c);
      HRegion meta = HRegion.createHRegion(metaHRI, rd, c);
      setInfoFamilyCaching(rootHRI, true);
      setInfoFamilyCaching(metaHRI, true);
      // Add first region from the META table to the ROOT region.
      HRegion.addRegionToMETA(root, meta);
      root.close();
      root.getLog().closeAndDelete();
      meta.close();
      meta.getLog().closeAndDelete();
    } catch (IOException e) {
      e = RemoteExceptionHandler.checkIOException(e);
      LOG.error("bootstrap", e);
      throw e;
    }
  }

  /**
   * @param hri Set all family block caching to <code>b</code>
   * @param b
   */
  private static void setInfoFamilyCaching(final HRegionInfo hri, final boolean b) {
    for (HColumnDescriptor hcd: hri.getTableDesc().families.values()) {
      if (Bytes.equals(hcd.getName(), HConstants.CATALOG_FAMILY)) {
        hcd.setBlockCacheEnabled(b);
        hcd.setInMemory(b);
      }
    }
  }

  public void deleteRegion(HRegionInfo region) throws IOException {
    fs.delete(HRegion.getRegionDir(rootdir, region), true);
  }

  public void deleteTable(byte[] tableName) throws IOException {
    fs.delete(new Path(rootdir, Bytes.toString(tableName)), true);
  }

  public void updateRegionInfo(HRegionInfo region) {
    // TODO implement this.  i think this is currently broken in trunk i don't
    //      see this getting updated.
    //      @see HRegion.checkRegioninfoOnFilesystem()
  }

  public void deleteFamily(HRegionInfo region, byte[] familyName)
  throws IOException {
    fs.delete(Store.getStoreHomedir(
        new Path(rootdir, region.getTableDesc().getNameAsString()),
        region.getEncodedName(), familyName), true);
  }

  public void stop() {
    if (splitLogManager != null) {
      this.splitLogManager.stop();
    }
  }
}
