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
package org.apache.hadoop.hbase;

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.apache.hadoop.dfs.MiniDFSCluster;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import org.apache.hadoop.hbase.util.FSUtils;

/**
 * This class creates a single process HBase cluster. One thread is created for
 * each server.
 */
public class MiniHBaseCluster implements HConstants {
  static final Logger LOG =
    Logger.getLogger(MiniHBaseCluster.class.getName());
  
  private HBaseConfiguration conf;
  private MiniDFSCluster cluster;
  private FileSystem fs;
  private boolean shutdownDFS;
  private Path parentdir;
  private LocalHBaseCluster hbaseCluster;
  private boolean deleteOnExit = true;

  /**
   * Starts a MiniHBaseCluster on top of a new MiniDFSCluster
   *
   * @param conf
   * @param nRegionNodes
   * @throws IOException
   */
  public MiniHBaseCluster(HBaseConfiguration conf, int nRegionNodes)
  throws IOException {
    this(conf, nRegionNodes, true, true, true);
  }

  /**
   * Start a MiniHBaseCluster. Use the native file system unless
   * miniHdfsFilesystem is set to true.
   *
   * @param conf
   * @param nRegionNodes
   * @param miniHdfsFilesystem
   * @throws IOException
   */
  public MiniHBaseCluster(HBaseConfiguration conf, int nRegionNodes,
      final boolean miniHdfsFilesystem) throws IOException {
    this(conf, nRegionNodes, miniHdfsFilesystem, true, true);
  }

  /**
   * Starts a MiniHBaseCluster on top of an existing HDFSCluster
   *<pre>
   ****************************************************************************
   *            *  *  *  *  *  N O T E  *  *  *  *  *
   *
   * If you use this constructor, you should shut down the mini dfs cluster
   * in your test case.
   *
   *            *  *  *  *  *  N O T E  *  *  *  *  *
   ****************************************************************************
   *</pre>
   *
   * @param conf
   * @param nRegionNodes
   * @param dfsCluster
   * @param deleteOnExit
   * @throws IOException
   */
  public MiniHBaseCluster(HBaseConfiguration conf, int nRegionNodes,
      MiniDFSCluster dfsCluster, boolean deleteOnExit) throws IOException {

    this.conf = conf;
    this.fs = dfsCluster.getFileSystem();
    this.cluster = dfsCluster;
    this.shutdownDFS = false;
    this.deleteOnExit = deleteOnExit;
    init(nRegionNodes);
  }

  /**
   * Constructor.
   * @param conf
   * @param nRegionNodes
   * @param miniHdfsFilesystem If true, set the hbase mini
   * cluster atop a mini hdfs cluster.  Otherwise, use the
   * filesystem configured in <code>conf</code>.
   * @param format the mini hdfs cluster
   * @param deleteOnExit clean up mini hdfs files
   * @throws IOException
   */
  public MiniHBaseCluster(HBaseConfiguration conf, int nRegionNodes,
      final boolean miniHdfsFilesystem, boolean format, boolean deleteOnExit)
    throws IOException {

    this.conf = conf;
    this.deleteOnExit = deleteOnExit;
    this.shutdownDFS = false;
    if (miniHdfsFilesystem) {
      try {
        this.cluster = new MiniDFSCluster(this.conf, 2, format, (String[])null);
        this.fs = cluster.getFileSystem();
        this.shutdownDFS = true;
      } catch (IOException e) {
        StaticTestEnvironment.shutdownDfs(cluster);
        throw e;
      }
    } else {
      this.cluster = null;
      this.fs = FileSystem.get(conf);
    }
    init(nRegionNodes);
  }

  private void init(final int nRegionNodes) throws IOException {
    try {
      this.parentdir = new Path(conf.get(HBASE_DIR, DEFAULT_HBASE_DIR));
      fs.mkdirs(parentdir);
      FSUtils.setVersion(fs, parentdir);
      this.hbaseCluster = new LocalHBaseCluster(this.conf, nRegionNodes);
      this.hbaseCluster.startup();
    } catch(IOException e) {
      shutdown();
      throw e;
    }
  }

  /**
   * Starts a region server thread running
   *
   * @throws IOException
   * @return Name of regionserver started.
   */
  public String startRegionServer() throws IOException {
    LocalHBaseCluster.RegionServerThread t =
      this.hbaseCluster.addRegionServer();
    t.start();
    return t.getName();
  }

  /**
   * Get the cluster on which this HBase cluster is running
   *
   * @return MiniDFSCluster
   */
  public MiniDFSCluster getDFSCluster() {
    return cluster;
  }

  /**
   * @return Returns the rpc address actually used by the master server, because
   * the supplied port is not necessarily the actual port used.
   */
  public HServerAddress getHMasterAddress() {
    return this.hbaseCluster.getMaster().getMasterAddress();
  }

  /**
   * @return the HMaster
   */
  public HMaster getMaster() {
    return this.hbaseCluster.getMaster();
  }
  
  /**
   * Cause a region server to exit without cleaning up
   *
   * @param serverNumber  Used as index into a list.
   */
  public void abortRegionServer(int serverNumber) {
    HRegionServer server =
      this.hbaseCluster.getRegionServers().get(serverNumber).getRegionServer();
    LOG.info("Aborting " + server.serverInfo.toString());
    server.abort();
  }

  /**
   * Shut down the specified region server cleanly
   *
   * @param serverNumber  Used as index into a list.
   * @return the region server that was stopped
   */
  public HRegionServer stopRegionServer(int serverNumber) {
    HRegionServer server =
      this.hbaseCluster.getRegionServers().get(serverNumber).getRegionServer();
    LOG.info("Stopping " + server.toString());
    server.stop();
    return server;
  }

  /**
   * Wait for the specified region server to stop
   * Removes this thread from list of running threads.
   * @param serverNumber
   * @return Name of region server that just went down.
   */
  public String waitOnRegionServer(final int serverNumber) {
    return this.hbaseCluster.waitOnRegionServer(serverNumber);
  }

  /**
   * Wait for Mini HBase Cluster to shut down.
   */
  public void join() {
    this.hbaseCluster.join();
  }

  /**
   * Shut down the mini HBase cluster
   */
  public void shutdown() {
    if (this.hbaseCluster != null) {
      this.hbaseCluster.shutdown();
    }
    if (shutdownDFS) {
      StaticTestEnvironment.shutdownDfs(cluster);
    }
    // Delete all DFS files
    if(deleteOnExit) {
      deleteFile(new File(System.getProperty(
          StaticTestEnvironment.TEST_DIRECTORY_KEY), "dfs"));
    }
  }

  private void deleteFile(File f) {
    if(f.isDirectory()) {
      File[] children = f.listFiles();
      for(int i = 0; i < children.length; i++) {
        deleteFile(children[i]);
      }
    }
    f.delete();
  }

  /**
   * Call flushCache on all regions on all participating regionservers.
   * @throws IOException
   */
  void flushcache() throws IOException {
    for (LocalHBaseCluster.RegionServerThread t:
        this.hbaseCluster.getRegionServers()) {
      for(HRegion r: t.getRegionServer().onlineRegions.values() ) {
        r.flushcache();
      }
    }
  }

  /**
   * @return List of region server threads.
   */
  public List<LocalHBaseCluster.RegionServerThread> getRegionThreads() {
    return this.hbaseCluster.getRegionServers();
  }
}
