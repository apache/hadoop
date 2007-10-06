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
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.dfs.MiniDFSCluster;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

/**
 * This class creates a single process HBase cluster for junit testing.
 * One thread is created for each server.
 *
 * <p>TestCases do not need to subclass to start a HBaseCluster.  Call
 * {@link #startMaster(Configuration)} and
 * {@link #startRegionServers(Configuration, int)} to startup master and
 * region servers.  Save off the returned values and pass them to
 * {@link #shutdown(org.apache.hadoop.hbase.MiniHBaseCluster.MasterThread, List)}
 * to shut it all down when done.
 *
 */
public class MiniHBaseCluster implements HConstants {
  static final Logger LOG =
    Logger.getLogger(MiniHBaseCluster.class.getName());
  private Configuration conf;
  private MiniDFSCluster cluster;
  private FileSystem fs;
  private boolean shutdownDFS;
  private Path parentdir;
  private MasterThread masterThread = null;
  ArrayList<RegionServerThread> regionThreads =
    new ArrayList<RegionServerThread>();
  private boolean deleteOnExit = true;

  /**
   * Starts a MiniHBaseCluster on top of a new MiniDFSCluster
   *
   * @param conf
   * @param nRegionNodes
   * @throws IOException
   */
  public MiniHBaseCluster(Configuration conf, int nRegionNodes)
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
  public MiniHBaseCluster(Configuration conf, int nRegionNodes,
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
   * @throws IOException
   */
  public MiniHBaseCluster(Configuration conf, int nRegionNodes,
      MiniDFSCluster dfsCluster) throws IOException {

    this.conf = conf;
    this.fs = dfsCluster.getFileSystem();
    this.cluster = dfsCluster;
    this.shutdownDFS = false;
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
  public MiniHBaseCluster(Configuration conf, int nRegionNodes,
      final boolean miniHdfsFilesystem, boolean format, boolean deleteOnExit)
    throws IOException {

    this.conf = conf;
    this.deleteOnExit = deleteOnExit;
    this.shutdownDFS = false;
    if (miniHdfsFilesystem) {
      this.cluster = new MiniDFSCluster(this.conf, 2, format, (String[])null);
      this.fs = cluster.getFileSystem();
      this.shutdownDFS = true;
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
      this.masterThread = startMaster(this.conf);
      this.regionThreads.addAll(startRegionServers(this.conf, nRegionNodes));
    } catch(IOException e) {
      shutdown();
      throw e;
    }
  }

  /** runs the master server */
  public static class MasterThread extends Thread {
    private final HMaster master;
    MasterThread(final HMaster m) {
      super(m, "Master:" + m.getMasterAddress().toString());
      this.master = m;
    }

    /** {@inheritDoc} */
    @Override
    public void run() {
      LOG.info("Starting " + getName());
      super.run();
    }

    /** @return master server */
    public HMaster getMaster() {
      return this.master;
    }
  }

  /** runs region servers */
  public static class RegionServerThread extends Thread {
    private final HRegionServer regionServer;
    RegionServerThread(final HRegionServer r, final int index) {
      super(r, "RegionServer:" + index);
      this.regionServer = r;
    }

    @Override
    public void run() {
      LOG.info("Starting " + getName());
      super.run();
    }

    /** @return the region server */
    public HRegionServer getRegionServer() {
      return this.regionServer;
    }
  }

  /**
   * Use this method to start a master.
   * If you want to start an hbase cluster
   * without subclassing this test case, run this method and
   * {@link #startRegionServers(Configuration, int)} to start servers.
   * Call {@link #shutdown(org.apache.hadoop.hbase.MiniHBaseCluster.MasterThread, List)}
   * to shut them down.
   * @param c
   * @return Thread running the master.
   * @throws IOException
   * @see #startRegionServers(Configuration, int)
   * @see #shutdown(org.apache.hadoop.hbase.MiniHBaseCluster.MasterThread, List)
   */
  public static MasterThread startMaster(final Configuration c)
    throws IOException {

    if(c.get(MASTER_ADDRESS) == null) {
      c.set(MASTER_ADDRESS, "localhost:0");
    }
    // Create the master
    final HMaster m = new HMaster(c);
    MasterThread masterThread = new MasterThread(m);
    // Start up the master
    masterThread.start();
    // Set the master's port for the HRegionServers
    c.set(MASTER_ADDRESS, m.getMasterAddress().toString());
    return masterThread;
  }

  /**
   * @param c
   * @param count
   * @return List of region server threads started.  Synchronize on the
   * returned list when iterating to avoid ConcurrentModificationExceptions.
   * @throws IOException
   * @see #startMaster(Configuration)
   */
  public static ArrayList<RegionServerThread> startRegionServers(
    final Configuration c, final int count) throws IOException {

    // Start the HRegionServers.  Always have regionservers come up on
    // port '0' so there won't be clashes over default port as unit tests
    // start/stop ports at different times during the life of the test.
    c.set(REGIONSERVER_ADDRESS, DEFAULT_HOST + ":0");
    LOG.info("Starting HRegionServers");
    ArrayList<RegionServerThread> threads =
      new ArrayList<RegionServerThread>();
    for(int i = 0; i < count; i++) {
      threads.add(startRegionServer(c, i));
    }
    return threads;
  }

  /**
   * Starts a region server thread running
   *
   * @throws IOException
   * @return Name of regionserver started.
   */
  public String startRegionServer() throws IOException {
    RegionServerThread t =
      startRegionServer(this.conf, this.regionThreads.size());
    this.regionThreads.add(t);
    return t.getName();
  }

  private static RegionServerThread startRegionServer(final Configuration c,
    final int index)
  throws IOException {
    final HRegionServer hrs = new HRegionServer(c);
    RegionServerThread t = new RegionServerThread(hrs, index);
    t.setName("regionserver" +
      t.getRegionServer().server.getListenerAddress().toString());
    t.start();
    return t;
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
    return this.masterThread.getMaster().getMasterAddress();
  }

  /**
   * @return the thread running the HMaster
   */
  public MasterThread getMasterThread() {
    return this.masterThread;
  }
  
  /**
   * Cause a region server to exit without cleaning up
   *
   * @param serverNumber
   */
  public void abortRegionServer(int serverNumber) {
    HRegionServer server =
      this.regionThreads.get(serverNumber).getRegionServer();
    LOG.info("Aborting " + server.serverInfo.toString());
    server.abort();
  }

  /**
   * Shut down the specified region server cleanly
   *
   * @param serverNumber
   * @return the region server that was stopped
   */
  public HRegionServer stopRegionServer(int serverNumber) {
    HRegionServer server =
      this.regionThreads.get(serverNumber).getRegionServer();
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
  public String waitOnRegionServer(int serverNumber) {
    RegionServerThread regionServerThread =
      this.regionThreads.remove(serverNumber);
    try {
      LOG.info("Waiting on " +
        regionServerThread.getRegionServer().serverInfo.toString());
      regionServerThread.join();
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    return regionServerThread.getName();
  }

  /**
   * Wait for Mini HBase Cluster to shut down.
   */
  public void join() {
    if (regionThreads != null) {
      synchronized(regionThreads) {
        for(Thread t: regionThreads) {
          if (t.isAlive()) {
            try {
              t.join();
            } catch (InterruptedException e) {
              // continue
            }
          }
        }
      }
    }
    if (masterThread != null && masterThread.isAlive()) {
      try {
        masterThread.join();
      } catch(InterruptedException e) {
        // continue
      }
    }
  }

  /**
   * Shut down HBase cluster started by calling
   * {@link #startMaster(Configuration)} and then
   * {@link #startRegionServers(Configuration, int)};
   * @param masterThread
   * @param regionServerThreads
   */
  public static void shutdown(final MasterThread masterThread,
      final List<RegionServerThread> regionServerThreads) {
    LOG.info("Shutting down HBase Cluster");
    /** This is not needed.  Remove.
    for(RegionServerThread hsr: regionServerThreads) {
      hsr.getRegionServer().stop();
    }
    */
    if(masterThread != null) {
      masterThread.getMaster().shutdown();
    }
    // regionServerThreads can never be null because they are initialized when
    // the class is constructed.
    synchronized(regionServerThreads) {
      for(Thread t: regionServerThreads) {
        if (t.isAlive()) {
          try {
            t.join();
          } catch (InterruptedException e) {
            // continue
          }
        }
      }
    }
    if (masterThread != null) {
      try {
        masterThread.join();
      } catch(InterruptedException e) {
        // continue
      }
    }
    LOG.info("Shutdown " +
      ((masterThread != null)? masterThread.getName(): "0 masters") + " " +
      regionServerThreads.size() + " region server(s)");
  }

  /**
   * Shut down the mini HBase cluster
   */
  public void shutdown() {
    MiniHBaseCluster.shutdown(this.masterThread, this.regionThreads);

    try {
      if (shutdownDFS && cluster != null) {
        FileSystem fs = cluster.getFileSystem();

        LOG.info("Shutting down Mini DFS cluster");
        cluster.shutdown();

        if (fs != null) {
          LOG.info("Shutting down FileSystem");
          fs.close();
        }
      }

    } catch (IOException e) {
      LOG.error("shutdown", e);

    } finally {
      // Delete all DFS files
      if(deleteOnExit) {
        deleteFile(new File(System.getProperty(
            StaticTestEnvironment.TEST_DIRECTORY_KEY), "dfs"));
      }
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
    HRegionServer s = this.regionThreads.get(0).getRegionServer();
    for(HRegion r: s.onlineRegions.values() ) {
      r.flushcache(false);
    }
  }

  public ArrayList<RegionServerThread> getRegionThreads() {
    return this.regionThreads;
  }
}
