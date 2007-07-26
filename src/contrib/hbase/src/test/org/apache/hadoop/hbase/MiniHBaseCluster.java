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
 */
public class MiniHBaseCluster implements HConstants {
  private static final Logger LOG =
    Logger.getLogger(MiniHBaseCluster.class.getName());
  private Configuration conf;
  private MiniDFSCluster cluster;
  private FileSystem fs;
  private Path parentdir;
  private HMaster master = null;
  private Thread masterThread = null;
  List<HRegionServer> regionServers;
  List<Thread> regionThreads;
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
   * 
   * @param conf
   * @param nRegionNodes
   * @param dfsCluster
   * @throws IOException 
   */
  public MiniHBaseCluster(Configuration conf, int nRegionNodes,
      MiniDFSCluster dfsCluster)
  throws IOException {

    this.conf = conf;
    this.cluster = dfsCluster;
    this.regionServers = new ArrayList<HRegionServer>(nRegionNodes);
    this.regionThreads = new ArrayList<Thread>(nRegionNodes);
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
    this.regionServers = new ArrayList<HRegionServer>(nRegionNodes);
    this.regionThreads = new ArrayList<Thread>(nRegionNodes);

    if (miniHdfsFilesystem) {
      try {
        this.cluster = new MiniDFSCluster(this.conf, 2, format, (String[])null);

      } catch(Throwable t) {
        LOG.error("Failed setup of mini dfs cluster", t);
        t.printStackTrace();
        return;
      }
    }
    init(nRegionNodes);
  }

  private void init(int nRegionNodes) throws IOException {
    try {
      try {
        this.fs = FileSystem.get(conf);
        this.parentdir = new Path(conf.get(HBASE_DIR, DEFAULT_HBASE_DIR));
        fs.mkdirs(parentdir);

      } catch(IOException e) {
        LOG.error("Failed setup of FileSystem", e);
        throw e;
      }

      if(this.conf.get(MASTER_ADDRESS) == null) {
        this.conf.set(MASTER_ADDRESS, "localhost:0");
      }

      // Create the master
      this.master = new HMaster(conf);
      this.masterThread = new Thread(this.master, "HMaster");

      // Start up the master
      LOG.info("Starting HMaster");
      masterThread.start();

      // Set the master's port for the HRegionServers
      String address = master.getMasterAddress().toString();
      this.conf.set(MASTER_ADDRESS, address);

      // Start the HRegionServers.  Always have regionservers come up on
      // port '0' so there won't be clashes over default port as unit tests
      // start/stop ports at different times during the life of the test.
      this.conf.set(REGIONSERVER_ADDRESS, DEFAULT_HOST + ":0");
      LOG.info("Starting HRegionServers");
      startRegionServers(nRegionNodes);
    } catch(IOException e) {
      shutdown();
      throw e;
    }
  }

  /**
   * Get the cluster on which this HBase cluster is running
   * 
   * @return MiniDFSCluster
   */
  public MiniDFSCluster getDFSCluster() {
    return cluster;
  }

  private void startRegionServers(final int nRegionNodes)
  throws IOException {
    for(int i = 0; i < nRegionNodes; i++) {
      startRegionServer();
    }
  }

  void startRegionServer() throws IOException {
    HRegionServer hsr = new HRegionServer(this.conf);
    this.regionServers.add(hsr);
    Thread t = new Thread(hsr, "HRegionServer-" + this.regionServers.size());
    t.start();
    this.regionThreads.add(t);
  }

  /** 
   * @return Returns the rpc address actually used by the master server, because
   * the supplied port is not necessarily the actual port used.
   */
  public HServerAddress getHMasterAddress() {
    return master.getMasterAddress();
  }

  /**
   * Cause a region server to exit without cleaning up
   * 
   * @param serverNumber
   */
  public void abortRegionServer(int serverNumber) {
    HRegionServer server = this.regionServers.remove(serverNumber);
    server.abort();
  }

  /**
   * Shut down the specified region server cleanly
   * 
   * @param serverNumber
   */
  public void stopRegionServer(int serverNumber) {
    HRegionServer server = this.regionServers.remove(serverNumber);
    server.stop();
  }

  /**
   * Wait for the specified region server to stop
   * 
   * @param serverNumber
   */
  public void waitOnRegionServer(int serverNumber) {
    Thread regionServerThread = this.regionThreads.remove(serverNumber);
    try {
      regionServerThread.join();
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }

  /** Shut down the HBase cluster */
  public void shutdown() {
    LOG.info("Shutting down the HBase Cluster");
    for(HRegionServer hsr: this.regionServers) {
      hsr.stop();
    }
    if(master != null) {
      master.shutdown();
    }
    for(Thread t: this.regionThreads) {
      if (t.isAlive()) {
        try {
          t.join();
        } catch (InterruptedException e) {
          // continue
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
    LOG.info("HBase Cluster shutdown complete");
    
    // Close the file system.  Will complain if files open so helps w/ leaks.
    try {
      this.cluster.getFileSystem().close();
    } catch (IOException e) {
      LOG.error("Closing down dfs", e);
    }
    if(cluster != null) {
      LOG.info("Shutting down Mini DFS cluster");
      cluster.shutdown();
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
}
