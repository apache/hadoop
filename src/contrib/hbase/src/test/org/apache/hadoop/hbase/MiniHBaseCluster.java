/**
 * Copyright 2006 The Apache Software Foundation
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
package org.apache.hadoop.hbase;

import java.io.File;
import java.io.IOException;

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
  private HMaster master;
  private Thread masterThread;
  private HRegionServer[] regionServers;
  private Thread[] regionThreads;
  
  /**
   * Starts a MiniHBaseCluster on top of a new MiniDFSCluster
   * 
   * @param conf
   * @param nRegionNodes
   */
  public MiniHBaseCluster(Configuration conf, int nRegionNodes) {
    this(conf, nRegionNodes, true);
  }
  
  /**
   * Starts a MiniHBaseCluster on top of an existing HDFSCluster
   * 
   * @param conf
   * @param nRegionNodes
   * @param dfsCluster
   */
  public MiniHBaseCluster(Configuration conf, int nRegionNodes,
      MiniDFSCluster dfsCluster) {

    this.conf = conf;
    this.cluster = dfsCluster;
    init(nRegionNodes);
  }
  
  /**
   * Constructor.
   * @param conf
   * @param nRegionNodes
   * @param miniHdfsFilesystem If true, set the hbase mini
   * cluster atop a mini hdfs cluster.  Otherwise, use the
   * filesystem configured in <code>conf</code>.
   */
  public MiniHBaseCluster(Configuration conf, int nRegionNodes,
      final boolean miniHdfsFilesystem) {
    this.conf = conf;
    
    if (miniHdfsFilesystem) {
      try {
        this.cluster = new MiniDFSCluster(this.conf, 2, true, (String[])null);
        
      } catch(Throwable t) {
        LOG.error("Failed setup of mini dfs cluster", t);
        t.printStackTrace();
        return;
      }
    }
    init(nRegionNodes);
  }

  private void init(int nRegionNodes) {
    try {
      try {
        this.fs = FileSystem.get(conf);
        this.parentdir = new Path(conf.get(HREGION_DIR, DEFAULT_HREGION_DIR));
        fs.mkdirs(parentdir);

      } catch(Throwable e) {
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

      // Start the HRegionServers

      if(this.conf.get(REGIONSERVER_ADDRESS) == null) {
        this.conf.set(REGIONSERVER_ADDRESS, "localhost:0");
      }
      
      LOG.info("Starting HRegionServers");
      startRegionServers(this.conf, nRegionNodes);
      
    } catch(Throwable e) {
      e.printStackTrace();
      shutdown();
    }
  }

  private void startRegionServers(Configuration conf, int nRegionNodes)
      throws IOException {
    this.regionServers = new HRegionServer[nRegionNodes];
    this.regionThreads = new Thread[nRegionNodes];
    
    for(int i = 0; i < nRegionNodes; i++) {
      regionServers[i] = new HRegionServer(conf);
      regionThreads[i] = new Thread(regionServers[i], "HRegionServer-" + i);
      regionThreads[i].start();
    }
  }
  
  /** 
   * Returns the rpc address actually used by the master server, because the 
   * supplied port is not necessarily the actual port used.
   */
  public HServerAddress getHMasterAddress() {
    return master.getMasterAddress();
  }
  
  /** Shut down the HBase cluster */
  public void shutdown() {
    LOG.info("Shutting down the HBase Cluster");
    for(int i = 0; i < regionServers.length; i++) {
      regionServers[i].stop();
    }
    master.shutdown();
    for(int i = 0; i < regionServers.length; i++) {
      try {
        regionThreads[i].join();
      } catch(InterruptedException e) {
        // continue
      }
    }
    try {
      masterThread.join();
    } catch(InterruptedException e) {
      // continue
    }
    LOG.info("HBase Cluster shutdown complete");

    if(cluster != null) {
      LOG.info("Shutting down Mini DFS cluster");
      cluster.shutdown();
    }
    
    // Delete all DFS files
    deleteFile(new File(System.getProperty(
        StaticTestEnvironment.TEST_DIRECTORY_KEY), "dfs"));
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
