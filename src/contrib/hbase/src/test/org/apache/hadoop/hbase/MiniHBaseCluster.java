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
  private HMasterRunner masterRunner;
  private Thread masterRunnerThread;
  private HRegionServerRunner[] regionServers;
  private Thread[] regionThreads;
  
  public MiniHBaseCluster(Configuration conf, int nRegionNodes) {
    this(conf, nRegionNodes, true);
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

    try {
      try {
        if(System.getProperty("test.build.data") == null) {
          File testDir = new File(new File("").getAbsolutePath(),
              "build/contrib/hbase/test");

          String dir = testDir.getAbsolutePath();
          LOG.info("Setting test.build.data to " + dir);
          System.setProperty("test.build.data", dir);
        }

        if (miniHdfsFilesystem) {
          this.cluster =
            new MiniDFSCluster(this.conf, 2, true, (String[])null);
        }
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
      this.masterRunner = new HMasterRunner();
      this.masterRunnerThread = new Thread(masterRunner, "masterRunner");

      // Start up the master
      LOG.info("Starting HMaster");
      masterRunnerThread.start();
      while(! masterRunner.isCrashed() && ! masterRunner.isInitialized()) {
        try {
          LOG.info("...waiting for HMaster to initialize...");
          Thread.sleep(1000);
        } catch(InterruptedException e) {
        }
        if(masterRunner.isCrashed()) {
          throw new RuntimeException("HMaster crashed");
        }
      }
      LOG.info("HMaster started.");
      
      // Set the master's port for the HRegionServers
      String address = masterRunner.getHMasterAddress().toString();
      this.conf.set(MASTER_ADDRESS, address);

      // Start the HRegionServers

      if(this.conf.get(REGIONSERVER_ADDRESS) == null) {
        this.conf.set(REGIONSERVER_ADDRESS, "localhost:0");
      }
      
      LOG.info("Starting HRegionServers");
      startRegionServers(this.conf, nRegionNodes);
      LOG.info("HRegionServers running");

      // Wait for things to get started

      while(! masterRunner.isCrashed() && ! masterRunner.isUp()) {
        try {
          LOG.info("Waiting for Mini HBase cluster to start...");
          Thread.sleep(1000);
        } catch(InterruptedException e) {
        }
        if(masterRunner.isCrashed()) {
          throw new RuntimeException("HMaster crashed");
        }
      }
      
    } catch(Throwable e) {
      // Delete all DFS files
      deleteFile(new File(System.getProperty("test.build.data"), "dfs"));
      throw new RuntimeException("Mini HBase cluster did not start");
    }
  }
  
  private void startRegionServers(Configuration conf, int nRegionNodes) {
    this.regionServers = new HRegionServerRunner[nRegionNodes];
    this.regionThreads = new Thread[nRegionNodes];
    
    for(int i = 0; i < nRegionNodes; i++) {
      regionServers[i] = new HRegionServerRunner(conf);
      regionThreads[i] = new Thread(regionServers[i], "HRegionServer-" + i);
      regionThreads[i].start();
    }
  }
  
  /** 
   * Returns the rpc address actually used by the master server, because the 
   * supplied port is not necessarily the actual port used.
   */
  public HServerAddress getHMasterAddress() {
    return masterRunner.getHMasterAddress();
  }
  
  /** Shut down the HBase cluster */
  public void shutdown() {
    LOG.info("Shutting down the HBase Cluster");
    for(int i = 0; i < regionServers.length; i++) {
      regionServers[i].shutdown();
    }
    masterRunner.shutdown();
    for(int i = 0; i < regionServers.length; i++) {
      try {
        regionThreads[i].join();
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
    try {
      masterRunnerThread.join();
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    if (cluster != null) {
      LOG.info("Shutting down Mini DFS cluster");
      cluster.shutdown();
    }
    
    // Delete all DFS files
    deleteFile(new File(System.getProperty("test.build.data"), "dfs"));
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
  
  private class HMasterRunner implements Runnable {
    private HMaster master = null;
    private Thread masterThread = null;
    private volatile boolean isInitialized = false;
    private boolean isCrashed = false;
    private boolean isRunning = true;
    private long threadSleepTime = conf.getLong(THREAD_WAKE_FREQUENCY, 10 * 1000);
    
    public HServerAddress getHMasterAddress() {
      return this.master.getMasterAddress();
    }
    
    public synchronized boolean isInitialized() {
      return isInitialized;
    }
    
    public synchronized boolean isCrashed() {
      return isCrashed;
    }
    
    public boolean isUp() {
      if(master == null) {
        return false;
      }
      synchronized(this) {
        return isInitialized;
      }
    }
    
    /** Create the HMaster and run it */
    public void run() {
      try {
        synchronized(this) {
          if(isRunning) {
            this.master = new HMaster(conf);
            masterThread = new Thread(this.master);
            masterThread.start();
          }
          isInitialized = true;
        }
      } catch(Throwable e) {
        shutdown();
        LOG.error("HMaster crashed:", e);
        synchronized(this) {
          isCrashed = true;
        }
      }

      while(this.master != null && this.master.isMasterRunning()) {
        try {
          Thread.sleep(threadSleepTime);
          
        } catch(InterruptedException e) {
        }
      }
      synchronized(this) {
        isCrashed = true;
      }
      shutdown();
    }
    
    /** Shut down the HMaster and wait for it to finish */
    public synchronized void shutdown() {
      isRunning = false;
      if (this.master != null) {
        try {
          this.master.shutdown();
        } catch(IOException e) {
          LOG.error("Master crashed during stop", e);
        } finally {
          try {
            masterThread.join();
          } catch(InterruptedException e) {
          }
          master = null;
        }
      }
    }
  }
  
  private class HRegionServerRunner implements Runnable {
    private HRegionServer server = null;
    private boolean isRunning = true;
    private Configuration conf;
    
    public HRegionServerRunner(Configuration conf) {
      this.conf = conf;
    }
    
    /** Start up the HRegionServer */
    public void run() {
      try {
        synchronized(this) {
          if(isRunning) {
            server = new HRegionServer(conf);
          }
        }
        server.run();
        
      } catch(Throwable e) {
        shutdown();
        LOG.error("HRegionServer crashed:", e);
      }
    }
    
    /** Shut down the HRegionServer */
    public synchronized void shutdown() {
      isRunning = false;
      if(server != null) {
        try {
          server.stop();
          
        } catch(IOException e) {
          LOG.error("HRegionServer crashed during stop", e);
        } finally {
          server.join();
          server = null;
        }
      }
    }
  }
}
