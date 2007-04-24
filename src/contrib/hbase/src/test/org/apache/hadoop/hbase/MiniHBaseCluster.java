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

/**
 * This class creates a single process HBase cluster for junit testing.
 * One thread is created for each server.
 */
public class MiniHBaseCluster implements HConstants {
  private Configuration conf;
  private MiniDFSCluster cluster;
  private FileSystem fs;
  private Path parentdir;
  private HMasterRunner master;
  private Thread masterThread;
  private HRegionServerRunner[] regionServers;
  private Thread[] regionThreads;
  
  public MiniHBaseCluster(Configuration conf, int nRegionNodes) {
    this.conf = conf;

    try {
      try {
        if(System.getProperty("test.build.data") == null) {
          File testDir = new File(new File("").getAbsolutePath(),
              "build/contrib/hbase/test");

          String dir = testDir.getAbsolutePath();
          System.out.println(dir);
          System.setProperty("test.build.data", dir);
        }

        // To run using configured filesystem, comment out this
        // line below that starts up the MiniDFSCluster.
        this.cluster = new MiniDFSCluster(this.conf, 2, true, (String[])null);
        this.fs = FileSystem.get(conf);
        this.parentdir =
          new Path(conf.get(HREGION_DIR, DEFAULT_HREGION_DIR));
        fs.mkdirs(parentdir);

      } catch(Throwable e) {
        System.err.println("Mini DFS cluster failed to start");
        e.printStackTrace();
        throw e;
      }

      if(this.conf.get(MASTER_ADDRESS) == null) {
        this.conf.set(MASTER_ADDRESS, "localhost:0");
      }
      
      // Create the master

      this.master = new HMasterRunner();
      this.masterThread = new Thread(master, "HMaster");

      // Start up the master

      masterThread.start();
      while(! master.isCrashed() && ! master.isInitialized()) {
        try {
          System.err.println("Waiting for HMaster to initialize...");
          Thread.sleep(1000);

        } catch(InterruptedException e) {
        }
        if(master.isCrashed()) {
          throw new RuntimeException("HMaster crashed");
        }
      }

      // Set the master's port for the HRegionServers

      this.conf.set(MASTER_ADDRESS, master.getHMasterAddress().toString());

      // Start the HRegionServers

      if(this.conf.get(REGIONSERVER_ADDRESS) == null) {
        this.conf.set(REGIONSERVER_ADDRESS, "localhost:0");
      }
      
      startRegionServers(this.conf, nRegionNodes);

      // Wait for things to get started

      while(! master.isCrashed() && ! master.isUp()) {
        try {
          System.err.println("Waiting for Mini HBase cluster to start...");
          Thread.sleep(1000);

        } catch(InterruptedException e) {
        }
        if(master.isCrashed()) {
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
    return master.getHMasterAddress();
  }
  
  /** Shut down the HBase cluster */
  public void shutdown() {
    System.out.println("Shutting down the HBase Cluster");
    for(int i = 0; i < regionServers.length; i++) {
      regionServers[i].shutdown();
    }
    master.shutdown();
    
    for(int i = 0; i < regionServers.length; i++) {
      try {
        regionThreads[i].join();
        
      } catch(InterruptedException e) {
      }
    }
    try {
      masterThread.join();
      
    } catch(InterruptedException e) {
    }
    
    System.out.println("Shutting down Mini DFS cluster");
    if (cluster != null) {
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
    private volatile boolean isInitialized = false;
    private boolean isCrashed = false;
    private boolean isRunning = true;
    
    public HServerAddress getHMasterAddress() {
      return master.getMasterAddress();
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
            master = new HMaster(conf);
          }
          isInitialized = true;
        }
      } catch(Throwable e) {
        shutdown();
        System.err.println("HMaster crashed:");
        e.printStackTrace();
        synchronized(this) {
          isCrashed = true;
        }
      }
    }
    
    /** Shut down the HMaster and wait for it to finish */
    public synchronized void shutdown() {
      isRunning = false;
      if(master != null) {
        try {
          master.stop();
          
        } catch(IOException e) {
          System.err.println("Master crashed during stop");
          e.printStackTrace();
          
        } finally {
          master.join();
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
        System.err.println("HRegionServer crashed:");
        e.printStackTrace();
      }
    }
    
    /** Shut down the HRegionServer */
    public synchronized void shutdown() {
      isRunning = false;
      if(server != null) {
        try {
          server.stop();
          
        } catch(IOException e) {
          System.err.println("HRegionServer crashed during stop");
          e.printStackTrace();
          
        } finally {
          server.join();
          server = null;
        }
      }
    }
  }
}
