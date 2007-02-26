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
package org.apache.hadoop.dfs;

import java.io.*;
import java.net.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.net.NetworkTopology;

/**
 * This class creates a single-process DFS cluster for junit testing.
 * One thread is created for each server.
 * The data directories for DFS are undering the testing directory.
 * @author Owen O'Malley
 */
public class MiniDFSCluster {

  private Configuration conf;
  int nDatanodes;
  private Thread nameNodeThread;
  private Thread dataNodeThreads[];
  private NameNodeRunner nameNode;
  private DataNodeRunner dataNodes[];
  static public int MAX_RETRIES  = 10;
  static public int MAX_RETRIES_PER_PORT = 10;

  private int nameNodePort = 0;
  private int nameNodeInfoPort = 0;
  
  /**
   * An inner class that runs a name node.
   */
  class NameNodeRunner implements Runnable {
    private NameNode node;
    
    public boolean isUp() {
      if (node == null) {
        return false;
      }
      try {
        long[] sizes = node.getStats();
        return !node.isInSafeMode() && sizes[0] != 0;
      } catch (IOException ie) {
        return false;
      }
    }
    
    /**
     * Create the name node and run it.
     */
    public void run() {
      try {
        node = new NameNode(conf);
      } catch (Throwable e) {
        node = null;
        System.err.println("Name node crashed:");
        e.printStackTrace();
      }
    }
    
    /**
     * Shutdown the name node and wait for it to finish.
     */
    public void shutdown() {
      if (node != null) {
        node.stop();
        node.join();
      }
    }
  }
  
  /**
   * An inner class to run the data node.
   */
  class DataNodeRunner implements Runnable {
    private DataNode node;
    Configuration conf = null;
    
    public DataNodeRunner(Configuration conf, File dataDir, int index) {
      this.conf = new Configuration(conf);
      this.conf.set("dfs.data.dir",
          new File(dataDir, "data"+(2*index+1)).getPath()+","+
          new File(dataDir, "data"+(2*index+2)).getPath());
    }
    
    public DataNodeRunner(Configuration conf, File dataDir, 
        String networkLoc, int index) {
        this(conf, dataDir, index);
        this.conf.set("dfs.datanode.rack", networkLoc);
    }
        
    /**
     * Create and run the data node.
     */
    public void run() {
      try {
        String[] dirs = conf.getStrings("dfs.data.dir");
        for (int idx = 0; idx < dirs.length; idx++) {
          File dataDir = new File(dirs[idx]);
          synchronized (DataNodeRunner.class) {
            if (!dataDir.mkdirs()) {
              if (!dataDir.isDirectory()) {
                throw new RuntimeException("Mkdirs failed to create directory " +
                    dataDir.toString());
              }
            }
          }
        }
        node = new DataNode(conf, conf.get("dfs.datanode.rack", 
            NetworkTopology.DEFAULT_RACK), dirs);
        node.run();
      } catch (Throwable e) {
        node = null;
        System.err.println("Data node crashed:");
        e.printStackTrace();
      }
    }

    /**    
     * Shut down the server and wait for it to finish.
     */
    public void shutdown() {
      if (node != null) {
        node.shutdown();
      }
    }
  }

  /**
   * Create the config and start up the servers.  If either the rpc or info port is already 
   * in use, we will try new ports.
   * @param namenodePort suggestion for which rpc port to use.  caller should use 
   *                     getNameNodePort() to get the actual port used.
   * @param dataNodeFirst should the datanode be brought up before the namenode?
   */
  public MiniDFSCluster(int namenodePort, 
                        Configuration conf,
                        boolean dataNodeFirst) throws IOException {
    this(namenodePort, conf, 1, dataNodeFirst, true, 
         MAX_RETRIES, MAX_RETRIES_PER_PORT, null);
  }

  /**
   * Create the config and start up the only the namenode.  If either the rpc or info port is already 
   * in use, we will try new ports.
   * @param namenodePort suggestion for which rpc port to use.  caller should use 
   *                     getNameNodePort() to get the actual port used.
   */
  public MiniDFSCluster(int namenodePort, 
                        Configuration conf,
                        int numRetries,
                        int numRetriesPerPort) throws IOException {
    this(namenodePort, conf, 0, false, false, 
         numRetries, numRetriesPerPort, null);
  }

  /**
   * Create the config and start up the servers.  If either the rpc or info port is already 
   * in use, we will try new ports.
   * @param namenodePort suggestion for which rpc port to use.  caller should use 
   *                     getNameNodePort() to get the actual port used.
   * @param nDatanodes Number of datanodes   
   * @param dataNodeFirst should the datanode be brought up before the namenode?
   */
  public MiniDFSCluster(int namenodePort, 
                        Configuration conf,
                        int nDatanodes,
                        boolean dataNodeFirst) throws IOException {
    this(namenodePort, conf, nDatanodes, dataNodeFirst, true, 
         MAX_RETRIES, MAX_RETRIES_PER_PORT, null);
  }
  
  /**
   * Create the config and start up the servers.  If either the rpc or info port is already 
   * in use, we will try new ports.
   * @param namenodePort suggestion for which rpc port to use.  caller should use 
   *                     getNameNodePort() to get the actual port used.
   * @param nDatanodes Number of datanodes   
   * @param dataNodeFirst should the datanode be brought up before the namenode?
   * @param formatNamenode should the namenode be formatted before starting up ?
   */
  public MiniDFSCluster(int namenodePort, 
                        Configuration conf,
                        int nDatanodes,
                        boolean dataNodeFirst,
                        boolean formatNamenode ) throws IOException {
    this(namenodePort, conf, nDatanodes, dataNodeFirst, formatNamenode, 
         MAX_RETRIES, MAX_RETRIES_PER_PORT, null);
  }

  public MiniDFSCluster(int namenodePort, 
                        Configuration conf,
                        int nDatanodes,
                        boolean dataNodeFirst,
                        boolean formatNamenode,
                        String[] racks) throws IOException {
    this(namenodePort, conf, nDatanodes, dataNodeFirst, formatNamenode, 
         MAX_RETRIES, MAX_RETRIES_PER_PORT, racks);
  }
  
  /**
   * Create the config and start up the servers.  If either the rpc or info port is already 
   * in use, we will try new ports.
   * @param namenodePort suggestion for which rpc port to use.  caller should use 
   *                     getNameNodePort() to get the actual port used.
   * @param nDatanodes Number of datanodes   
   * @param dataNodeFirst should the datanode be brought up before the namenode?
   * @param formatNamenode should the namenode be formatted before starting up ?
   * @param racks array of strings indicating racks that each datanode is on
   */
  public MiniDFSCluster(int namenodePort, 
                        Configuration conf,
                        int nDatanodes,
                        boolean dataNodeFirst,
                        boolean formatNamenode,
                        int numRetries,
                        int numRetriesPerPort,
                        String[] racks) throws IOException {

    this.conf = conf;

    this.nDatanodes = nDatanodes;
    this.nameNodePort = namenodePort;
    this.nameNodeInfoPort = 50080;   // We just want this port to be different from the default.
    File base_dir = new File(System.getProperty("test.build.data"),
                             "dfs/");
    File data_dir = new File(base_dir, "data");
    conf.set("dfs.name.dir", new File(base_dir, "name1").getPath()+","+
        new File(base_dir, "name2").getPath());
    conf.setInt("dfs.replication", Math.min(3, nDatanodes));
    conf.setInt("dfs.safemode.extension", 0);

    // Loops until we find ports that work or we give up because 
    // too many tries have failed.
    boolean foundPorts = false;
    int portsTried = 0;
    while ((!foundPorts) && (portsTried < numRetries)) {
      conf.set("fs.default.name", 
               "localhost:"+ Integer.toString(nameNodePort));
      conf.set("dfs.info.port", nameNodeInfoPort);
      
      if (formatNamenode) { NameNode.format(conf); }
      nameNode = new NameNodeRunner();
      nameNodeThread = new Thread(nameNode);
      dataNodes = new DataNodeRunner[nDatanodes];
      dataNodeThreads = new Thread[nDatanodes];
      for (int idx = 0; idx < nDatanodes; idx++) {
        if( racks == null || idx >= racks.length) {
          dataNodes[idx] = new DataNodeRunner(conf, data_dir, idx);
        } else {
          dataNodes[idx] = new DataNodeRunner(conf, data_dir, racks[idx], idx);          
        }
        dataNodeThreads[idx] = new Thread(dataNodes[idx]);
      }
      if (dataNodeFirst) {
        for (int idx = 0; idx < nDatanodes; idx++) {
          dataNodeThreads[idx].start();
        }
        nameNodeThread.start();      
      } else {
        nameNodeThread.start();
        for (int idx = 0; idx < nDatanodes; idx++) {
          dataNodeThreads[idx].start();
        }
      }

      int retry = 0;
      while (!nameNode.isUp() && (retry < numRetriesPerPort)) {
        try {                                     // let daemons get started
          System.out.println("waiting for dfs minicluster to start");
          Thread.sleep(1000);
        } catch(InterruptedException e) {
        }
        retry++;
      }
      if (retry >= numRetriesPerPort) {
        this.nameNodePort += 3;
        this.nameNodeInfoPort += 7;
        System.out.println("Failed to start DFS minicluster in " + retry + " attempts.  Trying new ports:");
        System.out.println("\tNameNode RPC port: " + nameNodePort);
        System.out.println("\tNameNode info port: " + nameNodeInfoPort);

        nameNode.shutdown();
        for (int idx = 0; idx < nDatanodes; idx++) {
          dataNodes[idx].shutdown();
        }
        
      } else {
        foundPorts = true;
      }
      portsTried++;
    } 
    System.out.println("\tNameNode portsTried " + portsTried);
    if (portsTried >= numRetries) {
        throw new IOException("Failed to start a DFS minicluster after trying " + portsTried + " ports.");
    }
  }

  /**
   * Returns the rpc port used by the mini cluster, because the caller supplied port is 
   * not necessarily the actual port used.
   */     
  public int getNameNodePort() {
    return nameNodePort;
  }
    
  /**
   * Shut down the servers.
   */
  public void shutdown() {
    for (int idx = 0; idx < nDatanodes; idx++) {
      dataNodes[idx].shutdown();
    }
    nameNode.shutdown();
  }
  
  /**
   * Get a client handle to the DFS cluster.
   */
  public FileSystem getFileSystem() throws IOException {
    return FileSystem.get(conf);
  }

  /**
   * Get the directories where the namenode stores image
   */
  public File[] getNameDirs() {
    return NameNode.getDirs(conf);
  }

   /**
   * Wait till the cluster is active and running.
   */
  public void waitActive() throws IOException {
    InetSocketAddress addr = new InetSocketAddress("localhost",
                                             getNameNodePort());
    DFSClient client = new DFSClient(addr, conf);

    //
    // get initial state of datanodes
    //  
    DatanodeInfo[] oldinfo = client.datanodeReport();
    while (oldinfo.length != nDatanodes) {
      try {
        Thread.sleep(500);
      } catch (Exception e) {
      }
      oldinfo = client.datanodeReport();
    }

    // 
    // wait till all datanodes send at least yet another heartbeat
    //
    int numdead = 0;
    while (numdead > 0) {
      try {
        Thread.sleep(500);
      } catch (Exception e) {
      }
      DatanodeInfo[] info = client.datanodeReport();
      if (info.length != nDatanodes) {
        continue;
      }
      numdead = 0;
      for (int i = 0; i < info.length; i++) {
        if (oldinfo[i].getLastUpdate() >= info[i].getLastUpdate()) {
          numdead++;
        }
      }
    }
    client.close();
  }
}
