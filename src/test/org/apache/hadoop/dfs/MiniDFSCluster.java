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
import org.apache.hadoop.dfs.FSConstants.StartupOption;
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

  private int nameNodePort = 0;
  private int nameNodeInfoPort = 0;
  
  /**
   * An inner class that runs a name node.
   */
  class NameNodeRunner implements Runnable {
    private NameNode node;
    private volatile boolean isInitialized = false;
    private boolean isCrashed = false;
    private boolean isRunning = true;

    public InetSocketAddress getAddress() {
      return node.getNameNodeAddress();
    }
    
    synchronized public boolean isInitialized() {
      return isInitialized;
    }
    
    synchronized public boolean isCrashed() {
      return isCrashed;
    }

    public boolean isUp() {
      if (node == null) {
        return false;
      }
      try {
        long[] sizes = node.getStats();
        boolean isUp = false;
        synchronized (this) {
          isUp = (isInitialized && !node.isInSafeMode() && sizes[0] != 0);
        }
        return isUp;
      } catch (IOException ie) {
        return false;
      }
    }
    
    /**
     * Create the name node and run it.
     */
    public void run() {
      try {
        synchronized( this ) {
          if( isRunning ) {
            node = new NameNode(conf);
          }
          isInitialized = true;
        }
      } catch (Throwable e) {
        shutdown();
        System.err.println("Name node crashed:");
        e.printStackTrace();
        synchronized (this) {
          isCrashed = true;
        }
      }
    }
    
    /**
     * Shutdown the name node and wait for it to finish.
     */
    public synchronized void shutdown() {
      isRunning = false;
      if (node != null) {
        node.stop();
        node.join();
        node = null;
      }
    }
  }
  
  /**
   * An inner class to run the data node.
   */
  class DataNodeRunner implements Runnable {
    private DataNode node;
    Configuration conf = null;
    private boolean isRunning = true;
    
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
        synchronized (this){
          if (isRunning) {
            node = new DataNode(conf, conf.get("dfs.datanode.rack", 
                NetworkTopology.DEFAULT_RACK), dirs);
          }
        }
        node.run();
      } catch (Throwable e) {
        shutdown();
        System.err.println("Data node crashed:");
        e.printStackTrace();
      }
    }

    /**    
     * Shut down the server and wait for it to finish.
     */
    public synchronized void shutdown() {
      isRunning = false;
      if (node != null) {
        node.shutdown();
        node = null;
      }
    }
  }

  public MiniDFSCluster(Configuration conf,
          int nDatanodes,
          boolean formatNamenode,
          String[] racks) throws IOException {
    this(0, conf, nDatanodes, formatNamenode, racks);
  }
  
  /**
   * Create the config and start up the servers.  If either the rpc or info port is already 
   * in use, we will try new ports.
   * @param namenodePort suggestion for which rpc port to use.  caller should use 
   *                     getNameNodePort() to get the actual port used.
   * @param dataNodeFirst should the datanode be brought up before the namenode?
   * @deprecated use {@link #MiniDFSCluster(Configuration, int, boolean, String[])}
   */
  public MiniDFSCluster(int namenodePort, 
                        Configuration conf,
                        boolean dataNodeFirst) throws IOException {
    this(namenodePort, conf, 1, dataNodeFirst, true, null);
  }

  /**
   * Create the config and start up the only the namenode.  If either the rpc or info port is already 
   * in use, we will try new ports.
   * @param namenodePort suggestion for which rpc port to use.  caller should use 
   *                     getNameNodePort() to get the actual port used.
   * @deprecated use {@link #MiniDFSCluster(Configuration, int, boolean, String[])}                     
   */
  public MiniDFSCluster(int namenodePort, 
                        Configuration conf,
                        int numRetries,
                        int numRetriesPerPort) throws IOException {
    this(namenodePort, conf, 0, false, false, null);  
  }

  /**
   * Create the config and start up the servers.  If either the rpc or info port is already 
   * in use, we will try new ports.
   * @param namenodePort suggestion for which rpc port to use.  caller should use 
   *                     getNameNodePort() to get the actual port used.
   * @param nDatanodes Number of datanodes   
   * @param dataNodeFirst should the datanode be brought up before the namenode?
   * @deprecated use {@link #MiniDFSCluster(Configuration, int, boolean, String[])}
   */
  public MiniDFSCluster(int namenodePort, 
                        Configuration conf,
                        int nDatanodes,
                        boolean dataNodeFirst) throws IOException {
    this(namenodePort, conf, nDatanodes, dataNodeFirst, true, null);
  }
  
  /**
   * Create the config and start up the servers.  If either the rpc or info port is already 
   * in use, we will try new ports.
   * @param namenodePort suggestion for which rpc port to use.  caller should use 
   *                     getNameNodePort() to get the actual port used.
   * @param nDatanodes Number of datanodes   
   * @param dataNodeFirst should the datanode be brought up before the namenode?
   * @param formatNamenode should the namenode be formatted before starting up ?
   * @deprecated use {@link #MiniDFSCluster(Configuration, int, boolean, String[])}
   */
  public MiniDFSCluster(int namenodePort, 
                        Configuration conf,
                        int nDatanodes,
                        boolean dataNodeFirst,
                        boolean formatNamenode ) throws IOException {
    this(namenodePort, conf, nDatanodes, dataNodeFirst, formatNamenode, null);
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
   * @deprecated use {@link #MiniDFSCluster(Configuration, int, String[])}
   */
  public MiniDFSCluster(int namenodePort, 
                        Configuration conf,
                        int nDatanodes,
                        boolean dataNodeFirst,
                        boolean formatNamenode,
                        String[] racks) throws IOException {
    this(namenodePort, conf, nDatanodes, 
        ! cannotStartDataNodeFirst(dataNodeFirst) &&  
        formatNamenode, racks);
  }

  /**
   * NameNode should be always started first.
   * Data-nodes need to handshake with the name-node before they can start.
   * 
   * @param dataNodeFirst should the datanode be brought up before the namenode?
   * @return false if dataNodeFirst is false
   * @throws IOException if dataNodeFirst is true
   * 
   * @deprecated should be removed when dataNodeFirst is gone.
   */
  private static boolean cannotStartDataNodeFirst( boolean dataNodeFirst 
                                                  ) throws IOException {
    if( dataNodeFirst )
      throw new IOException( "NameNode should be always started first." );
    return false;
  }

  /**
   * Create the config and start up the servers.  If either the rpc or info port is already 
   * in use, we will try new ports.
   * @param namenodePort suggestion for which rpc port to use.  caller should use 
   *                     getNameNodePort() to get the actual port used.
   * @param nDatanodes Number of datanodes   
   * @param formatNamenode should the namenode be formatted before starting up ?
   * @param racks array of strings indicating racks that each datanode is on
   */
  public MiniDFSCluster(int namenodePort, 
                        Configuration conf,
                        int nDatanodes,
                        boolean formatNamenode,
                        String[] racks) throws IOException {
    this.conf = conf;
    
    this.nDatanodes = nDatanodes;
    this.nameNodePort = namenodePort;

    this.conf.set("fs.default.name", "localhost:"+ Integer.toString(nameNodePort));
    this.conf.setInt("dfs.info.port", nameNodeInfoPort);
    this.conf.setInt("dfs.datanode.info.port", 0);

    File base_dir = new File(System.getProperty("test.build.data"), "dfs/");
    File data_dir = new File(base_dir, "data");
    this.conf.set("dfs.name.dir", new File(base_dir, "name1").getPath()+","+
        new File(base_dir, "name2").getPath());
    this.conf.setInt("dfs.replication", Math.min(3, nDatanodes));
    this.conf.setInt("dfs.safemode.extension", 0);

    // Create the NameNode
    StartupOption startOpt = 
      formatNamenode ? StartupOption.FORMAT : StartupOption.REGULAR;
    conf.setObject( "dfs.namenode.startup", startOpt );
    conf.setObject( "dfs.datanode.startup", startOpt );
    nameNode = new NameNodeRunner();
    nameNodeThread = new Thread(nameNode);

    //
    // Start the MiniDFSCluster
    //
    // Start the namenode and wait for it to be initialized
    nameNodeThread.start();
    while (!nameNode.isCrashed() && !nameNode.isInitialized()) {
      try {                                     // let daemons get started
        System.err.println("Waiting for the NameNode to initialize...");
        Thread.sleep(1000);
      } catch(InterruptedException e) {
      }
      if (nameNode.isCrashed()) {
        throw new RuntimeException("Namenode crashed");
      }
    }
    
    // Set up the right ports for the datanodes
    InetSocketAddress nnAddr = nameNode.getAddress(); 
    nameNodePort = nnAddr.getPort(); 
    this.conf.set("fs.default.name", nnAddr.getHostName()+ ":" + Integer.toString(nameNodePort));
    
    // Start the datanodes
    startDataNodes(conf, racks, data_dir);
    
    while (!nameNode.isCrashed() && !nameNode.isUp()) {
      try {                                     // let daemons get started
        System.err.println("Waiting for the Mini HDFS Cluster to start...");
        Thread.sleep(1000);
      } catch(InterruptedException e) {
      }
    }
    
    if (nameNode.isCrashed()) {
      throw new RuntimeException("Namenode crashed");
    }
  }

  private void startDataNodes(Configuration conf, String[] racks, File data_dir) {
    // Create the DataNodes & start them
    dataNodes = new DataNodeRunner[nDatanodes];
    dataNodeThreads = new Thread[nDatanodes];
    for (int idx = 0; idx < nDatanodes; idx++) {
      if( racks == null || idx >= racks.length) {
        dataNodes[idx] = new DataNodeRunner(conf, data_dir, idx);
      } else {
        dataNodes[idx] = new DataNodeRunner(conf, data_dir, racks[idx], idx);          
      }
      dataNodeThreads[idx] = new Thread(dataNodes[idx]);
      dataNodeThreads[idx].start();
    }
  }
  
  /**
   * Returns the rpc port used by the mini cluster, because the caller supplied port is 
   * not necessarily the actual port used.
   */     
  public int getNameNodePort() {
    return nameNode.getAddress().getPort();
  }
    
  /**
   * Shut down the servers.
   */
  public void shutdown() {
    System.out.println("Shutting down the cluster");
    for (int idx = 0; idx < nDatanodes; idx++) {
      dataNodes[idx].shutdown();
    }
    nameNode.shutdown();
    for (int idx = 0; idx < nDatanodes; idx++) {
      try {
        dataNodeThreads[idx].join();
      } catch(InterruptedException e) {
      }
    }
    try {
      nameNodeThread.join();
    } catch (InterruptedException e) {
    }
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
