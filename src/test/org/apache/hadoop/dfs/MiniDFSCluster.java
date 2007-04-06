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
import java.util.ArrayList;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.dfs.FSConstants.StartupOption;
import org.apache.hadoop.fs.*;

/**
 * This class creates a single-process DFS cluster for junit testing.
 * The data directories for DFS are undering the testing directory.
 */
public class MiniDFSCluster {

  private Configuration conf;
  private NameNode nameNode;
  private ArrayList<DataNode> dataNodes = new ArrayList<DataNode>();
  private File base_dir;
  private File data_dir;
  
  /**
   * Modify the config and start up the servers with the given operation.
   * Servers will be started on free ports.
   * <p>
   * The caller must manage the creation of NameNode and DataNode directories
   * and have already set dfs.name.dir and dfs.data.dir in the given conf.
   * 
   * @param conf the base configuration to use in starting the servers.  This
   *          will be modified as necessary.
   * @param numDataNodes Number of DataNodes to start; may be zero
   * @param operation the operation with which to start the servers.  If null
   *          or StartupOption.FORMAT, then StartupOption.REGULAR will be used.
   */
  public MiniDFSCluster(Configuration conf,
                        int numDataNodes,
                        StartupOption nameNodeOperation) throws IOException {
    this(0, conf, numDataNodes, false, false, nameNodeOperation, null);
  }
  
  /**
   * Modify the config and start up the servers.  The rpc and info ports for
   * servers are guaranteed to use free ports.
   * <p>
   * NameNode and DataNode directory creation and configuration will be
   * managed by this class.
   *
   * @param conf the base configuration to use in starting the servers.  This
   *          will be modified as necessary.
   * @param numDataNodes Number of DataNodes to start; may be zero
   * @param format if true, format the NameNode and DataNodes before starting up
   * @param racks array of strings indicating the rack that each DataNode is on
   */
  public MiniDFSCluster(Configuration conf,
                        int numDataNodes,
                        boolean format,
                        String[] racks) throws IOException {
    this(0, conf, numDataNodes, format, true, null, racks);
  }

  /**
   * NOTE: if possible, the other constructors should be used as they will
   * ensure that the servers use free ports.
   * <p>
   * Modify the config and start up the servers.  
   * 
   * @param nameNodePort suggestion for which rpc port to use.  caller should
   *          use getNameNodePort() to get the actual port used.
   * @param conf the base configuration to use in starting the servers.  This
   *          will be modified as necessary.
   * @param numDataNodes Number of DataNodes to start; may be zero
   * @param format if true, format the NameNode and DataNodes before starting up
   * @param manageDfsDirs if true, the data directories for servers will be
   *          created and dfs.name.dir and dfs.data.dir will be set in the conf
   * @param operation the operation with which to start the servers.  If null
   *          or StartupOption.FORMAT, then StartupOption.REGULAR will be used.
   * @param racks array of strings indicating the rack that each DataNode is on
   */
  public MiniDFSCluster(int nameNodePort, 
                        Configuration conf,
                        int numDataNodes,
                        boolean format,
                        boolean manageDfsDirs,
                        StartupOption operation,
                        String[] racks) throws IOException {
    this.conf = conf;
    base_dir = new File(System.getProperty("test.build.data"), "dfs/");
    data_dir = new File(base_dir, "data");
    
    // Setup the NameNode configuration
    conf.set("fs.default.name", "localhost:"+ Integer.toString(nameNodePort));
    conf.setInt("dfs.info.port", 0);
    if (manageDfsDirs) {
    conf.set("dfs.name.dir", new File(base_dir, "name1").getPath()+","+
        new File(base_dir, "name2").getPath());
    }
    conf.setInt("dfs.replication", Math.min(3, numDataNodes));
    conf.setInt("dfs.safemode.extension", 0);
    
    // Format and clean out DataNode directories
    if (format) {
      if (data_dir.exists() && !FileUtil.fullyDelete(data_dir)) {
        throw new IOException("Cannot remove data directory: " + data_dir);
      }
      NameNode.format(conf); 
    }
    
    // Start the NameNode
    String[] args = (operation == null ||
      operation == StartupOption.FORMAT ||
      operation == StartupOption.REGULAR) ?
        new String[] {} : new String[] {"-"+operation.toString()};
    nameNode = NameNode.createNameNode(args, conf);
    
    // Start the DataNodes
    startDataNodes(conf, numDataNodes, manageDfsDirs, operation, racks);
    
    if (numDataNodes > 0) {
      while (!isClusterUp()) {
        try {
          System.err.println("Waiting for the Mini HDFS Cluster to start...");
          Thread.sleep(1000);
        } catch (InterruptedException e) {
        }
      }
    }
  }
  
  /**
   * Modify the config and start up the DataNodes.  The info port for
   * DataNodes is guaranteed to use a free port.
   *
   * @param conf the base configuration to use in starting the DataNodes.  This
   *          will be modified as necessary.
   * @param numDataNodes Number of DataNodes to start; may be zero
   * @param manageDfsDirs if true, the data directories for DataNodes will be
   *          created and dfs.data.dir will be set in the conf
   * @param operation the operation with which to start the DataNodes.  If null
   *          or StartupOption.FORMAT, then StartupOption.REGULAR will be used.
   * @param racks array of strings indicating the rack that each DataNode is on
   *
   * @throws IllegalStateException if NameNode has been shutdown
   */
  public void startDataNodes(Configuration conf, int numDataNodes, 
                             boolean manageDfsDirs, StartupOption operation, 
                             String[] racks) throws IOException {
    if (nameNode == null) {
      throw new IllegalStateException("NameNode is not running");
    }

    // Set up the right ports for the datanodes
    conf.setInt("dfs.datanode.info.port", 0);
    InetSocketAddress nnAddr = nameNode.getNameNodeAddress(); 
    int nameNodePort = nnAddr.getPort(); 
    conf.set("fs.default.name", 
      nnAddr.getHostName()+ ":" + Integer.toString(nameNodePort));
    
    String[] args = (operation == null ||
      operation == StartupOption.FORMAT ||
      operation == StartupOption.REGULAR) ?
        new String[] {} : new String[] {"-"+operation.toString()};
        
    for (int i = 0; i < numDataNodes; i++) {
      Configuration dnConf = new Configuration(conf);
      if (manageDfsDirs) {
        File dir1 = new File(data_dir, "data"+(2*i+1));
        File dir2 = new File(data_dir, "data"+(2*i+2));
        dir1.mkdirs();
        dir2.mkdirs();
        if (!dir1.isDirectory() || !dir2.isDirectory()) { 
          throw new IOException("Mkdirs failed to create directory for DataNode "
            + i + ": " + dir1 + " or " + dir2);
        }
        dnConf.set("dfs.data.dir", dir1.getPath() + "," + dir2.getPath()); 
      }
      if (racks != null && i < racks.length) {
        dnConf.set("dfs.datanode.rack", racks[i]);
      }
      System.out.println("Starting DataNode " + i + " with dfs.data.dir: " 
        + dnConf.get("dfs.data.dir"));
      dataNodes.add(DataNode.createDataNode(args, dnConf));
    }
  }
  
  /**
   * If the NameNode is running, attempt to finalize a previous upgrade.
   * When this method return, the NameNode should be finalized, but
   * DataNodes may not be since that occurs asynchronously.
   *
   * @throw IllegalStateException if the Namenode is not running.
   */
  public void finalizeCluster(Configuration conf) throws Exception {
    if (nameNode == null) {
      throw new IllegalStateException("Attempting to finalize "
        + "Namenode but it is not running");
    }
    new DFSAdmin().doMain(conf, new String[] {"-finalizeUpgrade"});
  }
  
  /**
   * Gets the started NameNode.  May be null.
   */
  public NameNode getNameNode() {
    return nameNode;
  }
  
  /**
   * Gets a list of the started DataNodes.  May be empty.
   */
  public ArrayList<DataNode> getDataNodes() {
    return dataNodes;
  }
  
  /**
   * Gets the rpc port used by the NameNode, because the caller 
   * supplied port is not necessarily the actual port used.
   */     
  public int getNameNodePort() {
    return nameNode.getNameNodeAddress().getPort();
  }
    
  /**
   * Shut down the servers that are up.
   */
  public void shutdown() {
    System.out.println("Shutting down the Mini HDFS Cluster");
    shutdownDataNodes();
    if (nameNode != null) {
      nameNode.stop();
      nameNode.join();
      nameNode = null;
    }
  }
  
  /**
   * Shutdown all DataNodes started by this class.  The NameNode
   * is left running so that new DataNodes may be started.
   */
  public void shutdownDataNodes() {
    for (int i = dataNodes.size()-1; i >= 0; i--) {
      System.out.println("Shutting down DataNode " + i);
      DataNode dn = dataNodes.remove(i);
      dn.shutdown();
    }
  }
  
  /**
   * Returns true if the NameNode is running and is out of Safe Mode.
   */
  public boolean isClusterUp() {
    if (nameNode == null) {
      return false;
    }
    try {
      long[] sizes = nameNode.getStats();
      boolean isUp = false;
      synchronized (this) {
        isUp = (!nameNode.isInSafeMode() && sizes[0] != 0);
      }
      return isUp;
    } catch (IOException ie) {
      return false;
    }
  }
  
  /**
   * Returns true if there is at least one DataNode running.
   */
  public boolean isDataNodeUp() {
    if (dataNodes == null || dataNodes.size() == 0) {
      return false;
    }
    return true;
  }
  
  /**
   * Get a client handle to the DFS cluster.
   */
  public FileSystem getFileSystem() throws IOException {
    return FileSystem.get(conf);
  }

  /**
   * Get the directories where the namenode stores its state.
   */
  public File[] getNameDirs() {
    return NameNode.getDirs(conf);
  }

   /**
   * Wait until the cluster is active and running.
   */
  public void waitActive() throws IOException {
    InetSocketAddress addr = new InetSocketAddress("localhost",
                                             getNameNodePort());
    DFSClient client = new DFSClient(addr, conf);

    //
    // get initial state of datanodes
    //  
    DatanodeInfo[] oldinfo = client.datanodeReport();
    while (oldinfo.length != dataNodes.size()) {
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
      if (info.length != dataNodes.size()) {
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
