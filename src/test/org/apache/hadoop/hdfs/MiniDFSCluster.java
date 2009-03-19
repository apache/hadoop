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
package org.apache.hadoop.hdfs;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.nio.channels.FileChannel;
import java.util.Random;
import java.io.RandomAccessFile;
import java.io.Closeable;

import javax.security.auth.login.LoginException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.net.*;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.FSConstants.DatanodeReportType;
import org.apache.hadoop.hdfs.server.common.HdfsConstants.StartupOption;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.datanode.FSDatasetInterface;
import org.apache.hadoop.hdfs.server.datanode.SimulatedFSDataset;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.tools.DFSAdmin;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.security.*;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.util.Service;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * This class creates a single-process DFS cluster for junit testing.
 * The data directories for non-simulated DFS are under the testing directory.
 * For simulated data nodes, no underlying fs storage is used.
 */
public class MiniDFSCluster implements Closeable {
  private static final int WAIT_SLEEP_TIME_MILLIS = 500;
  private static final int STARTUP_TIMEOUT_MILLIS = 30000;

  public class DataNodeProperties {
    DataNode datanode;
    Configuration conf;
    String[] dnArgs;

    DataNodeProperties(DataNode node, Configuration conf, String[] args) {
      this.datanode = node;
      this.conf = conf;
      this.dnArgs = args;
    }
  }

  private static final Log LOG = LogFactory.getLog(MiniDFSCluster.class);
  private Configuration conf;
  private NameNode nameNode;
  private int numDataNodes;
  private ArrayList<DataNodeProperties> dataNodes = 
                         new ArrayList<DataNodeProperties>();
  private File base_dir;
  private File data_dir;
  
  
  /**
   * This null constructor is used only when wishing to start a data node cluster
   * without a name node (ie when the name node is started elsewhere).
   */
  public MiniDFSCluster() {
  }
  
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
   * @param nameNodeOperation the operation with which to start the servers.  If null
   *          or StartupOption.FORMAT, then StartupOption.REGULAR will be used.
   */
  public MiniDFSCluster(Configuration conf,
                        int numDataNodes,
                        StartupOption nameNodeOperation) throws IOException {
    this(0, conf, numDataNodes, false, false, false,  nameNodeOperation, 
          null, null, null);
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
    this(0, conf, numDataNodes, format, true, true,  null, racks, null, null);
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
   * @param hosts array of strings indicating the hostname for each DataNode
   */
  public MiniDFSCluster(Configuration conf,
                        int numDataNodes,
                        boolean format,
                        String[] racks, String[] hosts) throws IOException {
    this(0, conf, numDataNodes, format, true, true, null, racks, hosts, null);
  }
  
  /**
   * NOTE: if possible, the other constructors that don't have nameNode port 
   * parameter should be used as they will ensure that the servers use free ports.
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
    this(nameNodePort, conf, numDataNodes, format, manageDfsDirs, manageDfsDirs,
         operation, racks, null, null);
  }

  /**
   * NOTE: if possible, the other constructors that don't have nameNode port 
   * parameter should be used as they will ensure that the servers use free ports.
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
   * @param simulatedCapacities array of capacities of the simulated data nodes
   */
  public MiniDFSCluster(int nameNodePort, 
                        Configuration conf,
                        int numDataNodes,
                        boolean format,
                        boolean manageDfsDirs,
                        StartupOption operation,
                        String[] racks,
                        long[] simulatedCapacities) throws IOException {
    this(nameNodePort, conf, numDataNodes, format, manageDfsDirs, manageDfsDirs,
          operation, racks, null, simulatedCapacities);
  }
  
  /**
   * NOTE: if possible, the other constructors that don't have nameNode port 
   * parameter should be used as they will ensure that the servers use free ports.
   * <p>
   * Modify the config and start up the servers.  
   * 
   * @param nameNodePort suggestion for which rpc port to use.  caller should
   *          use getNameNodePort() to get the actual port used.
   * @param conf the base configuration to use in starting the servers.  This
   *          will be modified as necessary.
   * @param numDataNodes Number of DataNodes to start; may be zero
   * @param format if true, format the NameNode and DataNodes before starting up
   * @param manageNameDfsDirs if true, the data directories for servers will be
   *          created and dfs.name.dir and dfs.data.dir will be set in the conf
   * @param manageDataDfsDirs if true, the data directories for datanodes will
   *          be created and dfs.data.dir set to same in the conf
   * @param operation the operation with which to start the servers.  If null
   *          or StartupOption.FORMAT, then StartupOption.REGULAR will be used.
   * @param racks array of strings indicating the rack that each DataNode is on
   * @param hosts array of strings indicating the hostnames of each DataNode
   * @param simulatedCapacities array of capacities of the simulated data nodes
   */
  public MiniDFSCluster(int nameNodePort, 
                        Configuration conf,
                        int numDataNodes,
                        boolean format,
                        boolean manageNameDfsDirs,
                        boolean manageDataDfsDirs,
                        StartupOption operation,
                        String[] racks, String hosts[],
                        long[] simulatedCapacities) throws IOException {
    this.conf = conf;
    try {
      UserGroupInformation.setCurrentUser(UnixUserGroupInformation.login(conf));
    } catch (LoginException e) {
      IOException ioe = new IOException();
      ioe.initCause(e);
      throw ioe;
    }
    base_dir = new File(System.getProperty("test.build.data", "build/test/data"), "dfs/");
    data_dir = new File(base_dir, "data");
    
    // Setup the NameNode configuration
    FileSystem.setDefaultUri(conf, "hdfs://localhost:"+ Integer.toString(nameNodePort));
    conf.set("dfs.http.address", "127.0.0.1:0");  
    if (manageNameDfsDirs) {
      conf.set("dfs.name.dir", new File(base_dir, "name1").getPath()+","+
               new File(base_dir, "name2").getPath());
      conf.set("fs.checkpoint.dir", new File(base_dir, "namesecondary1").
                getPath()+"," + new File(base_dir, "namesecondary2").getPath());
    }
    
    int replication = conf.getInt("dfs.replication", 3);
    conf.setInt("dfs.replication", Math.min(replication, numDataNodes));
    conf.setInt("dfs.safemode.extension", 0);
    conf.setInt("dfs.namenode.decommission.interval", 3); // 3 second
    
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
      new String[] {} : new String[] {operation.getName()};
    conf.setClass("topology.node.switch.mapping.impl", 
                   StaticMapping.class, DNSToSwitchMapping.class);
    nameNode = NameNode.createNameNode(args, conf);
    
    // Start the DataNodes
    startDataNodes(conf, numDataNodes, manageDataDfsDirs, 
                    operation, racks, hosts, simulatedCapacities);
    waitClusterUp();
  }

  /**
   * wait for the cluster to get out of
   * safemode.
   */
  public void waitClusterUp() {
    if (numDataNodes > 0) {
      try {
        while (!isClusterUp()) {
          LOG.warn("Waiting for the Mini HDFS Cluster to start...");
          Thread.sleep(WAIT_SLEEP_TIME_MILLIS);
        }
      } catch (InterruptedException e) {
        LOG.warn("Interrupted during startup", e);
      }
    }
  }

  /**
   * Modify the config and start up additional DataNodes.  The info port for
   * DataNodes is guaranteed to use a free port.
   *  
   *  Data nodes can run with the name node in the mini cluster or
   *  a real name node. For example, running with a real name node is useful
   *  when running simulated data nodes with a real name node.
   *  If minicluster's name node is null assume that the conf has been
   *  set with the right address:port of the name node.
   *
   * @param conf the base configuration to use in starting the DataNodes.  This
   *          will be modified as necessary.
   * @param numDataNodes Number of DataNodes to start; may be zero
   * @param manageDfsDirs if true, the data directories for DataNodes will be
   *          created and dfs.data.dir will be set in the conf
   * @param operation the operation with which to start the DataNodes.  If null
   *          or StartupOption.FORMAT, then StartupOption.REGULAR will be used.
   * @param racks array of strings indicating the rack that each DataNode is on
   * @param hosts array of strings indicating the hostnames for each DataNode
   * @param simulatedCapacities array of capacities of the simulated data nodes
   *
   * @throws IllegalStateException if NameNode has been shutdown
   */
  public synchronized void startDataNodes(Configuration conf, int numDataNodes, 
                             boolean manageDfsDirs, StartupOption operation, 
                             String[] racks, String[] hosts,
                             long[] simulatedCapacities) throws IOException {
    int curDatanodesNum = dataNodes.size();
    // for mincluster's the default initialDelay for BRs is 0
    if (conf.get("dfs.blockreport.initialDelay") == null) {
      conf.setLong("dfs.blockreport.initialDelay", 0);
    }
    // If minicluster's name node is null assume that the conf has been
    // set with the right address:port of the name node.
    //
    if (nameNode != null) { // set conf from the name node
      InetSocketAddress nnAddr = nameNode.getNameNodeAddress(); 
      int nameNodePort = nnAddr.getPort(); 
      FileSystem.setDefaultUri(conf, 
                               "hdfs://"+ nnAddr.getHostName() +
                               ":" + Integer.toString(nameNodePort));
    }
    
    if (racks != null && numDataNodes > racks.length ) {
      throw new IllegalArgumentException( "The length of racks [" + racks.length
          + "] is less than the number of datanodes [" + numDataNodes + "].");
    }
    if (hosts != null && numDataNodes > hosts.length ) {
      throw new IllegalArgumentException( "The length of hosts [" + hosts.length
          + "] is less than the number of datanodes [" + numDataNodes + "].");
    }
    //Generate some hostnames if required
    if (racks != null && hosts == null) {
      LOG.info("Generating host names for datanodes");
      hosts = new String[numDataNodes];
      for (int i = curDatanodesNum; i < curDatanodesNum + numDataNodes; i++) {
        hosts[i - curDatanodesNum] = "host" + i + ".foo.com";
      }
    }

    if (simulatedCapacities != null 
        && numDataNodes > simulatedCapacities.length) {
      throw new IllegalArgumentException( "The length of simulatedCapacities [" 
          + simulatedCapacities.length
          + "] is less than the number of datanodes [" + numDataNodes + "].");
    }

    // Set up the right ports for the datanodes
    conf.set("dfs.datanode.address", "127.0.0.1:0");
    conf.set("dfs.datanode.http.address", "127.0.0.1:0");
    conf.set("dfs.datanode.ipc.address", "127.0.0.1:0");
    

    String [] dnArgs = (operation == null ||
                        operation != StartupOption.ROLLBACK) ?
        null : new String[] {operation.getName()};
    
    
    for (int i = curDatanodesNum; i < curDatanodesNum+numDataNodes; i++) {
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
      if (simulatedCapacities != null) {
        dnConf.setBoolean("dfs.datanode.simulateddatastorage", true);
        dnConf.setLong(SimulatedFSDataset.CONFIG_PROPERTY_CAPACITY,
            simulatedCapacities[i-curDatanodesNum]);
      }
      LOG.info("Starting DataNode " + i + " with dfs.data.dir: "
                         + dnConf.get("dfs.data.dir"));
      if (hosts != null) {
        dnConf.set("slave.host.name", hosts[i - curDatanodesNum]);
        LOG.info("Starting DataNode " + i + " with hostname set to: "
                           + dnConf.get("slave.host.name"));
      }
      if (racks != null) {
        String name = hosts[i - curDatanodesNum];
        LOG.info("Adding node with hostname : " + name + " to rack "+
                            racks[i-curDatanodesNum]);
        StaticMapping.addNodeToRack(name,
                                    racks[i-curDatanodesNum]);
      }
      Configuration newconf = new Configuration(dnConf); // save config
      if (hosts != null) {
        NetUtils.addStaticResolution(hosts[i - curDatanodesNum], "localhost");
      }
      DataNode dn = DataNode.instantiateDataNode(dnArgs, dnConf);
      //since the HDFS does things based on IP:port, we need to add the mapping
      //for IP:port to rackId
      String ipAddr = dn.getSelfAddr().getAddress().getHostAddress();
      if (racks != null) {
        int port = dn.getSelfAddr().getPort();
        LOG.info("Adding node with IP:port : " + ipAddr + ":" + port+
                            " to rack " + racks[i-curDatanodesNum]);
        StaticMapping.addNodeToRack(ipAddr + ":" + port,
                                  racks[i-curDatanodesNum]);
      }
      DataNode.runDatanodeDaemon(dn);
      dataNodes.add(new DataNodeProperties(dn, newconf, dnArgs));
    }
    curDatanodesNum += numDataNodes;
    this.numDataNodes += numDataNodes;
    waitActive();
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
      String[] racks
      ) throws IOException {
    startDataNodes( conf,  numDataNodes, manageDfsDirs,  operation, racks, null, null);
  }
  
  /**
   * Modify the config and start up additional DataNodes.  The info port for
   * DataNodes is guaranteed to use a free port.
   *  
   *  Data nodes can run with the name node in the mini cluster or
   *  a real name node. For example, running with a real name node is useful
   *  when running simulated data nodes with a real name node.
   *  If minicluster's name node is null assume that the conf has been
   *  set with the right address:port of the name node.
   *
   * @param conf the base configuration to use in starting the DataNodes.  This
   *          will be modified as necessary.
   * @param numDataNodes Number of DataNodes to start; may be zero
   * @param manageDfsDirs if true, the data directories for DataNodes will be
   *          created and dfs.data.dir will be set in the conf
   * @param operation the operation with which to start the DataNodes.  If null
   *          or StartupOption.FORMAT, then StartupOption.REGULAR will be used.
   * @param racks array of strings indicating the rack that each DataNode is on
   * @param simulatedCapacities array of capacities of the simulated data nodes
   *
   * @throws IllegalStateException if NameNode has been shutdown
   */
  public void startDataNodes(Configuration conf, int numDataNodes, 
                             boolean manageDfsDirs, StartupOption operation, 
                             String[] racks,
                             long[] simulatedCapacities) throws IOException {
    startDataNodes(conf, numDataNodes, manageDfsDirs, operation, racks, null,
                   simulatedCapacities);
    
  }
  /**
   * If the NameNode is running, attempt to finalize a previous upgrade.
   * When this method return, the NameNode should be finalized, but
   * DataNodes may not be since that occurs asynchronously.
   *
   * @throws IllegalStateException if the Namenode is not running.
   */
  public void finalizeCluster(Configuration conf) throws Exception {
    if (nameNode == null) {
      throw new IllegalStateException("Attempting to finalize "
                                      + "Namenode but it is not running");
    }
    ToolRunner.run(new DFSAdmin(conf), new String[] {"-finalizeUpgrade"});
  }
  
  /**
   * Gets the started NameNode.  May be null.
   */
  public NameNode getNameNode() {
    return nameNode;
  }
  
  /**
   * Return the {@link FSNamesystem} object.
   * @return {@link FSNamesystem} object.
   */
  public FSNamesystem getNamesystem() {
    return nameNode.getNamesystem();
  }

  /**
   * Gets a list of the started DataNodes.  May be empty.
   */
  public ArrayList<DataNode> getDataNodes() {
    ArrayList<DataNode> list = new ArrayList<DataNode>();
    for (int i = 0; i < dataNodes.size(); i++) {
      DataNode node = dataNodes.get(i).datanode;
      list.add(node);
    }
    return list;
  }
  
  /** @return the datanode having the ipc server listen port */
  public DataNode getDataNode(int ipcPort) {
    for(DataNode dn : getDataNodes()) {
      if (dn.ipcServer.getListenerAddress().getPort() == ipcPort) {
        return dn;
      }
    }
    return null;
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
  public synchronized void shutdown() {
    LOG.info("Shutting down the Mini HDFS Cluster");
    shutdownDataNodes();
    if (nameNode != null) {
      nameNode.stop();
      nameNode.join();
      nameNode = null;
    }
  }

  /**
   * Shuts down the cluster.  
   *
   * @throws IOException if an I/O error occurs
   */
  public void close() throws IOException {
    shutdown();
  }

  /**
   * Static operation to shut down a cluster;
   * harmless if the cluster argument is null
   *
   * @param cluster cluster to shut down, or null for no cluster
   */
  public static void close(Closeable cluster) {
    Service.close(cluster);
  }

  /**
   * Shutdown all DataNodes started by this class.  The NameNode
   * is left running so that new DataNodes may be started.
   */
  public void shutdownDataNodes() {
    for (int i = dataNodes.size()-1; i >= 0; i--) {
      LOG.info("Shutting down DataNode " + i);
      DataNode dn = dataNodes.remove(i).datanode;
      dn.shutdown();
      numDataNodes--;
    }
  }

  /**
   * Returns a string representation of the cluster.
   *
   * @return a string representation of the cluster
   */
  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder();
    builder.append("Cluster up:").append(isClusterUp());
    builder.append("\nName Node:").append(getNameNode());
    builder.append("\nData node count:").append(dataNodes.size());
    for (DataNodeProperties dnp : dataNodes) {
      builder.append("\n Datanode: ").append(dnp.datanode);
      builder.append("\n  state: ").append(dnp.datanode.getServiceState());
    }
    return builder.toString();
  }

  /*
   * Corrupt a block on all datanode
   */
  void corruptBlockOnDataNodes(String blockName) throws Exception{
    for (int i=0; i < dataNodes.size(); i++)
      corruptBlockOnDataNode(i,blockName);
  }

  /*
   * Corrupt a block on a particular datanode
   */
  boolean corruptBlockOnDataNode(int i, String blockName) throws Exception {
    Random random = new Random();
    boolean corrupted = false;
    File dataDir = new File(System.getProperty("test.build.data", "build/test/data"), "dfs/data");
    if (i < 0 || i >= dataNodes.size())
      return false;
    for (int dn = i*2; dn < i*2+2; dn++) {
      File blockFile = new File(dataDir, "data" + (dn+1) + "/current/" +
                                blockName);
      System.out.println("Corrupting for: " + blockFile);
      if (blockFile.exists()) {
        // Corrupt replica by writing random bytes into replica
        RandomAccessFile raFile = new RandomAccessFile(blockFile, "rw");
        try {
          FileChannel channel = raFile.getChannel();
          String badString = "BADBAD";
          int rand = random.nextInt((int) channel.size() / 2);
          raFile.seek(rand);
          raFile.write(badString.getBytes());
        } finally {
          raFile.close();
        }
      }
      corrupted = true;
    }
    return corrupted;
  }

  /*
   * Shutdown a particular datanode
   */
  public DataNodeProperties stopDataNode(int i) {
    if (i < 0 || i >= dataNodes.size()) {
      return null;
    }
    DataNodeProperties dnprop = dataNodes.remove(i);
    DataNode dn = dnprop.datanode;
    LOG.info("MiniDFSCluster Stopping DataNode " +
                       dn.dnRegistration.getName() +
                       " from a total of " + (dataNodes.size() + 1) + 
                       " datanodes.");
    dn.shutdown();
    numDataNodes--;
    return dnprop;
  }

  /**
   * Restart a datanode
   * @param dnprop datanode's property
   * @return true if restarting is successful
   * @throws IOException
   */
  public synchronized boolean restartDataNode(DataNodeProperties dnprop)
  throws IOException {
    Configuration conf = dnprop.conf;
    String[] args = dnprop.dnArgs;
    Configuration newconf = new Configuration(conf); // save cloned config
    dataNodes.add(new DataNodeProperties(
                     DataNode.createDataNode(args, conf), 
                     newconf, args));
    numDataNodes++;
    return true;

  }
  /*
   * Restart a particular datanode
   */
  public synchronized boolean restartDataNode(int i) throws IOException {
    DataNodeProperties dnprop = stopDataNode(i);
    if (dnprop == null) {
      return false;
    } else {
      return restartDataNode(dnprop);
    }
  }

  /**
   * Shutdown a datanode by name.
   * @param name datanode name
   * @return true if a node was shut down
   */
  public synchronized DataNodeProperties stopDataNode(String name) {
    int i;
    for (i = 0; i < dataNodes.size(); i++) {
      DataNode dn = dataNodes.get(i).datanode;
      if (dn.dnRegistration.getName().equals(name)) {
        break;
      }
    }
    return stopDataNode(i);
  }
  
  /**
   * @return true if the NameNode is running and is out of Safe Mode.
   */
  public boolean isClusterUp() {
    if (nameNode == null) {
      return false;
    }
    long[] sizes = nameNode.getStats();
    boolean isUp = false;
    synchronized (this) {
      isUp = (!nameNode.isInSafeMode() && sizes[0] != 0);
    }
    return isUp;
  }
  
  /**
   * @return true if there is at least one DataNode running.
   */
  public boolean isDataNodeUp() {
    if (dataNodes == null || dataNodes.size() == 0) {
      return false;
    }
    return true;
  }
  
  /**
   * Get a client handle to the DFS cluster.
   * @return a new filesystem, which must be closed when no longer needed.
   * @throws IOException if the filesystem cannot be created
   */
  public FileSystem getFileSystem() throws IOException {
    return FileSystem.get(conf);
  }

  /**
   * @return the directories where the namenode stores its state.
   */
  public Collection<File> getNameDirs() {
    return FSNamesystem.getNamespaceDirs(conf);
  }

  /**
   * Get the directories where the namenode stores its edits.
   */
  public Collection<File> getNameEditsDirs() {
    return FSNamesystem.getNamespaceEditsDirs(conf);
  }

  /**
   * Wait until the cluster is active and running.
   */
  public void waitActive() throws IOException {
    if (nameNode == null) {
      return;
    }
    InetSocketAddress addr = new InetSocketAddress("localhost",
                                                   getNameNodePort());
    DFSClient client = new DFSClient(addr, conf);
    try {
      DatanodeInfo[] dnInfos;

      // make sure all datanodes are alive
      long timeout=System.currentTimeMillis() + STARTUP_TIMEOUT_MILLIS;
      while((dnInfos = client.datanodeReport(DatanodeReportType.LIVE)).length
          != numDataNodes) {
        try {
          Thread.sleep(WAIT_SLEEP_TIME_MILLIS);
          if(System.currentTimeMillis() > timeout) {
            throw new IOException("Timeout waiting for the datanodes. "+
                    "Expected " + numDataNodes + "but got " + dnInfos.length);
          }
        } catch (InterruptedException e) {
          throw new IOException("Interrupted while waiting for the datanodes",e);
        }
      }
    } finally {
      client.close();
    }

  }
  
  public void formatDataNodeDirs() throws IOException {
    base_dir = new File(System.getProperty("test.build.data", "build/test/data"), "dfs/");
    data_dir = new File(base_dir, "data");
    if (data_dir.exists() && !FileUtil.fullyDelete(data_dir)) {
      throw new IOException("Cannot remove data directory: " + data_dir);
    }
  }
  
  /**
   * 
   * @param dataNodeIndex - data node whose block report is desired - the index is same as for getDataNodes()
   * @return the block report for the specified data node
   */
  public Block[] getBlockReport(int dataNodeIndex) {
    if (dataNodeIndex < 0 || dataNodeIndex > dataNodes.size()) {
      throw new IndexOutOfBoundsException();
    }
    return dataNodes.get(dataNodeIndex).datanode.getFSDataset().getBlockReport();
  }
  
  
  /**
   * 
   * @return block reports from all data nodes
   *    Block[] is indexed in the same order as the list of datanodes returned by getDataNodes()
   */
  public Block[][] getAllBlockReports() {
    int numDataNodes = dataNodes.size();
    Block[][] result = new Block[numDataNodes][];
    for (int i = 0; i < numDataNodes; ++i) {
     result[i] = getBlockReport(i);
    }
    return result;
  }
  
  
  /**
   * This method is valid only if the data nodes have simulated data
   * @param dataNodeIndex - data node i which to inject - the index is same as for getDataNodes()
   * @param blocksToInject - the blocks
   * @throws IOException
   *              if not simulatedFSDataset
   *             if any of blocks already exist in the data node
   *   
   */
  public void injectBlocks(int dataNodeIndex, Block[] blocksToInject) throws IOException {
    if (dataNodeIndex < 0 || dataNodeIndex > dataNodes.size()) {
      throw new IndexOutOfBoundsException();
    }
    FSDatasetInterface dataSet = dataNodes.get(dataNodeIndex).datanode.getFSDataset();
    if (!(dataSet instanceof SimulatedFSDataset)) {
      throw new IOException("injectBlocks is valid only for SimilatedFSDataset");
    }
    SimulatedFSDataset sdataset = (SimulatedFSDataset) dataSet;
    sdataset.injectBlocks(blocksToInject);
    dataNodes.get(dataNodeIndex).datanode.scheduleBlockReport(0);
  }
  
  /**
   * This method is valid only if the data nodes have simulated data
   * @param blocksToInject - blocksToInject[] is indexed in the same order as the list 
   *             of datanodes returned by getDataNodes()
   * @throws IOException
   *             if not simulatedFSDataset
   *             if any of blocks already exist in the data nodes
   *             Note the rest of the blocks are not injected.
   */
  public void injectBlocks(Block[][] blocksToInject) throws IOException {
    if (blocksToInject.length >  dataNodes.size()) {
      throw new IndexOutOfBoundsException();
    }
    for (int i = 0; i < blocksToInject.length; ++i) {
     injectBlocks(i, blocksToInject[i]);
    }
  }

  /**
   * Set the softLimit and hardLimit of client lease periods
   */
  void setLeasePeriod(long soft, long hard) {
    final FSNamesystem namesystem = nameNode.getNamesystem();
    namesystem.leaseManager.setLeasePeriod(soft, hard);
    namesystem.lmthread.interrupt();
  }

  /**
   * @return the current set of datanodes
   */
  DataNode[] listDataNodes() {
    DataNode[] list = new DataNode[dataNodes.size()];
    for (int i = 0; i < dataNodes.size(); i++) {
      list[i] = dataNodes.get(i).datanode;
    }
    return list;
  }

  /**
   * Access to the data directory used for Datanodes
   * @throws IOException 
   */
  public String getDataDirectory() {
    return data_dir.getAbsolutePath();
  }
}
