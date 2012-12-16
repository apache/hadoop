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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.server.common.HdfsConstants.StartupOption;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.datanode.SimulatedFSDataset;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.net.StaticMapping;

public class MiniDFSClusterWithNodeGroup extends MiniDFSCluster {

  private static String[] NODE_GROUPS = null;
  private static final Log LOG = LogFactory.getLog(MiniDFSClusterWithNodeGroup.class);
  
  public MiniDFSClusterWithNodeGroup(int nameNodePort, 
          Configuration conf,
          int numDataNodes,
          boolean format,
          boolean manageDfsDirs,
          StartupOption operation,
          String[] racks,
          long[] simulatedCapacities) throws IOException {
    super(nameNodePort, conf, numDataNodes, format, manageDfsDirs, 
        manageDfsDirs, operation, racks, null, simulatedCapacities);
  }
  
  public MiniDFSClusterWithNodeGroup(int nameNodePort, 
          Configuration conf,
          int numDataNodes,
          boolean format,
          boolean manageDfsDirs,
          StartupOption operation,
          String[] racks,
          String[] hosts,
          long[] simulatedCapacities) throws IOException {
    super(nameNodePort, conf, numDataNodes, format, manageDfsDirs, 
        manageDfsDirs, operation, racks, hosts, simulatedCapacities);
  }

  // NODE_GROUPS should be set before constructor being executed.
  public static void setNodeGroups(String[] nodeGroups) {
    NODE_GROUPS = nodeGroups;
  }

  public synchronized void startDataNodes(Configuration conf, int numDataNodes,
      boolean manageDfsDirs, StartupOption operation, 
      String[] racks, String[] nodeGroups, String[] hosts,
      long[] simulatedCapacities) throws IOException {
    conf.set("slave.host.name", "127.0.0.1");

    int curDatanodesNum = dataNodes.size();
    // for mincluster's the default initialDelay for BRs is 0
    if (conf.get(DFSConfigKeys.DFS_BLOCKREPORT_INITIAL_DELAY_KEY) == null) {
      conf.setLong(DFSConfigKeys.DFS_BLOCKREPORT_INITIAL_DELAY_KEY, 0);
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

    if (nodeGroups != null && numDataNodes > nodeGroups.length ) {
      throw new IllegalArgumentException( "The length of nodeGroups [" + nodeGroups.length
          + "] is less than the number of datanodes [" + numDataNodes + "].");
    }

    if (hosts != null && numDataNodes > hosts.length ) {
      throw new IllegalArgumentException( "The length of hosts [" + hosts.length
          + "] is less than the number of datanodes [" + numDataNodes + "].");
    }
    //Generate some hostnames if required
    if (racks != null && hosts == null) {
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
        dnConf.set(DataNode.DATA_DIR_KEY, dir1.getPath() + "," + dir2.getPath());
      }
      if (simulatedCapacities != null) {
        dnConf.setBoolean("dfs.datanode.simulateddatastorage", true);
        dnConf.setLong(SimulatedFSDataset.CONFIG_PROPERTY_CAPACITY,
        simulatedCapacities[i-curDatanodesNum]);
      }
      LOG.info("Starting DataNode " + i + " with "
          + DFSConfigKeys.DFS_DATANODE_DATA_DIR_KEY + ": "
          + dnConf.get(DFSConfigKeys.DFS_DATANODE_DATA_DIR_KEY));
      if (hosts != null) {
        dnConf.set("slave.host.name", hosts[i - curDatanodesNum]);
        LOG.info("Starting DataNode " + i + " with hostname set to: "
            + dnConf.get("slave.host.name"));
      }
      if (racks != null) {
        String name = hosts[i - curDatanodesNum];
        if (nodeGroups == null) {
          LOG.info("Adding node with hostname : " + name + " to rack " +
             racks[i-curDatanodesNum]);
          StaticMapping.addNodeToRack(name,racks[i-curDatanodesNum]);
        } else {
          LOG.info("Adding node with hostname : " + name + " to serverGroup " +
              nodeGroups[i-curDatanodesNum] + " and rack " +
              racks[i-curDatanodesNum]);
          StaticMapping.addNodeToRack(name,racks[i-curDatanodesNum] + 
              nodeGroups[i-curDatanodesNum]);
        }
      }
      Configuration newconf = new Configuration(dnConf); // save config
      if (hosts != null) {
        NetUtils.addStaticResolution(hosts[i - curDatanodesNum], "localhost");
      }
      DataNode dn = DataNode.instantiateDataNode(dnArgs, dnConf);
      if(dn == null)
        throw new IOException("Cannot start DataNode in "
          + dnConf.get(DFSConfigKeys.DFS_DATANODE_DATA_DIR_KEY));
      //since the HDFS does things based on IP:port, we need to add the mapping
      //for IP:port to rackId
      String ipAddr = dn.getSelfAddr().getAddress().getHostAddress();
      if (racks != null) {
        int port = dn.getSelfAddr().getPort();
        if (nodeGroups == null) {
          LOG.info("Adding node with IP:port : " + ipAddr + ":" + port +
              " to rack " + racks[i-curDatanodesNum]);
          StaticMapping.addNodeToRack(ipAddr + ":" + port,
              racks[i-curDatanodesNum]);
        } else {
          LOG.info("Adding node with IP:port : " + ipAddr + ":" + port + " to nodeGroup " +
          nodeGroups[i-curDatanodesNum] + " and rack " + racks[i-curDatanodesNum]);
          StaticMapping.addNodeToRack(ipAddr + ":" + port, racks[i-curDatanodesNum] + 
              nodeGroups[i-curDatanodesNum]);
        }
      }
      DataNode.runDatanodeDaemon(dn);
      dataNodes.add(new DataNodeProperties(dn, newconf, dnArgs));
    }
    curDatanodesNum += numDataNodes;
    this.numDataNodes += numDataNodes;
    waitActive();
  }

  public synchronized void startDataNodes(Configuration conf, int numDataNodes, 
      boolean manageDfsDirs, StartupOption operation, 
      String[] racks, String[] nodeGroups, String[] hosts,
      long[] simulatedCapacities,
      boolean setupHostsFile) throws IOException {
    startDataNodes(conf, numDataNodes, manageDfsDirs, operation, racks, nodeGroups, 
        hosts, simulatedCapacities);
  }

  // This is for initialize from parent class.
  @Override
  public synchronized void startDataNodes(Configuration conf, int numDataNodes, 
          boolean manageDfsDirs, StartupOption operation, 
          String[] racks, String[] hosts,
          long[] simulatedCapacities) throws IOException {
    startDataNodes(conf, numDataNodes, manageDfsDirs, operation, racks, NODE_GROUPS, hosts,
        simulatedCapacities);
  }
  
}

