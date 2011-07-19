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
package org.apache.hadoop.hdfs.server.blockmanagement;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.net.NetworkTopology;
import org.apache.hadoop.util.Daemon;

/**
 * Manage datanodes, include decommission and other activities.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class DatanodeManager {
  static final Log LOG = LogFactory.getLog(DatanodeManager.class);

  final FSNamesystem namesystem;

  /** Cluster network topology */
  private final NetworkTopology networktopology = new NetworkTopology();

  /** Host names to datanode descriptors mapping. */
  private final Host2NodesMap host2DatanodeMap = new Host2NodesMap();
  
  DatanodeManager(final FSNamesystem namesystem) {
    this.namesystem = namesystem;
  }

  private Daemon decommissionthread = null;

  void activate(final Configuration conf) {
    this.decommissionthread = new Daemon(new DecommissionManager(namesystem).new Monitor(
        conf.getInt(DFSConfigKeys.DFS_NAMENODE_DECOMMISSION_INTERVAL_KEY, 
                    DFSConfigKeys.DFS_NAMENODE_DECOMMISSION_INTERVAL_DEFAULT),
        conf.getInt(DFSConfigKeys.DFS_NAMENODE_DECOMMISSION_NODES_PER_INTERVAL_KEY, 
                    DFSConfigKeys.DFS_NAMENODE_DECOMMISSION_NODES_PER_INTERVAL_DEFAULT)));
    decommissionthread.start();
  }

  void close() {
    if (decommissionthread != null) decommissionthread.interrupt();
  }

  /** @return the network topology. */
  public NetworkTopology getNetworkTopology() {
    return networktopology;
  }
  
  /** Sort the located blocks by the distance to the target host. */
  public void sortLocatedBlocks(final String targethost,
      final List<LocatedBlock> locatedblocks) {
    //sort the blocks
    final DatanodeDescriptor client = getDatanodeByHost(targethost);
    for (LocatedBlock b : locatedblocks) {
      networktopology.pseudoSortByDistance(client, b.getLocations());
      
      // Move decommissioned datanodes to the bottom
      Arrays.sort(b.getLocations(), DFSUtil.DECOM_COMPARATOR);
    }    
  }
  
  /** @return the datanode descriptor for the host. */
  public DatanodeDescriptor getDatanodeByHost(final String host) {
    return host2DatanodeMap.getDatanodeByHost(host);
  }

  /** Add a datanode. */
  public void addDatanode(final DatanodeDescriptor node) {
    // To keep host2DatanodeMap consistent with datanodeMap,
    // remove  from host2DatanodeMap the datanodeDescriptor removed
    // from datanodeMap before adding node to host2DatanodeMap.
    synchronized (namesystem.datanodeMap) {
      host2DatanodeMap.remove(
          namesystem.datanodeMap.put(node.getStorageID(), node));
    }

    host2DatanodeMap.add(node);
    networktopology.add(node);

    if (LOG.isDebugEnabled()) {
      LOG.debug(getClass().getSimpleName() + ".addDatanode: "
          + "node " + node.getName() + " is added to datanodeMap.");
    }
  }

  /** Physically remove node from datanodeMap. */
  public void wipeDatanode(final DatanodeID node) throws IOException {
    final String key = node.getStorageID();
    synchronized (namesystem.datanodeMap) {
      host2DatanodeMap.remove(namesystem.datanodeMap.remove(key));
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug(getClass().getSimpleName() + ".wipeDatanode("
          + node.getName() + "): storage " + key 
          + " is removed from datanodeMap.");
    }
  }
}
