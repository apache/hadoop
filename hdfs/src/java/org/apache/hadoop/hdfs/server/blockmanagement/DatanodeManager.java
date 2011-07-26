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
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.FSConstants.DatanodeReportType;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.protocol.DatanodeRegistration;
import org.apache.hadoop.hdfs.server.protocol.DisallowedDatanodeException;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.net.CachedDNSToSwitchMapping;
import org.apache.hadoop.net.DNSToSwitchMapping;
import org.apache.hadoop.net.NetworkTopology;
import org.apache.hadoop.net.ScriptBasedMapping;
import org.apache.hadoop.util.Daemon;
import org.apache.hadoop.util.HostsFileReader;
import org.apache.hadoop.util.ReflectionUtils;

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

  private final DNSToSwitchMapping dnsToSwitchMapping;

  /** Read include/exclude files*/
  private final HostsFileReader hostsReader; 
  
  DatanodeManager(final FSNamesystem namesystem, final Configuration conf
      ) throws IOException {
    this.namesystem = namesystem;
    this.hostsReader = new HostsFileReader(
        conf.get(DFSConfigKeys.DFS_HOSTS, ""),
        conf.get(DFSConfigKeys.DFS_HOSTS_EXCLUDE, ""));

    this.dnsToSwitchMapping = ReflectionUtils.newInstance(
        conf.getClass(DFSConfigKeys.NET_TOPOLOGY_NODE_SWITCH_MAPPING_IMPL_KEY, 
            ScriptBasedMapping.class, DNSToSwitchMapping.class), conf);
    
    // If the dns to switch mapping supports cache, resolve network
    // locations of those hosts in the include list and store the mapping
    // in the cache; so future calls to resolve will be fast.
    if (dnsToSwitchMapping instanceof CachedDNSToSwitchMapping) {
      dnsToSwitchMapping.resolve(new ArrayList<String>(hostsReader.getHosts()));
    }
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
  private void addDatanode(final DatanodeDescriptor node) {
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
  private void wipeDatanode(final DatanodeID node) throws IOException {
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

  /* Resolve a node's network location */
  private void resolveNetworkLocation (DatanodeDescriptor node) {
    List<String> names = new ArrayList<String>(1);
    if (dnsToSwitchMapping instanceof CachedDNSToSwitchMapping) {
      // get the node's IP address
      names.add(node.getHost());
    } else {
      // get the node's host name
      String hostName = node.getHostName();
      int colon = hostName.indexOf(":");
      hostName = (colon==-1)?hostName:hostName.substring(0,colon);
      names.add(hostName);
    }
    
    // resolve its network location
    List<String> rName = dnsToSwitchMapping.resolve(names);
    String networkLocation;
    if (rName == null) {
      LOG.error("The resolve call returned null! Using " + 
          NetworkTopology.DEFAULT_RACK + " for host " + names);
      networkLocation = NetworkTopology.DEFAULT_RACK;
    } else {
      networkLocation = rName.get(0);
    }
    node.setNetworkLocation(networkLocation);
  }

  private boolean inHostsList(DatanodeID node, String ipAddr) {
     return checkInList(node, ipAddr, hostsReader.getHosts(), false);
  }
  
  private boolean inExcludedHostsList(DatanodeID node, String ipAddr) {
    return checkInList(node, ipAddr, hostsReader.getExcludedHosts(), true);
  }

  /**
   * Remove an already decommissioned data node who is neither in include nor
   * exclude hosts lists from the the list of live or dead nodes.  This is used
   * to not display an already decommssioned data node to the operators.
   * The operation procedure of making a already decommissioned data node not
   * to be displayed is as following:
   * <ol>
   *   <li> 
   *   Host must have been in the include hosts list and the include hosts list
   *   must not be empty.
   *   </li>
   *   <li>
   *   Host is decommissioned by remaining in the include hosts list and added
   *   into the exclude hosts list. Name node is updated with the new 
   *   information by issuing dfsadmin -refreshNodes command.
   *   </li>
   *   <li>
   *   Host is removed from both include hosts and exclude hosts lists.  Name 
   *   node is updated with the new informationby issuing dfsamin -refreshNodes 
   *   command.
   *   <li>
   * </ol>
   * 
   * @param nodeList
   *          , array list of live or dead nodes.
   */
  public void removeDecomNodeFromList(final List<DatanodeDescriptor> nodeList) {
    // If the include list is empty, any nodes are welcomed and it does not
    // make sense to exclude any nodes from the cluster. Therefore, no remove.
    if (hostsReader.getHosts().isEmpty()) {
      return;
    }
    
    for (Iterator<DatanodeDescriptor> it = nodeList.iterator(); it.hasNext();) {
      DatanodeDescriptor node = it.next();
      if ((!inHostsList(node, null)) && (!inExcludedHostsList(node, null))
          && node.isDecommissioned()) {
        // Include list is not empty, an existing datanode does not appear
        // in both include or exclude lists and it has been decommissioned.
        // Remove it from the node list.
        it.remove();
      }
    }
  }

  /**
   * Check if the given node (of DatanodeID or ipAddress) is in the (include or
   * exclude) list.  If ipAddress in null, check only based upon the given 
   * DatanodeID.  If ipAddress is not null, the ipAddress should refers to the
   * same host that given DatanodeID refers to.
   * 
   * @param node, the host DatanodeID
   * @param ipAddress, if not null, should refers to the same host
   *                   that DatanodeID refers to
   * @param hostsList, the list of hosts in the include/exclude file
   * @param isExcludeList, boolean, true if this is the exclude list
   * @return boolean, if in the list
   */
  private static boolean checkInList(final DatanodeID node,
      final String ipAddress,
      final Set<String> hostsList,
      final boolean isExcludeList) {
    final InetAddress iaddr;
    if (ipAddress != null) {
      try {
        iaddr = InetAddress.getByName(ipAddress);
      } catch (UnknownHostException e) {
        LOG.warn("Unknown ip address: " + ipAddress, e);
        return isExcludeList;
      }
    } else {
      try {
        iaddr = InetAddress.getByName(node.getHost());
      } catch (UnknownHostException e) {
        LOG.warn("Unknown host: " + node.getHost(), e);
        return isExcludeList;
      }
    }

    // if include list is empty, host is in include list
    if ( (!isExcludeList) && (hostsList.isEmpty()) ){
      return true;
    }
    return // compare ipaddress(:port)
    (hostsList.contains(iaddr.getHostAddress().toString()))
        || (hostsList.contains(iaddr.getHostAddress().toString() + ":"
            + node.getPort()))
        // compare hostname(:port)
        || (hostsList.contains(iaddr.getHostName()))
        || (hostsList.contains(iaddr.getHostName() + ":" + node.getPort()))
        || ((node instanceof DatanodeInfo) && hostsList
            .contains(((DatanodeInfo) node).getHostName()));
  }

  /**
   * Decommission the node if it is in exclude list.
   */
  private void checkDecommissioning(DatanodeDescriptor nodeReg, String ipAddr) 
    throws IOException {
    // If the registered node is in exclude list, then decommission it
    if (inExcludedHostsList(nodeReg, ipAddr)) {
      namesystem.getBlockManager().startDecommission(nodeReg);
    }
  }

  
  /**
   * Generate new storage ID.
   * 
   * @return unique storage ID
   * 
   * Note: that collisions are still possible if somebody will try 
   * to bring in a data storage from a different cluster.
   */
  private String newStorageID() {
    String newID = null;
    while(newID == null) {
      newID = "DS" + Integer.toString(DFSUtil.getRandom().nextInt());
      if (namesystem.datanodeMap.get(newID) != null)
        newID = null;
    }
    return newID;
  }

  public void registerDatanode(DatanodeRegistration nodeReg
      ) throws IOException {
    String dnAddress = Server.getRemoteAddress();
    if (dnAddress == null) {
      // Mostly called inside an RPC.
      // But if not, use address passed by the data-node.
      dnAddress = nodeReg.getHost();
    }      

    // Checks if the node is not on the hosts list.  If it is not, then
    // it will be disallowed from registering. 
    if (!inHostsList(nodeReg, dnAddress)) {
      throw new DisallowedDatanodeException(nodeReg);
    }

    String hostName = nodeReg.getHost();
      
    // update the datanode's name with ip:port
    DatanodeID dnReg = new DatanodeID(dnAddress + ":" + nodeReg.getPort(),
                                      nodeReg.getStorageID(),
                                      nodeReg.getInfoPort(),
                                      nodeReg.getIpcPort());
    nodeReg.updateRegInfo(dnReg);
    nodeReg.exportedKeys = namesystem.getBlockKeys();
      
    NameNode.stateChangeLog.info("BLOCK* NameSystem.registerDatanode: "
        + "node registration from " + nodeReg.getName()
        + " storage " + nodeReg.getStorageID());

    DatanodeDescriptor nodeS = namesystem.datanodeMap.get(nodeReg.getStorageID());
    DatanodeDescriptor nodeN = getDatanodeByHost(nodeReg.getName());
      
    if (nodeN != null && nodeN != nodeS) {
      NameNode.LOG.info("BLOCK* NameSystem.registerDatanode: "
                        + "node from name: " + nodeN.getName());
      // nodeN previously served a different data storage, 
      // which is not served by anybody anymore.
      namesystem.removeDatanode(nodeN);
      // physically remove node from datanodeMap
      wipeDatanode(nodeN);
      nodeN = null;
    }

    if (nodeS != null) {
      if (nodeN == nodeS) {
        // The same datanode has been just restarted to serve the same data 
        // storage. We do not need to remove old data blocks, the delta will
        // be calculated on the next block report from the datanode
        if(NameNode.stateChangeLog.isDebugEnabled()) {
          NameNode.stateChangeLog.debug("BLOCK* NameSystem.registerDatanode: "
                                        + "node restarted.");
        }
      } else {
        // nodeS is found
        /* The registering datanode is a replacement node for the existing 
          data storage, which from now on will be served by a new node.
          If this message repeats, both nodes might have same storageID 
          by (insanely rare) random chance. User needs to restart one of the
          nodes with its data cleared (or user can just remove the StorageID
          value in "VERSION" file under the data directory of the datanode,
          but this is might not work if VERSION file format has changed 
       */        
        NameNode.stateChangeLog.info( "BLOCK* NameSystem.registerDatanode: "
                                      + "node " + nodeS.getName()
                                      + " is replaced by " + nodeReg.getName() + 
                                      " with the same storageID " +
                                      nodeReg.getStorageID());
      }
      // update cluster map
      getNetworkTopology().remove(nodeS);
      nodeS.updateRegInfo(nodeReg);
      nodeS.setHostName(hostName);
      nodeS.setDisallowed(false); // Node is in the include list
      
      // resolve network location
      resolveNetworkLocation(nodeS);
      getNetworkTopology().add(nodeS);
        
      // also treat the registration message as a heartbeat
      synchronized(namesystem.heartbeats) {
        if( !namesystem.heartbeats.contains(nodeS)) {
          namesystem.heartbeats.add(nodeS);
          //update its timestamp
          nodeS.updateHeartbeat(0L, 0L, 0L, 0L, 0, 0);
          nodeS.isAlive = true;
        }
      }
      checkDecommissioning(nodeS, dnAddress);
      return;
    } 

    // this is a new datanode serving a new data storage
    if (nodeReg.getStorageID().equals("")) {
      // this data storage has never been registered
      // it is either empty or was created by pre-storageID version of DFS
      nodeReg.storageID = newStorageID();
      if(NameNode.stateChangeLog.isDebugEnabled()) {
        NameNode.stateChangeLog.debug(
            "BLOCK* NameSystem.registerDatanode: "
            + "new storageID " + nodeReg.getStorageID() + " assigned.");
      }
    }
    // register new datanode
    DatanodeDescriptor nodeDescr 
      = new DatanodeDescriptor(nodeReg, NetworkTopology.DEFAULT_RACK, hostName);
    resolveNetworkLocation(nodeDescr);
    addDatanode(nodeDescr);
    checkDecommissioning(nodeDescr, dnAddress);
    
    // also treat the registration message as a heartbeat
    synchronized(namesystem.heartbeats) {
      namesystem.heartbeats.add(nodeDescr);
      nodeDescr.isAlive = true;
      // no need to update its timestamp
      // because its is done when the descriptor is created
    }
  }

  /** Reread include/exclude files. */
  public void refreshHostsReader(Configuration conf) throws IOException {
    // Reread the conf to get dfs.hosts and dfs.hosts.exclude filenames.
    // Update the file names and refresh internal includes and excludes list.
    if (conf == null) {
      conf = new HdfsConfiguration();
    }
    hostsReader.updateFileNames(conf.get(DFSConfigKeys.DFS_HOSTS, ""), 
                                conf.get(DFSConfigKeys.DFS_HOSTS_EXCLUDE, ""));
    hostsReader.refresh();
  }
  
  /**
   * Rereads the config to get hosts and exclude list file names.
   * Rereads the files to update the hosts and exclude lists.  It
   * checks if any of the hosts have changed states:
   * 1. Added to hosts  --> no further work needed here.
   * 2. Removed from hosts --> mark AdminState as decommissioned. 
   * 3. Added to exclude --> start decommission.
   * 4. Removed from exclude --> stop decommission.
   */
  public void refreshDatanodes() throws IOException {
    for(DatanodeDescriptor node : namesystem.datanodeMap.values()) {
      // Check if not include.
      if (!inHostsList(node, null)) {
        node.setDisallowed(true);  // case 2.
      } else {
        if (inExcludedHostsList(node, null)) {
          namesystem.getBlockManager().startDecommission(node);   // case 3.
        } else {
          namesystem.getBlockManager().stopDecommission(node);   // case 4.
        }
      }
    }
  }

  /** For generating datanode reports */
  public List<DatanodeDescriptor> getDatanodeListForReport(
      final DatanodeReportType type) {
    boolean listLiveNodes = type == DatanodeReportType.ALL ||
                            type == DatanodeReportType.LIVE;
    boolean listDeadNodes = type == DatanodeReportType.ALL ||
                            type == DatanodeReportType.DEAD;

    HashMap<String, String> mustList = new HashMap<String, String>();

    if (listDeadNodes) {
      //first load all the nodes listed in include and exclude files.
      Iterator<String> it = hostsReader.getHosts().iterator();
      while (it.hasNext()) {
        mustList.put(it.next(), "");
      }
      it = hostsReader.getExcludedHosts().iterator(); 
      while (it.hasNext()) {
        mustList.put(it.next(), "");
      }
    }

    ArrayList<DatanodeDescriptor> nodes = null;
    
    synchronized (namesystem.datanodeMap) {
      nodes = new ArrayList<DatanodeDescriptor>(namesystem.datanodeMap.size() + 
                                                mustList.size());
      Iterator<DatanodeDescriptor> it = namesystem.datanodeMap.values().iterator();
      while (it.hasNext()) { 
        DatanodeDescriptor dn = it.next();
        boolean isDead = namesystem.isDatanodeDead(dn);
        if ( (isDead && listDeadNodes) || (!isDead && listLiveNodes) ) {
          nodes.add(dn);
        }
        //Remove any form of the this datanode in include/exclude lists.
        try {
          InetAddress inet = InetAddress.getByName(dn.getHost());
          // compare hostname(:port)
          mustList.remove(inet.getHostName());
          mustList.remove(inet.getHostName()+":"+dn.getPort());
          // compare ipaddress(:port)
          mustList.remove(inet.getHostAddress().toString());
          mustList.remove(inet.getHostAddress().toString()+ ":" +dn.getPort());
        } catch ( UnknownHostException e ) {
          mustList.remove(dn.getName());
          mustList.remove(dn.getHost());
          LOG.warn(e);
        }
      }
    }
    
    if (listDeadNodes) {
      Iterator<String> it = mustList.keySet().iterator();
      while (it.hasNext()) {
        DatanodeDescriptor dn = 
            new DatanodeDescriptor(new DatanodeID(it.next()));
        dn.setLastUpdate(0);
        nodes.add(dn);
      }
    }
    return nodes;
  }
}
