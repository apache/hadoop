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

import static org.apache.hadoop.hdfs.server.common.Util.now;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.FSConstants.DatanodeReportType;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.UnregisteredNodeException;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeDescriptor.BlockTargetPair;
import org.apache.hadoop.hdfs.server.common.Util;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.protocol.BalancerBandwidthCommand;
import org.apache.hadoop.hdfs.server.protocol.BlockCommand;
import org.apache.hadoop.hdfs.server.protocol.BlockRecoveryCommand;
import org.apache.hadoop.hdfs.server.protocol.BlockRecoveryCommand.RecoveringBlock;
import org.apache.hadoop.hdfs.server.protocol.DatanodeCommand;
import org.apache.hadoop.hdfs.server.protocol.DatanodeProtocol;
import org.apache.hadoop.hdfs.server.protocol.DatanodeRegistration;
import org.apache.hadoop.hdfs.server.protocol.DisallowedDatanodeException;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.net.CachedDNSToSwitchMapping;
import org.apache.hadoop.net.DNSToSwitchMapping;
import org.apache.hadoop.net.NetworkTopology;
import org.apache.hadoop.net.ScriptBasedMapping;
import org.apache.hadoop.util.CyclicIteration;
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

  private final FSNamesystem namesystem;
  private final BlockManager blockManager;

  private final HeartbeatManager heartbeatManager;

  /**
   * Stores the datanode -> block map.  
   * <p>
   * Done by storing a set of {@link DatanodeDescriptor} objects, sorted by 
   * storage id. In order to keep the storage map consistent it tracks 
   * all storages ever registered with the namenode.
   * A descriptor corresponding to a specific storage id can be
   * <ul> 
   * <li>added to the map if it is a new storage id;</li>
   * <li>updated with a new datanode started as a replacement for the old one 
   * with the same storage id; and </li>
   * <li>removed if and only if an existing datanode is restarted to serve a
   * different storage id.</li>
   * </ul> <br>
   * The list of the {@link DatanodeDescriptor}s in the map is checkpointed
   * in the namespace image file. Only the {@link DatanodeInfo} part is 
   * persistent, the list of blocks is restored from the datanode block
   * reports. 
   * <p>
   * Mapping: StorageID -> DatanodeDescriptor
   */
  private final NavigableMap<String, DatanodeDescriptor> datanodeMap
      = new TreeMap<String, DatanodeDescriptor>();

  /** Cluster network topology */
  private final NetworkTopology networktopology = new NetworkTopology();

  /** Host names to datanode descriptors mapping. */
  private final Host2NodesMap host2DatanodeMap = new Host2NodesMap();

  private final DNSToSwitchMapping dnsToSwitchMapping;

  /** Read include/exclude files*/
  private final HostsFileReader hostsReader;

  /** The period to wait for datanode heartbeat.*/
  private final long heartbeatExpireInterval;
  /** Ask Datanode only up to this many blocks to delete. */
  final int blockInvalidateLimit;
  
  DatanodeManager(final BlockManager blockManager,
      final FSNamesystem namesystem, final Configuration conf
      ) throws IOException {
    this.namesystem = namesystem;
    this.blockManager = blockManager;

    this.heartbeatManager = new HeartbeatManager(namesystem, conf);

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
    
    final long heartbeatIntervalSeconds = conf.getLong(
        DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_KEY,
        DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_DEFAULT);
    final int heartbeatRecheckInterval = conf.getInt(
        DFSConfigKeys.DFS_NAMENODE_HEARTBEAT_RECHECK_INTERVAL_KEY, 
        DFSConfigKeys.DFS_NAMENODE_HEARTBEAT_RECHECK_INTERVAL_DEFAULT); // 5 minutes
    this.heartbeatExpireInterval = 2 * heartbeatRecheckInterval
        + 10 * 1000 * heartbeatIntervalSeconds;
    this.blockInvalidateLimit = Math.max(20*(int)(heartbeatIntervalSeconds),
        DFSConfigKeys.DFS_BLOCK_INVALIDATE_LIMIT_DEFAULT);
    LOG.info(DFSConfigKeys.DFS_BLOCK_INVALIDATE_LIMIT_KEY
        + "=" + this.blockInvalidateLimit);
  }

  private Daemon decommissionthread = null;

  void activate(final Configuration conf) {
    this.decommissionthread = new Daemon(new DecommissionManager(namesystem).new Monitor(
        conf.getInt(DFSConfigKeys.DFS_NAMENODE_DECOMMISSION_INTERVAL_KEY, 
                    DFSConfigKeys.DFS_NAMENODE_DECOMMISSION_INTERVAL_DEFAULT),
        conf.getInt(DFSConfigKeys.DFS_NAMENODE_DECOMMISSION_NODES_PER_INTERVAL_KEY, 
                    DFSConfigKeys.DFS_NAMENODE_DECOMMISSION_NODES_PER_INTERVAL_DEFAULT)));
    decommissionthread.start();

    heartbeatManager.activate(conf);
  }

  void close() {
    if (decommissionthread != null) decommissionthread.interrupt();
    heartbeatManager.close();
  }

  /** @return the network topology. */
  public NetworkTopology getNetworkTopology() {
    return networktopology;
  }

  /** @return the heartbeat manager. */
  HeartbeatManager getHeartbeatManager() {
    return heartbeatManager;
  }

  /** @return the datanode statistics. */
  public DatanodeStatistics getDatanodeStatistics() {
    return heartbeatManager;
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

  CyclicIteration<String, DatanodeDescriptor> getDatanodeCyclicIteration(
      final String firstkey) {
    return new CyclicIteration<String, DatanodeDescriptor>(
        datanodeMap, firstkey);
  }

  /** @return the datanode descriptor for the host. */
  public DatanodeDescriptor getDatanodeByHost(final String host) {
    return host2DatanodeMap.getDatanodeByHost(host);
  }

  /** Get a datanode descriptor given corresponding storageID */
  DatanodeDescriptor getDatanode(final String storageID) {
    return datanodeMap.get(storageID);
  }

  /**
   * Get data node by storage ID.
   * 
   * @param nodeID
   * @return DatanodeDescriptor or null if the node is not found.
   * @throws UnregisteredNodeException
   */
  public DatanodeDescriptor getDatanode(DatanodeID nodeID
      ) throws UnregisteredNodeException {
    final DatanodeDescriptor node = getDatanode(nodeID.getStorageID());
    if (node == null) 
      return null;
    if (!node.getName().equals(nodeID.getName())) {
      final UnregisteredNodeException e = new UnregisteredNodeException(
          nodeID, node);
      NameNode.stateChangeLog.fatal("BLOCK* NameSystem.getDatanode: "
                                    + e.getLocalizedMessage());
      throw e;
    }
    return node;
  }

  /** Prints information about all datanodes. */
  void datanodeDump(final PrintWriter out) {
    synchronized (datanodeMap) {
      out.println("Metasave: Number of datanodes: " + datanodeMap.size());
      for(Iterator<DatanodeDescriptor> it = datanodeMap.values().iterator(); it.hasNext();) {
        DatanodeDescriptor node = it.next();
        out.println(node.dumpDatanode());
      }
    }
  }

  /**
   * Remove a datanode descriptor.
   * @param nodeInfo datanode descriptor.
   */
  private void removeDatanode(DatanodeDescriptor nodeInfo) {
    assert namesystem.hasWriteLock();
    heartbeatManager.removeDatanode(nodeInfo);
    blockManager.removeBlocksAssociatedTo(nodeInfo);
    networktopology.remove(nodeInfo);

    if (LOG.isDebugEnabled()) {
      LOG.debug("remove datanode " + nodeInfo.getName());
    }
    namesystem.checkSafeMode();
  }

  /**
   * Remove a datanode
   * @throws UnregisteredNodeException 
   */
  public void removeDatanode(final DatanodeID node
      ) throws UnregisteredNodeException {
    namesystem.writeLock();
    try {
      final DatanodeDescriptor descriptor = getDatanode(node);
      if (descriptor != null) {
        removeDatanode(descriptor);
      } else {
        NameNode.stateChangeLog.warn("BLOCK* removeDatanode: "
                                     + node.getName() + " does not exist");
      }
    } finally {
      namesystem.writeUnlock();
    }
  }

  /** Remove a dead datanode. */
  void removeDeadDatanode(final DatanodeID nodeID) {
      synchronized(datanodeMap) {
        DatanodeDescriptor d;
        try {
          d = getDatanode(nodeID);
        } catch(IOException e) {
          d = null;
        }
        if (d != null && isDatanodeDead(d)) {
          NameNode.stateChangeLog.info(
              "BLOCK* removeDeadDatanode: lost heartbeat from " + d.getName());
          removeDatanode(d);
        }
      }
  }

  /** Is the datanode dead? */
  boolean isDatanodeDead(DatanodeDescriptor node) {
    return (node.getLastUpdate() <
            (Util.now() - heartbeatExpireInterval));
  }

  /** Add a datanode. */
  private void addDatanode(final DatanodeDescriptor node) {
    // To keep host2DatanodeMap consistent with datanodeMap,
    // remove  from host2DatanodeMap the datanodeDescriptor removed
    // from datanodeMap before adding node to host2DatanodeMap.
    synchronized(datanodeMap) {
      host2DatanodeMap.remove(datanodeMap.put(node.getStorageID(), node));
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
    synchronized (datanodeMap) {
      host2DatanodeMap.remove(datanodeMap.remove(key));
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
      startDecommission(nodeReg);
    }
  }

  /**
   * Change, if appropriate, the admin state of a datanode to 
   * decommission completed. Return true if decommission is complete.
   */
  boolean checkDecommissionState(DatanodeDescriptor node) {
    // Check to see if all blocks in this decommissioned
    // node has reached their target replication factor.
    if (node.isDecommissionInProgress()) {
      if (!blockManager.isReplicationInProgress(node)) {
        node.setDecommissioned();
        LOG.info("Decommission complete for node " + node.getName());
      }
    }
    return node.isDecommissioned();
  }

  /** Start decommissioning the specified datanode. */
  private void startDecommission(DatanodeDescriptor node) throws IOException {
    if (!node.isDecommissionInProgress() && !node.isDecommissioned()) {
      LOG.info("Start Decommissioning node " + node.getName() + " with " + 
          node.numBlocks() +  " blocks.");
      heartbeatManager.startDecommission(node);
      node.decommissioningStatus.setStartTime(now());
      
      // all the blocks that reside on this node have to be replicated.
      checkDecommissionState(node);
    }
  }

  /** Stop decommissioning the specified datanodes. */
  void stopDecommission(DatanodeDescriptor node) throws IOException {
    if (node.isDecommissionInProgress() || node.isDecommissioned()) {
      LOG.info("Stop Decommissioning node " + node.getName());
      heartbeatManager.stopDecommission(node);
      blockManager.processOverReplicatedBlocksOnReCommission(node);
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
      if (datanodeMap.get(newID) != null)
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
    nodeReg.exportedKeys = namesystem.getBlockManager().getBlockKeys();
      
    NameNode.stateChangeLog.info("BLOCK* NameSystem.registerDatanode: "
        + "node registration from " + nodeReg.getName()
        + " storage " + nodeReg.getStorageID());

    DatanodeDescriptor nodeS = datanodeMap.get(nodeReg.getStorageID());
    DatanodeDescriptor nodeN = getDatanodeByHost(nodeReg.getName());
      
    if (nodeN != null && nodeN != nodeS) {
      NameNode.LOG.info("BLOCK* NameSystem.registerDatanode: "
                        + "node from name: " + nodeN.getName());
      // nodeN previously served a different data storage, 
      // which is not served by anybody anymore.
      removeDatanode(nodeN);
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
      heartbeatManager.register(nodeS);
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
    // no need to update its timestamp
    // because its is done when the descriptor is created
    heartbeatManager.addDatanode(nodeDescr);
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
    for(DatanodeDescriptor node : datanodeMap.values()) {
      // Check if not include.
      if (!inHostsList(node, null)) {
        node.setDisallowed(true); // case 2.
      } else {
        if (inExcludedHostsList(node, null)) {
          startDecommission(node); // case 3.
        } else {
          stopDecommission(node); // case 4.
        }
      }
    }
  }

  /** @return the number of live datanodes. */
  public int getNumLiveDataNodes() {
    int numLive = 0;
    synchronized (datanodeMap) {
      for(DatanodeDescriptor dn : datanodeMap.values()) {
        if (!isDatanodeDead(dn) ) {
          numLive++;
        }
      }
    }
    return numLive;
  }

  /** @return the number of dead datanodes. */
  public int getNumDeadDataNodes() {
    int numDead = 0;
    synchronized (datanodeMap) {   
      for(DatanodeDescriptor dn : datanodeMap.values()) {
        if (isDatanodeDead(dn) ) {
          numDead++;
        }
      }
    }
    return numDead;
  }

  /** Fetch live and dead datanodes. */
  public void fetchDatanodess(final List<DatanodeDescriptor> live, 
      final List<DatanodeDescriptor> dead) {
    final List<DatanodeDescriptor> results =
        getDatanodeListForReport(DatanodeReportType.ALL);    
    for(DatanodeDescriptor node : results) {
      if (isDatanodeDead(node))
        dead.add(node);
      else
        live.add(node);
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
    
    synchronized(datanodeMap) {
      nodes = new ArrayList<DatanodeDescriptor>(datanodeMap.size() + 
                                                mustList.size());
      Iterator<DatanodeDescriptor> it = datanodeMap.values().iterator();
      while (it.hasNext()) { 
        DatanodeDescriptor dn = it.next();
        final boolean isDead = isDatanodeDead(dn);
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
  
  private void setDatanodeDead(DatanodeDescriptor node) throws IOException {
    node.setLastUpdate(0);
  }

  /** Handle heartbeat from datanodes. */
  public DatanodeCommand[] handleHeartbeat(DatanodeRegistration nodeReg,
      final String blockPoolId,
      long capacity, long dfsUsed, long remaining, long blockPoolUsed,
      int xceiverCount, int maxTransfers, int failedVolumes
      ) throws IOException {
    synchronized (heartbeatManager) {
      synchronized (datanodeMap) {
        DatanodeDescriptor nodeinfo = null;
        try {
          nodeinfo = getDatanode(nodeReg);
        } catch(UnregisteredNodeException e) {
          return new DatanodeCommand[]{DatanodeCommand.REGISTER};
        }
        
        // Check if this datanode should actually be shutdown instead. 
        if (nodeinfo != null && nodeinfo.isDisallowed()) {
          setDatanodeDead(nodeinfo);
          throw new DisallowedDatanodeException(nodeinfo);
        }
         
        if (nodeinfo == null || !nodeinfo.isAlive) {
          return new DatanodeCommand[]{DatanodeCommand.REGISTER};
        }

        heartbeatManager.updateHeartbeat(nodeinfo, capacity, dfsUsed,
            remaining, blockPoolUsed, xceiverCount, failedVolumes);
        
        //check lease recovery
        BlockInfoUnderConstruction[] blocks = nodeinfo
            .getLeaseRecoveryCommand(Integer.MAX_VALUE);
        if (blocks != null) {
          BlockRecoveryCommand brCommand = new BlockRecoveryCommand(
              blocks.length);
          for (BlockInfoUnderConstruction b : blocks) {
            brCommand.add(new RecoveringBlock(
                new ExtendedBlock(blockPoolId, b), b.getExpectedLocations(), b
                    .getBlockRecoveryId()));
          }
          return new DatanodeCommand[] { brCommand };
        }

        final List<DatanodeCommand> cmds = new ArrayList<DatanodeCommand>();
        //check pending replication
        List<BlockTargetPair> pendingList = nodeinfo.getReplicationCommand(
              maxTransfers);
        if (pendingList != null) {
          cmds.add(new BlockCommand(DatanodeProtocol.DNA_TRANSFER, blockPoolId,
              pendingList));
        }
        //check block invalidation
        Block[] blks = nodeinfo.getInvalidateBlocks(blockInvalidateLimit);
        if (blks != null) {
          cmds.add(new BlockCommand(DatanodeProtocol.DNA_INVALIDATE,
              blockPoolId, blks));
        }
        
        namesystem.addKeyUpdateCommand(cmds, nodeinfo);

        // check for balancer bandwidth update
        if (nodeinfo.getBalancerBandwidth() > 0) {
          cmds.add(new BalancerBandwidthCommand(nodeinfo.getBalancerBandwidth()));
          // set back to 0 to indicate that datanode has been sent the new value
          nodeinfo.setBalancerBandwidth(0);
        }

        if (!cmds.isEmpty()) {
          return cmds.toArray(new DatanodeCommand[cmds.size()]);
        }
      }
    }

    return null;
  }

  /**
   * Tell all datanodes to use a new, non-persistent bandwidth value for
   * dfs.balance.bandwidthPerSec.
   *
   * A system administrator can tune the balancer bandwidth parameter
   * (dfs.datanode.balance.bandwidthPerSec) dynamically by calling
   * "dfsadmin -setBalanacerBandwidth newbandwidth", at which point the
   * following 'bandwidth' variable gets updated with the new value for each
   * node. Once the heartbeat command is issued to update the value on the
   * specified datanode, this value will be set back to 0.
   *
   * @param bandwidth Blanacer bandwidth in bytes per second for all datanodes.
   * @throws IOException
   */
  public void setBalancerBandwidth(long bandwidth) throws IOException {
    synchronized(datanodeMap) {
      for (DatanodeDescriptor nodeInfo : datanodeMap.values()) {
        nodeInfo.setBalancerBandwidth(bandwidth);
      }
    }
  }
}
