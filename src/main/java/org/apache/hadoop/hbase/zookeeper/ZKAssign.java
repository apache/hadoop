/**
 * Copyright 2010 The Apache Software Foundation
 *
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
package org.apache.hadoop.hbase.zookeeper;

import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.executor.RegionTransitionData;
import org.apache.hadoop.hbase.executor.EventHandler.EventType;
import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.KeeperException.NodeExistsException;
import org.apache.zookeeper.data.Stat;

/**
 * Utility class for doing region assignment in ZooKeeper.  This class extends
 * stuff done in {@link ZKUtil} to cover specific assignment operations.
 * <p>
 * Contains only static methods and constants.
 * <p>
 * Used by both the Master and RegionServer.
 * <p>
 * All valid transitions outlined below:
 * <p>
 * <b>MASTER</b>
 * <ol>
 *   <li>
 *     Master creates an unassigned node as OFFLINE.
 *     - Cluster startup and table enabling.
 *   </li>
 *   <li>
 *     Master forces an existing unassigned node to OFFLINE.
 *     - RegionServer failure.
 *     - Allows transitions from all states to OFFLINE.
 *   </li>
 *   <li>
 *     Master deletes an unassigned node that was in a OPENED state.
 *     - Normal region transitions.  Besides cluster startup, no other deletions
 *     of unassigned nodes is allowed.
 *   </li>
 *   <li>
 *     Master deletes all unassigned nodes regardless of state.
 *     - Cluster startup before any assignment happens.
 *   </li>
 * </ol>
 * <p>
 * <b>REGIONSERVER</b>
 * <ol>
 *   <li>
 *     RegionServer creates an unassigned node as CLOSING.
 *     - All region closes will do this in response to a CLOSE RPC from Master.
 *     - A node can never be transitioned to CLOSING, only created.
 *   </li>
 *   <li>
 *     RegionServer transitions an unassigned node from CLOSING to CLOSED.
 *     - Normal region closes.  CAS operation.
 *   </li>
 *   <li>
 *     RegionServer transitions an unassigned node from OFFLINE to OPENING.
 *     - All region opens will do this in response to an OPEN RPC from the Master.
 *     - Normal region opens.  CAS operation.
 *   </li>
 *   <li>
 *     RegionServer transitions an unassigned node from OPENING to OPENED.
 *     - Normal region opens.  CAS operation.
 *   </li>
 * </ol>
 */
public class ZKAssign {
  private static final Log LOG = LogFactory.getLog(ZKAssign.class);

  /**
   * Gets the full path node name for the unassigned node for the specified
   * region.
   * @param zkw zk reference
   * @param regionName region name
   * @return full path node name
   */
  public static String getNodeName(ZooKeeperWatcher zkw, String regionName) {
    return ZKUtil.joinZNode(zkw.assignmentZNode, regionName);
  }

  /**
   * Gets the region name from the full path node name of an unassigned node.
   * @param path full zk path
   * @return region name
   */
  public static String getRegionName(ZooKeeperWatcher zkw, String path) {
    return path.substring(zkw.assignmentZNode.length()+1);
  }

  // Master methods

  /**
   * Creates a new unassigned node in the OFFLINE state for the specified region.
   *
   * <p>Does not transition nodes from other states.  If a node already exists
   * for this region, a {@link NodeExistsException} will be thrown.
   *
   * <p>Sets a watcher on the unassigned region node if the method is successful.
   *
   * <p>This method should only be used during cluster startup and the enabling
   * of a table.
   *
   * @param zkw zk reference
   * @param region region to be created as offline
   * @param serverName server event originates from
   * @throws KeeperException if unexpected zookeeper exception
   * @throws KeeperException.NodeExistsException if node already exists
   */
  public static void createNodeOffline(ZooKeeperWatcher zkw, HRegionInfo region,
      String serverName)
  throws KeeperException, KeeperException.NodeExistsException {
    createNodeOffline(zkw, region, serverName, EventType.M_ZK_REGION_OFFLINE);
  }

  public static void createNodeOffline(ZooKeeperWatcher zkw, HRegionInfo region,
      String serverName, final EventType event)
  throws KeeperException, KeeperException.NodeExistsException {
    LOG.debug(zkw.prefix("Creating unassigned node for " +
      region.getEncodedName() + " in OFFLINE state"));
    RegionTransitionData data = new RegionTransitionData(event,
      region.getRegionName(), serverName);
    synchronized(zkw.getNodes()) {
      String node = getNodeName(zkw, region.getEncodedName());
      zkw.getNodes().add(node);
      ZKUtil.createAndWatch(zkw, node, data.getBytes());
    }
  }

  /**
   * Creates an unassigned node in the OFFLINE state for the specified region.
   * <p>
   * Runs asynchronously.  Depends on no pre-existing znode.
   *
   * <p>Sets a watcher on the unassigned region node.
   *
   * @param zkw zk reference
   * @param region region to be created as offline
   * @param serverName server event originates from
   * @param cb
   * @param ctx
   * @throws KeeperException if unexpected zookeeper exception
   * @throws KeeperException.NodeExistsException if node already exists
   */
  public static void asyncCreateNodeOffline(ZooKeeperWatcher zkw,
      HRegionInfo region, String serverName,
      final AsyncCallback.StringCallback cb, final Object ctx)
  throws KeeperException {
    LOG.debug(zkw.prefix("Async create of unassigned node for " +
      region.getEncodedName() + " with OFFLINE state"));
    RegionTransitionData data = new RegionTransitionData(
        EventType.M_ZK_REGION_OFFLINE, region.getRegionName(), serverName);
    synchronized(zkw.getNodes()) {
      String node = getNodeName(zkw, region.getEncodedName());
      zkw.getNodes().add(node);
      ZKUtil.asyncCreate(zkw, node, data.getBytes(), cb, ctx);
    }
  }

  /**
   * Forces an existing unassigned node to the OFFLINE state for the specified
   * region.
   *
   * <p>Does not create a new node.  If a node does not already exist for this
   * region, a {@link NoNodeException} will be thrown.
   *
   * <p>Sets a watcher on the unassigned region node if the method is
   * successful.
   *
   * <p>This method should only be used during recovery of regionserver failure.
   *
   * @param zkw zk reference
   * @param region region to be forced as offline
   * @param serverName server event originates from
   * @throws KeeperException if unexpected zookeeper exception
   * @throws KeeperException.NoNodeException if node does not exist
   */
  public static void forceNodeOffline(ZooKeeperWatcher zkw, HRegionInfo region,
      String serverName)
  throws KeeperException, KeeperException.NoNodeException {
    LOG.debug(zkw.prefix("Forcing existing unassigned node for " +
      region.getEncodedName() + " to OFFLINE state"));
    RegionTransitionData data = new RegionTransitionData(
        EventType.M_ZK_REGION_OFFLINE, region.getRegionName(), serverName);
    synchronized(zkw.getNodes()) {
      String node = getNodeName(zkw, region.getEncodedName());
      zkw.getNodes().add(node);
      ZKUtil.setData(zkw, node, data.getBytes());
    }
  }


  /**
   * Creates or force updates an unassigned node to the OFFLINE state for the
   * specified region.
   * <p>
   * Attempts to create the node but if it exists will force it to transition to
   * and OFFLINE state.
   *
   * <p>Sets a watcher on the unassigned region node if the method is
   * successful.
   *
   * <p>This method should be used when assigning a region.
   *
   * @param zkw zk reference
   * @param region region to be created as offline
   * @param serverName server event originates from
   * @throws KeeperException if unexpected zookeeper exception
   * @throws KeeperException.NodeExistsException if node already exists
   */
  public static boolean createOrForceNodeOffline(ZooKeeperWatcher zkw,
      HRegionInfo region, String serverName)
  throws KeeperException {
    LOG.debug(zkw.prefix("Creating (or updating) unassigned node for " +
      region.getEncodedName() + " with OFFLINE state"));
    RegionTransitionData data = new RegionTransitionData(
        EventType.M_ZK_REGION_OFFLINE, region.getRegionName(), serverName);
    synchronized(zkw.getNodes()) {
      String node = getNodeName(zkw, region.getEncodedName());
      zkw.sync(node);
      zkw.getNodes().add(node);
      int version = ZKUtil.checkExists(zkw, node);
      if(version == -1) {
        ZKUtil.createAndWatch(zkw, node, data.getBytes());
      } else {
        if (!ZKUtil.setData(zkw, node, data.getBytes(), version)) {
          return false;
        } else {
          // We successfully forced to OFFLINE, reset watch and handle if
          // the state changed in between our set and the watch
          RegionTransitionData curData =
            ZKAssign.getData(zkw, region.getEncodedName());
          if (curData.getEventType() != data.getEventType()) {
            // state changed, need to process
            return false;
          }
        }
      }
    }
    return true;
  }

  /**
   * Deletes an existing unassigned node that is in the OPENED state for the
   * specified region.
   *
   * <p>If a node does not already exist for this region, a
   * {@link NoNodeException} will be thrown.
   *
   * <p>No watcher is set whether this succeeds or not.
   *
   * <p>Returns false if the node was not in the proper state but did exist.
   *
   * <p>This method is used during normal region transitions when a region
   * finishes successfully opening.  This is the Master acknowledging completion
   * of the specified regions transition.
   *
   * @param zkw zk reference
   * @param regionName opened region to be deleted from zk
   * @throws KeeperException if unexpected zookeeper exception
   * @throws KeeperException.NoNodeException if node does not exist
   */
  public static boolean deleteOpenedNode(ZooKeeperWatcher zkw,
      String regionName)
  throws KeeperException, KeeperException.NoNodeException {
    return deleteNode(zkw, regionName, EventType.RS_ZK_REGION_OPENED);
  }

  /**
   * Deletes an existing unassigned node that is in the OFFLINE state for the
   * specified region.
   *
   * <p>If a node does not already exist for this region, a
   * {@link NoNodeException} will be thrown.
   *
   * <p>No watcher is set whether this succeeds or not.
   *
   * <p>Returns false if the node was not in the proper state but did exist.
   *
   * <p>This method is used during master failover when the regions on an RS
   * that has died are all set to OFFLINE before being processed.
   *
   * @param zkw zk reference
   * @param regionName closed region to be deleted from zk
   * @throws KeeperException if unexpected zookeeper exception
   * @throws KeeperException.NoNodeException if node does not exist
   */
  public static boolean deleteOfflineNode(ZooKeeperWatcher zkw,
      String regionName)
  throws KeeperException, KeeperException.NoNodeException {
    return deleteNode(zkw, regionName, EventType.M_ZK_REGION_OFFLINE);
  }

  /**
   * Deletes an existing unassigned node that is in the CLOSED state for the
   * specified region.
   *
   * <p>If a node does not already exist for this region, a
   * {@link NoNodeException} will be thrown.
   *
   * <p>No watcher is set whether this succeeds or not.
   *
   * <p>Returns false if the node was not in the proper state but did exist.
   *
   * <p>This method is used during table disables when a region finishes
   * successfully closing.  This is the Master acknowledging completion
   * of the specified regions transition to being closed.
   *
   * @param zkw zk reference
   * @param regionName closed region to be deleted from zk
   * @throws KeeperException if unexpected zookeeper exception
   * @throws KeeperException.NoNodeException if node does not exist
   */
  public static boolean deleteClosedNode(ZooKeeperWatcher zkw,
      String regionName)
  throws KeeperException, KeeperException.NoNodeException {
    return deleteNode(zkw, regionName, EventType.RS_ZK_REGION_CLOSED);
  }

  /**
   * Deletes an existing unassigned node that is in the CLOSING state for the
   * specified region.
   *
   * <p>If a node does not already exist for this region, a
   * {@link NoNodeException} will be thrown.
   *
   * <p>No watcher is set whether this succeeds or not.
   *
   * <p>Returns false if the node was not in the proper state but did exist.
   *
   * <p>This method is used during table disables when a region finishes
   * successfully closing.  This is the Master acknowledging completion
   * of the specified regions transition to being closed.
   *
   * @param zkw zk reference
   * @param region closing region to be deleted from zk
   * @throws KeeperException if unexpected zookeeper exception
   * @throws KeeperException.NoNodeException if node does not exist
   */
  public static boolean deleteClosingNode(ZooKeeperWatcher zkw,
      HRegionInfo region)
  throws KeeperException, KeeperException.NoNodeException {
    String regionName = region.getEncodedName();
    return deleteNode(zkw, regionName, EventType.RS_ZK_REGION_CLOSING);
  }

  /**
   * Deletes an existing unassigned node that is in the specified state for the
   * specified region.
   *
   * <p>If a node does not already exist for this region, a
   * {@link NoNodeException} will be thrown.
   *
   * <p>No watcher is set whether this succeeds or not.
   *
   * <p>Returns false if the node was not in the proper state but did exist.
   *
   * <p>This method is used during table disables when a region finishes
   * successfully closing.  This is the Master acknowledging completion
   * of the specified regions transition to being closed.
   *
   * @param zkw zk reference
   * @param regionName region to be deleted from zk
   * @param expectedState state region must be in for delete to complete
   * @throws KeeperException if unexpected zookeeper exception
   * @throws KeeperException.NoNodeException if node does not exist
   */
  public static boolean deleteNode(ZooKeeperWatcher zkw, String regionName,
      EventType expectedState)
  throws KeeperException, KeeperException.NoNodeException {
    LOG.debug(zkw.prefix("Deleting existing unassigned " +
      "node for " + regionName + " that is in expected state " + expectedState));
    String node = getNodeName(zkw, regionName);
    zkw.sync(node);
    Stat stat = new Stat();
    byte [] bytes = ZKUtil.getDataNoWatch(zkw, node, stat);
    if(bytes == null) {
      throw KeeperException.create(Code.NONODE);
    }
    RegionTransitionData data = RegionTransitionData.fromBytes(bytes);
    if(!data.getEventType().equals(expectedState)) {
      LOG.warn(zkw.prefix("Attempting to delete unassigned " +
        "node in " + expectedState +
        " state but node is in " + data.getEventType() + " state"));
      return false;
    }
    synchronized(zkw.getNodes()) {
      // TODO: Does this go here or only if we successfully delete node?
      zkw.getNodes().remove(node);
      if(!ZKUtil.deleteNode(zkw, node, stat.getVersion())) {
        LOG.warn(zkw.prefix("Attempting to delete " +
          "unassigned node in " + expectedState +
            " state but after verifying state, we got a version mismatch"));
        return false;
      }
      LOG.debug(zkw.prefix("Successfully deleted unassigned node for region " +
          regionName + " in expected state " + expectedState));
      return true;
    }
  }

  /**
   * Deletes all unassigned nodes regardless of their state.
   *
   * <p>No watchers are set.
   *
   * <p>This method is used by the Master during cluster startup to clear out
   * any existing state from other cluster runs.
   *
   * @param zkw zk reference
   * @throws KeeperException if unexpected zookeeper exception
   */
  public static void deleteAllNodes(ZooKeeperWatcher zkw)
  throws KeeperException {
    LOG.debug(zkw.prefix("Deleting any existing unassigned nodes"));
    ZKUtil.deleteChildrenRecursively(zkw, zkw.assignmentZNode);
  }

  // RegionServer methods

  /**
   * Creates a new unassigned node in the CLOSING state for the specified
   * region.
   *
   * <p>Does not transition nodes from any states.  If a node already exists
   * for this region, a {@link NodeExistsException} will be thrown.
   *
   * <p>If creation is successful, returns the version number of the CLOSING
   * node created.
   *
   * <p>Does not set any watches.
   *
   * <p>This method should only be used by a RegionServer when initiating a
   * close of a region after receiving a CLOSE RPC from the Master.
   *
   * @param zkw zk reference
   * @param region region to be created as closing
   * @param serverName server event originates from
   * @return version of node after transition, -1 if unsuccessful transition
   * @throws KeeperException if unexpected zookeeper exception
   * @throws KeeperException.NodeExistsException if node already exists
   */
  public static int createNodeClosing(ZooKeeperWatcher zkw, HRegionInfo region,
      String serverName)
  throws KeeperException, KeeperException.NodeExistsException {
    LOG.debug(zkw.prefix("Creating unassigned node for " +
      region.getEncodedName() + " in a CLOSING state"));

    RegionTransitionData data = new RegionTransitionData(
        EventType.RS_ZK_REGION_CLOSING, region.getRegionName(), serverName);

    synchronized (zkw.getNodes()) {
      String node = getNodeName(zkw, region.getEncodedName());
      zkw.getNodes().add(node);
      return ZKUtil.createAndWatch(zkw, node, data.getBytes());
    }
  }

  /**
   * Transitions an existing unassigned node for the specified region which is
   * currently in the CLOSING state to be in the CLOSED state.
   *
   * <p>Does not transition nodes from other states.  If for some reason the
   * node could not be transitioned, the method returns -1.  If the transition
   * is successful, the version of the node after transition is returned.
   *
   * <p>This method can fail and return false for three different reasons:
   * <ul><li>Unassigned node for this region does not exist</li>
   * <li>Unassigned node for this region is not in CLOSING state</li>
   * <li>After verifying CLOSING state, update fails because of wrong version
   * (someone else already transitioned the node)</li>
   * </ul>
   *
   * <p>Does not set any watches.
   *
   * <p>This method should only be used by a RegionServer when initiating a
   * close of a region after receiving a CLOSE RPC from the Master.
   *
   * @param zkw zk reference
   * @param region region to be transitioned to closed
   * @param serverName server event originates from
   * @return version of node after transition, -1 if unsuccessful transition
   * @throws KeeperException if unexpected zookeeper exception
   */
  public static int transitionNodeClosed(ZooKeeperWatcher zkw,
      HRegionInfo region, String serverName, int expectedVersion)
  throws KeeperException {
    return transitionNode(zkw, region, serverName,
        EventType.RS_ZK_REGION_CLOSING,
        EventType.RS_ZK_REGION_CLOSED, expectedVersion);
  }

  /**
   * Transitions an existing unassigned node for the specified region which is
   * currently in the OFFLINE state to be in the OPENING state.
   *
   * <p>Does not transition nodes from other states.  If for some reason the
   * node could not be transitioned, the method returns -1.  If the transition
   * is successful, the version of the node written as OPENING is returned.
   *
   * <p>This method can fail and return -1 for three different reasons:
   * <ul><li>Unassigned node for this region does not exist</li>
   * <li>Unassigned node for this region is not in OFFLINE state</li>
   * <li>After verifying OFFLINE state, update fails because of wrong version
   * (someone else already transitioned the node)</li>
   * </ul>
   *
   * <p>Does not set any watches.
   *
   * <p>This method should only be used by a RegionServer when initiating an
   * open of a region after receiving an OPEN RPC from the Master.
   *
   * @param zkw zk reference
   * @param region region to be transitioned to opening
   * @param serverName server event originates from
   * @return version of node after transition, -1 if unsuccessful transition
   * @throws KeeperException if unexpected zookeeper exception
   */
  public static int transitionNodeOpening(ZooKeeperWatcher zkw,
      HRegionInfo region, String serverName)
  throws KeeperException {
    return transitionNodeOpening(zkw, region, serverName,
      EventType.M_ZK_REGION_OFFLINE);
  }

  public static int transitionNodeOpening(ZooKeeperWatcher zkw,
      HRegionInfo region, String serverName, final EventType beginState)
  throws KeeperException {
    return transitionNode(zkw, region, serverName, beginState,
      EventType.RS_ZK_REGION_OPENING, -1);
  }

  /**
   * Retransitions an existing unassigned node for the specified region which is
   * currently in the OPENING state to be in the OPENING state.
   *
   * <p>Does not transition nodes from other states.  If for some reason the
   * node could not be transitioned, the method returns -1.  If the transition
   * is successful, the version of the node rewritten as OPENING is returned.
   *
   * <p>This method can fail and return -1 for three different reasons:
   * <ul><li>Unassigned node for this region does not exist</li>
   * <li>Unassigned node for this region is not in OPENING state</li>
   * <li>After verifying OPENING state, update fails because of wrong version
   * (someone else already transitioned the node)</li>
   * </ul>
   *
   * <p>Does not set any watches.
   *
   * <p>This method should only be used by a RegionServer when initiating an
   * open of a region after receiving an OPEN RPC from the Master.
   *
   * @param zkw zk reference
   * @param region region to be transitioned to opening
   * @param serverName server event originates from
   * @return version of node after transition, -1 if unsuccessful transition
   * @throws KeeperException if unexpected zookeeper exception
   */
  public static int retransitionNodeOpening(ZooKeeperWatcher zkw,
      HRegionInfo region, String serverName, int expectedVersion)
  throws KeeperException {
    return transitionNode(zkw, region, serverName,
        EventType.RS_ZK_REGION_OPENING,
        EventType.RS_ZK_REGION_OPENING, expectedVersion);
  }

  /**
   * Transitions an existing unassigned node for the specified region which is
   * currently in the OPENING state to be in the OPENED state.
   *
   * <p>Does not transition nodes from other states.  If for some reason the
   * node could not be transitioned, the method returns -1.  If the transition
   * is successful, the version of the node after transition is returned.
   *
   * <p>This method can fail and return false for three different reasons:
   * <ul><li>Unassigned node for this region does not exist</li>
   * <li>Unassigned node for this region is not in OPENING state</li>
   * <li>After verifying OPENING state, update fails because of wrong version
   * (this should never actually happen since an RS only does this transition
   * following a transition to OPENING.  if two RS are conflicting, one would
   * fail the original transition to OPENING and not this transition)</li>
   * </ul>
   *
   * <p>Does not set any watches.
   *
   * <p>This method should only be used by a RegionServer when completing the
   * open of a region.
   *
   * @param zkw zk reference
   * @param region region to be transitioned to opened
   * @param serverName server event originates from
   * @return version of node after transition, -1 if unsuccessful transition
   * @throws KeeperException if unexpected zookeeper exception
   */
  public static int transitionNodeOpened(ZooKeeperWatcher zkw,
      HRegionInfo region, String serverName, int expectedVersion)
  throws KeeperException {
    return transitionNode(zkw, region, serverName,
        EventType.RS_ZK_REGION_OPENING,
        EventType.RS_ZK_REGION_OPENED, expectedVersion);
  }

  /**
   * Method that actually performs unassigned node transitions.
   *
   * <p>Attempts to transition the unassigned node for the specified region
   * from the expected state to the state in the specified transition data.
   *
   * <p>Method first reads existing data and verifies it is in the expected
   * state.  If the node does not exist or the node is not in the expected
   * state, the method returns -1.  If the transition is successful, the
   * version number of the node following the transition is returned.
   *
   * <p>If the read state is what is expected, it attempts to write the new
   * state and data into the node.  When doing this, it includes the expected
   * version (determined when the existing state was verified) to ensure that
   * only one transition is successful.  If there is a version mismatch, the
   * method returns -1.
   *
   * <p>If the write is successful, no watch is set and the method returns true.
   *
   * @param zkw zk reference
   * @param region region to be transitioned to opened
   * @param serverName server event originates from
   * @param endState state to transition node to if all checks pass
   * @param beginState state the node must currently be in to do transition
   * @param expectedVersion expected version of data before modification, or -1
   * @return version of node after transition, -1 if unsuccessful transition
   * @throws KeeperException if unexpected zookeeper exception
   */
  public static int transitionNode(ZooKeeperWatcher zkw, HRegionInfo region,
      String serverName, EventType beginState, EventType endState,
      int expectedVersion)
  throws KeeperException {
    return transitionNode(zkw, region, serverName, beginState, endState,
        expectedVersion, null);
  }

  public static int transitionNode(ZooKeeperWatcher zkw, HRegionInfo region,
      String serverName, EventType beginState, EventType endState,
      int expectedVersion, final byte [] payload)
  throws KeeperException {
    String encoded = region.getEncodedName();
    if(LOG.isDebugEnabled()) {
      LOG.debug(zkw.prefix("Attempting to transition node " +
        HRegionInfo.prettyPrint(encoded) +
        " from " + beginState.toString() + " to " + endState.toString()));
    }

    String node = getNodeName(zkw, encoded);
    zkw.sync(node);

    // Read existing data of the node
    Stat stat = new Stat();
    byte [] existingBytes =
      ZKUtil.getDataNoWatch(zkw, node, stat);
    RegionTransitionData existingData =
      RegionTransitionData.fromBytes(existingBytes);

    // Verify it is the expected version
    if(expectedVersion != -1 && stat.getVersion() != expectedVersion) {
      LOG.warn(zkw.prefix("Attempt to transition the " +
        "unassigned node for " + encoded +
        " from " + beginState + " to " + endState + " failed, " +
        "the node existed but was version " + stat.getVersion() +
        " not the expected version " + expectedVersion));
        return -1;
    }

    // Verify it is in expected state
    if(!existingData.getEventType().equals(beginState)) {
      LOG.warn(zkw.prefix("Attempt to transition the " +
        "unassigned node for " + encoded +
        " from " + beginState + " to " + endState + " failed, " +
        "the node existed but was in the state " + existingData.getEventType() +
        " set by the server " + existingData.getServerName()));
      return -1;
    }

    // Write new data, ensuring data has not changed since we last read it
    try {
      RegionTransitionData data = new RegionTransitionData(endState,
          region.getRegionName(), serverName, payload);
      if(!ZKUtil.setData(zkw, node, data.getBytes(), stat.getVersion())) {
        LOG.warn(zkw.prefix("Attempt to transition the " +
        "unassigned node for " + encoded +
        " from " + beginState + " to " + endState + " failed, " +
        "the node existed and was in the expected state but then when " +
        "setting data we got a version mismatch"));
        return -1;
      }
      if(LOG.isDebugEnabled()) {
        LOG.debug(zkw.prefix("Successfully transitioned node " + encoded +
          " from " + beginState + " to " + endState));
      }
      return stat.getVersion() + 1;
    } catch (KeeperException.NoNodeException nne) {
      LOG.warn(zkw.prefix("Attempt to transition the " +
        "unassigned node for " + encoded +
        " from " + beginState + " to " + endState + " failed, " +
        "the node existed and was in the expected state but then when " +
        "setting data it no longer existed"));
      return -1;
    }
  }

  /**
   * Gets the current data in the unassigned node for the specified region name
   * or fully-qualified path.
   *
   * <p>Returns null if the region does not currently have a node.
   *
   * <p>Sets a watch on the node if the node exists.
   *
   * @param zkw zk reference
   * @param pathOrRegionName fully-specified path or region name
   * @return data for the unassigned node
   * @throws KeeperException if unexpected zookeeper exception
   */
  public static RegionTransitionData getData(ZooKeeperWatcher zkw,
      String pathOrRegionName)
  throws KeeperException {
    String node = pathOrRegionName.startsWith("/") ?
        pathOrRegionName : getNodeName(zkw, pathOrRegionName);
    byte [] data = ZKUtil.getDataAndWatch(zkw, node);
    if(data == null) {
      return null;
    }
    return RegionTransitionData.fromBytes(data);
  }

  /**
   * Gets the current data in the unassigned node for the specified region name
   * or fully-qualified path.
   *
   * <p>Returns null if the region does not currently have a node.
   *
   * <p>Does not set a watch.
   *
   * @param zkw zk reference
   * @param pathOrRegionName fully-specified path or region name
   * @param stat object to store node info into on getData call
   * @return data for the unassigned node
   * @throws KeeperException if unexpected zookeeper exception
   */
  public static RegionTransitionData getDataNoWatch(ZooKeeperWatcher zkw,
      String pathOrRegionName, Stat stat)
  throws KeeperException {
    String node = pathOrRegionName.startsWith("/") ?
        pathOrRegionName : getNodeName(zkw, pathOrRegionName);
    byte [] data = ZKUtil.getDataNoWatch(zkw, node, stat);
    if(data == null) {
      return null;
    }
    return RegionTransitionData.fromBytes(data);
  }

  /**
   * Delete the assignment node regardless of its current state.
   * <p>
   * Fail silent even if the node does not exist at all.
   * @param watcher
   * @param regionInfo
   * @throws KeeperException
   */
  public static void deleteNodeFailSilent(ZooKeeperWatcher watcher,
      HRegionInfo regionInfo)
  throws KeeperException {
    String node = getNodeName(watcher, regionInfo.getEncodedName());
    ZKUtil.deleteNodeFailSilent(watcher, node);
  }

  /**
   * Blocks until there are no node in regions in transition.
   * <p>
   * Used in testing only.
   * @param zkw zk reference
   * @throws KeeperException
   * @throws InterruptedException
   */
  public static void blockUntilNoRIT(ZooKeeperWatcher zkw)
  throws KeeperException, InterruptedException {
    while (ZKUtil.nodeHasChildren(zkw, zkw.assignmentZNode)) {
      List<String> znodes =
        ZKUtil.listChildrenAndWatchForNewChildren(zkw, zkw.assignmentZNode);
      if (znodes != null && !znodes.isEmpty()) {
        for (String znode : znodes) {
          LOG.debug("ZK RIT -> " + znode);
        }
      }
      Thread.sleep(100);
    }
  }

  /**
   * Blocks until there is at least one node in regions in transition.
   * <p>
   * Used in testing only.
   * @param zkw zk reference
   * @throws KeeperException
   * @throws InterruptedException
   */
  public static void blockUntilRIT(ZooKeeperWatcher zkw)
  throws KeeperException, InterruptedException {
    while (!ZKUtil.nodeHasChildren(zkw, zkw.assignmentZNode)) {
      List<String> znodes =
        ZKUtil.listChildrenAndWatchForNewChildren(zkw, zkw.assignmentZNode);
      if (znodes == null || znodes.isEmpty()) {
        LOG.debug("No RIT in ZK");
      }
      Thread.sleep(100);
    }
  }

  /**
   * Verifies that the specified region is in the specified state in ZooKeeper.
   * <p>
   * Returns true if region is in transition and in the specified state in
   * ZooKeeper.  Returns false if the region does not exist in ZK or is in
   * a different state.
   * <p>
   * Method synchronizes() with ZK so will yield an up-to-date result but is
   * a slow read.
   * @param zkw
   * @param region
   * @param expectedState
   * @return true if region exists and is in expected state
   */
  public static boolean verifyRegionState(ZooKeeperWatcher zkw,
      HRegionInfo region, EventType expectedState)
  throws KeeperException {
    String encoded = region.getEncodedName();

    String node = getNodeName(zkw, encoded);
    zkw.sync(node);

    // Read existing data of the node
    byte [] existingBytes = null;
    try {
      existingBytes = ZKUtil.getDataAndWatch(zkw, node);
    } catch (KeeperException.NoNodeException nne) {
      return false;
    } catch (KeeperException e) {
      throw e;
    }
    if (existingBytes == null) return false;
    RegionTransitionData existingData =
      RegionTransitionData.fromBytes(existingBytes);
    if (existingData.getEventType() == expectedState){
      return true;
    }
    return false;
  }
}
