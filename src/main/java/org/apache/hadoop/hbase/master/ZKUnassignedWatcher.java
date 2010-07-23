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
package org.apache.hadoop.hbase.master;

import java.io.IOException;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.executor.HBaseEventHandler.HBaseEventType;
import org.apache.hadoop.hbase.master.handler.MasterCloseRegionHandler;
import org.apache.hadoop.hbase.master.handler.MasterOpenRegionHandler;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWrapper;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWrapper.ZNodePathAndData;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;

/**
 * Watches the UNASSIGNED znode in ZK for the master, and handles all events 
 * relating to region transitions.
 */
public class ZKUnassignedWatcher implements Watcher {
  private static final Log LOG = LogFactory.getLog(ZKUnassignedWatcher.class);

  private ZooKeeperWrapper zkWrapper;
  String serverName;
  ServerManager serverManager;

  public static void start(Configuration conf, HMaster master) 
  throws IOException {
    new ZKUnassignedWatcher(conf, master);
    LOG.debug("Started ZKUnassigned watcher");
  }

  public ZKUnassignedWatcher(Configuration conf, HMaster master) 
  throws IOException {
    this.serverName = master.getHServerAddress().toString();
    this.serverManager = master.getServerManager();
    zkWrapper = ZooKeeperWrapper.getInstance(conf, HMaster.class.getName());
    String unassignedZNode = zkWrapper.getRegionInTransitionZNode();
    
    // If the UNASSIGNED ZNode exists and this is a fresh cluster start, then 
    // delete it.
    if(master.isClusterStartup() && zkWrapper.exists(unassignedZNode, false)) {
      LOG.info("Cluster start, but found " + unassignedZNode + ", deleting it.");
      try {
        zkWrapper.deleteZNode(unassignedZNode, true);
      } catch (KeeperException e) {
        LOG.error("Could not delete znode " + unassignedZNode, e);
        throw new IOException(e);
      } catch (InterruptedException e) {
        LOG.error("Could not delete znode " + unassignedZNode, e);
        throw new IOException(e);
      }
    }
    
    // If the UNASSIGNED ZNode does not exist, create it.
    zkWrapper.createZNodeIfNotExists(unassignedZNode);
    
    // TODO: get the outstanding changes in UNASSIGNED

    // Set a watch on Zookeeper's UNASSIGNED node if it exists.
    zkWrapper.registerListener(this);
  }

  /**
   * This is the processing loop that gets triggered from the ZooKeeperWrapper.
   * This zookeeper events process function dies the following:
   *   - WATCHES the following events: NodeCreated, NodeDataChanged, NodeChildrenChanged
   *   - IGNORES the following events: None, NodeDeleted
   */
  @Override
  public synchronized void process(WatchedEvent event) {
    EventType type = event.getType();
    LOG.debug("ZK-EVENT-PROCESS: Got zkEvent " + type +
              " state:" + event.getState() +
              " path:" + event.getPath());

    // Handle the ignored events
    if(type.equals(EventType.None)       ||
       type.equals(EventType.NodeDeleted)) {
      return;
    }

    // check if the path is for the UNASSIGNED directory we care about
    if(event.getPath() == null ||
       !event.getPath().startsWith(zkWrapper.getZNodePathForHBase(
           zkWrapper.getRegionInTransitionZNode()))) {
      return;
    }

    try
    {
      /*
       * If a node is created in the UNASSIGNED directory in zookeeper, then:
       *   1. watch its updates (this is an unassigned region).
       *   2. read to see what its state is and handle as needed (state may have
       *      changed before we started watching it)
       */
      if(type.equals(EventType.NodeCreated)) {
        zkWrapper.watchZNode(event.getPath());
        handleRegionStateInZK(event.getPath());
      }
      /*
       * Data on some node has changed. Read to see what the state is and handle
       * as needed.
       */
      else if(type.equals(EventType.NodeDataChanged)) {
        handleRegionStateInZK(event.getPath());
      }
      /*
       * If there were some nodes created then watch those nodes
       */
      else if(type.equals(EventType.NodeChildrenChanged)) {
        List<ZNodePathAndData> newZNodes =
            zkWrapper.watchAndGetNewChildren(event.getPath());
        for(ZNodePathAndData zNodePathAndData : newZNodes) {
          LOG.debug("Handling updates for znode: " + zNodePathAndData.getzNodePath());
          handleRegionStateInZK(zNodePathAndData.getzNodePath(),
              zNodePathAndData.getData());
        }
      }
    }
    catch (IOException e)
    {
      LOG.error("Could not process event from ZooKeeper", e);
    }
  }

  /**
   * Read the state of a node in ZK, and do the needful. We want to do the
   * following:
   *   1. If region's state is updated as CLOSED, invoke the ClosedRegionHandler.
   *   2. If region's state is updated as OPENED, invoke the OpenRegionHandler.
   * @param zNodePath
   * @throws IOException
   */
  private void handleRegionStateInZK(String zNodePath) throws IOException {
    byte[] data = zkWrapper.readZNode(zNodePath, null);
    handleRegionStateInZK(zNodePath, data);
  }
  
  private void handleRegionStateInZK(String zNodePath, byte[] data) {
    // a null value is set when a node is created, we don't need to handle this
    if(data == null) {
      return;
    }
    String rgnInTransitNode = zkWrapper.getRegionInTransitionZNode();
    String region = zNodePath.substring(
        zNodePath.indexOf(rgnInTransitNode) + rgnInTransitNode.length() + 1);
    HBaseEventType rsEvent = HBaseEventType.fromByte(data[0]);
    LOG.debug("Got event type [ " + rsEvent + " ] for region " + region);

    // if the node was CLOSED then handle it
    if(rsEvent == HBaseEventType.RS2ZK_REGION_CLOSED) {
      new MasterCloseRegionHandler(rsEvent, serverManager, serverName, region, data).submit();
    }
    // if the region was OPENED then handle that
    else if(rsEvent == HBaseEventType.RS2ZK_REGION_OPENED || 
            rsEvent == HBaseEventType.RS2ZK_REGION_OPENING) {
      new MasterOpenRegionHandler(rsEvent, serverManager, serverName, region, data).submit();
    }
  }
}

