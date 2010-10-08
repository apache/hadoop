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

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.HConstants;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

/**
 * Acts as the single ZooKeeper Watcher.  One instance of this is instantiated
 * for each Master, RegionServer, and client process.
 *
 * <p>This is the only class that implements {@link Watcher}.  Other internal
 * classes which need to be notified of ZooKeeper events must register with
 * the local instance of this watcher via {@link #registerListener}.
 *
 * <p>This class also holds and manages the connection to ZooKeeper.  Code to
 * deal with connection related events and exceptions are handled here.
 */
public class ZooKeeperWatcher implements Watcher {
  private static final Log LOG = LogFactory.getLog(ZooKeeperWatcher.class);

  // Identifiier for this watcher (for logging only).  Its made of the prefix
  // passed on construction and the zookeeper sessionid.
  private String identifier;

  // zookeeper quorum
  private String quorum;

  // zookeeper connection
  private ZooKeeper zooKeeper;

  // abortable in case of zk failure
  private Abortable abortable;

  // listeners to be notified
  private final Set<ZooKeeperListener> listeners =
    new CopyOnWriteArraySet<ZooKeeperListener>();

  // set of unassigned nodes watched
  private Set<String> unassignedNodes = new HashSet<String>();

  // node names

  // base znode for this cluster
  public String baseZNode;
  // znode containing location of server hosting root region
  public String rootServerZNode;
  // znode containing ephemeral nodes of the regionservers
  public String rsZNode;
  // znode of currently active master
  public String masterAddressZNode;
  // znode containing the current cluster state
  public String clusterStateZNode;
  // znode used for region transitioning and assignment
  public String assignmentZNode;
  // znode used for table disabling/enabling
  public String tableZNode;

  /**
   * Instantiate a ZooKeeper connection and watcher.
   * @param descriptor Descriptive string that is added to zookeeper sessionid
   * and used as identifier for this instance.
   * @throws IOException
   */
  public ZooKeeperWatcher(Configuration conf, String descriptor,
      Abortable abortable)
  throws IOException {
    this.quorum = ZKConfig.getZKQuorumServersString(conf);
    // Identifier will get the sessionid appended later below down when we
    // handle the syncconnect event.
    this.identifier = descriptor;
    this.abortable = abortable;
    setNodeNames(conf);
    this.zooKeeper = ZKUtil.connect(conf, quorum, this, descriptor);
    try {
      // Create all the necessary "directories" of znodes
      // TODO: Move this to an init method somewhere so not everyone calls it?
      ZKUtil.createAndFailSilent(this, baseZNode);
      ZKUtil.createAndFailSilent(this, assignmentZNode);
      ZKUtil.createAndFailSilent(this, rsZNode);
      ZKUtil.createAndFailSilent(this, tableZNode);
    } catch (KeeperException e) {
      LOG.error(prefix("Unexpected KeeperException creating base node"), e);
      throw new IOException(e);
    }
  }

  @Override
  public String toString() {
    return this.identifier;
  }

  /**
   * Adds this instance's identifier as a prefix to the passed <code>str</code>
   * @param str String to amend.
   * @return A new string with this instance's identifier as prefix: e.g.
   * if passed 'hello world', the returned string could be
   */
  public String prefix(final String str) {
    return this.toString() + " " + str;
  }

  /**
   * Set the local variable node names using the specified configuration.
   */
  private void setNodeNames(Configuration conf) {
    baseZNode = conf.get(HConstants.ZOOKEEPER_ZNODE_PARENT,
        HConstants.DEFAULT_ZOOKEEPER_ZNODE_PARENT);
    rootServerZNode = ZKUtil.joinZNode(baseZNode,
        conf.get("zookeeper.znode.rootserver", "root-region-server"));
    rsZNode = ZKUtil.joinZNode(baseZNode,
        conf.get("zookeeper.znode.rs", "rs"));
    masterAddressZNode = ZKUtil.joinZNode(baseZNode,
        conf.get("zookeeper.znode.master", "master"));
    clusterStateZNode = ZKUtil.joinZNode(baseZNode,
        conf.get("zookeeper.znode.state", "shutdown"));
    assignmentZNode = ZKUtil.joinZNode(baseZNode,
        conf.get("zookeeper.znode.unassigned", "unassigned"));
    tableZNode = ZKUtil.joinZNode(baseZNode,
        conf.get("zookeeper.znode.tableEnableDisable", "table"));
  }

  /**
   * Register the specified listener to receive ZooKeeper events.
   * @param listener
   */
  public void registerListener(ZooKeeperListener listener) {
    listeners.add(listener);
  }

  /**
   * Get the connection to ZooKeeper.
   * @return connection reference to zookeeper
   */
  public ZooKeeper getZooKeeper() {
    return zooKeeper;
  }

  /**
   * Get the quorum address of this instance.
   * @returns quorum string of this zookeeper connection instance
   */
  public String getQuorum() {
    return quorum;
  }

  /**
   * Method called from ZooKeeper for events and connection status.
   *
   * Valid events are passed along to listeners.  Connection status changes
   * are dealt with locally.
   */
  @Override
  public void process(WatchedEvent event) {
    LOG.debug(prefix("Received ZooKeeper Event, " +
        "type=" + event.getType() + ", " +
        "state=" + event.getState() + ", " +
        "path=" + event.getPath()));

    switch(event.getType()) {

      // If event type is NONE, this is a connection status change
      case None: {
        connectionEvent(event);
        break;
      }

      // Otherwise pass along to the listeners

      case NodeCreated: {
        for(ZooKeeperListener listener : listeners) {
          listener.nodeCreated(event.getPath());
        }
        break;
      }

      case NodeDeleted: {
        for(ZooKeeperListener listener : listeners) {
          listener.nodeDeleted(event.getPath());
        }
        break;
      }

      case NodeDataChanged: {
        for(ZooKeeperListener listener : listeners) {
          listener.nodeDataChanged(event.getPath());
        }
        break;
      }

      case NodeChildrenChanged: {
        for(ZooKeeperListener listener : listeners) {
          listener.nodeChildrenChanged(event.getPath());
        }
        break;
      }
    }
  }

  // Connection management

  /**
   * Called when there is a connection-related event via the Watcher callback.
   *
   * If Disconnected or Expired, this should shutdown the cluster.
   *
   * @param event
   */
  private void connectionEvent(WatchedEvent event) {
    switch(event.getState()) {
      case SyncConnected:
        // Update our identifier.  Otherwise ignore.
        LOG.info(this.identifier + " connected");
        this.identifier = this.identifier + "-0x" +
          Long.toHexString(this.zooKeeper.getSessionId());
        break;

      // Abort the server if Disconnected or Expired
      // TODO: Åny reason to handle these two differently?
      case Disconnected:
        LOG.info(prefix("Received Disconnected from ZooKeeper, ignoring"));
        break;

      case Expired:
        String msg = prefix(this.identifier + " received expired from " +
          "ZooKeeper, aborting");
        // TODO: One thought is to add call to ZooKeeperListener so say,
        // ZooKeperNodeTracker can zero out its data values.
        if (this.abortable != null) this.abortable.abort(msg, null);
        break;
    }
  }

  /**
   * Get the set of already watched unassigned nodes.
   * @return
   */
  public Set<String> getNodes() {
    return unassignedNodes;
  }

  /**
   * Handles KeeperExceptions in client calls.
   *
   * This may be temporary but for now this gives one place to deal with these.
   *
   * TODO: Currently this method rethrows the exception to let the caller handle
   *
   * @param ke
   * @throws KeeperException
   */
  public void keeperException(KeeperException ke)
  throws KeeperException {
    LOG.error(prefix("Received unexpected KeeperException, re-throwing exception"), ke);
    throw ke;
  }

  /**
   * Handles InterruptedExceptions in client calls.
   *
   * This may be temporary but for now this gives one place to deal with these.
   *
   * TODO: Currently, this method does nothing.
   *       Is this ever expected to happen?  Do we abort or can we let it run?
   *       Maybe this should be logged as WARN?  It shouldn't happen?
   *
   * @param ie
   */
  public void interruptedException(InterruptedException ie) {
    LOG.debug(prefix("Received InterruptedException, doing nothing here"), ie);
    // At least preserver interrupt.
    Thread.currentThread().interrupt();
    // no-op
  }

  /**
   * Close the connection to ZooKeeper.
   * @throws InterruptedException
   */
  public void close() {
    try {
      if (zooKeeper != null) {
        zooKeeper.close();
//        super.close();
      }
    } catch (InterruptedException e) {
    }
  }
}
