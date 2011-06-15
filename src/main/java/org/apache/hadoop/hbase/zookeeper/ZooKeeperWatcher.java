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
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.util.Threads;
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
public class ZooKeeperWatcher implements Watcher, Abortable {
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
  private final List<ZooKeeperListener> listeners =
    new CopyOnWriteArrayList<ZooKeeperListener>();

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
  // znode containing the unique cluster ID
  public String clusterIdZNode;
  // znode used for log splitting work assignment
  public String splitLogZNode;

  private final Configuration conf;

  private final Exception constructorCaller;

  /**
   * Instantiate a ZooKeeper connection and watcher.
   * @param descriptor Descriptive string that is added to zookeeper sessionid
   * and used as identifier for this instance.
   * @throws IOException
   * @throws ZooKeeperConnectionException
   */
  public ZooKeeperWatcher(Configuration conf, String descriptor,
      Abortable abortable)
  throws IOException, ZooKeeperConnectionException {
    this.conf = conf;
    // Capture a stack trace now.  Will print it out later if problem so we can
    // distingush amongst the myriad ZKWs.
    try {
      throw new Exception("ZKW CONSTRUCTOR STACK TRACE FOR DEBUGGING");
    } catch (Exception e) {
      this.constructorCaller = e;
    }
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

      // The first call against zk can fail with connection loss.  Seems common.
      // Apparently this is recoverable.  Retry a while.
      // See http://wiki.apache.org/hadoop/ZooKeeper/ErrorHandling
      // TODO: Generalize out in ZKUtil.
      long wait = conf.getLong(HConstants.ZOOKEEPER_RECOVERABLE_WAITTIME,
          HConstants.DEFAULT_ZOOKEPER_RECOVERABLE_WAITIME);
      long finished = System.currentTimeMillis() + wait;
      KeeperException ke = null;
      do {
        try {
          ZKUtil.createAndFailSilent(this, baseZNode);
          ke = null;
          break;
        } catch (KeeperException.ConnectionLossException e) {
          if (LOG.isDebugEnabled() && (isFinishedRetryingRecoverable(finished))) {
            LOG.debug("Retrying zk create for another " +
              (finished - System.currentTimeMillis()) +
              "ms; set 'hbase.zookeeper.recoverable.waittime' to change " +
              "wait time); " + e.getMessage());
          }
          ke = e;
        }
      } while (isFinishedRetryingRecoverable(finished));
      // Convert connectionloss exception to ZKCE.
      if (ke != null) {
        try {
          // If we don't close it, the zk connection managers won't be killed
          this.zooKeeper.close();
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          LOG.warn("Interrupted while closing", e);
        }
        throw new ZooKeeperConnectionException("HBase is able to connect to" +
            " ZooKeeper but the connection closes immediately. This could be" +
            " a sign that the server has too many connections (30 is the" +
            " default). Consider inspecting your ZK server logs for that" +
            " error and then make sure you are reusing HBaseConfiguration" +
            " as often as you can. See HTable's javadoc for more information.",
            ke);
      }
      ZKUtil.createAndFailSilent(this, assignmentZNode);
      ZKUtil.createAndFailSilent(this, rsZNode);
      ZKUtil.createAndFailSilent(this, tableZNode);
      ZKUtil.createAndFailSilent(this, splitLogZNode);
    } catch (KeeperException e) {
      throw new ZooKeeperConnectionException(
          prefix("Unexpected KeeperException creating base node"), e);
    }
  }

  private boolean isFinishedRetryingRecoverable(final long finished) {
    return System.currentTimeMillis() < finished;
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
    clusterIdZNode = ZKUtil.joinZNode(baseZNode,
        conf.get("zookeeper.znode.clusterId", "hbaseid"));
    splitLogZNode = ZKUtil.joinZNode(baseZNode,
        conf.get("zookeeper.znode.splitlog", "splitlog"));
  }

  /**
   * Register the specified listener to receive ZooKeeper events.
   * @param listener
   */
  public void registerListener(ZooKeeperListener listener) {
    listeners.add(listener);
  }

  /**
   * Register the specified listener to receive ZooKeeper events and add it as
   * the first in the list of current listeners.
   * @param listener
   */
  public void registerListenerFirst(ZooKeeperListener listener) {
    listeners.add(0, listener);
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
   * @return quorum string of this zookeeper connection instance
   */
  public String getQuorum() {
    return quorum;
  }

  /**
   * Method called from ZooKeeper for events and connection status.
   * <p>
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
   * <p>
   * If Disconnected or Expired, this should shutdown the cluster. But, since
   * we send a KeeperException.SessionExpiredException along with the abort
   * call, it's possible for the Abortable to catch it and try to create a new
   * session with ZooKeeper. This is what the client does in HCM.
   * <p>
   * @param event
   */
  private void connectionEvent(WatchedEvent event) {
    switch(event.getState()) {
      case SyncConnected:
        // Now, this callback can be invoked before the this.zookeeper is set.
        // Wait a little while.
        long finished = System.currentTimeMillis() +
          this.conf.getLong("hbase.zookeeper.watcher.sync.connected.wait", 2000);
        while (System.currentTimeMillis() < finished) {
          Threads.sleep(1);
          if (this.zooKeeper != null) break;
        }
        if (this.zooKeeper == null) {
          LOG.error("ZK is null on connection event -- see stack trace " +
            "for the stack trace when constructor was called on this zkw",
            this.constructorCaller);
          throw new NullPointerException("ZK is null");
        }
        this.identifier = this.identifier + "-0x" +
          Long.toHexString(this.zooKeeper.getSessionId());
        // Update our identifier.  Otherwise ignore.
        LOG.debug(this.identifier + " connected");
        break;

      // Abort the server if Disconnected or Expired
      // TODO: Åny reason to handle these two differently?
      case Disconnected:
        LOG.debug(prefix("Received Disconnected from ZooKeeper, ignoring"));
        break;

      case Expired:
        String msg = prefix(this.identifier + " received expired from " +
          "ZooKeeper, aborting");
        // TODO: One thought is to add call to ZooKeeperListener so say,
        // ZooKeperNodeTracker can zero out its data values.
        if (this.abortable != null) this.abortable.abort(msg,
            new KeeperException.SessionExpiredException());
        break;
    }
  }

  /**
   * Forces a synchronization of this ZooKeeper client connection.
   * <p>
   * Executing this method before running other methods will ensure that the
   * subsequent operations are up-to-date and consistent as of the time that
   * the sync is complete.
   * <p>
   * This is used for compareAndSwap type operations where we need to read the
   * data of an existing node and delete or transition that node, utilizing the
   * previously read version and data.  We want to ensure that the version read
   * is up-to-date from when we begin the operation.
   */
  public void sync(String path) {
    this.zooKeeper.sync(path, null, null);
  }

  /**
   * Handles KeeperExceptions in client calls.
   * <p>
   * This may be temporary but for now this gives one place to deal with these.
   * <p>
   * TODO: Currently this method rethrows the exception to let the caller handle
   * <p>
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
   * <p>
   * This may be temporary but for now this gives one place to deal with these.
   * <p>
   * TODO: Currently, this method does nothing.
   *       Is this ever expected to happen?  Do we abort or can we let it run?
   *       Maybe this should be logged as WARN?  It shouldn't happen?
   * <p>
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

  @Override
  public void abort(String why, Throwable e) {
    this.abortable.abort(why, e);
  }
}
