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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.Abortable;
import org.apache.zookeeper.KeeperException;

/**
 * Tracks the availability and value of a single ZooKeeper node.
 *
 * <p>Utilizes the {@link ZooKeeperListener} interface to get the necessary
 * ZooKeeper events related to the node.
 *
 * <p>This is the base class used by trackers in both the Master and
 * RegionServers.
 */
public abstract class ZooKeeperNodeTracker extends ZooKeeperListener {
  
  static final Log LOG = LogFactory.getLog(ZooKeeperNodeTracker.class);
  /** Path of node being tracked */
  protected final String node;

  /** Data of the node being tracked */
  private byte [] data;

  /** Used to abort if a fatal error occurs */
  protected final Abortable abortable;

  private boolean stopped = false;

  /**
   * Constructs a new ZK node tracker.
   *
   * <p>After construction, use {@link #start} to kick off tracking.
   *
   * @param watcher
   * @param node
   * @param abortable
   */
  public ZooKeeperNodeTracker(ZooKeeperWatcher watcher, String node,
      Abortable abortable) {
    super(watcher);
    this.node = node;
    this.abortable = abortable;
    this.data = null;
  }

  /**
   * Starts the tracking of the node in ZooKeeper.
   *
   * <p>Use {@link #blockUntilAvailable()} to block until the node is available
   * or {@link #getData()} to get the data of the node if it is available.
   */
  public synchronized void start() {
    this.watcher.registerListener(this);
    try {
      if(ZKUtil.watchAndCheckExists(watcher, node)) {
        byte [] data = ZKUtil.getDataAndWatch(watcher, node);
        if(data != null) {
          this.data = data;
        } else {
          // It existed but now does not, try again to ensure a watch is set
          LOG.debug("Try starting again because there is no data from " + node);
          start();
        }
      }
    } catch (KeeperException e) {
      abortable.abort("Unexpected exception during initialization, aborting", e);
    }
  }

  public synchronized void stop() {
    this.stopped = true;
    notifyAll();
  }

  /**
   * Gets the data of the node, blocking until the node is available.
   *
   * @return data of the node
   * @throws InterruptedException if the waiting thread is interrupted
   */
  public synchronized byte [] blockUntilAvailable()
  throws InterruptedException {
    return blockUntilAvailable(0);
  }

  /**
   * Gets the data of the node, blocking until the node is available or the
   * specified timeout has elapsed.
   *
   * @param timeout maximum time to wait for the node data to be available,
   * n milliseconds.  Pass 0 for no timeout.
   * @return data of the node
   * @throws InterruptedException if the waiting thread is interrupted
   */
  public synchronized byte [] blockUntilAvailable(long timeout)
  throws InterruptedException {
    if (timeout < 0) throw new IllegalArgumentException();
    boolean notimeout = timeout == 0;
    long startTime = System.currentTimeMillis();
    long remaining = timeout;
    while (!this.stopped && (notimeout || remaining > 0) && this.data == null) {
      if (notimeout) {
        wait();
        continue;
      }
      wait(remaining);
      remaining = timeout - (System.currentTimeMillis() - startTime);
    }
    return data;
  }

  /**
   * Gets the data of the node.
   *
   * <p>If the node is currently available, the most up-to-date known version of
   * the data is returned.  If the node is not currently available, null is
   * returned.
   *
   * @return data of the node, null if unavailable
   */
  public synchronized byte [] getData() {
    return data;
  }

  public String getNode() {
    return this.node;
  }

  @Override
  public synchronized void nodeCreated(String path) {
    if (!path.equals(node)) return;
    try {
      byte [] data = ZKUtil.getDataAndWatch(watcher, node);
      if (data != null) {
        this.data = data;
        notifyAll();
      } else {
        nodeDeleted(path);
      }
    } catch(KeeperException e) {
      abortable.abort("Unexpected exception handling nodeCreated event", e);
    }
  }

  @Override
  public synchronized void nodeDeleted(String path) {
    if(path.equals(node)) {
      try {
        if(ZKUtil.watchAndCheckExists(watcher, node)) {
          nodeCreated(path);
        } else {
          this.data = null;
        }
      } catch(KeeperException e) {
        abortable.abort("Unexpected exception handling nodeDeleted event", e);
      }
    }
  }

  @Override
  public synchronized void nodeDataChanged(String path) {
    if(path.equals(node)) {
      nodeCreated(path);
    }
  }
  
  /**
   * Checks if the baseznode set as per the property 'zookeeper.znode.parent'
   * exists.
   * @return true if baseznode exists.
   *         false if doesnot exists.
   */
  public boolean checkIfBaseNodeAvailable() {
    try {
      if (ZKUtil.checkExists(watcher, watcher.baseZNode) == -1) {
        return false;
      }
    } catch (KeeperException e) {
      abortable
          .abort(
              "Exception while checking if basenode exists.",
              e);
    }
    return true;
  }
}
