/*
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

import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.Stoppable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.zookeeper.KeeperException;

/**
 * Handles coordination of a single "leader" instance among many possible
 * candidates.  The first {@link ZKLeaderManager} to successfully create
 * the given znode becomes the leader, allowing the instance to continue
 * with whatever processing must be protected.  Other {@link ZKLeaderManager}
 * instances will wait to be notified of changes to the leader znode.
 * If the current master instance fails, the ephemeral leader znode will
 * be removed, and all waiting instances will be notified, with the race
 * to claim the leader znode beginning all over again.
 */
public class ZKLeaderManager extends ZooKeeperListener {
  private static Log LOG = LogFactory.getLog(ZKLeaderManager.class);

  private final AtomicBoolean leaderExists = new AtomicBoolean();
  private String leaderZNode;
  private byte[] nodeId;
  private Stoppable candidate;

  public ZKLeaderManager(ZooKeeperWatcher watcher, String leaderZNode,
      byte[] identifier, Stoppable candidate) {
    super(watcher);
    this.leaderZNode = leaderZNode;
    this.nodeId = identifier;
    this.candidate = candidate;
  }

  public void start() {
    try {
      watcher.registerListener(this);
      String parent = ZKUtil.getParent(leaderZNode);
      if (ZKUtil.checkExists(watcher, parent) < 0) {
        ZKUtil.createWithParents(watcher, parent);
      }
    } catch (KeeperException ke) {
      watcher.abort("Unhandled zk exception when starting", ke);
      candidate.stop("Unhandled zk exception starting up: "+ke.getMessage());
    }
  }

  @Override
  public void nodeCreated(String path) {
    if (leaderZNode.equals(path) && !candidate.isStopped()) {
      handleLeaderChange();
    }
  }

  @Override
  public void nodeDeleted(String path) {
    if (leaderZNode.equals(path) && !candidate.isStopped()) {
      handleLeaderChange();
    }
  }

  private void handleLeaderChange() {
    try {
      synchronized(leaderExists) {
        if (ZKUtil.watchAndCheckExists(watcher, leaderZNode)) {
          LOG.info("Found new leader for znode: "+leaderZNode);
          leaderExists.set(true);
        } else {
          LOG.info("Leader change, but no new leader found");
          leaderExists.set(false);
          leaderExists.notifyAll();
        }
      }
    } catch (KeeperException ke) {
      watcher.abort("ZooKeeper error checking for leader znode", ke);
      candidate.stop("ZooKeeper error checking for leader: "+ke.getMessage());
    }
  }

  /**
   * Blocks until this instance has claimed the leader ZNode in ZooKeeper
   */
  public void waitToBecomeLeader() {
    while (!candidate.isStopped()) {
      try {
        if (ZKUtil.createEphemeralNodeAndWatch(watcher, leaderZNode, nodeId)) {
          // claimed the leader znode
          leaderExists.set(true);
          if (LOG.isDebugEnabled()) {
            LOG.debug("Claimed the leader znode as '"+
                Bytes.toStringBinary(nodeId)+"'");
          }
          return;
        }

        // if claiming the node failed, there should be another existing node
        byte[] currentId = ZKUtil.getDataAndWatch(watcher, leaderZNode);
        if (currentId != null && Bytes.equals(currentId, nodeId)) {
          // claimed with our ID, but we didn't grab it, possibly restarted?
          LOG.info("Found existing leader with our ID ("+
              Bytes.toStringBinary(nodeId)+"), removing");
          ZKUtil.deleteNode(watcher, leaderZNode);
          leaderExists.set(false);
        } else {
          LOG.info("Found existing leader with ID: "+Bytes.toStringBinary(nodeId));
          leaderExists.set(true);
        }
      } catch (KeeperException ke) {
        watcher.abort("Unexpected error from ZK, stopping candidate", ke);
        candidate.stop("Unexpected error from ZK: "+ke.getMessage());
        return;
      }

      // wait for next chance
      synchronized(leaderExists) {
        while (leaderExists.get() && !candidate.isStopped()) {
          try {
            leaderExists.wait();
          } catch (InterruptedException ie) {
            LOG.debug("Interrupted waiting on leader", ie);
          }
        }
      }
    }
  }

  /**
   * Removes the leader znode, if it is currently claimed by this instance.
   */
  public void stepDownAsLeader() {
    try {
      synchronized(leaderExists) {
        if (!leaderExists.get()) {
          return;
        }
        byte[] leaderId = ZKUtil.getData(watcher, leaderZNode);
        if (leaderId != null && Bytes.equals(nodeId, leaderId)) {
          LOG.info("Stepping down as leader");
          ZKUtil.deleteNodeFailSilent(watcher, leaderZNode);
          leaderExists.set(false);
        } else {
          LOG.info("Not current leader, no need to step down");
        }
      }
    } catch (KeeperException ke) {
      watcher.abort("Unhandled zookeeper exception removing leader node", ke);
      candidate.stop("Unhandled zookeeper exception removing leader node: "
          + ke.getMessage());
    }
  }

  public boolean hasLeader() {
    return leaderExists.get();
  }
}
