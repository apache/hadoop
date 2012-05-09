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
package org.apache.hadoop.contrib.bkjournal;

import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs.Ids;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.List;
import java.util.Collections;
import java.util.Comparator;

import java.net.InetAddress;
import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Distributed lock, using ZooKeeper.
 *
 * The lock is vulnerable to timing issues. For example, the process could
 * encounter a really long GC cycle between acquiring the lock, and writing to
 * a ledger. This could have timed out the lock, and another process could have
 * acquired the lock and started writing to bookkeeper. Therefore other
 * mechanisms are required to ensure correctness (i.e. Fencing).
 */
class WriteLock implements Watcher {
  static final Log LOG = LogFactory.getLog(WriteLock.class);

  private final ZooKeeper zkc;
  private final String lockpath;

  private AtomicInteger lockCount = new AtomicInteger(0);
  private String myznode = null;

  WriteLock(ZooKeeper zkc, String lockpath) throws IOException {
    this.lockpath = lockpath;

    this.zkc = zkc;
    try {
      if (zkc.exists(lockpath, false) == null) {
        String localString = InetAddress.getLocalHost().toString();
        zkc.create(lockpath, localString.getBytes(),
                   Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
      }
    } catch (Exception e) {
      throw new IOException("Exception accessing Zookeeper", e);
    }
  }

  void acquire() throws IOException {
    while (true) {
      if (lockCount.get() == 0) {
        try {
          synchronized(this) {
            if (lockCount.get() > 0) {
              lockCount.incrementAndGet();
              return;
            }
            myznode = zkc.create(lockpath + "/lock-", new byte[] {'0'},
                                 Ids.OPEN_ACL_UNSAFE,
                                 CreateMode.EPHEMERAL_SEQUENTIAL);
            if (LOG.isTraceEnabled()) {
              LOG.trace("Acquiring lock, trying " + myznode);
            }

            List<String> nodes = zkc.getChildren(lockpath, false);
            Collections.sort(nodes, new Comparator<String>() {
                public int compare(String o1,
                                   String o2) {
                  Integer l1 = Integer.valueOf(o1.replace("lock-", ""));
                  Integer l2 = Integer.valueOf(o2.replace("lock-", ""));
                  return l1 - l2;
                }
              });
            if ((lockpath + "/" + nodes.get(0)).equals(myznode)) {
              if (LOG.isTraceEnabled()) {
                LOG.trace("Lock acquired - " + myznode);
              }
              lockCount.set(1);
              zkc.exists(myznode, this);
              return;
            } else {
              LOG.error("Failed to acquire lock with " + myznode
                        + ", " + nodes.get(0) + " already has it");
              throw new IOException("Could not acquire lock");
            }
          }
        } catch (KeeperException e) {
          throw new IOException("Exception accessing Zookeeper", e);
        } catch (InterruptedException ie) {
          throw new IOException("Exception accessing Zookeeper", ie);
        }
      } else {
        int ret = lockCount.getAndIncrement();
        if (ret == 0) {
          lockCount.decrementAndGet();
          continue; // try again;
        } else {
          return;
        }
      }
    }
  }

  void release() throws IOException {
    try {
      if (lockCount.decrementAndGet() <= 0) {
        if (lockCount.get() < 0) {
          LOG.warn("Unbalanced lock handling somewhere, lockCount down to "
                   + lockCount.get());
        }
        synchronized(this) {
          if (lockCount.get() <= 0) {
            if (LOG.isTraceEnabled()) {
              LOG.trace("releasing lock " + myznode);
            }
            if (myznode != null) {
              zkc.delete(myznode, -1);
              myznode = null;
            }
          }
        }
      }
    } catch (Exception e) {
      throw new IOException("Exception accessing Zookeeper", e);
    }
  }

  public void checkWriteLock() throws IOException {
    if (!haveLock()) {
      throw new IOException("Lost writer lock");
    }
  }

  boolean haveLock() throws IOException {
    return lockCount.get() > 0;
  }

  public void process(WatchedEvent event) {
    if (event.getState() == KeeperState.Disconnected
        || event.getState() == KeeperState.Expired) {
      LOG.warn("Lost zookeeper session, lost lock ");
      lockCount.set(0);
    } else {
      // reapply the watch
      synchronized (this) {
        LOG.info("Zookeeper event " + event
                 + " received, reapplying watch to " + myznode);
        if (myznode != null) {
          try {
            zkc.exists(myznode, this);
          } catch (Exception e) {
            LOG.warn("Could not set watch on lock, releasing", e);
            try {
              release();
            } catch (IOException ioe) {
              LOG.error("Could not release Zk lock", ioe);
            }
          }
        }
      }
    }
  }
}
