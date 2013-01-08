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

package org.apache.hadoop.hdfs;

import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.LinkedListMultimap;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.hdfs.net.Peer;
import org.apache.hadoop.util.Daemon;
import org.apache.hadoop.util.Time;

/**
 * A cache of input stream sockets to Data Node.
 */
class PeerCache {
  private static final Log LOG = LogFactory.getLog(PeerCache.class);
  
  private static class Value {
    private final Peer peer;
    private final long time;

    Value(Peer peer, long time) {
      this.peer = peer;
      this.time = time;
    }

    Peer getPeer() {
      return peer;
    }

    long getTime() {
      return time;
    }
  }

  private Daemon daemon;
  /** A map for per user per datanode. */
  private static LinkedListMultimap<DatanodeID, Value> multimap =
    LinkedListMultimap.create();
  private static int capacity;
  private static long expiryPeriod;
  private static PeerCache instance = new PeerCache();
  private static boolean isInitedOnce = false;
 
  public static synchronized PeerCache getInstance(int c, long e) {
    // capacity is only initialized once
    if (isInitedOnce == false) {
      capacity = c;
      expiryPeriod = e;

      if (capacity == 0 ) {
        LOG.info("SocketCache disabled.");
      }
      else if (expiryPeriod == 0) {
        throw new IllegalStateException("Cannot initialize expiryPeriod to " +
           expiryPeriod + "when cache is enabled.");
      }
      isInitedOnce = true;
    } else { //already initialized once
      if (capacity != c || expiryPeriod != e) {
        LOG.info("capacity and expiry periods already set to " + capacity + 
          " and " + expiryPeriod + " respectively. Cannot set it to " + c + 
          " and " + e);
      }
    }

    return instance;
  }

  private boolean isDaemonStarted() {
    return (daemon == null)? false: true;
  }

  private synchronized void startExpiryDaemon() {
    // start daemon only if not already started
    if (isDaemonStarted() == true) {
      return;
    }
    
    daemon = new Daemon(new Runnable() {
      @Override
      public void run() {
        try {
          PeerCache.this.run();
        } catch(InterruptedException e) {
          //noop
        } finally {
          PeerCache.this.clear();
        }
      }

      @Override
      public String toString() {
        return String.valueOf(PeerCache.this);
      }
    });
    daemon.start();
  }

  /**
   * Get a cached peer connected to the given DataNode.
   * @param dnId         The DataNode to get a Peer for.
   * @return             An open Peer connected to the DN, or null if none
   *                     was found. 
   */
  public synchronized Peer get(DatanodeID dnId) {

    if (capacity <= 0) { // disabled
      return null;
    }

    List<Value> sockStreamList = multimap.get(dnId);
    if (sockStreamList == null) {
      return null;
    }

    Iterator<Value> iter = sockStreamList.iterator();
    while (iter.hasNext()) {
      Value candidate = iter.next();
      iter.remove();
      if (!candidate.getPeer().isClosed()) {
        return candidate.getPeer();
      }
    }
    return null;
  }

  /**
   * Give an unused socket to the cache.
   * @param sock socket not used by anyone.
   */
  public synchronized void put(DatanodeID dnId, Peer peer) {
    Preconditions.checkNotNull(dnId);
    Preconditions.checkNotNull(peer);
    if (peer.isClosed()) return;
    if (capacity <= 0) {
      // Cache disabled.
      IOUtils.cleanup(LOG, peer);
      return;
    }
 
    startExpiryDaemon();

    if (capacity == multimap.size()) {
      evictOldest();
    }
    multimap.put(dnId, new Value(peer, Time.monotonicNow()));
  }

  public synchronized int size() {
    return multimap.size();
  }

  /**
   * Evict and close sockets older than expiry period from the cache.
   */
  private synchronized void evictExpired(long expiryPeriod) {
    while (multimap.size() != 0) {
      Iterator<Entry<DatanodeID, Value>> iter =
        multimap.entries().iterator();
      Entry<DatanodeID, Value> entry = iter.next();
      // if oldest socket expired, remove it
      if (entry == null || 
        Time.monotonicNow() - entry.getValue().getTime() <
        expiryPeriod) {
        break;
      }
      IOUtils.cleanup(LOG, entry.getValue().getPeer());
      iter.remove();
    }
  }

  /**
   * Evict the oldest entry in the cache.
   */
  private synchronized void evictOldest() {
    // We can get the oldest element immediately, because of an interesting
    // property of LinkedListMultimap: its iterator traverses entries in the
    // order that they were added.
    Iterator<Entry<DatanodeID, Value>> iter =
      multimap.entries().iterator();
    if (!iter.hasNext()) {
      throw new IllegalStateException("Cannot evict from empty cache! " +
        "capacity: " + capacity);
    }
    Entry<DatanodeID, Value> entry = iter.next();
    IOUtils.cleanup(LOG, entry.getValue().getPeer());
    iter.remove();
  }

  /**
   * Periodically check in the cache and expire the entries
   * older than expiryPeriod minutes
   */
  private void run() throws InterruptedException {
    for(long lastExpiryTime = Time.monotonicNow();
        !Thread.interrupted();
        Thread.sleep(expiryPeriod)) {
      final long elapsed = Time.monotonicNow() - lastExpiryTime;
      if (elapsed >= expiryPeriod) {
        evictExpired(expiryPeriod);
        lastExpiryTime = Time.monotonicNow();
      }
    }
    clear();
    throw new InterruptedException("Daemon Interrupted");
  }

  /**
   * Empty the cache, and close all sockets.
   */
  @VisibleForTesting
  synchronized void clear() {
    for (Value value : multimap.values()) {
      IOUtils.cleanup(LOG, value.getPeer());
    }
    multimap.clear();
  }

}
