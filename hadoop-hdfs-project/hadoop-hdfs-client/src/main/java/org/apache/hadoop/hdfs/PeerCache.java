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

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.LinkedListMultimap;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hdfs.net.Peer;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.util.IOUtilsClient;
import org.apache.hadoop.util.Daemon;
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A cache of input stream sockets to Data Node.
 */
@InterfaceStability.Unstable
@InterfaceAudience.Private
@VisibleForTesting
public class PeerCache {
  private static final Logger LOG = LoggerFactory.getLogger(PeerCache.class);

  private static class Key {
    final DatanodeID dnID;
    final boolean isDomain;

    Key(DatanodeID dnID, boolean isDomain) {
      this.dnID = dnID;
      this.isDomain = isDomain;
    }

    @Override
    public boolean equals(Object o) {
      if (!(o instanceof Key)) {
        return false;
      }
      Key other = (Key)o;
      return dnID.equals(other.dnID) && isDomain == other.isDomain;
    }

    @Override
    public int hashCode() {
      return dnID.hashCode() ^ (isDomain ? 1 : 0);
    }
  }

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
  private final LinkedListMultimap<Key, Value> multimap =
      LinkedListMultimap.create();
  private final int capacity;
  private final long expiryPeriod;

  public PeerCache(int c, long e) {
    this.capacity = c;
    this.expiryPeriod = e;

    if (capacity == 0 ) {
      LOG.debug("SocketCache disabled.");
    } else if (expiryPeriod == 0) {
      throw new IllegalStateException("Cannot initialize expiryPeriod to " +
         expiryPeriod + " when cache is enabled.");
    }
  }

  private boolean isDaemonStarted() {
    return daemon != null;
  }

  private synchronized void startExpiryDaemon() {
    // start daemon only if not already started
    if (isDaemonStarted()) {
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
   * @param isDomain     Whether to retrieve a DomainPeer or not.
   *
   * @return             An open Peer connected to the DN, or null if none
   *                     was found.
   */
  public Peer get(DatanodeID dnId, boolean isDomain) {

    if (capacity <= 0) { // disabled
      return null;
    }
    return getInternal(dnId, isDomain);
  }

  private synchronized Peer getInternal(DatanodeID dnId, boolean isDomain) {
    List<Value> sockStreamList = multimap.get(new Key(dnId, isDomain));
    if (sockStreamList == null) {
      return null;
    }

    Iterator<Value> iter = sockStreamList.iterator();
    while (iter.hasNext()) {
      Value candidate = iter.next();
      iter.remove();
      long ageMs = Time.monotonicNow() - candidate.getTime();
      Peer peer = candidate.getPeer();
      if (ageMs >= expiryPeriod) {
        try {
          peer.close();
        } catch (IOException e) {
          LOG.warn("got IOException closing stale peer " + peer +
                ", which is " + ageMs + " ms old");
        }
      } else if (!peer.isClosed()) {
        return peer;
      }
    }
    return null;
  }

  /**
   * Give an unused socket to the cache.
   */
  public void put(DatanodeID dnId, Peer peer) {
    Preconditions.checkNotNull(dnId);
    Preconditions.checkNotNull(peer);
    if (peer.isClosed()) return;
    if (capacity <= 0) {
      // Cache disabled.
      IOUtilsClient.cleanupWithLogger(LOG, peer);
      return;
    }
    putInternal(dnId, peer);
  }

  private synchronized void putInternal(DatanodeID dnId, Peer peer) {
    startExpiryDaemon();

    if (capacity == multimap.size()) {
      evictOldest();
    }
    multimap.put(new Key(dnId, peer.getDomainSocket() != null),
        new Value(peer, Time.monotonicNow()));
  }

  public synchronized int size() {
    return multimap.size();
  }

  /**
   * Evict and close sockets older than expiry period from the cache.
   */
  private synchronized void evictExpired(long expiryPeriod) {
    while (multimap.size() != 0) {
      Iterator<Entry<Key, Value>> iter =
          multimap.entries().iterator();
      Entry<Key, Value> entry = iter.next();
      // if oldest socket expired, remove it
      if (entry == null ||
          Time.monotonicNow() - entry.getValue().getTime() < expiryPeriod) {
        break;
      }
      IOUtilsClient.cleanupWithLogger(LOG, entry.getValue().getPeer());
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
    Iterator<Entry<Key, Value>> iter = multimap.entries().iterator();
    if (!iter.hasNext()) {
      throw new IllegalStateException("Cannot evict from empty cache! " +
        "capacity: " + capacity);
    }
    Entry<Key, Value> entry = iter.next();
    IOUtilsClient.cleanupWithLogger(LOG, entry.getValue().getPeer());
    iter.remove();
  }

  /**
   * Periodically check in the cache and expire the entries older than
   * expiryPeriod minutes.
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
      IOUtilsClient.cleanupWithLogger(LOG, value.getPeer());
    }
    multimap.clear();
  }

  @VisibleForTesting
  void close() {
    clear();
    if (daemon != null) {
      daemon.interrupt();
      try {
        daemon.join();
      } catch (InterruptedException e) {
        throw new RuntimeException("failed to join thread");
      }
    }
    daemon = null;
  }
}
