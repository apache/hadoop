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

import java.net.Socket;
import java.net.SocketAddress;

import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;

import java.io.IOException;
import com.google.common.base.Preconditions;
import com.google.common.collect.LinkedListMultimap;
import org.apache.commons.logging.Log;
import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Daemon;
import org.apache.hadoop.util.StringUtils;

/**
 * A cache of input stream sockets to Data Node.
 */
class SocketCache {
  private static final Log LOG = LogFactory.getLog(SocketCache.class);
  private Daemon daemon;
  /** A map for per user per datanode. */
  private static LinkedListMultimap<SocketAddress, SocketProp> multimap =
    LinkedListMultimap.create();
  private static int capacity;
  private static long expiryPeriod;
  private static SocketCache scInstance = new SocketCache();

  private static class SocketProp {
    Socket s;
    long createTime;
    public SocketProp(Socket s)
    {
      this.s=s;
      this.createTime = System.currentTimeMillis();
    }

    public long getCreateTime() {
      return this.createTime;
    }

    public Socket getSocket() {
      return this.s;
    }
  }

  // capacity and expiryPeriod are only initialized once.
  private static boolean isInitedOnce() {
    if (capacity == 0 || expiryPeriod == 0) {
      return false;
    }
    return true;
  }

  public static synchronized SocketCache getInstance(int c, long e) {

    if (c == 0 || e == 0) {
      throw new IllegalStateException("Cannot initialize ZERO capacity " +
        "or expiryPeriod");
    }

    // capacity is only initialzied once
    if (isInitedOnce() == false) {
      capacity = c;
      expiryPeriod = e;
    } else if (capacity != c || expiryPeriod != e) {
      LOG.info("capacity and expiry periods already set to " + capacity +
        " and " + expiryPeriod + " respectively. Cannot set it to " + c +
        " and " + e);
    }

    return scInstance;
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
          SocketCache.this.run();
        } catch(InterruptedException e) {
          //noop
        } finally {
          SocketCache.this.clear();
        }
      }

      @Override
      public String toString() {
        return String.valueOf(SocketCache.this);
      }
    });
    daemon.start();
  }

  /**
   * Get a cached socket to the given address.
   * @param remote  Remote address the socket is connected to.
   * @return  A socket with unknown state, possibly closed underneath. Or null.
   */
  public synchronized Socket get(SocketAddress remote) {
    List<SocketProp> sockPropList = multimap.get(remote);
    if (sockPropList == null) {
      return null;
    }

    Iterator<SocketProp> iter = sockPropList.iterator();
    while (iter.hasNext()) {
      Socket candidate = iter.next().getSocket();
      iter.remove();
      if (!candidate.isClosed()) {
        return candidate;
      }
    }
    return null;
  }

  /**
   * Give an unused socket to the cache.
   * @param sock socket not used by anyone.
   */
  public synchronized void put(Socket sock) {

    Preconditions.checkNotNull(sock);
    startExpiryDaemon();

    SocketAddress remoteAddr = sock.getRemoteSocketAddress();
    if (remoteAddr == null) {
      LOG.warn("Cannot cache (unconnected) socket with no remote address: " +
               sock);
      IOUtils.closeSocket(sock);
      return;
    }

    if (capacity == multimap.size()) {
      evictOldest();
    }
    multimap.put(remoteAddr, new SocketProp (sock));
  }

  public synchronized int size() {
    return multimap.size();
  }

  /**
   * Evict and close sockets older than expiry period from the cache.
   */
  private synchronized void evictExpired(long expiryPeriod) {
    while (multimap.size() != 0) {
      Iterator<Entry<SocketAddress, SocketProp>> iter =
        multimap.entries().iterator();
      Entry<SocketAddress, SocketProp> entry = iter.next();

      // if oldest socket expired, remove it
      if (entry == null || 
        System.currentTimeMillis() - entry.getValue().getCreateTime() < 
        expiryPeriod) {
        break;
      }
      iter.remove();
      Socket sock = entry.getValue().getSocket();
      IOUtils.closeSocket(sock);
    }
  }

  /**
   * Evict the oldest entry in the cache.
   */
  private synchronized void evictOldest() {
    Iterator<Entry<SocketAddress, SocketProp>> iter =
      multimap.entries().iterator();
    if (!iter.hasNext()) {
      throw new IllegalStateException("Cannot evict from empty cache!");
    }
    Entry<SocketAddress, SocketProp> entry = iter.next();
    iter.remove();
    Socket sock = entry.getValue().getSocket();
    IOUtils.closeSocket(sock);
  }

  /**
   * Periodically check in the cache and expire the entries
   * older than expiryPeriod minutes
   */
  private void run() throws InterruptedException {
    for(long lastExpiryTime = System.currentTimeMillis();
        !Thread.interrupted();
        Thread.sleep(expiryPeriod)) {
      final long elapsed = System.currentTimeMillis() - lastExpiryTime;
      if (elapsed >= expiryPeriod) {
        evictExpired(expiryPeriod);
        lastExpiryTime = System.currentTimeMillis();
      }
    }
    clear();
    throw new InterruptedException("Daemon Interrupted");
  }

  /**
   * Empty the cache, and close all sockets.
   */
  private synchronized void clear() {
    for (SocketProp sockProp : multimap.values()) {
      IOUtils.closeSocket(sockProp.getSocket());
    }
    multimap.clear();
  }

}
