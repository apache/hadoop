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

import java.io.Closeable;
import java.net.Socket;
import java.net.SocketAddress;

import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;

import java.io.IOException;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.LinkedListMultimap;
import org.apache.commons.logging.Log;
import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hdfs.protocol.datatransfer.IOStreamPair;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Daemon;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Time;

/**
 * A cache of input stream sockets to Data Node.
 */
class SocketCache {
  private static final Log LOG = LogFactory.getLog(SocketCache.class);

  @InterfaceAudience.Private
  static class SocketAndStreams implements Closeable {
    public final Socket sock;
    public final IOStreamPair ioStreams;
    long createTime;
    
    public SocketAndStreams(Socket s, IOStreamPair ioStreams) {
      this.sock = s;
      this.ioStreams = ioStreams;
      this.createTime = Time.monotonicNow();
    }
    
    @Override
    public void close() {
      if (ioStreams != null) { 
        IOUtils.closeStream(ioStreams.in);
        IOUtils.closeStream(ioStreams.out);
      }
      IOUtils.closeSocket(sock);
    }

    public long getCreateTime() {
      return this.createTime;
    }
  }

  private Daemon daemon;
  /** A map for per user per datanode. */
  private static LinkedListMultimap<SocketAddress, SocketAndStreams> multimap =
    LinkedListMultimap.create();
  private static int capacity;
  private static long expiryPeriod;
  private static SocketCache scInstance = new SocketCache();
  private static boolean isInitedOnce = false;
 
  public static synchronized SocketCache getInstance(int c, long e) {
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
  public synchronized SocketAndStreams get(SocketAddress remote) {

    if (capacity <= 0) { // disabled
      return null;
    }

    List<SocketAndStreams> sockStreamList = multimap.get(remote);
    if (sockStreamList == null) {
      return null;
    }

    Iterator<SocketAndStreams> iter = sockStreamList.iterator();
    while (iter.hasNext()) {
      SocketAndStreams candidate = iter.next();
      iter.remove();
      if (!candidate.sock.isClosed()) {
        return candidate;
      }
    }
    return null;
  }

  /**
   * Give an unused socket to the cache.
   * @param sock socket not used by anyone.
   */
  public synchronized void put(Socket sock, IOStreamPair ioStreams) {

    Preconditions.checkNotNull(sock);
    SocketAndStreams s = new SocketAndStreams(sock, ioStreams);
    if (capacity <= 0) {
      // Cache disabled.
      s.close();
      return;
    }
 
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
    multimap.put(remoteAddr, s);
  }

  public synchronized int size() {
    return multimap.size();
  }

  /**
   * Evict and close sockets older than expiry period from the cache.
   */
  private synchronized void evictExpired(long expiryPeriod) {
    while (multimap.size() != 0) {
      Iterator<Entry<SocketAddress, SocketAndStreams>> iter =
        multimap.entries().iterator();
      Entry<SocketAddress, SocketAndStreams> entry = iter.next();
      // if oldest socket expired, remove it
      if (entry == null || 
        Time.monotonicNow() - entry.getValue().getCreateTime() < 
        expiryPeriod) {
        break;
      }
      iter.remove();
      SocketAndStreams s = entry.getValue();
      s.close();
    }
  }

  /**
   * Evict the oldest entry in the cache.
   */
  private synchronized void evictOldest() {
    Iterator<Entry<SocketAddress, SocketAndStreams>> iter =
      multimap.entries().iterator();
    if (!iter.hasNext()) {
      throw new IllegalStateException("Cannot evict from empty cache! " +
        "capacity: " + capacity);
    }
    Entry<SocketAddress, SocketAndStreams> entry = iter.next();
    iter.remove();
    SocketAndStreams s = entry.getValue();
    s.close();
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
  protected synchronized void clear() {
    for (SocketAndStreams sockAndStream : multimap.values()) {
      sockAndStream.close();
    }
    multimap.clear();
  }

}
