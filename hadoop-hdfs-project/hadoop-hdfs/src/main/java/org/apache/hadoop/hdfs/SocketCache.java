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

import com.google.common.base.Preconditions;
import com.google.common.collect.LinkedListMultimap;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hdfs.protocol.datatransfer.IOStreamPair;
import org.apache.hadoop.io.IOUtils;

/**
 * A cache of sockets.
 */
class SocketCache {
  static final Log LOG = LogFactory.getLog(SocketCache.class);

  private final LinkedListMultimap<SocketAddress, SocketAndStreams> multimap;
  private final int capacity;

  /**
   * Create a SocketCache with the given capacity.
   * @param capacity  Max cache size.
   */
  public SocketCache(int capacity) {
    multimap = LinkedListMultimap.create();
    this.capacity = capacity;
    if (capacity <= 0) {
      LOG.debug("SocketCache disabled in configuration.");
    }
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
    
    List<SocketAndStreams> socklist = multimap.get(remote);
    if (socklist == null) {
      return null;
    }

    Iterator<SocketAndStreams> iter = socklist.iterator();
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
    SocketAndStreams s = new SocketAndStreams(sock, ioStreams);
    if (capacity <= 0) {
      // Cache disabled.
      s.close();
      return;
    }
    
    Preconditions.checkNotNull(sock);

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
    multimap.put(remoteAddr, new SocketAndStreams(sock, ioStreams));
  }

  public synchronized int size() {
    return multimap.size();
  }

  /**
   * Evict the oldest entry in the cache.
   */
  private synchronized void evictOldest() {
    Iterator<Entry<SocketAddress, SocketAndStreams>> iter =
      multimap.entries().iterator();
    if (!iter.hasNext()) {
      throw new IllegalStateException("Cannot evict from empty cache!");
    }
    Entry<SocketAddress, SocketAndStreams> entry = iter.next();
    iter.remove();
    SocketAndStreams s = entry.getValue();
    s.close();
  }

  /**
   * Empty the cache, and close all sockets.
   */
  public synchronized void clear() {
    for (SocketAndStreams s : multimap.values()) {
      s.close();
    }
    multimap.clear();
  }

  @Override
  protected void finalize() {
    clear();
  }
  
  @InterfaceAudience.Private
  static class SocketAndStreams implements Closeable {
    public final Socket sock;
    public final IOStreamPair ioStreams;
    
    public SocketAndStreams(Socket s, IOStreamPair ioStreams) {
      this.sock = s;
      this.ioStreams = ioStreams;
    }
    
    @Override
    public void close() {
      if (ioStreams != null) { 
        IOUtils.closeStream(ioStreams.in);
        IOUtils.closeStream(ioStreams.out);
      }
      IOUtils.closeSocket(sock);
    }
  }

}
