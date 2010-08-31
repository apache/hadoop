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
package org.apache.hadoop.hbase.master;

import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.apache.commons.lang.NotImplementedException;
import org.apache.hadoop.hbase.HServerInfo;

/**
 * Class to hold dead servers list and utility querying dead server list.
 */
public class DeadServer implements Set<String> {
  /**
   * Set of known dead servers.  On znode expiration, servers are added here.
   * This is needed in case of a network partitioning where the server's lease
   * expires, but the server is still running. After the network is healed,
   * and it's server logs are recovered, it will be told to call server startup
   * because by then, its regions have probably been reassigned.
   */
  private final Set<String> deadServers = new HashSet<String>();


  /**
   * @param serverName
   * @return true if server is dead
   */
  public boolean isDeadServer(final String serverName) {
    return isDeadServer(serverName, false);
  }

  /**
   * @param serverName Servername as either <code>host:port</code> or
   * <code>host,port,startcode</code>.
   * @param hostAndPortOnly True if <code>serverName</code> is host and
   * port only (<code>host:port</code>) and if so, then we do a prefix compare
   * (ignoring start codes) looking for dead server.
   * @return true if server is dead
   */
  boolean isDeadServer(final String serverName, final boolean hostAndPortOnly) {
    return HServerInfo.isServer(this, serverName, hostAndPortOnly);
  }

  public synchronized Set<String> clone() {
    Set<String> clone = new HashSet<String>(this.deadServers.size());
    clone.addAll(this.deadServers);
    return clone;
  }

  public synchronized int size() {
    return deadServers.size();
  }

  public synchronized boolean isEmpty() {
    return deadServers.isEmpty();
  }

  public synchronized boolean contains(Object o) {
    return deadServers.contains(o);
  }

  public Iterator<String> iterator() {
    return this.deadServers.iterator();
  }

  public synchronized Object[] toArray() {
    return deadServers.toArray();
  }

  public synchronized <T> T[] toArray(T[] a) {
    return deadServers.toArray(a);
  }

  public synchronized boolean add(String e) {
    return deadServers.add(e);
  }

  public synchronized boolean remove(Object o) {
    return deadServers.remove(o);
  }

  public synchronized boolean containsAll(Collection<?> c) {
    return deadServers.containsAll(c);
  }

  public synchronized boolean addAll(Collection<? extends String> c) {
    return deadServers.addAll(c);
  }

  public synchronized boolean retainAll(Collection<?> c) {
    return deadServers.retainAll(c);
  }

  public synchronized boolean removeAll(Collection<?> c) {
    return deadServers.removeAll(c);
  }

  public synchronized void clear() {
    throw new NotImplementedException();
  }

  public synchronized boolean equals(Object o) {
    return deadServers.equals(o);
  }

  public synchronized int hashCode() {
    return deadServers.hashCode();
  }

  public synchronized String toString() {
    return this.deadServers.toString();
  }
}