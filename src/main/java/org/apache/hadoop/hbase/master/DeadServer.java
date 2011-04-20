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
import java.util.LinkedList;
import java.util.List;
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

  /** Maximum number of dead servers to keep track of */
  private final int maxDeadServers;

  /** Number of dead servers currently being processed */
  private int numProcessing;

  public DeadServer(int maxDeadServers) {
    super();
    this.maxDeadServers = maxDeadServers;
    this.numProcessing = 0;
  }

  /**
   * @param serverName
   * @return true if server is dead
   */
  public boolean isDeadServer(final String serverName) {
    return isDeadServer(serverName, false);
  }

  /**
   * A dead server that comes back alive has a different start code.
   * @param newServerName Servername as either <code>host:port</code> or
   * <code>host,port,startcode</code>.
   * @return true if this server was dead before and coming back alive again
   */
  public boolean cleanPreviousInstance(final String newServerName) {

    String serverAddress =
        HServerInfo.getServerNameLessStartCode(newServerName);
    for (String serverName: deadServers) {
      String deadServerAddress =
          HServerInfo.getServerNameLessStartCode(serverName);
      if (deadServerAddress.equals(serverAddress)) {
        remove(serverName);
        return true;
      }
    }
    return false;
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

  /**
   * Checks if there are currently any dead servers being processed by the
   * master.  Returns true if at least one region server is currently being
   * processed as dead.
   * @return true if any RS are being processed as dead
   */
  public boolean areDeadServersInProgress() {
    return numProcessing != 0;
  }

  public synchronized Set<String> clone() {
    Set<String> clone = new HashSet<String>(this.deadServers.size());
    clone.addAll(this.deadServers);
    return clone;
  }

  public synchronized boolean add(String e) {
    this.numProcessing++;
    return deadServers.add(e);
  }

  public synchronized void finish(String e) {
    this.numProcessing--;
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

  public synchronized boolean remove(Object o) {
    return this.deadServers.remove(o);
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