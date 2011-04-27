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
import org.apache.hadoop.hbase.ServerName;

/**
 * Class to hold dead servers list and utility querying dead server list.
 */
public class DeadServer implements Set<ServerName> {
  /**
   * Set of known dead servers.  On znode expiration, servers are added here.
   * This is needed in case of a network partitioning where the server's lease
   * expires, but the server is still running. After the network is healed,
   * and it's server logs are recovered, it will be told to call server startup
   * because by then, its regions have probably been reassigned.
   */
  private final Set<ServerName> deadServers = new HashSet<ServerName>();

  /** Number of dead servers currently being processed */
  private int numProcessing;

  public DeadServer() {
    super();
    this.numProcessing = 0;
  }

  /**
   * @param serverName Server name
   * @return true if server is dead
   */
  public boolean isDeadServer(final String serverName) {
    return isDeadServer(new ServerName(serverName));
  }

  /**
   * A dead server that comes back alive has a different start code.
   * @param newServerName Servername as either <code>host:port</code> or
   * <code>host,port,startcode</code>.
   * @return true if this server was dead before and coming back alive again
   */
  public boolean cleanPreviousInstance(final ServerName newServerName) {
    ServerName sn =
      ServerName.findServerWithSameHostnamePort(this.deadServers, newServerName);
    if (sn == null) return false;
    return this.deadServers.remove(sn);
  }

  /**
   * @param serverName
   * @return true if this server is on the dead servers list.
   */
  boolean isDeadServer(final ServerName serverName) {
    return this.deadServers.contains(serverName);
  }

  /**
   * @return True if we have a server with matching hostname and port.
   */
  boolean isDeadServerWithSameHostnamePort(final ServerName serverName) {
    return ServerName.findServerWithSameHostnamePort(this.deadServers,
      serverName) != null;
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

  public synchronized Set<ServerName> clone() {
    Set<ServerName> clone = new HashSet<ServerName>(this.deadServers.size());
    clone.addAll(this.deadServers);
    return clone;
  }

  public synchronized boolean add(ServerName e) {
    this.numProcessing++;
    return deadServers.add(e);
  }

  public synchronized void finish(ServerName e) {
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

  public Iterator<ServerName> iterator() {
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

  public synchronized boolean addAll(Collection<? extends ServerName> c) {
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