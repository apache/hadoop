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
package org.apache.hadoop.hdfs.server.blockmanagement;


import org.apache.hadoop.thirdparty.com.google.common.base.Preconditions;
import org.apache.hadoop.thirdparty.com.google.common.collect.HashMultimap;
import org.apache.hadoop.thirdparty.com.google.common.collect.Multimap;
import org.apache.hadoop.thirdparty.com.google.common.collect.UnmodifiableIterator;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;


/**
 * The HostSet allows efficient queries on matching wildcard addresses.
 * <p>
 * For InetSocketAddress A and B with the same host address,
 * we define a partial order between A and B, A &lt;= B iff A.getPort() == B
 * .getPort() || B.getPort() == 0.
 */
public class HostSet implements Iterable<InetSocketAddress> {
  // Host -> lists of ports
  private final Multimap<InetAddress, Integer> addrs = HashMultimap.create();

  /**
   * The function that checks whether there exists an entry foo in the set
   * so that foo &lt;= addr.
   */
  boolean matchedBy(InetSocketAddress addr) {
    Collection<Integer> ports = addrs.get(addr.getAddress());
    return addr.getPort() == 0 ? !ports.isEmpty() : ports.contains(addr
        .getPort());
  }

  /**
   * The function that checks whether there exists an entry foo in the set
   * so that addr &lt;= foo.
   */
  boolean match(InetSocketAddress addr) {
    int port = addr.getPort();
    Collection<Integer> ports = addrs.get(addr.getAddress());
    boolean exactMatch = ports.contains(port);
    boolean genericMatch = ports.contains(0);
    return exactMatch || genericMatch;
  }

  boolean isEmpty() {
    return addrs.isEmpty();
  }

  int size() {
    return addrs.size();
  }

  void add(InetSocketAddress addr) {
    Preconditions.checkArgument(!addr.isUnresolved());
    addrs.put(addr.getAddress(), addr.getPort());
  }

  @Override
  public Iterator<InetSocketAddress> iterator() {
    return new UnmodifiableIterator<InetSocketAddress>() {
      private final Iterator<Map.Entry<InetAddress,
          Integer>> it = addrs.entries().iterator();

      @Override
      public boolean hasNext() {
        return it.hasNext();
      }

      @Override
      public InetSocketAddress next() {
        Map.Entry<InetAddress, Integer> e = it.next();
        return new InetSocketAddress(e.getKey(), e.getValue());
      }
    };
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("HostSet(");
    Iterator<InetSocketAddress> iter = iterator();
    String sep = "";
    while (iter.hasNext()) {
      InetSocketAddress addr = iter.next();
      sb.append(sep);
      sb.append(addr.getAddress().getHostAddress());
      sb.append(':');
      sb.append(addr.getPort());
      sep = ",";
    }
    return sb.append(')').toString();
  }
}
