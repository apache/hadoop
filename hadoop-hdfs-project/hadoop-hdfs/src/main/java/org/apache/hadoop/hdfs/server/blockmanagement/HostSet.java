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

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Iterators;
import com.google.common.collect.Multimap;
import com.google.common.collect.UnmodifiableIterator;

import javax.annotation.Nullable;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;


/**
 * The HostSet allows efficient queries on matching wildcard addresses.
 * <p/>
 * For InetSocketAddress A and B with the same host address,
 * we define a partial order between A and B, A <= B iff A.getPort() == B
 * .getPort() || B.getPort() == 0.
 */
public class HostSet implements Iterable<InetSocketAddress> {
  // Host -> lists of ports
  private final Multimap<InetAddress, Integer> addrs = HashMultimap.create();

  /**
   * The function that checks whether there exists an entry foo in the set
   * so that foo <= addr.
   */
  boolean matchedBy(InetSocketAddress addr) {
    Collection<Integer> ports = addrs.get(addr.getAddress());
    return addr.getPort() == 0 ? !ports.isEmpty() : ports.contains(addr
        .getPort());
  }

  /**
   * The function that checks whether there exists an entry foo in the set
   * so that addr <= foo.
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
    Joiner.on(",").appendTo(sb, Iterators.transform(iterator(),
        new Function<InetSocketAddress, String>() {
          @Override
          public String apply(@Nullable InetSocketAddress addr) {
            assert addr != null;
            return addr.getAddress().getHostAddress() + ":" + addr.getPort();
          }
        }));
    return sb.append(")").toString();
  }
}
