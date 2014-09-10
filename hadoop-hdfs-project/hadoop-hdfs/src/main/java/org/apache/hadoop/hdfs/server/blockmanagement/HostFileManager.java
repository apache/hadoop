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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Iterators;
import com.google.common.collect.Multimap;
import com.google.common.collect.UnmodifiableIterator;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.util.HostsFileReader;

import javax.annotation.Nullable;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;

/**
 * This class manages the include and exclude files for HDFS.
 * <p/>
 * These files control which DataNodes the NameNode expects to see in the
 * cluster.  Loosely speaking, the include file, if it exists and is not
 * empty, is a list of everything we expect to see.  The exclude file is
 * a list of everything we want to ignore if we do see it.
 * <p/>
 * Entries may or may not specify a port.  If they don't, we consider
 * them to apply to every DataNode on that host. The code canonicalizes the
 * entries into IP addresses.
 * <p/>
 * <p/>
 * The code ignores all entries that the DNS fails to resolve their IP
 * addresses. This is okay because by default the NN rejects the registrations
 * of DNs when it fails to do a forward and reverse lookup. Note that DNS
 * resolutions are only done during the loading time to minimize the latency.
 */
class HostFileManager {
  private static final Log LOG = LogFactory.getLog(HostFileManager.class);
  private HostSet includes = new HostSet();
  private HostSet excludes = new HostSet();

  private static HostSet readFile(String type, String filename)
          throws IOException {
    HostSet res = new HostSet();
    if (!filename.isEmpty()) {
      HashSet<String> entrySet = new HashSet<String>();
      HostsFileReader.readFileToSet(type, filename, entrySet);
      for (String str : entrySet) {
        InetSocketAddress addr = parseEntry(type, filename, str);
        if (addr != null) {
          res.add(addr);
        }
      }
    }
    return res;
  }

  @VisibleForTesting
  static InetSocketAddress parseEntry(String type, String fn, String line) {
    try {
      URI uri = new URI("dummy", line, null, null, null);
      int port = uri.getPort() == -1 ? 0 : uri.getPort();
      InetSocketAddress addr = new InetSocketAddress(uri.getHost(), port);
      if (addr.isUnresolved()) {
        LOG.warn(String.format("Failed to resolve address `%s` in `%s`. " +
                "Ignoring in the %s list.", line, fn, type));
        return null;
      }
      return addr;
    } catch (URISyntaxException e) {
      LOG.warn(String.format("Failed to parse `%s` in `%s`. " + "Ignoring in " +
              "the %s list.", line, fn, type));
    }
    return null;
  }

  static InetSocketAddress resolvedAddressFromDatanodeID(DatanodeID id) {
    return new InetSocketAddress(id.getIpAddr(), id.getXferPort());
  }

  synchronized HostSet getIncludes() {
    return includes;
  }

  synchronized HostSet getExcludes() {
    return excludes;
  }

  // If the includes list is empty, act as if everything is in the
  // includes list.
  synchronized boolean isIncluded(DatanodeID dn) {
    return includes.isEmpty() || includes.match
            (resolvedAddressFromDatanodeID(dn));
  }

  synchronized boolean isExcluded(DatanodeID dn) {
    return excludes.match(resolvedAddressFromDatanodeID(dn));
  }

  synchronized boolean hasIncludes() {
    return !includes.isEmpty();
  }

  void refresh(String includeFile, String excludeFile) throws IOException {
    HostSet newIncludes = readFile("included", includeFile);
    HostSet newExcludes = readFile("excluded", excludeFile);
    synchronized (this) {
      includes = newIncludes;
      excludes = newExcludes;
    }
  }

  /**
   * The HostSet allows efficient queries on matching wildcard addresses.
   * <p/>
   * For InetSocketAddress A and B with the same host address,
   * we define a partial order between A and B, A <= B iff A.getPort() == B
   * .getPort() || B.getPort() == 0.
   */
  static class HostSet implements Iterable<InetSocketAddress> {
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
}
