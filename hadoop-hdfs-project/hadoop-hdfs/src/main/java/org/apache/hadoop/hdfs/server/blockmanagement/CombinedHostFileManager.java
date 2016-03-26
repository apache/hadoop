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
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.UnmodifiableIterator;
import com.google.common.collect.Iterables;
import com.google.common.collect.Collections2;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.protocol.DatanodeAdminProperties;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo.AdminStates;

import java.io.IOException;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import com.google.common.base.Predicate;

import org.apache.hadoop.hdfs.util.CombinedHostsFileReader;

/**
 * This class manages datanode configuration using a json file.
 * Please refer to {@link CombinedHostsFileReader} for the json format.
 * <p/>
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
public class CombinedHostFileManager extends HostConfigManager {
  private static final Logger LOG = LoggerFactory.getLogger(
      CombinedHostFileManager.class);
  private Configuration conf;
  private HostProperties hostProperties = new HostProperties();

  static class HostProperties {
    private Multimap<InetAddress, DatanodeAdminProperties> allDNs =
        HashMultimap.create();
    // optimization. If every node in the file isn't in service, it implies
    // any node is allowed to register with nn. This is equivalent to having
    // an empty "include" file.
    private boolean emptyInServiceNodeLists = true;
    synchronized void add(InetAddress addr,
        DatanodeAdminProperties properties) {
      allDNs.put(addr, properties);
      if (properties.getAdminState().equals(
          AdminStates.NORMAL)) {
        emptyInServiceNodeLists = false;
      }
    }

    // If the includes list is empty, act as if everything is in the
    // includes list.
    synchronized boolean isIncluded(final InetSocketAddress address) {
      return emptyInServiceNodeLists || Iterables.any(
          allDNs.get(address.getAddress()),
          new Predicate<DatanodeAdminProperties>() {
            public boolean apply(DatanodeAdminProperties input) {
              return input.getPort() == 0 ||
                  input.getPort() == address.getPort();
            }
          });
    }

    synchronized boolean isExcluded(final InetSocketAddress address) {
      return Iterables.any(allDNs.get(address.getAddress()),
          new Predicate<DatanodeAdminProperties>() {
            public boolean apply(DatanodeAdminProperties input) {
              return input.getAdminState().equals(
                  AdminStates.DECOMMISSIONED) &&
                  (input.getPort() == 0 ||
                      input.getPort() == address.getPort());
            }
          });
    }

    synchronized String getUpgradeDomain(final InetSocketAddress address) {
      Iterable<DatanodeAdminProperties> datanode = Iterables.filter(
          allDNs.get(address.getAddress()),
          new Predicate<DatanodeAdminProperties>() {
            public boolean apply(DatanodeAdminProperties input) {
              return (input.getPort() == 0 ||
                  input.getPort() == address.getPort());
            }
          });
      return datanode.iterator().hasNext() ?
          datanode.iterator().next().getUpgradeDomain() : null;
    }

    Iterable<InetSocketAddress> getIncludes() {
      return new Iterable<InetSocketAddress>() {
        @Override
        public Iterator<InetSocketAddress> iterator() {
            return new HostIterator(allDNs.entries());
        }
      };
    }

    Iterable<InetSocketAddress> getExcludes() {
      return new Iterable<InetSocketAddress>() {
        @Override
        public Iterator<InetSocketAddress> iterator() {
          return new HostIterator(
              Collections2.filter(allDNs.entries(),
                  new Predicate<java.util.Map.Entry<InetAddress,
                      DatanodeAdminProperties>>() {
                    public boolean apply(java.util.Map.Entry<InetAddress,
                        DatanodeAdminProperties> entry) {
                      return entry.getValue().getAdminState().equals(
                          AdminStates.DECOMMISSIONED);
                    }
                  }
              ));
        }
      };
    }

    static class HostIterator extends UnmodifiableIterator<InetSocketAddress> {
      private final Iterator<Map.Entry<InetAddress,
          DatanodeAdminProperties>> it;
      public HostIterator(Collection<java.util.Map.Entry<InetAddress,
          DatanodeAdminProperties>> nodes) {
        this.it = nodes.iterator();
      }
      @Override
      public boolean hasNext() {
        return it.hasNext();
      }

      @Override
      public InetSocketAddress next() {
        Map.Entry<InetAddress, DatanodeAdminProperties> e = it.next();
        return new InetSocketAddress(e.getKey(), e.getValue().getPort());
      }
    }
  }

  @Override
  public Iterable<InetSocketAddress> getIncludes() {
    return hostProperties.getIncludes();
  }

  @Override
  public Iterable<InetSocketAddress> getExcludes() {
    return hostProperties.getExcludes();
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  @Override
  public Configuration getConf() {
    return conf;
  }

  @Override
  public void refresh() throws IOException {
    refresh(conf.get(DFSConfigKeys.DFS_HOSTS, ""));
  }
  private void refresh(final String hostsFile) throws IOException {
    HostProperties hostProps = new HostProperties();
    Set<DatanodeAdminProperties> all =
        CombinedHostsFileReader.readFile(hostsFile);
    for(DatanodeAdminProperties properties : all) {
      InetSocketAddress addr = parseEntry(hostsFile,
          properties.getHostName(), properties.getPort());
      if (addr != null) {
        hostProps.add(addr.getAddress(), properties);
      }
    }
    refresh(hostProps);
  }

  @VisibleForTesting
  static InetSocketAddress parseEntry(final String fn, final String hostName,
      final int port) {
    InetSocketAddress addr = new InetSocketAddress(hostName, port);
    if (addr.isUnresolved()) {
      LOG.warn("Failed to resolve {} in {}. ", hostName, fn);
      return null;
    }
    return addr;
  }

  @Override
  public synchronized boolean isIncluded(final DatanodeID dn) {
    return hostProperties.isIncluded(dn.getResolvedAddress());
  }

  @Override
  public synchronized boolean isExcluded(final DatanodeID dn) {
    return isExcluded(dn.getResolvedAddress());
  }

  private boolean isExcluded(final InetSocketAddress address) {
    return hostProperties.isExcluded(address);
  }

  @Override
  public synchronized String getUpgradeDomain(final DatanodeID dn) {
    return hostProperties.getUpgradeDomain(dn.getResolvedAddress());
  }

  /**
   * Set the properties lists by the new instances. The
   * old instance is discarded.
   * @param hostProperties the new properties list
   */
  @VisibleForTesting
  private void refresh(final HostProperties hostProperties) {
    synchronized (this) {
      this.hostProperties = hostProperties;
    }
  }
}
