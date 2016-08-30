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
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.util.HostsFileReader;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashSet;

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
public class HostFileManager extends HostConfigManager {
  private static final Log LOG = LogFactory.getLog(HostFileManager.class);
  private Configuration conf;
  private HostSet includes = new HostSet();
  private HostSet excludes = new HostSet();

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
    refresh(conf.get(DFSConfigKeys.DFS_HOSTS, ""),
        conf.get(DFSConfigKeys.DFS_HOSTS_EXCLUDE, ""));
  }
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

  @Override
  public synchronized HostSet getIncludes() {
    return includes;
  }

  @Override
  public synchronized HostSet getExcludes() {
    return excludes;
  }

  // If the includes list is empty, act as if everything is in the
  // includes list.
  @Override
  public synchronized boolean isIncluded(DatanodeID dn) {
    return includes.isEmpty() || includes.match(dn.getResolvedAddress());
  }

  @Override
  public synchronized boolean isExcluded(DatanodeID dn) {
    return isExcluded(dn.getResolvedAddress());
  }

  private boolean isExcluded(InetSocketAddress address) {
    return excludes.match(address);
  }

  @Override
  public synchronized String getUpgradeDomain(final DatanodeID dn) {
    // The include/exclude files based config doesn't support upgrade domain
    // config.
    return null;
  }

  @Override
  public long getMaintenanceExpirationTimeInMS(DatanodeID dn) {
    // The include/exclude files based config doesn't support maintenance mode.
    return 0;
  }

  /**
   * Read the includes and excludes lists from the named files.  Any previous
   * includes and excludes lists are discarded.
   * @param includeFile the path to the new includes list
   * @param excludeFile the path to the new excludes list
   * @throws IOException thrown if there is a problem reading one of the files
   */
  private void refresh(String includeFile, String excludeFile)
      throws IOException {
    HostSet newIncludes = readFile("included", includeFile);
    HostSet newExcludes = readFile("excluded", excludeFile);

    refresh(newIncludes, newExcludes);
  }

  /**
   * Set the includes and excludes lists by the new HostSet instances. The
   * old instances are discarded.
   * @param newIncludes the new includes list
   * @param newExcludes the new excludes list
   */
  @VisibleForTesting
  void refresh(HostSet newIncludes, HostSet newExcludes) {
    synchronized (this) {
      includes = newIncludes;
      excludes = newExcludes;
    }
  }
}
