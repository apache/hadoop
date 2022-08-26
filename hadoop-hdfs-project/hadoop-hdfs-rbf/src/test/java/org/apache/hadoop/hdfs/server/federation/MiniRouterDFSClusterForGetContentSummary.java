/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hdfs.server.federation;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.server.federation.resolver.ActiveNamenodeResolver;
import org.apache.hadoop.hdfs.server.federation.resolver.FileSubclusterResolver;
import org.apache.hadoop.hdfs.server.federation.resolver.MultipleDestinationMountTableResolver;
import org.apache.hadoop.hdfs.server.federation.store.records.MountTable;

import java.io.IOException;
import java.util.Collections;
import java.util.Map.Entry;

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_HA_NAMENODE_ID_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMESERVICE_ID;
import static org.apache.hadoop.hdfs.server.federation.router.RBFConfigKeys.DFS_ROUTER_ADMIN_ADDRESS_KEY;
import static org.apache.hadoop.hdfs.server.federation.router.RBFConfigKeys.DFS_ROUTER_ADMIN_BIND_HOST_KEY;
import static org.apache.hadoop.hdfs.server.federation.router.RBFConfigKeys.DFS_ROUTER_CACHE_TIME_TO_LIVE_MS;
import static org.apache.hadoop.hdfs.server.federation.router.RBFConfigKeys.DFS_ROUTER_DEFAULT_NAMESERVICE;
import static org.apache.hadoop.hdfs.server.federation.router.RBFConfigKeys.DFS_ROUTER_HANDLER_COUNT_KEY;
import static org.apache.hadoop.hdfs.server.federation.router.RBFConfigKeys.DFS_ROUTER_HEARTBEAT_INTERVAL_MS;
import static org.apache.hadoop.hdfs.server.federation.router.RBFConfigKeys.DFS_ROUTER_HTTPS_ADDRESS_KEY;
import static org.apache.hadoop.hdfs.server.federation.router.RBFConfigKeys.DFS_ROUTER_HTTP_ADDRESS_KEY;
import static org.apache.hadoop.hdfs.server.federation.router.RBFConfigKeys.DFS_ROUTER_HTTP_BIND_HOST_KEY;
import static org.apache.hadoop.hdfs.server.federation.router.RBFConfigKeys.DFS_ROUTER_MONITOR_NAMENODE;
import static org.apache.hadoop.hdfs.server.federation.router.RBFConfigKeys.DFS_ROUTER_RPC_ADDRESS_KEY;
import static org.apache.hadoop.hdfs.server.federation.router.RBFConfigKeys.DFS_ROUTER_RPC_BIND_HOST_KEY;
import static org.apache.hadoop.hdfs.server.federation.router.RBFConfigKeys.DFS_ROUTER_SAFEMODE_ENABLE;
import static org.apache.hadoop.hdfs.server.federation.router.RBFConfigKeys.FEDERATION_FILE_RESOLVER_CLIENT_CLASS;
import static org.apache.hadoop.hdfs.server.federation.router.RBFConfigKeys.FEDERATION_NAMENODE_RESOLVER_CLIENT_CLASS;

public class MiniRouterDFSClusterForGetContentSummary extends MiniRouterDFSCluster {

  public MiniRouterDFSClusterForGetContentSummary(boolean ha, int numNameservices) {
    super(ha, numNameservices);
  }

  /**
   * Generate the configuration for a Router.
   *
   * @param nsId Nameservice identifier.
   * @param nnId Namenode identifier.
   * @return New configuration for a Router.
   */
  public Configuration generateRouterConfiguration(String nsId, String nnId) {

    Configuration conf = new HdfsConfiguration(false);
    conf.addResource(generateNamenodeConfiguration(nsId));

    conf.setInt(DFS_ROUTER_HANDLER_COUNT_KEY, 10);
    conf.set(DFS_ROUTER_RPC_ADDRESS_KEY, "127.0.0.1:0");
    conf.set(DFS_ROUTER_RPC_BIND_HOST_KEY, "0.0.0.0");

    conf.set(DFS_ROUTER_ADMIN_ADDRESS_KEY, "127.0.0.1:0");
    conf.set(DFS_ROUTER_ADMIN_BIND_HOST_KEY, "0.0.0.0");

    conf.set(DFS_ROUTER_HTTP_ADDRESS_KEY, "127.0.0.1:0");
    conf.set(DFS_ROUTER_HTTPS_ADDRESS_KEY, "127.0.0.1:0");
    conf.set(DFS_ROUTER_HTTP_BIND_HOST_KEY, "0.0.0.0");

    conf.set(DFS_ROUTER_DEFAULT_NAMESERVICE, nameservices.get(0));
    conf.setLong(DFS_ROUTER_HEARTBEAT_INTERVAL_MS, heartbeatInterval);
    conf.setLong(DFS_ROUTER_CACHE_TIME_TO_LIVE_MS, cacheFlushInterval);

    // Use mock resolver classes
    conf.setClass(FEDERATION_NAMENODE_RESOLVER_CLIENT_CLASS,
                  MockResolver.class, ActiveNamenodeResolver.class);
    conf.setClass(FEDERATION_FILE_RESOLVER_CLIENT_CLASS,
                  MultipleDestinationMountTableResolver.class, FileSubclusterResolver.class);

    // Disable safemode on startup
    conf.setBoolean(DFS_ROUTER_SAFEMODE_ENABLE, false);

    // Set the nameservice ID for the default NN monitor
    conf.set(DFS_NAMESERVICE_ID, nsId);
    if (nnId != null) {
      conf.set(DFS_HA_NAMENODE_ID_KEY, nnId);
    }

    // Namenodes to monitor
    StringBuilder sb = new StringBuilder();
    for (String ns : this.nameservices) {
      for (NamenodeContext context : getNamenodes(ns)) {
        String suffix = context.getConfSuffix();
        if (sb.length() != 0) {
          sb.append(",");
        }
        sb.append(suffix);
      }
    }
    conf.set(DFS_ROUTER_MONITOR_NAMENODE, sb.toString());

    // Add custom overrides if available
    if (this.routerOverrides != null) {
      for (Entry<String, String> entry : this.routerOverrides) {
        String confKey = entry.getKey();
        String confValue = entry.getValue();
        conf.set(confKey, confValue);
      }
    }
    return conf;
  }

  /**
   * <ul>
   * <li>/ -> [ns0->/].
   * <li>/nso -> ns0->/target-ns0.
   * <li>/ns1 -> ns1->/target-ns1.
   * </ul>
   */
  public void installMockLocations() throws IOException {
    for (RouterContext r : routers) {
      MultipleDestinationMountTableResolver resolver =
          (MultipleDestinationMountTableResolver) r.getRouter().getSubclusterResolver();
      // create table entries
      for (String nsId : nameservices) {
        // Direct path
        String routerPath = getFederatedPathForNS(nsId);
        String nnPath = getNamenodePathForNS(nsId);
        MountTable entry = MountTable.newInstance(routerPath, Collections.singletonMap(nsId, nnPath));
        resolver.addEntry(entry);
      }

      // Root path points to both first nameservice
      String ns0 = nameservices.get(0);
      MountTable entry = MountTable.newInstance("/", Collections.singletonMap(ns0, "/"));
      resolver.addEntry(entry);
    }
  }
}
