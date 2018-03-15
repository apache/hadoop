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
package org.apache.hadoop.hdfs.server.federation.resolver.order;

import static org.apache.hadoop.util.Time.monotonicNow;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.net.HostAndPort;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.security.PrivilegedAction;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.DatanodeReportType;
import org.apache.hadoop.hdfs.server.federation.resolver.PathLocation;
import org.apache.hadoop.hdfs.server.federation.router.Router;
import org.apache.hadoop.hdfs.server.federation.router.RouterRpcServer;
import org.apache.hadoop.hdfs.server.federation.store.MembershipStore;
import org.apache.hadoop.hdfs.server.federation.store.StateStoreService;
import org.apache.hadoop.hdfs.server.federation.store.protocol.GetNamenodeRegistrationsRequest;
import org.apache.hadoop.hdfs.server.federation.store.protocol.GetNamenodeRegistrationsResponse;
import org.apache.hadoop.hdfs.server.federation.store.records.MembershipState;
import org.apache.hadoop.hdfs.server.protocol.DatanodeStorageReport;
import org.apache.hadoop.ipc.RPC.Server;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The local subcluster (where the writer is) should be tried first. The writer
 * is defined from the RPC query received in the RPC server.
 */
public class LocalResolver implements OrderedResolver {

  private static final Logger LOG =
      LoggerFactory.getLogger(LocalResolver.class);

  /** Configuration key to set the minimum time to update the local cache.*/
  public static final String MIN_UPDATE_PERIOD_KEY =
      DFSConfigKeys.FEDERATION_ROUTER_PREFIX + "local-resolver.update-period";
  /** 10 seconds by default. */
  private static final long MIN_UPDATE_PERIOD_DEFAULT =
      TimeUnit.SECONDS.toMillis(10);


  /** Router service. */
  private final Router router;
  /** Minimum update time. */
  private final long minUpdateTime;

  /** Node IP -> Subcluster. */
  private Map<String, String> nodeSubcluster = null;
  /** Last time the subcluster map was updated. */
  private long lastUpdated;


  public LocalResolver(final Configuration conf, final Router routerService) {
    this.minUpdateTime = conf.getTimeDuration(
        MIN_UPDATE_PERIOD_KEY, MIN_UPDATE_PERIOD_DEFAULT,
        TimeUnit.MILLISECONDS);
    this.router = routerService;
  }

  /**
   * Get the local name space. This relies on the RPC Server to get the address
   * from the client.
   *
   * TODO we only support DN and NN locations, we need to add others like
   * Resource Managers.
   *
   * @param path Path ignored by this policy.
   * @param loc Federated location with multiple destinations.
   * @return Local name space. Null if we don't know about this machine.
   */
  @Override
  public String getFirstNamespace(final String path, final PathLocation loc) {
    String localSubcluster = null;
    String clientAddr = getClientAddr();
    Map<String, String> nodeToSubcluster = getSubclusterMappings();
    if (nodeToSubcluster != null) {
      localSubcluster = nodeToSubcluster.get(clientAddr);
      if (localSubcluster != null) {
        LOG.debug("Local namespace for {} is {}", clientAddr, localSubcluster);
      } else {
        LOG.error("Cannot get local namespace for {}", clientAddr);
      }
    } else {
      LOG.error("Cannot get node mapping when resolving {} at {} from {}",
          path, loc, clientAddr);
    }
    return localSubcluster;
  }

  @VisibleForTesting
  String getClientAddr() {
    return Server.getRemoteAddress();
  }

  /**
   * Get the mapping from nodes to subcluster. It gets this mapping from the
   * subclusters through expensive calls (e.g., RPC) and uses caching to avoid
   * too many calls. The cache might be updated asynchronously to reduce
   * latency.
   *
   * @return Node IP -> Subcluster.
   */
  @VisibleForTesting
  synchronized Map<String, String> getSubclusterMappings() {
    if (nodeSubcluster == null ||
        (monotonicNow() - lastUpdated) > minUpdateTime) {
      // Fetch the mapping asynchronously
      Thread updater = new Thread(new Runnable() {
        @Override
        public void run() {
          Map<String, String> mapping = new HashMap<>();

          Map<String, String> dnSubcluster = getDatanodesSubcluster();
          if (dnSubcluster != null) {
            mapping.putAll(dnSubcluster);
          }

          Map<String, String> nnSubcluster = getNamenodesSubcluster();
          if (nnSubcluster != null) {
            mapping.putAll(nnSubcluster);
          }
          nodeSubcluster = mapping;
          lastUpdated = monotonicNow();
        }
      });
      updater.start();

      // Wait until initialized
      if (nodeSubcluster == null) {
        try {
          LOG.debug("Wait to get the mapping for the first time");
          updater.join();
        } catch (InterruptedException e) {
          LOG.error("Cannot wait for the updater to finish");
        }
      }
    }
    return nodeSubcluster;
  }

  /**
   * Get the Datanode mapping from the subclusters from the Namenodes. This
   * needs to be done as a privileged action to use the user for the Router and
   * not the one from the client in the RPC call.
   *
   * @return DN IP -> Subcluster.
   */
  private Map<String, String> getDatanodesSubcluster() {

    final RouterRpcServer rpcServer = getRpcServer();
    if (rpcServer == null) {
      LOG.error("Cannot access the Router RPC server");
      return null;
    }

    Map<String, String> ret = new HashMap<>();
    try {
      // We need to get the DNs as a privileged user
      UserGroupInformation loginUser = UserGroupInformation.getLoginUser();
      Map<String, DatanodeStorageReport[]> dnMap = loginUser.doAs(
          new PrivilegedAction<Map<String, DatanodeStorageReport[]>>() {
            @Override
            public Map<String, DatanodeStorageReport[]> run() {
              try {
                return rpcServer.getDatanodeStorageReportMap(
                    DatanodeReportType.ALL);
              } catch (IOException e) {
                LOG.error("Cannot get the datanodes from the RPC server", e);
                return null;
              }
            }
          });
      for (Entry<String, DatanodeStorageReport[]> entry : dnMap.entrySet()) {
        String nsId = entry.getKey();
        DatanodeStorageReport[] dns = entry.getValue();
        for (DatanodeStorageReport dn : dns) {
          DatanodeInfo dnInfo = dn.getDatanodeInfo();
          String ipAddr = dnInfo.getIpAddr();
          ret.put(ipAddr, nsId);
        }
      }
    } catch (IOException e) {
      LOG.error("Cannot get Datanodes from the Namenodes: {}", e.getMessage());
    }
    return ret;
  }

  /**
   * Get the Namenode mapping from the subclusters from the Membership store. As
   * the Routers are usually co-located with Namenodes, we also check for the
   * local address for this Router here.
   *
   * @return NN IP -> Subcluster.
   */
  private Map<String, String> getNamenodesSubcluster() {

    final MembershipStore membershipStore = getMembershipStore();
    if (membershipStore == null) {
      LOG.error("Cannot access the Membership store");
      return null;
    }

    // Manage requests from this hostname (127.0.0.1)
    String localIp = "127.0.0.1";
    String localHostname = localIp;
    try {
      localHostname = InetAddress.getLocalHost().getHostName();
    } catch (UnknownHostException e) {
      LOG.error("Cannot get local host name");
    }

    Map<String, String> ret = new HashMap<>();
    try {
      // Get the values from the store
      GetNamenodeRegistrationsRequest request =
          GetNamenodeRegistrationsRequest.newInstance();
      GetNamenodeRegistrationsResponse response =
          membershipStore.getNamenodeRegistrations(request);
      final List<MembershipState> nns = response.getNamenodeMemberships();
      for (MembershipState nn : nns) {
        try {
          String nsId = nn.getNameserviceId();
          String rpcAddress = nn.getRpcAddress();
          String hostname = HostAndPort.fromString(rpcAddress).getHostText();
          ret.put(hostname, nsId);
          if (hostname.equals(localHostname)) {
            ret.put(localIp, nsId);
          }

          InetAddress addr = InetAddress.getByName(hostname);
          String ipAddr = addr.getHostAddress();
          ret.put(ipAddr, nsId);
        } catch (Exception e) {
          LOG.error("Cannot get address for {}: {}", nn, e.getMessage());
        }
      }
    } catch (IOException ioe) {
      LOG.error("Cannot get Namenodes from the State Store: {}",
          ioe.getMessage());
    }
    return ret;
  }

  /**
   * Get the Router RPC server.
   *
   * @return Router RPC server. Null if not possible.
   */
  private RouterRpcServer getRpcServer() {
    if (this.router == null) {
      return null;
    }
    return router.getRpcServer();
  }

  /**
   * Get the Membership store.
   *
   * @return Membership store.
   */
  private MembershipStore getMembershipStore() {
    StateStoreService stateStore = router.getStateStore();
    if (stateStore == null) {
      return null;
    }
    return stateStore.getRegisteredRecordStore(MembershipStore.class);
  }
}