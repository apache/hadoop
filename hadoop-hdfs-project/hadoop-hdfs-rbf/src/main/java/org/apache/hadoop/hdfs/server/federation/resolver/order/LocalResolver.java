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

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.security.PrivilegedAction;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.DatanodeReportType;
import org.apache.hadoop.hdfs.server.federation.resolver.PathLocation;
import org.apache.hadoop.hdfs.server.federation.router.Router;
import org.apache.hadoop.hdfs.server.federation.router.RouterRpcServer;
import org.apache.hadoop.hdfs.server.federation.store.MembershipStore;
import org.apache.hadoop.hdfs.server.federation.store.protocol.GetNamenodeRegistrationsRequest;
import org.apache.hadoop.hdfs.server.federation.store.protocol.GetNamenodeRegistrationsResponse;
import org.apache.hadoop.hdfs.server.federation.store.records.MembershipState;
import org.apache.hadoop.hdfs.server.protocol.DatanodeStorageReport;
import org.apache.hadoop.ipc.RPC.Server;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.net.HostAndPort;


/**
 * The local subcluster (where the writer is) should be tried first. The writer
 * is defined from the RPC query received in the RPC server.
 */
public class LocalResolver extends RouterResolver<String, String> {

  private static final Logger LOG =
      LoggerFactory.getLogger(LocalResolver.class);

  public LocalResolver(final Configuration conf, final Router routerService) {
    super(conf, routerService);
  }

  /**
   * Get the mapping from nodes to subcluster. It gets this mapping from the
   * subclusters through expensive calls (e.g., RPC) and uses caching to avoid
   * too many calls. The cache might be updated asynchronously to reduce
   * latency.
   *
   * @return Node IP -> Subcluster.
   */
  @Override
  protected Map<String, String> getSubclusterInfo(
      MembershipStore membershipStore) {
    Map<String, String> mapping = new HashMap<>();

    Map<String, String> dnSubcluster = getDatanodesSubcluster();
    if (dnSubcluster != null) {
      mapping.putAll(dnSubcluster);
    }

    Map<String, String> nnSubcluster = getNamenodesSubcluster(membershipStore);
    if (nnSubcluster != null) {
      mapping.putAll(nnSubcluster);
    }
    return mapping;
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
  protected String chooseFirstNamespace(String path, PathLocation loc) {
    String localSubcluster = null;
    String clientAddr = getClientAddr();
    Map<String, String> subclusterInfo = getSubclusterMapping();
    if (subclusterInfo != null) {
      localSubcluster = subclusterInfo.get(clientAddr);
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
  private Map<String, String> getNamenodesSubcluster(
      MembershipStore membershipStore) {
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
      LOG.error("Cannot get Namenodes from the State Store", ioe);
    }
    return ret;
  }
}