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
package org.apache.hadoop.hdfs.server.federation.router;

import static org.apache.hadoop.hdfs.server.federation.router.RBFConfigKeys.DFS_ROUTER_HEARTBEAT_INTERVAL_MS;
import static org.apache.hadoop.hdfs.server.federation.router.RBFConfigKeys.DFS_ROUTER_HEARTBEAT_INTERVAL_MS_DEFAULT;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ha.HAServiceProtocol;
import org.apache.hadoop.ha.HAServiceStatus;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.NameNodeProxies;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.SafeModeAction;
import org.apache.hadoop.hdfs.server.federation.resolver.ActiveNamenodeResolver;
import org.apache.hadoop.hdfs.server.federation.resolver.NamenodeStatusReport;
import org.apache.hadoop.hdfs.server.protocol.NamenodeProtocol;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo;
import org.apache.hadoop.hdfs.tools.NNHAServiceTarget;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The {@link Router} periodically checks the state of a Namenode (usually on
 * the same server) and reports their high availability (HA) state and
 * load/space status to the
 * {@link org.apache.hadoop.hdfs.server.federation.store.StateStoreService}
 * . Note that this is an optional role as a Router can be independent of any
 * subcluster.
 * <p>
 * For performance with Namenode HA, the Router uses the high availability state
 * information in the State Store to forward the request to the Namenode that is
 * most likely to be active.
 * <p>
 * Note that this service can be embedded into the Namenode itself to simplify
 * the operation.
 */
public class NamenodeHeartbeatService extends PeriodicService {

  private static final Logger LOG =
      LoggerFactory.getLogger(NamenodeHeartbeatService.class);


  /** Configuration for the heartbeat. */
  private Configuration conf;

  /** Router performing the heartbeating. */
  private final ActiveNamenodeResolver resolver;

  /** Interface to the tracked NN. */
  private final String nameserviceId;
  private final String namenodeId;

  /** Namenode HA target. */
  private NNHAServiceTarget localTarget;
  /** RPC address for the namenode. */
  private String rpcAddress;
  /** Service RPC address for the namenode. */
  private String serviceAddress;
  /** Service RPC address for the namenode. */
  private String lifelineAddress;
  /** HTTP address for the namenode. */
  private String webAddress;

  /**
   * Create a new Namenode status updater.
   * @param resolver Namenode resolver service to handle NN registration.
   * @param nsId Identifier of the nameservice.
   * @param nnId Identifier of the namenode in HA.
   */
  public NamenodeHeartbeatService(
      ActiveNamenodeResolver resolver, String nsId, String nnId) {
    super(NamenodeHeartbeatService.class.getSimpleName() +
        (nsId == null ? "" : " " + nsId) +
        (nnId == null ? "" : " " + nnId));

    this.resolver = resolver;

    this.nameserviceId = nsId;
    this.namenodeId = nnId;

  }

  @Override
  protected void serviceInit(Configuration configuration) throws Exception {

    this.conf = configuration;

    String nnDesc = nameserviceId;
    if (this.namenodeId != null && !this.namenodeId.isEmpty()) {
      this.localTarget = new NNHAServiceTarget(
          conf, nameserviceId, namenodeId);
      nnDesc += "-" + namenodeId;
    } else {
      this.localTarget = null;
    }

    // Get the RPC address for the clients to connect
    this.rpcAddress = getRpcAddress(conf, nameserviceId, namenodeId);
    LOG.info("{} RPC address: {}", nnDesc, rpcAddress);

    // Get the Service RPC address for monitoring
    this.serviceAddress =
        DFSUtil.getNamenodeServiceAddr(conf, nameserviceId, namenodeId);
    if (this.serviceAddress == null) {
      LOG.error("Cannot locate RPC service address for NN {}, " +
          "using RPC address {}", nnDesc, this.rpcAddress);
      this.serviceAddress = this.rpcAddress;
    }
    LOG.info("{} Service RPC address: {}", nnDesc, serviceAddress);

    // Get the Lifeline RPC address for faster monitoring
    this.lifelineAddress =
        DFSUtil.getNamenodeLifelineAddr(conf, nameserviceId, namenodeId);
    if (this.lifelineAddress == null) {
      this.lifelineAddress = this.serviceAddress;
    }
    LOG.info("{} Lifeline RPC address: {}", nnDesc, lifelineAddress);

    // Get the Web address for UI
    this.webAddress =
        DFSUtil.getNamenodeWebAddr(conf, nameserviceId, namenodeId);
    LOG.info("{} Web address: {}", nnDesc, webAddress);

    this.setIntervalMs(conf.getLong(
        DFS_ROUTER_HEARTBEAT_INTERVAL_MS,
        DFS_ROUTER_HEARTBEAT_INTERVAL_MS_DEFAULT));


    super.serviceInit(configuration);
  }

  @Override
  public void periodicInvoke() {
    updateState();
  }

  /**
   * Get the RPC address for a Namenode.
   * @param conf Configuration.
   * @param nsId Name service identifier.
   * @param nnId Name node identifier.
   * @return RPC address in format hostname:1234.
   */
  private static String getRpcAddress(
      Configuration conf, String nsId, String nnId) {

    // Get it from the regular RPC setting
    String confKey = DFSConfigKeys.DFS_NAMENODE_RPC_ADDRESS_KEY;
    String ret = conf.get(confKey);

    if (nsId != null || nnId != null) {
      // Get if for the proper nameservice and namenode
      confKey = DFSUtil.addKeySuffixes(confKey, nsId, nnId);
      ret = conf.get(confKey);

      // If not available, get it from the map
      if (ret == null) {
        Map<String, InetSocketAddress> rpcAddresses =
            DFSUtil.getRpcAddressesForNameserviceId(conf, nsId, null);
        InetSocketAddress sockAddr = null;
        if (nnId != null) {
          sockAddr = rpcAddresses.get(nnId);
        } else if (rpcAddresses.size() == 1) {
          // Get the only namenode in the namespace
          sockAddr = rpcAddresses.values().iterator().next();
        }
        if (sockAddr != null) {
          InetAddress addr = sockAddr.getAddress();
          ret = addr.getHostName() + ":" + sockAddr.getPort();
        }
      }
    }
    return ret;
  }

  /**
   * Update the state of the Namenode.
   */
  private void updateState() {
    NamenodeStatusReport report = getNamenodeStatusReport();
    if (!report.registrationValid()) {
      // Not operational
      LOG.error("Namenode is not operational: {}", getNamenodeDesc());
    } else if (report.haStateValid()) {
      // block and HA status available
      LOG.debug("Received service state: {} from HA namenode: {}",
          report.getState(), getNamenodeDesc());
    } else if (localTarget == null) {
      // block info available, HA status not expected
      LOG.debug(
          "Reporting non-HA namenode as operational: " + getNamenodeDesc());
    } else {
      // block info available, HA status should be available, but was not
      // fetched do nothing and let the current state stand
      return;
    }
    try {
      if (!resolver.registerNamenode(report)) {
        LOG.warn("Cannot register namenode {}", report);
      }
    } catch (IOException e) {
      LOG.info("Cannot register namenode in the State Store");
    } catch (Exception ex) {
      LOG.error("Unhandled exception updating NN registration for {}",
          getNamenodeDesc(), ex);
    }
  }

  /**
   * Get the status report for the Namenode monitored by this heartbeater.
   * @return Namenode status report.
   */
  protected NamenodeStatusReport getNamenodeStatusReport() {
    NamenodeStatusReport report = new NamenodeStatusReport(nameserviceId,
        namenodeId, rpcAddress, serviceAddress, lifelineAddress, webAddress);

    try {
      LOG.debug("Probing NN at service address: {}", serviceAddress);

      URI serviceURI = new URI("hdfs://" + serviceAddress);
      // Read the filesystem info from RPC (required)
      NamenodeProtocol nn = NameNodeProxies
          .createProxy(this.conf, serviceURI, NamenodeProtocol.class)
          .getProxy();

      if (nn != null) {
        NamespaceInfo info = nn.versionRequest();
        if (info != null) {
          report.setNamespaceInfo(info);
        }
      }
      if (!report.registrationValid()) {
        return report;
      }

      // Check for safemode from the client protocol. Currently optional, but
      // should be required at some point for QoS
      try {
        ClientProtocol client = NameNodeProxies
            .createProxy(this.conf, serviceURI, ClientProtocol.class)
            .getProxy();
        if (client != null) {
          boolean isSafeMode = client.setSafeMode(
              SafeModeAction.SAFEMODE_GET, false);
          report.setSafeMode(isSafeMode);
        }
      } catch (Exception e) {
        LOG.error("Cannot fetch safemode state for {}", getNamenodeDesc(), e);
      }

      // Read the stats from JMX (optional)
      updateJMXParameters(webAddress, report);

      if (localTarget != null) {
        // Try to get the HA status
        try {
          // Determine if NN is active
          // TODO: dynamic timeout
          HAServiceProtocol haProtocol = localTarget.getProxy(conf, 30*1000);
          HAServiceStatus status = haProtocol.getServiceStatus();
          report.setHAServiceState(status.getState());
        } catch (Throwable e) {
          if (e.getMessage().startsWith("HA for namenode is not enabled")) {
            LOG.error("HA for {} is not enabled", getNamenodeDesc());
            localTarget = null;
          } else {
            // Failed to fetch HA status, ignoring failure
            LOG.error("Cannot fetch HA status for {}: {}",
                getNamenodeDesc(), e.getMessage(), e);
          }
        }
      }
    } catch(IOException e) {
      LOG.error("Cannot communicate with {}: {}",
          getNamenodeDesc(), e.getMessage());
    } catch(Throwable e) {
      // Generic error that we don't know about
      LOG.error("Unexpected exception while communicating with {}: {}",
          getNamenodeDesc(), e.getMessage(), e);
    }
    return report;
  }

  /**
   * Get the description of the Namenode to monitor.
   * @return Description of the Namenode to monitor.
   */
  public String getNamenodeDesc() {
    if (namenodeId != null && !namenodeId.isEmpty()) {
      return nameserviceId + "-" + namenodeId + ":" + serviceAddress;
    } else {
      return nameserviceId + ":" + serviceAddress;
    }
  }

  /**
   * Get the parameters for a Namenode from JMX and add them to the report.
   * @param address Web interface of the Namenode to monitor.
   * @param report Namenode status report to update with JMX data.
   */
  private void updateJMXParameters(
      String address, NamenodeStatusReport report) {
    try {
      // TODO part of this should be moved to its own utility
      String query = "Hadoop:service=NameNode,name=FSNamesystem*";
      JSONArray aux = FederationUtil.getJmx(query, address);
      if (aux != null) {
        for (int i = 0; i < aux.length(); i++) {
          JSONObject jsonObject = aux.getJSONObject(i);
          String name = jsonObject.getString("name");
          if (name.equals("Hadoop:service=NameNode,name=FSNamesystemState")) {
            report.setDatanodeInfo(
                jsonObject.getInt("NumLiveDataNodes"),
                jsonObject.getInt("NumDeadDataNodes"),
                jsonObject.getInt("NumDecommissioningDataNodes"),
                jsonObject.getInt("NumDecomLiveDataNodes"),
                jsonObject.getInt("NumDecomDeadDataNodes"));
          } else if (name.equals(
              "Hadoop:service=NameNode,name=FSNamesystem")) {
            report.setNamesystemInfo(
                jsonObject.getLong("CapacityRemaining"),
                jsonObject.getLong("CapacityTotal"),
                jsonObject.getLong("FilesTotal"),
                jsonObject.getLong("BlocksTotal"),
                jsonObject.getLong("MissingBlocks"),
                jsonObject.getLong("PendingReplicationBlocks"),
                jsonObject.getLong("UnderReplicatedBlocks"),
                jsonObject.getLong("PendingDeletionBlocks"),
                jsonObject.getLong("ProvidedCapacityTotal"));
          }
        }
      }
    } catch (Exception e) {
      LOG.error("Cannot get stat from {} using JMX", getNamenodeDesc(), e);
    }
  }
}
