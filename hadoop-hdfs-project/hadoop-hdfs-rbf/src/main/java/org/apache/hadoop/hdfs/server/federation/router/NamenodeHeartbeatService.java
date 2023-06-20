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

import static org.apache.hadoop.hdfs.server.federation.router.RBFConfigKeys.DFS_ROUTER_HEALTH_MONITOR_TIMEOUT;
import static org.apache.hadoop.hdfs.server.federation.router.RBFConfigKeys.DFS_ROUTER_HEALTH_MONITOR_TIMEOUT_DEFAULT;
import static org.apache.hadoop.hdfs.server.federation.router.RBFConfigKeys.DFS_ROUTER_HEARTBEAT_INTERVAL_MS;
import static org.apache.hadoop.hdfs.server.federation.router.RBFConfigKeys.DFS_ROUTER_HEARTBEAT_INTERVAL_MS_DEFAULT;
import static org.apache.hadoop.hdfs.server.federation.router.RBFConfigKeys.DFS_ROUTER_NAMENODE_HEARTBEAT_JMX_INTERVAL_MS;
import static org.apache.hadoop.hdfs.server.federation.router.RBFConfigKeys.DFS_ROUTER_NAMENODE_HEARTBEAT_JMX_INTERVAL_MS_DEFAULT;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URI;
import java.security.PrivilegedExceptionAction;
import java.util.Map;
import java.util.concurrent.TimeUnit;

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
import org.apache.hadoop.hdfs.tools.DFSHAAdmin;
import org.apache.hadoop.hdfs.tools.NNHAServiceTarget;
import org.apache.hadoop.hdfs.web.URLConnectionFactory;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.util.Time;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.classification.VisibleForTesting;

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
  /** Cache HA protocol. */
  private HAServiceProtocol localTargetHAProtocol;
  /** Cache NN protocol. */
  private NamenodeProtocol namenodeProtocol;
  /** Cache Client protocol. */
  private ClientProtocol clientProtocol;
  /** RPC address for the namenode. */
  private String rpcAddress;
  /** Service RPC address for the namenode. */
  private String serviceAddress;
  /** Service RPC address for the namenode. */
  private String lifelineAddress;
  /** HTTP address for the namenode. */
  private String webAddress;
  /** Connection factory for JMX calls. */
  private URLConnectionFactory connectionFactory;
  /** URL scheme to use for JMX calls. */
  private String scheme;

  /** Frequency of updates to JMX report. */
  private long updateJmxIntervalMs;
  /** Timestamp of last attempt to update JMX report. */
  private long lastJmxUpdateAttempt;
  /** Result of the last successful FsNamesystemMetrics report. */
  private JSONArray fsNamesystemMetrics;
  /** Result of the last successful NamenodeInfoMetrics report. */
  private JSONArray namenodeInfoMetrics;

  private String resolvedHost;
  private String originalNnId;

  private int healthMonitorTimeoutMs = (int) DFS_ROUTER_HEALTH_MONITOR_TIMEOUT_DEFAULT;

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

  /**
   * Create a new Namenode status updater.
   *
   * @param resolver Namenode resolver service to handle NN registration.
   * @param nsId          Identifier of the nameservice.
   * @param nnId          Identifier of the namenode in HA.
   * @param resolvedHost  resolvedHostname for this specific namenode.
   */
  public NamenodeHeartbeatService(
      ActiveNamenodeResolver resolver, String nsId, String nnId, String resolvedHost) {
    super(getNnHeartBeatServiceName(nsId, nnId));

    this.resolver = resolver;

    this.nameserviceId = nsId;
    // Concat a uniq id from original nnId and resolvedHost
    this.namenodeId = nnId + "-" + resolvedHost;
    this.resolvedHost = resolvedHost;
    // Same the original nnid to get the ports from config.
    this.originalNnId = nnId;

  }

  @Override
  protected void serviceInit(Configuration configuration) throws Exception {

    this.conf = DFSHAAdmin.addSecurityConfiguration(configuration);

    String nnDesc = nameserviceId;
    if (this.namenodeId != null && !this.namenodeId.isEmpty()) {
      nnDesc += "-" + namenodeId;
    } else {
      this.localTarget = null;
    }

    if (originalNnId == null) {
      originalNnId = namenodeId;
    }
    // Get the RPC address for the clients to connect
    this.rpcAddress = getRpcAddress(conf, nameserviceId, originalNnId);

    // Get the Service RPC address for monitoring
    this.serviceAddress =
        DFSUtil.getNamenodeServiceAddr(conf, nameserviceId, originalNnId);
    if (this.serviceAddress == null) {
      LOG.error("Cannot locate RPC service address for NN {}, " +
          "using RPC address {}", nnDesc, this.rpcAddress);
      this.serviceAddress = this.rpcAddress;
    }

    // Get the Lifeline RPC address for faster monitoring
    this.lifelineAddress =
        DFSUtil.getNamenodeLifelineAddr(conf, nameserviceId, originalNnId);
    if (this.lifelineAddress == null) {
      this.lifelineAddress = this.serviceAddress;
    }

    // Get the Web address for UI
    this.webAddress =
        DFSUtil.getNamenodeWebAddr(conf, nameserviceId, originalNnId);

    if (resolvedHost != null) {
      // Get the addresses from resolvedHost plus the configured ports.
      rpcAddress = resolvedHost + ":"
          + NetUtils.getPortFromHostPortString(rpcAddress);
      serviceAddress = resolvedHost + ":"
          + NetUtils.getPortFromHostPortString(serviceAddress);
      lifelineAddress = resolvedHost + ":"
          + NetUtils.getPortFromHostPortString(lifelineAddress);
      webAddress = resolvedHost + ":"
          + NetUtils.getPortFromHostPortString(webAddress);
    }

    LOG.info("{} RPC address: {}", nnDesc, rpcAddress);
    LOG.info("{} Service RPC address: {}", nnDesc, serviceAddress);
    LOG.info("{} Lifeline RPC address: {}", nnDesc, lifelineAddress);
    LOG.info("{} Web address: {}", nnDesc, webAddress);

    if (this.namenodeId != null && !this.namenodeId.isEmpty()) {
      this.localTarget = new NNHAServiceTarget(
          conf, nameserviceId, namenodeId, serviceAddress, lifelineAddress);
    }

    this.connectionFactory =
        URLConnectionFactory.newDefaultURLConnectionFactory(conf);

    this.scheme =
        DFSUtil.getHttpPolicy(conf).isHttpEnabled() ? "http" : "https";

    this.setIntervalMs(conf.getLong(
        DFS_ROUTER_HEARTBEAT_INTERVAL_MS,
        DFS_ROUTER_HEARTBEAT_INTERVAL_MS_DEFAULT));

    long timeoutMs = conf.getTimeDuration(DFS_ROUTER_HEALTH_MONITOR_TIMEOUT,
        DFS_ROUTER_HEALTH_MONITOR_TIMEOUT_DEFAULT, TimeUnit.MILLISECONDS);
    if (timeoutMs < 0) {
      LOG.warn("Invalid value {} configured for {} should be greater than or equal to 0. " +
          "Using value of : 0ms instead.", timeoutMs, DFS_ROUTER_HEALTH_MONITOR_TIMEOUT);
      this.healthMonitorTimeoutMs = 0;
    } else {
      this.healthMonitorTimeoutMs = (int) timeoutMs;
    }

    this.updateJmxIntervalMs = conf.getTimeDuration(DFS_ROUTER_NAMENODE_HEARTBEAT_JMX_INTERVAL_MS,
        DFS_ROUTER_NAMENODE_HEARTBEAT_JMX_INTERVAL_MS_DEFAULT, TimeUnit.MILLISECONDS);

    super.serviceInit(configuration);
  }

  @Override
  public void periodicInvoke() {
    try {
      // Run using the login user credentials
      SecurityUtil.doAsLoginUser((PrivilegedExceptionAction<Void>) () -> {
        updateState();
        return null;
      });
    } catch (IOException e) {
      LOG.error("Cannot update namenode state", e);
    }
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
      if (LOG.isDebugEnabled()) {
        LOG.debug("Received service state: {} from HA namenode: {}",
            report.getState(), getNamenodeDesc());
      }
    } else if (localTarget == null) {
      // block info available, HA status not expected
      if (LOG.isDebugEnabled()) {
        LOG.debug(
            "Reporting non-HA namenode as operational: {}", getNamenodeDesc());
      }
    } else {
      // block info available, HA status should be available, but was not
      // fetched do nothing and let the current state stand
      return;
    }
    try {
      if (!resolver.registerNamenode(report)) {
        LOG.warn("Cannot register namenode {}", report);
      }
    } catch (Exception e) {
      LOG.error("Cannot register namenode {} in the State Store", getNamenodeDesc(), e);
    }
  }

  /**
   * Get the status report for the Namenode monitored by this heartbeater.
   * @return Namenode status report.
   */
  protected NamenodeStatusReport getNamenodeStatusReport() {
    NamenodeStatusReport report = new NamenodeStatusReport(nameserviceId,
        namenodeId, rpcAddress, serviceAddress,
        lifelineAddress, scheme, webAddress);

    try {
      LOG.debug("Probing NN at service address: {}", serviceAddress);

      URI serviceURI = new URI("hdfs://" + serviceAddress);

      // Read the filesystem info from RPC (required)
      updateNameSpaceInfoParameters(serviceURI, report);
      if (!report.registrationValid()) {
        return report;
      }

      // Check for safemode from the client protocol. Currently optional, but
      // should be required at some point for QoS
      updateSafeModeParameters(serviceURI, report);

      // Read the stats from JMX (optional)
      updateJMXParameters(webAddress, report);

      // Try to get the HA status
      updateHAStatusParameters(report);
    } catch (IOException e) {
      LOG.error("Cannot communicate with {}: {}",
          getNamenodeDesc(), e.getMessage());
    } catch (Throwable e) {
      // Generic error that we don't know about
      LOG.error("Unexpected exception while communicating with {}: {}",
          getNamenodeDesc(), e.getMessage(), e);
    }
    return report;
  }

  @VisibleForTesting
  NNHAServiceTarget getLocalTarget(){
    return this.localTarget;
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

  private static String getNnHeartBeatServiceName(String nsId, String nnId) {
    return NamenodeHeartbeatService.class.getSimpleName() +
        (nsId == null ? "" : " " + nsId) +
        (nnId == null ? "" : " " + nnId);
  }

  /**
   * Get the namespace information for a Namenode via RPC and add them to the report.
   * @param serviceURI Server address of the Namenode to monitor.
   * @param report Namenode status report updating with namespace information data.
   * @throws IOException This method will throw IOException up, because RBF need
   *                     use Namespace Info to identify this NS. If there are some IOExceptions,
   *                     RBF doesn't need to get other information from NameNode,
   *                     so throw IOException up.
   */
  private void updateNameSpaceInfoParameters(URI serviceURI,
      NamenodeStatusReport report) throws IOException {
    try {
      if (this.namenodeProtocol == null) {
        this.namenodeProtocol = NameNodeProxies.createProxy(this.conf, serviceURI,
            NamenodeProtocol.class).getProxy();
      }
      if (namenodeProtocol != null) {
        NamespaceInfo info = namenodeProtocol.versionRequest();
        if (info != null) {
          report.setNamespaceInfo(info);
        }
      }
    } catch (IOException e) {
      this.namenodeProtocol = null;
      throw e;
    }
  }

  /**
   * Get the safemode information for a Namenode via RPC and add them to the report.
   * Safemode is only one status of NameNode and is useless for RBF identify one NameNode.
   * So If there are some IOExceptions, RBF can just ignore it and try to collect
   * other information form namenode continue.
   * @param serviceURI Server address of the Namenode to monitor.
   * @param report Namenode status report updating with safemode information data.
   */
  private void updateSafeModeParameters(URI serviceURI, NamenodeStatusReport report) {
    try {
      if (this.clientProtocol == null) {
        this.clientProtocol = NameNodeProxies
            .createProxy(this.conf, serviceURI, ClientProtocol.class)
            .getProxy();
      }
      if (clientProtocol != null) {
        boolean isSafeMode = clientProtocol.setSafeMode(SafeModeAction.SAFEMODE_GET, false);
        report.setSafeMode(isSafeMode);
      }
    } catch (Exception e) {
      LOG.error("Cannot fetch safemode state for {}", getNamenodeDesc(), e);
      this.clientProtocol = null;
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
      if (shouldUpdateJmx()) {
        this.lastJmxUpdateAttempt = Time.monotonicNow();
        getFsNamesystemMetrics(address);
        getNamenodeInfoMetrics(address);
      }
      populateFsNamesystemMetrics(this.fsNamesystemMetrics, report);
      populateNamenodeInfoMetrics(this.namenodeInfoMetrics, report);
    } catch (Exception e) {
      LOG.error("Cannot get stat from {} using JMX", getNamenodeDesc(), e);
    }
  }

  /**
   * Get the HA status for a Namenode via RPC and add them to the report.
   * @param report Namenode status report updating with HA status information data.
   */
  private void updateHAStatusParameters(NamenodeStatusReport report) {
    if (localTarget != null) {
      try {
        // Determine if NN is active
        if (localTargetHAProtocol == null) {
          localTargetHAProtocol = localTarget.getHealthMonitorProxy(
              conf, this.healthMonitorTimeoutMs);
          LOG.debug("Get HA status with address {}", lifelineAddress);
        }
        HAServiceStatus status = localTargetHAProtocol.getServiceStatus();
        report.setHAServiceState(status.getState());
      } catch (Throwable e) {
        if (e.getMessage().startsWith("HA for namenode is not enabled")) {
          LOG.error("HA for {} is not enabled", getNamenodeDesc());
          localTarget = null;
        } else {
          // Failed to fetch HA status, ignoring failure
          LOG.error("Cannot fetch HA status for {}", getNamenodeDesc(), e);
        }
        localTargetHAProtocol = null;
      }
    }
  }

  /**
   * Evaluates whether the JMX report should be refreshed by
   * calling the Namenode, based on the following conditions:
   * 1. JMX Updates must be enabled.
   * 2. The last attempt to update JMX occurred before the
   *    configured interval (if any).
   */
  private boolean shouldUpdateJmx() {
    if (this.updateJmxIntervalMs < 0) {
      return false;
    }

    return Time.monotonicNow() - this.lastJmxUpdateAttempt > this.updateJmxIntervalMs;
  }

  /**
   * Fetches NamenodeInfo metrics from namenode.
   * @param address Web interface of the Namenode to monitor.
   */
  private void getNamenodeInfoMetrics(String address) {
    String query = "Hadoop:service=NameNode,name=NameNodeInfo";
    this.namenodeInfoMetrics = FederationUtil.getJmx(query, address, connectionFactory, scheme);
  }

  /**
   * Populates NamenodeInfo metrics into report.
   * @param aux NamenodeInfo metrics from namenode.
   * @param report Namenode status report to update with JMX data.
   * @throws JSONException When an invalid JSONObject is found
   */
  private void populateNamenodeInfoMetrics(JSONArray aux, NamenodeStatusReport report)
      throws JSONException {
    if (aux != null && aux.length() > 0) {
      JSONObject jsonObject = aux.getJSONObject(0);
      String name = jsonObject.getString("name");
      if (name.equals("Hadoop:service=NameNode,name=NameNodeInfo")) {
        report.setNamenodeInfo(jsonObject.optInt("CorruptFilesCount"),
            jsonObject
                .optLong("NumberOfMissingBlocksWithReplicationFactorOne"),
            jsonObject
                .optLong("HighestPriorityLowRedundancyReplicatedBlocks"),
            jsonObject.optLong("HighestPriorityLowRedundancyECBlocks"));
      }
    }
  }

  /**
   * Fetches FSNamesystem* metrics from namenode.
   * @param address Web interface of the Namenode to monitor.
   */
  private void getFsNamesystemMetrics(String address) {
    String query = "Hadoop:service=NameNode,name=FSNamesystem*";
    this.fsNamesystemMetrics = FederationUtil.getJmx(query, address, connectionFactory, scheme);
  }

  /**
   * Populates FSNamesystem* metrics into report.
   * @param aux FSNamesystem* metrics from namenode.
   * @param report Namenode status report to update with JMX data.
   * @throws JSONException When invalid JSONObject is found.
   */
  private void populateFsNamesystemMetrics(JSONArray aux, NamenodeStatusReport report)
      throws JSONException {
    if (aux != null) {
      for (int i = 0; i < aux.length(); i++) {
        JSONObject jsonObject = aux.getJSONObject(i);
        String name = jsonObject.getString("name");
        if (name.equals("Hadoop:service=NameNode,name=FSNamesystemState")) {
          report.setDatanodeInfo(
              jsonObject.getInt("NumLiveDataNodes"),
              jsonObject.getInt("NumDeadDataNodes"),
              jsonObject.getInt("NumStaleDataNodes"),
              jsonObject.getInt("NumDecommissioningDataNodes"),
              jsonObject.getInt("NumDecomLiveDataNodes"),
              jsonObject.getInt("NumDecomDeadDataNodes"),
              jsonObject.optInt("NumInMaintenanceLiveDataNodes"),
              jsonObject.optInt("NumInMaintenanceDeadDataNodes"),
              jsonObject.optInt("NumEnteringMaintenanceDataNodes"),
              jsonObject.optLong("ScheduledReplicationBlocks"));
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
              jsonObject.optLong("ProvidedCapacityTotal"),
              jsonObject.getInt("PendingSPSPaths"));
        }
      }
    }
  }

  @Override
  protected void serviceStop() throws Exception {
    LOG.info("Stopping NamenodeHeartbeat service for, NS {} NN {} ",
        this.nameserviceId, this.namenodeId);
    if (this.connectionFactory != null) {
      this.connectionFactory.destroy();
    }
    super.serviceStop();
  }
}