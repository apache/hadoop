/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.hdds.scm;

import com.google.common.base.Strings;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.server.ServerUtils;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static org.apache.hadoop.hdds.HddsConfigKeys
    .HDDS_HEARTBEAT_INTERVAL;
import static org.apache.hadoop.hdds.HddsConfigKeys
    .HDDS_HEARTBEAT_INTERVAL_DEFAULT;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys
    .OZONE_SCM_DEADNODE_INTERVAL;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys
    .OZONE_SCM_DEADNODE_INTERVAL_DEFAULT;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys
    .OZONE_SCM_HEARTBEAT_LOG_WARN_DEFAULT;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys
    .OZONE_SCM_HEARTBEAT_LOG_WARN_INTERVAL_COUNT;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys
    .OZONE_SCM_HEARTBEAT_PROCESS_INTERVAL;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys
    .OZONE_SCM_HEARTBEAT_RPC_TIMEOUT;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys
    .OZONE_SCM_HEARTBEAT_RPC_TIMEOUT_DEFAULT;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys
    .OZONE_SCM_STALENODE_INTERVAL;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys
    .OZONE_SCM_STALENODE_INTERVAL_DEFAULT;
import static org.apache.hadoop.hdds.HddsUtils.*;
import static org.apache.hadoop.hdds.server.ServerUtils.sanitizeUserArgs;

/**
 * Hdds stateless helper functions for server side components.
 */
public final class HddsServerUtil {

  private HddsServerUtil() {
  }

  private static final Logger LOG = LoggerFactory.getLogger(
      HddsServerUtil.class);

  /**
   * Retrieve the socket address that should be used by DataNodes to connect
   * to the SCM.
   *
   * @param conf
   * @return Target InetSocketAddress for the SCM service endpoint.
   */
  public static InetSocketAddress getScmAddressForDataNodes(
      Configuration conf) {
    // We try the following settings in decreasing priority to retrieve the
    // target host.
    // - OZONE_SCM_DATANODE_ADDRESS_KEY
    // - OZONE_SCM_CLIENT_ADDRESS_KEY
    // - OZONE_SCM_NAMES
    //
    Optional<String> host = getHostNameFromConfigKeys(conf,
        ScmConfigKeys.OZONE_SCM_DATANODE_ADDRESS_KEY,
        ScmConfigKeys.OZONE_SCM_CLIENT_ADDRESS_KEY);

    if (!host.isPresent()) {
      // Fallback to Ozone SCM names.
      Collection<InetSocketAddress> scmAddresses = getSCMAddresses(conf);
      if (scmAddresses.size() > 1) {
        throw new IllegalArgumentException(
            ScmConfigKeys.OZONE_SCM_NAMES +
                " must contain a single hostname. Multiple SCM hosts are " +
                "currently unsupported");
      }
      host = Optional.of(scmAddresses.iterator().next().getHostName());
    }

    if (!host.isPresent()) {
      throw new IllegalArgumentException(
          ScmConfigKeys.OZONE_SCM_CLIENT_ADDRESS_KEY +
              " must be defined. See" +
              " https://wiki.apache.org/hadoop/Ozone#Configuration "
              + "for details on configuring Ozone.");
    }

    // If no port number is specified then we'll just try the defaultBindPort.
    final Optional<Integer> port = getPortNumberFromConfigKeys(conf,
        ScmConfigKeys.OZONE_SCM_DATANODE_ADDRESS_KEY);

    InetSocketAddress addr = NetUtils.createSocketAddr(host.get() + ":" +
        port.orElse(ScmConfigKeys.OZONE_SCM_DATANODE_PORT_DEFAULT));

    return addr;
  }

  /**
   * Retrieve the socket address that should be used by clients to connect
   * to the SCM.
   *
   * @param conf
   * @return Target InetSocketAddress for the SCM client endpoint.
   */
  public static InetSocketAddress getScmClientBindAddress(
      Configuration conf) {
    final Optional<String> host = getHostNameFromConfigKeys(conf,
        ScmConfigKeys.OZONE_SCM_CLIENT_BIND_HOST_KEY);

    final Optional<Integer> port = getPortNumberFromConfigKeys(conf,
        ScmConfigKeys.OZONE_SCM_CLIENT_ADDRESS_KEY);

    return NetUtils.createSocketAddr(
        host.orElse(ScmConfigKeys.OZONE_SCM_CLIENT_BIND_HOST_DEFAULT) + ":" +
            port.orElse(ScmConfigKeys.OZONE_SCM_CLIENT_PORT_DEFAULT));
  }

  /**
   * Retrieve the socket address that should be used by clients to connect
   * to the SCM Block service.
   *
   * @param conf
   * @return Target InetSocketAddress for the SCM block client endpoint.
   */
  public static InetSocketAddress getScmBlockClientBindAddress(
      Configuration conf) {
    final Optional<String> host = getHostNameFromConfigKeys(conf,
        ScmConfigKeys.OZONE_SCM_BLOCK_CLIENT_BIND_HOST_KEY);

    final Optional<Integer> port = getPortNumberFromConfigKeys(conf,
        ScmConfigKeys.OZONE_SCM_BLOCK_CLIENT_ADDRESS_KEY);

    return NetUtils.createSocketAddr(
        host.orElse(ScmConfigKeys.OZONE_SCM_BLOCK_CLIENT_BIND_HOST_DEFAULT)
            + ":"
            + port.orElse(ScmConfigKeys.OZONE_SCM_BLOCK_CLIENT_PORT_DEFAULT));
  }

  /**
   * Retrieve the socket address that should be used by scm security server to
   * service clients.
   *
   * @param conf
   * @return Target InetSocketAddress for the SCM security service.
   */
  public static InetSocketAddress getScmSecurityInetAddress(
      Configuration conf) {
    final Optional<String> host = getHostNameFromConfigKeys(conf,
        ScmConfigKeys.OZONE_SCM_SECURITY_SERVICE_BIND_HOST_KEY);

    final Optional<Integer> port = getPortNumberFromConfigKeys(conf,
        ScmConfigKeys.OZONE_SCM_SECURITY_SERVICE_ADDRESS_KEY);

    return NetUtils.createSocketAddr(
        host.orElse(ScmConfigKeys.OZONE_SCM_SECURITY_SERVICE_BIND_HOST_DEFAULT)
            + ":" + port
            .orElse(conf.getInt(ScmConfigKeys
                    .OZONE_SCM_SECURITY_SERVICE_PORT_KEY,
                ScmConfigKeys.OZONE_SCM_SECURITY_SERVICE_PORT_DEFAULT)));
  }

  /**
   * Retrieve the socket address that should be used by DataNodes to connect
   * to the SCM.
   *
   * @param conf
   * @return Target InetSocketAddress for the SCM service endpoint.
   */
  public static InetSocketAddress getScmDataNodeBindAddress(
      Configuration conf) {
    final Optional<String> host = getHostNameFromConfigKeys(conf,
        ScmConfigKeys.OZONE_SCM_DATANODE_BIND_HOST_KEY);

    // If no port number is specified then we'll just try the defaultBindPort.
    final Optional<Integer> port = getPortNumberFromConfigKeys(conf,
        ScmConfigKeys.OZONE_SCM_DATANODE_ADDRESS_KEY);

    return NetUtils.createSocketAddr(
        host.orElse(ScmConfigKeys.OZONE_SCM_DATANODE_BIND_HOST_DEFAULT) + ":" +
            port.orElse(ScmConfigKeys.OZONE_SCM_DATANODE_PORT_DEFAULT));
  }


  /**
   * Returns the interval in which the heartbeat processor thread runs.
   *
   * @param conf - Configuration
   * @return long in Milliseconds.
   */
  public static long getScmheartbeatCheckerInterval(Configuration conf) {
    return conf.getTimeDuration(OZONE_SCM_HEARTBEAT_PROCESS_INTERVAL,
        ScmConfigKeys.OZONE_SCM_HEARTBEAT_PROCESS_INTERVAL_DEFAULT,
        TimeUnit.MILLISECONDS);
  }

  /**
   * Heartbeat Interval - Defines the heartbeat frequency from a datanode to
   * SCM.
   *
   * @param conf - Ozone Config
   * @return - HB interval in milli seconds.
   */
  public static long getScmHeartbeatInterval(Configuration conf) {
    return conf.getTimeDuration(HDDS_HEARTBEAT_INTERVAL,
        HDDS_HEARTBEAT_INTERVAL_DEFAULT, TimeUnit.MILLISECONDS);
  }

  /**
   * Get the Stale Node interval, which is used by SCM to flag a datanode as
   * stale, if the heartbeat from that node has been missing for this duration.
   *
   * @param conf - Configuration.
   * @return - Long, Milliseconds to wait before flagging a node as stale.
   */
  public static long getStaleNodeInterval(Configuration conf) {

    long staleNodeIntervalMs =
        conf.getTimeDuration(OZONE_SCM_STALENODE_INTERVAL,
            OZONE_SCM_STALENODE_INTERVAL_DEFAULT, TimeUnit.MILLISECONDS);

    long heartbeatThreadFrequencyMs = getScmheartbeatCheckerInterval(conf);

    long heartbeatIntervalMs = getScmHeartbeatInterval(conf);


    // Make sure that StaleNodeInterval is configured way above the frequency
    // at which we run the heartbeat thread.
    //
    // Here we check that staleNodeInterval is at least five times more than the
    // frequency at which the accounting thread is going to run.
    try {
      sanitizeUserArgs(staleNodeIntervalMs, heartbeatThreadFrequencyMs,
          5, 1000);
    } catch (IllegalArgumentException ex) {
      LOG.error("Stale Node Interval is cannot be honored due to " +
              "mis-configured {}. ex:  {}",
          OZONE_SCM_HEARTBEAT_PROCESS_INTERVAL, ex);
      throw ex;
    }

    // Make sure that stale node value is greater than configured value that
    // datanodes are going to send HBs.
    try {
      sanitizeUserArgs(staleNodeIntervalMs, heartbeatIntervalMs, 3, 1000);
    } catch (IllegalArgumentException ex) {
      LOG.error("Stale Node Interval MS is cannot be honored due to " +
          "mis-configured {}. ex:  {}", HDDS_HEARTBEAT_INTERVAL, ex);
      throw ex;
    }
    return staleNodeIntervalMs;
  }

  /**
   * Gets the interval for dead node flagging. This has to be a value that is
   * greater than stale node value,  and by transitive relation we also know
   * that this value is greater than heartbeat interval and heartbeatProcess
   * Interval.
   *
   * @param conf - Configuration.
   * @return - the interval for dead node flagging.
   */
  public static long getDeadNodeInterval(Configuration conf) {
    long staleNodeIntervalMs = getStaleNodeInterval(conf);
    long deadNodeIntervalMs = conf.getTimeDuration(OZONE_SCM_DEADNODE_INTERVAL,
        OZONE_SCM_DEADNODE_INTERVAL_DEFAULT,
        TimeUnit.MILLISECONDS);

    try {
      // Make sure that dead nodes Ms is at least twice the time for staleNodes
      // with a max of 1000 times the staleNodes.
      sanitizeUserArgs(deadNodeIntervalMs, staleNodeIntervalMs, 2, 1000);
    } catch (IllegalArgumentException ex) {
      LOG.error("Dead Node Interval MS is cannot be honored due to " +
          "mis-configured {}. ex:  {}", OZONE_SCM_STALENODE_INTERVAL, ex);
      throw ex;
    }
    return deadNodeIntervalMs;
  }

  /**
   * Timeout value for the RPC from Datanode to SCM, primarily used for
   * Heartbeats and container reports.
   *
   * @param conf - Ozone Config
   * @return - Rpc timeout in Milliseconds.
   */
  public static long getScmRpcTimeOutInMilliseconds(Configuration conf) {
    return conf.getTimeDuration(OZONE_SCM_HEARTBEAT_RPC_TIMEOUT,
        OZONE_SCM_HEARTBEAT_RPC_TIMEOUT_DEFAULT, TimeUnit.MILLISECONDS);
  }

  /**
   * Log Warn interval.
   *
   * @param conf - Ozone Config
   * @return - Log warn interval.
   */
  public static int getLogWarnInterval(Configuration conf) {
    return conf.getInt(OZONE_SCM_HEARTBEAT_LOG_WARN_INTERVAL_COUNT,
        OZONE_SCM_HEARTBEAT_LOG_WARN_DEFAULT);
  }

  /**
   * returns the Container port.
   * @param conf - Conf
   * @return port number.
   */
  public static int getContainerPort(Configuration conf) {
    return conf.getInt(OzoneConfigKeys.DFS_CONTAINER_IPC_PORT,
        OzoneConfigKeys.DFS_CONTAINER_IPC_PORT_DEFAULT);
  }


  /**
   * Return the list of service addresses for the Ozone SCM. This method is used
   * by the DataNodes to determine the service instances to connect to.
   *
   * @param conf
   * @return list of SCM service addresses.
   */
  public static Map<String, ? extends Map<String, InetSocketAddress>>
      getScmServiceRpcAddresses(Configuration conf) {

    final Map<String, InetSocketAddress> serviceInstances = new HashMap<>();
    serviceInstances.put(OZONE_SCM_SERVICE_INSTANCE_ID,
        getScmAddressForDataNodes(conf));

    final Map<String, Map<String, InetSocketAddress>> services =
        new HashMap<>();
    services.put(OZONE_SCM_SERVICE_ID, serviceInstances);
    return services;
  }

  public static String getOzoneDatanodeRatisDirectory(Configuration conf) {
    String storageDir = conf.get(
            OzoneConfigKeys.DFS_CONTAINER_RATIS_DATANODE_STORAGE_DIR);

    if (Strings.isNullOrEmpty(storageDir)) {
      storageDir = getDefaultRatisDirectory(conf);
    }
    return storageDir;
  }

  public static String getDefaultRatisDirectory(Configuration conf) {
    LOG.warn("Storage directory for Ratis is not configured. It is a good " +
            "idea to map this to an SSD disk. Falling back to {}",
        HddsConfigKeys.OZONE_METADATA_DIRS);
    File metaDirPath = ServerUtils.getOzoneMetaDirPath(conf);
    return (new File(metaDirPath, "ratis")).getPath();
  }

  /**
   * Get the path for datanode id file.
   *
   * @param conf - Configuration
   * @return the path of datanode id as string
   */
  public static String getDatanodeIdFilePath(Configuration conf) {
    String dataNodeIDPath = conf.get(ScmConfigKeys.OZONE_SCM_DATANODE_ID);
    if (dataNodeIDPath == null) {
      File metaDirPath = ServerUtils.getOzoneMetaDirPath(conf);
      if (metaDirPath == null) {
        // this means meta data is not found, in theory should not happen at
        // this point because should've failed earlier.
        throw new IllegalArgumentException("Unable to locate meta data" +
            "directory when getting datanode id path");
      }
      dataNodeIDPath = new File(metaDirPath,
          ScmConfigKeys.OZONE_SCM_DATANODE_ID_PATH_DEFAULT).toString();
    }
    return dataNodeIDPath;
  }
}
