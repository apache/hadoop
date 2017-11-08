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

package org.apache.hadoop.ozone.client;

import com.google.common.base.Optional;

import com.google.common.base.Preconditions;
import com.google.common.net.HostAndPort;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.scm.ScmConfigKeys;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.text.ParseException;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.apache.hadoop.cblock.CBlockConfigKeys
    .DFS_CBLOCK_SERVICERPC_ADDRESS_KEY;
import static org.apache.hadoop.cblock.CBlockConfigKeys
    .DFS_CBLOCK_SERVICERPC_HOSTNAME_DEFAULT;
import static org.apache.hadoop.cblock.CBlockConfigKeys
    .DFS_CBLOCK_SERVICERPC_PORT_DEFAULT;
import static org.apache.hadoop.cblock.CBlockConfigKeys
    .DFS_CBLOCK_JSCSIRPC_ADDRESS_KEY;
import static org.apache.hadoop.cblock.CBlockConfigKeys
    .DFS_CBLOCK_JSCSI_PORT_DEFAULT;
import static org.apache.hadoop.ozone.ksm.KSMConfigKeys
    .OZONE_KSM_ADDRESS_KEY;
import static org.apache.hadoop.ozone.ksm.KSMConfigKeys
    .OZONE_KSM_BIND_HOST_DEFAULT;
import static org.apache.hadoop.ozone.ksm.KSMConfigKeys
    .OZONE_KSM_PORT_DEFAULT;
import static org.apache.hadoop.scm.ScmConfigKeys
    .OZONE_SCM_DEADNODE_INTERVAL_DEFAULT;
import static org.apache.hadoop.scm.ScmConfigKeys
    .OZONE_SCM_DEADNODE_INTERVAL_MS;
import static org.apache.hadoop.scm.ScmConfigKeys
    .OZONE_SCM_HEARTBEAT_INTERVAL_SECONDS;

import static org.apache.hadoop.scm.ScmConfigKeys
    .OZONE_SCM_HEARTBEAT_LOG_WARN_DEFAULT;
import static org.apache.hadoop.scm.ScmConfigKeys
    .OZONE_SCM_HEARTBEAT_LOG_WARN_INTERVAL_COUNT;
import static org.apache.hadoop.scm.ScmConfigKeys
    .OZONE_SCM_HEARTBEAT_PROCESS_INTERVAL_MS;

import static org.apache.hadoop.scm.ScmConfigKeys
    .OZONE_SCM_HEARTBEAT_RPC_TIMEOUT;
import static org.apache.hadoop.scm.ScmConfigKeys
    .OZONE_SCM_HEARTBEAT_RPC_TIMEOUT_DEFAULT;
import static org.apache.hadoop.scm.ScmConfigKeys
    .OZONE_SCM_STALENODE_INTERVAL_DEFAULT;
import static org.apache.hadoop.scm.ScmConfigKeys
    .OZONE_SCM_STALENODE_INTERVAL_MS;

/**
 * Utility methods for Ozone and Container Clients.
 *
 * The methods to retrieve SCM service endpoints assume there is a single
 * SCM service instance. This will change when we switch to replicated service
 * instances for redundancy.
 */
@InterfaceAudience.Public
@InterfaceStability.Unstable
public final class OzoneClientUtils {
  private static final Logger LOG = LoggerFactory.getLogger(
      OzoneClientUtils.class);
  private static final int NO_PORT = -1;

  /**
   * Date format that used in ozone. Here the format is thread safe to use.
   */
  private static final ThreadLocal<DateTimeFormatter> DATE_FORMAT =
      ThreadLocal.withInitial(() -> {
        DateTimeFormatter format =
            DateTimeFormatter.ofPattern(OzoneConsts.OZONE_DATE_FORMAT);
        return format.withZone(ZoneId.of(OzoneConsts.OZONE_TIME_ZONE));
      });

  /**
   * The service ID of the solitary Ozone SCM service.
   */
  public static final String OZONE_SCM_SERVICE_ID = "OzoneScmService";
  public static final String OZONE_SCM_SERVICE_INSTANCE_ID =
      "OzoneScmServiceInstance";

  private OzoneClientUtils() {
    // Never constructed
  }

  /**
   * Retrieve the socket addresses of all storage container managers.
   *
   * @param conf
   * @return A collection of SCM addresses
   * @throws IllegalArgumentException If the configuration is invalid
   */
  public static Collection<InetSocketAddress> getSCMAddresses(
      Configuration conf) throws IllegalArgumentException {
    Collection<InetSocketAddress> addresses =
        new HashSet<InetSocketAddress>();
    Collection<String> names =
        conf.getTrimmedStringCollection(ScmConfigKeys.OZONE_SCM_NAMES);
    if (names == null || names.isEmpty()) {
      throw new IllegalArgumentException(ScmConfigKeys.OZONE_SCM_NAMES
          + " need to be a set of valid DNS names or IP addresses."
          + " Null or empty address list found.");
    }

    final com.google.common.base.Optional<Integer>
        defaultPort =  com.google.common.base.Optional.of(ScmConfigKeys
        .OZONE_SCM_DEFAULT_PORT);
    for (String address : names) {
      com.google.common.base.Optional<String> hostname =
          OzoneClientUtils.getHostName(address);
      if (!hostname.isPresent()) {
        throw new IllegalArgumentException("Invalid hostname for SCM: "
            + hostname);
      }
      com.google.common.base.Optional<Integer> port =
          OzoneClientUtils.getHostPort(address);
      InetSocketAddress addr = NetUtils.createSocketAddr(hostname.get(),
          port.or(defaultPort.get()));
      addresses.add(addr);
    }
    return addresses;
  }

  /**
   * Retrieve the socket address that should be used by clients to connect
   * to the SCM.
   *
   * @param conf
   * @return Target InetSocketAddress for the SCM client endpoint.
   */
  public static InetSocketAddress getScmAddressForClients(Configuration conf) {
    final Optional<String> host = getHostNameFromConfigKeys(conf,
        ScmConfigKeys.OZONE_SCM_CLIENT_ADDRESS_KEY);

    if (!host.isPresent()) {
      throw new IllegalArgumentException(
          ScmConfigKeys.OZONE_SCM_CLIENT_ADDRESS_KEY +
          " must be defined. See" +
          " https://wiki.apache.org/hadoop/Ozone#Configuration for details" +
          " on configuring Ozone.");
    }

    final Optional<Integer> port = getPortNumberFromConfigKeys(conf,
        ScmConfigKeys.OZONE_SCM_CLIENT_ADDRESS_KEY);

    return NetUtils.createSocketAddr(host.get() + ":" +
        port.or(ScmConfigKeys.OZONE_SCM_CLIENT_PORT_DEFAULT));
  }

  /**
   * Retrieve the socket address that should be used by clients to connect
   * to the SCM for block service. If
   * {@link ScmConfigKeys#OZONE_SCM_BLOCK_CLIENT_ADDRESS_KEY} is not defined
   * then {@link ScmConfigKeys#OZONE_SCM_CLIENT_ADDRESS_KEY} is used.
   *
   * @param conf
   * @return Target InetSocketAddress for the SCM block client endpoint.
   * @throws IllegalArgumentException if configuration is not defined.
   */
  public static InetSocketAddress getScmAddressForBlockClients(
      Configuration conf) {
    Optional<String> host = getHostNameFromConfigKeys(conf,
        ScmConfigKeys.OZONE_SCM_BLOCK_CLIENT_ADDRESS_KEY);

    if (!host.isPresent()) {
      host = getHostNameFromConfigKeys(conf,
              ScmConfigKeys.OZONE_SCM_CLIENT_ADDRESS_KEY);
      if (!host.isPresent()) {
        throw new IllegalArgumentException(
                ScmConfigKeys.OZONE_SCM_BLOCK_CLIENT_ADDRESS_KEY +
                        " must be defined. See" +
                        " https://wiki.apache.org/hadoop/Ozone#Configuration" +
                        " for details on configuring Ozone.");
      }
    }

    final Optional<Integer> port = getPortNumberFromConfigKeys(conf,
        ScmConfigKeys.OZONE_SCM_BLOCK_CLIENT_ADDRESS_KEY);

    return NetUtils.createSocketAddr(host.get() + ":" +
        port.or(ScmConfigKeys.OZONE_SCM_BLOCK_CLIENT_PORT_DEFAULT));
  }

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
    //
    final Optional<String> host = getHostNameFromConfigKeys(conf,
        ScmConfigKeys.OZONE_SCM_DATANODE_ADDRESS_KEY,
        ScmConfigKeys.OZONE_SCM_CLIENT_ADDRESS_KEY);

    if (!host.isPresent()) {
      throw new IllegalArgumentException(
          ScmConfigKeys.OZONE_SCM_CLIENT_ADDRESS_KEY +
          " must be defined. See" +
          " https://wiki.apache.org/hadoop/Ozone#Configuration for details" +
          " on configuring Ozone.");
    }

    // If no port number is specified then we'll just try the defaultBindPort.
    final Optional<Integer> port = getPortNumberFromConfigKeys(conf,
        ScmConfigKeys.OZONE_SCM_DATANODE_ADDRESS_KEY);

    InetSocketAddress addr = NetUtils.createSocketAddr(host.get() + ":" +
        port.or(ScmConfigKeys.OZONE_SCM_DATANODE_PORT_DEFAULT));

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
        host.or(ScmConfigKeys.OZONE_SCM_CLIENT_BIND_HOST_DEFAULT) + ":" +
            port.or(ScmConfigKeys.OZONE_SCM_CLIENT_PORT_DEFAULT));
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
        host.or(ScmConfigKeys.OZONE_SCM_BLOCK_CLIENT_BIND_HOST_DEFAULT) +
            ":" + port.or(ScmConfigKeys.OZONE_SCM_BLOCK_CLIENT_PORT_DEFAULT));
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
        host.or(ScmConfigKeys.OZONE_SCM_DATANODE_BIND_HOST_DEFAULT) + ":" +
            port.or(ScmConfigKeys.OZONE_SCM_DATANODE_PORT_DEFAULT));
  }


  /**
   * Retrieve the socket address that is used by KSM.
   * @param conf
   * @return Target InetSocketAddress for the SCM service endpoint.
   */
  public static InetSocketAddress getKsmAddress(
      Configuration conf) {
    final Optional<String> host = getHostNameFromConfigKeys(conf,
        OZONE_KSM_ADDRESS_KEY);

    // If no port number is specified then we'll just try the defaultBindPort.
    final Optional<Integer> port = getPortNumberFromConfigKeys(conf,
        OZONE_KSM_ADDRESS_KEY);

    return NetUtils.createSocketAddr(
        host.or(OZONE_KSM_BIND_HOST_DEFAULT) + ":" +
            port.or(OZONE_KSM_PORT_DEFAULT));
  }

  /**
   * Retrieve the socket address that should be used by clients to connect
   * to KSM.
   * @param conf
   * @return Target InetSocketAddress for the KSM service endpoint.
   */
  public static InetSocketAddress getKsmAddressForClients(
      Configuration conf) {
    final Optional<String> host = getHostNameFromConfigKeys(conf,
        OZONE_KSM_ADDRESS_KEY);

    if (!host.isPresent()) {
      throw new IllegalArgumentException(
          OZONE_KSM_ADDRESS_KEY + " must be defined. See" +
              " https://wiki.apache.org/hadoop/Ozone#Configuration for" +
              " details on configuring Ozone.");
    }

    // If no port number is specified then we'll just try the defaultBindPort.
    final Optional<Integer> port = getPortNumberFromConfigKeys(conf,
        OZONE_KSM_ADDRESS_KEY);

    return NetUtils.createSocketAddr(
        host.get() + ":" + port.or(OZONE_KSM_PORT_DEFAULT));
  }

  /**
   * Retrieve the socket address that is used by CBlock Service.
   * @param conf
   * @return Target InetSocketAddress for the CBlock Service endpoint.
   */
  public static InetSocketAddress getCblockServiceRpcAddr(
      Configuration conf) {
    final Optional<String> host = getHostNameFromConfigKeys(conf,
        DFS_CBLOCK_SERVICERPC_ADDRESS_KEY);

    // If no port number is specified then we'll just try the defaultBindPort.
    final Optional<Integer> port = getPortNumberFromConfigKeys(conf,
        DFS_CBLOCK_SERVICERPC_ADDRESS_KEY);

    return NetUtils.createSocketAddr(
        host.or(DFS_CBLOCK_SERVICERPC_HOSTNAME_DEFAULT) + ":" +
            port.or(DFS_CBLOCK_SERVICERPC_PORT_DEFAULT));
  }

  /**
   * Retrieve the socket address that is used by CBlock Server.
   * @param conf
   * @return Target InetSocketAddress for the CBlock Server endpoint.
   */
  public static InetSocketAddress getCblockServerRpcAddr(
      Configuration conf) {
    final Optional<String> host = getHostNameFromConfigKeys(conf,
        DFS_CBLOCK_JSCSIRPC_ADDRESS_KEY);

    // If no port number is specified then we'll just try the defaultBindPort.
    final Optional<Integer> port = getPortNumberFromConfigKeys(conf,
        DFS_CBLOCK_JSCSIRPC_ADDRESS_KEY);

    return NetUtils.createSocketAddr(
        host.or(DFS_CBLOCK_SERVICERPC_HOSTNAME_DEFAULT) + ":" +
            port.or(DFS_CBLOCK_JSCSI_PORT_DEFAULT));
  }

  /**
   * Retrieve the hostname, trying the supplied config keys in order.
   * Each config value may be absent, or if present in the format
   * host:port (the :port part is optional).
   *
   * @param conf  - Conf
   * @param keys a list of configuration key names.
   *
   * @return first hostname component found from the given keys, or absent.
   * @throws IllegalArgumentException if any values are not in the 'host'
   *             or host:port format.
   */
  public static Optional<String> getHostNameFromConfigKeys(Configuration conf,
      String... keys) {
    for (final String key : keys) {
      final String value = conf.getTrimmed(key);
      final Optional<String> hostName = getHostName(value);
      if (hostName.isPresent()) {
        return hostName;
      }
    }
    return Optional.absent();
  }

  /**
   * Gets the hostname or Indicates that it is absent.
   * @param value host or host:port
   * @return hostname
   */
  public static Optional<String> getHostName(String value) {
    if ((value == null) || value.isEmpty()) {
      return Optional.absent();
    }
    return Optional.of(HostAndPort.fromString(value).getHostText());
  }

  /**
   * Gets the port if there is one, throws otherwise.
   * @param value  String in host:port format.
   * @return Port
   */
  public static Optional<Integer> getHostPort(String value) {
    if((value == null) || value.isEmpty()) {
      return Optional.absent();
    }
    int port = HostAndPort.fromString(value).getPortOrDefault(NO_PORT);
    if (port == NO_PORT) {
      return Optional.absent();
    } else {
      return Optional.of(port);
    }
  }

  /**
   * Returns the cache value to be used for list calls.
   * @param conf Configuration object
   * @return list cache size
   */
  public static int getListCacheSize(Configuration conf) {
    return conf.getInt(OzoneConfigKeys.OZONE_CLIENT_LIST_CACHE_SIZE,
        OzoneConfigKeys.OZONE_CLIENT_LIST_CACHE_SIZE_DEFAULT);
  }

  /**
   * Retrieve the port number, trying the supplied config keys in order.
   * Each config value may be absent, or if present in the format
   * host:port (the :port part is optional).
   *
   * @param conf Conf
   * @param keys a list of configuration key names.
   *
   * @return first port number component found from the given keys, or absent.
   * @throws IllegalArgumentException if any values are not in the 'host'
   *             or host:port format.
   */
  public static Optional<Integer> getPortNumberFromConfigKeys(
      Configuration conf, String... keys) {
    for (final String key : keys) {
      final String value = conf.getTrimmed(key);
      final Optional<Integer> hostPort = getHostPort(value);
      if (hostPort.isPresent()) {
        return hostPort;
      }
    }
    return Optional.absent();
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

  /**
   * Checks that a given value is with a range.
   *
   * For example, sanitizeUserArgs(17, 3, 5, 10)
   * ensures that 17 is greater/equal than 3 * 5 and less/equal to 3 * 10.
   *
   * @param valueTocheck  - value to check
   * @param baseValue     - the base value that is being used.
   * @param minFactor     - range min - a 2 here makes us ensure that value
   *                        valueTocheck is at least twice the baseValue.
   * @param maxFactor     - range max
   * @return long
   */
  private static long sanitizeUserArgs(long valueTocheck, long baseValue,
      long minFactor, long maxFactor)
      throws IllegalArgumentException {
    if ((valueTocheck >= (baseValue * minFactor)) &&
        (valueTocheck <= (baseValue * maxFactor))) {
      return valueTocheck;
    }
    String errMsg = String.format("%d is not within min = %d or max = " +
        "%d", valueTocheck, baseValue * minFactor, baseValue * maxFactor);
    throw new IllegalArgumentException(errMsg);
  }

  /**
   * Returns the interval in which the heartbeat processor thread runs.
   *
   * @param conf - Configuration
   * @return long in Milliseconds.
   */
  public static long getScmheartbeatCheckerInterval(Configuration conf) {
    return conf.getLong(OZONE_SCM_HEARTBEAT_PROCESS_INTERVAL_MS,
        ScmConfigKeys.OZONE_SCM_HEARTBEAT_PROCESS_INTERVAL_MS_DEFAULT);
  }

  /**
   * Heartbeat Interval - Defines the heartbeat frequency from a datanode to
   * SCM.
   *
   * @param conf - Ozone Config
   * @return - HB interval in seconds.
   */
  public static long getScmHeartbeatInterval(Configuration conf) {
    return conf.getTimeDuration(
        OZONE_SCM_HEARTBEAT_INTERVAL_SECONDS,
        ScmConfigKeys.OZONE_SCM_HEARBEAT_INTERVAL_SECONDS_DEFAULT,
        TimeUnit.SECONDS);
  }

  /**
   * Get the Stale Node interval, which is used by SCM to flag a datanode as
   * stale, if the heartbeat from that node has been missing for this duration.
   *
   * @param conf - Configuration.
   * @return - Long, Milliseconds to wait before flagging a node as stale.
   */
  public static long getStaleNodeInterval(Configuration conf) {

    long staleNodeIntevalMs = conf.getLong(OZONE_SCM_STALENODE_INTERVAL_MS,
        OZONE_SCM_STALENODE_INTERVAL_DEFAULT);

    long heartbeatThreadFrequencyMs = getScmheartbeatCheckerInterval(conf);

    long heartbeatIntervalMs = getScmHeartbeatInterval(conf) * 1000;


    // Make sure that StaleNodeInterval is configured way above the frequency
    // at which we run the heartbeat thread.
    //
    // Here we check that staleNodeInterval is at least five times more than the
    // frequency at which the accounting thread is going to run.
    try {
      sanitizeUserArgs(staleNodeIntevalMs, heartbeatThreadFrequencyMs, 5, 1000);
    } catch (IllegalArgumentException ex) {
      LOG.error("Stale Node Interval MS is cannot be honored due to " +
              "mis-configured {}. ex:  {}",
          OZONE_SCM_HEARTBEAT_PROCESS_INTERVAL_MS, ex);
      throw ex;
    }

    // Make sure that stale node value is greater than configured value that
    // datanodes are going to send HBs.
    try {
      sanitizeUserArgs(staleNodeIntevalMs, heartbeatIntervalMs, 3, 1000);
    } catch (IllegalArgumentException ex) {
      LOG.error("Stale Node Interval MS is cannot be honored due to " +
              "mis-configured {}. ex:  {}",
          OZONE_SCM_HEARTBEAT_INTERVAL_SECONDS, ex);
      throw ex;
    }
    return staleNodeIntevalMs;
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
    long deadNodeIntervalMs = conf.getLong(
        OZONE_SCM_DEADNODE_INTERVAL_MS, OZONE_SCM_DEADNODE_INTERVAL_DEFAULT);

    try {
      // Make sure that dead nodes Ms is at least twice the time for staleNodes
      // with a max of 1000 times the staleNodes.
      sanitizeUserArgs(deadNodeIntervalMs, staleNodeIntervalMs, 2, 1000);
    } catch (IllegalArgumentException ex) {
      LOG.error("Dead Node Interval MS is cannot be honored due to " +
              "mis-configured {}. ex:  {}",
          OZONE_SCM_STALENODE_INTERVAL_MS, ex);
      throw ex;
    }
    return deadNodeIntervalMs;
  }

  /**
   * Returns the maximum number of heartbeat to process per loop of the process
   * thread.
   * @param conf Configuration
   * @return - int -- Number of HBs to process
   */
  public static int getMaxHBToProcessPerLoop(Configuration conf) {
    return conf.getInt(ScmConfigKeys.OZONE_SCM_MAX_HB_COUNT_TO_PROCESS,
        ScmConfigKeys.OZONE_SCM_MAX_HB_COUNT_TO_PROCESS_DEFAULT);
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
   * After starting an RPC server, updates configuration with the actual
   * listening address of that server. The listening address may be different
   * from the configured address if, for example, the configured address uses
   * port 0 to request use of an ephemeral port.
   *
   * @param conf configuration to update
   * @param rpcAddressKey configuration key for RPC server address
   * @param addr configured address
   * @param rpcServer started RPC server.
   */
  public static InetSocketAddress updateRPCListenAddress(
      OzoneConfiguration conf, String rpcAddressKey,
      InetSocketAddress addr, RPC.Server rpcServer) {
    return updateListenAddress(conf, rpcAddressKey, addr,
        rpcServer.getListenerAddress());
  }

  /**
   * After starting an server, updates configuration with the actual
   * listening address of that server. The listening address may be different
   * from the configured address if, for example, the configured address uses
   * port 0 to request use of an ephemeral port.
   *
   * @param conf       configuration to update
   * @param addressKey configuration key for RPC server address
   * @param addr       configured address
   * @param listenAddr the real listening address.
   */
  public static InetSocketAddress updateListenAddress(OzoneConfiguration conf,
      String addressKey, InetSocketAddress addr, InetSocketAddress listenAddr) {
    InetSocketAddress updatedAddr = new InetSocketAddress(addr.getHostString(),
        listenAddr.getPort());
    conf.set(addressKey,
        addr.getHostString() + ":" + listenAddr.getPort());
    return updatedAddr;
  }

  /**
   * Releases a http connection if the request is not null.
   * @param request
   */
  public static void releaseConnection(HttpRequestBase request) {
    if (request != null) {
      request.releaseConnection();
    }
  }

  /**
   * @return a default instance of {@link CloseableHttpClient}.
   */
  public static CloseableHttpClient newHttpClient() {
    return OzoneClientUtils.newHttpClient(new OzoneConfiguration());
  }

  /**
   * Returns a {@link CloseableHttpClient} configured by given configuration.
   * If conf is null, returns a default instance.
   *
   * @param conf configuration
   * @return a {@link CloseableHttpClient} instance.
   */
  public static CloseableHttpClient newHttpClient(Configuration conf) {
    int socketTimeout = OzoneConfigKeys
        .OZONE_CLIENT_SOCKET_TIMEOUT_MS_DEFAULT;
    int connectionTimeout = OzoneConfigKeys
        .OZONE_CLIENT_CONNECTION_TIMEOUT_MS_DEFAULT;
    if (conf != null) {
      socketTimeout = conf.getInt(
          OzoneConfigKeys.OZONE_CLIENT_SOCKET_TIMEOUT_MS,
          OzoneConfigKeys.OZONE_CLIENT_SOCKET_TIMEOUT_MS_DEFAULT);
      connectionTimeout = conf.getInt(
          OzoneConfigKeys.OZONE_CLIENT_CONNECTION_TIMEOUT_MS,
          OzoneConfigKeys.OZONE_CLIENT_CONNECTION_TIMEOUT_MS_DEFAULT);
    }

    CloseableHttpClient client = HttpClients.custom()
        .setDefaultRequestConfig(
            RequestConfig.custom()
                .setSocketTimeout(socketTimeout)
                .setConnectTimeout(connectionTimeout)
                .build())
        .build();
    return client;
  }

  /**
   * verifies that bucket name / volume name is a valid DNS name.
   *
   * @param resName Bucket or volume Name to be validated
   *
   * @throws IllegalArgumentException
   */
  public static void verifyResourceName(String resName)
      throws IllegalArgumentException {

    if (resName == null) {
      throw new IllegalArgumentException("Bucket or Volume name is null");
    }

    if ((resName.length() < OzoneConsts.OZONE_MIN_BUCKET_NAME_LENGTH) ||
        (resName.length() > OzoneConsts.OZONE_MAX_BUCKET_NAME_LENGTH)) {
      throw new IllegalArgumentException(
          "Bucket or Volume length is illegal, " +
              "valid length is 3-63 characters");
    }

    if ((resName.charAt(0) == '.') || (resName.charAt(0) == '-')) {
      throw new IllegalArgumentException(
          "Bucket or Volume name cannot start with a period or dash");
    }

    if ((resName.charAt(resName.length() - 1) == '.') ||
        (resName.charAt(resName.length() - 1) == '-')) {
      throw new IllegalArgumentException(
          "Bucket or Volume name cannot end with a period or dash");
    }

    boolean isIPv4 = true;
    char prev = (char) 0;

    for (int index = 0; index < resName.length(); index++) {
      char currChar = resName.charAt(index);

      if (currChar != '.') {
        isIPv4 = ((currChar >= '0') && (currChar <= '9')) && isIPv4;
      }

      if (currChar > 'A' && currChar < 'Z') {
        throw new IllegalArgumentException(
            "Bucket or Volume name does not support uppercase characters");
      }

      if ((currChar != '.') && (currChar != '-')) {
        if ((currChar < '0') || (currChar > '9' && currChar < 'a') ||
            (currChar > 'z')) {
          throw new IllegalArgumentException("Bucket or Volume name has an " +
              "unsupported character : " +
              currChar);
        }
      }

      if ((prev == '.') && (currChar == '.')) {
        throw new IllegalArgumentException("Bucket or Volume name should not " +
            "have two contiguous periods");
      }

      if ((prev == '-') && (currChar == '.')) {
        throw new IllegalArgumentException(
            "Bucket or Volume name should not have period after dash");
      }

      if ((prev == '.') && (currChar == '-')) {
        throw new IllegalArgumentException(
            "Bucket or Volume name should not have dash after period");
      }
      prev = currChar;
    }

    if (isIPv4) {
      throw new IllegalArgumentException(
          "Bucket or Volume name cannot be an IPv4 address or all numeric");
    }
  }

  /**
   * Convert time in millisecond to a human readable format required in ozone.
   * @return a human readable string for the input time
   */
  public static String formatDateTime(long millis) {
    ZonedDateTime dateTime = ZonedDateTime.ofInstant(
        Instant.ofEpochSecond(millis), DATE_FORMAT.get().getZone());
    return  DATE_FORMAT.get().format(dateTime);
  }

  /**
   * Convert time in ozone date format to millisecond.
   * @return time in milliseconds
   */
  public static long formatDateTime(String date) throws ParseException {
    Preconditions.checkNotNull(date, "Date string should not be null.");
    return ZonedDateTime.parse(date, DATE_FORMAT.get())
        .toInstant().getEpochSecond();
  }
}
