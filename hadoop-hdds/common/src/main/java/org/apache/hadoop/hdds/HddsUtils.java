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

package org.apache.hadoop.hdds;

import javax.management.ObjectName;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.Calendar;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.TimeZone;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.protocolPB.SCMSecurityProtocolClientSideTranslatorPB;
import org.apache.hadoop.hdds.protocolPB.SCMSecurityProtocolPB;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.SCMSecurityProtocol;
import org.apache.hadoop.hdds.scm.protocolPB.ScmBlockLocationProtocolPB;
import org.apache.hadoop.ipc.Client;
import org.apache.hadoop.ipc.ProtobufRpcEngine;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.metrics2.util.MBeans;
import org.apache.hadoop.net.DNS;
import org.apache.hadoop.net.NetUtils;

import com.google.common.net.HostAndPort;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_DNS_INTERFACE_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_DNS_NAMESERVER_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_HOST_NAME_KEY;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_ENABLED;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_ENABLED_DEFAULT;

import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * HDDS specific stateless utility functions.
 */
@InterfaceAudience.Private
@InterfaceStability.Stable
public final class HddsUtils {


  private static final Logger LOG = LoggerFactory.getLogger(HddsUtils.class);

  /**
   * The service ID of the solitary Ozone SCM service.
   */
  public static final String OZONE_SCM_SERVICE_ID = "OzoneScmService";
  public static final String OZONE_SCM_SERVICE_INSTANCE_ID =
      "OzoneScmServiceInstance";
  private static final TimeZone UTC_ZONE = TimeZone.getTimeZone("UTC");


  private static final int NO_PORT = -1;

  private HddsUtils() {
  }

  /**
   * Retrieve the socket address that should be used by clients to connect
   * to the SCM.
   *
   * @param conf
   * @return Target InetSocketAddress for the SCM client endpoint.
   */
  public static InetSocketAddress getScmAddressForClients(Configuration conf) {
    Optional<String> host = getHostNameFromConfigKeys(conf,
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
          ScmConfigKeys.OZONE_SCM_CLIENT_ADDRESS_KEY + " must be defined. See"
              + " https://wiki.apache.org/hadoop/Ozone#Configuration for "
              + "details"
              + " on configuring Ozone.");
    }

    final Optional<Integer> port = getPortNumberFromConfigKeys(conf,
        ScmConfigKeys.OZONE_SCM_CLIENT_ADDRESS_KEY);

    return NetUtils.createSocketAddr(host.get() + ":" + port
        .orElse(ScmConfigKeys.OZONE_SCM_CLIENT_PORT_DEFAULT));
  }

  /**
   * Retrieve the socket address that should be used by clients to connect
   * to the SCM for block service. If
   * {@link ScmConfigKeys#OZONE_SCM_BLOCK_CLIENT_ADDRESS_KEY} is not defined
   * then {@link ScmConfigKeys#OZONE_SCM_CLIENT_ADDRESS_KEY} is used. If neither
   * is defined then {@link ScmConfigKeys#OZONE_SCM_NAMES} is used.
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
    }

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
          ScmConfigKeys.OZONE_SCM_BLOCK_CLIENT_ADDRESS_KEY
              + " must be defined. See"
              + " https://wiki.apache.org/hadoop/Ozone#Configuration"
              + " for details on configuring Ozone.");
    }

    final Optional<Integer> port = getPortNumberFromConfigKeys(conf,
        ScmConfigKeys.OZONE_SCM_BLOCK_CLIENT_ADDRESS_KEY);

    return NetUtils.createSocketAddr(host.get() + ":" + port
        .orElse(ScmConfigKeys.OZONE_SCM_BLOCK_CLIENT_PORT_DEFAULT));
  }

  /**
   * Create a scm security client.
   * @param conf    - Ozone configuration.
   * @param address - inet socket address of scm.
   *
   * @return {@link SCMSecurityProtocol}
   * @throws IOException
   */
  public static SCMSecurityProtocol getScmSecurityClient(
      OzoneConfiguration conf, InetSocketAddress address) throws IOException {
    RPC.setProtocolEngine(conf, SCMSecurityProtocolPB.class,
        ProtobufRpcEngine.class);
    long scmVersion =
        RPC.getProtocolVersion(ScmBlockLocationProtocolPB.class);
    SCMSecurityProtocolClientSideTranslatorPB scmSecurityClient =
        new SCMSecurityProtocolClientSideTranslatorPB(
            RPC.getProxy(SCMSecurityProtocolPB.class, scmVersion,
                address, UserGroupInformation.getCurrentUser(),
                conf, NetUtils.getDefaultSocketFactory(conf),
                Client.getRpcTimeout(conf)));
    return scmSecurityClient;
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
    return Optional.empty();
  }

  /**
   * Gets the hostname or Indicates that it is absent.
   * @param value host or host:port
   * @return hostname
   */
  public static Optional<String> getHostName(String value) {
    if ((value == null) || value.isEmpty()) {
      return Optional.empty();
    }
    return Optional.of(HostAndPort.fromString(value).getHostText());
  }

  /**
   * Gets the port if there is one, throws otherwise.
   * @param value  String in host:port format.
   * @return Port
   */
  public static Optional<Integer> getHostPort(String value) {
    if ((value == null) || value.isEmpty()) {
      return Optional.empty();
    }
    int port = HostAndPort.fromString(value).getPortOrDefault(NO_PORT);
    if (port == NO_PORT) {
      return Optional.empty();
    } else {
      return Optional.of(port);
    }
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
    return Optional.empty();
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

    final Optional<Integer> defaultPort = Optional
        .of(ScmConfigKeys.OZONE_SCM_DEFAULT_PORT);
    for (String address : names) {
      Optional<String> hostname = getHostName(address);
      if (!hostname.isPresent()) {
        throw new IllegalArgumentException("Invalid hostname for SCM: "
            + hostname);
      }
      Optional<Integer> port = getHostPort(address);
      InetSocketAddress addr = NetUtils.createSocketAddr(hostname.get(),
          port.orElse(defaultPort.get()));
      addresses.add(addr);
    }
    return addresses;
  }

  public static boolean isHddsEnabled(Configuration conf) {
    return conf.getBoolean(OZONE_ENABLED, OZONE_ENABLED_DEFAULT);
  }


  /**
   * Returns the hostname for this datanode. If the hostname is not
   * explicitly configured in the given config, then it is determined
   * via the DNS class.
   *
   * @param conf Configuration
   *
   * @return the hostname (NB: may not be a FQDN)
   * @throws UnknownHostException if the dfs.datanode.dns.interface
   *    option is used and the hostname can not be determined
   */
  public static String getHostName(Configuration conf)
      throws UnknownHostException {
    String name = conf.get(DFS_DATANODE_HOST_NAME_KEY);
    if (name == null) {
      String dnsInterface = conf.get(
          CommonConfigurationKeys.HADOOP_SECURITY_DNS_INTERFACE_KEY);
      String nameServer = conf.get(
          CommonConfigurationKeys.HADOOP_SECURITY_DNS_NAMESERVER_KEY);
      boolean fallbackToHosts = false;

      if (dnsInterface == null) {
        // Try the legacy configuration keys.
        dnsInterface = conf.get(DFS_DATANODE_DNS_INTERFACE_KEY);
        nameServer = conf.get(DFS_DATANODE_DNS_NAMESERVER_KEY);
      } else {
        // If HADOOP_SECURITY_DNS_* is set then also attempt hosts file
        // resolution if DNS fails. We will not use hosts file resolution
        // by default to avoid breaking existing clusters.
        fallbackToHosts = true;
      }

      name = DNS.getDefaultHost(dnsInterface, nameServer, fallbackToHosts);
    }
    return name;
  }

  /**
   * Checks if the container command is read only or not.
   * @param proto ContainerCommand Request proto
   * @return True if its readOnly , false otherwise.
   */
  public static boolean isReadOnly(
      ContainerProtos.ContainerCommandRequestProto proto) {
    switch (proto.getCmdType()) {
    case ReadContainer:
    case ReadChunk:
    case ListBlock:
    case GetBlock:
    case GetSmallFile:
    case ListContainer:
    case ListChunk:
    case GetCommittedBlockLength:
      return true;
    case CloseContainer:
    case WriteChunk:
    case UpdateContainer:
    case CompactChunk:
    case CreateContainer:
    case DeleteChunk:
    case DeleteContainer:
    case DeleteBlock:
    case PutBlock:
    case PutSmallFile:
    default:
      return false;
    }
  }

  /**
   * Register the provided MBean with additional JMX ObjectName properties.
   * If additional properties are not supported then fallback to registering
   * without properties.
   *
   * @param serviceName - see {@link MBeans#register}
   * @param mBeanName - see {@link MBeans#register}
   * @param jmxProperties - additional JMX ObjectName properties.
   * @param mBean - the MBean to register.
   * @return the named used to register the MBean.
   */
  public static ObjectName registerWithJmxProperties(
      String serviceName, String mBeanName, Map<String, String> jmxProperties,
      Object mBean) {
    try {

      // Check support for registering with additional properties.
      final Method registerMethod = MBeans.class.getMethod(
          "register", String.class, String.class,
          Map.class, Object.class);

      return (ObjectName) registerMethod.invoke(
          null, serviceName, mBeanName, jmxProperties, mBean);

    } catch (NoSuchMethodException | IllegalAccessException |
        InvocationTargetException e) {

      // Fallback
      LOG.trace("Registering MBean {} without additional properties {}",
          mBeanName, jmxProperties);
      return MBeans.register(serviceName, mBeanName, mBean);
    }
  }

  /**
   * Get the current UTC time in milliseconds.
   * @return the current UTC time in milliseconds.
   */
  public static long getUtcTime() {
    return Calendar.getInstance(UTC_ZONE).getTimeInMillis();
  }

  /**
   * Retrieve the socket address that should be used by clients to connect
   * to the SCM for
   * {@link org.apache.hadoop.hdds.protocol.SCMSecurityProtocol}. If
   * {@link ScmConfigKeys#OZONE_SCM_SECURITY_SERVICE_ADDRESS_KEY} is not defined
   * then {@link ScmConfigKeys#OZONE_SCM_CLIENT_ADDRESS_KEY} is used. If neither
   * is defined then {@link ScmConfigKeys#OZONE_SCM_NAMES} is used.
   *
   * @param conf
   * @return Target InetSocketAddress for the SCM block client endpoint.
   * @throws IllegalArgumentException if configuration is not defined.
   */
  public static InetSocketAddress getScmAddressForSecurityProtocol(
      Configuration conf) {
    Optional<String> host = getHostNameFromConfigKeys(conf,
        ScmConfigKeys.OZONE_SCM_SECURITY_SERVICE_ADDRESS_KEY);

    if (!host.isPresent()) {
      host = getHostNameFromConfigKeys(conf,
          ScmConfigKeys.OZONE_SCM_CLIENT_ADDRESS_KEY);
    }

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
          ScmConfigKeys.OZONE_SCM_SECURITY_SERVICE_ADDRESS_KEY
              + " must be defined. See"
              + " https://wiki.apache.org/hadoop/Ozone#Configuration"
              + " for details on configuring Ozone.");
    }

    final Optional<Integer> port = getPortNumberFromConfigKeys(conf,
        ScmConfigKeys.OZONE_SCM_SECURITY_SERVICE_PORT_KEY);

    return NetUtils.createSocketAddr(host.get() + ":" + port
        .orElse(ScmConfigKeys.OZONE_SCM_SECURITY_SERVICE_PORT_DEFAULT));
  }

}
