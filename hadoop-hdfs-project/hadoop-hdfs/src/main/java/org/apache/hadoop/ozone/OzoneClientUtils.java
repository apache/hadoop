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

package org.apache.hadoop.ozone;

import com.google.common.base.Optional;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.net.NetUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;

import static org.apache.hadoop.ozone.OzoneConfigKeys.*;

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
   * Retrieve the socket address that should be used by clients to connect
   * to the SCM.
   *
   * @param conf
   * @return Target InetSocketAddress for the SCM client endpoint.
   */
  public static InetSocketAddress getScmAddressForClients(Configuration conf) {
    final Optional<String> host = getHostNameFromConfigKeys(conf,
        OZONE_SCM_CLIENT_ADDRESS_KEY);

    if (!host.isPresent()) {
      throw new IllegalArgumentException(OZONE_SCM_CLIENT_ADDRESS_KEY +
          " must be defined. See" +
          " https://wiki.apache.org/hadoop/Ozone#Configuration for details" +
          " on configuring Ozone.");
    }

    final Optional<Integer> port = getPortNumberFromConfigKeys(conf,
        OZONE_SCM_CLIENT_ADDRESS_KEY);

    return NetUtils.createSocketAddr(host.get() + ":" +
        port.or(OZONE_SCM_CLIENT_PORT_DEFAULT));
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
        OZONE_SCM_DATANODE_ADDRESS_KEY, OZONE_SCM_CLIENT_ADDRESS_KEY);

    if (!host.isPresent()) {
      throw new IllegalArgumentException(OZONE_SCM_CLIENT_ADDRESS_KEY +
          " must be defined. See" +
          " https://wiki.apache.org/hadoop/Ozone#Configuration for details" +
          " on configuring Ozone.");
    }

    // If no port number is specified then we'll just try the defaultBindPort.
    final Optional<Integer> port = getPortNumberFromConfigKeys(conf,
        OZONE_SCM_DATANODE_ADDRESS_KEY);

    return NetUtils.createSocketAddr(host.get() + ":" +
        port.or(OZONE_SCM_DATANODE_PORT_DEFAULT));
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
        OZONE_SCM_CLIENT_BIND_HOST_KEY);

    final Optional<Integer> port = getPortNumberFromConfigKeys(conf,
        OZONE_SCM_CLIENT_ADDRESS_KEY);

    return NetUtils.createSocketAddr(
        host.or(OZONE_SCM_CLIENT_BIND_HOST_DEFAULT) + ":" +
        port.or(OZONE_SCM_CLIENT_PORT_DEFAULT));
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
        OZONE_SCM_DATANODE_BIND_HOST_KEY);

    // If no port number is specified then we'll just try the defaultBindPort.
    final Optional<Integer> port = getPortNumberFromConfigKeys(conf,
        OZONE_SCM_DATANODE_ADDRESS_KEY);

    return NetUtils.createSocketAddr(
        host.or(OZONE_SCM_DATANODE_BIND_HOST_DEFAULT) + ":" +
        port.or(OZONE_SCM_DATANODE_PORT_DEFAULT));
  }

  /**
   * Retrieve the hostname, trying the supplied config keys in order.
   * Each config value may be absent, or if present in the format
   * host:port (the :port part is optional).
   *
   * @param conf
   * @param keys a list of configuration key names.
   *
   * @return first hostname component found from the given keys, or absent.
   * @throws IllegalArgumentException if any values are not in the 'host'
   *             or host:port format.
   */
  static Optional<String> getHostNameFromConfigKeys(
      Configuration conf, String ... keys) {
    for (final String key : keys) {
      final String value = conf.getTrimmed(key);
      if (value != null && !value.isEmpty()) {
        String[] splits = value.split(":");

        if(splits.length < 1 || splits.length > 2) {
          throw new IllegalArgumentException(
              "Invalid value " + value + " for config key " + key +
                  ". It should be in 'host' or 'host:port' format");
        }
        return Optional.of(splits[0]);
      }
    }
    return Optional.absent();
  }

  /**
   * Retrieve the port number, trying the supplied config keys in order.
   * Each config value may be absent, or if present in the format
   * host:port (the :port part is optional).
   *
   * @param conf
   * @param keys a list of configuration key names.
   *
   * @return first port number component found from the given keys, or absent.
   * @throws IllegalArgumentException if any values are not in the 'host'
   *             or host:port format.
   */
  static Optional<Integer> getPortNumberFromConfigKeys(
      Configuration conf, String ... keys) {
    for (final String key : keys) {
      final String value = conf.getTrimmed(key);
      if (value != null && !value.isEmpty()) {
        String[] splits = value.split(":");

        if(splits.length < 1 || splits.length > 2) {
          throw new IllegalArgumentException(
              "Invalid value " + value + " for config key " + key +
                  ". It should be in 'host' or 'host:port' format");
        }

        if (splits.length == 2) {
          return Optional.of(Integer.parseInt(splits[1]));
        }
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
}
