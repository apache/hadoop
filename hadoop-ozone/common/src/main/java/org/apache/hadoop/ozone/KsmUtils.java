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

package org.apache.hadoop.ozone;

import java.net.InetSocketAddress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.net.NetUtils;

import com.google.common.base.Optional;
import static org.apache.hadoop.hdsl.HdslUtils.getHostNameFromConfigKeys;
import static org.apache.hadoop.hdsl.HdslUtils.getPortNumberFromConfigKeys;
import static org.apache.hadoop.ozone.ksm.KSMConfigKeys.OZONE_KSM_ADDRESS_KEY;
import static org.apache.hadoop.ozone.ksm.KSMConfigKeys
    .OZONE_KSM_BIND_HOST_DEFAULT;
import static org.apache.hadoop.ozone.ksm.KSMConfigKeys.OZONE_KSM_PORT_DEFAULT;

/**
 * Stateless helper functions for the server and client side of KSM
 * communication.
 */
public class KsmUtils {

  private KsmUtils() {
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

}
