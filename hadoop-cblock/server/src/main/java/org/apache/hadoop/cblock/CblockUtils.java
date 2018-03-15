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

package org.apache.hadoop.cblock;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.net.NetUtils;

import com.google.common.base.Optional;
import static org.apache.hadoop.cblock.CBlockConfigKeys
    .DFS_CBLOCK_JSCSIRPC_ADDRESS_KEY;
import static org.apache.hadoop.cblock.CBlockConfigKeys
    .DFS_CBLOCK_JSCSI_PORT_DEFAULT;
import static org.apache.hadoop.cblock.CBlockConfigKeys
    .DFS_CBLOCK_SERVICERPC_ADDRESS_KEY;
import static org.apache.hadoop.cblock.CBlockConfigKeys
    .DFS_CBLOCK_SERVICERPC_HOSTNAME_DEFAULT;
import static org.apache.hadoop.cblock.CBlockConfigKeys
    .DFS_CBLOCK_SERVICERPC_PORT_DEFAULT;
import static org.apache.hadoop.hdsl.HdslUtils.getHostNameFromConfigKeys;
import static org.apache.hadoop.hdsl.HdslUtils.getPortNumberFromConfigKeys;

/**
 * Generic stateless utility functions for CBlock components.
 */
public class CblockUtils {

  private CblockUtils() {
  }

  /**
   * Retrieve the socket address that is used by CBlock Service.
   *
   * @param conf
   * @return Target InetSocketAddress for the CBlock Service endpoint.
   */
  public static InetSocketAddress getCblockServiceRpcAddr(Configuration conf) {
    final Optional<String> host =
        getHostNameFromConfigKeys(conf, DFS_CBLOCK_SERVICERPC_ADDRESS_KEY);

    // If no port number is specified then we'll just try the defaultBindPort.
    final Optional<Integer> port =
        getPortNumberFromConfigKeys(conf, DFS_CBLOCK_SERVICERPC_ADDRESS_KEY);

    return NetUtils.createSocketAddr(
        host.or(DFS_CBLOCK_SERVICERPC_HOSTNAME_DEFAULT) + ":" + port
            .or(DFS_CBLOCK_SERVICERPC_PORT_DEFAULT));
  }

  /**
   * Retrieve the socket address that is used by CBlock Server.
   *
   * @param conf
   * @return Target InetSocketAddress for the CBlock Server endpoint.
   */
  public static InetSocketAddress getCblockServerRpcAddr(Configuration conf) {
    final Optional<String> host =
        getHostNameFromConfigKeys(conf, DFS_CBLOCK_JSCSIRPC_ADDRESS_KEY);

    // If no port number is specified then we'll just try the defaultBindPort.
    final Optional<Integer> port =
        getPortNumberFromConfigKeys(conf, DFS_CBLOCK_JSCSIRPC_ADDRESS_KEY);

    return NetUtils.createSocketAddr(
        host.or(DFS_CBLOCK_SERVICERPC_HOSTNAME_DEFAULT) + ":" + port
            .or(DFS_CBLOCK_JSCSI_PORT_DEFAULT));
  }

  /**
   * Parse size with size prefix string and return in bytes.
   *
   */
  public static long parseSize(String volumeSizeArgs) throws IOException {
    long multiplier = 1;

    Pattern p = Pattern.compile("([0-9]+)([a-zA-Z]+)");
    Matcher m = p.matcher(volumeSizeArgs);

    if (!m.find()) {
      throw new IOException("Invalid volume size args " + volumeSizeArgs);
    }

    int size = Integer.parseInt(m.group(1));
    String s = m.group(2);

    if (s.equalsIgnoreCase("MB") ||
        s.equalsIgnoreCase("Mi")) {
      multiplier = 1024L * 1024;
    } else if (s.equalsIgnoreCase("GB") ||
        s.equalsIgnoreCase("Gi")) {
      multiplier = 1024L * 1024 * 1024;
    } else if (s.equalsIgnoreCase("TB") ||
        s.equalsIgnoreCase("Ti")) {
      multiplier = 1024L * 1024 * 1024 * 1024;
    } else {
      throw new IOException("Invalid volume size args " + volumeSizeArgs);
    }
    return size * multiplier;
  }

  public static void activateConfigs(){
    Configuration.addDefaultResource("hdfs-default.xml");
    Configuration.addDefaultResource("hdfs-site.xml");
    Configuration.addDefaultResource("ozone-default.xml");
    Configuration.addDefaultResource("ozone-site.xml");
    Configuration.addDefaultResource("cblock-default.xml");
    Configuration.addDefaultResource("cblock-site.xml");

  }
}