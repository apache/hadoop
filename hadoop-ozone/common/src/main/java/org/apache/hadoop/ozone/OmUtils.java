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

import java.io.File;
import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.Optional;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.server.ServerUtils;
import org.apache.hadoop.net.NetUtils;

import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hadoop.hdds.HddsUtils.getHostNameFromConfigKeys;
import static org.apache.hadoop.hdds.HddsUtils.getPortNumberFromConfigKeys;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_ADDRESS_KEY;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_BIND_HOST_DEFAULT;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_HTTP_ADDRESS_KEY;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_HTTP_BIND_PORT_DEFAULT;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_PORT_DEFAULT;

/**
 * Stateless helper functions for the server and client side of OM
 * communication.
 */
public final class OmUtils {
  private static final Logger LOG = LoggerFactory.getLogger(OmUtils.class);

  private OmUtils() {
  }

  /**
   * Retrieve the socket address that is used by OM.
   * @param conf
   * @return Target InetSocketAddress for the SCM service endpoint.
   */
  public static InetSocketAddress getOmAddress(
      Configuration conf) {
    final Optional<String> host = getHostNameFromConfigKeys(conf,
        OZONE_OM_ADDRESS_KEY);

    return NetUtils.createSocketAddr(
        host.orElse(OZONE_OM_BIND_HOST_DEFAULT) + ":" +
            getOmRpcPort(conf));
  }

  /**
   * Retrieve the socket address that should be used by clients to connect
   * to OM.
   * @param conf
   * @return Target InetSocketAddress for the OM service endpoint.
   */
  public static InetSocketAddress getOmAddressForClients(
      Configuration conf) {
    final Optional<String> host = getHostNameFromConfigKeys(conf,
        OZONE_OM_ADDRESS_KEY);

    if (!host.isPresent()) {
      throw new IllegalArgumentException(
          OZONE_OM_ADDRESS_KEY + " must be defined. See" +
              " https://wiki.apache.org/hadoop/Ozone#Configuration for" +
              " details on configuring Ozone.");
    }

    return NetUtils.createSocketAddr(
        host.get() + ":" + getOmRpcPort(conf));
  }

  public static int getOmRpcPort(Configuration conf) {
    // If no port number is specified then we'll just try the defaultBindPort.
    final Optional<Integer> port = getPortNumberFromConfigKeys(conf,
        OZONE_OM_ADDRESS_KEY);
    return port.orElse(OZONE_OM_PORT_DEFAULT);
  }

  public static int getOmRestPort(Configuration conf) {
    // If no port number is specified then we'll just try the default
    // HTTP BindPort.
    final Optional<Integer> port =
        getPortNumberFromConfigKeys(conf, OZONE_OM_HTTP_ADDRESS_KEY);
    return port.orElse(OZONE_OM_HTTP_BIND_PORT_DEFAULT);
  }

  /**
   * Get the location where OM should store its metadata directories.
   * Fall back to OZONE_METADATA_DIRS if not defined.
   *
   * @param conf
   * @return
   */
  public static File getOmDbDir(Configuration conf) {
    final Collection<String> dbDirs = conf.getTrimmedStringCollection(
        OMConfigKeys.OZONE_OM_DB_DIRS);

    if (dbDirs.size() > 1) {
      throw new IllegalArgumentException(
          "Bad configuration setting " + OMConfigKeys.OZONE_OM_DB_DIRS +
              ". OM does not support multiple metadata dirs currently.");
    }

    if (dbDirs.size() == 1) {
      final File dbDirPath = new File(dbDirs.iterator().next());
      if (!dbDirPath.exists() && !dbDirPath.mkdirs()) {
        throw new IllegalArgumentException("Unable to create directory " +
            dbDirPath + " specified in configuration setting " +
            OMConfigKeys.OZONE_OM_DB_DIRS);
      }
      return dbDirPath;
    }

    LOG.warn("{} is not configured. We recommend adding this setting. " +
        "Falling back to {} instead.",
        OMConfigKeys.OZONE_OM_DB_DIRS, HddsConfigKeys.OZONE_METADATA_DIRS);
    return ServerUtils.getOzoneMetaDirPath(conf);
  }
}
