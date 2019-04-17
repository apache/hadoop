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

package org.apache.hadoop.hdds.server;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.ipc.RPC;
import org.apache.http.client.methods.HttpRequestBase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.net.InetSocketAddress;
import java.util.Collection;

/**
 * Generic utilities for all HDDS/Ozone servers.
 */
public final class ServerUtils {

  private static final Logger LOG = LoggerFactory.getLogger(
      ServerUtils.class);

  private ServerUtils() {
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
  public static long sanitizeUserArgs(long valueTocheck, long baseValue,
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
   * Get the location where SCM should store its metadata directories.
   * Fall back to OZONE_METADATA_DIRS if not defined.
   *
   * @param conf
   * @return
   */
  public static File getScmDbDir(Configuration conf) {
    File metadataDir = getDirectoryFromConfig(conf,
        ScmConfigKeys.OZONE_SCM_DB_DIRS, "SCM");
    if (metadataDir != null) {
      return metadataDir;
    }

    LOG.warn("{} is not configured. We recommend adding this setting. " +
        "Falling back to {} instead.",
        ScmConfigKeys.OZONE_SCM_DB_DIRS, HddsConfigKeys.OZONE_METADATA_DIRS);
    return getOzoneMetaDirPath(conf);
  }

  /**
   * Utility method to get value of a given key that corresponds to a DB
   * directory.
   * @param conf configuration bag
   * @param key Key to test
   * @param componentName Which component's key is this
   * @return File created from the value of the key in conf.
   */
  public static File getDirectoryFromConfig(Configuration conf,
                                            String key,
                                            String componentName) {
    final Collection<String> metadirs = conf.getTrimmedStringCollection(key);

    if (metadirs.size() > 1) {
      throw new IllegalArgumentException(
          "Bad config setting " + key +
              ". " + componentName +
              " does not support multiple metadata dirs currently");
    }

    if (metadirs.size() == 1) {
      final File dbDirPath = new File(metadirs.iterator().next());
      if (!dbDirPath.exists() && !dbDirPath.mkdirs()) {
        throw new IllegalArgumentException("Unable to create directory " +
            dbDirPath + " specified in configuration setting " +
            key);
      }
      return dbDirPath;
    }

    return null;
  }

  /**
   * Checks and creates Ozone Metadir Path if it does not exist.
   *
   * @param conf - Configuration
   * @return File MetaDir
   * @throws IllegalArgumentException if the configuration setting is not set
   */
  public static File getOzoneMetaDirPath(Configuration conf) {
    File dirPath = getDirectoryFromConfig(conf,
        HddsConfigKeys.OZONE_METADATA_DIRS, "Ozone");
    if (dirPath == null) {
      throw new IllegalArgumentException(
          HddsConfigKeys.OZONE_METADATA_DIRS + " must be defined.");
    }
    return dirPath;
  }

  public static void setOzoneMetaDirPath(OzoneConfiguration conf,
      String path) {
    conf.set(HddsConfigKeys.OZONE_METADATA_DIRS, path);
  }

}
