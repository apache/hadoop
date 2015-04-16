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
package org.apache.hadoop.hdfs;

import com.google.common.base.Joiner;
import com.google.common.collect.Maps;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.hdfs.web.WebHdfsConstants;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;

import static org.apache.hadoop.hdfs.client.HdfsClientConfigKeys.DFS_HA_NAMENODES_KEY_PREFIX;
import static org.apache.hadoop.hdfs.client.HdfsClientConfigKeys.DFS_NAMESERVICES;

public class DFSUtilClient {
  private static final Logger LOG = LoggerFactory.getLogger(
      DFSUtilClient.class);
  /**
   * Converts a byte array to a string using UTF8 encoding.
   */
  public static String bytes2String(byte[] bytes) {
    return bytes2String(bytes, 0, bytes.length);
  }

  /** Return used as percentage of capacity */
  public static float getPercentUsed(long used, long capacity) {
    return capacity <= 0 ? 100 : (used * 100.0f)/capacity;
  }

  /** Return remaining as percentage of capacity */
  public static float getPercentRemaining(long remaining, long capacity) {
    return capacity <= 0 ? 0 : (remaining * 100.0f)/capacity;
  }

  /** Convert percentage to a string. */
  public static String percent2String(double percentage) {
    return StringUtils.format("%.2f%%", percentage);
  }

  /**
   * Returns collection of nameservice Ids from the configuration.
   * @param conf configuration
   * @return collection of nameservice Ids, or null if not specified
   */
  public static Collection<String> getNameServiceIds(Configuration conf) {
    return conf.getTrimmedStringCollection(DFS_NAMESERVICES);
  }

  /**
   * Namenode HighAvailability related configuration.
   * Returns collection of namenode Ids from the configuration. One logical id
   * for each namenode in the in the HA setup.
   *
   * @param conf configuration
   * @param nsId the nameservice ID to look at, or null for non-federated
   * @return collection of namenode Ids
   */
  public static Collection<String> getNameNodeIds(Configuration conf, String nsId) {
    String key = addSuffix(DFS_HA_NAMENODES_KEY_PREFIX, nsId);
    return conf.getTrimmedStringCollection(key);
  }

  /** Add non empty and non null suffix to a key */
  static String addSuffix(String key, String suffix) {
    if (suffix == null || suffix.isEmpty()) {
      return key;
    }
    assert !suffix.startsWith(".") :
      "suffix '" + suffix + "' should not already have '.' prepended.";
    return key + "." + suffix;
  }

  /**
   * Returns list of InetSocketAddress corresponding to HA NN HTTP addresses from
   * the configuration.
   *
   * @return list of InetSocketAddresses
   */
  public static Map<String, Map<String, InetSocketAddress>> getHaNnWebHdfsAddresses(
      Configuration conf, String scheme) {
    if (WebHdfsConstants.WEBHDFS_SCHEME.equals(scheme)) {
      return getAddresses(conf, null,
          HdfsClientConfigKeys.DFS_NAMENODE_HTTP_ADDRESS_KEY);
    } else if (WebHdfsConstants.SWEBHDFS_SCHEME.equals(scheme)) {
      return getAddresses(conf, null,
          HdfsClientConfigKeys.DFS_NAMENODE_HTTPS_ADDRESS_KEY);
    } else {
      throw new IllegalArgumentException("Unsupported scheme: " + scheme);
    }
  }

  /**
   * Decode a specific range of bytes of the given byte array to a string
   * using UTF8.
   *
   * @param bytes The bytes to be decoded into characters
   * @param offset The index of the first byte to decode
   * @param length The number of bytes to decode
   * @return The decoded string
   */
  private static String bytes2String(byte[] bytes, int offset, int length) {
    try {
      return new String(bytes, offset, length, "UTF8");
    } catch(UnsupportedEncodingException e) {
      assert false : "UTF8 encoding is not supported ";
    }
    return null;
  }

  /**
   * @return <code>coll</code> if it is non-null and non-empty. Otherwise,
   * returns a list with a single null value.
   */
  static Collection<String> emptyAsSingletonNull(Collection<String> coll) {
    if (coll == null || coll.isEmpty()) {
      return Collections.singletonList(null);
    } else {
      return coll;
    }
  }

  /** Concatenate list of suffix strings '.' separated */
  static String concatSuffixes(String... suffixes) {
    if (suffixes == null) {
      return null;
    }
    return Joiner.on(".").skipNulls().join(suffixes);
  }

  /**
   * Returns the configured address for all NameNodes in the cluster.
   * @param conf configuration
   * @param defaultAddress default address to return in case key is not found.
   * @param keys Set of keys to look for in the order of preference
   * @return a map(nameserviceId to map(namenodeId to InetSocketAddress))
   */
  static Map<String, Map<String, InetSocketAddress>>
    getAddresses(Configuration conf, String defaultAddress, String... keys) {
    Collection<String> nameserviceIds = getNameServiceIds(conf);
    return getAddressesForNsIds(conf, nameserviceIds, defaultAddress, keys);
  }

  /**
   * Returns the configured address for all NameNodes in the cluster.
   * @param conf configuration
   * @param defaultAddress default address to return in case key is not found.
   * @param keys Set of keys to look for in the order of preference
   *
   * @return a map(nameserviceId to map(namenodeId to InetSocketAddress))
   */
  static Map<String, Map<String, InetSocketAddress>>
    getAddressesForNsIds(
      Configuration conf, Collection<String> nsIds, String defaultAddress,
      String... keys) {
    // Look for configurations of the form <key>[.<nameserviceId>][.<namenodeId>]
    // across all of the configured nameservices and namenodes.
    Map<String, Map<String, InetSocketAddress>> ret = Maps.newLinkedHashMap();
    for (String nsId : emptyAsSingletonNull(nsIds)) {
      Map<String, InetSocketAddress> isas =
        getAddressesForNameserviceId(conf, nsId, defaultAddress, keys);
      if (!isas.isEmpty()) {
        ret.put(nsId, isas);
      }
    }
    return ret;
  }

  static Map<String, InetSocketAddress> getAddressesForNameserviceId(
      Configuration conf, String nsId, String defaultValue, String... keys) {
    Collection<String> nnIds = getNameNodeIds(conf, nsId);
    Map<String, InetSocketAddress> ret = Maps.newHashMap();
    for (String nnId : emptyAsSingletonNull(nnIds)) {
      String suffix = concatSuffixes(nsId, nnId);
      String address = getConfValue(defaultValue, suffix, conf, keys);
      if (address != null) {
        InetSocketAddress isa = NetUtils.createSocketAddr(address);
        if (isa.isUnresolved()) {
          LOG.warn("Namenode for " + nsId +
                       " remains unresolved for ID " + nnId +
                   ".  Check your hdfs-site.xml file to " +
                   "ensure namenodes are configured properly.");
        }
        ret.put(nnId, isa);
      }
    }
    return ret;
  }

  /**
   * Given a list of keys in the order of preference, returns a value
   * for the key in the given order from the configuration.
   * @param defaultValue default value to return, when key was not found
   * @param keySuffix suffix to add to the key, if it is not null
   * @param conf Configuration
   * @param keys list of keys in the order of preference
   * @return value of the key or default if a key was not found in configuration
   */
  private static String getConfValue(String defaultValue, String keySuffix,
      Configuration conf, String... keys) {
    String value = null;
    for (String key : keys) {
      key = addSuffix(key, keySuffix);
      value = conf.get(key);
      if (value != null) {
        break;
      }
    }
    if (value == null) {
      value = defaultValue;
    }
    return value;
  }
}
