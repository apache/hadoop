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

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_ADMIN;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_CLIENT_HTTPS_NEED_AUTH_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_CLIENT_HTTPS_NEED_AUTH_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_HA_NAMENODES_KEY_PREFIX;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_HA_NAMENODE_ID_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_BACKUP_ADDRESS_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_HTTPS_ADDRESS_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_HTTPS_ADDRESS_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_HTTP_ADDRESS_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_HTTP_ADDRESS_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_RPC_ADDRESS_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_SECONDARY_HTTP_ADDRESS_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_SERVICE_RPC_ADDRESS_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMESERVICES;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMESERVICE_ID;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_SERVER_HTTPS_KEYPASSWORD_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_SERVER_HTTPS_KEYSTORE_PASSWORD_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_SERVER_HTTPS_TRUSTSTORE_PASSWORD_KEY;

import java.io.IOException;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import javax.net.SocketFactory;

import com.google.common.collect.Sets;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.crypto.key.KeyProvider;
import org.apache.hadoop.crypto.key.KeyProviderCryptoExtension;
import org.apache.hadoop.crypto.key.KeyProviderFactory;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.protocol.ClientDatanodeProtocol;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.protocolPB.ClientDatanodeProtocolTranslatorPB;
import org.apache.hadoop.hdfs.server.namenode.FSDirectory;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.web.SWebHdfsFileSystem;
import org.apache.hadoop.hdfs.web.WebHdfsFileSystem;
import org.apache.hadoop.http.HttpConfig;
import org.apache.hadoop.http.HttpServer2;
import org.apache.hadoop.ipc.ProtobufRpcEngine;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.net.NodeBase;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.AccessControlList;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.ToolRunner;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.primitives.SignedBytes;
import com.google.protobuf.BlockingService;

@InterfaceAudience.Private
public class DFSUtil {
  public static final Log LOG = LogFactory.getLog(DFSUtil.class.getName());
  
  public static final byte[] EMPTY_BYTES = {};

  /** Compare two byte arrays by lexicographical order. */
  public static int compareBytes(byte[] left, byte[] right) {
    if (left == null) {
      left = EMPTY_BYTES;
    }
    if (right == null) {
      right = EMPTY_BYTES;
    }
    return SignedBytes.lexicographicalComparator().compare(left, right);
  }

  private DFSUtil() { /* Hidden constructor */ }
  private static final ThreadLocal<Random> RANDOM = new ThreadLocal<Random>() {
    @Override
    protected Random initialValue() {
      return new Random();
    }
  };
  
  private static final ThreadLocal<SecureRandom> SECURE_RANDOM = new ThreadLocal<SecureRandom>() {
    @Override
    protected SecureRandom initialValue() {
      return new SecureRandom();
    }
  };

  /** @return a pseudo random number generator. */
  public static Random getRandom() {
    return RANDOM.get();
  }
  
  /** @return a pseudo secure random number generator. */
  public static SecureRandom getSecureRandom() {
    return SECURE_RANDOM.get();
  }

  /** Shuffle the elements in the given array. */
  public static <T> T[] shuffle(final T[] array) {
    if (array != null && array.length > 0) {
      final Random random = getRandom();
      for (int n = array.length; n > 1; ) {
        final int randomIndex = random.nextInt(n);
        n--;
        if (n != randomIndex) {
          final T tmp = array[randomIndex];
          array[randomIndex] = array[n];
          array[n] = tmp;
        }
      }
    }
    return array;
  }

  /**
   * Compartor for sorting DataNodeInfo[] based on decommissioned states.
   * Decommissioned nodes are moved to the end of the array on sorting with
   * this compartor.
   */
  public static final Comparator<DatanodeInfo> DECOM_COMPARATOR = 
    new Comparator<DatanodeInfo>() {
      @Override
      public int compare(DatanodeInfo a, DatanodeInfo b) {
        return a.isDecommissioned() == b.isDecommissioned() ? 0 : 
          a.isDecommissioned() ? 1 : -1;
      }
    };


  /**
   * Comparator for sorting DataNodeInfo[] based on decommissioned/stale states.
   * Decommissioned/stale nodes are moved to the end of the array on sorting
   * with this comparator.
   */ 
  @InterfaceAudience.Private 
  public static class DecomStaleComparator implements Comparator<DatanodeInfo> {
    private final long staleInterval;

    /**
     * Constructor of DecomStaleComparator
     * 
     * @param interval
     *          The time interval for marking datanodes as stale is passed from
     *          outside, since the interval may be changed dynamically
     */
    public DecomStaleComparator(long interval) {
      this.staleInterval = interval;
    }

    @Override
    public int compare(DatanodeInfo a, DatanodeInfo b) {
      // Decommissioned nodes will still be moved to the end of the list
      if (a.isDecommissioned()) {
        return b.isDecommissioned() ? 0 : 1;
      } else if (b.isDecommissioned()) {
        return -1;
      }
      // Stale nodes will be moved behind the normal nodes
      boolean aStale = a.isStale(staleInterval);
      boolean bStale = b.isStale(staleInterval);
      return aStale == bStale ? 0 : (aStale ? 1 : -1);
    }
  }    
    
  /**
   * Address matcher for matching an address to local address
   */
  static final AddressMatcher LOCAL_ADDRESS_MATCHER = new AddressMatcher() {
    @Override
    public boolean match(InetSocketAddress s) {
      return NetUtils.isLocalAddress(s.getAddress());
    };
  };
  
  /**
   * Whether the pathname is valid.  Currently prohibits relative paths, 
   * names which contain a ":" or "//", or other non-canonical paths.
   */
  public static boolean isValidName(String src) {
    // Path must be absolute.
    if (!src.startsWith(Path.SEPARATOR)) {
      return false;
    }
      
    // Check for ".." "." ":" "/"
    String[] components = StringUtils.split(src, '/');
    for (int i = 0; i < components.length; i++) {
      String element = components[i];
      if (element.equals(".")  ||
          (element.indexOf(":") >= 0)  ||
          (element.indexOf("/") >= 0)) {
        return false;
      }
      // ".." is allowed in path starting with /.reserved/.inodes
      if (element.equals("..")) {
        if (components.length > 4
            && components[1].equals(FSDirectory.DOT_RESERVED_STRING)
            && components[2].equals(FSDirectory.DOT_INODES_STRING)) {
          continue;
        }
        return false;
      }
      // The string may start or end with a /, but not have
      // "//" in the middle.
      if (element.isEmpty() && i != components.length - 1 &&
          i != 0) {
        return false;
      }
    }
    return true;
  }

  /**
   * Checks if a string is a valid path component. For instance, components
   * cannot contain a ":" or "/", and cannot be equal to a reserved component
   * like ".snapshot".
   * <p>
   * The primary use of this method is for validating paths when loading the
   * FSImage. During normal NN operation, paths are sometimes allowed to
   * contain reserved components.
   * 
   * @return If component is valid
   */
  public static boolean isValidNameForComponent(String component) {
    if (component.equals(".") ||
        component.equals("..") ||
        component.indexOf(":") >= 0 ||
        component.indexOf("/") >= 0) {
      return false;
    }
    return !isReservedPathComponent(component);
  }


  /**
   * Returns if the component is reserved.
   * 
   * <p>
   * Note that some components are only reserved under certain directories, e.g.
   * "/.reserved" is reserved, while "/hadoop/.reserved" is not.
   * @return true, if the component is reserved
   */
  public static boolean isReservedPathComponent(String component) {
    for (String reserved : HdfsConstants.RESERVED_PATH_COMPONENTS) {
      if (component.equals(reserved)) {
        return true;
      }
    }
    return false;
  }

  /**
   * Converts a byte array to a string using UTF8 encoding.
   */
  public static String bytes2String(byte[] bytes) {
    return bytes2String(bytes, 0, bytes.length);
  }

  // Using the charset canonical name for String/byte[] conversions is much
  // more efficient due to use of cached encoders/decoders.
  private static final String UTF8_CSN = StandardCharsets.UTF_8.name();

  /**
   * Decode a specific range of bytes of the given byte array to a string
   * using UTF8.
   * 
   * @param bytes The bytes to be decoded into characters
   * @param offset The index of the first byte to decode
   * @param length The number of bytes to decode
   * @return The decoded string
   */
  public static String bytes2String(byte[] bytes, int offset, int length) {
    try {
      return new String(bytes, offset, length, UTF8_CSN);
    } catch (UnsupportedEncodingException e) {
      // should never happen!
      throw new IllegalArgumentException("UTF8 encoding is not supported", e);
    }
  }

  /**
   * Converts a string to a byte array using UTF8 encoding.
   */
  public static byte[] string2Bytes(String str) {
    try {
      return str.getBytes(UTF8_CSN);
    } catch (UnsupportedEncodingException e) {
      // should never happen!
      throw new IllegalArgumentException("UTF8 decoding is not supported", e);
    }

  }

  /**
   * Given a list of path components returns a path as a UTF8 String
   */
  public static String byteArray2PathString(final byte[][] components,
      final int offset, final int length) {
    // specifically not using StringBuilder to more efficiently build
    // string w/o excessive byte[] copies and charset conversions.
    final int range = offset + length;
    Preconditions.checkPositionIndexes(offset, range, components.length);
    if (length == 0) {
      return "";
    }
    // absolute paths start with either null or empty byte[]
    byte[] firstComponent = components[offset];
    boolean isAbsolute = (offset == 0 &&
        (firstComponent == null || firstComponent.length == 0));
    if (offset == 0 && length == 1) {
      return isAbsolute ? Path.SEPARATOR : bytes2String(firstComponent);
    }
    // compute length of full byte[], seed with 1st component and delimiters
    int pos = isAbsolute ? 0 : firstComponent.length;
    int size = pos + length - 1;
    for (int i=offset + 1; i < range; i++) {
      size += components[i].length;
    }
    final byte[] result = new byte[size];
    if (!isAbsolute) {
      System.arraycopy(firstComponent, 0, result, 0, firstComponent.length);
    }
    // append remaining components as "/component".
    for (int i=offset + 1; i < range; i++) {
      result[pos++] = (byte)Path.SEPARATOR_CHAR;
      int len = components[i].length;
      System.arraycopy(components[i], 0, result, pos, len);
      pos += len;
    }
    return bytes2String(result);
  }

  public static String byteArray2PathString(byte[][] pathComponents) {
    return byteArray2PathString(pathComponents, 0, pathComponents.length);
  }

  /**
   * Converts a list of path components into a path using Path.SEPARATOR.
   * 
   * @param components Path components
   * @return Combined path as a UTF-8 string
   */
  public static String strings2PathString(String[] components) {
    if (components.length == 0) {
      return "";
    }
    if (components.length == 1) {
      if (components[0] == null || components[0].isEmpty()) {
        return Path.SEPARATOR;
      }
    }
    return Joiner.on(Path.SEPARATOR).join(components);
  }

  /**
   * Given a list of path components returns a byte array
   */
  public static byte[] byteArray2bytes(byte[][] pathComponents) {
    if (pathComponents.length == 0) {
      return EMPTY_BYTES;
    } else if (pathComponents.length == 1
        && (pathComponents[0] == null || pathComponents[0].length == 0)) {
      return new byte[]{(byte) Path.SEPARATOR_CHAR};
    }
    int length = 0;
    for (int i = 0; i < pathComponents.length; i++) {
      length += pathComponents[i].length;
      if (i < pathComponents.length - 1) {
        length++; // for SEPARATOR
      }
    }
    byte[] path = new byte[length];
    int index = 0;
    for (int i = 0; i < pathComponents.length; i++) {
      System.arraycopy(pathComponents[i], 0, path, index,
          pathComponents[i].length);
      index += pathComponents[i].length;
      if (i < pathComponents.length - 1) {
        path[index] = (byte) Path.SEPARATOR_CHAR;
        index++;
      }
    }
    return path;
  }

  /** Convert an object representing a path to a string. */
  public static String path2String(final Object path) {
    return path == null? null
        : path instanceof String? (String)path
        : path instanceof byte[][]? byteArray2PathString((byte[][])path)
        : path.toString();
  }

  /**
   * Convert a UTF8 string to an array of byte arrays.
   */
  public static byte[][] getPathComponents(String path) {
    // avoid intermediate split to String[]
    final byte[] bytes = string2Bytes(path);
    return bytes2byteArray(bytes, bytes.length, (byte)Path.SEPARATOR_CHAR);
  }

  /**
   * Splits the array of bytes into array of arrays of bytes
   * on byte separator
   * @param bytes the array of bytes to split
   * @param separator the delimiting byte
   */
  public static byte[][] bytes2byteArray(byte[] bytes, byte separator) {
    return bytes2byteArray(bytes, bytes.length, separator);
  }

  /**
   * Splits first len bytes in bytes to array of arrays of bytes
   * on byte separator
   * @param bytes the byte array to split
   * @param len the number of bytes to split
   * @param separator the delimiting byte
   */
  public static byte[][] bytes2byteArray(byte[] bytes,
                                         int len,
                                         byte separator) {
    Preconditions.checkPositionIndex(len, bytes.length);
    if (len == 0) {
      return new byte[][]{null};
    }
    // Count the splits. Omit multiple separators and the last one by
    // peeking at prior byte.
    int splits = 0;
    for (int i = 1; i < len; i++) {
      if (bytes[i-1] == separator && bytes[i] != separator) {
        splits++;
      }
    }
    if (splits == 0 && bytes[0] == separator) {
      return new byte[][]{null};
    }
    splits++;
    byte[][] result = new byte[splits][];
    int nextIndex = 0;
    // Build the splits.
    for (int i = 0; i < splits; i++) {
      int startIndex = nextIndex;
      // find next separator in the bytes.
      while (nextIndex < len && bytes[nextIndex] != separator) {
        nextIndex++;
      }
      result[i] = (nextIndex > 0)
          ? Arrays.copyOfRange(bytes, startIndex, nextIndex)
          : EMPTY_BYTES; // reuse empty bytes for root.
      do { // skip over separators.
        nextIndex++;
      } while (nextIndex < len && bytes[nextIndex] == separator);
    }
    return result;
  }
  
  /**
   * Convert a LocatedBlocks to BlockLocations[]
   * @param blocks a LocatedBlocks
   * @return an array of BlockLocations
   */
  public static BlockLocation[] locatedBlocks2Locations(LocatedBlocks blocks) {
    if (blocks == null) {
      return new BlockLocation[0];
    }
    return locatedBlocks2Locations(blocks.getLocatedBlocks());
  }
  
  /**
   * Convert a List<LocatedBlock> to BlockLocation[]
   * @param blocks A List<LocatedBlock> to be converted
   * @return converted array of BlockLocation
   */
  public static BlockLocation[] locatedBlocks2Locations(List<LocatedBlock> blocks) {
    if (blocks == null) {
      return new BlockLocation[0];
    }
    int nrBlocks = blocks.size();
    BlockLocation[] blkLocations = new BlockLocation[nrBlocks];
    if (nrBlocks == 0) {
      return blkLocations;
    }
    int idx = 0;
    for (LocatedBlock blk : blocks) {
      assert idx < nrBlocks : "Incorrect index";
      DatanodeInfo[] locations = blk.getLocations();
      String[] hosts = new String[locations.length];
      String[] xferAddrs = new String[locations.length];
      String[] racks = new String[locations.length];
      for (int hCnt = 0; hCnt < locations.length; hCnt++) {
        hosts[hCnt] = locations[hCnt].getHostName();
        xferAddrs[hCnt] = locations[hCnt].getXferAddr();
        NodeBase node = new NodeBase(xferAddrs[hCnt], 
                                     locations[hCnt].getNetworkLocation());
        racks[hCnt] = node.toString();
      }
      DatanodeInfo[] cachedLocations = blk.getCachedLocations();
      String[] cachedHosts = new String[cachedLocations.length];
      for (int i=0; i<cachedLocations.length; i++) {
        cachedHosts[i] = cachedLocations[i].getHostName();
      }
      blkLocations[idx] = new BlockLocation(xferAddrs, hosts, cachedHosts,
                                            racks,
                                            blk.getStartOffset(),
                                            blk.getBlockSize(),
                                            blk.isCorrupt());
      idx++;
    }
    return blkLocations;
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
   * @return <code>coll</code> if it is non-null and non-empty. Otherwise,
   * returns a list with a single null value.
   */
  private static Collection<String> emptyAsSingletonNull(Collection<String> coll) {
    if (coll == null || coll.isEmpty()) {
      return Collections.singletonList(null);
    } else {
      return coll;
    }
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
  
  /** Add non empty and non null suffix to a key */
  private static String addSuffix(String key, String suffix) {
    if (suffix == null || suffix.isEmpty()) {
      return key;
    }
    assert !suffix.startsWith(".") :
      "suffix '" + suffix + "' should not already have '.' prepended.";
    return key + "." + suffix;
  }
  
  /** Concatenate list of suffix strings '.' separated */
  private static String concatSuffixes(String... suffixes) {
    if (suffixes == null) {
      return null;
    }
    return Joiner.on(".").skipNulls().join(suffixes);
  }
  
  /**
   * Return configuration key of format key.suffix1.suffix2...suffixN
   */
  public static String addKeySuffixes(String key, String... suffixes) {
    String keySuffix = concatSuffixes(suffixes);
    return addSuffix(key, keySuffix);
  }

  /**
   * Returns the configured address for all NameNodes in the cluster.
   * @param conf configuration
   * @param defaultAddress default address to return in case key is not found.
   * @param keys Set of keys to look for in the order of preference
   * @return a map(nameserviceId to map(namenodeId to InetSocketAddress))
   */
  private static Map<String, Map<String, InetSocketAddress>>
    getAddresses(Configuration conf, String defaultAddress, String... keys) {
    Collection<String> nameserviceIds = getNameServiceIds(conf);
    return getAddressesForNsIds(conf, nameserviceIds, defaultAddress, keys);
  }

  /**
   * Returns the configured address for all NameNodes in the cluster.
   * @param conf configuration
   * @param nsIds
   *@param defaultAddress default address to return in case key is not found.
   * @param keys Set of keys to look for in the order of preference   @return a map(nameserviceId to map(namenodeId to InetSocketAddress))
   */
  private static Map<String, Map<String, InetSocketAddress>>
    getAddressesForNsIds(Configuration conf, Collection<String> nsIds,
                         String defaultAddress, String... keys) {
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
  
  /**
   * Get all of the RPC addresses of the individual NNs in a given nameservice.
   * 
   * @param conf Configuration
   * @param nsId the nameservice whose NNs addresses we want.
   * @param defaultValue default address to return in case key is not found.
   * @return A map from nnId -> RPC address of each NN in the nameservice.
   */
  public static Map<String, InetSocketAddress> getRpcAddressesForNameserviceId(
      Configuration conf, String nsId, String defaultValue) {
    return getAddressesForNameserviceId(conf, nsId, defaultValue,
        DFS_NAMENODE_RPC_ADDRESS_KEY);
  }

  private static Map<String, InetSocketAddress> getAddressesForNameserviceId(
      Configuration conf, String nsId, String defaultValue,
      String... keys) {
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
   * @return a collection of all configured NN Kerberos principals.
   */
  public static Set<String> getAllNnPrincipals(Configuration conf) throws IOException {
    Set<String> principals = new HashSet<String>();
    for (String nsId : DFSUtil.getNameServiceIds(conf)) {
      if (HAUtil.isHAEnabled(conf, nsId)) {
        for (String nnId : DFSUtil.getNameNodeIds(conf, nsId)) {
          Configuration confForNn = new Configuration(conf);
          NameNode.initializeGenericKeys(confForNn, nsId, nnId);
          String principal = SecurityUtil.getServerPrincipal(confForNn
              .get(DFSConfigKeys.DFS_NAMENODE_KERBEROS_PRINCIPAL_KEY),
              NameNode.getAddress(confForNn).getHostName());
          principals.add(principal);
        }
      } else {
        Configuration confForNn = new Configuration(conf);
        NameNode.initializeGenericKeys(confForNn, nsId, null);
        String principal = SecurityUtil.getServerPrincipal(confForNn
            .get(DFSConfigKeys.DFS_NAMENODE_KERBEROS_PRINCIPAL_KEY),
            NameNode.getAddress(confForNn).getHostName());
        principals.add(principal);
      }
    }

    return principals;
  }

  /**
   * Returns list of InetSocketAddress corresponding to HA NN RPC addresses from
   * the configuration.
   * 
   * @param conf configuration
   * @return list of InetSocketAddresses
   */
  public static Map<String, Map<String, InetSocketAddress>> getHaNnRpcAddresses(
      Configuration conf) {
    return getAddresses(conf, null, DFSConfigKeys.DFS_NAMENODE_RPC_ADDRESS_KEY);
  }

  /**
   * Returns list of InetSocketAddress corresponding to HA NN HTTP addresses from
   * the configuration.
   *
   * @return list of InetSocketAddresses
   */
  public static Map<String, Map<String, InetSocketAddress>> getHaNnWebHdfsAddresses(
      Configuration conf, String scheme) {
    if (WebHdfsFileSystem.SCHEME.equals(scheme)) {
      return getAddresses(conf, null,
          DFSConfigKeys.DFS_NAMENODE_HTTP_ADDRESS_KEY);
    } else if (SWebHdfsFileSystem.SCHEME.equals(scheme)) {
      return getAddresses(conf, null,
          DFSConfigKeys.DFS_NAMENODE_HTTPS_ADDRESS_KEY);
    } else {
      throw new IllegalArgumentException("Unsupported scheme: " + scheme);
    }
  }

  /**
   * Returns list of InetSocketAddress corresponding to  backup node rpc 
   * addresses from the configuration.
   * 
   * @param conf configuration
   * @return list of InetSocketAddresses
   * @throws IOException on error
   */
  public static Map<String, Map<String, InetSocketAddress>> getBackupNodeAddresses(
      Configuration conf) throws IOException {
    Map<String, Map<String, InetSocketAddress>> addressList = getAddresses(conf,
        null, DFS_NAMENODE_BACKUP_ADDRESS_KEY);
    if (addressList.isEmpty()) {
      throw new IOException("Incorrect configuration: backup node address "
          + DFS_NAMENODE_BACKUP_ADDRESS_KEY + " is not configured.");
    }
    return addressList;
  }

  /**
   * Returns list of InetSocketAddresses of corresponding to secondary namenode
   * http addresses from the configuration.
   * 
   * @param conf configuration
   * @return list of InetSocketAddresses
   * @throws IOException on error
   */
  public static Map<String, Map<String, InetSocketAddress>> getSecondaryNameNodeAddresses(
      Configuration conf) throws IOException {
    Map<String, Map<String, InetSocketAddress>> addressList = getAddresses(conf, null,
        DFS_NAMENODE_SECONDARY_HTTP_ADDRESS_KEY);
    if (addressList.isEmpty()) {
      throw new IOException("Incorrect configuration: secondary namenode address "
          + DFS_NAMENODE_SECONDARY_HTTP_ADDRESS_KEY + " is not configured.");
    }
    return addressList;
  }

  /**
   * Returns list of InetSocketAddresses corresponding to namenodes from the
   * configuration.
   * 
   * Returns namenode address specifically configured for datanodes (using
   * service ports), if found. If not, regular RPC address configured for other
   * clients is returned.
   * 
   * @param conf configuration
   * @return list of InetSocketAddress
   * @throws IOException on error
   */
  public static Map<String, Map<String, InetSocketAddress>> getNNServiceRpcAddresses(
      Configuration conf) throws IOException {
    // Use default address as fall back
    String defaultAddress;
    try {
      defaultAddress = NetUtils.getHostPortString(NameNode.getAddress(conf));
    } catch (IllegalArgumentException e) {
      defaultAddress = null;
    }
    
    Map<String, Map<String, InetSocketAddress>> addressList =
      getAddresses(conf, defaultAddress,
        DFS_NAMENODE_SERVICE_RPC_ADDRESS_KEY, DFS_NAMENODE_RPC_ADDRESS_KEY);
    if (addressList.isEmpty()) {
      throw new IOException("Incorrect configuration: namenode address "
          + DFS_NAMENODE_SERVICE_RPC_ADDRESS_KEY + " or "  
          + DFS_NAMENODE_RPC_ADDRESS_KEY
          + " is not configured.");
    }
    return addressList;
  }

  /**
   * Returns list of InetSocketAddresses corresponding to the namenode
   * that manages this cluster. Note this is to be used by datanodes to get
   * the list of namenode addresses to talk to.
   *
   * Returns namenode address specifically configured for datanodes (using
   * service ports), if found. If not, regular RPC address configured for other
   * clients is returned.
   *
   * @param conf configuration
   * @return list of InetSocketAddress
   * @throws IOException on error
   */
  public static Map<String, Map<String, InetSocketAddress>>
    getNNServiceRpcAddressesForCluster(Configuration conf) throws IOException {
    // Use default address as fall back
    String defaultAddress;
    try {
      defaultAddress = NetUtils.getHostPortString(NameNode.getAddress(conf));
    } catch (IllegalArgumentException e) {
      defaultAddress = null;
    }

    Collection<String> parentNameServices = conf.getTrimmedStringCollection
            (DFSConfigKeys.DFS_INTERNAL_NAMESERVICES_KEY);

    if (parentNameServices.isEmpty()) {
      parentNameServices = conf.getTrimmedStringCollection
              (DFSConfigKeys.DFS_NAMESERVICES);
    } else {
      // Ensure that the internal service is ineed in the list of all available
      // nameservices.
      Set<String> availableNameServices = Sets.newHashSet(conf
              .getTrimmedStringCollection(DFSConfigKeys.DFS_NAMESERVICES));
      for (String nsId : parentNameServices) {
        if (!availableNameServices.contains(nsId)) {
          throw new IOException("Unknown nameservice: " + nsId);
        }
      }
    }

    Map<String, Map<String, InetSocketAddress>> addressList =
            getAddressesForNsIds(conf, parentNameServices, defaultAddress,
                    DFS_NAMENODE_SERVICE_RPC_ADDRESS_KEY, DFS_NAMENODE_RPC_ADDRESS_KEY);
    if (addressList.isEmpty()) {
      throw new IOException("Incorrect configuration: namenode address "
              + DFS_NAMENODE_SERVICE_RPC_ADDRESS_KEY + " or "
              + DFS_NAMENODE_RPC_ADDRESS_KEY
              + " is not configured.");
    }
    return addressList;
  }

  /**
   * Flatten the given map, as returned by other functions in this class,
   * into a flat list of {@link ConfiguredNNAddress} instances.
   */
  public static List<ConfiguredNNAddress> flattenAddressMap(
      Map<String, Map<String, InetSocketAddress>> map) {
    List<ConfiguredNNAddress> ret = Lists.newArrayList();
    
    for (Map.Entry<String, Map<String, InetSocketAddress>> entry :
      map.entrySet()) {
      String nsId = entry.getKey();
      Map<String, InetSocketAddress> nnMap = entry.getValue();
      for (Map.Entry<String, InetSocketAddress> e2 : nnMap.entrySet()) {
        String nnId = e2.getKey();
        InetSocketAddress addr = e2.getValue();
        
        ret.add(new ConfiguredNNAddress(nsId, nnId, addr));
      }
    }
    return ret;
  }

  /**
   * Format the given map, as returned by other functions in this class,
   * into a string suitable for debugging display. The format of this string
   * should not be considered an interface, and is liable to change.
   */
  public static String addressMapToString(
      Map<String, Map<String, InetSocketAddress>> map) {
    StringBuilder b = new StringBuilder();
    for (Map.Entry<String, Map<String, InetSocketAddress>> entry :
         map.entrySet()) {
      String nsId = entry.getKey();
      Map<String, InetSocketAddress> nnMap = entry.getValue();
      b.append("Nameservice <").append(nsId).append(">:").append("\n");
      for (Map.Entry<String, InetSocketAddress> e2 : nnMap.entrySet()) {
        b.append("  NN ID ").append(e2.getKey())
          .append(" => ").append(e2.getValue()).append("\n");
      }
    }
    return b.toString();
  }
  
  public static String nnAddressesAsString(Configuration conf) {
    Map<String, Map<String, InetSocketAddress>> addresses =
      getHaNnRpcAddresses(conf);
    return addressMapToString(addresses);
  }

  /**
   * Represent one of the NameNodes configured in the cluster.
   */
  public static class ConfiguredNNAddress {
    private final String nameserviceId;
    private final String namenodeId;
    private final InetSocketAddress addr;

    private ConfiguredNNAddress(String nameserviceId, String namenodeId,
        InetSocketAddress addr) {
      this.nameserviceId = nameserviceId;
      this.namenodeId = namenodeId;
      this.addr = addr;
    }

    public String getNameserviceId() {
      return nameserviceId;
    }

    public String getNamenodeId() {
      return namenodeId;
    }

    public InetSocketAddress getAddress() {
      return addr;
    }
    
    @Override
    public String toString() {
      return "ConfiguredNNAddress[nsId=" + nameserviceId + ";" +
        "nnId=" + namenodeId + ";addr=" + addr + "]";
    }
  }

  /** @return Internal name services specified in the conf. */
  static Collection<String> getInternalNameServices(Configuration conf) {
    final Collection<String> ids = conf.getTrimmedStringCollection(
        DFSConfigKeys.DFS_INTERNAL_NAMESERVICES_KEY);
    return !ids.isEmpty()? ids: getNameServiceIds(conf);
  }

  /**
   * Get a URI for each internal nameservice. If a nameservice is
   * HA-enabled, and the configured failover proxy provider supports logical
   * URIs, then the logical URI of the nameservice is returned.
   * Otherwise, a URI corresponding to an RPC address of the single NN for that
   * nameservice is returned, preferring the service RPC address over the
   * client RPC address.
   * 
   * @param conf configuration
   * @return a collection of all configured NN URIs, preferring service
   *         addresses
   */
  public static Collection<URI> getInternalNsRpcUris(Configuration conf) {
    return getNameServiceUris(conf, getInternalNameServices(conf),
        DFSConfigKeys.DFS_NAMENODE_SERVICE_RPC_ADDRESS_KEY,
        DFSConfigKeys.DFS_NAMENODE_RPC_ADDRESS_KEY);
  }

  /**
   * Get a URI for each configured nameservice. If a nameservice is
   * HA-enabled, and the configured failover proxy provider supports logical
   * URIs, then the logical URI of the nameservice is returned.
   * Otherwise, a URI corresponding to the address of the single NN for that
   * nameservice is returned.
   * 
   * @param conf configuration
   * @param keys configuration keys to try in order to get the URI for non-HA
   *        nameservices
   * @return a collection of all configured NN URIs
   */
  static Collection<URI> getNameServiceUris(Configuration conf,
      Collection<String> nameServices, String... keys) {
    Set<URI> ret = new HashSet<URI>();
    
    // We're passed multiple possible configuration keys for any given NN or HA
    // nameservice, and search the config in order of these keys. In order to
    // make sure that a later config lookup (e.g. fs.defaultFS) doesn't add a
    // URI for a config key for which we've already found a preferred entry, we
    // keep track of non-preferred keys here.
    Set<URI> nonPreferredUris = new HashSet<URI>();
    
    for (String nsId : nameServices) {
      URI nsUri;
      try {
        nsUri = new URI(HdfsConstants.HDFS_URI_SCHEME + "://" + nsId);
      } catch (URISyntaxException ue) {
        throw new IllegalArgumentException(ue);
      }
      /**
       * Determine whether the logical URI of the name service can be resolved
       * by the configured failover proxy provider. If not, we should try to
       * resolve the URI here
       */
      boolean useLogicalUri = false;
      try {
        useLogicalUri = HAUtil.useLogicalUri(conf, nsUri);
      } catch (IOException e){
        LOG.warn("Getting exception  while trying to determine if nameservice "
            + nsId + " can use logical URI: " + e);
      }
      if (HAUtil.isHAEnabled(conf, nsId) && useLogicalUri) {
        // Add the logical URI of the nameservice.
        ret.add(nsUri);
      } else {
        // Add the URI corresponding to the address of the NN.
        boolean uriFound = false;
        for (String key : keys) {
          String addr = conf.get(concatSuffixes(key, nsId));
          if (addr != null) {
            URI uri = createUri(HdfsConstants.HDFS_URI_SCHEME,
                NetUtils.createSocketAddr(addr));
            if (!uriFound) {
              uriFound = true;
              ret.add(uri);
            } else {
              nonPreferredUris.add(uri);
            }
          }
        }
      }
    }
    
    // Add the generic configuration keys.
    boolean uriFound = false;
    for (String key : keys) {
      String addr = conf.get(key);
      if (addr != null) {
        URI uri = createUri("hdfs", NetUtils.createSocketAddr(addr));
        if (!uriFound) {
          uriFound = true;
          ret.add(uri);
        } else {
          nonPreferredUris.add(uri);
        }
      }
    }
    
    // Add the default URI if it is an HDFS URI.
    URI defaultUri = FileSystem.getDefaultUri(conf);
    // checks if defaultUri is ip:port format
    // and convert it to hostname:port format
    if (defaultUri != null && (defaultUri.getPort() != -1)) {
      defaultUri = createUri(defaultUri.getScheme(),
          NetUtils.createSocketAddr(defaultUri.getHost(), 
              defaultUri.getPort()));
    }
    if (defaultUri != null &&
        HdfsConstants.HDFS_URI_SCHEME.equals(defaultUri.getScheme()) &&
        !nonPreferredUris.contains(defaultUri)) {
      ret.add(defaultUri);
    }
    
    return ret;
  }

  /**
   * Given the InetSocketAddress this method returns the nameservice Id
   * corresponding to the key with matching address, by doing a reverse 
   * lookup on the list of nameservices until it finds a match.
   * 
   * Since the process of resolving URIs to Addresses is slightly expensive,
   * this utility method should not be used in performance-critical routines.
   * 
   * @param conf - configuration
   * @param address - InetSocketAddress for configured communication with NN.
   *     Configured addresses are typically given as URIs, but we may have to
   *     compare against a URI typed in by a human, or the server name may be
   *     aliased, so we compare unambiguous InetSocketAddresses instead of just
   *     comparing URI substrings.
   * @param keys - list of configured communication parameters that should
   *     be checked for matches.  For example, to compare against RPC addresses,
   *     provide the list DFS_NAMENODE_SERVICE_RPC_ADDRESS_KEY,
   *     DFS_NAMENODE_RPC_ADDRESS_KEY.  Use the generic parameter keys,
   *     not the NameServiceId-suffixed keys.
   * @return nameserviceId, or null if no match found
   */
  public static String getNameServiceIdFromAddress(final Configuration conf, 
      final InetSocketAddress address, String... keys) {
    // Configuration with a single namenode and no nameserviceId
    String[] ids = getSuffixIDs(conf, address, keys);
    return (ids != null) ? ids[0] : null;
  }
  
  /**
   * return server http or https address from the configuration for a
   * given namenode rpc address.
   * @param namenodeAddr - namenode RPC address
   * @param conf configuration
   * @param scheme - the scheme (http / https)
   * @return server http or https address
   * @throws IOException 
   */
  public static URI getInfoServer(InetSocketAddress namenodeAddr,
      Configuration conf, String scheme) throws IOException {
    String[] suffixes = null;
    if (namenodeAddr != null) {
      // if non-default namenode, try reverse look up 
      // the nameServiceID if it is available
      suffixes = getSuffixIDs(conf, namenodeAddr,
          DFSConfigKeys.DFS_NAMENODE_SERVICE_RPC_ADDRESS_KEY,
          DFSConfigKeys.DFS_NAMENODE_RPC_ADDRESS_KEY);
    }

    String authority;
    if ("http".equals(scheme)) {
      authority = getSuffixedConf(conf, DFS_NAMENODE_HTTP_ADDRESS_KEY,
          DFS_NAMENODE_HTTP_ADDRESS_DEFAULT, suffixes);
    } else if ("https".equals(scheme)) {
      authority = getSuffixedConf(conf, DFS_NAMENODE_HTTPS_ADDRESS_KEY,
          DFS_NAMENODE_HTTPS_ADDRESS_DEFAULT, suffixes);
    } else {
      throw new IllegalArgumentException("Invalid scheme:" + scheme);
    }

    if (namenodeAddr != null) {
      authority = substituteForWildcardAddress(authority,
          namenodeAddr.getHostName());
    }
    return URI.create(scheme + "://" + authority);
  }

  /**
   * Lookup the HTTP / HTTPS address of the namenode, and replace its hostname
   * with defaultHost when it found out that the address is a wildcard / local
   * address.
   *
   * @param defaultHost
   *          The default host name of the namenode.
   * @param conf
   *          The configuration
   * @param scheme
   *          HTTP or HTTPS
   * @throws IOException
   */
  public static URI getInfoServerWithDefaultHost(String defaultHost,
      Configuration conf, final String scheme) throws IOException {
    URI configuredAddr = getInfoServer(null, conf, scheme);
    String authority = substituteForWildcardAddress(
        configuredAddr.getAuthority(), defaultHost);
    return URI.create(scheme + "://" + authority);
  }

  /**
   * Determine whether HTTP or HTTPS should be used to connect to the remote
   * server. Currently the client only connects to the server via HTTPS if the
   * policy is set to HTTPS_ONLY.
   *
   * @return the scheme (HTTP / HTTPS)
   */
  public static String getHttpClientScheme(Configuration conf) {
    HttpConfig.Policy policy = DFSUtil.getHttpPolicy(conf);
    return policy == HttpConfig.Policy.HTTPS_ONLY ? "https" : "http";
  }

  /**
   * Substitute a default host in the case that an address has been configured
   * with a wildcard. This is used, for example, when determining the HTTP
   * address of the NN -- if it's configured to bind to 0.0.0.0, we want to
   * substitute the hostname from the filesystem URI rather than trying to
   * connect to 0.0.0.0.
   * @param configuredAddress the address found in the configuration
   * @param defaultHost the host to substitute with, if configuredAddress
   * is a local/wildcard address.
   * @return the substituted address
   * @throws IOException if it is a wildcard address and security is enabled
   */
  @VisibleForTesting
  static String substituteForWildcardAddress(String configuredAddress,
    String defaultHost) throws IOException {
    InetSocketAddress sockAddr = NetUtils.createSocketAddr(configuredAddress);
    InetSocketAddress defaultSockAddr = NetUtils.createSocketAddr(defaultHost
        + ":0");
    final InetAddress addr = sockAddr.getAddress();
    if (addr != null && addr.isAnyLocalAddress()) {
      if (UserGroupInformation.isSecurityEnabled() &&
          defaultSockAddr.getAddress().isAnyLocalAddress()) {
        throw new IOException("Cannot use a wildcard address with security. " +
            "Must explicitly set bind address for Kerberos");
      }
      return defaultHost + ":" + sockAddr.getPort();
    } else {
      return configuredAddress;
    }
  }
  
  private static String getSuffixedConf(Configuration conf,
      String key, String defaultVal, String[] suffixes) {
    String ret = conf.get(DFSUtil.addKeySuffixes(key, suffixes));
    if (ret != null) {
      return ret;
    }
    return conf.get(key, defaultVal);
  }
  
  /**
   * Sets the node specific setting into generic configuration key. Looks up
   * value of "key.nameserviceId.namenodeId" and if found sets that value into 
   * generic key in the conf. If this is not found, falls back to
   * "key.nameserviceId" and then the unmodified key.
   *
   * Note that this only modifies the runtime conf.
   * 
   * @param conf
   *          Configuration object to lookup specific key and to set the value
   *          to the key passed. Note the conf object is modified.
   * @param nameserviceId
   *          nameservice Id to construct the node specific key. Pass null if
   *          federation is not configuration.
   * @param nnId
   *          namenode Id to construct the node specific key. Pass null if
   *          HA is not configured.
   * @param keys
   *          The key for which node specific value is looked up
   */
  public static void setGenericConf(Configuration conf,
      String nameserviceId, String nnId, String... keys) {
    for (String key : keys) {
      String value = conf.get(addKeySuffixes(key, nameserviceId, nnId));
      if (value != null) {
        conf.set(key, value);
        continue;
      }
      value = conf.get(addKeySuffixes(key, nameserviceId));
      if (value != null) {
        conf.set(key, value);
      }
    }
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
   * Round bytes to GiB (gibibyte)
   * @param bytes number of bytes
   * @return number of GiB
   */
  public static int roundBytesToGB(long bytes) {
    return Math.round((float)bytes/ 1024 / 1024 / 1024);
  }
  
  /** Create a {@link ClientDatanodeProtocol} proxy */
  public static ClientDatanodeProtocol createClientDatanodeProtocolProxy(
      DatanodeID datanodeid, Configuration conf, int socketTimeout,
      boolean connectToDnViaHostname, LocatedBlock locatedBlock) throws IOException {
    return new ClientDatanodeProtocolTranslatorPB(datanodeid, conf, socketTimeout,
        connectToDnViaHostname, locatedBlock);
  }
  
  /** Create {@link ClientDatanodeProtocol} proxy using kerberos ticket */
  public static ClientDatanodeProtocol createClientDatanodeProtocolProxy(
      DatanodeID datanodeid, Configuration conf, int socketTimeout,
      boolean connectToDnViaHostname) throws IOException {
    return new ClientDatanodeProtocolTranslatorPB(
        datanodeid, conf, socketTimeout, connectToDnViaHostname);
  }
  
  /** Create a {@link ClientDatanodeProtocol} proxy */
  public static ClientDatanodeProtocol createClientDatanodeProtocolProxy(
      InetSocketAddress addr, UserGroupInformation ticket, Configuration conf,
      SocketFactory factory) throws IOException {
    return new ClientDatanodeProtocolTranslatorPB(addr, ticket, conf, factory);
  }

  /**
   * Get nameservice Id for the {@link NameNode} based on namenode RPC address
   * matching the local node address.
   */
  public static String getNamenodeNameServiceId(Configuration conf) {
    return getNameServiceId(conf, DFS_NAMENODE_RPC_ADDRESS_KEY);
  }
  
  /**
   * Get nameservice Id for the BackupNode based on backup node RPC address
   * matching the local node address.
   */
  public static String getBackupNameServiceId(Configuration conf) {
    return getNameServiceId(conf, DFS_NAMENODE_BACKUP_ADDRESS_KEY);
  }
  
  /**
   * Get nameservice Id for the secondary node based on secondary http address
   * matching the local node address.
   */
  public static String getSecondaryNameServiceId(Configuration conf) {
    return getNameServiceId(conf, DFS_NAMENODE_SECONDARY_HTTP_ADDRESS_KEY);
  }
  
  /**
   * Get the nameservice Id by matching the {@code addressKey} with the
   * the address of the local node. 
   * 
   * If {@link DFSConfigKeys#DFS_NAMESERVICE_ID} is not specifically
   * configured, and more than one nameservice Id is configured, this method 
   * determines the nameservice Id by matching the local node's address with the
   * configured addresses. When a match is found, it returns the nameservice Id
   * from the corresponding configuration key.
   * 
   * @param conf Configuration
   * @param addressKey configuration key to get the address.
   * @return nameservice Id on success, null if federation is not configured.
   * @throws HadoopIllegalArgumentException on error
   */
  private static String getNameServiceId(Configuration conf, String addressKey) {
    String nameserviceId = conf.get(DFS_NAMESERVICE_ID);
    if (nameserviceId != null) {
      return nameserviceId;
    }
    Collection<String> nsIds = getNameServiceIds(conf);
    if (1 == nsIds.size()) {
      return nsIds.toArray(new String[1])[0];
    }
    String nnId = conf.get(DFS_HA_NAMENODE_ID_KEY);
    
    return getSuffixIDs(conf, addressKey, null, nnId, LOCAL_ADDRESS_MATCHER)[0];
  }
  
  /**
   * Returns nameservice Id and namenode Id when the local host matches the
   * configuration parameter {@code addressKey}.<nameservice Id>.<namenode Id>
   * 
   * @param conf Configuration
   * @param addressKey configuration key corresponding to the address.
   * @param knownNsId only look at configs for the given nameservice, if not-null
   * @param knownNNId only look at configs for the given namenode, if not null
   * @param matcher matching criteria for matching the address
   * @return Array with nameservice Id and namenode Id on success. First element
   *         in the array is nameservice Id and second element is namenode Id.
   *         Null value indicates that the configuration does not have the the
   *         Id.
   * @throws HadoopIllegalArgumentException on error
   */
  static String[] getSuffixIDs(final Configuration conf, final String addressKey,
      String knownNsId, String knownNNId,
      final AddressMatcher matcher) {
    String nameserviceId = null;
    String namenodeId = null;
    int found = 0;
    
    Collection<String> nsIds = getNameServiceIds(conf);
    for (String nsId : emptyAsSingletonNull(nsIds)) {
      if (knownNsId != null && !knownNsId.equals(nsId)) {
        continue;
      }
      
      Collection<String> nnIds = getNameNodeIds(conf, nsId);
      for (String nnId : emptyAsSingletonNull(nnIds)) {
        if (LOG.isTraceEnabled()) {
          LOG.trace(String.format("addressKey: %s nsId: %s nnId: %s",
              addressKey, nsId, nnId));
        }
        if (knownNNId != null && !knownNNId.equals(nnId)) {
          continue;
        }
        String key = addKeySuffixes(addressKey, nsId, nnId);
        String addr = conf.get(key);
        if (addr == null) {
          continue;
        }
        InetSocketAddress s = null;
        try {
          s = NetUtils.createSocketAddr(addr);
        } catch (Exception e) {
          LOG.warn("Exception in creating socket address " + addr, e);
          continue;
        }
        if (!s.isUnresolved() && matcher.match(s)) {
          nameserviceId = nsId;
          namenodeId = nnId;
          found++;
        }
      }
    }
    if (found > 1) { // Only one address must match the local address
      String msg = "Configuration has multiple addresses that match "
          + "local node's address. Please configure the system with "
          + DFS_NAMESERVICE_ID + " and "
          + DFS_HA_NAMENODE_ID_KEY;
      throw new HadoopIllegalArgumentException(msg);
    }
    return new String[] { nameserviceId, namenodeId };
  }
  
  /**
   * For given set of {@code keys} adds nameservice Id and or namenode Id
   * and returns {nameserviceId, namenodeId} when address match is found.
   * @see #getSuffixIDs(Configuration, String, String, String, AddressMatcher)
   */
  static String[] getSuffixIDs(final Configuration conf,
      final InetSocketAddress address, final String... keys) {
    AddressMatcher matcher = new AddressMatcher() {
     @Override
      public boolean match(InetSocketAddress s) {
        return address.equals(s);
      } 
    };
    
    for (String key : keys) {
      String[] ids = getSuffixIDs(conf, key, null, null, matcher);
      if (ids != null && (ids [0] != null || ids[1] != null)) {
        return ids;
      }
    }
    return null;
  }
  
  private interface AddressMatcher {
    public boolean match(InetSocketAddress s);
  }

  /** Create a URI from the scheme and address */
  public static URI createUri(String scheme, InetSocketAddress address) {
    try {
      return new URI(scheme, null, address.getHostName(), address.getPort(),
          null, null, null);
    } catch (URISyntaxException ue) {
      throw new IllegalArgumentException(ue);
    }
  }
  
  /**
   * Add protobuf based protocol to the {@link org.apache.hadoop.ipc.RPC.Server}
   * @param conf configuration
   * @param protocol Protocol interface
   * @param service service that implements the protocol
   * @param server RPC server to which the protocol & implementation is added to
   * @throws IOException
   */
  public static void addPBProtocol(Configuration conf, Class<?> protocol,
      BlockingService service, RPC.Server server) throws IOException {
    RPC.setProtocolEngine(conf, protocol, ProtobufRpcEngine.class);
    server.addProtocol(RPC.RpcKind.RPC_PROTOCOL_BUFFER, protocol, service);
  }

  /**
   * Map a logical namenode ID to its service address. Use the given
   * nameservice if specified, or the configured one if none is given.
   *
   * @param conf Configuration
   * @param nsId which nameservice nnId is a part of, optional
   * @param nnId the namenode ID to get the service addr for
   * @return the service addr, null if it could not be determined
   */
  public static String getNamenodeServiceAddr(final Configuration conf,
      String nsId, String nnId) {

    if (nsId == null) {
      nsId = getOnlyNameServiceIdOrNull(conf);
    }

    String serviceAddrKey = concatSuffixes(
        DFSConfigKeys.DFS_NAMENODE_SERVICE_RPC_ADDRESS_KEY, nsId, nnId);

    String addrKey = concatSuffixes(
        DFSConfigKeys.DFS_NAMENODE_RPC_ADDRESS_KEY, nsId, nnId);

    String serviceRpcAddr = conf.get(serviceAddrKey);
    if (serviceRpcAddr == null) {
      serviceRpcAddr = conf.get(addrKey);
    }
    return serviceRpcAddr;
  }

  /**
   * If the configuration refers to only a single nameservice, return the
   * name of that nameservice. If it refers to 0 or more than 1, return null.
   */
  public static String getOnlyNameServiceIdOrNull(Configuration conf) {
    Collection<String> nsIds = getNameServiceIds(conf);
    if (1 == nsIds.size()) {
      return nsIds.toArray(new String[1])[0];
    } else {
      // No nameservice ID was given and more than one is configured
      return null;
    }
  }
  
  public static final Options helpOptions = new Options();
  public static final Option helpOpt = new Option("h", "help", false,
      "get help information");

  static {
    helpOptions.addOption(helpOpt);
  }

  /**
   * Parse the arguments for commands
   * 
   * @param args the argument to be parsed
   * @param helpDescription help information to be printed out
   * @param out Printer
   * @param printGenericCommandUsage whether to print the 
   *              generic command usage defined in ToolRunner
   * @return true when the argument matches help option, false if not
   */
  public static boolean parseHelpArgument(String[] args,
      String helpDescription, PrintStream out, boolean printGenericCommandUsage) {
    if (args.length == 1) {
      try {
        CommandLineParser parser = new PosixParser();
        CommandLine cmdLine = parser.parse(helpOptions, args);
        if (cmdLine.hasOption(helpOpt.getOpt())
            || cmdLine.hasOption(helpOpt.getLongOpt())) {
          // should print out the help information
          out.println(helpDescription + "\n");
          if (printGenericCommandUsage) {
            ToolRunner.printGenericCommandUsage(out);
          }
          return true;
        }
      } catch (ParseException pe) {
        return false;
      }
    }
    return false;
  }
  
  /**
   * Get DFS_NAMENODE_INVALIDATE_WORK_PCT_PER_ITERATION from configuration.
   * 
   * @param conf Configuration
   * @return Value of DFS_NAMENODE_INVALIDATE_WORK_PCT_PER_ITERATION
   */
  public static float getInvalidateWorkPctPerIteration(Configuration conf) {
    float blocksInvalidateWorkPct = conf.getFloat(
        DFSConfigKeys.DFS_NAMENODE_INVALIDATE_WORK_PCT_PER_ITERATION,
        DFSConfigKeys.DFS_NAMENODE_INVALIDATE_WORK_PCT_PER_ITERATION_DEFAULT);
    Preconditions.checkArgument(
        (blocksInvalidateWorkPct > 0 && blocksInvalidateWorkPct <= 1.0f),
        DFSConfigKeys.DFS_NAMENODE_INVALIDATE_WORK_PCT_PER_ITERATION +
        " = '" + blocksInvalidateWorkPct + "' is invalid. " +
        "It should be a positive, non-zero float value, not greater than 1.0f, " +
        "to indicate a percentage.");
    return blocksInvalidateWorkPct;
  }

  /**
   * Get DFS_NAMENODE_REPLICATION_WORK_MULTIPLIER_PER_ITERATION from
   * configuration.
   * 
   * @param conf Configuration
   * @return Value of DFS_NAMENODE_REPLICATION_WORK_MULTIPLIER_PER_ITERATION
   */
  public static int getReplWorkMultiplier(Configuration conf) {
    int blocksReplWorkMultiplier = conf.getInt(
            DFSConfigKeys.DFS_NAMENODE_REPLICATION_WORK_MULTIPLIER_PER_ITERATION,
            DFSConfigKeys.DFS_NAMENODE_REPLICATION_WORK_MULTIPLIER_PER_ITERATION_DEFAULT);
    Preconditions.checkArgument(
        (blocksReplWorkMultiplier > 0),
        DFSConfigKeys.DFS_NAMENODE_REPLICATION_WORK_MULTIPLIER_PER_ITERATION +
        " = '" + blocksReplWorkMultiplier + "' is invalid. " +
        "It should be a positive, non-zero integer value.");
    return blocksReplWorkMultiplier;
  }
  
  /**
   * Get SPNEGO keytab Key from configuration
   * 
   * @param conf Configuration
   * @param defaultKey default key to be used for config lookup
   * @return DFS_WEB_AUTHENTICATION_KERBEROS_KEYTAB_KEY if the key is not empty
   *         else return defaultKey
   */
  public static String getSpnegoKeytabKey(Configuration conf, String defaultKey) {
    String value = 
        conf.get(DFSConfigKeys.DFS_WEB_AUTHENTICATION_KERBEROS_KEYTAB_KEY);
    return (value == null || value.isEmpty()) ?
        defaultKey : DFSConfigKeys.DFS_WEB_AUTHENTICATION_KERBEROS_KEYTAB_KEY;
  }

  /**
   * Get http policy. Http Policy is chosen as follows:
   * <ol>
   * <li>If hadoop.ssl.enabled is set, http endpoints are not started. Only
   * https endpoints are started on configured https ports</li>
   * <li>This configuration is overridden by dfs.https.enable configuration, if
   * it is set to true. In that case, both http and https endpoints are stared.</li>
   * <li>All the above configurations are overridden by dfs.http.policy
   * configuration. With this configuration you can set http-only, https-only
   * and http-and-https endpoints.</li>
   * </ol>
   * See hdfs-default.xml documentation for more details on each of the above
   * configuration settings.
   */
  public static HttpConfig.Policy getHttpPolicy(Configuration conf) {
    String policyStr = conf.get(DFSConfigKeys.DFS_HTTP_POLICY_KEY);
    if (policyStr == null) {
      boolean https = conf.getBoolean(DFSConfigKeys.DFS_HTTPS_ENABLE_KEY,
          DFSConfigKeys.DFS_HTTPS_ENABLE_DEFAULT);

      boolean hadoopSsl = conf.getBoolean(
          CommonConfigurationKeys.HADOOP_SSL_ENABLED_KEY,
          CommonConfigurationKeys.HADOOP_SSL_ENABLED_DEFAULT);

      if (hadoopSsl) {
        LOG.warn(CommonConfigurationKeys.HADOOP_SSL_ENABLED_KEY
            + " is deprecated. Please use " + DFSConfigKeys.DFS_HTTP_POLICY_KEY
            + ".");
      }
      if (https) {
        LOG.warn(DFSConfigKeys.DFS_HTTPS_ENABLE_KEY
            + " is deprecated. Please use " + DFSConfigKeys.DFS_HTTP_POLICY_KEY
            + ".");
      }

      return (hadoopSsl || https) ? HttpConfig.Policy.HTTP_AND_HTTPS
          : HttpConfig.Policy.HTTP_ONLY;
    }

    HttpConfig.Policy policy = HttpConfig.Policy.fromString(policyStr);
    if (policy == null) {
      throw new HadoopIllegalArgumentException("Unregonized value '"
          + policyStr + "' for " + DFSConfigKeys.DFS_HTTP_POLICY_KEY);
    }

    conf.set(DFSConfigKeys.DFS_HTTP_POLICY_KEY, policy.name());
    return policy;
  }

  public static HttpServer2.Builder loadSslConfToHttpServerBuilder(HttpServer2.Builder builder,
      Configuration sslConf) {
    return builder
        .needsClientAuth(
            sslConf.getBoolean(DFS_CLIENT_HTTPS_NEED_AUTH_KEY,
                DFS_CLIENT_HTTPS_NEED_AUTH_DEFAULT))
        .keyPassword(getPassword(sslConf, DFS_SERVER_HTTPS_KEYPASSWORD_KEY))
        .keyStore(sslConf.get("ssl.server.keystore.location"),
            getPassword(sslConf, DFS_SERVER_HTTPS_KEYSTORE_PASSWORD_KEY),
            sslConf.get("ssl.server.keystore.type", "jks"))
        .trustStore(sslConf.get("ssl.server.truststore.location"),
            getPassword(sslConf, DFS_SERVER_HTTPS_TRUSTSTORE_PASSWORD_KEY),
            sslConf.get("ssl.server.truststore.type", "jks"))
        .excludeCiphers(
            sslConf.get("ssl.server.exclude.cipher.list"));
  }

  /**
   * Load HTTPS-related configuration.
   */
  public static Configuration loadSslConfiguration(Configuration conf) {
    Configuration sslConf = new Configuration(false);

    sslConf.addResource(conf.get(
        DFSConfigKeys.DFS_SERVER_HTTPS_KEYSTORE_RESOURCE_KEY,
        DFSConfigKeys.DFS_SERVER_HTTPS_KEYSTORE_RESOURCE_DEFAULT));

    boolean requireClientAuth = conf.getBoolean(DFS_CLIENT_HTTPS_NEED_AUTH_KEY,
        DFS_CLIENT_HTTPS_NEED_AUTH_DEFAULT);
    sslConf.setBoolean(DFS_CLIENT_HTTPS_NEED_AUTH_KEY, requireClientAuth);
    return sslConf;
  }

  /**
   * Return a HttpServer.Builder that the journalnode / namenode / secondary
   * namenode can use to initialize their HTTP / HTTPS server.
   *
   */
  public static HttpServer2.Builder httpServerTemplateForNNAndJN(
      Configuration conf, final InetSocketAddress httpAddr,
      final InetSocketAddress httpsAddr, String name, String spnegoUserNameKey,
      String spnegoKeytabFileKey) throws IOException {
    HttpConfig.Policy policy = getHttpPolicy(conf);

    HttpServer2.Builder builder = new HttpServer2.Builder().setName(name)
        .setConf(conf).setACL(new AccessControlList(conf.get(DFS_ADMIN, " ")))
        .setSecurityEnabled(UserGroupInformation.isSecurityEnabled())
        .setUsernameConfKey(spnegoUserNameKey)
        .setKeytabConfKey(getSpnegoKeytabKey(conf, spnegoKeytabFileKey));

    // initialize the webserver for uploading/downloading files.
    if (UserGroupInformation.isSecurityEnabled()) {
      LOG.info("Starting web server as: "
          + SecurityUtil.getServerPrincipal(conf.get(spnegoUserNameKey),
              httpAddr.getHostName()));
    }

    if (policy.isHttpEnabled()) {
      if (httpAddr.getPort() == 0) {
        builder.setFindPort(true);
      }

      URI uri = URI.create("http://" + NetUtils.getHostPortString(httpAddr));
      builder.addEndpoint(uri);
      LOG.info("Starting Web-server for " + name + " at: " + uri);
    }

    if (policy.isHttpsEnabled() && httpsAddr != null) {
      Configuration sslConf = loadSslConfiguration(conf);
      loadSslConfToHttpServerBuilder(builder, sslConf);

      if (httpsAddr.getPort() == 0) {
        builder.setFindPort(true);
      }

      URI uri = URI.create("https://" + NetUtils.getHostPortString(httpsAddr));
      builder.addEndpoint(uri);
      LOG.info("Starting Web-server for " + name + " at: " + uri);
    }
    return builder;
  }

  /**
   * Leverages the Configuration.getPassword method to attempt to get
   * passwords from the CredentialProvider API before falling back to
   * clear text in config - if falling back is allowed.
   * @param conf Configuration instance
   * @param alias name of the credential to retreive
   * @return String credential value or null
   */
  static String getPassword(Configuration conf, String alias) {
    String password = null;
    try {
      char[] passchars = conf.getPassword(alias);
      if (passchars != null) {
        password = new String(passchars);
      }
    }
    catch (IOException ioe) {
      password = null;
    }
    return password;
  }

  /**
   * Converts a Date into an ISO-8601 formatted datetime string.
   */
  public static String dateToIso8601String(Date date) {
    SimpleDateFormat df =
        new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssZ", Locale.ENGLISH);
    return df.format(date);
  }

  /**
   * Converts a time duration in milliseconds into DDD:HH:MM:SS format.
   */
  public static String durationToString(long durationMs) {
    boolean negative = false;
    if (durationMs < 0) {
      negative = true;
      durationMs = -durationMs;
    }
    // Chop off the milliseconds
    long durationSec = durationMs / 1000;
    final int secondsPerMinute = 60;
    final int secondsPerHour = 60*60;
    final int secondsPerDay = 60*60*24;
    final long days = durationSec / secondsPerDay;
    durationSec -= days * secondsPerDay;
    final long hours = durationSec / secondsPerHour;
    durationSec -= hours * secondsPerHour;
    final long minutes = durationSec / secondsPerMinute;
    durationSec -= minutes * secondsPerMinute;
    final long seconds = durationSec;
    final long milliseconds = durationMs % 1000;
    String format = "%03d:%02d:%02d:%02d.%03d";
    if (negative)  {
      format = "-" + format;
    }
    return String.format(format, days, hours, minutes, seconds, milliseconds);
  }

  /**
   * Converts a relative time string into a duration in milliseconds.
   */
  public static long parseRelativeTime(String relTime) throws IOException {
    if (relTime.length() < 2) {
      throw new IOException("Unable to parse relative time value of " + relTime
          + ": too short");
    }
    String ttlString = relTime.substring(0, relTime.length()-1);
    long ttl;
    try {
      ttl = Long.parseLong(ttlString);
    } catch (NumberFormatException e) {
      throw new IOException("Unable to parse relative time value of " + relTime
          + ": " + ttlString + " is not a number");
    }
    if (relTime.endsWith("s")) {
      // pass
    } else if (relTime.endsWith("m")) {
      ttl *= 60;
    } else if (relTime.endsWith("h")) {
      ttl *= 60*60;
    } else if (relTime.endsWith("d")) {
      ttl *= 60*60*24;
    } else {
      throw new IOException("Unable to parse relative time value of " + relTime
          + ": unknown time unit " + relTime.charAt(relTime.length() - 1));
    }
    return ttl*1000;
  }

  /**
   * Assert that all objects in the collection are equal. Returns silently if
   * so, throws an AssertionError if any object is not equal. All null values
   * are considered equal.
   * 
   * @param objects the collection of objects to check for equality.
   */
  public static void assertAllResultsEqual(Collection<?> objects)
      throws AssertionError {
    if (objects.size() == 0 || objects.size() == 1)
      return;
    
    Object[] resultsArray = objects.toArray();
    for (int i = 1; i < resultsArray.length; i++) {
      Object currElement = resultsArray[i];
      Object lastElement = resultsArray[i - 1];
      if ((currElement == null && currElement != lastElement) ||
          (currElement != null && !currElement.equals(lastElement))) {
        throw new AssertionError("Not all elements match in results: " +
          Arrays.toString(resultsArray));
      }
    }
  }

  /**
   * Creates a new KeyProvider from the given Configuration.
   *
   * @param conf Configuration
   * @return new KeyProvider, or null if no provider was found.
   * @throws IOException if the KeyProvider is improperly specified in
   *                             the Configuration
   */
  public static KeyProvider createKeyProvider(
      final Configuration conf) throws IOException {
    final String providerUriStr =
        conf.getTrimmed(DFSConfigKeys.DFS_ENCRYPTION_KEY_PROVIDER_URI, "");
    // No provider set in conf
    if (providerUriStr.isEmpty()) {
      return null;
    }
    final URI providerUri;
    try {
      providerUri = new URI(providerUriStr);
    } catch (URISyntaxException e) {
      throw new IOException(e);
    }
    KeyProvider keyProvider = KeyProviderFactory.get(providerUri, conf);
    if (keyProvider == null) {
      throw new IOException("Could not instantiate KeyProvider from " + 
          DFSConfigKeys.DFS_ENCRYPTION_KEY_PROVIDER_URI + " setting of '" + 
          providerUriStr +"'");
    }
    if (keyProvider.isTransient()) {
      throw new IOException("KeyProvider " + keyProvider.toString()
          + " was found but it is a transient provider.");
    }
    return keyProvider;
  }

  /**
   * Creates a new KeyProviderCryptoExtension by wrapping the
   * KeyProvider specified in the given Configuration.
   *
   * @param conf Configuration
   * @return new KeyProviderCryptoExtension, or null if no provider was found.
   * @throws IOException if the KeyProvider is improperly specified in
   *                             the Configuration
   */
  public static KeyProviderCryptoExtension createKeyProviderCryptoExtension(
      final Configuration conf) throws IOException {
    KeyProvider keyProvider = createKeyProvider(conf);
    if (keyProvider == null) {
      return null;
    }
    KeyProviderCryptoExtension cryptoProvider = KeyProviderCryptoExtension
        .createKeyProviderCryptoExtension(keyProvider);
    return cryptoProvider;
  }

  /**
   * Probe for HDFS Encryption being enabled; this uses the value of
   * the option {@link DFSConfigKeys.DFS_ENCRYPTION_KEY_PROVIDER_URI},
   * returning true if that property contains a non-empty, non-whitespace
   * string.
   * @param conf configuration to probe
   * @return true if encryption is considered enabled.
   */
  public static boolean isHDFSEncryptionEnabled(Configuration conf) {
    return !conf.getTrimmed(
        DFSConfigKeys.DFS_ENCRYPTION_KEY_PROVIDER_URI, "").isEmpty();
  }

}
