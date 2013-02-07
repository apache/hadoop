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

import java.io.IOException;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.SecureRandom;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import javax.net.SocketFactory;

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
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.protocol.ClientDatanodeProtocol;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.protocolPB.ClientDatanodeProtocolTranslatorPB;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.ipc.ProtobufRpcEngine;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.net.NodeBase;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.ToolRunner;

import com.google.common.base.Charsets;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.protobuf.BlockingService;

@InterfaceAudience.Private
public class DFSUtil {
  public static final Log LOG = LogFactory.getLog(DFSUtil.class.getName());
  
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
    private long staleInterval;

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
      if (element.equals("..") || 
          element.equals(".")  ||
          (element.indexOf(":") >= 0)  ||
          (element.indexOf("/") >= 0)) {
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
   * Converts a byte array to a string using UTF8 encoding.
   */
  public static String bytes2String(byte[] bytes) {
    try {
      return new String(bytes, "UTF8");
    } catch(UnsupportedEncodingException e) {
      assert false : "UTF8 encoding is not supported ";
    }
    return null;
  }

  /**
   * Converts a string to a byte array using UTF8 encoding.
   */
  public static byte[] string2Bytes(String str) {
    return str.getBytes(Charsets.UTF_8);
  }

  /**
   * Given a list of path components returns a path as a UTF8 String
   */
  public static String byteArray2PathString(byte[][] pathComponents) {
    if (pathComponents.length == 0)
      return "";
    if (pathComponents.length == 1 && pathComponents[0].length == 0) {
      return Path.SEPARATOR;
    }
    StringBuilder result = new StringBuilder();
    for (int i = 0; i < pathComponents.length; i++) {
      result.append(new String(pathComponents[i], Charsets.UTF_8));
      if (i < pathComponents.length - 1) {
        result.append(Path.SEPARATOR_CHAR);
      }
    }
    return result.toString();
  }

  /** Convert an object representing a path to a string. */
  public static String path2String(final Object path) {
    return path == null? null
        : path instanceof String? (String)path
        : path instanceof byte[][]? byteArray2PathString((byte[][])path)
        : path.toString();
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
    assert len <= bytes.length;
    int splits = 0;
    if (len == 0) {
      return new byte[][]{null};
    }
    // Count the splits. Omit multiple separators and the last one
    for (int i = 0; i < len; i++) {
      if (bytes[i] == separator) {
        splits++;
      }
    }
    int last = len - 1;
    while (last > -1 && bytes[last--] == separator) {
      splits--;
    }
    if (splits == 0 && bytes[0] == separator) {
      return new byte[][]{null};
    }
    splits++;
    byte[][] result = new byte[splits][];
    int startIndex = 0;
    int nextIndex = 0;
    int index = 0;
    // Build the splits
    while (index < splits) {
      while (nextIndex < len && bytes[nextIndex] != separator) {
        nextIndex++;
      }
      result[index] = new byte[nextIndex - startIndex];
      System.arraycopy(bytes, startIndex, result[index], 0, nextIndex
              - startIndex);
      index++;
      startIndex = nextIndex + 1;
      nextIndex = startIndex;
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
      blkLocations[idx] = new BlockLocation(xferAddrs, hosts, racks,
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
    getAddresses(Configuration conf,
      String defaultAddress, String... keys) {
    Collection<String> nameserviceIds = getNameServiceIds(conf);
    
    // Look for configurations of the form <key>[.<nameserviceId>][.<namenodeId>]
    // across all of the configured nameservices and namenodes.
    Map<String, Map<String, InetSocketAddress>> ret = Maps.newHashMap();
    for (String nsId : emptyAsSingletonNull(nameserviceIds)) {
      Map<String, InetSocketAddress> isas =
        getAddressesForNameserviceId(conf, nsId, defaultAddress, keys);
      if (!isas.isEmpty()) {
        ret.put(nsId, isas);
      }
    }
    return ret;
  }

  private static Map<String, InetSocketAddress> getAddressesForNameserviceId(
      Configuration conf, String nsId, String defaultValue,
      String[] keys) {
    Collection<String> nnIds = getNameNodeIds(conf, nsId);
    Map<String, InetSocketAddress> ret = Maps.newHashMap();
    for (String nnId : emptyAsSingletonNull(nnIds)) {
      String suffix = concatSuffixes(nsId, nnId);
      String address = getConfValue(defaultValue, suffix, conf, keys);
      if (address != null) {
        InetSocketAddress isa = NetUtils.createSocketAddr(address);
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
              .get(DFSConfigKeys.DFS_NAMENODE_USER_NAME_KEY),
              NameNode.getAddress(confForNn).getHostName());
          principals.add(principal);
        }
      } else {
        Configuration confForNn = new Configuration(conf);
        NameNode.initializeGenericKeys(confForNn, nsId, null);
        String principal = SecurityUtil.getServerPrincipal(confForNn
            .get(DFSConfigKeys.DFS_NAMENODE_USER_NAME_KEY),
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
   * configuration. Note this is to be used by datanodes to get the list of
   * namenode addresses to talk to.
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
  
  /**
   * Get a URI for each configured nameservice. If a nameservice is
   * HA-enabled, then the logical URI of the nameservice is returned. If the
   * nameservice is not HA-enabled, then a URI corresponding to an RPC address
   * of the single NN for that nameservice is returned, preferring the service
   * RPC address over the client RPC address.
   * 
   * @param conf configuration
   * @return a collection of all configured NN URIs, preferring service
   *         addresses
   */
  public static Collection<URI> getNsServiceRpcUris(Configuration conf) {
    return getNameServiceUris(conf,
        DFSConfigKeys.DFS_NAMENODE_SERVICE_RPC_ADDRESS_KEY,
        DFSConfigKeys.DFS_NAMENODE_RPC_ADDRESS_KEY);
  }

  /**
   * Get a URI for each configured nameservice. If a nameservice is
   * HA-enabled, then the logical URI of the nameservice is returned. If the
   * nameservice is not HA-enabled, then a URI corresponding to the address of
   * the single NN for that nameservice is returned.
   * 
   * @param conf configuration
   * @param keys configuration keys to try in order to get the URI for non-HA
   *        nameservices
   * @return a collection of all configured NN URIs
   */
  public static Collection<URI> getNameServiceUris(Configuration conf,
      String... keys) {
    Set<URI> ret = new HashSet<URI>();
    
    // We're passed multiple possible configuration keys for any given NN or HA
    // nameservice, and search the config in order of these keys. In order to
    // make sure that a later config lookup (e.g. fs.defaultFS) doesn't add a
    // URI for a config key for which we've already found a preferred entry, we
    // keep track of non-preferred keys here.
    Set<URI> nonPreferredUris = new HashSet<URI>();
    
    for (String nsId : getNameServiceIds(conf)) {
      if (HAUtil.isHAEnabled(conf, nsId)) {
        // Add the logical URI of the nameservice.
        try {
          ret.add(new URI(HdfsConstants.HDFS_URI_SCHEME + "://" + nsId));
        } catch (URISyntaxException ue) {
          throw new IllegalArgumentException(ue);
        }
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
   * @param conf
   * @param namenodeAddr - namenode RPC address
   * @param httpsAddress -If true, and if security is enabled, returns server 
   *                      https address. If false, returns server http address.
   * @return server http or https address
   * @throws IOException 
   */
  public static String getInfoServer(InetSocketAddress namenodeAddr,
      Configuration conf, boolean httpsAddress) throws IOException {
    boolean securityOn = UserGroupInformation.isSecurityEnabled();
    String httpAddressKey = (securityOn && httpsAddress) ? 
        DFS_NAMENODE_HTTPS_ADDRESS_KEY : DFS_NAMENODE_HTTP_ADDRESS_KEY;
    String httpAddressDefault = (securityOn && httpsAddress) ? 
        DFS_NAMENODE_HTTPS_ADDRESS_DEFAULT : DFS_NAMENODE_HTTP_ADDRESS_DEFAULT;
      
    String suffixes[];
    if (namenodeAddr != null) {
      // if non-default namenode, try reverse look up 
      // the nameServiceID if it is available
      suffixes = getSuffixIDs(conf, namenodeAddr,
          DFSConfigKeys.DFS_NAMENODE_SERVICE_RPC_ADDRESS_KEY,
          DFSConfigKeys.DFS_NAMENODE_RPC_ADDRESS_KEY);
    } else {
      suffixes = new String[2];
    }
    String configuredInfoAddr = getSuffixedConf(conf, httpAddressKey,
        httpAddressDefault, suffixes);
    if (namenodeAddr != null) {
      return substituteForWildcardAddress(configuredInfoAddr,
          namenodeAddr.getHostName());
    } else {
      return configuredInfoAddr;
    }
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
  public static String substituteForWildcardAddress(String configuredAddress,
      String defaultHost) throws IOException {
    InetSocketAddress sockAddr = NetUtils.createSocketAddr(configuredAddress);
    InetSocketAddress defaultSockAddr = NetUtils.createSocketAddr(defaultHost
        + ":0");
    if (sockAddr.getAddress().isAnyLocalAddress()) {
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
  static ClientDatanodeProtocol createClientDatanodeProtocolProxy(
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
   * @see #getSuffixIDs(Configuration, String, AddressMatcher)
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
  
  public static Options helpOptions = new Options();
  public static Option helpOpt = new Option("h", "help", false,
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
}
