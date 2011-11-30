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

import static org.apache.hadoop.hdfs.DFSConfigKeys.*;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.StringTokenizer;

import javax.net.SocketFactory;

import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.protocol.ClientDatanodeProtocol;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.net.NodeBase;
import org.apache.hadoop.security.UserGroupInformation;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

@InterfaceAudience.Private
public class DFSUtil {
  private DFSUtil() { /* Hidden constructor */ }
  private static final ThreadLocal<Random> RANDOM = new ThreadLocal<Random>() {
    @Override
    protected Random initialValue() {
      return new Random();
    }
  };

  /** @return a pseudorandom number generator. */
  public static Random getRandom() {
    return RANDOM.get();
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
   * Address matcher for matching an address to local address
   */
  static final AddressMatcher LOCAL_ADDRESS_MATCHER = new AddressMatcher() {
    public boolean match(InetSocketAddress s) {
      return NetUtils.isLocalAddress(s.getAddress());
    };
  };
  
  /**
   * Whether the pathname is valid.  Currently prohibits relative paths, 
   * and names which contain a ":" or "/" 
   */
  public static boolean isValidName(String src) {
    // Path must be absolute.
    if (!src.startsWith(Path.SEPARATOR)) {
      return false;
    }
      
    // Check for ".." "." ":" "/"
    StringTokenizer tokens = new StringTokenizer(src, Path.SEPARATOR);
    while(tokens.hasMoreTokens()) {
      String element = tokens.nextToken();
      if (element.equals("..") || 
          element.equals(".")  ||
          (element.indexOf(":") >= 0)  ||
          (element.indexOf("/") >= 0)) {
        return false;
      }
    }
    return true;
  }

  /**
   * Utility class to facilitate junit test error simulation.
   */
  @InterfaceAudience.Private
  public static class ErrorSimulator {
    private static boolean[] simulation = null; // error simulation events
    public static void initializeErrorSimulationEvent(int numberOfEvents) {
      simulation = new boolean[numberOfEvents]; 
      for (int i = 0; i < numberOfEvents; i++) {
        simulation[i] = false;
      }
    }
    
    public static boolean getErrorSimulation(int index) {
      if(simulation == null)
        return false;
      assert(index < simulation.length);
      return simulation[index];
    }
    
    public static void setErrorSimulation(int index) {
      assert(index < simulation.length);
      simulation[index] = true;
    }
    
    public static void clearErrorSimulation(int index) {
      assert(index < simulation.length);
      simulation[index] = false;
    }
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
    try {
      return str.getBytes("UTF8");
    } catch(UnsupportedEncodingException e) {
      assert false : "UTF8 encoding is not supported ";
    }
    return null;
  }

  /**
   * Given a list of path components returns a path as a UTF8 String
   */
  public static String byteArray2String(byte[][] pathComponents) {
    if (pathComponents.length == 0)
      return "";
    if (pathComponents.length == 1 && pathComponents[0].length == 0) {
      return Path.SEPARATOR;
    }
    try {
      StringBuilder result = new StringBuilder();
      for (int i = 0; i < pathComponents.length; i++) {
        result.append(new String(pathComponents[i], "UTF-8"));
        if (i < pathComponents.length - 1) {
          result.append(Path.SEPARATOR_CHAR);
        }
      }
      return result.toString();
    } catch (UnsupportedEncodingException ex) {
      assert false : "UTF8 encoding is not supported ";
    }
    return null;
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
    int nrBlocks = blocks.locatedBlockCount();
    BlockLocation[] blkLocations = new BlockLocation[nrBlocks];
    if (nrBlocks == 0) {
      return blkLocations;
    }
    int idx = 0;
    for (LocatedBlock blk : blocks.getLocatedBlocks()) {
      assert idx < nrBlocks : "Incorrect index";
      DatanodeInfo[] locations = blk.getLocations();
      String[] hosts = new String[locations.length];
      String[] names = new String[locations.length];
      String[] racks = new String[locations.length];
      for (int hCnt = 0; hCnt < locations.length; hCnt++) {
        hosts[hCnt] = locations[hCnt].getHostName();
        names[hCnt] = locations[hCnt].getName();
        NodeBase node = new NodeBase(names[hCnt], 
                                     locations[hCnt].getNetworkLocation());
        racks[hCnt] = node.toString();
      }
      blkLocations[idx] = new BlockLocation(names, hosts, racks,
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
    return conf.getTrimmedStringCollection(DFS_FEDERATION_NAMESERVICES);
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
  static Collection<String> getNameNodeIds(Configuration conf, String nsId) {
    String key = addSuffix(DFS_HA_NAMENODES_KEY, nsId);
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
   * Returns list of InetSocketAddress corresponding to HA NN RPC addresses from
   * the configuration.
   * 
   * @param conf configuration
   * @return list of InetSocketAddresses
   * @throws IOException if no addresses are configured
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
      defaultAddress = NameNode.getHostPortString(NameNode.getAddress(conf));
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
   * Given the InetSocketAddress this method returns the nameservice Id
   * corresponding to the key with matching address, by doing a reverse 
   * lookup on the list of nameservices until it finds a match.
   * 
   * If null is returned, client should try {@link #isDefaultNamenodeAddress}
   * to check pre-Federation, non-HA configurations.
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
   */
  public static String getInfoServer(
      InetSocketAddress namenodeAddr, Configuration conf, boolean httpsAddress) {
    String httpAddress = null;
    boolean securityOn = UserGroupInformation.isSecurityEnabled();
    String httpAddressKey = (securityOn && httpsAddress) ? 
        DFS_NAMENODE_HTTPS_ADDRESS_KEY : DFS_NAMENODE_HTTP_ADDRESS_KEY;
    String httpAddressDefault = (securityOn && httpsAddress) ? 
        DFS_NAMENODE_HTTPS_ADDRESS_DEFAULT : DFS_NAMENODE_HTTP_ADDRESS_DEFAULT;
    if (namenodeAddr != null) {
      // if non-default namenode, try reverse look up 
      // the nameServiceID if it is available
      String nameServiceId = DFSUtil.getNameServiceIdFromAddress(
          conf, namenodeAddr,
          DFSConfigKeys.DFS_NAMENODE_SERVICE_RPC_ADDRESS_KEY,
          DFSConfigKeys.DFS_NAMENODE_RPC_ADDRESS_KEY);

      if (nameServiceId != null) {
        httpAddress = conf.get(DFSUtil.addKeySuffixes(
            httpAddressKey, nameServiceId));
      }
    }
    // else - Use non-federation style configuration
    if (httpAddress == null) {
      httpAddress = conf.get(httpAddressKey, httpAddressDefault);
    }
    return httpAddress;
  }
  
  /**
   * Given the InetSocketAddress for any configured communication with a 
   * namenode, this method determines whether it is the configured
   * communication channel for the "default" namenode.
   * It does a reverse lookup on the list of default communication parameters
   * to see if the given address matches any of them.
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
   *     DFS_NAMENODE_RPC_ADDRESS_KEY
   * @return - boolean confirmation if matched generic parameter
   */
  public static boolean isDefaultNamenodeAddress(Configuration conf,
      InetSocketAddress address, String... keys) {
    for (String key : keys) {
      String candidateAddress = conf.get(key);
      if (candidateAddress != null
          && address.equals(NetUtils.createSocketAddr(candidateAddress)))
        return true;
    }
    return false;
  }
  
  /**
   * Sets the node specific setting into generic configuration key. Looks up
   * value of "key.nameserviceId.namenodeId" and if found sets that value into 
   * generic key in the conf. Note that this only modifies the runtime conf.
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

  /**
   * @param address address of format host:port
   * @return InetSocketAddress for the address
   */
  public static InetSocketAddress getSocketAddress(String address) {
    int colon = address.indexOf(":");
    if (colon < 0) {
      return new InetSocketAddress(address, 0);
    }
    return new InetSocketAddress(address.substring(0, colon), 
        Integer.parseInt(address.substring(colon + 1)));
  }

  /**
   * Round bytes to GiB (gibibyte)
   * @param bytes number of bytes
   * @return number of GiB
   */
  public static int roundBytesToGB(long bytes) {
    return Math.round((float)bytes/ 1024 / 1024 / 1024);
  }


  /** Create a {@link NameNode} proxy */
  public static ClientProtocol createNamenode(Configuration conf)
      throws IOException {
    return createNamenode(NameNode.getAddress(conf), conf);
  }

  /** Create a {@link NameNode} proxy */
  public static ClientProtocol createNamenode(InetSocketAddress nameNodeAddr,
      Configuration conf) throws IOException {   
    return createNamenode(nameNodeAddr, conf,
        UserGroupInformation.getCurrentUser());
  }

  /** Create a {@link NameNode} proxy */
  public static ClientProtocol createNamenode( InetSocketAddress nameNodeAddr,
      Configuration conf, UserGroupInformation ugi) throws IOException {
    /** 
     * Currently we have simply burnt-in support for a SINGLE
     * protocol - protocolR23Compatible. This will be replaced
     * by a way to pick the right protocol based on the 
     * version of the target server.  
     */
    return new org.apache.hadoop.hdfs.protocolR23Compatible.
        ClientNamenodeProtocolTranslatorR23(nameNodeAddr, conf, ugi);
  }

  /** Create a {@link ClientDatanodeProtocol} proxy */
  public static ClientDatanodeProtocol createClientDatanodeProtocolProxy(
      DatanodeID datanodeid, Configuration conf, int socketTimeout,
      LocatedBlock locatedBlock) throws IOException {
    return new org.apache.hadoop.hdfs.protocolR23Compatible.
        ClientDatanodeProtocolTranslatorR23(datanodeid, conf, socketTimeout,
             locatedBlock);
  }
  
  /** Create {@link ClientDatanodeProtocol} proxy using kerberos ticket */
  static ClientDatanodeProtocol createClientDatanodeProtocolProxy(
      DatanodeID datanodeid, Configuration conf, int socketTimeout)
      throws IOException {
    return new org.apache.hadoop.hdfs.protocolR23Compatible.ClientDatanodeProtocolTranslatorR23(
        datanodeid, conf, socketTimeout);
  }
  
  /** Create a {@link ClientDatanodeProtocol} proxy */
  public static ClientDatanodeProtocol createClientDatanodeProtocolProxy(
      InetSocketAddress addr, UserGroupInformation ticket, Configuration conf,
      SocketFactory factory) throws IOException {
    return new org.apache.hadoop.hdfs.protocolR23Compatible.
        ClientDatanodeProtocolTranslatorR23(addr, ticket, conf, factory);
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
   * If {@link DFSConfigKeys#DFS_FEDERATION_NAMESERVICE_ID} is not specifically
   * configured, this method determines the nameservice Id by matching the local
   * node's address with the configured addresses. When a match is found, it
   * returns the nameservice Id from the corresponding configuration key.
   * 
   * @param conf Configuration
   * @param addressKey configuration key to get the address.
   * @return nameservice Id on success, null if federation is not configured.
   * @throws HadoopIllegalArgumentException on error
   */
  private static String getNameServiceId(Configuration conf, String addressKey) {
    String nameserviceId = conf.get(DFS_FEDERATION_NAMESERVICE_ID);
    if (nameserviceId != null) {
      return nameserviceId;
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
        if (knownNNId != null && !knownNNId.equals(nnId)) {
          continue;
        }
        String key = addKeySuffixes(addressKey, nsId, nnId);
        String addr = conf.get(key);
        InetSocketAddress s = null;
        try {
          s = NetUtils.createSocketAddr(addr);
        } catch (Exception e) {
          continue;
        }
        if (matcher.match(s)) {
          nameserviceId = nsId;
          namenodeId = nnId;
          found++;
        }
      }
    }
    if (found > 1) { // Only one address must match the local address
      String msg = "Configuration has multiple addresses that match "
          + "local node's address. Please configure the system with "
          + DFS_FEDERATION_NAMESERVICE_ID + " and "
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
}
