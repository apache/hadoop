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

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_FEDERATION_NAMESERVICES;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_FEDERATION_NAMESERVICE_ID;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_BACKUP_ADDRESS_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_RPC_ADDRESS_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_SECONDARY_HTTP_ADDRESS_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_SERVICE_RPC_ADDRESS_KEY;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
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

@InterfaceAudience.Private
public class DFSUtil {
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
   * @return collection of nameservice Ids
   */
  public static Collection<String> getNameServiceIds(Configuration conf) {
    return conf.getStringCollection(DFS_FEDERATION_NAMESERVICES);
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
      if (keySuffix != null) {
        key += "." + keySuffix;
      }
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
  
  /**
   * Returns list of InetSocketAddress for a given set of keys.
   * @param conf configuration
   * @param defaultAddress default address to return in case key is not found
   * @param keys Set of keys to look for in the order of preference
   * @return list of InetSocketAddress corresponding to the key
   */
  private static List<InetSocketAddress> getAddresses(Configuration conf,
      String defaultAddress, String... keys) {
    Collection<String> nameserviceIds = getNameServiceIds(conf);
    List<InetSocketAddress> isas = new ArrayList<InetSocketAddress>();

    // Configuration with a single namenode
    if (nameserviceIds == null || nameserviceIds.isEmpty()) {
      String address = getConfValue(defaultAddress, null, conf, keys);
      if (address == null) {
        return null;
      }
      isas.add(NetUtils.createSocketAddr(address));
    } else {
      // Get the namenodes for all the configured nameServiceIds
      for (String nameserviceId : nameserviceIds) {
        String address = getConfValue(null, nameserviceId, conf, keys);
        if (address == null) {
          return null;
        }
        isas.add(NetUtils.createSocketAddr(address));
      }
    }
    return isas;
  }
  
  /**
   * Returns list of InetSocketAddress corresponding to  backup node rpc 
   * addresses from the configuration.
   * 
   * @param conf configuration
   * @return list of InetSocketAddresses
   * @throws IOException on error
   */
  public static List<InetSocketAddress> getBackupNodeAddresses(
      Configuration conf) throws IOException {
    List<InetSocketAddress> addressList = getAddresses(conf,
        null, DFS_NAMENODE_BACKUP_ADDRESS_KEY);
    if (addressList == null) {
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
  public static List<InetSocketAddress> getSecondaryNameNodeAddresses(
      Configuration conf) throws IOException {
    List<InetSocketAddress> addressList = getAddresses(conf, null,
        DFS_NAMENODE_SECONDARY_HTTP_ADDRESS_KEY);
    if (addressList == null) {
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
  public static List<InetSocketAddress> getNNServiceRpcAddresses(
      Configuration conf) throws IOException {
    // Use default address as fall back
    String defaultAddress;
    try {
      defaultAddress = NameNode.getHostPortString(NameNode.getAddress(conf));
    } catch (IllegalArgumentException e) {
      defaultAddress = null;
    }
    
    List<InetSocketAddress> addressList = getAddresses(conf, defaultAddress,
        DFS_NAMENODE_SERVICE_RPC_ADDRESS_KEY, DFS_NAMENODE_RPC_ADDRESS_KEY);
    if (addressList == null) {
      throw new IOException("Incorrect configuration: namenode address "
          + DFS_NAMENODE_SERVICE_RPC_ADDRESS_KEY + " or "  
          + DFS_NAMENODE_RPC_ADDRESS_KEY
          + " is not configured.");
    }
    return addressList;
  }
  
  /**
   * Given the InetSocketAddress for any configured communication with a 
   * namenode, this method returns the corresponding nameservice ID,
   * by doing a reverse lookup on the list of nameservices until it
   * finds a match.
   * If null is returned, client should try {@link #isDefaultNamenodeAddress}
   * to check pre-Federated configurations.
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
  public static String getNameServiceIdFromAddress(Configuration conf, 
      InetSocketAddress address, String... keys) {
    Collection<String> nameserviceIds = getNameServiceIds(conf);

    // Configuration with a single namenode and no nameserviceId
    if (nameserviceIds == null || nameserviceIds.isEmpty()) {
      // client should try {@link isDefaultNamenodeAddress} instead
      return null;
    }
    // Get the candidateAddresses for all the configured nameServiceIds
    for (String nameserviceId : nameserviceIds) {
      for (String key : keys) {
        String candidateAddress = conf.get(
            getNameServiceIdKey(key, nameserviceId));
        if (candidateAddress != null
            && address.equals(NetUtils.createSocketAddr(candidateAddress)))
          return nameserviceId;
      }
    }
    // didn't find a match
    // client should try {@link isDefaultNamenodeAddress} instead
    return null;
  }

  /**
   * return server http or https address from the configuration
   * @param conf
   * @param namenode - namenode address
   * @param httpsAddress -If true, and if security is enabled, returns server 
   *                      https address. If false, returns server http address.
   * @return server http or https address
   */
  public static String getInfoServer(
      InetSocketAddress namenode, Configuration conf, boolean httpsAddress) {
    String httpAddress = null;
    
    String httpAddressKey = (UserGroupInformation.isSecurityEnabled() 
        && httpsAddress) ? DFSConfigKeys.DFS_NAMENODE_HTTPS_ADDRESS_KEY
        : DFSConfigKeys.DFS_NAMENODE_HTTP_ADDRESS_KEY;
    String httpAddressDefault = (UserGroupInformation.isSecurityEnabled() 
        && httpsAddress) ? DFSConfigKeys.DFS_NAMENODE_HTTPS_ADDRESS_DEFAULT
        : DFSConfigKeys.DFS_NAMENODE_HTTP_ADDRESS_DEFAULT;
    if(namenode != null) {
      // if non-default namenode, try reverse look up 
      // the nameServiceID if it is available
      String nameServiceId = DFSUtil.getNameServiceIdFromAddress(
          conf, namenode,
          DFSConfigKeys.DFS_NAMENODE_SERVICE_RPC_ADDRESS_KEY,
          DFSConfigKeys.DFS_NAMENODE_RPC_ADDRESS_KEY);

      if (nameServiceId != null) {
        httpAddress = conf.get(DFSUtil.getNameServiceIdKey(
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
   * @return key specific to a nameserviceId from a generic key
   */
  public static String getNameServiceIdKey(String key, String nameserviceId) {
    return key + "." + nameserviceId;
  }
  
  /**
   * Sets the node specific setting into generic configuration key. Looks up
   * value of "key.nameserviceId" and if found sets that value into generic key 
   * in the conf. Note that this only modifies the runtime conf.
   * 
   * @param conf
   *          Configuration object to lookup specific key and to set the value
   *          to the key passed. Note the conf object is modified.
   * @param nameserviceId
   *          nameservice Id to construct the node specific key.
   * @param keys
   *          The key for which node specific value is looked up
   */
  public static void setGenericConf(Configuration conf,
      String nameserviceId, String... keys) {
    for (String key : keys) {
      String value = conf.get(getNameServiceIdKey(key, nameserviceId));
      if (value != null) {
        conf.set(key, value);
      }
    }
  }
  
  /** Return used as percentage of capacity */
  public static float getPercentUsed(long used, long capacity) {
    return capacity <= 0 ? 100 : ((float)used * 100.0f)/(float)capacity; 
  }
  
  /** Return remaining as percentage of capacity */
  public static float getPercentRemaining(long remaining, long capacity) {
    return capacity <= 0 ? 0 : ((float)remaining * 100.0f)/(float)capacity; 
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
  public static ClientProtocol createNamenode( InetSocketAddress nameNodeAddr,
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
  
  /** Create a {@link ClientDatanodeProtocol} proxy */
  public static ClientDatanodeProtocol createClientDatanodeProtocolProxy(
      InetSocketAddress addr, UserGroupInformation ticket, Configuration conf,
      SocketFactory factory) throws IOException {
    return new org.apache.hadoop.hdfs.protocolR23Compatible.
        ClientDatanodeProtocolTranslatorR23(addr, ticket, conf, factory);
  }
  
  /**
   * Get name service Id for the {@link NameNode} based on namenode RPC address
   * matching the local node address.
   */
  public static String getNamenodeNameServiceId(Configuration conf) {
    return getNameServiceId(conf, DFS_NAMENODE_RPC_ADDRESS_KEY);
  }
  
  /**
   * Get name service Id for the BackupNode based on backup node RPC address
   * matching the local node address.
   */
  public static String getBackupNameServiceId(Configuration conf) {
    return getNameServiceId(conf, DFS_NAMENODE_BACKUP_ADDRESS_KEY);
  }
  
  /**
   * Get name service Id for the secondary node based on secondary http address
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
   * nodes address with the configured addresses. When a match is found, it
   * returns the nameservice Id from the corresponding configuration key.
   * 
   * @param conf Configuration
   * @param addressKey configuration key to get the address.
   * @return name service Id on success, null on failure.
   * @throws HadoopIllegalArgumentException on error
   */
  private static String getNameServiceId(Configuration conf, String addressKey) {
    String nameserviceId = conf.get(DFS_FEDERATION_NAMESERVICE_ID);
    if (nameserviceId != null) {
      return nameserviceId;
    }
    
    Collection<String> ids = getNameServiceIds(conf);
    if (ids == null || ids.size() == 0) {
      // Not federation configuration, hence no nameservice Id
      return null;
    }
    
    // Match the rpc address with that of local address
    int found = 0;
    for (String id : ids) {
      String addr = conf.get(getNameServiceIdKey(addressKey, id));
      InetSocketAddress s = NetUtils.createSocketAddr(addr);
      if (NetUtils.isLocalAddress(s.getAddress())) {
        nameserviceId = id;
        found++;
      }
    }
    if (found > 1) { // Only one address must match the local address
      throw new HadoopIllegalArgumentException(
          "Configuration has multiple RPC addresses that matches "
              + "the local node's address. Please configure the system with "
              + "the parameter " + DFS_FEDERATION_NAMESERVICE_ID);
    }
    if (found == 0) {
      throw new HadoopIllegalArgumentException("Configuration address "
          + addressKey + " is missing in configuration with name service Id");
    }
    return nameserviceId;
  }
}
