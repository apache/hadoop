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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.StringTokenizer;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.protocol.AlreadyBeingCreatedException;
import org.apache.hadoop.hdfs.protocol.ClientDatanodeProtocol;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.io.retry.RetryPolicies;
import org.apache.hadoop.io.retry.RetryPolicy;
import org.apache.hadoop.io.retry.RetryProxy;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.net.NodeBase;
import org.apache.hadoop.security.UserGroupInformation;

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
   * @return collection of nameservice Ids
   */
  public static Collection<String> getNameServiceIds(Configuration conf) {
    return conf.getStringCollection(DFS_FEDERATION_NAMESERVICES);
  }
  
  /**
   * Namenode HighAvailability related configuration.
   * Returns collection of namenode Ids from the configuration. One logical id
   * for each namenode in the in the HA setup.
   * 
   * @param conf configuration
   * @return collection of namenode Ids
   */
  public static Collection<String> getNameNodeIds(Configuration conf) {
    return conf.getStringCollection(DFS_HA_NAMENODES_KEY);
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
    if (suffix == null || suffix.length() == 0) {
      return key;
    }
    if (!suffix.startsWith(".")) {
      key += ".";
    }
    return key += suffix;
  }
  
  /** Concatenate list of suffix strings '.' separated */
  private static String concatSuffixes(String... suffixes) {
    if (suffixes == null) {
      return null;
    }
    String ret = "";
    for (int i = 0; i < suffixes.length - 1; i++) {
      ret = addSuffix(ret, suffixes[i]);
    }
    return addSuffix(ret, suffixes[suffixes.length - 1]);
  }
  
  /**
   * Return configuration key of format key.suffix1.suffix2...suffixN
   */
  public static String addKeySuffixes(String key, String... suffixes) {
    String keySuffix = concatSuffixes(suffixes);
    return addSuffix(key, keySuffix);
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
    Collection<String> namenodeIds = getNameNodeIds(conf);
    List<InetSocketAddress> isas = new ArrayList<InetSocketAddress>();

    final boolean federationEnabled = nameserviceIds != null
        && !nameserviceIds.isEmpty();
    final boolean haEnabled = namenodeIds != null
        && !namenodeIds.isEmpty();
    
    // Configuration with no federation and ha, return default address
    if (!federationEnabled && !haEnabled) {
      String address = getConfValue(defaultAddress, null, conf, keys);
      if (address == null) {
        return null;
      }
      isas.add(NetUtils.createSocketAddr(address));
      return isas;
    }
    
    if (!federationEnabled) {
      nameserviceIds = new ArrayList<String>();
      nameserviceIds.add(null);
    }
    if (!haEnabled) {
      namenodeIds = new ArrayList<String>();
      namenodeIds.add(null);
    }
    
    // Get configuration suffixed with nameserviceId and/or namenodeId
    if (federationEnabled && haEnabled) {
      for (String nameserviceId : nameserviceIds) {
        for (String nnId : namenodeIds) {
          String keySuffix = concatSuffixes(nameserviceId, nnId);
          String address = getConfValue(null, keySuffix, conf, keys);
          if (address != null) {
            isas.add(NetUtils.createSocketAddr(address));
          }
        }
      }
    } else if (!federationEnabled && haEnabled) {
      for (String nnId : namenodeIds) {
        String address = getConfValue(null, nnId, conf, keys);
        if (address != null) {
          isas.add(NetUtils.createSocketAddr(address));
        }
      }
    } else if (federationEnabled && !haEnabled) {
      for (String nameserviceId : nameserviceIds) {
          String address = getConfValue(null, nameserviceId, conf, keys);
          if (address != null) {
            isas.add(NetUtils.createSocketAddr(address));
          }
      }
    }
    return isas;
  }

  /**
   * Returns list of InetSocketAddress corresponding to HA NN RPC addresses from
   * the configuration.
   * 
   * @param conf configuration
   * @return list of InetSocketAddresses
   * @throws IOException if no addresses are configured
   */
  public static List<InetSocketAddress> getHaNnRpcAddresses(
      Configuration conf) throws IOException {
    List<InetSocketAddress> addressList = getAddresses(conf, null,
        DFSConfigKeys.DFS_NAMENODE_RPC_ADDRESS_KEY);
    if (addressList == null) {
      throw new IOException("Incorrect configuration: HA name node addresses "
          + DFS_NAMENODE_RPC_ADDRESS_KEY + " is not configured.");
    }
    return addressList;
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
    if (!isFederationEnabled(conf)) {
      return null;
    }    
    String[] ids = getSuffixIDs(conf, address, keys);
    return (ids != null && ids.length > 0) ? ids[0] : null;
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
  public static ClientProtocol createNamenode(Configuration conf) throws IOException {
    return createNamenode(NameNode.getAddress(conf), conf);
  }

  /** Create a {@link NameNode} proxy */
  public static ClientProtocol createNamenode(InetSocketAddress nameNodeAddr,
      Configuration conf) throws IOException {
    return createNamenode(nameNodeAddr, conf, UserGroupInformation.getCurrentUser());
  }
  
  /** Create a {@link NameNode} proxy */
  public static ClientProtocol createNamenode(InetSocketAddress nameNodeAddr,
      Configuration conf, UserGroupInformation ugi) throws IOException {
    return createNamenode(createRPCNamenode(nameNodeAddr, conf, ugi));
  }

  /** Create a {@link NameNode} proxy */
  public static ClientProtocol createRPCNamenode(InetSocketAddress nameNodeAddr,
      Configuration conf, UserGroupInformation ugi) 
    throws IOException {
    return (ClientProtocol)RPC.getProxy(ClientProtocol.class,
        ClientProtocol.versionID, nameNodeAddr, ugi, conf,
        NetUtils.getSocketFactory(conf, ClientProtocol.class));
  }

  /** Create a {@link NameNode} proxy */
  public static ClientProtocol createNamenode(ClientProtocol rpcNamenode)
    throws IOException {
    RetryPolicy createPolicy = RetryPolicies.retryUpToMaximumCountWithFixedSleep(
        5, HdfsConstants.LEASE_SOFTLIMIT_PERIOD, TimeUnit.MILLISECONDS);
    
    Map<Class<? extends Exception>,RetryPolicy> remoteExceptionToPolicyMap =
      new HashMap<Class<? extends Exception>, RetryPolicy>();
    remoteExceptionToPolicyMap.put(AlreadyBeingCreatedException.class, createPolicy);

    Map<Class<? extends Exception>,RetryPolicy> exceptionToPolicyMap =
      new HashMap<Class<? extends Exception>, RetryPolicy>();
    exceptionToPolicyMap.put(RemoteException.class, 
        RetryPolicies.retryByRemoteException(
            RetryPolicies.TRY_ONCE_THEN_FAIL, remoteExceptionToPolicyMap));
    RetryPolicy methodPolicy = RetryPolicies.retryByException(
        RetryPolicies.TRY_ONCE_THEN_FAIL, exceptionToPolicyMap);
    Map<String,RetryPolicy> methodNameToPolicyMap = new HashMap<String,RetryPolicy>();
    
    methodNameToPolicyMap.put("create", methodPolicy);

    return (ClientProtocol) RetryProxy.create(ClientProtocol.class,
        rpcNamenode, methodNameToPolicyMap);
  }

  /** Create a {@link ClientDatanodeProtocol} proxy */
  public static ClientDatanodeProtocol createClientDatanodeProtocolProxy(
      DatanodeID datanodeid, Configuration conf, int socketTimeout,
      LocatedBlock locatedBlock)
      throws IOException {
    InetSocketAddress addr = NetUtils.createSocketAddr(
      datanodeid.getHost() + ":" + datanodeid.getIpcPort());
    if (ClientDatanodeProtocol.LOG.isDebugEnabled()) {
      ClientDatanodeProtocol.LOG.debug("ClientDatanodeProtocol addr=" + addr);
    }
    
    // Since we're creating a new UserGroupInformation here, we know that no
    // future RPC proxies will be able to re-use the same connection. And
    // usages of this proxy tend to be one-off calls.
    //
    // This is a temporary fix: callers should really achieve this by using
    // RPC.stopProxy() on the resulting object, but this is currently not
    // working in trunk. See the discussion on HDFS-1965.
    Configuration confWithNoIpcIdle = new Configuration(conf);
    confWithNoIpcIdle.setInt(CommonConfigurationKeysPublic
        .IPC_CLIENT_CONNECTION_MAXIDLETIME_KEY, 0);

    UserGroupInformation ticket = UserGroupInformation
        .createRemoteUser(locatedBlock.getBlock().getLocalBlock().toString());
    ticket.addToken(locatedBlock.getBlockToken());
    return RPC.getProxy(ClientDatanodeProtocol.class,
        ClientDatanodeProtocol.versionID, addr, ticket, confWithNoIpcIdle,
        NetUtils.getDefaultSocketFactory(conf), socketTimeout);
  }

  /**
   * Returns true if federation configuration is enabled
   */
  public static boolean isFederationEnabled(Configuration conf) {
    Collection<String> collection = getNameServiceIds(conf);
    return collection != null && collection.size() != 0;
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
    if (!isFederationEnabled(conf)) {
      return null;
    }
    nameserviceId = getSuffixIDs(conf, addressKey, LOCAL_ADDRESS_MATCHER)[0];
    if (nameserviceId == null) {
      String msg = "Configuration " + addressKey + " must be suffixed with" +
      		" nameserviceId for federation configuration.";
      throw new HadoopIllegalArgumentException(msg);
    }
    return nameserviceId;
  }
  
  /**
   * Returns nameservice Id and namenode Id when the local host matches the
   * configuration parameter {@code addressKey}.<nameservice Id>.<namenode Id>
   * 
   * @param conf Configuration
   * @param addressKey configuration key corresponding to the address.
   * @param matcher matching criteria for matching the address
   * @return Array with nameservice Id and namenode Id on success. First element
   *         in the array is nameservice Id and second element is namenode Id.
   *         Null value indicates that the configuration does not have the the
   *         Id.
   * @throws HadoopIllegalArgumentException on error
   */
  static String[] getSuffixIDs(final Configuration conf, final String addressKey,
      final AddressMatcher matcher) {
    Collection<String> nsIds = getNameServiceIds(conf);
    boolean federationEnabled = true;
    if (nsIds == null || nsIds.size() == 0) {
      federationEnabled = false; // federation not configured
      nsIds = new ArrayList<String>();
      nsIds.add(null);
    }
    
    boolean haEnabled = true;
    Collection<String> nnIds = getNameNodeIds(conf);
    if (nnIds == null || nnIds.size() == 0) {
      haEnabled = false; // HA not configured
      nnIds = new ArrayList<String>();
      nnIds.add(null);
    }
    
    // Match the address from addressKey.nsId.nnId based on the given matcher
    String nameserviceId = null;
    String namenodeId = null;
    int found = 0;
    for (String nsId : nsIds) {
      for (String nnId : nnIds) {
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
          + (federationEnabled ? DFS_FEDERATION_NAMESERVICE_ID : "")
          + (haEnabled ? (" and " + DFS_HA_NAMENODE_ID_KEY) : "");
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
      String[] ids = getSuffixIDs(conf, key, matcher);
      if (ids != null && (ids [0] != null || ids[1] != null)) {
        return ids;
      }
    }
    return null;
  }
  
  private interface AddressMatcher {
    public boolean match(InetSocketAddress s);
  }
}
