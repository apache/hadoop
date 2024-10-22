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
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_HA_NAMENODE_ID_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_BACKUP_ADDRESS_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_HTTPS_ADDRESS_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_HTTPS_ADDRESS_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_HTTP_ADDRESS_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_HTTP_ADDRESS_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_LIFELINE_RPC_ADDRESS_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_RPC_ADDRESS_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_SECONDARY_HTTP_ADDRESS_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_SERVICE_RPC_ADDRESS_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_SHARED_EDITS_DIR_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMESERVICE_ID;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_SERVER_HTTPS_KEYPASSWORD_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_SERVER_HTTPS_KEYSTORE_PASSWORD_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_SERVER_HTTPS_TRUSTSTORE_PASSWORD_KEY;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;

import java.security.SecureRandom;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.Date;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;

import java.util.concurrent.TimeUnit;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.ParentNotDirectoryException;
import org.apache.hadoop.fs.UnresolvedLinkException;
import org.apache.hadoop.hdfs.server.blockmanagement.HostSet;
import org.apache.hadoop.hdfs.server.datanode.metrics.DataNodeMetrics;
import org.apache.hadoop.hdfs.server.namenode.FSDirectory;
import org.apache.hadoop.hdfs.server.namenode.INodesInPath;
import org.apache.hadoop.ipc.ProtobufRpcEngine;
import org.apache.hadoop.net.DomainNameResolver;
import org.apache.hadoop.net.DomainNameResolverFactory;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.util.Lists;
import org.apache.hadoop.thirdparty.com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.crypto.key.KeyProvider;
import org.apache.hadoop.crypto.key.KeyProviderCryptoExtension;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.common.Util;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.web.AuthFilterInitializer;
import org.apache.hadoop.http.HttpConfig;
import org.apache.hadoop.http.HttpServer2;
import org.apache.hadoop.ipc.ProtobufRpcEngine2;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.AuthenticationFilterInitializer;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authentication.server.ProxyUserAuthenticationFilterInitializer;
import org.apache.hadoop.security.authorize.AccessControlList;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.ToolRunner;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.thirdparty.com.google.common.base.Joiner;
import org.apache.hadoop.util.Preconditions;
import org.apache.hadoop.thirdparty.protobuf.BlockingService;

@InterfaceAudience.Private
public class DFSUtil {
  public static final Logger LOG =
      LoggerFactory.getLogger(DFSUtil.class.getName());
  
  private DFSUtil() { /* Hidden constructor */ }
  
  private static final ThreadLocal<SecureRandom> SECURE_RANDOM = new ThreadLocal<SecureRandom>() {
    @Override
    protected SecureRandom initialValue() {
      return new SecureRandom();
    }
  };

  /** @return a pseudo secure random number generator. */
  public static SecureRandom getSecureRandom() {
    return SECURE_RANDOM.get();
  }

  /**
   * Comparator for sorting DataNodeInfo[] based on
   * decommissioned and entering_maintenance states.
   */
  public static class ServiceComparator implements Comparator<DatanodeInfo> {
    @Override
    public int compare(DatanodeInfo a, DatanodeInfo b) {
      // Decommissioned nodes will be moved to the end of the list.
      if (a.isDecommissioned()) {
        return b.isDecommissioned() ? 0 : 1;
      } else if (b.isDecommissioned()) {
        return -1;
      }

      // Decommissioning nodes will be placed before decommissioned nodes.
      if (a.isDecommissionInProgress()) {
        return b.isDecommissionInProgress() ? 0 : 1;
      } else if (b.isDecommissionInProgress()) {
        return -1;
      }

      // ENTERING_MAINTENANCE nodes should be after live nodes.
      if (a.isEnteringMaintenance()) {
        return b.isEnteringMaintenance() ? 0 : 1;
      } else if (b.isEnteringMaintenance()) {
        return -1;
      } else {
        return 0;
      }
    }
  }

  /**
   * Comparator for sorting DataNodeInfo[] based on
   * slow, stale, entering_maintenance, decommissioning and decommissioned states.
   * Order: live {@literal ->} slow {@literal ->} stale {@literal ->}
   * entering_maintenance {@literal ->} decommissioning {@literal ->} decommissioned
   */
  @InterfaceAudience.Private 
  public static class StaleAndSlowComparator extends ServiceComparator {
    private final boolean avoidStaleDataNodesForRead;
    private final long staleInterval;
    private final boolean avoidSlowDataNodesForRead;
    private final Set<String> slowNodesUuidSet;

    /**
     * Constructor of ServiceAndStaleComparator
     * @param avoidStaleDataNodesForRead
     *          Whether or not to avoid using stale DataNodes for reading.
     * @param interval
     *          The time interval for marking datanodes as stale is passed from
     *          outside, since the interval may be changed dynamically.
     * @param avoidSlowDataNodesForRead
     *          Whether or not to avoid using slow DataNodes for reading.
     * @param slowNodesUuidSet
     *          Slow DataNodes UUID set.
     */
    public StaleAndSlowComparator(
        boolean avoidStaleDataNodesForRead, long interval,
        boolean avoidSlowDataNodesForRead, Set<String> slowNodesUuidSet) {
      this.avoidStaleDataNodesForRead = avoidStaleDataNodesForRead;
      this.staleInterval = interval;
      this.avoidSlowDataNodesForRead = avoidSlowDataNodesForRead;
      this.slowNodesUuidSet = slowNodesUuidSet;
    }

    @Override
    public int compare(DatanodeInfo a, DatanodeInfo b) {
      int ret = super.compare(a, b);
      if (ret != 0) {
        return ret;
      }

      // Stale nodes will be moved behind the normal nodes
      if (avoidStaleDataNodesForRead) {
        boolean aStale = a.isStale(staleInterval);
        boolean bStale = b.isStale(staleInterval);
        ret = aStale == bStale ? 0 : (aStale ? 1 : -1);
        if (ret != 0) {
          return ret;
        }
      }

      // Slow nodes will be moved behind the normal nodes
      if (avoidSlowDataNodesForRead) {
        boolean aSlow = slowNodesUuidSet.contains(a.getDatanodeUuid());
        boolean bSlow = slowNodesUuidSet.contains(b.getDatanodeUuid());
        ret = aSlow == bSlow ? 0 : (aSlow ? 1 : -1);
      }
      return ret;
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
    return DFSUtilClient.isValidName(src);
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
    for (String reserved : HdfsServerConstants.RESERVED_PATH_COMPONENTS) {
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
    return DFSUtilClient.bytes2String(bytes, 0, bytes.length);
  }

  /**
   * Converts a string to a byte array using UTF8 encoding.
   */
  public static byte[] string2Bytes(String str) {
    return DFSUtilClient.string2Bytes(str);
  }

  /**
   * Given a list of path components returns a path as a UTF8 String
   */
  public static String byteArray2PathString(final byte[][] components,
      final int offset, final int length) {
    // specifically not using StringBuilder to more efficiently build
    // string w/o excessive byte[] copies and charset conversions.
    final int range = offset + length;
    if (offset < 0 || range < offset || range > components.length) {
      throw new IndexOutOfBoundsException(
          "Incorrect index [offset, range, size] ["
              + offset + ", " + range + ", " + components.length + "]");
    }
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
    return DFSUtilClient
        .bytes2byteArray(bytes, bytes.length, (byte) Path.SEPARATOR_CHAR);
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
  public static byte[][] bytes2byteArray(byte[] bytes, int len,
      byte separator) {
    return DFSUtilClient.bytes2byteArray(bytes, len, separator);
  }

  /**
   * Return configuration key of format key.suffix1.suffix2...suffixN
   */
  public static String addKeySuffixes(String key, String... suffixes) {
    String keySuffix = DFSUtilClient.concatSuffixes(suffixes);
    return DFSUtilClient.addSuffix(key, keySuffix);
  }

  /**
   * Get all of the RPC addresses of the individual NNs in a given nameservice.
   * 
   * @param conf Configuration
   * @param nsId the nameservice whose NNs addresses we want.
   * @param defaultValue default address to return in case key is not found.
   * @return A map from nnId {@literal ->} RPC address of each NN in the
   * nameservice.
   */
  public static Map<String, InetSocketAddress> getRpcAddressesForNameserviceId(
      Configuration conf, String nsId, String defaultValue) {
    return DFSUtilClient.getAddressesForNameserviceId(conf, nsId, defaultValue,
                                                      DFS_NAMENODE_RPC_ADDRESS_KEY);
  }

  /**
   * @return a collection of all configured NN Kerberos principals.
   */
  public static Set<String> getAllNnPrincipals(Configuration conf) throws IOException {
    Set<String> principals = new HashSet<String>();
    for (String nsId : DFSUtilClient.getNameServiceIds(conf)) {
      if (HAUtil.isHAEnabled(conf, nsId)) {
        for (String nnId : DFSUtilClient.getNameNodeIds(conf, nsId)) {
          Configuration confForNn = new Configuration(conf);
          NameNode.initializeGenericKeys(confForNn, nsId, nnId);
          String principal = SecurityUtil.getServerPrincipal(confForNn
              .get(DFSConfigKeys.DFS_NAMENODE_KERBEROS_PRINCIPAL_KEY),
              DFSUtilClient.getNNAddress(confForNn).getHostName());
          principals.add(principal);
        }
      } else {
        Configuration confForNn = new Configuration(conf);
        NameNode.initializeGenericKeys(confForNn, nsId, null);
        String principal = SecurityUtil.getServerPrincipal(confForNn
            .get(DFSConfigKeys.DFS_NAMENODE_KERBEROS_PRINCIPAL_KEY),
            DFSUtilClient.getNNAddress(confForNn).getHostName());
        principals.add(principal);
      }
    }

    return principals;
  }

  /**
   * Returns list of Journalnode addresses from the configuration.
   *
   * @param conf configuration
   * @return list of journalnode host names
   * @throws URISyntaxException
   * @throws IOException
   */
  public static Set<String> getJournalNodeAddresses(
      Configuration conf) throws URISyntaxException, IOException {
    Set<String> journalNodeList = new HashSet<>();
    String journalsUri = "";
    try {
      journalsUri = conf.get(DFS_NAMENODE_SHARED_EDITS_DIR_KEY);
      if (journalsUri == null) {
        Collection<String> nameserviceIds = DFSUtilClient.
            getNameServiceIds(conf);
        for (String nsId : nameserviceIds) {
          journalsUri = DFSUtilClient.getConfValue(
              null, nsId, conf, DFS_NAMENODE_SHARED_EDITS_DIR_KEY);
          if (journalsUri == null) {
            Collection<String> nnIds = DFSUtilClient.getNameNodeIds(conf, nsId);
            for (String nnId : nnIds) {
              String suffix = DFSUtilClient.concatSuffixes(nsId, nnId);
              journalsUri = DFSUtilClient.getConfValue(
                  null, suffix, conf, DFS_NAMENODE_SHARED_EDITS_DIR_KEY);
              if (journalsUri == null ||
                  !journalsUri.startsWith("qjournal://")) {
                return journalNodeList;
              } else {
                LOG.warn(DFS_NAMENODE_SHARED_EDITS_DIR_KEY +" is to be " +
                    "configured as nameservice" +
                    " specific key(append it with nameserviceId), no need" +
                    " to append it with namenodeId");
                URI uri = new URI(journalsUri);
                List<InetSocketAddress> socketAddresses = Util.
                    getAddressesList(uri, conf);
                for (InetSocketAddress is : socketAddresses) {
                  journalNodeList.add(is.getHostName());
                }
              }
            }
          } else if (!journalsUri.startsWith("qjournal://")) {
            return journalNodeList;
          } else {
            URI uri = new URI(journalsUri);
            List<InetSocketAddress> socketAddresses = Util.
                getAddressesList(uri, conf);
            for (InetSocketAddress is : socketAddresses) {
              journalNodeList.add(is.getHostName());
            }
          }
        }
      } else {
        if (!journalsUri.startsWith("qjournal://")) {
          return journalNodeList;
        } else {
          URI uri = new URI(journalsUri);
          List<InetSocketAddress> socketAddresses = Util.getAddressesList(uri, conf);
          for (InetSocketAddress is : socketAddresses) {
            journalNodeList.add(is.getHostName());
          }
        }
      }
    } catch(UnknownHostException e) {
      LOG.error("The conf property " + DFS_NAMENODE_SHARED_EDITS_DIR_KEY
          + " is not properly set with correct journal node hostnames");
      throw new UnknownHostException(journalsUri);
    } catch(URISyntaxException e)  {
      LOG.error("The conf property " + DFS_NAMENODE_SHARED_EDITS_DIR_KEY
          + "is not set properly with correct journal node uri");
      throw new URISyntaxException(journalsUri, "The conf property " +
          DFS_NAMENODE_SHARED_EDITS_DIR_KEY + "is not" +
          " properly set with correct journal node uri");
    }

    return journalNodeList;
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
    Map<String, Map<String, InetSocketAddress>> addressList = DFSUtilClient.getAddresses(
        conf, null, DFS_NAMENODE_BACKUP_ADDRESS_KEY);
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
    Map<String, Map<String, InetSocketAddress>> addressList = DFSUtilClient.getAddresses(
        conf, null, DFS_NAMENODE_SECONDARY_HTTP_ADDRESS_KEY);
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
      defaultAddress = NetUtils.getHostPortString(
          DFSUtilClient.getNNAddress(conf));
    } catch (IllegalArgumentException e) {
      defaultAddress = null;
    }
    
    Map<String, Map<String, InetSocketAddress>> addressList =
      DFSUtilClient.getAddresses(conf, defaultAddress,
                                 DFS_NAMENODE_SERVICE_RPC_ADDRESS_KEY,
                                 DFS_NAMENODE_RPC_ADDRESS_KEY);
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
      defaultAddress = NetUtils.getHostPortString(
          DFSUtilClient.getNNAddress(conf));
    } catch (IllegalArgumentException e) {
      defaultAddress = null;
    }

    Collection<String> parentNameServices = getParentNameServices(conf);

    Map<String, Map<String, InetSocketAddress>> addressList =
            getAddressesForNsIds(conf, parentNameServices,
                                               defaultAddress,
                                               DFS_NAMENODE_SERVICE_RPC_ADDRESS_KEY,
                                               DFS_NAMENODE_RPC_ADDRESS_KEY);
    if (addressList.isEmpty()) {
      throw new IOException("Incorrect configuration: namenode address "
              + DFS_NAMENODE_SERVICE_RPC_ADDRESS_KEY + "." + parentNameServices
              + " or "
              + DFS_NAMENODE_RPC_ADDRESS_KEY + "." + parentNameServices
              + " is not configured.");
    }
    return addressList;
  }

  /**
   * Returns list of InetSocketAddresses corresponding to lifeline RPC servers
   * at namenodes from the configuration.
   *
   * @param conf configuration
   * @return list of InetSocketAddress
   * @throws IOException on error
   */
  public static Map<String, Map<String, InetSocketAddress>>
      getNNLifelineRpcAddressesForCluster(Configuration conf)
      throws IOException {

    Collection<String> parentNameServices = getParentNameServices(conf);

    return getAddressesForNsIds(conf, parentNameServices, null,
        DFS_NAMENODE_LIFELINE_RPC_ADDRESS_KEY);
  }

  //
  /**
   * Returns the configured address for all NameNodes in the cluster.
   * This is similar with DFSUtilClient.getAddressesForNsIds()
   * but can access DFSConfigKeys.
   *
   * @param conf configuration
   * @param defaultAddress default address to return in case key is not found.
   * @param keys Set of keys to look for in the order of preference
   *
   * @return a map(nameserviceId to map(namenodeId to InetSocketAddress))
   */
  static Map<String, Map<String, InetSocketAddress>> getAddressesForNsIds(
      Configuration conf, Collection<String> nsIds, String defaultAddress,
      String... keys) {
    // Look for configurations of the form
    // <key>[.<nameserviceId>][.<namenodeId>]
    // across all of the configured nameservices and namenodes.
    Map<String, Map<String, InetSocketAddress>> ret = Maps.newLinkedHashMap();
    for (String nsId : DFSUtilClient.emptyAsSingletonNull(nsIds)) {

      String configKeyWithHost =
          DFSConfigKeys.DFS_NAMESERVICES_RESOLUTION_ENABLED + "." + nsId;
      boolean resolveNeeded = conf.getBoolean(configKeyWithHost,
          DFSConfigKeys.DFS_NAMESERVICES_RESOLUTION_ENABLED_DEFAULT);

      Map<String, InetSocketAddress> isas;

      if (resolveNeeded) {
        DomainNameResolver dnr = DomainNameResolverFactory.newInstance(
            conf, nsId, DFSConfigKeys.DFS_NAMESERVICES_RESOLVER_IMPL);
        isas = DFSUtilClient.getResolvedAddressesForNsId(
            conf, nsId, dnr, defaultAddress, keys);
      } else {
        isas = DFSUtilClient.getAddressesForNameserviceId(
            conf, nsId, defaultAddress, keys);
      }
      if (!isas.isEmpty()) {
        ret.put(nsId, isas);
      }
    }
    return ret;
  }

  private static Collection<String> getParentNameServices(Configuration conf)
      throws IOException {
    Collection<String> parentNameServices = conf.getTrimmedStringCollection(
        DFSConfigKeys.DFS_INTERNAL_NAMESERVICES_KEY);

    if (parentNameServices.isEmpty()) {
      parentNameServices = conf.getTrimmedStringCollection(
          DFSConfigKeys.DFS_NAMESERVICES);
    } else {
      // Ensure that the internal service is indeed in the list of all available
      // nameservices.
      Collection<String> namespaces = conf
          .getTrimmedStringCollection(DFSConfigKeys.DFS_NAMESERVICES);
      Set<String> availableNameServices = new HashSet<>(namespaces);
      for (String nsId : parentNameServices) {
        if (!availableNameServices.contains(nsId)) {
          throw new IOException("Unknown nameservice: " + nsId);
        }
      }
    }

    return parentNameServices;
  }

  /**
   * Map a logical namenode ID to its lifeline address.  Use the given
   * nameservice if specified, or the configured one if none is given.
   *
   * @param conf Configuration
   * @param nsId which nameservice nnId is a part of, optional
   * @param nnId the namenode ID to get the service addr for
   * @return the lifeline addr, null if it could not be determined
   */
  public static String getNamenodeLifelineAddr(final Configuration conf,
      String nsId, String nnId) {

    if (nsId == null) {
      nsId = getOnlyNameServiceIdOrNull(conf);
    }

    String lifelineAddrKey = DFSUtilClient.concatSuffixes(
        DFSConfigKeys.DFS_NAMENODE_LIFELINE_RPC_ADDRESS_KEY, nsId, nnId);

    return conf.get(lifelineAddrKey);
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
        DFSUtilClient.getHaNnRpcAddresses(conf);
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
    return !ids.isEmpty()? ids: DFSUtilClient.getNameServiceIds(conf);
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
      URI nsUri = createUri(HdfsConstants.HDFS_URI_SCHEME, nsId, -1);
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
          String addr = conf.get(DFSUtilClient.concatSuffixes(key, nsId));
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

    // Add the default URI if it is an HDFS URI and we haven't come up with a
    // valid non-nameservice NN address yet.  Consider the servicerpc-address
    // and rpc-address to be the "unnamed" nameservice.  defaultFS is our
    // fallback when rpc-address isn't given.  We therefore only want to add
    // the defaultFS when neither the servicerpc-address (which is preferred)
    // nor the rpc-address (which overrides defaultFS) is given.
    if (!uriFound) {
      URI defaultUri = FileSystem.getDefaultUri(conf);
      if (defaultUri != null) {
        // checks if defaultUri is ip:port format
        // and convert it to hostname:port format
        if (defaultUri.getPort() != -1) {
          defaultUri = createUri(defaultUri.getScheme(),
              NetUtils.createSocketAddr(defaultUri.getHost(),
                  defaultUri.getPort()));
        }

        defaultUri = trimUri(defaultUri);

        if (HdfsConstants.HDFS_URI_SCHEME.equals(defaultUri.getScheme()) &&
            !nonPreferredUris.contains(defaultUri)) {
          ret.add(defaultUri);
        }
      }
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
    String defaultHost) {
    InetSocketAddress sockAddr = NetUtils.createSocketAddr(configuredAddress);
    final InetAddress addr = sockAddr.getAddress();
    if (addr != null && addr.isAnyLocalAddress()) {
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

  /**
   * Round bytes to GiB (gibibyte)
   * @param bytes number of bytes
   * @return number of GiB
   */
  public static int roundBytesToGB(long bytes) {
    return Math.round((float)bytes/ 1024 / 1024 / 1024);
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
    Collection<String> nsIds = DFSUtilClient.getNameServiceIds(conf);
    if (1 == nsIds.size()) {
      return nsIds.toArray(new String[1])[0];
    }
    String nnId = conf.get(DFS_HA_NAMENODE_ID_KEY);
    
    return getSuffixIDs(conf, addressKey, null, nnId, LOCAL_ADDRESS_MATCHER)[0];
  }

  /**
   * Determine the {@link InetSocketAddress} to bind to, for any service.
   * In case of HA or federation, the address is assumed to specified as
   * {@code confKey}.NAMESPACEID.NAMENODEID as appropriate.
   *
   * @param conf configuration.
   * @param confKey configuration key (prefix if HA/federation) used to
   *        specify the address for the service.
   * @param defaultValue default value for the address.
   * @param bindHostKey configuration key (prefix if HA/federation)
   *        specifying host to bind to.
   * @return the address to bind to.
   */
  public static InetSocketAddress getBindAddress(Configuration conf,
      String confKey, String defaultValue, String bindHostKey) {
    InetSocketAddress address;
    String nsId = DFSUtil.getNamenodeNameServiceId(conf);
    String bindHostActualKey;
    if (nsId != null) {
      String namenodeId = HAUtil.getNameNodeId(conf, nsId);
      address = DFSUtilClient.getAddressesForNameserviceId(
          conf, nsId, null, confKey).get(namenodeId);
      bindHostActualKey = DFSUtil.addKeySuffixes(bindHostKey, nsId, namenodeId);
    } else {
      address = NetUtils.createSocketAddr(conf.get(confKey, defaultValue));
      bindHostActualKey = bindHostKey;
    }

    String bindHost = conf.get(bindHostActualKey);
    if (bindHost == null || bindHost.isEmpty()) {
      bindHost = address.getHostName();
    }
    return new InetSocketAddress(bindHost, address.getPort());
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
    
    Collection<String> nsIds = DFSUtilClient.getNameServiceIds(conf);
    for (String nsId : DFSUtilClient.emptyAsSingletonNull(nsIds)) {
      if (knownNsId != null && !knownNsId.equals(nsId)) {
        continue;
      }
      
      Collection<String> nnIds = DFSUtilClient.getNameNodeIds(conf, nsId);
      for (String nnId : DFSUtilClient.emptyAsSingletonNull(nnIds)) {
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

  /** Create an URI from scheme and address. */
  public static URI createUri(String scheme, InetSocketAddress address) {
    return createUri(scheme, address.getHostName(), address.getPort());
  }

  /** Create an URI from scheme, host, and port. */
  public static URI createUri(String scheme, String host, int port) {
    try {
      return new URI(scheme, null, host, port, null, null, null);
    } catch (URISyntaxException x) {
      throw new IllegalArgumentException(x.getMessage(), x);
    }
  }

  /** Remove unnecessary path from HDFS URI. */
  static URI trimUri(URI uri) {
    String path = uri.getPath();
    if (HdfsConstants.HDFS_URI_SCHEME.equals(uri.getScheme()) &&
        path != null && !path.isEmpty()) {
      uri = createUri(uri.getScheme(), uri.getHost(), uri.getPort());
    }
    return uri;
  }

  /**
   * Add protobuf based protocol to the {@link org.apache.hadoop.ipc.RPC.Server}.
   * This method is for exclusive use by the hadoop libraries, as its signature
   * changes with the version of the shaded protobuf library it has been built with.
   * @param conf configuration
   * @param protocol Protocol interface
   * @param service service that implements the protocol
   * @param server RPC server to which the protocol &amp; implementation is
   *               added to
   * @throws IOException failure
   */
  @InterfaceAudience.Private
  @InterfaceStability.Unstable
  public static void addInternalPBProtocol(Configuration conf,
      Class<?> protocol,
      BlockingService service,
      RPC.Server server) throws IOException {
    RPC.setProtocolEngine(conf, protocol, ProtobufRpcEngine2.class);
    server.addProtocol(RPC.RpcKind.RPC_PROTOCOL_BUFFER, protocol, service);
  }

  /**
   * Add protobuf based protocol to the {@link org.apache.hadoop.ipc.RPC.Server}.
   * Deprecated as it will only reliably compile if an unshaded protobuf library
   * is also on the classpath.
   * @param conf configuration
   * @param protocol Protocol interface
   * @param service service that implements the protocol
   * @param server RPC server to which the protocol &amp; implementation is
   *               added to
   * @throws IOException
   */
  @Deprecated
  public static void addPBProtocol(Configuration conf, Class<?> protocol,
      BlockingService service, RPC.Server server) throws IOException {
    addInternalPBProtocol(conf, protocol, service, server);
  }

  /**
   * Add protobuf based protocol to the {@link RPC.Server}.
   * This engine uses Protobuf 2.5.0. Recommended to upgrade to
   * Protobuf 3.x from hadoop-thirdparty and use
   * {@link DFSUtil#addInternalPBProtocol(Configuration, Class, BlockingService,
   * RPC.Server)}.
   * @param conf configuration
   * @param protocol Protocol interface
   * @param service service that implements the protocol
   * @param server RPC server to which the protocol &amp; implementation is
   *               added to
   * @throws IOException
   */
  @Deprecated
  public static void addPBProtocol(Configuration conf, Class<?> protocol,
      com.google.protobuf.BlockingService service, RPC.Server server)
      throws IOException {
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

    String serviceAddrKey = DFSUtilClient.concatSuffixes(
        DFSConfigKeys.DFS_NAMENODE_SERVICE_RPC_ADDRESS_KEY, nsId, nnId);

    String addrKey = DFSUtilClient.concatSuffixes(
        DFSConfigKeys.DFS_NAMENODE_RPC_ADDRESS_KEY, nsId, nnId);

    String serviceRpcAddr = conf.get(serviceAddrKey);
    if (serviceRpcAddr == null) {
      serviceRpcAddr = conf.get(addrKey);
    }
    return serviceRpcAddr;
  }

  /**
   * Map a logical namenode ID to its web address. Use the given nameservice if
   * specified, or the configured one if none is given.
   *
   * @param conf Configuration
   * @param nsId which nameservice nnId is a part of, optional
   * @param nnId the namenode ID to get the service addr for
   * @return the service addr, null if it could not be determined
   */
  public static String getNamenodeWebAddr(final Configuration conf, String nsId,
      String nnId) {

    if (nsId == null) {
      nsId = getOnlyNameServiceIdOrNull(conf);
    }

    String webAddrBaseKey = DFSConfigKeys.DFS_NAMENODE_HTTP_ADDRESS_KEY;
    String webAddrDefault = DFSConfigKeys.DFS_NAMENODE_HTTP_ADDRESS_DEFAULT;
    if (getHttpPolicy(conf) == HttpConfig.Policy.HTTPS_ONLY) {
      webAddrBaseKey = DFSConfigKeys.DFS_NAMENODE_HTTPS_ADDRESS_KEY;
      webAddrDefault = DFSConfigKeys.DFS_NAMENODE_HTTPS_ADDRESS_DEFAULT;
    }
    String webAddrKey = DFSUtilClient.concatSuffixes(
        webAddrBaseKey, nsId, nnId);
    String webAddr = conf.get(webAddrKey, webAddrDefault);
    return webAddr;
  }

  /**
   * Get all of the Web addresses of the individual NNs in a given nameservice.
   *
   * @param conf Configuration
   * @param nsId the nameservice whose NNs addresses we want.
   * @param defaultValue default address to return in case key is not found.
   * @return A map from nnId {@literal ->} Web address of each NN in the
   * nameservice.
   */
  public static Map<String, InetSocketAddress> getWebAddressesForNameserviceId(
      Configuration conf, String nsId, String defaultValue) {
    return DFSUtilClient.getAddressesForNameserviceId(conf, nsId, defaultValue,
        DFSConfigKeys.DFS_NAMENODE_HTTP_ADDRESS_KEY);
  }

  /**
   * If the configuration refers to only a single nameservice, return the
   * name of that nameservice. If it refers to 0 or more than 1, return null.
   */
  public static String getOnlyNameServiceIdOrNull(Configuration conf) {
    Collection<String> nsIds = DFSUtilClient.getNameServiceIds(conf);
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
   * Get http policy.
   */
  public static HttpConfig.Policy getHttpPolicy(Configuration conf) {
    String policyStr = conf.get(DFSConfigKeys.DFS_HTTP_POLICY_KEY,
        DFSConfigKeys.DFS_HTTP_POLICY_DEFAULT);
    HttpConfig.Policy policy = HttpConfig.Policy.fromString(policyStr);
    if (policy == null) {
      throw new HadoopIllegalArgumentException("Unrecognized value '"
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
      LOG.warn("Setting password to null since IOException is caught"
          + " when getting password", ioe);

      password = null;
    }
    return password;
  }

  /**
   * Converts a Date into an ISO-8601 formatted datetime string.
   */
  public static String dateToIso8601String(Date date) {
    return DFSUtilClient.dateToIso8601String(date);
  }

  /**
   * Converts a time duration in milliseconds into DDD:HH:MM:SS format.
   */
  public static String durationToString(long durationMs) {
    return DFSUtilClient.durationToString(durationMs);
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
   * Load HTTPS-related configuration.
   */
  public static Configuration loadSslConfiguration(Configuration conf) {
    Configuration sslConf = new Configuration(false);

    sslConf.addResource(conf.get(
        DFSConfigKeys.DFS_SERVER_HTTPS_KEYSTORE_RESOURCE_KEY,
        DFSConfigKeys.DFS_SERVER_HTTPS_KEYSTORE_RESOURCE_DEFAULT));

    final String[] reqSslProps = {
        DFSConfigKeys.DFS_SERVER_HTTPS_TRUSTSTORE_LOCATION_KEY,
        DFSConfigKeys.DFS_SERVER_HTTPS_KEYSTORE_LOCATION_KEY,
        DFSConfigKeys.DFS_SERVER_HTTPS_KEYSTORE_PASSWORD_KEY,
        DFSConfigKeys.DFS_SERVER_HTTPS_KEYPASSWORD_KEY
    };

    // Check if the required properties are included
    for (String sslProp : reqSslProps) {
      if (sslConf.get(sslProp) == null) {
        LOG.warn("SSL config " + sslProp + " is missing. If " +
            DFSConfigKeys.DFS_SERVER_HTTPS_KEYSTORE_RESOURCE_KEY +
            " is specified, make sure it is a relative path");
      }
    }

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

    String filterInitializerConfKey = "hadoop.http.filter.initializers";
    String initializers = conf.get(filterInitializerConfKey, "");

    String[] parts = initializers.split(",");
    Set<String> target = new LinkedHashSet<String>();
    for (String filterInitializer : parts) {
      filterInitializer = filterInitializer.trim();
      if (filterInitializer.equals(
          AuthenticationFilterInitializer.class.getName()) ||
          filterInitializer.equals(
          ProxyUserAuthenticationFilterInitializer.class.getName()) ||
          filterInitializer.isEmpty()) {
        continue;
      }
      target.add(filterInitializer);
    }
    target.add(AuthFilterInitializer.class.getName());
    initializers = StringUtils.join(target, ",");
    conf.set(filterInitializerConfKey, initializers);

    LOG.info("Filter initializers set : " + initializers);

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
    KeyProvider keyProvider = HdfsKMSUtil.createKeyProvider(conf);
    if (keyProvider == null) {
      return null;
    }
    KeyProviderCryptoExtension cryptoProvider = KeyProviderCryptoExtension
        .createKeyProviderCryptoExtension(keyProvider);
    return cryptoProvider;
  }

  /**
   * Decodes an HDFS delegation token to its identifier.
   *
   * @param token the token
   * @return the decoded identifier.
   * @throws IOException
   */
  public static DelegationTokenIdentifier decodeDelegationToken(
      final Token<DelegationTokenIdentifier> token) throws IOException {
    final DelegationTokenIdentifier id = new DelegationTokenIdentifier();
    final ByteArrayInputStream buf =
        new ByteArrayInputStream(token.getIdentifier());
    try (DataInputStream in = new DataInputStream(buf)) {
      id.readFields(in);
    }
    return id;
  }

  /**
   * Throw if the given directory has any non-empty protected descendants
   * (including itself).
   *
   * @param fsd the namespace tree.
   * @param iip directory whose descendants are to be checked.
   * @throws AccessControlException if a non-empty protected descendant
   *                                was found.
   * @throws ParentNotDirectoryException
   * @throws UnresolvedLinkException
   */
  public static void checkProtectedDescendants(
      FSDirectory fsd, INodesInPath iip)
          throws AccessControlException, UnresolvedLinkException,
          ParentNotDirectoryException {
    final SortedSet<String> protectedDirs = fsd.getProtectedDirectories();
    if (protectedDirs.isEmpty()) {
      return;
    }

    String src = iip.getPath();
    // Is src protected? Caller has already checked it is non-empty.
    if (protectedDirs.contains(src)) {
      throw new AccessControlException(
          "Cannot delete/rename non-empty protected directory " + src);
    }

    // Are any descendants of src protected?
    // The subSet call returns only the descendants of src since
    // {@link Path#SEPARATOR} is "/" and '0' is the next ASCII
    // character after '/'.
    for (String descendant :
        protectedDirs.subSet(src + Path.SEPARATOR, src + "0")) {
      INodesInPath subdirIIP =
          fsd.getINodesInPath(descendant, FSDirectory.DirOp.WRITE);
      if (fsd.isNonEmptyDirectory(subdirIIP)) {
        throw new AccessControlException(
            "Cannot delete/rename non-empty protected subdirectory "
            + descendant);
      }
    }

    if (fsd.isProtectedSubDirectoriesEnable()) {
      while (!src.isEmpty()) {
        int index = src.lastIndexOf(Path.SEPARATOR_CHAR);
        src = src.substring(0, index);
        if (protectedDirs.contains(src)) {
          throw new AccessControlException(
              "Cannot delete/rename subdirectory under protected subdirectory "
              + src);
        }
      }
    }
  }

  /**
   * Generates HdfsFileStatus flags.
   * @param isEncrypted Sets HAS_CRYPT
   * @param isErasureCoded Sets HAS_EC
   * @param isSnapShottable Sets SNAPSHOT_ENABLED
   * @param hasAcl Sets HAS_ACL
   * @return HdfsFileStatus Flags
   */
  public static EnumSet<HdfsFileStatus.Flags> getFlags(
      final boolean isEncrypted, final boolean isErasureCoded,
      boolean isSnapShottable, boolean hasAcl) {
    EnumSet<HdfsFileStatus.Flags> flags =
        EnumSet.noneOf(HdfsFileStatus.Flags.class);
    if (hasAcl) {
      flags.add(HdfsFileStatus.Flags.HAS_ACL);
    }
    if (isEncrypted) {
      flags.add(HdfsFileStatus.Flags.HAS_CRYPT);
    }
    if (isErasureCoded) {
      flags.add(HdfsFileStatus.Flags.HAS_EC);
    }
    if (isSnapShottable) {
      flags.add(HdfsFileStatus.Flags.SNAPSHOT_ENABLED);
    }
    return flags;
  }

  /**
   * Check if the given path is the child of parent path.
   * @param path Path to be check.
   * @param parent Parent path.
   * @return True if parent path is parent entry for given path.
   */
  public static boolean isParentEntry(final String path, final String parent) {
    if (!path.startsWith(parent)) {
      return false;
    }

    if (path.equals(parent)) {
      return true;
    }

    return path.charAt(parent.length()) == Path.SEPARATOR_CHAR
        || parent.equals(Path.SEPARATOR);
  }

  /**
   * Add transfer rate metrics in bytes per second.
   * @param metrics metrics for datanodes
   * @param read bytes read
   * @param durationInNS read duration in nanoseconds
   */
  public static void addTransferRateMetric(final DataNodeMetrics metrics, final long read,
      final long durationInNS) {
    metrics.addReadTransferRate(getTransferRateInBytesPerSecond(read, durationInNS));
  }

  /**
   * Calculate the transfer rate in bytes per second.
   *
   * We have the read duration in nanoseconds for precision for transfers taking a few nanoseconds.
   * We treat shorter durations below 1 ns as 1 ns as we also want to capture reads taking less
   * than a nanosecond. To calculate transferRate in bytes per second, we avoid multiplying bytes
   * read by 10^9 to avoid overflow. Instead, we first calculate the duration in seconds in double
   * to keep the decimal values for smaller durations. We then divide bytes read by
   * durationInSeconds to get the transferRate in bytes per second.
   *
   * We also replace a negative value for transferred bytes with 0 byte.
   *
   * @param bytes bytes read
   * @param durationInNS read duration in nanoseconds
   * @return bytes per second
   */
  public static long getTransferRateInBytesPerSecond(long bytes, long durationInNS) {
    bytes = Math.max(bytes, 0);
    durationInNS = Math.max(durationInNS, 1);
    double durationInSeconds = (double) durationInNS / TimeUnit.SECONDS.toNanos(1);
    return (long) (bytes / durationInSeconds);
  }

  /**
   * Construct a HostSet from an array of "ip:port" strings.
   * @param nodesHostPort ip port string array.
   * @return HostSet of InetSocketAddress.
   */
  public static HostSet getHostSet(String[] nodesHostPort) {
    HostSet retSet = new HostSet();
    for (String hostPort : nodesHostPort) {
      try {
        URI uri = new URI("dummy", hostPort, null, null, null);
        int port = uri.getPort();
        if (port < 0) {
          LOG.warn(String.format("The ip:port `%s` is invalid, skip this node.", hostPort));
          continue;
        }
        InetSocketAddress inetSocketAddress = new InetSocketAddress(uri.getHost(), port);
        if (inetSocketAddress.isUnresolved()) {
          LOG.warn(String.format("Failed to resolve address `%s`", hostPort));
          continue;
        }
        retSet.add(inetSocketAddress);
      } catch (URISyntaxException e) {
        LOG.warn(String.format("Failed to parse `%s`", hostPort));
      }
    }
    return retSet;
  }
}
