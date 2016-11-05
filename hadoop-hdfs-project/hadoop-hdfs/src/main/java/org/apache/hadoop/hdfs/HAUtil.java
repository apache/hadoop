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

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_HA_NAMENODE_ID_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_HTTPS_ADDRESS_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_HTTPS_BIND_HOST_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_HTTP_ADDRESS_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_HTTP_BIND_HOST_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_LIFELINE_RPC_ADDRESS_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_LIFELINE_RPC_BIND_HOST_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_RPC_ADDRESS_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_RPC_BIND_HOST_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_SERVICE_RPC_ADDRESS_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_SERVICE_RPC_BIND_HOST_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_SHARED_EDITS_DIR_KEY;
import static org.apache.hadoop.security.SecurityUtil.buildTokenService;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.NameNodeProxiesClient.ProxyAndInfo;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenSelector;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.namenode.ha.AbstractNNFailoverProxyProvider;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.ipc.StandbyException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

@InterfaceAudience.Private
public class HAUtil {
  
  private static final Log LOG = 
    LogFactory.getLog(HAUtil.class);
  
  private static final DelegationTokenSelector tokenSelector =
      new DelegationTokenSelector();

  private static final String[] HA_SPECIAL_INDEPENDENT_KEYS = new String[]{
    DFS_NAMENODE_RPC_ADDRESS_KEY,
    DFS_NAMENODE_RPC_BIND_HOST_KEY,
    DFS_NAMENODE_LIFELINE_RPC_ADDRESS_KEY,
    DFS_NAMENODE_LIFELINE_RPC_BIND_HOST_KEY,
    DFS_NAMENODE_SERVICE_RPC_ADDRESS_KEY,
    DFS_NAMENODE_SERVICE_RPC_BIND_HOST_KEY,
    DFS_NAMENODE_HTTP_ADDRESS_KEY,
    DFS_NAMENODE_HTTPS_ADDRESS_KEY,
    DFS_NAMENODE_HTTP_BIND_HOST_KEY,
    DFS_NAMENODE_HTTPS_BIND_HOST_KEY,
  };

  private HAUtil() { /* Hidden constructor */ }

  /**
   * Returns true if HA for namenode is configured for the given nameservice
   * 
   * @param conf Configuration
   * @param nsId nameservice, or null if no federated NS is configured
   * @return true if HA is configured in the configuration; else false.
   */
  public static boolean isHAEnabled(Configuration conf, String nsId) {
    Map<String, Map<String, InetSocketAddress>> addresses =
      DFSUtil.getHaNnRpcAddresses(conf);
    if (addresses == null) return false;
    Map<String, InetSocketAddress> nnMap = addresses.get(nsId);
    return nnMap != null && nnMap.size() > 1;
  }

  /**
   * Returns true if HA is using a shared edits directory.
   *
   * @param conf Configuration
   * @return true if HA config is using a shared edits dir, false otherwise.
   */
  public static boolean usesSharedEditsDir(Configuration conf) {
    return null != conf.get(DFS_NAMENODE_SHARED_EDITS_DIR_KEY);
  }

  /**
   * Get the namenode Id by matching the {@code addressKey}
   * with the the address of the local node.
   * 
   * If {@link DFSConfigKeys#DFS_HA_NAMENODE_ID_KEY} is not specifically
   * configured, this method determines the namenode Id by matching the local
   * node's address with the configured addresses. When a match is found, it
   * returns the namenode Id from the corresponding configuration key.
   * 
   * @param conf Configuration
   * @return namenode Id on success, null on failure.
   * @throws HadoopIllegalArgumentException on error
   */
  public static String getNameNodeId(Configuration conf, String nsId) {
    String namenodeId = conf.getTrimmed(DFS_HA_NAMENODE_ID_KEY);
    if (namenodeId != null) {
      return namenodeId;
    }
    
    String suffixes[] = DFSUtil.getSuffixIDs(conf, DFS_NAMENODE_RPC_ADDRESS_KEY,
        nsId, null, DFSUtil.LOCAL_ADDRESS_MATCHER);
    if (suffixes == null) {
      String msg = "Configuration " + DFS_NAMENODE_RPC_ADDRESS_KEY + 
          " must be suffixed with nameservice and namenode ID for HA " +
          "configuration.";
      throw new HadoopIllegalArgumentException(msg);
    }
    
    return suffixes[1];
  }

  /**
   * Similar to
   * {@link DFSUtil#getNameServiceIdFromAddress(Configuration, 
   * InetSocketAddress, String...)}
   */
  public static String getNameNodeIdFromAddress(final Configuration conf, 
      final InetSocketAddress address, String... keys) {
    // Configuration with a single namenode and no nameserviceId
    String[] ids = DFSUtil.getSuffixIDs(conf, address, keys);
    if (ids != null && ids.length > 1) {
      return ids[1];
    }
    return null;
  }
  
  /**
   * Get the NN ID of the other node in an HA setup.
   * 
   * @param conf the configuration of this node
   * @return the NN ID of the other node in this nameservice
   */
  public static List<String> getNameNodeIdOfOtherNodes(Configuration conf, String nsId) {
    Preconditions.checkArgument(nsId != null,
        "Could not determine namespace id. Please ensure that this " +
        "machine is one of the machines listed as a NN RPC address, " +
        "or configure " + DFSConfigKeys.DFS_NAMESERVICE_ID);
    
    Collection<String> nnIds = DFSUtilClient.getNameNodeIds(conf, nsId);
    String myNNId = conf.get(DFSConfigKeys.DFS_HA_NAMENODE_ID_KEY);
    Preconditions.checkArgument(nnIds != null,
        "Could not determine namenode ids in namespace '%s'. " +
        "Please configure " +
        DFSUtil.addKeySuffixes(DFSConfigKeys.DFS_HA_NAMENODES_KEY_PREFIX,
            nsId),
        nsId);
    Preconditions.checkArgument(nnIds.size() >= 2,
        "Expected at least 2 NameNodes in namespace '%s'. " +
          "Instead, got only %s (NN ids were '%s')",
          nsId, nnIds.size(), Joiner.on("','").join(nnIds));
    Preconditions.checkState(myNNId != null && !myNNId.isEmpty(),
        "Could not determine own NN ID in namespace '%s'. Please " +
        "ensure that this node is one of the machines listed as an " +
        "NN RPC address, or configure " + DFSConfigKeys.DFS_HA_NAMENODE_ID_KEY,
        nsId);

    ArrayList<String> namenodes = Lists.newArrayList(nnIds);
    namenodes.remove(myNNId);
    assert namenodes.size() >= 1;
    return namenodes;
  }

  /**
   * Given the configuration for this node, return a Configuration object for
   * the other node in an HA setup.
   * 
   * @param myConf the configuration of this node
   * @return the configuration of the other node in an HA setup
   */
  public static List<Configuration> getConfForOtherNodes(
      Configuration myConf) {
    
    String nsId = DFSUtil.getNamenodeNameServiceId(myConf);
    List<String> otherNn = getNameNodeIdOfOtherNodes(myConf, nsId);

    // Look up the address of the other NNs
    List<Configuration> confs = new ArrayList<Configuration>(otherNn.size());
    myConf = new Configuration(myConf);
    // unset independent properties
    for (String idpKey : HA_SPECIAL_INDEPENDENT_KEYS) {
      myConf.unset(idpKey);
    }
    for (String nn : otherNn) {
      Configuration confForOtherNode = new Configuration(myConf);
      NameNode.initializeGenericKeys(confForOtherNode, nsId, nn);
      confs.add(confForOtherNode);
    }
    return confs;
  }

  /**
   * This is used only by tests at the moment.
   * @return true if the NN should allow read operations while in standby mode.
   */
  public static boolean shouldAllowStandbyReads(Configuration conf) {
    return conf.getBoolean("dfs.ha.allow.stale.reads", false);
  }
  
  public static void setAllowStandbyReads(Configuration conf, boolean val) {
    conf.setBoolean("dfs.ha.allow.stale.reads", val);
  }

  /**
   * Check whether logical URI is needed for the namenode and
   * the corresponding failover proxy provider in the config.
   *
   * @param conf Configuration
   * @param nameNodeUri The URI of namenode
   * @return true if logical URI is needed. false, if not needed.
   * @throws IOException most likely due to misconfiguration.
   */
  public static boolean useLogicalUri(Configuration conf, URI nameNodeUri) 
      throws IOException {
    // Create the proxy provider. Actual proxy is not created.
    AbstractNNFailoverProxyProvider<ClientProtocol> provider = NameNodeProxiesClient
        .createFailoverProxyProvider(conf, nameNodeUri, ClientProtocol.class,
            false, null);

    // No need to use logical URI since failover is not configured.
    if (provider == null) {
      return false;
    }
    // Check whether the failover proxy provider uses logical URI.
    return provider.useLogicalURI();
  }

  /**
   * Locate a delegation token associated with the given HA cluster URI, and if
   * one is found, clone it to also represent the underlying namenode address.
   * @param ugi the UGI to modify
   * @param haUri the logical URI for the cluster
   * @param nnAddrs collection of NNs in the cluster to which the token
   * applies
   */
  public static void cloneDelegationTokenForLogicalUri(
      UserGroupInformation ugi, URI haUri,
      Collection<InetSocketAddress> nnAddrs) {
    // this cloning logic is only used by hdfs
    Text haService = HAUtilClient.buildTokenServiceForLogicalUri(haUri,
                                                                 HdfsConstants.HDFS_URI_SCHEME);
    Token<DelegationTokenIdentifier> haToken =
        tokenSelector.selectToken(haService, ugi.getTokens());
    if (haToken != null) {
      for (InetSocketAddress singleNNAddr : nnAddrs) {
        // this is a minor hack to prevent physical HA tokens from being
        // exposed to the user via UGI.getCredentials(), otherwise these
        // cloned tokens may be inadvertently propagated to jobs
        Token<DelegationTokenIdentifier> specificToken =
            haToken.privateClone(buildTokenService(singleNNAddr));
        Text alias = new Text(
            HAUtilClient.buildTokenServicePrefixForLogicalUri(
                HdfsConstants.HDFS_URI_SCHEME)
                + "//" + specificToken.getService());
        ugi.addToken(alias, specificToken);
        if (LOG.isDebugEnabled()) {
          LOG.debug("Mapped HA service delegation token for logical URI " +
              haUri + " to namenode " + singleNNAddr);
        }
      }
    } else {
      if (LOG.isDebugEnabled()) {
        LOG.debug("No HA service delegation token found for logical URI " +
            haUri);
      }
    }
  }

  /**
   * Get the internet address of the currently-active NN. This should rarely be
   * used, since callers of this method who connect directly to the NN using the
   * resulting InetSocketAddress will not be able to connect to the active NN if
   * a failover were to occur after this method has been called.
   * 
   * @param fs the file system to get the active address of.
   * @return the internet address of the currently-active NN.
   * @throws IOException if an error occurs while resolving the active NN.
   */
  public static InetSocketAddress getAddressOfActive(FileSystem fs)
      throws IOException {
    if (!(fs instanceof DistributedFileSystem)) {
      throw new IllegalArgumentException("FileSystem " + fs + " is not a DFS.");
    }
    // force client address resolution.
    fs.exists(new Path("/"));
    DistributedFileSystem dfs = (DistributedFileSystem) fs;
    DFSClient dfsClient = dfs.getClient();
    return RPC.getServerAddress(dfsClient.getNamenode());
  }
  
  /**
   * Get an RPC proxy for each NN in an HA nameservice. Used when a given RPC
   * call should be made on every NN in an HA nameservice, not just the active.
   * 
   * @param conf configuration
   * @param nsId the nameservice to get all of the proxies for.
   * @return a list of RPC proxies for each NN in the nameservice.
   * @throws IOException in the event of error.
   */
  public static List<ClientProtocol> getProxiesForAllNameNodesInNameservice(
      Configuration conf, String nsId) throws IOException {
    List<ProxyAndInfo<ClientProtocol>> proxies =
        getProxiesForAllNameNodesInNameservice(conf, nsId, ClientProtocol.class);

    List<ClientProtocol> namenodes = new ArrayList<ClientProtocol>(
        proxies.size());
    for (ProxyAndInfo<ClientProtocol> proxy : proxies) {
      namenodes.add(proxy.getProxy());
    }
    return namenodes;
  }

  /**
   * Get an RPC proxy for each NN in an HA nameservice. Used when a given RPC
   * call should be made on every NN in an HA nameservice, not just the active.
   *
   * @param conf configuration
   * @param nsId the nameservice to get all of the proxies for.
   * @param xface the protocol class.
   * @return a list of RPC proxies for each NN in the nameservice.
   * @throws IOException in the event of error.
   */
  public static <T> List<ProxyAndInfo<T>> getProxiesForAllNameNodesInNameservice(
      Configuration conf, String nsId, Class<T> xface) throws IOException {
    Map<String, InetSocketAddress> nnAddresses =
        DFSUtil.getRpcAddressesForNameserviceId(conf, nsId, null);
    
    List<ProxyAndInfo<T>> proxies = new ArrayList<ProxyAndInfo<T>>(
        nnAddresses.size());
    for (InetSocketAddress nnAddress : nnAddresses.values()) {
      ProxyAndInfo<T> proxyInfo = NameNodeProxies.createNonHAProxy(conf,
          nnAddress, xface,
          UserGroupInformation.getCurrentUser(), false);
      proxies.add(proxyInfo);
    }
    return proxies;
  }
  
  /**
   * Used to ensure that at least one of the given HA NNs is currently in the
   * active state..
   * 
   * @param namenodes list of RPC proxies for each NN to check.
   * @return true if at least one NN is active, false if all are in the standby state.
   * @throws IOException in the event of error.
   */
  public static boolean isAtLeastOneActive(List<ClientProtocol> namenodes)
      throws IOException {
    for (ClientProtocol namenode : namenodes) {
      try {
        namenode.getFileInfo("/");
        return true;
      } catch (RemoteException re) {
        IOException cause = re.unwrapRemoteException();
        if (cause instanceof StandbyException) {
          // This is expected to happen for a standby NN.
        } else {
          throw re;
        }
      }
    }
    return false;
  }
}
