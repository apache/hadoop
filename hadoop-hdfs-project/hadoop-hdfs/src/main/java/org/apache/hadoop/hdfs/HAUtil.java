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
import java.lang.reflect.Constructor;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.Map;

import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSClient.Conf;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.protocol.NamenodeProtocols;
import org.apache.hadoop.io.retry.FailoverProxyProvider;
import org.apache.hadoop.io.retry.RetryPolicies;
import org.apache.hadoop.io.retry.RetryProxy;
import org.apache.hadoop.util.ReflectionUtils;

import com.google.common.base.Preconditions;

public class HAUtil {
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
          " must be suffixed with" + namenodeId + " for HA configuration.";
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
   * This is used only by tests at the moment.
   * @return true if the NN should allow read operations while in standby mode.
   */
  public static boolean shouldAllowStandbyReads(Configuration conf) {
    return conf.getBoolean("dfs.ha.allow.stale.reads", false);
  }
  
  public static void setAllowStandbyReads(Configuration conf, boolean val) {
    conf.setBoolean("dfs.ha.allow.stale.reads", val);
  }
 
  /** Creates the Failover proxy provider instance*/
  @SuppressWarnings("unchecked")
  public static <T> FailoverProxyProvider<T> createFailoverProxyProvider(
      Configuration conf, Class<FailoverProxyProvider<?>> failoverProxyProviderClass,
      Class xface) throws IOException {
    Preconditions.checkArgument(
        xface.isAssignableFrom(NamenodeProtocols.class),
        "Interface %s is not a NameNode protocol", xface);
    try {
      Constructor<FailoverProxyProvider<?>> ctor = failoverProxyProviderClass
          .getConstructor(Class.class);
      FailoverProxyProvider<?> provider = ctor.newInstance(xface);
      ReflectionUtils.setConf(provider, conf);
      return (FailoverProxyProvider<T>) provider;
    } catch (Exception e) {
      if (e.getCause() instanceof IOException) {
        throw (IOException) e.getCause();
      } else {
        throw new IOException(
            "Couldn't create proxy provider " + failoverProxyProviderClass, e);
      }
    }
  }

  /** Gets the configured Failover proxy provider's class */
  public static <T> Class<FailoverProxyProvider<T>> getFailoverProxyProviderClass(
      Configuration conf, URI nameNodeUri, Class<T> xface) throws IOException {
    if (nameNodeUri == null) {
      return null;
    }
    String host = nameNodeUri.getHost();

    String configKey = DFS_CLIENT_FAILOVER_PROXY_PROVIDER_KEY_PREFIX + "."
        + host;
    try {
      @SuppressWarnings("unchecked")
      Class<FailoverProxyProvider<T>> ret = (Class<FailoverProxyProvider<T>>) conf
          .getClass(configKey, null, FailoverProxyProvider.class);
      if (ret != null) {
        // If we found a proxy provider, then this URI should be a logical NN.
        // Given that, it shouldn't have a non-default port number.
        int port = nameNodeUri.getPort();
        if (port > 0 && port != NameNode.DEFAULT_PORT) {
          throw new IOException("Port " + port + " specified in URI "
              + nameNodeUri + " but host '" + host
              + "' is a logical (HA) namenode"
              + " and does not use port information.");
        }
      }
      return ret;
    } catch (RuntimeException e) {
      if (e.getCause() instanceof ClassNotFoundException) {
        throw new IOException("Could not load failover proxy provider class "
            + conf.get(configKey) + " which is configured for authority "
            + nameNodeUri, e);
      } else {
        throw e;
      }
    }
  }

  /** Creates the namenode proxy with the passed Protocol */
  @SuppressWarnings("unchecked")
  public static Object createFailoverProxy(Configuration conf, URI nameNodeUri,
      Class xface) throws IOException {
    Class<FailoverProxyProvider<?>> failoverProxyProviderClass = HAUtil
        .getFailoverProxyProviderClass(conf, nameNodeUri, xface);
    if (failoverProxyProviderClass != null) {
      FailoverProxyProvider<?> failoverProxyProvider = HAUtil
          .createFailoverProxyProvider(conf, failoverProxyProviderClass, xface);
      Conf config = new Conf(conf);
      return RetryProxy.create(xface, failoverProxyProvider, RetryPolicies
          .failoverOnNetworkException(RetryPolicies.TRY_ONCE_THEN_FAIL,
              config.maxFailoverAttempts, config.failoverSleepBaseMillis,
              config.failoverSleepMaxMillis));
    }
    return null;
  }
  
}
