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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenSelector;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.net.URI;
import java.util.Collection;

import static org.apache.hadoop.hdfs.protocol.HdfsConstants.HA_DT_SERVICE_PREFIX;
import static org.apache.hadoop.security.SecurityUtil.buildTokenService;

@InterfaceAudience.Private
public class HAUtilClient {
  private static final Logger LOG = LoggerFactory.getLogger(HAUtilClient.class);

  private static final DelegationTokenSelector tokenSelector =
      new DelegationTokenSelector();

  /**
   * @return true if the given nameNodeUri appears to be a logical URI.
   */
  public static boolean isLogicalUri(
      Configuration conf, URI nameNodeUri) {
    String host = nameNodeUri.getHost();
    // A logical name must be one of the service IDs.
    return DFSUtilClient.getNameServiceIds(conf).contains(host);
  }

  /**
   * Check whether the client has a failover proxy provider configured
   * for the namenode/nameservice.
   *
   * @param conf Configuration
   * @param nameNodeUri The URI of namenode
   * @return true if failover is configured.
   */
  public static boolean isClientFailoverConfigured(
      Configuration conf, URI nameNodeUri) {
    String host = nameNodeUri.getHost();
    String configKey = HdfsClientConfigKeys.Failover.PROXY_PROVIDER_KEY_PREFIX
        + "." + host;
    return conf.get(configKey) != null;
  }

  /**
   * Get the service name used in the delegation token for the given logical
   * HA service.
   * @param uri the logical URI of the cluster
   * @param scheme the scheme of the corresponding FileSystem
   * @return the service name
   */
  public static Text buildTokenServiceForLogicalUri(final URI uri,
      final String scheme) {
    return new Text(buildTokenServicePrefixForLogicalUri(scheme)
        + uri.getHost());
  }

  public static String buildTokenServicePrefixForLogicalUri(String scheme) {
    return HA_DT_SERVICE_PREFIX + scheme + ":";
  }

  /**
   * Parse the file system URI out of the provided token.
   */
  public static URI getServiceUriFromToken(final String scheme, Token<?> token) {
    String tokStr = token.getService().toString();
    final String prefix = buildTokenServicePrefixForLogicalUri(
        scheme);
    if (tokStr.startsWith(prefix)) {
      tokStr = tokStr.replaceFirst(prefix, "");
    }
    return URI.create(scheme + "://" + tokStr);
  }

  /**
   * @return true if this token corresponds to a logical nameservice
   * rather than a specific namenode.
   */
  public static boolean isTokenForLogicalUri(Token<?> token) {
    return token.getService().toString().startsWith(HA_DT_SERVICE_PREFIX);
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
}
