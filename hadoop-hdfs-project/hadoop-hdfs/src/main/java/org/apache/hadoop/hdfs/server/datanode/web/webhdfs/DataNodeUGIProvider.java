/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License. See accompanying LICENSE file.
 */
package org.apache.hadoop.hdfs.server.datanode.web.webhdfs;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.hdfs.server.common.JspHelper;
import org.apache.hadoop.ipc.Client;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;

import org.apache.hadoop.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.thirdparty.com.google.common.cache.Cache;
import org.apache.hadoop.thirdparty.com.google.common.cache.CacheBuilder;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * Create UGI from the request for the WebHDFS requests for the DNs. Note that
 * the DN does not authenticate the UGI -- the NN will authenticate them in
 * subsequent operations.
 */
public class DataNodeUGIProvider {
  private final ParameterParser params;
  @VisibleForTesting
  static Cache<String, UserGroupInformation> ugiCache;
  public static final Logger LOG = LoggerFactory.getLogger(Client.class);

  DataNodeUGIProvider(ParameterParser params) {
    this.params = params;
  }

  public static synchronized void init(Configuration conf) {
    if (ugiCache == null) {
      ugiCache = CacheBuilder
          .newBuilder()
          .expireAfterAccess(
              conf.getInt(
                  DFSConfigKeys.DFS_WEBHDFS_UGI_EXPIRE_AFTER_ACCESS_KEY,
                  DFSConfigKeys.DFS_WEBHDFS_UGI_EXPIRE_AFTER_ACCESS_DEFAULT),
              TimeUnit.MILLISECONDS).build();
    }
  }

  @VisibleForTesting
  void clearCache() throws IOException {
    if (UserGroupInformation.isSecurityEnabled()) {
      params.delegationToken().decodeIdentifier().clearCache();
    }
  }

  UserGroupInformation ugi() throws IOException {
    UserGroupInformation ugi;

    try {
      if (UserGroupInformation.isSecurityEnabled()) {
        final Token<DelegationTokenIdentifier> token = params.delegationToken();

        ugi = ugiCache.get(buildTokenCacheKey(token),
            new Callable<UserGroupInformation>() {
              @Override
              public UserGroupInformation call() throws Exception {
                return tokenUGI(token);
              }
            });
      } else {
        final String usernameFromQuery = params.userName();
        final String doAsUserFromQuery = params.doAsUser();
        final String remoteUser = usernameFromQuery == null ? JspHelper
            .getDefaultWebUserName(params.conf()) // not specified in request
            : usernameFromQuery;

        ugi = ugiCache.get(
            buildNonTokenCacheKey(doAsUserFromQuery, remoteUser),
            new Callable<UserGroupInformation>() {
              @Override
              public UserGroupInformation call() throws Exception {
                return nonTokenUGI(usernameFromQuery, doAsUserFromQuery,
                    remoteUser);
              }
            });
      }
    } catch (ExecutionException e) {
      Throwable cause = e.getCause();
      if (cause instanceof IOException) {
        throw (IOException) cause;
      } else {
        throw new IOException(cause);
      }
    }

    return ugi;
  }

  private String buildTokenCacheKey(Token<DelegationTokenIdentifier> token) {
    return token.buildCacheKey();
  }

  private UserGroupInformation tokenUGI(Token<DelegationTokenIdentifier> token)
      throws IOException {
    ByteArrayInputStream buf =
      new ByteArrayInputStream(token.getIdentifier());
    DataInputStream in = new DataInputStream(buf);
    DelegationTokenIdentifier id = new DelegationTokenIdentifier();
    id.readFields(in);
    UserGroupInformation ugi = id.getUser();
    ugi.addToken(token);
    return ugi;
  }

  private String buildNonTokenCacheKey(String doAsUserFromQuery,
      String remoteUser) throws IOException {
    String key = doAsUserFromQuery == null ? String.format("{%s}", remoteUser)
        : String.format("{%s}:{%s}", remoteUser, doAsUserFromQuery);
    return key;
  }

  private UserGroupInformation nonTokenUGI(String usernameFromQuery,
      String doAsUserFromQuery, String remoteUser) throws IOException {

    UserGroupInformation ugi = UserGroupInformation
        .createRemoteUser(remoteUser);
    JspHelper.checkUsername(ugi.getShortUserName(), usernameFromQuery);
    if (doAsUserFromQuery != null) {
      // create and attempt to authorize a proxy user
      ugi = UserGroupInformation.createProxyUser(doAsUserFromQuery, ugi);
    }
    return ugi;
  }
}
