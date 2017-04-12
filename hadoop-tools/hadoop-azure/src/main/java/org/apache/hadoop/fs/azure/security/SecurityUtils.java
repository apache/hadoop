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

package org.apache.hadoop.fs.azure.security;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.azure.RemoteWasbAuthorizerImpl;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Iterator;

/**
 * Security Utils class for WASB.
 */
public final class SecurityUtils {

  private SecurityUtils() {
  }

  /**
   * Utility method to get remote service URLs from the configuration.
   * @param conf configuration object.
   * @return remote service URL
   * @throws UnknownHostException thrown when getting the default value.
   */
  public static String getCredServiceUrls(Configuration conf)
      throws UnknownHostException {
    return conf.get(Constants.KEY_CRED_SERVICE_URL, String
        .format("http://%s:%s",
            InetAddress.getLocalHost().getCanonicalHostName(),
            Constants.DEFAULT_CRED_SERVICE_PORT));
  }

  /**
   * Utility method to get remote Authorization service URLs from the configuration.
   * @param conf Configuration object.
   * @return remote Authorization server URL
   * @throws UnknownHostException thrown when getting the default value.
   */
  public static String getRemoteAuthServiceUrls(Configuration conf)
      throws UnknownHostException {
    return conf.get(RemoteWasbAuthorizerImpl.KEY_REMOTE_AUTH_SERVICE_URL, String
        .format("http://%s:%s",
            InetAddress.getLocalHost().getCanonicalHostName(),
            Constants.DEFAULT_CRED_SERVICE_PORT));
  }

  /**
   * Utility method to get delegation token from the UGI credentials.
   * @return delegation token
   * @throws IOException thrown when getting the current user.
   */
  public static String getDelegationTokenFromCredentials() throws IOException {
    String delegationToken = null;
    Iterator<Token<? extends TokenIdentifier>> tokenIterator = UserGroupInformation
        .getCurrentUser().getCredentials().getAllTokens().iterator();
    while (tokenIterator.hasNext()) {
      Token<? extends TokenIdentifier> iteratedToken = tokenIterator.next();
      if (iteratedToken.getKind()
          .equals(WasbDelegationTokenIdentifier.TOKEN_KIND)) {
        delegationToken = iteratedToken.encodeToUrlString();
      }
    }
    return delegationToken;
  }
}
