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
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenRenewer;
import org.apache.hadoop.security.token.delegation.AbstractDelegationTokenIdentifier;
import org.apache.hadoop.security.token.delegation.web.DelegationTokenAuthenticatedURL;
import org.apache.hadoop.security.token.delegation.web.DelegationTokenAuthenticator;
import org.apache.hadoop.security.token.delegation.web.KerberosDelegationTokenAuthenticator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetAddress;
import java.net.URL;
import java.security.PrivilegedExceptionAction;

/**
 * Token Renewer for renewing WASB delegation tokens with remote service.
 */
public class WasbTokenRenewer extends TokenRenewer {
  public static final Logger LOG = LoggerFactory
      .getLogger(WasbTokenRenewer.class);

  /**
   * Checks if this particular object handles the Kind of token passed.
   * @param kind the kind of the token
   * @return true if it handles passed token kind false otherwise.
   */
  @Override
  public boolean handleKind(Text kind) {
    return WasbDelegationTokenIdentifier.TOKEN_KIND.equals(kind);
  }

  /**
   * Checks if passed token is managed.
   * @param token the token being checked
   * @return true if it is managed.
   * @throws IOException thrown when evaluating if token is managed.
   */
  @Override
  public boolean isManaged(Token<?> token) throws IOException {
    return true;
  }

  /**
   * Renew the delegation token.
   * @param token token to renew.
   * @param conf configuration object.
   * @return extended expiry time of the token.
   * @throws IOException thrown when trying get current user.
   * @throws InterruptedException thrown when thread is interrupted
   */
  @Override
  public long renew(final Token<?> token, Configuration conf)
      throws IOException, InterruptedException {
    LOG.debug("Renewing the delegation token");
    final UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
    UserGroupInformation connectUgi = ugi.getRealUser();
    final UserGroupInformation proxyUser = connectUgi;
    if (connectUgi == null) {
      connectUgi = ugi;
    }
    connectUgi.checkTGTAndReloginFromKeytab();
    final DelegationTokenAuthenticatedURL.Token authToken = new DelegationTokenAuthenticatedURL.Token();
    authToken
        .setDelegationToken((Token<AbstractDelegationTokenIdentifier>) token);
    final String credServiceUrl = conf.get(Constants.KEY_CRED_SERVICE_URL,
        String.format("http://%s:%s",
            InetAddress.getLocalHost().getCanonicalHostName(),
            Constants.DEFAULT_CRED_SERVICE_PORT));
    DelegationTokenAuthenticator authenticator = new KerberosDelegationTokenAuthenticator();
    final DelegationTokenAuthenticatedURL authURL = new DelegationTokenAuthenticatedURL(
        authenticator);

    return connectUgi.doAs(new PrivilegedExceptionAction<Long>() {
      @Override
      public Long run() throws Exception {
        return authURL.renewDelegationToken(new URL(credServiceUrl
                + Constants.DEFAULT_DELEGATION_TOKEN_MANAGER_ENDPOINT),
            authToken, (proxyUser != null) ? ugi.getShortUserName() : null);
      }
    });
  }

  /**
   * Cancel the delegation token.
   * @param token token to cancel.
   * @param conf configuration object.
   * @throws IOException thrown when trying get current user.
   * @throws InterruptedException thrown when thread is interrupted.
   */
  @Override
  public void cancel(final Token<?> token, Configuration conf)
      throws IOException, InterruptedException {
    LOG.debug("Cancelling the delegation token");
    final UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
    UserGroupInformation connectUgi = ugi.getRealUser();
    final UserGroupInformation proxyUser = connectUgi;
    if (connectUgi == null) {
      connectUgi = ugi;
    }
    connectUgi.checkTGTAndReloginFromKeytab();
    final DelegationTokenAuthenticatedURL.Token authToken = new DelegationTokenAuthenticatedURL.Token();
    authToken
        .setDelegationToken((Token<AbstractDelegationTokenIdentifier>) token);
    final String credServiceUrl = conf.get(Constants.KEY_CRED_SERVICE_URL,
        String.format("http://%s:%s",
            InetAddress.getLocalHost().getCanonicalHostName(),
            Constants.DEFAULT_CRED_SERVICE_PORT));
    DelegationTokenAuthenticator authenticator = new KerberosDelegationTokenAuthenticator();
    final DelegationTokenAuthenticatedURL authURL = new DelegationTokenAuthenticatedURL(
        authenticator);
    connectUgi.doAs(new PrivilegedExceptionAction<Void>() {
      @Override
      public Void run() throws Exception {
        authURL.cancelDelegationToken(new URL(credServiceUrl
                + Constants.DEFAULT_DELEGATION_TOKEN_MANAGER_ENDPOINT),
            authToken, (proxyUser != null) ? ugi.getShortUserName() : null);
        return null;
      }
    });
  }
}