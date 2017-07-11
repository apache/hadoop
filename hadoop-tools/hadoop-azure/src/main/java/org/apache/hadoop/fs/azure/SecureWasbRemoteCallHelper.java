/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.fs.azure;

import org.apache.commons.lang.Validate;
import org.apache.hadoop.fs.azure.security.Constants;
import org.apache.hadoop.fs.azure.security.WasbDelegationTokenIdentifier;
import org.apache.hadoop.io.retry.RetryPolicy;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authentication.client.AuthenticatedURL;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.apache.hadoop.security.authentication.client.Authenticator;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.security.token.delegation.web.KerberosDelegationTokenAuthenticator;
import org.apache.http.NameValuePair;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.client.utils.URIBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URISyntaxException;
import java.security.PrivilegedExceptionAction;
import java.util.List;

/**
 * Helper class the has constants and helper methods
 * used in WASB when integrating with a remote http cred
 * service which uses Kerberos and delegation tokens.
 * Currently, remote service will be used to generate
 * SAS keys, authorization and delegation token operations.
 */
public class SecureWasbRemoteCallHelper extends WasbRemoteCallHelper {

  public static final Logger LOG =
      LoggerFactory.getLogger(SecureWasbRemoteCallHelper.class);
  /**
   * Delegation token query parameter to be used when making rest call.
   */
  private static final String DELEGATION_TOKEN_QUERY_PARAM_NAME = "delegation";

  /**
   * Delegation token to be used for making the remote call.
   */
  private Token<?> delegationToken = null;

  /**
   * Does Remote Http Call requires Kerberos Authentication always, even if the delegation token is present.
   */
  private boolean alwaysRequiresKerberosAuth;

  public SecureWasbRemoteCallHelper(RetryPolicy retryPolicy,
      boolean alwaysRequiresKerberosAuth) {
    super(retryPolicy);
    this.alwaysRequiresKerberosAuth = alwaysRequiresKerberosAuth;
  }

  @Override
  public String makeRemoteRequest(final String[] urls,
      final String path, final List<NameValuePair> queryParams,
      final String httpMethod) throws IOException {
    final UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
    UserGroupInformation connectUgi = ugi.getRealUser();
    if (connectUgi == null) {
      connectUgi = ugi;
    }
    if (delegationToken == null) {
      connectUgi.checkTGTAndReloginFromKeytab();
    }
    String s = null;
    try {
      s = connectUgi.doAs(new PrivilegedExceptionAction<String>() {
        @Override public String run() throws Exception {
          return retryableRequest(urls, path, queryParams, httpMethod);
        }
      });
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IOException(e.getMessage(), e);
    }
    return s;
  }

  @Override
  public HttpUriRequest getHttpRequest(String[] urls, String path,
      List<NameValuePair> queryParams, int urlIndex, String httpMethod)
      throws URISyntaxException, IOException {
    final UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
    UserGroupInformation connectUgi = ugi.getRealUser();
    if (connectUgi != null) {
      queryParams.add(new NameValuePair() {
        @Override public String getName() {
          return Constants.DOAS_PARAM;
        }

        @Override public String getValue() {
          return ugi.getShortUserName();
        }
      });
    }

    final Token delegationToken = getDelegationToken(ugi);
    if (!alwaysRequiresKerberosAuth && delegationToken != null) {
      final String delegationTokenEncodedUrlString =
          delegationToken.encodeToUrlString();
      queryParams.add(new NameValuePair() {
        @Override public String getName() {
          return DELEGATION_TOKEN_QUERY_PARAM_NAME;
        }

        @Override public String getValue() {
          return delegationTokenEncodedUrlString;
        }
      });
    }

    URIBuilder uriBuilder =
        new URIBuilder(urls[urlIndex]).setPath(path).setParameters(queryParams);
    HttpUriRequest httpUriRequest = null;
    switch (httpMethod) {
    case HttpPut.METHOD_NAME:
      httpUriRequest = new HttpPut(uriBuilder.build());
      break;
    case HttpPost.METHOD_NAME:
      httpUriRequest = new HttpPost(uriBuilder.build());
      break;
    default:
      httpUriRequest = new HttpGet(uriBuilder.build());
      break;
    }

    LOG.debug("SecureWasbRemoteCallHelper#getHttpRequest() {}",
        uriBuilder.build().toURL());
    if (alwaysRequiresKerberosAuth || delegationToken == null) {
      AuthenticatedURL.Token token = new AuthenticatedURL.Token();
      final Authenticator kerberosAuthenticator =
          new KerberosDelegationTokenAuthenticator();
      try {
        kerberosAuthenticator.authenticate(uriBuilder.build().toURL(), token);
      } catch (AuthenticationException e) {
        throw new WasbRemoteCallException(
            Constants.AUTHENTICATION_FAILED_ERROR_MESSAGE, e);
      }
      Validate.isTrue(token.isSet(),
          "Authenticated Token is NOT present. The request cannot proceed.");

      httpUriRequest.setHeader("Cookie",
          AuthenticatedURL.AUTH_COOKIE + "=" + token);
    }
    return httpUriRequest;
  }

  private synchronized Token<?> getDelegationToken(
      UserGroupInformation userGroupInformation) throws IOException {
    if (this.delegationToken == null) {
      Token<?> token = null;
      for (Token iterToken : userGroupInformation.getTokens()) {
        if (iterToken.getKind()
            .equals(WasbDelegationTokenIdentifier.TOKEN_KIND)) {
          token = iterToken;
          LOG.debug("{} token found in cache : {}",
              WasbDelegationTokenIdentifier.TOKEN_KIND, iterToken);
          break;
        }
      }
      LOG.debug("UGI Information: {}", userGroupInformation.toString());

      // ugi tokens are usually indicative of a task which can't
      // refetch tokens.  even if ugi has credentials, don't attempt
      // to get another token to match hdfs/rpc behavior
      if (token != null) {
        LOG.debug("Using UGI token: {}", token);
        setDelegationToken(token);
      }
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("Delegation token from cache - {}", delegationToken != null
          ? delegationToken.encodeToUrlString()
          : "null");
    }
    return this.delegationToken;
  }

  private <T extends TokenIdentifier> void setDelegationToken(
      final Token<T> token) {
    synchronized (this) {
      this.delegationToken = token;
    }
  }
}
