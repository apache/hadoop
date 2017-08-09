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
import org.apache.hadoop.fs.azure.SecureWasbRemoteCallHelper;
import org.apache.hadoop.fs.azure.WasbRemoteCallHelper;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.retry.RetryPolicy;
import org.apache.hadoop.io.retry.RetryUtils;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.delegation.web.DelegationTokenIdentifier;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.utils.URIBuilder;

import java.io.IOException;
import java.util.Map;

/**
 * Class to manage delegation token operations by making rest call to remote service.
 */
public class RemoteWasbDelegationTokenManager
    implements WasbDelegationTokenManager {

  /**
   * Configuration parameter name expected in the configuration
   * object to provide the url of the delegation token service to fetch the delegation tokens.
   */
  public static final String KEY_DELEGATION_TOKEN_SERVICE_URLS =
      "fs.azure.delegation.token.service.urls";
  /**
   * Configuration key to enable http retry policy for delegation token service calls.
   */
  public static final String DT_MANAGER_HTTP_CLIENT_RETRY_POLICY_ENABLED_KEY =
      "fs.azure.delegationtokenservice.http.retry.policy.enabled";
  /**
   * Configuration key for delegation token service http retry policy spec.
   */
  public static final String DT_MANAGER_HTTP_CLIENT_RETRY_POLICY_SPEC_KEY =
      "fs.azure.delegationtokenservice.http.retry.policy.spec";
  /**
   * Default remote delegation token manager endpoint.
   */
  private static final String DEFAULT_DELEGATION_TOKEN_MANAGER_ENDPOINT =
      "/tokenmanager/v1";
  /**
   * Default for delegation token service http retry policy spec.
   */
  private static final String DT_MANAGER_HTTP_CLIENT_RETRY_POLICY_SPEC_DEFAULT =
      "10,3,100,2";

  private static final boolean
      DT_MANAGER_HTTP_CLIENT_RETRY_POLICY_ENABLED_DEFAULT = true;

  private static final Text WASB_DT_SERVICE_NAME = new Text("WASB_DT_SERVICE");
  /**
   * Query parameter value for Getting delegation token http request
   */
  private static final String GET_DELEGATION_TOKEN_OP = "GETDELEGATIONTOKEN";
  /**
   * Query parameter value for renewing delegation token http request
   */
  private static final String RENEW_DELEGATION_TOKEN_OP =
      "RENEWDELEGATIONTOKEN";
  /**
   * Query parameter value for canceling the delegation token http request
   */
  private static final String CANCEL_DELEGATION_TOKEN_OP =
      "CANCELDELEGATIONTOKEN";
  /**
   * op parameter to represent the operation.
   */
  private static final String OP_PARAM_KEY_NAME = "op";
  /**
   * renewer parameter to represent the renewer of the delegation token.
   */
  private static final String RENEWER_PARAM_KEY_NAME = "renewer";
  /**
   * service parameter to represent the service which returns delegation tokens.
   */
  private static final String SERVICE_PARAM_KEY_NAME = "service";
  /**
   * token parameter to represent the delegation token.
   */
  private static final String TOKEN_PARAM_KEY_NAME = "token";
  private WasbRemoteCallHelper remoteCallHelper;
  private String[] dtServiceUrls;
  private boolean isSpnegoTokenCacheEnabled;

  public RemoteWasbDelegationTokenManager(Configuration conf)
      throws IOException {
    RetryPolicy retryPolicy = RetryUtils.getMultipleLinearRandomRetry(conf,
        DT_MANAGER_HTTP_CLIENT_RETRY_POLICY_ENABLED_KEY,
        DT_MANAGER_HTTP_CLIENT_RETRY_POLICY_ENABLED_DEFAULT,
        DT_MANAGER_HTTP_CLIENT_RETRY_POLICY_SPEC_KEY,
        DT_MANAGER_HTTP_CLIENT_RETRY_POLICY_SPEC_DEFAULT);
    this.isSpnegoTokenCacheEnabled =
        conf.getBoolean(Constants.AZURE_ENABLE_SPNEGO_TOKEN_CACHE, true);

    remoteCallHelper = new SecureWasbRemoteCallHelper(retryPolicy, true,
        isSpnegoTokenCacheEnabled);
    this.dtServiceUrls =
        conf.getTrimmedStrings(KEY_DELEGATION_TOKEN_SERVICE_URLS);
    if (this.dtServiceUrls == null || this.dtServiceUrls.length <= 0) {
      throw new IOException(
          KEY_DELEGATION_TOKEN_SERVICE_URLS + " config not set"
              + " in configuration.");
    }
  }

  @Override
  public Token<DelegationTokenIdentifier> getDelegationToken(
      String renewer) throws IOException {
    URIBuilder uriBuilder =
        new URIBuilder().setPath(DEFAULT_DELEGATION_TOKEN_MANAGER_ENDPOINT)
            .addParameter(OP_PARAM_KEY_NAME, GET_DELEGATION_TOKEN_OP)
            .addParameter(RENEWER_PARAM_KEY_NAME, renewer)
            .addParameter(SERVICE_PARAM_KEY_NAME,
                WASB_DT_SERVICE_NAME.toString());
    String responseBody = remoteCallHelper
        .makeRemoteRequest(dtServiceUrls, uriBuilder.getPath(),
            uriBuilder.getQueryParams(), HttpGet.METHOD_NAME);
    return TokenUtils.toDelegationToken(JsonUtils.parse(responseBody));
  }

  @Override
  public long renewDelegationToken(Token<?> token)
      throws IOException {
    URIBuilder uriBuilder =
        new URIBuilder().setPath(DEFAULT_DELEGATION_TOKEN_MANAGER_ENDPOINT)
            .addParameter(OP_PARAM_KEY_NAME, RENEW_DELEGATION_TOKEN_OP)
            .addParameter(TOKEN_PARAM_KEY_NAME, token.encodeToUrlString());

    String responseBody = remoteCallHelper
        .makeRemoteRequest(dtServiceUrls, uriBuilder.getPath(),
            uriBuilder.getQueryParams(), HttpPut.METHOD_NAME);

    Map<?, ?> parsedResp = JsonUtils.parse(responseBody);
    return ((Number) parsedResp.get("long")).longValue();
  }

  @Override
  public void cancelDelegationToken(Token<?> token)
      throws IOException {
    URIBuilder uriBuilder =
        new URIBuilder().setPath(DEFAULT_DELEGATION_TOKEN_MANAGER_ENDPOINT)
            .addParameter(OP_PARAM_KEY_NAME, CANCEL_DELEGATION_TOKEN_OP)
            .addParameter(TOKEN_PARAM_KEY_NAME, token.encodeToUrlString());
    remoteCallHelper.makeRemoteRequest(dtServiceUrls, uriBuilder.getPath(),
        uriBuilder.getQueryParams(), HttpPut.METHOD_NAME);
  }
}
