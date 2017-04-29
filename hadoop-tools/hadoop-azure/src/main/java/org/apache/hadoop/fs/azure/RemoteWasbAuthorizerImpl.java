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

package org.apache.hadoop.fs.azure;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.Validate;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.azure.security.Constants;
import org.apache.hadoop.fs.azure.security.SecurityUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authentication.client.AuthenticatedURL;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.apache.hadoop.security.authentication.client.Authenticator;
import org.apache.hadoop.security.token.delegation.web.KerberosDelegationTokenAuthenticator;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.utils.URIBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URISyntaxException;
import java.security.PrivilegedExceptionAction;

import static org.apache.hadoop.fs.azure.WasbRemoteCallHelper.REMOTE_CALL_SUCCESS_CODE;

/**
 * Class implementing WasbAuthorizerInterface using a remote
 * service that implements the authorization operation. This
 * class expects the url of the remote service to be passed
 * via config.
 */
public class RemoteWasbAuthorizerImpl implements WasbAuthorizerInterface {

  public static final Logger LOG = LoggerFactory
      .getLogger(RemoteWasbAuthorizerImpl.class);

  private String remoteAuthorizerServiceUrl = null;

  /**
   * Configuration parameter name expected in the Configuration object to
   * provide the url of the remote service. {@value}
   */
  public static final String KEY_REMOTE_AUTH_SERVICE_URL =
      "fs.azure.authorization.remote.service.url";

  /**
   * Authorization operation OP name in the remote service {@value}
   */
  private static final String CHECK_AUTHORIZATION_OP =
      "CHECK_AUTHORIZATION";

  /**
   * Query parameter specifying the access operation type. {@value}
   */
  private static final String ACCESS_OPERATION_QUERY_PARAM_NAME =
      "operation_type";

  /**
   * Query parameter specifying the wasb absolute path. {@value}
   */
  private static final String WASB_ABSOLUTE_PATH_QUERY_PARAM_NAME =
      "wasb_absolute_path";

  /**
   * Query parameter name for user info {@value}
   */
  private static final String DELEGATION_TOKEN_QUERY_PARAM_NAME =
      "delegation";

  private WasbRemoteCallHelper remoteCallHelper = null;
  private String delegationToken;
  private boolean isSecurityEnabled;
  private boolean isKerberosSupportEnabled;

  @VisibleForTesting
  public void updateWasbRemoteCallHelper(WasbRemoteCallHelper helper) {
    this.remoteCallHelper = helper;
  }

  @Override
  public void init(Configuration conf)
      throws WasbAuthorizationException, IOException {
    LOG.debug("Initializing RemoteWasbAuthorizerImpl instance");
    setDelegationToken();
    remoteAuthorizerServiceUrl = SecurityUtils
        .getRemoteAuthServiceUrls(conf);

    if (remoteAuthorizerServiceUrl == null
          || remoteAuthorizerServiceUrl.isEmpty()) {
      throw new WasbAuthorizationException(
          "fs.azure.authorization.remote.service.url config not set"
              + " in configuration.");
    }

    this.remoteCallHelper = new WasbRemoteCallHelper();
    this.isSecurityEnabled = UserGroupInformation.isSecurityEnabled();
    this.isKerberosSupportEnabled = conf
        .getBoolean(Constants.AZURE_KERBEROS_SUPPORT_PROPERTY_NAME, false);
  }

  @Override
  public boolean authorize(String wasbAbsolutePath, String accessType)
      throws WasbAuthorizationException, IOException {

      try {

        /* Make an exception for the internal -RenamePending files */
        if (wasbAbsolutePath.endsWith(NativeAzureFileSystem.FolderRenamePending.SUFFIX)) {
          return true;
        }

        setDelegationToken();
        URIBuilder uriBuilder = new URIBuilder(remoteAuthorizerServiceUrl);
        uriBuilder.setPath("/" + CHECK_AUTHORIZATION_OP);
        uriBuilder.addParameter(WASB_ABSOLUTE_PATH_QUERY_PARAM_NAME,
            wasbAbsolutePath);
        uriBuilder.addParameter(ACCESS_OPERATION_QUERY_PARAM_NAME,
            accessType);
        if (isSecurityEnabled && StringUtils.isNotEmpty(delegationToken)) {
          uriBuilder.addParameter(DELEGATION_TOKEN_QUERY_PARAM_NAME,
              delegationToken);
        }

        String responseBody = null;
        UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
        UserGroupInformation connectUgi = ugi.getRealUser();
        if (connectUgi == null) {
          connectUgi = ugi;
        } else {
          uriBuilder.addParameter(Constants.DOAS_PARAM, ugi.getShortUserName());
        }

        try {
          responseBody = connectUgi
              .doAs(new PrivilegedExceptionAction<String>() {
                @Override
                public String run() throws Exception {
                  AuthenticatedURL.Token token = null;
                  HttpGet httpGet = new HttpGet(uriBuilder.build());
                  if (isKerberosSupportEnabled && UserGroupInformation
                      .isSecurityEnabled() && (delegationToken == null
                      || delegationToken.isEmpty())) {
                    token = new AuthenticatedURL.Token();
                    final Authenticator kerberosAuthenticator = new KerberosDelegationTokenAuthenticator();
                    try {
                      kerberosAuthenticator
                          .authenticate(uriBuilder.build().toURL(), token);
                      Validate.isTrue(token.isSet(),
                          "Authenticated Token is NOT present. The request cannot proceed.");
                    } catch (AuthenticationException e){
                      throw new IOException("Authentication failed in check authorization", e);
                    }
                    if (token != null) {
                      httpGet.setHeader("Cookie",
                          AuthenticatedURL.AUTH_COOKIE + "=" + token);
                    }
                  }
                  return remoteCallHelper.makeRemoteGetRequest(httpGet);
                }
              });
        } catch (InterruptedException e) {
          LOG.error("Error in check authorization", e);
          throw new WasbAuthorizationException("Error in check authorize", e);
        }

        ObjectMapper objectMapper = new ObjectMapper();
        RemoteAuthorizerResponse authorizerResponse =
            objectMapper
            .readValue(responseBody, RemoteAuthorizerResponse.class);

        if (authorizerResponse == null) {
          throw new WasbAuthorizationException(
              "RemoteAuthorizerResponse object null from remote call");
        } else if (authorizerResponse.getResponseCode()
            == REMOTE_CALL_SUCCESS_CODE) {
          return authorizerResponse.getAuthorizationResult();
        } else {
          throw new WasbAuthorizationException("Remote authorization"
              + " service encountered an error "
              + authorizerResponse.getResponseMessage());
        }
      } catch (URISyntaxException | WasbRemoteCallException
          | JsonParseException | JsonMappingException ex) {
        throw new WasbAuthorizationException(ex);
      }
  }

  private void setDelegationToken() throws IOException {
    this.delegationToken = SecurityUtils.getDelegationTokenFromCredentials();
  }
}

/**
 * POJO representing the response expected from a remote
 * authorization service.
 * The remote service is expected to return the authorization
 * response in the following JSON format
 * {
 *    "responseCode" : 0 or non-zero <int>,
 *    "responseMessage" : relevant message of failure <String>
 *    "authorizationResult" : authorization result <boolean>
 *                            true - if auhorization allowed
 *                            false - otherwise.
 *
 * }
 */
class RemoteAuthorizerResponse {

  private int responseCode;
  private boolean authorizationResult;
  private String responseMessage;

  public RemoteAuthorizerResponse(int responseCode,
      boolean authorizationResult, String message) {
    this.responseCode = responseCode;
    this.authorizationResult = authorizationResult;
    this.responseMessage = message;
  }

  public RemoteAuthorizerResponse() {
  }

  public int getResponseCode() {
    return responseCode;
  }

  public void setResponseCode(int responseCode) {
    this.responseCode = responseCode;
  }

  public boolean getAuthorizationResult() {
    return authorizationResult;
  }

  public void setAuthorizationResult(boolean authorizationResult) {
    this.authorizationResult = authorizationResult;
  }

  public String getResponseMessage() {
    return responseMessage;
  }

  public void setResponseMessage(String message) {
    this.responseMessage = message;
  }
}