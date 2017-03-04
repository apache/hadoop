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

import java.io.IOException;
import java.net.InetAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.security.PrivilegedExceptionAction;
import java.util.Iterator;

import org.apache.commons.lang.Validate;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.azure.security.Constants;
import org.apache.hadoop.fs.azure.security.WasbDelegationTokenIdentifier;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authentication.client.AuthenticatedURL;
import org.apache.hadoop.security.authentication.client.Authenticator;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.security.token.delegation.web.KerberosDelegationTokenAuthenticator;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.utils.URIBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import static org.apache.hadoop.fs.azure.WasbRemoteCallHelper.REMOTE_CALL_SUCCESS_CODE;

/**
 * Class implementing a RemoteSASKeyGenerator. This class
 * uses the url passed in via the Configuration to make a
 * rest call to generate the required SAS Key.
 */
public class RemoteSASKeyGeneratorImpl extends SASKeyGeneratorImpl {

  public static final Logger LOG =
      LoggerFactory.getLogger(AzureNativeFileSystemStore.class);

  /**
   * Container SAS Key generation OP name. {@value}
   */
  private static final String CONTAINER_SAS_OP = "GET_CONTAINER_SAS";

  /**
   * Relative Blob SAS Key generation OP name. {@value}
   */
  private static final String BLOB_SAS_OP = "GET_RELATIVE_BLOB_SAS";

  /**
   * Query parameter specifying the expiry period to be used for sas key
   * {@value}
   */
  private static final String SAS_EXPIRY_QUERY_PARAM_NAME = "sas_expiry";

  /**
   * Query parameter name for the storage account. {@value}
   */
  private static final String STORAGE_ACCOUNT_QUERY_PARAM_NAME =
      "storage_account";

  /**
   * Query parameter name for the storage account container. {@value}
   */
  private static final String CONTAINER_QUERY_PARAM_NAME =
      "container";

  /**
   * Query parameter name for user info {@value}
   */
  private static final String DELEGATION_TOKEN_QUERY_PARAM_NAME =
      "delegation";

  /**
   * Query parameter name for the relative path inside the storage
   * account container. {@value}
   */
  private static final String RELATIVE_PATH_QUERY_PARAM_NAME =
      "relative_path";

  private String delegationToken = "";
  private String credServiceUrl = "";
  private WasbRemoteCallHelper remoteCallHelper = null;
  private boolean isSecurityEnabled;
  private boolean isKerberosSupportEnabled;

  public RemoteSASKeyGeneratorImpl(Configuration conf) {
    super(conf);
  }

  public boolean initialize(Configuration conf) {

    LOG.debug("Initializing RemoteSASKeyGeneratorImpl instance");
    Iterator<Token<? extends TokenIdentifier>> tokenIterator = null;
    try {
      tokenIterator = UserGroupInformation.getCurrentUser().getCredentials()
          .getAllTokens().iterator();
      while (tokenIterator.hasNext()) {
        Token<? extends TokenIdentifier> iteratedToken = tokenIterator.next();
        if (iteratedToken.getKind().equals(WasbDelegationTokenIdentifier.TOKEN_KIND)) {
          delegationToken = iteratedToken.encodeToUrlString();
        }
      }
    } catch (IOException e) {
      LOG.error("Error in fetching the WASB delegation token");
    }

    try {
      credServiceUrl = conf.get(Constants.KEY_CRED_SERVICE_URL, String
          .format("http://%s:%s",
              InetAddress.getLocalHost().getCanonicalHostName(),
              Constants.DEFAULT_CRED_SERVICE_PORT));
    } catch (UnknownHostException e) {
      LOG.error("Invalid CredService Url, configure it correctly.");
      return false;
    }

    if (credServiceUrl == null || credServiceUrl.isEmpty()) {
      LOG.error("CredService Url not found in configuration to initialize"
          + " RemoteSASKeyGenerator");
      return false;
    }

    remoteCallHelper = new WasbRemoteCallHelper();
    this.isSecurityEnabled = UserGroupInformation.isSecurityEnabled();
    this.isKerberosSupportEnabled = conf.getBoolean(
        Constants.AZURE_KERBEROS_SUPPORT_PROPERTY_NAME, false);
    LOG.debug("Initialization of RemoteSASKeyGenerator instance successful");
    return true;
  }

  @Override
  public URI getContainerSASUri(String storageAccount, String container)
      throws SASKeyGenerationException {
    try {
      LOG.debug("Generating Container SAS Key for Container {} "
          + "inside Storage Account {} ", container, storageAccount);
      URIBuilder uriBuilder = new URIBuilder(credServiceUrl);
      uriBuilder.setPath("/" + CONTAINER_SAS_OP);
      uriBuilder.addParameter(STORAGE_ACCOUNT_QUERY_PARAM_NAME,
          storageAccount);
      uriBuilder.addParameter(CONTAINER_QUERY_PARAM_NAME,
          container);
      uriBuilder.addParameter(SAS_EXPIRY_QUERY_PARAM_NAME, ""
          + getSasKeyExpiryPeriod());
      if (isSecurityEnabled && (delegationToken != null && !delegationToken
          .isEmpty())) {
        uriBuilder.addParameter(DELEGATION_TOKEN_QUERY_PARAM_NAME,
            this.delegationToken);
      }

      UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
      UserGroupInformation connectUgi = ugi.getRealUser();
      if (connectUgi == null) {
        connectUgi = ugi;
      } else {
        uriBuilder.addParameter(Constants.DOAS_PARAM, ugi.getShortUserName());
      }

      if(isSecurityEnabled && !connectUgi.hasKerberosCredentials()){
        connectUgi = UserGroupInformation.getLoginUser();
      }
      return getSASKey(uriBuilder.build(), connectUgi);
    } catch (URISyntaxException uriSyntaxEx) {
      throw new SASKeyGenerationException("Encountered URISyntaxException "
          + "while building the HttpGetRequest to remote cred service",
          uriSyntaxEx);
    } catch (IOException e) {
      throw new SASKeyGenerationException("Encountered IOException"
          + " while building the HttpGetRequest to remote service", e);
    }
  }

  @Override
  public URI getRelativeBlobSASUri(String storageAccount, String container,
      String relativePath) throws SASKeyGenerationException {
    try {
      LOG.debug("Generating RelativePath SAS Key for relativePath {} inside"
          + " Container {} inside Storage Account {} ",
          relativePath, container, storageAccount);
      URIBuilder uriBuilder = new URIBuilder(credServiceUrl);
      uriBuilder.setPath("/" + BLOB_SAS_OP);
      uriBuilder.addParameter(STORAGE_ACCOUNT_QUERY_PARAM_NAME, storageAccount);
      uriBuilder.addParameter(CONTAINER_QUERY_PARAM_NAME, container);
      uriBuilder.addParameter(RELATIVE_PATH_QUERY_PARAM_NAME,
          relativePath);
      uriBuilder.addParameter(SAS_EXPIRY_QUERY_PARAM_NAME, ""
          + getSasKeyExpiryPeriod());

      if (isSecurityEnabled && (delegationToken != null && !delegationToken
          .isEmpty())) {
        uriBuilder.addParameter(DELEGATION_TOKEN_QUERY_PARAM_NAME,
            this.delegationToken);
      }

        UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
        UserGroupInformation connectUgi = ugi.getRealUser();
        if (connectUgi == null) {
          connectUgi = ugi;
        } else{
          uriBuilder.addParameter(Constants.DOAS_PARAM, ugi.getShortUserName());
        }

      if(isSecurityEnabled && !connectUgi.hasKerberosCredentials()){
        connectUgi = UserGroupInformation.getLoginUser();
      }
      return getSASKey(uriBuilder.build(), connectUgi);
    } catch (URISyntaxException uriSyntaxEx) {
      throw new SASKeyGenerationException("Encountered URISyntaxException"
          + " while building the HttpGetRequest to " + " remote service",
          uriSyntaxEx);
    } catch (IOException e) {
      throw new SASKeyGenerationException("Encountered IOException"
      + " while building the HttpGetRequest to remote service", e);
    }
  }

  private URI getSASKey(final URI uri, UserGroupInformation connectUgi)
      throws URISyntaxException, SASKeyGenerationException {
    RemoteSASKeyGenerationResponse sasKeyResponse = null;
    try {
      connectUgi.checkTGTAndReloginFromKeytab();
      sasKeyResponse = connectUgi.doAs(new PrivilegedExceptionAction<RemoteSASKeyGenerationResponse>() {
            @Override
            public RemoteSASKeyGenerationResponse run() throws Exception {
              AuthenticatedURL.Token token = null;
              if (isKerberosSupportEnabled && UserGroupInformation.isSecurityEnabled() && (
                  delegationToken == null || delegationToken.isEmpty())) {
                token = new AuthenticatedURL.Token();
                final Authenticator kerberosAuthenticator = new KerberosDelegationTokenAuthenticator();
                kerberosAuthenticator.authenticate(uri.toURL(), token);
                Validate.isTrue(token.isSet(),
                    "Authenticated Token is NOT present. The request cannot proceed.");
              }
              return makeRemoteRequest(uri, (token != null ? token.toString() : null));
            }
          });
    } catch (InterruptedException e) {
      LOG.error("Error fetching the SAS Key from Remote Service", e);
    } catch (IOException e) {
      LOG.error("Error fetching the SAS Key from Remote Service", e);
    }

    if (sasKeyResponse == null) {
      throw new SASKeyGenerationException(
          "RemoteSASKeyGenerationResponse" + " object null from remote call");
    } else if (sasKeyResponse.getResponseCode() == REMOTE_CALL_SUCCESS_CODE) {
      return new URI(sasKeyResponse.getSasKey());
    } else {
      throw new SASKeyGenerationException("Remote Service encountered error"
          + " in SAS Key generation : " + sasKeyResponse.getResponseMessage());
    }
  }

  /**
   * Helper method to make a remote request.
   * @param uri - Uri to use for the remote request
   * @param token - hadoop.auth token for the remote request
   * @return RemoteSASKeyGenerationResponse
   */
  private RemoteSASKeyGenerationResponse makeRemoteRequest(URI uri, String token)
      throws SASKeyGenerationException {

    try {
      HttpGet httpGet = new HttpGet(uri);
      if(token != null){
        httpGet.setHeader("Cookie", AuthenticatedURL.AUTH_COOKIE + "=" + token);
      }
      String responseBody =
          remoteCallHelper.makeRemoteGetRequest(httpGet);

      ObjectMapper objectMapper = new ObjectMapper();
      return objectMapper.readValue(responseBody,
          RemoteSASKeyGenerationResponse.class);

    } catch (WasbRemoteCallException remoteCallEx) {
      throw new SASKeyGenerationException("Encountered RemoteCallException"
          + " while retrieving SAS key from remote service", remoteCallEx);
    } catch (JsonParseException jsonParserEx) {
      throw new SASKeyGenerationException("Encountered JsonParseException "
          + "while parsing the response from remote"
          + " service into RemoteSASKeyGenerationResponse object", jsonParserEx);
    } catch (JsonMappingException jsonMappingEx) {
      throw new SASKeyGenerationException("Encountered JsonMappingException"
          + " while mapping the response from remote service into "
          + "RemoteSASKeyGenerationResponse object", jsonMappingEx);
    } catch (IOException ioEx) {
      throw new SASKeyGenerationException("Encountered IOException while "
          + "accessing remote service to retrieve SAS Key", ioEx);
    }
  }
}

/**
 * POJO representing the response expected from a Remote
 * SAS Key generation service.
 * The remote SAS Key generation service is expected to
 * return SAS key in json format:
 * {
 *    "responseCode" : 0 or non-zero <int>,
 *    "responseMessage" : relavant message on failure <String>,
 *    "sasKey" : Requested SAS Key <String>
 * }
 */
class RemoteSASKeyGenerationResponse {

  /**
   * Response code for the call.
   */
  private int responseCode;

  /**
   * An intelligent message corresponding to
   * result. Specifically in case of failure
   * the reason for failure.
   */
  private String responseMessage;

  /**
   * SAS Key corresponding to the request.
   */
  private String sasKey;

  public int getResponseCode() {
    return responseCode;
  }

  public void setResponseCode(int responseCode) {
    this.responseCode = responseCode;
  }

  public String getResponseMessage() {
    return responseMessage;
  }

  public void setResponseMessage(String responseMessage) {
    this.responseMessage = responseMessage;
  }

  public String getSasKey() {
    return sasKey;
  }

  public void setSasKey(String sasKey) {
    this.sasKey = sasKey;
  }
}