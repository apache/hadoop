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
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.fasterxml.jackson.databind.ObjectReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.azure.security.Constants;
import org.apache.hadoop.io.retry.RetryPolicy;
import org.apache.hadoop.io.retry.RetryUtils;
import org.apache.hadoop.security.UserGroupInformation;

import org.apache.http.NameValuePair;
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
  private static final ObjectReader RESPONSE_READER = new ObjectMapper()
      .readerFor(RemoteSASKeyGenerationResponse.class);

  /**
   * Configuration parameter name expected in the Configuration
   * object to provide the url of the remote service {@value}
   */
  public static final String KEY_CRED_SERVICE_URLS =
      "fs.azure.cred.service.urls";
  /**
   * Configuration key to enable http retry policy for SAS Key generation. {@value}
   */
  public static final String
      SAS_KEY_GENERATOR_HTTP_CLIENT_RETRY_POLICY_ENABLED_KEY =
      "fs.azure.saskeygenerator.http.retry.policy.enabled";
  /**
   * Configuration key for SAS Key Generation http retry policy spec. {@value}
   */
  public static final String
      SAS_KEY_GENERATOR_HTTP_CLIENT_RETRY_POLICY_SPEC_KEY =
      "fs.azure.saskeygenerator.http.retry.policy.spec";
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
  private static final String CONTAINER_QUERY_PARAM_NAME = "container";
  /**
   * Query parameter name for the relative path inside the storage
   * account container. {@value}
   */
  private static final String RELATIVE_PATH_QUERY_PARAM_NAME = "relative_path";
  /**
   * SAS Key Generation Remote http client retry policy spec. {@value}
   */
  private static final String
      SAS_KEY_GENERATOR_HTTP_CLIENT_RETRY_POLICY_SPEC_DEFAULT =
      "10,3,100,2";
  /**
   * Saskey caching period
   */
  private static final String SASKEY_CACHEENTRY_EXPIRY_PERIOD =
      "fs.azure.saskey.cacheentry.expiry.period";

  private WasbRemoteCallHelper remoteCallHelper = null;
  private boolean isKerberosSupportEnabled;
  private boolean isSpnegoTokenCacheEnabled;
  private RetryPolicy retryPolicy;
  private String[] commaSeparatedUrls;
  private CachingAuthorizer<CachedSASKeyEntry, URI> cache;

  private static final int HOURS_IN_DAY = 24;
  private static final int MINUTES_IN_HOUR = 60;

  public RemoteSASKeyGeneratorImpl(Configuration conf) {
    super(conf);
  }

  public void initialize(Configuration conf) throws IOException {

    LOG.debug("Initializing RemoteSASKeyGeneratorImpl instance");

    this.retryPolicy = RetryUtils.getMultipleLinearRandomRetry(conf,
        SAS_KEY_GENERATOR_HTTP_CLIENT_RETRY_POLICY_ENABLED_KEY, true,
        SAS_KEY_GENERATOR_HTTP_CLIENT_RETRY_POLICY_SPEC_KEY,
        SAS_KEY_GENERATOR_HTTP_CLIENT_RETRY_POLICY_SPEC_DEFAULT);

    this.isKerberosSupportEnabled =
        conf.getBoolean(Constants.AZURE_KERBEROS_SUPPORT_PROPERTY_NAME, false);
    this.isSpnegoTokenCacheEnabled =
        conf.getBoolean(Constants.AZURE_ENABLE_SPNEGO_TOKEN_CACHE, true);
    this.commaSeparatedUrls = conf.getTrimmedStrings(KEY_CRED_SERVICE_URLS);
    if (this.commaSeparatedUrls == null || this.commaSeparatedUrls.length <= 0) {
      throw new IOException(
          KEY_CRED_SERVICE_URLS + " config not set" + " in configuration.");
    }
    if (isKerberosSupportEnabled && UserGroupInformation.isSecurityEnabled()) {
      this.remoteCallHelper = new SecureWasbRemoteCallHelper(retryPolicy, false,
          isSpnegoTokenCacheEnabled);
    } else {
      this.remoteCallHelper = new WasbRemoteCallHelper(retryPolicy);
    }

    /* Expire the cache entry five minutes before the actual saskey expiry, so that we never encounter a case
     * where a stale sas-key-entry is picked up from the cache; which is expired on use.
     */
    long sasKeyExpiryPeriodInMinutes = getSasKeyExpiryPeriod() * HOURS_IN_DAY * MINUTES_IN_HOUR; // sas-expiry is in days, convert into mins
    long cacheEntryDurationInMinutes =
        conf.getTimeDuration(SASKEY_CACHEENTRY_EXPIRY_PERIOD, sasKeyExpiryPeriodInMinutes, TimeUnit.MINUTES);
    cacheEntryDurationInMinutes = (cacheEntryDurationInMinutes > (sasKeyExpiryPeriodInMinutes - 5))
        ? (sasKeyExpiryPeriodInMinutes - 5)
        : cacheEntryDurationInMinutes;
    this.cache = new CachingAuthorizer<>(cacheEntryDurationInMinutes, "SASKEY");
    this.cache.init(conf);
    LOG.debug("Initialization of RemoteSASKeyGenerator instance successful");
  }

  @Override
  public URI getContainerSASUri(String storageAccount,
      String container) throws SASKeyGenerationException {
    RemoteSASKeyGenerationResponse sasKeyResponse = null;
    try {
      CachedSASKeyEntry cacheKey = new CachedSASKeyEntry(storageAccount, container, "/");
      URI cacheResult = cache.get(cacheKey);
      if (cacheResult != null) {
        return cacheResult;
      }

      LOG.debug("Generating Container SAS Key: Storage Account {}, Container {}", storageAccount, container);
      URIBuilder uriBuilder = new URIBuilder();
      uriBuilder.setPath("/" + CONTAINER_SAS_OP);
      uriBuilder.addParameter(STORAGE_ACCOUNT_QUERY_PARAM_NAME, storageAccount);
      uriBuilder.addParameter(CONTAINER_QUERY_PARAM_NAME, container);
      uriBuilder.addParameter(SAS_EXPIRY_QUERY_PARAM_NAME,
          "" + getSasKeyExpiryPeriod());

      sasKeyResponse = makeRemoteRequest(commaSeparatedUrls, uriBuilder.getPath(),
              uriBuilder.getQueryParams());

      if (sasKeyResponse.getResponseCode() == REMOTE_CALL_SUCCESS_CODE) {
        URI sasKey = new URI(sasKeyResponse.getSasKey());
        cache.put(cacheKey, sasKey);
        return sasKey;
      } else {
        throw new SASKeyGenerationException(
            "Remote Service encountered error in SAS Key generation : "
                + sasKeyResponse.getResponseMessage());
      }
    } catch (URISyntaxException uriSyntaxEx) {
      throw new SASKeyGenerationException("Encountered URISyntaxException"
          + " while building the HttpGetRequest to remote service for ",
          uriSyntaxEx);
    }
  }

  @Override
  public URI getRelativeBlobSASUri(String storageAccount,
      String container, String relativePath) throws SASKeyGenerationException {

    try {
      CachedSASKeyEntry cacheKey = new CachedSASKeyEntry(storageAccount, container, relativePath);
      URI cacheResult = cache.get(cacheKey);
      if (cacheResult != null) {
        return cacheResult;
      }

      LOG.debug("Generating RelativePath SAS Key for relativePath {} inside Container {} inside Storage Account {}",
          relativePath, container, storageAccount);

      URIBuilder uriBuilder = new URIBuilder();
      uriBuilder.setPath("/" + BLOB_SAS_OP);
      uriBuilder.addParameter(STORAGE_ACCOUNT_QUERY_PARAM_NAME, storageAccount);
      uriBuilder.addParameter(CONTAINER_QUERY_PARAM_NAME, container);
      uriBuilder.addParameter(RELATIVE_PATH_QUERY_PARAM_NAME, relativePath);
      uriBuilder.addParameter(SAS_EXPIRY_QUERY_PARAM_NAME,
          "" + getSasKeyExpiryPeriod());

      RemoteSASKeyGenerationResponse sasKeyResponse =
          makeRemoteRequest(commaSeparatedUrls, uriBuilder.getPath(),
              uriBuilder.getQueryParams());
      if (sasKeyResponse.getResponseCode() == REMOTE_CALL_SUCCESS_CODE) {
        URI sasKey = new URI(sasKeyResponse.getSasKey());
        cache.put(cacheKey, sasKey);
        return sasKey;
      } else {
        throw new SASKeyGenerationException(
            "Remote Service encountered error in SAS Key generation : "
                + sasKeyResponse.getResponseMessage());
      }
    } catch (URISyntaxException uriSyntaxEx) {
      throw new SASKeyGenerationException("Encountered URISyntaxException"
          + " while building the HttpGetRequest to " + " remote service",
          uriSyntaxEx);
    }
  }

  /**
   * Helper method to make a remote request.
   *
   * @param urls        - Urls to use for the remote request
   * @param path        - hadoop.auth token for the remote request
   * @param queryParams - queryParams to be used.
   * @return RemoteSASKeyGenerationResponse
   */
  private RemoteSASKeyGenerationResponse makeRemoteRequest(String[] urls,
      String path, List<NameValuePair> queryParams)
      throws SASKeyGenerationException {

    try {
      String responseBody = remoteCallHelper
          .makeRemoteRequest(urls, path, queryParams, HttpGet.METHOD_NAME);
      return RESPONSE_READER.readValue(responseBody);

    } catch (WasbRemoteCallException remoteCallEx) {
      throw new SASKeyGenerationException("Encountered RemoteCallException"
          + " while retrieving SAS key from remote service", remoteCallEx);
    } catch (JsonParseException jsonParserEx) {
      throw new SASKeyGenerationException("Encountered JsonParseException "
          + "while parsing the response from remote"
          + " service into RemoteSASKeyGenerationResponse object",
          jsonParserEx);
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
 *   "responseCode" : 0 or non-zero <int>,
 *   "responseMessage" : relavant message on failure <String>,
 *   "sasKey" : Requested SAS Key <String>
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