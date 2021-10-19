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

package org.apache.hadoop.fs.azurebfs.oauth2;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Date;
import java.util.Hashtable;
import java.util.Map;

import org.apache.hadoop.thirdparty.com.google.common.base.Preconditions;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.JsonToken;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.fs.azurebfs.AbfsConfiguration;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.azurebfs.services.AbfsIoUtils;
import org.apache.hadoop.fs.azurebfs.services.ExponentialRetryPolicy;

/**
 * This class provides convenience methods to obtain AAD tokens.
 * While convenient, it is not necessary to use these methods to
 * obtain the tokens. Customers can use any other method
 * (e.g., using the adal4j client) to obtain tokens.
 */

@InterfaceAudience.Private
@InterfaceStability.Evolving
public final class AzureADAuthenticator {

  private static final Logger LOG = LoggerFactory.getLogger(AzureADAuthenticator.class);
  private static final String RESOURCE_NAME = "https://storage.azure.com/";
  private static final String SCOPE = "https://storage.azure.com/.default";
  private static final int CONNECT_TIMEOUT = 30 * 1000;
  private static final int READ_TIMEOUT = 30 * 1000;

  private static ExponentialRetryPolicy tokenFetchRetryPolicy;

  private AzureADAuthenticator() {
    // no operation
  }

  public static void init(AbfsConfiguration abfsConfiguration) {
    tokenFetchRetryPolicy = abfsConfiguration.getOauthTokenFetchRetryPolicy();
  }

  /**
   * gets Azure Active Directory token using the user ID and password of
   * a service principal (that is, Web App in Azure Active Directory).
   *
   * Azure Active Directory allows users to set up a web app as a
   * service principal. Users can optionally obtain service principal keys
   * from AAD. This method gets a token using a service principal's client ID
   * and keys. In addition, it needs the token endpoint associated with the
   * user's directory.
   *
   *
   * @param authEndpoint the OAuth 2.0 token endpoint associated
   *                     with the user's directory (obtain from
   *                     Active Directory configuration)
   * @param clientId     the client ID (GUID) of the client web app
   *                     btained from Azure Active Directory configuration
   * @param clientSecret the secret key of the client web app
   * @return {@link AzureADToken} obtained using the creds
   * @throws IOException throws IOException if there is a failure in connecting to Azure AD
   */
  public static AzureADToken getTokenUsingClientCreds(String authEndpoint,
      String clientId, String clientSecret) throws IOException {
    Preconditions.checkNotNull(authEndpoint, "authEndpoint");
    Preconditions.checkNotNull(clientId, "clientId");
    Preconditions.checkNotNull(clientSecret, "clientSecret");
    boolean isVersion2AuthenticationEndpoint = authEndpoint.contains("/oauth2/v2.0/");

    QueryParams qp = new QueryParams();
    if (isVersion2AuthenticationEndpoint) {
      qp.add("scope", SCOPE);
    } else {
      qp.add("resource", RESOURCE_NAME);
    }
    qp.add("grant_type", "client_credentials");
    qp.add("client_id", clientId);
    qp.add("client_secret", clientSecret);
    LOG.debug("AADToken: starting to fetch token using client creds for client ID " + clientId);

    return getTokenCall(authEndpoint, qp.serialize(), null, null);
  }

  /**
   * Gets AAD token from the local virtual machine's VM extension. This only works on
   * an Azure VM with MSI extension
   * enabled.
   *
   * @param authEndpoint the OAuth 2.0 token endpoint associated
   *                     with the user's directory (obtain from
   *                     Active Directory configuration)
   * @param tenantGuid  (optional) The guid of the AAD tenant. Can be {@code null}.
   * @param clientId    (optional) The clientId guid of the MSI service
   *                    principal to use. Can be {@code null}.
   * @param bypassCache {@code boolean} specifying whether a cached token is acceptable or a fresh token
   *                    request should me made to AAD
   * @return {@link AzureADToken} obtained using the creds
   * @throws IOException throws IOException if there is a failure in obtaining the token
   */
  public static AzureADToken getTokenFromMsi(final String authEndpoint,
      final String tenantGuid, final String clientId, String authority,
      boolean bypassCache) throws IOException {
    QueryParams qp = new QueryParams();
    qp.add("api-version", "2018-02-01");
    qp.add("resource", RESOURCE_NAME);

    if (tenantGuid != null && tenantGuid.length() > 0) {
      authority = authority + tenantGuid;
      LOG.debug("MSI authority : {}", authority);
      qp.add("authority", authority);
    }

    if (clientId != null && clientId.length() > 0) {
      qp.add("client_id", clientId);
    }

    if (bypassCache) {
      qp.add("bypass_cache", "true");
    }

    Hashtable<String, String> headers = new Hashtable<>();
    headers.put("Metadata", "true");

    LOG.debug("AADToken: starting to fetch token using MSI");
    return getTokenCall(authEndpoint, qp.serialize(), headers, "GET", true);
  }

  /**
   * Gets Azure Active Directory token using refresh token.
   *
   * @param authEndpoint the OAuth 2.0 token endpoint associated
   *                     with the user's directory (obtain from
   *                     Active Directory configuration)
   * @param clientId the client ID (GUID) of the client web app obtained from Azure Active Directory configuration
   * @param refreshToken the refresh token
   * @return {@link AzureADToken} obtained using the refresh token
   * @throws IOException throws IOException if there is a failure in connecting to Azure AD
   */
  public static AzureADToken getTokenUsingRefreshToken(
      final String authEndpoint, final String clientId,
      final String refreshToken) throws IOException {
    QueryParams qp = new QueryParams();
    qp.add("grant_type", "refresh_token");
    qp.add("refresh_token", refreshToken);
    if (clientId != null) {
      qp.add("client_id", clientId);
    }
    LOG.debug("AADToken: starting to fetch token using refresh token for client ID " + clientId);
    return getTokenCall(authEndpoint, qp.serialize(), null, null);
  }


  /**
   * This exception class contains the http error code,
   * requestId and error message, it is thrown when AzureADAuthenticator
   * failed to get the Azure Active Directory token.
   */
  @InterfaceAudience.LimitedPrivate("authorization-subsystems")
  @InterfaceStability.Unstable
  public static class HttpException extends IOException {
    private final int httpErrorCode;
    private final String requestId;

    private final String url;

    private final String contentType;

    private final String body;

    /**
     * Gets Http error status code.
     * @return  http error code.
     */
    public int getHttpErrorCode() {
      return this.httpErrorCode;
    }

    /**
     * Gets http request id .
     * @return  http request id.
     */
    public String getRequestId() {
      return this.requestId;
    }

    protected HttpException(
        final int httpErrorCode,
        final String requestId,
        final String message,
        final String url,
        final String contentType,
        final String body) {
      super(message);
      this.httpErrorCode = httpErrorCode;
      this.requestId = requestId;
      this.url = url;
      this.contentType = contentType;
      this.body = body;
    }

    public String getUrl() {
      return url;
    }

    public String getContentType() {
      return contentType;
    }

    public String getBody() {
      return body;
    }

    @Override
    public String getMessage() {
      final StringBuilder sb = new StringBuilder();
      sb.append("HTTP Error ");
      sb.append(httpErrorCode);
      if (!url.isEmpty()) {
        sb.append("; url='").append(url).append('\'').append(' ');
      }

      sb.append(super.getMessage());
      if (!requestId.isEmpty()) {
        sb.append("; requestId='").append(requestId).append('\'');
      }

      if (!contentType.isEmpty()) {
        sb.append("; contentType='").append(contentType).append('\'');
      }

      if (!body.isEmpty()) {
        sb.append("; response '").append(body).append('\'');
      }

      return sb.toString();
    }
  }

  /**
   * An unexpected HTTP response was raised, such as text coming back
   * from what should be an OAuth endpoint.
   */
  public static class UnexpectedResponseException extends HttpException {

    public UnexpectedResponseException(final int httpErrorCode,
        final String requestId,
        final String message,
        final String url,
        final String contentType,
        final String body) {
      super(httpErrorCode, requestId, message, url, contentType, body);
    }

  }

  private static AzureADToken getTokenCall(String authEndpoint, String body,
      Hashtable<String, String> headers, String httpMethod) throws IOException {
    return getTokenCall(authEndpoint, body, headers, httpMethod, false);
  }

  private static AzureADToken getTokenCall(String authEndpoint, String body,
      Hashtable<String, String> headers, String httpMethod, boolean isMsi)
      throws IOException {
    AzureADToken token = null;

    int httperror = 0;
    IOException ex = null;
    boolean succeeded = false;
    boolean isRecoverableFailure = true;
    int retryCount = 0;
    boolean shouldRetry;
    LOG.trace("First execution of REST operation getTokenSingleCall");
    do {
      httperror = 0;
      ex = null;
      try {
        token = getTokenSingleCall(authEndpoint, body, headers, httpMethod, isMsi);
      } catch (HttpException e) {
        httperror = e.httpErrorCode;
        ex = e;
      } catch (IOException e) {
        httperror = -1;
        isRecoverableFailure = isRecoverableFailure(e);
        ex = new HttpException(httperror, "", String
            .format("AzureADAuthenticator.getTokenCall threw %s : %s",
                e.getClass().getTypeName(), e.getMessage()), authEndpoint, "",
            "");
      }
      succeeded = ((httperror == 0) && (ex == null));
      shouldRetry = !succeeded && isRecoverableFailure
          && tokenFetchRetryPolicy.shouldRetry(retryCount, httperror);
      retryCount++;
      if (shouldRetry) {
        LOG.debug("Retrying getTokenSingleCall. RetryCount = {}", retryCount);
        try {
          Thread.sleep(tokenFetchRetryPolicy.getRetryInterval(retryCount));
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      }

    } while (shouldRetry);
    if (!succeeded) {
      throw ex;
    }
    return token;
  }

  private static boolean isRecoverableFailure(IOException e) {
    return !(e instanceof MalformedURLException
        || e instanceof FileNotFoundException);
  }

  private static AzureADToken getTokenSingleCall(String authEndpoint,
      String payload, Hashtable<String, String> headers, String httpMethod,
      boolean isMsi)
          throws IOException {

    AzureADToken token = null;
    HttpURLConnection conn = null;
    String urlString = authEndpoint;

    httpMethod = (httpMethod == null) ? "POST" : httpMethod;
    if (httpMethod.equals("GET")) {
      urlString = urlString + "?" + payload;
    }

    try {
      LOG.debug("Requesting an OAuth token by {} to {}",
          httpMethod, authEndpoint);
      URL url = new URL(urlString);
      conn = (HttpURLConnection) url.openConnection();
      conn.setRequestMethod(httpMethod);
      conn.setReadTimeout(READ_TIMEOUT);
      conn.setConnectTimeout(CONNECT_TIMEOUT);

      if (headers != null && headers.size() > 0) {
        for (Map.Entry<String, String> entry : headers.entrySet()) {
          conn.setRequestProperty(entry.getKey(), entry.getValue());
        }
      }
      conn.setRequestProperty("Connection", "close");
      AbfsIoUtils.dumpHeadersToDebugLog("Request Headers",
          conn.getRequestProperties());
      if (httpMethod.equals("POST")) {
        conn.setDoOutput(true);
        conn.getOutputStream().write(payload.getBytes("UTF-8"));
      }

      int httpResponseCode = conn.getResponseCode();
      LOG.debug("Response {}", httpResponseCode);
      AbfsIoUtils.dumpHeadersToDebugLog("Response Headers",
          conn.getHeaderFields());

      String requestId = conn.getHeaderField("x-ms-request-id");
      String responseContentType = conn.getHeaderField("Content-Type");
      long responseContentLength = conn.getHeaderFieldLong("Content-Length", 0);

      requestId = requestId == null ? "" : requestId;
      if (httpResponseCode == HttpURLConnection.HTTP_OK
              && responseContentType.startsWith("application/json") && responseContentLength > 0) {
        InputStream httpResponseStream = conn.getInputStream();
        token = parseTokenFromStream(httpResponseStream, isMsi);
      } else {
        InputStream stream = conn.getErrorStream();
        if (stream == null) {
          // no error stream, try the original input stream
          stream = conn.getInputStream();
        }
        String responseBody = consumeInputStream(stream, 1024);
        String proxies = "none";
        String httpProxy = System.getProperty("http.proxy");
        String httpsProxy = System.getProperty("https.proxy");
        if (httpProxy != null || httpsProxy != null) {
          proxies = "http:" + httpProxy + "; https:" + httpsProxy;
        }
        String operation = "AADToken: HTTP connection to " + authEndpoint
            + " failed for getting token from AzureAD.";
        String logMessage = operation
                        + " HTTP response: " + httpResponseCode
                        + " " + conn.getResponseMessage()
                        + " Proxies: " + proxies
                        + (responseBody.isEmpty()
                          ? ""
                          : ("\nFirst 1K of Body: " + responseBody));
        LOG.debug(logMessage);
        if (httpResponseCode == HttpURLConnection.HTTP_OK) {
          // 200 is returned by some of the sign-on pages, but can also
          // come from proxies, utterly wrong URLs, etc.
          throw new UnexpectedResponseException(httpResponseCode,
              requestId,
              operation
                  + " Unexpected response."
                  + " Check configuration, URLs and proxy settings."
                  + " proxies=" + proxies,
              authEndpoint,
              responseContentType,
              responseBody);
        } else {
          // general HTTP error
          throw new HttpException(httpResponseCode,
              requestId,
              operation,
              authEndpoint,
              responseContentType,
              responseBody);
        }
      }
    } finally {
      if (conn != null) {
        conn.disconnect();
      }
    }
    return token;
  }

  private static AzureADToken parseTokenFromStream(
      InputStream httpResponseStream, boolean isMsi) throws IOException {
    AzureADToken token = new AzureADToken();
    try {
      int expiryPeriodInSecs = 0;
      long expiresOnInSecs = -1;

      JsonFactory jf = new JsonFactory();
      JsonParser jp = jf.createJsonParser(httpResponseStream);
      String fieldName, fieldValue;
      jp.nextToken();
      while (jp.hasCurrentToken()) {
        if (jp.getCurrentToken() == JsonToken.FIELD_NAME) {
          fieldName = jp.getCurrentName();
          jp.nextToken();  // field value
          fieldValue = jp.getText();

          if (fieldName.equals("access_token")) {
            token.setAccessToken(fieldValue);
          }

          if (fieldName.equals("expires_in")) {
            expiryPeriodInSecs = Integer.parseInt(fieldValue);
          }

          if (fieldName.equals("expires_on")) {
            expiresOnInSecs = Long.parseLong(fieldValue);
          }

        }
        jp.nextToken();
      }
      jp.close();
      if (expiresOnInSecs > 0) {
        LOG.debug("Expiry based on expires_on: {}", expiresOnInSecs);
        token.setExpiry(new Date(expiresOnInSecs * 1000));
      } else {
        if (isMsi) {
          // Currently there is a known issue that MSI does not update expires_in
          // for refresh and will have the value from first AAD token fetch request.
          // Due to this known limitation, expires_in is not supported for MSI token fetch flow.
          throw new UnsupportedOperationException("MSI Responded with invalid expires_on");
        }

        LOG.debug("Expiry based on expires_in: {}", expiryPeriodInSecs);
        long expiry = System.currentTimeMillis();
        expiry = expiry + expiryPeriodInSecs * 1000L; // convert expiryPeriod to milliseconds and add
        token.setExpiry(new Date(expiry));
      }

      LOG.debug("AADToken: fetched token with expiry {}, expiresOn passed: {}",
          token.getExpiry().toString(), expiresOnInSecs);
    } catch (Exception ex) {
      LOG.debug("AADToken: got exception when parsing json token " + ex.toString());
      throw ex;
    } finally {
      httpResponseStream.close();
    }
    return token;
  }

  private static String consumeInputStream(InputStream inStream, int length) throws IOException {
    if (inStream == null) {
      // the HTTP request returned an empty body
      return "";
    }
    byte[] b = new byte[length];
    int totalBytesRead = 0;
    int bytesRead = 0;

    do {
      bytesRead = inStream.read(b, totalBytesRead, length - totalBytesRead);
      if (bytesRead > 0) {
        totalBytesRead += bytesRead;
      }
    } while (bytesRead >= 0 && totalBytesRead < length);

    return new String(b, 0, totalBytesRead, StandardCharsets.UTF_8);
  }
}
