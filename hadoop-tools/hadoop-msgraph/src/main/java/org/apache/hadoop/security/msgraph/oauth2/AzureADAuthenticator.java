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

package org.apache.hadoop.security.msgraph.oauth2;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Date;
import java.util.concurrent.TimeUnit;

import org.eclipse.jetty.http.HttpHeader;
import org.eclipse.jetty.http.HttpMethod;
import org.eclipse.jetty.http.HttpStatus;
import org.eclipse.jetty.http.MimeTypes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Utility class for constructing token requests to Azure AD.
 */
public class AzureADAuthenticator {

  private static final Logger LOG = LoggerFactory.getLogger(
      AzureADAuthenticator.class.getName());


  private AzureADAuthenticator() {
    // Utility class
  }

  /**
   * Get a token using the client credentials.
   * @param authEndpoint Authentication endpoint.
   * @param clientId Client identifier.
   * @param clientSecret Client secret.
   * @param grantType Type of grant.
   * @param resource Resource to get a token for.
   */
  public static AzureADToken getTokenUsingClientCreds(
      String authEndpoint, String clientId, String clientSecret,
      String grantType, String resource) throws IOException {
    QueryParams qp = new QueryParams();
    qp.add("resource", resource);
    qp.add("grant_type", grantType);
    qp.add("client_id", clientId);
    qp.add("client_secret", clientSecret);
    LOG.debug("AADToken: starting to fetch token using client credentials " +
        "for client ID: {}.", clientId);
    return getToken(authEndpoint, qp.serialize());
  }

  /**
   * Get a token.
   * @param authEndpoint Endpoint of the auth service.
   * @param body Body of the request.
   */
  private static AzureADToken getToken(String authEndpoint, String body)
      throws IOException {
    AzureADToken token = null;
    RetryPolicy retryPolicy = new ExponentialBackoffPolicy(3, 1000, 2);

    IOException lastException;
    boolean succeeded;
    int httperror;

    do {
      httperror = 0;
      lastException = null;

      try {
        token = getTokenImpl(authEndpoint, body);
      } catch (HttpException httpException) {
        lastException = httpException;
        httperror = httpException.getHttpErrorCode();
      } catch (IOException e) {
        lastException = e;
      }

      succeeded = lastException == null;
    } while (!succeeded && retryPolicy.shouldRetry(httperror, lastException));

    if (!succeeded) {
      throw lastException;
    }

    return token;
  }

  private static AzureADToken getTokenImpl(
      String authEndpoint, String payload) throws IOException {
    AzureADToken token;
    HttpURLConnection conn = null;

    try {
      URL url = new URL(authEndpoint);
      conn = (HttpURLConnection) url.openConnection();
      conn.setRequestMethod(HttpMethod.POST.asString());
      conn.setReadTimeout((int) TimeUnit.SECONDS.toMillis(30));
      conn.setConnectTimeout((int) TimeUnit.SECONDS.toMillis(30));
      String requestId;

      conn.setRequestProperty(HttpHeader.CONNECTION.asString(), "close");
      conn.setDoOutput(true);
      conn.getOutputStream().write(payload.getBytes(StandardCharsets.UTF_8));

      int httpResponseCode = conn.getResponseCode();
      requestId = conn.getHeaderField("x-ms-request-id");
      String responseContentType = conn.getHeaderField(
          HttpHeader.CONTENT_TYPE.asString());
      long responseContentLength = conn.getHeaderFieldLong(
          HttpHeader.CONTENT_LENGTH.asString(), 0L);
      requestId = requestId == null ? "" : requestId;

      if (httpResponseCode != HttpStatus.OK_200 ||
          !responseContentType.startsWith(
              MimeTypes.Type.APPLICATION_JSON.asString()) ||
          responseContentLength <= 0L) {
        String responseBody = consumeInputStream(conn.getInputStream());
        String proxies = "none";
        String httpProxy = System.getProperty("http.proxy");
        String httpsProxy = System.getProperty("https.proxy");
        if (httpProxy != null || httpsProxy != null) {
          proxies = "http:" + httpProxy + ";https:" + httpsProxy;
        }

        String logMessage = String.format("AADToken: HTTP connection failed " +
                "for getting token from AzureAD. Http response: %d %s " +
                ", Content-Type: %s, Content-Length: %d, Request ID: %s" +
                ", Proxies: %s, First 1K of Body: %s.",
            httpResponseCode, conn.getResponseMessage(), responseContentType,
            responseContentLength, requestId, proxies, responseBody);
        LOG.debug(logMessage);
        throw new HttpException(httpResponseCode, requestId, logMessage);
      }

      InputStream httpResponseStream = conn.getInputStream();
      token = parseTokenFromStream(httpResponseStream);
    } finally {
      if (conn != null) {
        conn.disconnect();
      }
    }

    return token;
  }

  private static String consumeInputStream(InputStream inStream)
      throws IOException {
    final int bufferSize = 1024;

    int totalBytesRead = 0;
    int bytesRead;
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();

    do {
      byte[] b = new byte[bufferSize];
      bytesRead = inStream.read(b, totalBytesRead, bufferSize);
      if (bytesRead > 0) {
        totalBytesRead += bytesRead;
        bytes.write(b);
      }
    } while (bytesRead > 0);

    return new String(bytes.toByteArray(), 0, totalBytesRead);
  }

  /**
   * Parse a token from an HTTP stream.
   * @param httpResponseStream HTTP response stream.
   */
  private static AzureADToken parseTokenFromStream(
      InputStream httpResponseStream) throws IOException {
    AzureADToken token;

    try {
      Integer expiryPeriod = null;
      String accessToken = null;

      JsonFactory jf = new JsonFactory();
      JsonParser jp = jf.createParser(httpResponseStream);
      jp.nextToken();
      for(; jp.hasCurrentToken(); jp.nextToken()) {
        if (jp.getCurrentToken() == JsonToken.FIELD_NAME) {
          String fieldName = jp.getCurrentName();
          jp.nextToken();
          String fieldValue = jp.getText();
          if (fieldName.equals("access_token")) {
            accessToken = fieldValue;
          }

          if (fieldName.equals("expires_in")) {
            expiryPeriod = Integer.parseInt(fieldValue);
          }
        }
      }

      if (accessToken == null) {
        throw new IOException("Could not retrieve the access token");
      }

      if (expiryPeriod == null) {
        throw new IOException("Could not retrieve the expiry period");
      }

      jp.close();

      long expiry = System.currentTimeMillis();
      expiry += TimeUnit.SECONDS.toMillis(expiryPeriod);
      Date expiryDate = new Date(expiry);
      token = new AzureADToken(accessToken, expiryDate);
      LOG.debug("AADToken: fetched token with expiry {}.", token.getExpiry());
    } catch (Exception e) {
      LOG.warn("AADToken: got an exception when parsing JSON token {}.",
          e.getMessage());
      throw e;
    } finally {
      httpResponseStream.close();
    }

    return token;
  }
}