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

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.fs.azure.security.Constants;
import org.apache.hadoop.io.retry.RetryPolicy;
import org.apache.http.Header;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.NameValuePair;
import org.apache.http.StatusLine;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.impl.client.HttpClientBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.InterruptedIOException;
import java.net.InetAddress;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Random;

/**
 * Helper class the has constants and helper methods
 * used in WASB when integrating with a remote http cred
 * service. Currently, remote service will be used to generate
 * SAS keys.
 */
public class WasbRemoteCallHelper {

  public static final Logger LOG =
      LoggerFactory.getLogger(WasbRemoteCallHelper.class);
  /**
   * Return code when the remote call is successful. {@value}
   */
  public static final int REMOTE_CALL_SUCCESS_CODE = 0;

  /**
   * Application Json content type.
   */
  private static final String APPLICATION_JSON = "application/json";

  /**
   * Max content length of the response.
   */
  private static final int MAX_CONTENT_LENGTH = 1024;

  /**
   * Client instance to be used for making the remote call.
   */
  private HttpClient client = null;

  private Random random = new Random();

  private RetryPolicy retryPolicy = null;

  public WasbRemoteCallHelper(RetryPolicy retryPolicy) {
    this.client = HttpClientBuilder.create().build();
    this.retryPolicy = retryPolicy;
  }

  @VisibleForTesting public void updateHttpClient(HttpClient client) {
    this.client = client;
  }

  /**
   * Helper method to make remote HTTP Get request.
   *
   * @param urls        - Service urls to be used, if one fails try another.
   * @param path        - URL endpoint for the resource.
   * @param queryParams - list of query parameters
   * @param httpMethod  - http Method to be used.
   * @return Http Response body returned as a string. The caller
   * is expected to semantically understand the response.
   * @throws IOException when there an error in executing the remote http request.
   */
  public String makeRemoteRequest(String[] urls, String path,
      List<NameValuePair> queryParams, String httpMethod) throws IOException {

    return retryableRequest(urls, path, queryParams, httpMethod);
  }

  protected String retryableRequest(String[] urls, String path,
      List<NameValuePair> queryParams, String httpMethod) throws IOException {
    HttpResponse response = null;
    HttpUriRequest httpRequest = null;

    /**
     * Get the index of local url if any. If list of urls contains strings like
     * "https://localhost:" or "http://localhost", consider it as local url and
     * give it affinity more than other urls in the list.
     */

    int indexOfLocalUrl = -1;
    for (int i = 0; i < urls.length; i++) {
      if (urls[i].toLowerCase().startsWith("https://localhost:") || urls[i]
          .toLowerCase().startsWith("http://localhost:")) {
        indexOfLocalUrl = i;
      }
    }

    boolean requiresNewAuth = false;
    for (int retry = 0, index = (indexOfLocalUrl != -1)
                                ? indexOfLocalUrl
                                : random
                                    .nextInt(urls.length);; retry++, index++) {
      if (index >= urls.length) {
        index = index % urls.length;
      }
      /**
       * If the first request fails to localhost, then randomly pick the next url
       * from the remaining urls in the list, so that load can be balanced.
       */
      if (indexOfLocalUrl != -1 && retry == 1) {
        index = (index + random.nextInt(urls.length)) % urls.length;
        if (index == indexOfLocalUrl) {
          index = (index + 1) % urls.length;
        }
      }
      try {
        httpRequest =
            getHttpRequest(urls, path, queryParams, index, httpMethod,
                requiresNewAuth);
        httpRequest.setHeader("Accept", APPLICATION_JSON);
        response = client.execute(httpRequest);
        StatusLine statusLine = response.getStatusLine();
        if (statusLine == null
            || statusLine.getStatusCode() != HttpStatus.SC_OK) {
          requiresNewAuth =
              (statusLine == null)
                  || (statusLine.getStatusCode() == HttpStatus.SC_UNAUTHORIZED);

          throw new WasbRemoteCallException(
              httpRequest.getURI().toString() + ":" + ((statusLine != null)
                                                       ? statusLine.toString()
                                                       : "NULL"));
        } else {
          requiresNewAuth = false;
        }

        Header contentTypeHeader = response.getFirstHeader("Content-Type");
        if (contentTypeHeader == null || !APPLICATION_JSON
            .equals(contentTypeHeader.getValue())) {
          throw new WasbRemoteCallException(
              httpRequest.getURI().toString() + ":"
                  + "Content-Type mismatch: expected: " + APPLICATION_JSON
                  + ", got " + ((contentTypeHeader != null) ? contentTypeHeader
                  .getValue() : "NULL"));
        }

        Header contentLengthHeader = response.getFirstHeader("Content-Length");
        if (contentLengthHeader == null) {
          throw new WasbRemoteCallException(
              httpRequest.getURI().toString() + ":"
                  + "Content-Length header missing");
        }

        try {
          if (Integer.parseInt(contentLengthHeader.getValue())
              > MAX_CONTENT_LENGTH) {
            throw new WasbRemoteCallException(
                httpRequest.getURI().toString() + ":" + "Content-Length:"
                    + contentLengthHeader.getValue() + "exceeded max:"
                    + MAX_CONTENT_LENGTH);
          }
        } catch (NumberFormatException nfe) {
          throw new WasbRemoteCallException(
              httpRequest.getURI().toString() + ":"
                  + "Invalid Content-Length value :" + contentLengthHeader
                  .getValue());
        }

        BufferedReader rd = null;
        StringBuilder responseBody = new StringBuilder();
        try {
          rd = new BufferedReader(
              new InputStreamReader(response.getEntity().getContent(),
                  StandardCharsets.UTF_8));
          String responseLine = "";
          while ((responseLine = rd.readLine()) != null) {
            responseBody.append(responseLine);
          }
        } finally {
          rd.close();
        }
        return responseBody.toString();
      } catch (URISyntaxException uriSyntaxEx) {
        throw new WasbRemoteCallException("Encountered URISyntaxException "
            + "while building the HttpGetRequest to remote service",
            uriSyntaxEx);
      } catch (IOException e) {
        LOG.debug(e.getMessage(), e);
        try {
          shouldRetry(e, retry, (httpRequest != null)
                                ? httpRequest.getURI().toString()
                                : urls[index]);
        } catch (IOException ioex) {
          String message =
              "Encountered error while making remote call to " + String
                  .join(",", urls) + " retried " + retry + " time(s).";
          LOG.error(message, ioex);
          throw new WasbRemoteCallException(message, ioex);
        }
      }
    }
  }

  protected HttpUriRequest getHttpRequest(String[] urls, String path,
      List<NameValuePair> queryParams, int urlIndex, String httpMethod,
      boolean requiresNewAuth) throws URISyntaxException, IOException {
    URIBuilder uriBuilder = null;
    uriBuilder =
        new URIBuilder(urls[urlIndex]).setPath(path).setParameters(queryParams);
    if (uriBuilder.getHost().equals("localhost")) {
      uriBuilder.setHost(InetAddress.getLocalHost().getCanonicalHostName());
    }
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
    return httpUriRequest;
  }

  private void shouldRetry(final IOException ioe, final int retry,
      final String url) throws IOException {
    CharSequence authenticationExceptionMessage =
        Constants.AUTHENTICATION_FAILED_ERROR_MESSAGE;
    if (ioe instanceof WasbRemoteCallException && ioe.getMessage()
        .equals(authenticationExceptionMessage)) {
      throw ioe;
    }
    try {
      final RetryPolicy.RetryAction a = (retryPolicy != null)
                                        ? retryPolicy
                                            .shouldRetry(ioe, retry, 0, true)
                                        : RetryPolicy.RetryAction.FAIL;

      boolean isRetry = a.action == RetryPolicy.RetryAction.RetryDecision.RETRY;
      boolean isFailoverAndRetry =
          a.action == RetryPolicy.RetryAction.RetryDecision.FAILOVER_AND_RETRY;

      if (isRetry || isFailoverAndRetry) {
        LOG.debug("Retrying connect to Remote service:{}. Already tried {}"
                + " time(s); retry policy is {}, " + "delay {}ms.", url, retry,
            retryPolicy, a.delayMillis);

        Thread.sleep(a.delayMillis);
        return;
      }
    } catch (InterruptedIOException e) {
      LOG.warn(e.getMessage(), e);
      Thread.currentThread().interrupt();
      return;
    } catch (Exception e) {
      LOG.warn("Original exception is ", ioe);
      throw new WasbRemoteCallException(e.getMessage(), e);
    }
    LOG.debug("Not retrying anymore, already retried the urls {} time(s)",
        retry);
    throw new WasbRemoteCallException(
        url + ":" + "Encountered IOException while making remote call", ioe);
  }
}
