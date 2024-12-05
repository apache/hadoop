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

package org.apache.hadoop.fs.azurebfs.services;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.fs.PathIOException;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AbfsApacheHttpExpect100Exception;
import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpEntityEnclosingRequestBase;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpHead;
import org.apache.http.client.methods.HttpPatch;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.util.EntityUtils;

import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.APACHE_IMPL;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.EMPTY_STRING;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.HTTP_METHOD_DELETE;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.HTTP_METHOD_GET;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.HTTP_METHOD_HEAD;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.HTTP_METHOD_PATCH;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.HTTP_METHOD_POST;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.HTTP_METHOD_PUT;
import static org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations.CONTENT_LENGTH;
import static org.apache.http.entity.ContentType.TEXT_PLAIN;

/**
 * Implementation of {@link AbfsHttpOperation} for orchestrating server calls using
 * Apache Http Client.
 */
public class AbfsAHCHttpOperation extends AbfsHttpOperation {

  private static final Logger LOG = LoggerFactory.getLogger(
      AbfsAHCHttpOperation.class);

  /**
   * Request object for network call over ApacheHttpClient.
   */
  private final HttpRequestBase httpRequestBase;

  /**
   * Response object received from a server call over ApacheHttpClient.
   */
  private HttpResponse httpResponse;

  /**
   * Flag to indicate if the request is a payload request. HTTP methods PUT, POST,
   * PATCH qualify for payload requests.
   */
  private final boolean isPayloadRequest;

  /**
   * ApacheHttpClient to make network calls.
   */
  private final AbfsApacheHttpClient abfsApacheHttpClient;

  public AbfsAHCHttpOperation(final URL url,
      final String method,
      final List<AbfsHttpHeader> requestHeaders,
      final Duration connectionTimeout,
      final Duration readTimeout,
      final AbfsApacheHttpClient abfsApacheHttpClient,
      final AbfsClient abfsClient) throws IOException {
    super(LOG, url, method, requestHeaders, connectionTimeout, readTimeout, abfsClient);
    this.isPayloadRequest = HTTP_METHOD_PUT.equals(method)
        || HTTP_METHOD_PATCH.equals(method)
        || HTTP_METHOD_POST.equals(method);
    this.abfsApacheHttpClient = abfsApacheHttpClient;
    LOG.debug("Creating AbfsAHCHttpOperation for URL: {}, method: {}",
        url, method);

    final URI requestUri;
    try {
      requestUri = url.toURI();
    } catch (URISyntaxException e) {
      throw new IOException(e);
    }
    switch (getMethod()) {
    case HTTP_METHOD_PUT:
      httpRequestBase = new HttpPut(requestUri);
      break;
    case HTTP_METHOD_PATCH:
      httpRequestBase = new HttpPatch(requestUri);
      break;
    case HTTP_METHOD_POST:
      httpRequestBase = new HttpPost(requestUri);
      break;
    case HTTP_METHOD_GET:
      httpRequestBase = new HttpGet(requestUri);
      break;
    case HTTP_METHOD_DELETE:
      httpRequestBase = new HttpDelete(requestUri);
      break;
    case HTTP_METHOD_HEAD:
      httpRequestBase = new HttpHead(requestUri);
      break;
    default:
      /*
       * This would not happen as the AbfsClient would always be sending valid
       * method.
       */
      throw new PathIOException(getUrl().toString(),
          "Unsupported HTTP method: " + getMethod());
    }
  }

  /**
   * @return AbfsManagedHttpClientContext instance that captures latencies at
   * different phases of network call.
   */
  @VisibleForTesting
  AbfsManagedHttpClientContext getHttpClientContext() {
    return new AbfsManagedHttpClientContext();
  }

  /**{@inheritDoc}*/
  @Override
  protected InputStream getErrorStream() throws IOException {
    HttpEntity entity = httpResponse.getEntity();
    if (entity == null) {
      return null;
    }
    return entity.getContent();
  }

  /**{@inheritDoc}*/
  @Override
  String getConnProperty(final String key) {
    for (AbfsHttpHeader header : getRequestHeaders()) {
      if (header.getName().equals(key)) {
        return header.getValue();
      }
    }
    return null;
  }

  /**{@inheritDoc}*/
  @Override
  URL getConnUrl() {
    return getUrl();
  }

  /**{@inheritDoc}*/
  @Override
  Integer getConnResponseCode() throws IOException {
    return getStatusCode();
  }

  /**{@inheritDoc}*/
  @Override
  String getConnResponseMessage() throws IOException {
    return getStatusDescription();
  }

  /**{@inheritDoc}*/
  @Override
  public void processResponse(final byte[] buffer,
      final int offset,
      final int length) throws IOException {
    try {
      if (!isPayloadRequest) {
        prepareRequest();
        LOG.debug("Sending request: {}", httpRequestBase);
        httpResponse = executeRequest();
        LOG.debug("Request sent: {}; response {}", httpRequestBase,
            httpResponse);
      }
      parseResponseHeaderAndBody(buffer, offset, length);
    } finally {
      if (httpResponse != null) {
        try {
          EntityUtils.consume(httpResponse.getEntity());
        } finally {
          if (httpResponse instanceof CloseableHttpResponse) {
            ((CloseableHttpResponse) httpResponse).close();
          }
        }
      }
    }
  }

  /**
   * Parse response stream for headers and body.
   *
   * @param buffer byte array to store response body.
   * @param offset offset in the buffer to start storing the response body.
   * @param length length of the response body.
   *
   * @throws IOException network error while read response stream
   */
  @VisibleForTesting
  void parseResponseHeaderAndBody(final byte[] buffer,
      final int offset,
      final int length) throws IOException {
    setStatusCode(parseStatusCode(httpResponse));

    setStatusDescription(httpResponse.getStatusLine().getReasonPhrase());
    setRequestId();

    // dump the headers
    if (LOG.isDebugEnabled()) {
      AbfsIoUtils.dumpHeadersToDebugLog("Request Headers",
          getRequestProperties());
    }
    parseResponse(buffer, offset, length);
  }

  /**
   * Parse status code from response
   *
   * @param httpResponse response object
   * @return status code
   */
  @VisibleForTesting
  int parseStatusCode(HttpResponse httpResponse) {
    return httpResponse.getStatusLine().getStatusCode();
  }

  /**
   * Execute network call for the request
   *
   * @return response object
   * @throws IOException network error while executing the request
   */
  @VisibleForTesting
  HttpResponse executeRequest() throws IOException {
    AbfsManagedHttpClientContext abfsHttpClientContext
        = getHttpClientContext();
    try {
      LOG.debug("Executing request: {}", httpRequestBase);
      HttpResponse response = abfsApacheHttpClient.execute(httpRequestBase,
          abfsHttpClientContext, getConnectionTimeout(), getReadTimeout());
      setConnectionTimeMs(abfsHttpClientContext.getConnectTime());
      setSendRequestTimeMs(abfsHttpClientContext.getSendTime());
      setRecvResponseTimeMs(abfsHttpClientContext.getReadTime());
      return response;
    } catch (IOException e) {
      LOG.debug("Failed to execute request: {}", httpRequestBase, e);
      throw e;
    }
  }

  /**{@inheritDoc}*/
  @Override
  public void setRequestProperty(final String key, final String value) {
    List<AbfsHttpHeader> headers = getRequestHeaders();
    if (headers != null) {
      headers.add(new AbfsHttpHeader(key, value));
    }
  }

  /**{@inheritDoc}*/
  @Override
  Map<String, List<String>> getRequestProperties() {
    Map<String, List<String>> map = new HashMap<>();
    for (AbfsHttpHeader header : getRequestHeaders()) {
      map.put(header.getName(),
          new ArrayList<String>() {{
            add(header.getValue());
          }});
    }
    return map;
  }

  /**{@inheritDoc}*/
  @Override
  public String getResponseHeader(final String headerName) {
    if (httpResponse == null) {
      return null;
    }
    Header header = httpResponse.getFirstHeader(headerName);
    if (header != null) {
      return header.getValue();
    }
    return null;
  }

  /**{@inheritDoc}*/
  @Override
  public Map<String, List<String>> getResponseHeaders() {
    Map<String, List<String>> headers = new HashMap<>();
    if (httpResponse == null) {
      return headers;
    }
    for (Header header : httpResponse.getAllHeaders()) {
      headers.computeIfAbsent(header.getName(), k -> new ArrayList<>())
          .add(header.getValue());
    }
    return headers;
  }

  /**{@inheritDoc}*/
  @Override
  protected InputStream getContentInputStream()
      throws IOException {
    if (httpResponse == null || httpResponse.getEntity() == null) {
      return null;
    }
    return httpResponse.getEntity().getContent();
  }

  /**{@inheritDoc}*/
  @Override
  public void sendPayload(final byte[] buffer,
      final int offset,
      final int length)
      throws IOException {
    if (!isPayloadRequest) {
      return;
    }

    setExpectedBytesToBeSent(length);
    if (buffer != null) {
      HttpEntity httpEntity = new ByteArrayEntity(buffer, offset, length,
          TEXT_PLAIN);
      ((HttpEntityEnclosingRequestBase) httpRequestBase).setEntity(
          httpEntity);
    }

    prepareRequest();
    try {
      LOG.debug("Sending request: {}", httpRequestBase);
      httpResponse = executeRequest();
    } catch (AbfsApacheHttpExpect100Exception ex) {
      LOG.debug(
          "Getting output stream failed with expect header enabled, returning back."
              + "Expect 100 assertion failed for uri {} with status code: {}",
          getMaskedUrl(), parseStatusCode(ex.getHttpResponse()),
          ex);
      setConnectionDisconnectedOnError();
      httpResponse = ex.getHttpResponse();
    } catch (IOException ex) {
      LOG.debug("Getting output stream failed for uri {}, exception: {}",
          getMaskedUrl(), ex);
      throw ex;
    } finally {
      if (httpResponse != null) {
        LOG.debug("Request sent: {}; response {}", httpRequestBase,
            httpResponse);
      }
      if (!isConnectionDisconnectedOnError()
          && httpRequestBase instanceof HttpEntityEnclosingRequestBase) {
        setBytesSent(length);
      }
    }
  }

  /**
   * Sets the header on the request.
   */
  private void prepareRequest() {
    final boolean isEntityBasedRequest
        = httpRequestBase instanceof HttpEntityEnclosingRequestBase;
    for (AbfsHttpHeader header : getRequestHeaders()) {
      if (CONTENT_LENGTH.equals(header.getName()) && isEntityBasedRequest) {
        continue;
      }
      httpRequestBase.setHeader(header.getName(), header.getValue());
    }
  }

  /**{@inheritDoc}*/
  @Override
  public String getRequestProperty(String name) {
    for (AbfsHttpHeader header : getRequestHeaders()) {
      if (header.getName().equals(name)) {
        String val = header.getValue();
        val = val == null ? EMPTY_STRING : val;
        if (EMPTY_STRING.equals(val)) {
          continue;
        }
        return val;
      }
    }
    return EMPTY_STRING;
  }

  /**{@inheritDoc}*/
  @Override
  public String getTracingContextSuffix() {
    return APACHE_IMPL;
  }
}
