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
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.fs.azurebfs.AbfsConfiguration;
import org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants;
import org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AbfsApacheHttpExpect100Exception;
import org.apache.hadoop.security.ssl.DelegatingSSLSocketFactory;
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
import static org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations.X_MS_CLIENT_REQUEST_ID;
import static org.apache.http.entity.ContentType.TEXT_PLAIN;

/**
 * Implementation of {@link HttpOperation} for orchestrating server calls using
 * Apache Http Client.
 */
public class AbfsAHCHttpOperation extends HttpOperation {

  private static final Logger LOG = LoggerFactory.getLogger(
      AbfsAHCHttpOperation.class);

  private static AbfsApacheHttpClient ABFS_APACHE_HTTP_CLIENT;

  private HttpRequestBase httpRequestBase;

  private HttpResponse httpResponse;

  private AbfsManagedHttpContext abfsHttpClientContext;

  private final AbfsRestOperationType abfsRestOperationType;

  private boolean connectionDisconnectedOnError = false;

  private AbfsApacheHttpExpect100Exception abfsApacheHttpExpect100Exception;

  private final boolean isPayloadRequest;

  private List<AbfsHttpHeader> requestHeaders;

  private AbfsAHCHttpOperation(final URL url,
      final String method,
      final List<AbfsHttpHeader> requestHeaders,
      final AbfsRestOperationType abfsRestOperationType) {
    super(LOG, url, method);
    this.abfsRestOperationType = abfsRestOperationType;
    this.requestHeaders = requestHeaders;
    this.isPayloadRequest = isPayloadRequest(method);
  }

  public AbfsAHCHttpOperation(final URL url,
      final String method,
      final List<AbfsHttpHeader> requestHeaders,
      final AbfsConfiguration abfsConfiguration,
      final AbfsRestOperationType abfsRestOperationType) {
    super(LOG, url, method);
    this.abfsRestOperationType = abfsRestOperationType;
    this.requestHeaders = requestHeaders;
    setAbfsApacheHttpClient(abfsConfiguration);
    this.isPayloadRequest = isPayloadRequest(method);
  }

  public AbfsAHCHttpOperation(final URL url,
      final String method,
      final ArrayList<AbfsHttpHeader> requestHeaders,
      final int httpStatus) {
    this(url, method, requestHeaders, null);
    setStatusCode(httpStatus);
  }

  private void setAbfsApacheHttpClient(final AbfsConfiguration abfsConfiguration) {
    if (ABFS_APACHE_HTTP_CLIENT == null) {
      synchronized (this) {
        if (ABFS_APACHE_HTTP_CLIENT == null) {
          ABFS_APACHE_HTTP_CLIENT = new AbfsApacheHttpClient(
              DelegatingSSLSocketFactory.getDefaultFactory(),
              abfsConfiguration);
        }
      }
    }
  }

  @VisibleForTesting
  AbfsManagedHttpContext setFinalAbfsClientContext() {
    return new AbfsManagedHttpContext();
  }

  private boolean isPayloadRequest(final String method) {
    return HTTP_METHOD_PUT.equals(method) || HTTP_METHOD_PATCH.equals(method)
        || HTTP_METHOD_POST.equals(method);
  }


  public static AbfsAHCHttpOperation getAbfsApacheHttpClientHttpOperationWithFixedResult(
      final URL url,
      final String method,
      final int httpStatus) {
    return new AbfsAHCHttpOperation(url, method, new ArrayList<>(), httpStatus);
  }

  @Override
  protected InputStream getErrorStream() throws IOException {
    HttpEntity entity = httpResponse.getEntity();
    if (entity == null) {
      return null;
    }
    return entity.getContent();
  }

  @Override
  String getConnProperty(final String key) {
    for (AbfsHttpHeader header : requestHeaders) {
      if (header.getName().equals(key)) {
        return header.getValue();
      }
    }
    return null;
  }

  @Override
  URL getConnUrl() {
    return getUrl();
  }

  @Override
  String getConnRequestMethod() {
    return getMethod();
  }

  @Override
  Integer getConnResponseCode() throws IOException {
    return getStatusCode();
  }

  @Override
  String getConnResponseMessage() throws IOException {
    return getStatusDescription();
  }

  public void processResponse(final byte[] buffer,
      final int offset,
      final int length) throws IOException {
    try {
      if (!isPayloadRequest) {
        prepareRequest();
        httpResponse = executeRequest();
      }
      parseResponseHeaderAndBody(buffer, offset, length);
    } finally {
      if (httpResponse != null) {
        try {
          EntityUtils.consume(httpResponse.getEntity());
          } finally {
          if(httpResponse instanceof CloseableHttpResponse) {
            ((CloseableHttpResponse) httpResponse).close();
          }
        }
      }
    }
  }

  @VisibleForTesting
  void parseResponseHeaderAndBody(final byte[] buffer,
      final int offset,
      final int length) throws IOException {
    setStatusCode(httpResponse.getStatusLine().getStatusCode());

    setStatusDescription(httpResponse.getStatusLine().getReasonPhrase());

    String requestId = getResponseHeader(
        HttpHeaderConfigurations.X_MS_REQUEST_ID);
    if (requestId == null) {
      requestId = AbfsHttpConstants.EMPTY_STRING;
    }
    setRequestId(requestId);

    // dump the headers
    AbfsIoUtils.dumpHeadersToDebugLog("Response Headers",
        getResponseHeaders(httpResponse));
    parseResponse(buffer, offset, length);
  }

  @VisibleForTesting
  HttpResponse executeRequest() throws IOException {
    abfsHttpClientContext = setFinalAbfsClientContext();
    HttpResponse response = ABFS_APACHE_HTTP_CLIENT.execute(httpRequestBase,
        abfsHttpClientContext);
    setConnectionTimeMs(abfsHttpClientContext.getConnectTime());
    setSendRequestTimeMs(abfsHttpClientContext.getSendTime());
    setRecvResponseTimeMs(abfsHttpClientContext.getReadTime());
    return response;
  }

  private Map<String, List<String>> getResponseHeaders(final HttpResponse httpResponse) {
    if (httpResponse == null || httpResponse.getAllHeaders() == null) {
      return new HashMap<>();
    }
    Map<String, List<String>> map = new HashMap<>();
    for (Header header : httpResponse.getAllHeaders()) {
      map.put(header.getName(), new ArrayList<String>(
          Collections.singleton(header.getValue())));
    }
    return map;
  }

  @Override
  public void setRequestProperty(final String key, final String value) {
    setHeader(key, value);
  }

  @Override
  Map<String, List<String>> getRequestProperties() {
    Map<String, List<String>> map = new HashMap<>();
    for (AbfsHttpHeader header : requestHeaders) {
      map.put(header.getName(),
          new ArrayList<String>() {{
            add(header.getValue());
          }});
    }
    return map;
  }

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

  @Override
  InputStream getContentInputStream()
      throws IOException {
    if(httpResponse == null || httpResponse.getEntity() == null) {
      return null;
    }
    return httpResponse.getEntity().getContent();
  }

  public void sendPayload(final byte[] buffer,
      final int offset,
      final int length)
      throws IOException {
    if (!isPayloadRequest) {
      return;
    }

    if (HTTP_METHOD_PUT.equals(getMethod())) {
      httpRequestBase = new HttpPut(getUri());
    }
    if (HTTP_METHOD_PATCH.equals(getMethod())) {
      httpRequestBase = new HttpPatch(getUri());
    }
    if (HTTP_METHOD_POST.equals(getMethod())) {
      httpRequestBase = new HttpPost(getUri());
    }

    setExpectedBytesToBeSent(length);
    if (buffer != null) {
      HttpEntity httpEntity = new ByteArrayEntity(buffer, offset, length,
          TEXT_PLAIN);
      ((HttpEntityEnclosingRequestBase) httpRequestBase).setEntity(
          httpEntity);
    }

    translateHeaders(httpRequestBase, requestHeaders);
    try {
      httpResponse = executeRequest();
    } catch (AbfsApacheHttpExpect100Exception ex) {
      LOG.debug(
          "Getting output stream failed with expect header enabled, returning back."
              + "Expect 100 assertion failed for uri {} with status code: {}",
          getMaskedUrl(), ex.getHttpResponse().getStatusLine().getStatusCode(),
          ex);
      connectionDisconnectedOnError = true;
      httpResponse = ex.getHttpResponse();
      abfsApacheHttpExpect100Exception = ex;
    } finally {
      if (!connectionDisconnectedOnError
          && httpRequestBase instanceof HttpEntityEnclosingRequestBase) {
        setBytesSent(length);
      }
    }
  }

  private void prepareRequest() throws IOException {
    if (HTTP_METHOD_GET.equals(getMethod())) {
      httpRequestBase = new HttpGet(getUri());
    }
    if (HTTP_METHOD_DELETE.equals(getMethod())) {
      httpRequestBase = new HttpDelete(getUri());
    }
    if (HTTP_METHOD_HEAD.equals(getMethod())) {
      httpRequestBase = new HttpHead(getUri());
    }
    translateHeaders(httpRequestBase, requestHeaders);
  }

  private URI getUri() throws IOException {
    try {
      return getUrl().toURI();
    } catch (URISyntaxException e) {
      throw new IOException(e);
    }
  }

  private void translateHeaders(final HttpRequestBase httpRequestBase,
      final List<AbfsHttpHeader> requestHeaders) {
    for (AbfsHttpHeader header : requestHeaders) {
      httpRequestBase.setHeader(header.getName(), header.getValue());
    }
  }

  public void setHeader(String name, String val) {
    requestHeaders.add(new AbfsHttpHeader(name, val));
  }

  @Override
  public String getRequestProperty(String name) {
    for (AbfsHttpHeader header : requestHeaders) {
      if (header.getName().equals(name)) {
        return header.getValue();
      }
    }
    return "";
  }

  @Override
  boolean getConnectionDisconnectedOnError() {
    return connectionDisconnectedOnError;
  }

  @Override
  public String getTracingContextSuffix() {
    return APACHE_IMPL;
  }

  public String getClientRequestId() {
    for (AbfsHttpHeader header : requestHeaders) {
      if (X_MS_CLIENT_REQUEST_ID.equals(header.getName())) {
        return header.getValue();
      }
    }
    return EMPTY_STRING;
  }
}
