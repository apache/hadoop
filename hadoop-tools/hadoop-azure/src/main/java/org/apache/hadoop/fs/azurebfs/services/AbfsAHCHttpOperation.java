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
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.fs.azurebfs.AbfsConfiguration;
import org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants;
import org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AbfsApacheHttpExpect100Exception;
import org.apache.hadoop.security.ssl.DelegatingSSLSocketFactory;
import org.apache.http.Header;
import org.apache.http.HttpClientConnection;
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

import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.HTTP_METHOD_DELETE;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.HTTP_METHOD_GET;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.HTTP_METHOD_HEAD;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.HTTP_METHOD_PATCH;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.HTTP_METHOD_POST;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.HTTP_METHOD_PUT;
import static org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations.X_MS_CLIENT_REQUEST_ID;
import static org.apache.http.entity.ContentType.TEXT_PLAIN;

//javadocs with discription
/**
 * AbfsAHCHttpOperation.
 * <p>
 * This class is used to perform the HTTP operations using Apache Http Client.
 * </p>
 */
public class AbfsAHCHttpOperation extends HttpOperation {

  private static final Logger LOG = LoggerFactory.getLogger(AbfsAHCHttpOperation.class);

  /**
   * Map to store the AbfsApacheHttpClient. Each instance of AbfsClient to have
   * a unique AbfsApacheHttpClient instance. The key of the map is the UUID of the client.
   */
  private static final Map<String, AbfsApacheHttpClient> abfsApacheHttpClientMap = new HashMap<>();

  private AbfsApacheHttpClient abfsApacheHttpClient;

  public HttpRequestBase httpRequestBase;

  private HttpResponse httpResponse;

  private final AbfsApacheHttpClient.AbfsHttpClientContext abfsHttpClientContext;

  private final AbfsRestOperationType abfsRestOperationType;

  private boolean connectionDisconnectedOnError = false;

  private void setAbfsApacheHttpClient(final AbfsConfiguration abfsConfiguration,
      final String clientId) {
    AbfsApacheHttpClient client = abfsApacheHttpClientMap.get(clientId);
    if (client == null) {
      synchronized (abfsApacheHttpClientMap) {
        client = abfsApacheHttpClientMap.get(clientId);
        if (client == null) {
          client = new AbfsApacheHttpClient(
              DelegatingSSLSocketFactory.getDefaultFactory(),
              abfsConfiguration);
          abfsApacheHttpClientMap.put(clientId, client);
        }
      }
    }
    abfsApacheHttpClient = client;
  }

  static void removeClient(final String clientId) throws IOException {
    AbfsApacheHttpClient client = abfsApacheHttpClientMap.remove(clientId);
    if(client != null) {
      client.close();
    }
  }

  private AbfsAHCHttpOperation(final URL url, final String method, final List<AbfsHttpHeader> requestHeaders,
      final AbfsRestOperationType abfsRestOperationType) {
    super(LOG);
    this.abfsRestOperationType = abfsRestOperationType;
    this.url = url;
    this.method = method;
    this.requestHeaders = requestHeaders;
    abfsHttpClientContext = setFinalAbfsClientContext(method);
  }

  private AbfsApacheHttpClient.AbfsHttpClientContext setFinalAbfsClientContext(
      final String method) {
    final AbfsApacheHttpClient.AbfsHttpClientContext abfsHttpClientContext;
    if(HTTP_METHOD_GET.equals(method)) {
      abfsHttpClientContext = new AbfsApacheHttpClient.AbfsHttpClientContext(true, abfsRestOperationType);
    } else {
      abfsHttpClientContext = new AbfsApacheHttpClient.AbfsHttpClientContext(false, abfsRestOperationType);
    }
    return abfsHttpClientContext;
  }

  public AbfsAHCHttpOperation(final URL url,
      final String method,
      final List<AbfsHttpHeader> requestHeaders,
      final AbfsConfiguration abfsConfiguration,
      final String clientId, final AbfsRestOperationType abfsRestOperationType) {
    super(LOG);
    this.abfsRestOperationType = abfsRestOperationType;
    this.method = method;
    this.url = url;
    this.requestHeaders = requestHeaders;
    setAbfsApacheHttpClient(abfsConfiguration, clientId);
    abfsHttpClientContext = setFinalAbfsClientContext(method);
  }


  public static AbfsAHCHttpOperation getAbfsApacheHttpClientHttpOperationWithFixedResult(
      final URL url,
      final String method,
      final int httpStatus) {
    AbfsAHCHttpOperation abfsApacheHttpClientHttpOperation = new AbfsAHCHttpOperation(url, method, new ArrayList<>(), null);
    abfsApacheHttpClientHttpOperation.statusCode = httpStatus;
    return abfsApacheHttpClientHttpOperation;
  }

  @Override
  protected InputStream getErrorStream() throws IOException {
    HttpEntity entity = httpResponse.getEntity();
    if(entity == null) {
      return null;
    }
    return entity.getContent();
  }

  @Override
  String getConnProperty(final String key) {
    for(AbfsHttpHeader header : requestHeaders) {
      if(header.getName().equals(key)) {
        return header.getValue();
      }
    }
    return null;
  }

  @Override
  URL getConnUrl() {
    return url;
  }

  @Override
  String getConnRequestMethod() {
    return method;
  }

  @Override
  Integer getConnResponseCode() throws IOException {
    return statusCode;
  }

  @Override
  String getConnResponseMessage() throws IOException {
    return statusDescription;
  }

  public void processResponse(final byte[] buffer,
      final int offset,
      final int length) throws IOException {
    try {
      try {
        httpResponse = abfsApacheHttpClient.execute(httpRequestBase, abfsHttpClientContext);
        sendRequestTimeMs = abfsHttpClientContext.sendTime;
        recvResponseTimeMs = abfsHttpClientContext.readTime;
      } catch (AbfsApacheHttpExpect100Exception ex) {
        LOG.debug(
            "Getting output stream failed with expect header enabled, returning back ",
            ex);
        connectionDisconnectedOnError = true;
        httpResponse = ex.getHttpResponse();
        throw ex;
      }

      this.statusCode = httpResponse.getStatusLine().getStatusCode();

      this.statusDescription = httpResponse.getStatusLine().getReasonPhrase();

      this.requestId = getResponseHeader(HttpHeaderConfigurations.X_MS_REQUEST_ID);
      if (this.requestId == null) {
        this.requestId = AbfsHttpConstants.EMPTY_STRING;
      }
      // dump the headers
      AbfsIoUtils.dumpHeadersToDebugLog("Response Headers",
          getResponseHeaders(httpResponse));
      parseResponse(buffer, offset, length);
      abfsHttpClientContext.isBeingRead = false;
    } finally {
      if(httpResponse != null) {
        EntityUtils.consume(httpResponse.getEntity());
      }
      if(httpResponse != null && httpResponse instanceof CloseableHttpResponse) {
        ((CloseableHttpResponse) httpResponse).close();
      }
    }
  }

  private Map<String, List<String>> getResponseHeaders(final HttpResponse httpResponse) {
    if(httpResponse == null || httpResponse.getAllHeaders() == null) {
      return new HashMap<>();
    }
    Map<String, List<String>> map = new HashMap<>();
    for(Header header : httpResponse.getAllHeaders()) {
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
    for(AbfsHttpHeader header : requestHeaders) {
      map.put(header.getName(), new ArrayList<String>(){{add(header.getValue());}});
    }
    return map;
  }

  @Override
  public String getResponseHeader(final String headerName) {
    Header header = httpResponse.getFirstHeader(headerName);
    if(header != null) {
      return header.getValue();
    }
    return null;
  }

  @Override
  InputStream getContentInputStream()
      throws IOException {
    if(httpResponse == null) {
      return null;
    }
    HttpEntity entity = httpResponse.getEntity();
    if(entity != null) {
      return httpResponse.getEntity().getContent();
    }
    return null;
  }

  public void sendRequest(final byte[] buffer,
      final int offset,
      final int length)
      throws IOException {
    try {
      HttpRequestBase httpRequestBase = null;
      if (HTTP_METHOD_PUT.equals(method)) {
        httpRequestBase = new HttpPut(url.toURI());
      }
      if(HTTP_METHOD_PATCH.equals(method)) {
        httpRequestBase = new HttpPatch(url.toURI());
      }
      if(HTTP_METHOD_POST.equals(method)) {
        httpRequestBase = new HttpPost(url.toURI());
      }
      if(httpRequestBase != null) {

        this.expectedBytesToBeSent = length;
        this.bytesSent = length;
        if(buffer != null) {
          HttpEntity httpEntity = new ByteArrayEntity(buffer, offset, length,
              TEXT_PLAIN);
          ((HttpEntityEnclosingRequestBase)httpRequestBase).setEntity(httpEntity);
        }
      } else {
        if(HTTP_METHOD_GET.equals(method)) {
          httpRequestBase = new HttpGet(url.toURI());
        }
        if(HTTP_METHOD_DELETE.equals(method)) {
          httpRequestBase = new HttpDelete((url.toURI()));
        }
        if(HTTP_METHOD_HEAD.equals(method)) {
          httpRequestBase = new HttpHead(url.toURI());
        }
      }
      translateHeaders(httpRequestBase, requestHeaders);
      this.httpRequestBase = httpRequestBase;
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  private void translateHeaders(final HttpRequestBase httpRequestBase, final List<AbfsHttpHeader> requestHeaders) {
    for(AbfsHttpHeader header : requestHeaders) {
      httpRequestBase.setHeader(header.getName(), header.getValue());
    }
  }

  public void setHeader(String name, String val) {
    requestHeaders.add(new AbfsHttpHeader(name, val));
  }

  @Override
  public String getRequestProperty(String name) {
    for(AbfsHttpHeader header : requestHeaders) {
      if(header.getName().equals(name)) {
        return header.getValue();
      }
    }
    return "";
  }

  @Override
  boolean getConnectionDisconnectedOnError() {
    return connectionDisconnectedOnError;
  }

  public String getClientRequestId() {
    for(AbfsHttpHeader header : requestHeaders) {
      if(X_MS_CLIENT_REQUEST_ID.equals(header.getName())) {
        return header.getValue();
      }
    }
    return "";
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder();
    sb.append(statusCode);
    sb.append(",");
    sb.append(storageErrorCode);
    sb.append(",");
    sb.append(expectedAppendPos);
    sb.append(",cid=");
    sb.append(getClientRequestId());
    sb.append(",rid=");
    sb.append(requestId);
    sb.append(",connMs=");
    sb.append(connectionTimeMs);
    sb.append(",sendMs=");
    sb.append(sendRequestTimeMs);
    sb.append(",recvMs=");
    sb.append(recvResponseTimeMs);
    sb.append(",sent=");
    sb.append(bytesSent);
    sb.append(",recv=");
    sb.append(bytesReceived);
    sb.append(",");
    sb.append(method);
    sb.append(",");
    sb.append(getMaskedUrl());
    return sb.toString();
  }

  @Override
  public void incrementServerCall() {
    ApacheHttpClientHealthMonitor.incrementServerCalls();
  }

  @Override
  public void registerIOException() {
    ApacheHttpClientHealthMonitor.incrementUnknownIoExceptions();
  }
}
