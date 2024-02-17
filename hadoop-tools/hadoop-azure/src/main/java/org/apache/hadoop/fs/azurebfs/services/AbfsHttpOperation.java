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
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.EmptyStackException;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Stack;
import java.util.concurrent.ConcurrentLinkedQueue;

import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLSocketFactory;

import org.apache.hadoop.fs.azurebfs.conn.AbfsHttpUrlConnection;
import org.apache.hadoop.fs.azurebfs.conn.AbfsHttpsUrlConnection;
import org.apache.hadoop.fs.azurebfs.utils.UriUtils;
import org.apache.hadoop.security.ssl.DelegatingSSLSocketFactory;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants;
import org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations;
import org.apache.hadoop.fs.azurebfs.contracts.services.AbfsPerfLoggable;
import org.apache.hadoop.fs.azurebfs.contracts.services.ListResultSchema;

import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.HUNDRED_CONTINUE;
import static org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations.EXPECT;
import static org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations.X_MS_CLIENT_REQUEST_ID;

/**
 * Represents an HTTP operation.
 */
public class AbfsHttpOperation extends HttpOperation {
  private static final Logger LOG = LoggerFactory.getLogger(AbfsHttpOperation.class);

  private HttpURLConnection connection;

  private AbfsRestOperationType operationType;

  private final static Stack<LatencyCaptureInfo> readLatencyInfos = new Stack<>();

  private final static Stack<LatencyCaptureInfo> connLatencyInfos = new Stack<>();

  private static class LatencyCaptureInfo {

    long latencyCapture;

    AbfsRestOperationType operationType;

    int status;
  }

  public void  addConnInfo(AbfsRestOperationType operationType) {
    AbfsHttpsUrlConnection conn = ((AbfsHttpsUrlConnection) connection);
    LatencyCaptureInfo latencyCaptureInfo = new LatencyCaptureInfo();
    latencyCaptureInfo.operationType = operationType;
    latencyCaptureInfo.latencyCapture = conn.timeTaken;
    if(!conn.isFromCache) {
      connLatencyInfos.add(latencyCaptureInfo);
    }
    return;
  }

  private void captureReadLatency() {
    LatencyCaptureInfo latencyCaptureInfo = new LatencyCaptureInfo();
    latencyCaptureInfo.status = statusCode;
    latencyCaptureInfo.latencyCapture = recvResponseTimeMs;
    latencyCaptureInfo.operationType =  operationType;

    readLatencyInfos.add(latencyCaptureInfo);
  }

  public void setOperationType(AbfsRestOperationType operationType) {
    this.operationType = operationType;
  }

  public static AbfsHttpOperation getAbfsHttpOperationWithFixedResult(
      final URL url,
      final String method,
      final int httpStatus) {
    AbfsHttpOperationWithFixedResult httpOp
        = new AbfsHttpOperationWithFixedResult(url, method, httpStatus);
    return httpOp;
  }

  /**
   * Constructor for FixedResult instance, avoiding connection init.
   * @param url request url
   * @param method Http method
   * @param httpStatus HttpStatus
   */
  protected AbfsHttpOperation(final URL url,
      final String method,
      final int httpStatus) {
    super(LOG);
    this.url = url;
    this.method = method;
    this.statusCode = httpStatus;
  }

  protected  HttpURLConnection getConnection() {
    return connection;
  }

  public String getMethod() {
    return method;
  }

  public String getHost() {
    return url.getHost();
  }

  public int getStatusCode() {
    return statusCode;
  }

  public String getStatusDescription() {
    return statusDescription;
  }

  public String getStorageErrorCode() {
    return storageErrorCode;
  }

  public String getStorageErrorMessage() {
    return storageErrorMessage;
  }

  public String getClientRequestId() {
    return this.connection
        .getRequestProperty(HttpHeaderConfigurations.X_MS_CLIENT_REQUEST_ID);
  }

  public String getExpectedAppendPos() {
    return expectedAppendPos;
  }

  public String getRequestId() {
    return requestId;
  }

  public void setMaskForSAS() {
    shouldMask = true;
  }

  public int getBytesSent() {
    return bytesSent;
  }

  public int getExpectedBytesToBeSent() {
    return expectedBytesToBeSent;
  }

  public long getBytesReceived() {
    return bytesReceived;
  }

  public ListResultSchema getListResultSchema() {
    return listResultSchema;
  }

  public String getResponseHeader(String httpHeader) {
    return connection.getHeaderField(httpHeader);
  }

  /**
   * Initializes a new HTTP request and opens the connection.
   *
   * @param url The full URL including query string parameters.
   * @param method The HTTP method (PUT, PATCH, POST, GET, HEAD, or DELETE).
   * @param requestHeaders The HTTP request headers.READ_TIMEOUT
   *
   * @throws IOException if an error occurs.
   */
  public AbfsHttpOperation(final URL url, final String method, final List<AbfsHttpHeader> requestHeaders)
      throws IOException {
    super(LOG);
    this.url = url;
    this.method = method;

    this.connection = openConnection();
//    if (this.connection instanceof HttpsURLConnection) {
//      HttpsURLConnection secureConn = (HttpsURLConnection) this.connection;
//      SSLSocketFactory sslSocketFactory = DelegatingSSLSocketFactory.getDefaultFactory();
//      if (sslSocketFactory != null) {
//        secureConn.setSSLSocketFactory(sslSocketFactory);
//      }
//    }

    this.connection.setConnectTimeout(CONNECT_TIMEOUT);
    this.connection.setReadTimeout(READ_TIMEOUT);

    this.connection.setRequestMethod(method);

    for (AbfsHttpHeader header : requestHeaders) {
      setRequestProperty(header.getName(), header.getValue());
    }
  }

   /**
   * Sends the HTTP request.  Note that HttpUrlConnection requires that an
   * empty buffer be sent in order to set the "Content-Length: 0" header, which
   * is required by our endpoint.
   *
   * @param buffer the request entity body.
   * @param offset an offset into the buffer where the data beings.
   * @param length the length of the data in the buffer.
   *
   * @throws IOException if an error occurs.
   */
  public void sendRequest(byte[] buffer, int offset, int length) throws IOException {
    this.connection.setDoOutput(true);
    this.connection.setFixedLengthStreamingMode(length);
    if (buffer == null) {
      // An empty buffer is sent to set the "Content-Length: 0" header, which
      // is required by our endpoint.
      buffer = new byte[]{};
      offset = 0;
      length = 0;
    }

    // send the request body

    long startTime = 0;
    startTime = System.nanoTime();
    OutputStream outputStream = null;
    // Updates the expected bytes to be sent based on length.
    this.expectedBytesToBeSent = length;
    try {
      try {
        /* Without expect header enabled, if getOutputStream() throws
           an exception, it gets caught by the restOperation. But with
           expect header enabled we return back without throwing an exception
           for the correct response code processing.
         */
        outputStream = getConnOutputStream();
      } catch (IOException e) {
        /* If getOutputStream fails with an exception and expect header
           is enabled, we return back without throwing an exception to
           the caller. The caller is responsible for setting the correct status code.
           If expect header is not enabled, we throw back the exception.
         */
        String expectHeader = getConnProperty(EXPECT);
        if (expectHeader != null && expectHeader.equals(HUNDRED_CONTINUE)) {
          LOG.debug("Getting output stream failed with expect header enabled, returning back ", e);
          return;
        } else {
          LOG.debug("Getting output stream failed without expect header enabled, throwing exception ", e);
          throw e;
        }
      }
      // update bytes sent for successful as well as failed attempts via the
      // accompanying statusCode.
      this.bytesSent = length;

      // If this fails with or without expect header enabled,
      // it throws an IOException.
      outputStream.write(buffer, offset, length);
    } finally {
      // Closing the opened output stream
      if (outputStream != null) {
        outputStream.close();
      }
      this.sendRequestTimeMs = elapsedTimeMs(startTime);
    }
  }

  @Override
  String getRequestProperty(final String headerName) {
    return connection.getRequestProperty(headerName);
  }

  @Override
  Map<String, List<String>> getRequestProperties() {
    return connection.getRequestProperties();
  }

  @Override
  InputStream getContentInputStream() throws IOException {
    return connection.getInputStream();
  }

  /**
   * Gets and processes the HTTP response.
   *
   * @param buffer a buffer to hold the response entity body
   * @param offset an offset in the buffer where the data will being.
   * @param length the number of bytes to be written to the buffer.
   *
   * @throws IOException if an error occurs.
   */
  public void processResponse(final byte[] buffer, final int offset, final int length) throws IOException {

    // get the response
    long startTime = 0;
    startTime = System.nanoTime();

    this.statusCode = getConnResponseCode();
    this.recvResponseTimeMs = elapsedTimeMs(startTime);

    this.statusDescription = getConnResponseMessage();

    this.requestId = this.connection.getHeaderField(HttpHeaderConfigurations.X_MS_REQUEST_ID);
    if (this.requestId == null) {
      this.requestId = AbfsHttpConstants.EMPTY_STRING;
    }
    // dump the headers
    AbfsIoUtils.dumpHeadersToDebugLog("Response Headers",
        connection.getHeaderFields());

    if (AbfsHttpConstants.HTTP_METHOD_HEAD.equals(this.method)) {
      // If it is HEAD, and it is ERROR
      return;
    }

    startTime = System.nanoTime();

    parseResponse(buffer, offset, length);
    captureReadLatency();
  }

  public void setRequestProperty(String key, String value) {
    StringBuilder stringBuilder = new StringBuilder(value);
    if(X_MS_CLIENT_REQUEST_ID.equals(key)) {
      try {
        LatencyCaptureInfo connLatencyCaptureInfo = connLatencyInfos.pop();
        stringBuilder.append(":Conn_").append(
            connLatencyCaptureInfo.operationType).append("_").append(
            connLatencyCaptureInfo.latencyCapture);
      } catch (EmptyStackException ignored) {}
      try {
        LatencyCaptureInfo readCaptureInfo = readLatencyInfos.pop();
        stringBuilder.append(":Read_").append(readCaptureInfo.operationType)
            .append("_").append(readCaptureInfo.latencyCapture).append("_")
            .append(readCaptureInfo.status);
      } catch (EmptyStackException ex) {}
      try {
        AbfsHttpsUrlConnection.AbfsHttpClient.finishedStack.pop();
        stringBuilder.append(":FinsihedConn");
      } catch (EmptyStackException ex) {}
    }
    this.connection.setRequestProperty(key, stringBuilder.toString());
  }

  /**
   * Open the HTTP connection.
   *
   * @throws IOException if an error occurs.
   */
  private HttpURLConnection openConnection() throws IOException {
    if("https".equals(url.getProtocol())) {
      AbfsHttpsUrlConnection conn = new AbfsHttpsUrlConnection(url, null, new sun.net.www.protocol.https.Handler());
      SSLSocketFactory sslSocketFactory = DelegatingSSLSocketFactory.getDefaultFactory();
      if (sslSocketFactory != null) {
        conn.setSSLSocketFactory(sslSocketFactory);
      }
      return conn;
    }

    return  new AbfsHttpUrlConnection(url, null, new sun.net.www.protocol.http.Handler());
//    if (!isTraceEnabled) {
//      return (HttpURLConnection) url.openConnection();
//    }
//    long start = System.nanoTime();
//    try {
//      return (HttpURLConnection) url.openConnection();
//    } finally {
//      connectionTimeMs = elapsedTimeMs(start);
//    }
  }

  @Override
  protected InputStream getErrorStream() {
    return connection.getErrorStream();
  }

  /**
   * Gets the connection request property for a key.
   * @param key The request property key.
   * @return request peoperty value.
   */
  String getConnProperty(String key) {
    return connection.getRequestProperty(key);
  }

  /**
   * Gets the connection url.
   * @return url.
   */
  URL getConnUrl() {
    return connection.getURL();
  }

  /**
   * Gets the connection request method.
   * @return request method.
   */
  String getConnRequestMethod() {
    return connection.getRequestMethod();
  }

  /**
   * Gets the connection response code.
   * @return response code.
   * @throws IOException
   */
  Integer getConnResponseCode() throws IOException {
    return connection.getResponseCode();
  }

  /**
   * Gets the connection output stream.
   * @return output stream.
   * @throws IOException
   */
  OutputStream getConnOutputStream() throws IOException {
    return connection.getOutputStream();
  }

  /**
   * Gets the connection response message.
   * @return response message.
   * @throws IOException
   */
  String getConnResponseMessage() throws IOException {
    return connection.getResponseMessage();
  }

  public static class AbfsHttpOperationWithFixedResult extends AbfsHttpOperation {
    /**
     * Creates an instance to represent fixed results.
     * This is used in idempotency handling.
     *
     * @param url The full URL including query string parameters.
     * @param method The HTTP method (PUT, PATCH, POST, GET, HEAD, or DELETE).
     * @param httpStatus StatusCode to hard set
     */
    public AbfsHttpOperationWithFixedResult(final URL url,
        final String method,
        final int httpStatus) {
      super(url, method, httpStatus);
    }

    @Override
    public String getResponseHeader(final String httpHeader) {
      return "";
    }
  }
}
