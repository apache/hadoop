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
import java.net.ProtocolException;
import java.net.URL;
import java.time.Duration;
import java.util.List;
import java.util.Map;

import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLSocketFactory;

import org.apache.hadoop.security.ssl.DelegatingSSLSocketFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants;

import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.EXPECT_100_JDK_ERROR;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.HUNDRED_CONTINUE;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.JDK_FALLBACK;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.JDK_IMPL;
import static org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations.EXPECT;

/**
 * Implementation of {@link AbfsHttpOperation} for orchestrating calls using JDK's HttpURLConnection.
 */
public class AbfsJdkHttpOperation extends AbfsHttpOperation {

  private static final Logger LOG = LoggerFactory.getLogger(
      AbfsJdkHttpOperation.class);

  private final HttpURLConnection connection;

  /**
   * Initializes a new HTTP request and opens the connection.
   *
   * @param url The full URL including query string parameters.
   * @param method The HTTP method (PUT, PATCH, POST, GET, HEAD, or DELETE).
   * @param requestHeaders The HTTP request headers.READ_TIMEOUT
   * @param connectionTimeout The Connection Timeout value to be used while establishing http connection
   * @param readTimeout The Read Timeout value to be used with http connection while making a request
   * @param abfsClient The AbfsClient instance.
   * @throws IOException if an error occurs.
   */
  public AbfsJdkHttpOperation(final URL url,
      final String method,
      final List<AbfsHttpHeader> requestHeaders,
      final Duration connectionTimeout,
      final Duration readTimeout,
      final AbfsClient abfsClient)
      throws IOException {
    super(LOG, url, method, requestHeaders, connectionTimeout, readTimeout, abfsClient);

    this.connection = openConnection();
    if (this.connection instanceof HttpsURLConnection) {
      HttpsURLConnection secureConn = (HttpsURLConnection) this.connection;
      SSLSocketFactory sslSocketFactory
          = DelegatingSSLSocketFactory.getDefaultFactory();
      if (sslSocketFactory != null) {
        secureConn.setSSLSocketFactory(sslSocketFactory);
      }
    }

    this.connection.setConnectTimeout(getConnectionTimeout());
    this.connection.setReadTimeout(getReadTimeout());
    this.connection.setRequestMethod(method);

    for (AbfsHttpHeader header : requestHeaders) {
      setRequestProperty(header.getName(), header.getValue());
    }
  }

  /**{@inheritDoc}*/
  public String getResponseHeader(String httpHeader) {
    return connection.getHeaderField(httpHeader);
  }

  /**{@inheritDoc}*/
  @Override
  public Map<String, List<String>> getResponseHeaders() {
    return connection.getHeaderFields();
  }

  /**{@inheritDoc}*/
  @Override
  public String getResponseHeaderIgnoreCase(final String headerName) {
    Map<String, List<String>> responseHeaders = getResponseHeaders();
    if (responseHeaders == null || responseHeaders.isEmpty()) {
      return null;
    }
    // Search for the header value case-insensitively
    return responseHeaders.entrySet().stream()
        .filter(entry -> entry.getKey() != null
            && entry.getKey().equalsIgnoreCase(headerName))
        .flatMap(entry -> entry.getValue().stream())
        .findFirst()
        .orElse(null); // Return null if no match is found
  }

  /**{@inheritDoc}*/
  public void sendPayload(byte[] buffer, int offset, int length)
      throws IOException {
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
    setExpectedBytesToBeSent(length);
    try {
      try {
        /* Without expect header enabled, if getOutputStream() throws
           an exception, it gets caught by the restOperation. But with
           expect header enabled we return back without throwing an exception
           for the correct response code processing.
         */
        outputStream = getConnOutputStream();
      } catch (IOException e) {
        setConnectionDisconnectedOnError();
        /* If getOutputStream fails with an expect-100 exception , we return back
           without throwing an exception to the caller. Else, we throw back the exception.
         */
        String expectHeader = getConnProperty(EXPECT);
        if (expectHeader != null && expectHeader.equals(HUNDRED_CONTINUE)
            && e instanceof ProtocolException
            && EXPECT_100_JDK_ERROR.equals(e.getMessage())) {
          LOG.debug(
              "Getting output stream failed with expect header enabled, returning back ",
              e);
          /*
           * In case expect-100 assertion has failed, headers and inputStream should not
           * be parsed. Reason being, conn.getHeaderField(), conn.getHeaderFields(),
           * conn.getInputStream() will lead to repeated server call.
           * ref: https://bugs.openjdk.org/browse/JDK-8314978.
           * Reading conn.responseCode() and conn.getResponseMessage() is safe in
           * case of Expect-100 error. Reason being, in JDK, it stores the responseCode
           * in the HttpUrlConnection object before throwing exception to the caller.
           */
          setStatusCode(getConnResponseCode());
          setStatusDescription(getConnResponseMessage());
          return;
        } else {
          LOG.debug(
              "Getting output stream failed without expect header enabled, throwing exception ",
              e);
          throw e;
        }
      }
      // update bytes sent for successful as well as failed attempts via the
      // accompanying statusCode.
      setBytesSent(length);

      // If this fails with or without expect header enabled,
      // it throws an IOException.
      outputStream.write(buffer, offset, length);
    } finally {
      // Closing the opened output stream
      if (outputStream != null) {
        outputStream.close();
      }
      setSendRequestTimeMs(elapsedTimeMs(startTime));
    }
  }

  /**{@inheritDoc}*/
  @Override
  String getRequestProperty(final String headerName) {
    return connection.getRequestProperty(headerName);
  }

  /**{@inheritDoc}*/
  @Override
  Map<String, List<String>> getRequestProperties() {
    return connection.getRequestProperties();
  }

  /**{@inheritDoc}*/
  @Override
  protected InputStream getContentInputStream() throws IOException {
    return connection.getInputStream();
  }

  /**{@inheritDoc}*/
  public void processResponse(final byte[] buffer,
      final int offset,
      final int length) throws IOException {
    if (isConnectionDisconnectedOnError()) {
      LOG.debug("This connection was not successful or has been disconnected, "
          + "hence not parsing headers and inputStream");
      return;
    }
    processConnHeadersAndInputStreams(buffer, offset, length);
  }

  /**
   * Parses headers and body of the response. Execute server call if {@link #sendPayload(byte[], int, int)}
   * is not called.
   *
   * @param buffer buffer to store the response body.
   * @param offset offset in the buffer.
   * @param length length of the response body.
   *
   * @throws IOException network error or parsing error.
   */
  void processConnHeadersAndInputStreams(final byte[] buffer,
      final int offset,
      final int length) throws IOException {
    // get the response
    long startTime = 0;
    startTime = System.nanoTime();

    setStatusCode(getConnResponseCode());
    setRecvResponseTimeMs(elapsedTimeMs(startTime));

    setStatusDescription(getConnResponseMessage());
    setRequestId();

    // dump the headers
    AbfsIoUtils.dumpHeadersToDebugLog("Response Headers",
        connection.getHeaderFields());

    if (AbfsHttpConstants.HTTP_METHOD_HEAD.equals(getMethod())) {
      // If it is HEAD, and it is ERROR
      return;
    }

    parseResponse(buffer, offset, length);
  }

  /**{@inheritDoc}*/
  public void setRequestProperty(String key, String value) {
    this.connection.setRequestProperty(key, value);
  }

  /**
   * Creates a new {@link HttpURLConnection} instance. This instance is not connected.
   * Any API call on the instance would make it reuse an existing connection or
   * establish a new connection.
   *
   * @throws IOException if an error occurs.
   */
  private HttpURLConnection openConnection() throws IOException {
    long start = System.nanoTime();
    try {
      return (HttpURLConnection) getUrl().openConnection();
    } finally {
      setConnectionTimeMs(elapsedTimeMs(start));
    }
  }

  /**{@inheritDoc}*/
  @Override
  protected InputStream getErrorStream() {
    return connection.getErrorStream();
  }

  /**{@inheritDoc}*/
  String getConnProperty(String key) {
    return connection.getRequestProperty(key);
  }

  /**{@inheritDoc}*/
  URL getConnUrl() {
    return connection.getURL();
  }

  /**{@inheritDoc}*/
  Integer getConnResponseCode() throws IOException {
    return connection.getResponseCode();
  }

  /**
   * Gets the connection output stream.
   * @return output stream.
   * @throws IOException if creating outputStream on connection failed
   */
  OutputStream getConnOutputStream() throws IOException {
    return connection.getOutputStream();
  }

  /**{@inheritDoc}*/
  String getConnResponseMessage() throws IOException {
    return connection.getResponseMessage();
  }

  /**{@inheritDoc}*/
  @Override
  public String getTracingContextSuffix() {
    return AbfsApacheHttpClient.usable() ? JDK_IMPL : JDK_FALLBACK;
  }
}
