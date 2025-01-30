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
import java.net.HttpURLConnection;
import java.net.URL;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants;
import org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations;
import org.apache.hadoop.fs.azurebfs.contracts.services.AbfsPerfLoggable;
import org.apache.hadoop.fs.azurebfs.contracts.services.ListResultSchema;
import org.apache.hadoop.fs.azurebfs.contracts.services.StorageErrorResponseSchema;
import org.apache.hadoop.fs.azurebfs.utils.UriUtils;

import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.BLOCKLIST;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.EMPTY_STRING;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.EQUAL;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.TRUE;
import static org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations.X_MS_META_HDI_ISFOLDER;
import static org.apache.hadoop.fs.azurebfs.constants.HttpQueryParams.QUERY_PARAM_COMP;

/**
 * Base Http operation class for orchestrating server IO calls. Child classes would
 * define the certain orchestration implementation on the basis of network library used.
 * <p>
 * For JDK netlib usage, the child class would be {@link AbfsJdkHttpOperation}. <br>
 * For ApacheHttpClient netlib usage, the child class would be {@link AbfsAHCHttpOperation}.
 */
public abstract class AbfsHttpOperation implements AbfsPerfLoggable {

  private final Logger log;

  private static final int CLEAN_UP_BUFFER_SIZE = 64 * 1024;

  private static final int ONE_THOUSAND = 1000;

  private static final int ONE_MILLION = ONE_THOUSAND * ONE_THOUSAND;

  private final String method;
  private final URL url;
  private String maskedUrl;
  private AbfsClient client;
  private String maskedEncodedUrl;
  private int statusCode;
  private String statusDescription;
  private String storageErrorCode = "";
  private String storageErrorMessage = "";
  private String requestId = "";
  private String expectedAppendPos = "";
  private ListResultSchema listResultSchema = null;
  private List<String> blockIdList = null;

  // metrics
  private int bytesSent;
  private int expectedBytesToBeSent;
  private long bytesReceived;

  private long connectionTimeMs;
  private long sendRequestTimeMs;
  private long recvResponseTimeMs;
  private boolean shouldMask = false;
  private boolean connectionDisconnectedOnError = false;

  /**Request headers to be sent in the request.*/
  private final List<AbfsHttpHeader> requestHeaders;

  /**
   * Timeout that defines maximum allowed connection establishment time for a request.
   * Timeout is in milliseconds. Not all requests need to establish a new connection,
   * it depends on the connection pooling-heuristic of the networking library.
   */
  private final int connectionTimeout;

  /**
   * Timeout in milliseconds that defines maximum allowed time to read the response.
   * This timeout starts once request is sent. It includes server reponse time,
   * network latency, and time to read the response.
   */
  private final int readTimeout;

  public static AbfsHttpOperation getAbfsHttpOperationWithFixedResult(
      final URL url,
      final String method,
      final int httpStatus) {
    AbfsHttpOperationWithFixedResult httpOp
        = new AbfsHttpOperationWithFixedResult(url, method, httpStatus);
    return httpOp;
  }

  public AbfsHttpOperation(
      final Logger log,
      final URL url,
      final String method,
      final List<AbfsHttpHeader> requestHeaders,
      final Duration connectionTimeout,
      final Duration readTimeout, AbfsClient abfsClient) {
    this.log = log;
    this.url = url;
    this.method = method;
    this.requestHeaders = requestHeaders;
    this.connectionTimeout = (int) connectionTimeout.toMillis();
    this.readTimeout = (int) readTimeout.toMillis();
    this.client = abfsClient;
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
    this.log = LoggerFactory.getLogger(AbfsHttpOperation.class);
    this.url = url;
    this.method = method;
    this.statusCode = httpStatus;
    this.requestHeaders = new ArrayList<>();
    this.connectionTimeout = 0;
    this.readTimeout = 0;
  }

  int getConnectionTimeout() {
    return connectionTimeout;
  }

  int getReadTimeout() {
    return readTimeout;
  }

  List<AbfsHttpHeader> getRequestHeaders() {
    return requestHeaders;
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
    return getRequestProperty(HttpHeaderConfigurations.X_MS_CLIENT_REQUEST_ID);
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

  public URL getUrl() {
    return url;
  }

  public ListResultSchema getListResultSchema() {
    return listResultSchema;
  }

  /**
   * Get response header value for the given headerKey.
   *
   * @param httpHeader header key.
   * @return header value.
   */
  public abstract String getResponseHeader(String httpHeader);

  public abstract Map<String, List<String>> getResponseHeaders();

  // Returns a trace message for the request
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

  // Returns a trace message for the ABFS API logging service to consume
  public String getLogString() {

    final StringBuilder sb = new StringBuilder();
    sb.append("s=")
      .append(statusCode)
      .append(" e=")
      .append(storageErrorCode)
      .append(" ci=")
      .append(getClientRequestId())
      .append(" ri=")
      .append(requestId)

      .append(" ct=")
      .append(connectionTimeMs)
      .append(" st=")
      .append(sendRequestTimeMs)
      .append(" rt=")
      .append(recvResponseTimeMs)

      .append(" bs=")
      .append(bytesSent)
      .append(" br=")
      .append(bytesReceived)
      .append(" m=")
      .append(method)
      .append(" u=")
      .append(getMaskedEncodedUrl());

    return sb.toString();
  }

  @VisibleForTesting
  public String getMaskedUrl() {
    if (!shouldMask) {
      return url.toString();
    }
    if (maskedUrl != null) {
      return maskedUrl;
    }
    maskedUrl = UriUtils.getMaskedUrl(url);
    return maskedUrl;
  }

  public final String getMaskedEncodedUrl() {
    if (maskedEncodedUrl != null) {
      return maskedEncodedUrl;
    }
    maskedEncodedUrl = UriUtils.encodedUrlStr(getMaskedUrl());
    return maskedEncodedUrl;
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

  public abstract void sendPayload(byte[] buffer, int offset, int length) throws
      IOException;

  /**
   * Gets and processes the HTTP response.
   *
   * @param buffer a buffer to hold the response entity body
   * @param offset an offset in the buffer where the data will being.
   * @param length the number of bytes to be written to the buffer.
   *
   * @throws IOException if an error occurs.
   */
  public abstract void processResponse(byte[] buffer,
      int offset,
      int length) throws IOException;

  /**
   * Set request header.
   *
   * @param key header key.
   * @param value header value.
   */
  public abstract void setRequestProperty(String key, String value);

  /**
   * Parse response body from the connection.
   *
   * @param buffer byte array to store the response body.
   * @param offset offset in the buffer.
   * @param length length of the response body.
   *
   * @throws IOException if network error occurs while reading the response.
   */
  final void parseResponse(final byte[] buffer,
      final int offset,
      final int length) throws IOException {
    long startTime;
    if (AbfsHttpConstants.HTTP_METHOD_HEAD.equals(this.method)) {
      // If it is HEAD, and it is ERROR
      return;
    }

    startTime = System.nanoTime();

    if (statusCode >= HttpURLConnection.HTTP_BAD_REQUEST) {
      processStorageErrorResponse();
      this.recvResponseTimeMs += elapsedTimeMs(startTime);
      String contentLength = getResponseHeader(
          HttpHeaderConfigurations.CONTENT_LENGTH);
      if (contentLength != null) {
        this.bytesReceived = Long.parseLong(contentLength);
      } else {
        this.bytesReceived = 0L;
      }

    } else {
      // consume the input stream to release resources
      int totalBytesRead = 0;

      try (InputStream stream = getContentInputStream()) {
        if (isNullInputStream(stream)) {
          return;
        }
        boolean endOfStream = false;

        // this is a list operation and need to retrieve the data
        // need a better solution
        if (AbfsHttpConstants.HTTP_METHOD_GET.equals(this.method) && buffer == null) {
          if (url.toString().contains(QUERY_PARAM_COMP + EQUAL + BLOCKLIST)) {
            parseBlockListResponse(stream);
          } else {
            parseListFilesResponse(stream);
          }
        } else {
          if (buffer != null) {
            while (totalBytesRead < length) {
              int bytesRead = stream.read(buffer, offset + totalBytesRead,
                  length
                      - totalBytesRead);
              if (bytesRead == -1) {
                endOfStream = true;
                break;
              }
              totalBytesRead += bytesRead;
            }
          }
          if (!endOfStream && stream.read() != -1) {
            // read and discard
            int bytesRead = 0;
            byte[] b = new byte[CLEAN_UP_BUFFER_SIZE];
            while ((bytesRead = stream.read(b)) >= 0) {
              totalBytesRead += bytesRead;
            }
          }
        }
      } catch (IOException ex) {
        log.warn("IO/Network error: {} {}: {}",
            method, getMaskedUrl(), ex.getMessage());
        log.debug("IO Error: ", ex);
        throw ex;
      } finally {
        this.recvResponseTimeMs += elapsedTimeMs(startTime);
        this.bytesReceived = totalBytesRead;
      }
    }
  }

  /**
   * Get the response stream from the connection.
   * @return InputStream: response stream from the connection after network call.
   * @throws IOException if the response stream could not be created from the connection.
   */
  protected abstract InputStream getContentInputStream() throws IOException;

  /**
   * When the request fails, this function is used to parse the responseAbfsHttpClient.LOG.debug("ExpectedError: ", ex);
   * and extract the storageErrorCode and storageErrorMessage.  Any errors
   * encountered while attempting to process the error response are logged,
   * but otherwise ignored.
   *
   * For storage errors, the response body *usually* has the following format:
   *
   * {
   *   "error":
   *   {
   *     "code": "string",
   *     "message": "string"
   *   }
   * }
   *
   */
  private void processStorageErrorResponse() {
    try (InputStream stream = getErrorStream()) {
      if (stream == null) {
        return;
      }
      StorageErrorResponseSchema storageErrorResponse = client.processStorageErrorResponse(stream);
      storageErrorCode = storageErrorResponse.getStorageErrorCode();
      storageErrorMessage = storageErrorResponse.getStorageErrorMessage();
      expectedAppendPos = storageErrorResponse.getExpectedAppendPos();
    } catch (IOException ex) {
      // Ignore errors that occur while attempting to parse the storage
      // error, since the response may have been handled by the HTTP driver
      // or for other reasons have an unexpected
      log.debug("Error parsing storage error response", ex);
    }
  }

  /**
   * Get the error stream from the connection.
   * @return InputStream
   * @throws IOException if the error stream could not be created from the response stream.
   */
  protected abstract InputStream getErrorStream() throws IOException;

  /**
   * Parse the list file response
   *
   * @param stream InputStream contains the list results.
   * @throws IOException if the response cannot be deserialized.
   */
  private void parseListFilesResponse(final InputStream stream) throws IOException {
    if (stream == null || listResultSchema != null) {
      return;
    }
    listResultSchema = client.parseListPathResults(stream);
  }

  private void parseBlockListResponse(final InputStream stream) throws IOException {
    if (stream == null || blockIdList != null) {
      return;
    }
    blockIdList = client.parseBlockListResponse(stream);
  }

  public List<String> getBlockIdList() {
    return blockIdList;
  }

  /**
   * Returns the elapsed time in milliseconds.
   */
  final long elapsedTimeMs(final long startTime) {
    return (System.nanoTime() - startTime) / ONE_MILLION;
  }

  /**
   * Check null stream, this is to pass findbugs's redundant check for NULL
   * @param stream InputStream
   */
  private boolean isNullInputStream(InputStream stream) {
    return stream == null ? true : false;
  }

  /**
   * Gets the connection request property for a key.
   * @param key The request property key.
   * @return request peoperty value.
   */
  abstract String getConnProperty(String key);

  /**
   * Gets the connection url.
   * @return url.
   */
  abstract URL getConnUrl();

  /**
   * Gets the connection response code.
   * @return response code.
   * @throws IOException
   */
  abstract Integer getConnResponseCode() throws IOException;


  /**
   * Gets the connection response message.
   * @return response message.
   * @throws IOException
   */
  abstract String getConnResponseMessage() throws IOException;

  /**
   * Get request headers.
   *
   * @return request headers.
   */
  abstract Map<String, List<String>> getRequestProperties();

  /**
   * Get request header value for a header name.
   *
   * @param headerName header name.
   * @return header value.
   */
  abstract String getRequestProperty(String headerName);

  boolean getConnectionDisconnectedOnError() {
    return connectionDisconnectedOnError;
  }

  /**
   * Get the suffix to add to the tracing context that defines what http-client is
   * used to make the network call
   * @return the suffix to distinguish http client
   */
  public abstract String getTracingContextSuffix();

  public final long getSendLatency() {
    return sendRequestTimeMs;
  }

  public final long getRecvLatency() {
    return recvResponseTimeMs;
  }

  /**
   * Set response status code for the server call.
   *
   * @param statusCode status code.
   */
  protected void setStatusCode(final int statusCode) {
    this.statusCode = statusCode;
  }

  /**
   * Sets response status description for the server call.
   *
   * @param statusDescription status description.
   */
  protected void setStatusDescription(final String statusDescription) {
    this.statusDescription = statusDescription;
  }

  /**
   * Set x-ms-request-id value from the server call response header.
   */
  protected void setRequestId() {
    requestId = getResponseHeader(
        HttpHeaderConfigurations.X_MS_REQUEST_ID);
    if (requestId == null) {
      requestId = EMPTY_STRING;
    }
  }

  /**
   * Sets byteSent metric.
   *
   * @param bytesSent bytes sent.
   */
  protected void setBytesSent(final int bytesSent) {
    this.bytesSent = bytesSent;
  }

  /**
   * Sets expected bytes to be sent.
   *
   * @param expectedBytesToBeSent expected bytes to be sent.
   */
  protected void setExpectedBytesToBeSent(final int expectedBytesToBeSent) {
    this.expectedBytesToBeSent = expectedBytesToBeSent;
  }

  /**
   * Sets connection time in milliseconds taken to establish the connection.
   *
   * @param connectionTimeMs connection time in milliseconds.
   */
  protected void setConnectionTimeMs(final long connectionTimeMs) {
    this.connectionTimeMs = connectionTimeMs;
  }

  /**
   * Sets send request time in milliseconds.
   *
   * @param sendRequestTimeMs send request time in milliseconds.
   */
  protected void setSendRequestTimeMs(final long sendRequestTimeMs) {
    this.sendRequestTimeMs = sendRequestTimeMs;
  }

  /**
   * Sets receive response time in milliseconds.
   *
   * @param recvResponseTimeMs receive response time in milliseconds.
   */
  protected void setRecvResponseTimeMs(final long recvResponseTimeMs) {
    this.recvResponseTimeMs = recvResponseTimeMs;
  }

  /**
   * Marks network error and expect100 failures for send-payload phase.
   */
  protected void setConnectionDisconnectedOnError() {
    this.connectionDisconnectedOnError = true;
  }

  /**
   * @return value of {@link #connectionDisconnectedOnError}
   */
  protected boolean isConnectionDisconnectedOnError() {
    return connectionDisconnectedOnError;
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
    public void processResponse(final byte[] buffer,
        final int offset,
        final int length)
        throws IOException {

    }

    @Override
    public void setRequestProperty(final String key, final String value) {

    }

    @Override
    protected InputStream getContentInputStream() throws IOException {
      return null;
    }

    @Override
    protected InputStream getErrorStream() throws IOException {
      return null;
    }

    @Override
    String getConnProperty(final String key) {
      return null;
    }

    @Override
    URL getConnUrl() {
      return null;
    }

    @Override
    Integer getConnResponseCode() throws IOException {
      return null;
    }

    @Override
    String getConnResponseMessage() throws IOException {
      return null;
    }

    @Override
    Map<String, List<String>> getRequestProperties() {
      return null;
    }

    @Override
    String getRequestProperty(final String headerName) {
      return null;
    }

    @Override
    public String getTracingContextSuffix() {
      return null;
    }

    @Override
    public String getResponseHeader(final String httpHeader) {
      return "";
    }

    @Override
    public Map<String, List<String>> getResponseHeaders() {
      return new HashMap<>();
    }

    @Override
    public void sendPayload(final byte[] buffer,
        final int offset,
        final int length)
        throws IOException {

    }
  }

  /**
   * Dummy Result to be returned for getFileStatus for implicit directory paths.
   * Blob Endpoint is not capable of understanding implicit paths and handling
   * on client side is needed to make sure HDFS compatibility holds.
   */
  public static class AbfsHttpOperationWithFixedResultForGetFileStatus extends AbfsHttpOperation {

    public AbfsHttpOperationWithFixedResultForGetFileStatus(final URL url,
        final String method,
        final int httpStatus) {
      super(url, method, httpStatus);
    }

    @Override
    public String getResponseHeader(final String httpHeader) {
      // Directories on FNS-Blob are identified by a special metadata header.
      if (httpHeader.equals(X_MS_META_HDI_ISFOLDER)) {
        return TRUE;
      }
      return EMPTY_STRING;
    }

    @Override
    public Map<String, List<String>> getResponseHeaders() {
      return new HashMap<>();
    }

    @Override
    public void processResponse(final byte[] buffer,
        final int offset,
        final int length)
        throws IOException {

    }

    @Override
    public void setRequestProperty(final String key, final String value) {

    }

    @Override
    protected InputStream getContentInputStream() throws IOException {
      return null;
    }

    @Override
    protected InputStream getErrorStream() throws IOException {
      return null;
    }

    @Override
    String getConnProperty(final String key) {
      return null;
    }

    @Override
    URL getConnUrl() {
      return null;
    }

    @Override
    Integer getConnResponseCode() throws IOException {
      return null;
    }

    @Override
    String getConnResponseMessage() throws IOException {
      return null;
    }

    @Override
    Map<String, List<String>> getRequestProperties() {
      return null;
    }

    @Override
    String getRequestProperty(final String headerName) {
      return null;
    }

    @Override
    public String getTracingContextSuffix() {
      return null;
    }

    @Override
    public void sendPayload(final byte[] buffer,
        final int offset,
        final int length)
        throws IOException {

    }
  }

  /**
   * Dummy Result to be returned for listBlobs for paths existing as files.
   * Blob Endpoint listing returns empty results and client handling
   * is needed to make sure HDFS compatibility holds.
   */
  public static class AbfsHttpOperationWithFixedResultForGetListStatus extends AbfsHttpOperation {
    private final ListResultSchema hardSetListResultSchema;

    public AbfsHttpOperationWithFixedResultForGetListStatus(final URL url,
        final String method,
        final int httpStatus,
        final ListResultSchema listResult) {
      super(url, method, httpStatus);
      hardSetListResultSchema = listResult;
    }

    @Override
    public ListResultSchema getListResultSchema() {
      return hardSetListResultSchema;
    }

    @Override
    public void processResponse(final byte[] buffer,
        final int offset,
        final int length)
        throws IOException {

    }

    @Override
    public void setRequestProperty(final String key, final String value) {

    }

    @Override
    protected InputStream getContentInputStream() throws IOException {
      return null;
    }

    @Override
    protected InputStream getErrorStream() throws IOException {
      return null;
    }

    @Override
    String getConnProperty(final String key) {
      return null;
    }

    @Override
    URL getConnUrl() {
      return null;
    }

    @Override
    Integer getConnResponseCode() throws IOException {
      return null;
    }

    @Override
    String getConnResponseMessage() throws IOException {
      return null;
    }

    @Override
    Map<String, List<String>> getRequestProperties() {
      return null;
    }

    @Override
    String getRequestProperty(final String headerName) {
      return null;
    }

    @Override
    public String getTracingContextSuffix() {
      return null;
    }

    @Override
    public String getResponseHeader(final String httpHeader) {
      return "";
    }

    @Override
    public Map<String, List<String>> getResponseHeaders() {
      return new HashMap<>();
    }

    @Override
    public void sendPayload(final byte[] buffer,
        final int offset,
        final int length)
        throws IOException {
    }
  }
}
