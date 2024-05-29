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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;

import org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants;
import org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations;
import org.apache.hadoop.fs.azurebfs.contracts.services.AbfsPerfLoggable;
import org.apache.hadoop.fs.azurebfs.contracts.services.ListResultSchema;
import org.apache.hadoop.fs.azurebfs.utils.UriUtils;

/**
 * Base Http operation class for orchestrating server IO calls. Child classes would
 * define the certain orchestration implementation on the basis of network library used.
 * <p>
 * For JDK netlib usage, the child class would be {@link AbfsJdkHttpOperation}. <br>
 * For ApacheHttpClient netlib usage, the child class would be {@link AbfsAHCHttpOperation}.
 * </p>
 */
public abstract class AbfsHttpOperation implements AbfsPerfLoggable {

  private final Logger log;

  private static final int CLEAN_UP_BUFFER_SIZE = 64 * 1024;

  private static final int ONE_THOUSAND = 1000;

  private static final int ONE_MILLION = ONE_THOUSAND * ONE_THOUSAND;

  private final String method;
  private final URL url;
  private String maskedUrl;
  private String maskedEncodedUrl;
  private int statusCode;
  private String statusDescription;
  private String storageErrorCode = "";
  private String storageErrorMessage = "";
  private String requestId = "";
  private String expectedAppendPos = "";
  private ListResultSchema listResultSchema = null;

  // metrics
  private int bytesSent;
  private int expectedBytesToBeSent;
  private long bytesReceived;

  private long connectionTimeMs;
  private long sendRequestTimeMs;
  private long recvResponseTimeMs;
  private boolean shouldMask = false;

  private final List<AbfsHttpHeader> requestHeaders;

  private final int connectionTimeout, readTimeout;

  public static AbfsHttpOperation getAbfsHttpOperationWithFixedResult(
      final URL url,
      final String method,
      final int httpStatus) {
    AbfsHttpOperationWithFixedResult httpOp
        = new AbfsHttpOperationWithFixedResult(url, method, httpStatus);
    return httpOp;
  }

  public AbfsHttpOperation(final Logger log, final URL url, final String method,
      final List<AbfsHttpHeader> requestHeaders, final int connectionTimeout,
      final int readTimeout) {
    this.log = log;
    this.url = url;
    this.method = method;
    this.requestHeaders = requestHeaders;
    this.connectionTimeout = connectionTimeout;
    this.readTimeout = readTimeout;
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
    this.log = null;
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

  void addHeaderToRequestHeaderList(AbfsHttpHeader abfsHttpHeader) {
    if (requestHeaders != null) {
      requestHeaders.add(abfsHttpHeader);
    }
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

  public abstract String getClientRequestId();

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

  public abstract String getResponseHeader(String httpHeader);

  void setExpectedBytesToBeSent(int expectedBytesToBeSent) {
    this.expectedBytesToBeSent = expectedBytesToBeSent;
  }

  void setStatusCode(int statusCode) {
    this.statusCode = statusCode;
  }

  void setStatusDescription(String statusDescription) {
    this.statusDescription = statusDescription;
  }

  void setBytesSent(int bytesSent) {
    this.bytesSent = bytesSent;
  }

  void setSendRequestTimeMs(long sendRequestTimeMs) {
    this.sendRequestTimeMs = sendRequestTimeMs;
  }

  void setRecvResponseTimeMs(long recvResponseTimeMs) {
    this.recvResponseTimeMs = recvResponseTimeMs;
  }

  void setRequestId(String requestId) {
    this.requestId = requestId;
  }

  void setConnectionTimeMs(long connectionTimeMs) {
    this.connectionTimeMs = connectionTimeMs;
  }

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

  public String getMaskedEncodedUrl() {
    if (maskedEncodedUrl != null) {
      return maskedEncodedUrl;
    }
    maskedEncodedUrl = UriUtils.encodedUrlStr(getMaskedUrl());
    return maskedEncodedUrl;
  }

  public abstract void sendPayload(byte[] buffer, int offset, int length) throws
      IOException;

  public abstract void processResponse(byte[] buffer,
      int offset,
      int length) throws IOException;

  public abstract void setRequestProperty(String key, String value);

  void parseResponse(final byte[] buffer,
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
        if (AbfsHttpConstants.HTTP_METHOD_GET.equals(this.method)
            && buffer == null) {
          parseListFilesResponse(stream);
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

  abstract InputStream getContentInputStream() throws IOException;

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
  protected void processStorageErrorResponse() {
    try (InputStream stream = getErrorStream()) {
      if (stream == null) {
        return;
      }
      JsonFactory jf = new JsonFactory();
      try (JsonParser jp = jf.createParser(stream)) {
        String fieldName, fieldValue;
        jp.nextToken();  // START_OBJECT - {
        jp.nextToken();  // FIELD_NAME - "error":
        jp.nextToken();  // START_OBJECT - {
        jp.nextToken();
        while (jp.hasCurrentToken()) {
          if (jp.getCurrentToken() == JsonToken.FIELD_NAME) {
            fieldName = jp.getCurrentName();
            jp.nextToken();
            fieldValue = jp.getText();
            switch (fieldName) {
              case "code":
                storageErrorCode = fieldValue;
                break;
              case "message":
                storageErrorMessage = fieldValue;
                break;
              case "ExpectedAppendPos":
                expectedAppendPos = fieldValue;
                break;
              default:
                break;
            }
          }
          jp.nextToken();
        }
      }
    } catch (IOException ex) {
      // Ignore errors that occur while attempting to parse the storage
      // error, since the response may have been handled by the HTTP driver
      // or for other reasons have an unexpected
      log.debug("ExpectedError: ", ex);
    }
  }

  protected abstract InputStream getErrorStream() throws IOException;

  /**
   * Parse the list file response
   *
   * @param stream InputStream contains the list results.
   * @throws IOException if the response cannot be deserialized.
   */
  protected void parseListFilesResponse(final InputStream stream)
      throws IOException {
    if (stream == null) {
      return;
    }

    if (listResultSchema != null) {
      // already parse the response
      return;
    }

    try {
      final ObjectMapper objectMapper = new ObjectMapper();
      this.listResultSchema = objectMapper.readValue(stream,
          ListResultSchema.class);
    } catch (IOException ex) {
      log.error("Unable to deserialize list results", ex);
      throw ex;
    }
  }

  /**
   * Returns the elapsed time in milliseconds.
   */
  long elapsedTimeMs(final long startTime) {
    return (System.nanoTime() - startTime) / ONE_MILLION;
  }

  /**
   * Check null stream, this is to pass findbugs's redundant check for NULL
   * @param stream InputStream
   */
  boolean isNullInputStream(InputStream stream) {
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
   * Gets the connection request method.
   * @return request method.
   */
  abstract String getConnRequestMethod();

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

  abstract Map<String, List<String>> getRequestProperties();

  abstract String getRequestProperty(String headerName);

  abstract boolean getConnectionDisconnectedOnError();

  public abstract String getTracingContextSuffix();

  public long getSendLatency() {
    return sendRequestTimeMs;
  }

  public long getRecvLatency() {
    return recvResponseTimeMs;
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
    int getConnectionTimeout() {
      return super.getConnectionTimeout();
    }

    @Override
    int getReadTimeout() {
      return super.getReadTimeout();
    }

    @Override
    List<AbfsHttpHeader> getRequestHeaders() {
      return super.getRequestHeaders();
    }

    @Override
    void addHeaderToRequestHeaderList(final AbfsHttpHeader abfsHttpHeader) {
      super.addHeaderToRequestHeaderList(abfsHttpHeader);
    }

    @Override
    public String getMethod() {
      return super.getMethod();
    }

    @Override
    public String getHost() {
      return super.getHost();
    }

    @Override
    public int getStatusCode() {
      return super.getStatusCode();
    }

    @Override
    public String getStatusDescription() {
      return super.getStatusDescription();
    }

    @Override
    public String getStorageErrorCode() {
      return super.getStorageErrorCode();
    }

    @Override
    public String getStorageErrorMessage() {
      return super.getStorageErrorMessage();
    }

    @Override
    public String getClientRequestId() {
      return null;
    }

    @Override
    public String getExpectedAppendPos() {
      return super.getExpectedAppendPos();
    }

    @Override
    public String getRequestId() {
      return super.getRequestId();
    }

    @Override
    public void setMaskForSAS() {
      super.setMaskForSAS();
    }

    @Override
    public int getBytesSent() {
      return super.getBytesSent();
    }

    @Override
    public int getExpectedBytesToBeSent() {
      return super.getExpectedBytesToBeSent();
    }

    @Override
    public long getBytesReceived() {
      return super.getBytesReceived();
    }

    @Override
    public URL getUrl() {
      return super.getUrl();
    }

    @Override
    public ListResultSchema getListResultSchema() {
      return super.getListResultSchema();
    }

    @Override
    void setExpectedBytesToBeSent(final int expectedBytesToBeSent) {
      super.setExpectedBytesToBeSent(expectedBytesToBeSent);
    }

    @Override
    void setStatusCode(final int statusCode) {
      super.setStatusCode(statusCode);
    }

    @Override
    void setStatusDescription(final String statusDescription) {
      super.setStatusDescription(statusDescription);
    }

    @Override
    void setBytesSent(final int bytesSent) {
      super.setBytesSent(bytesSent);
    }

    @Override
    void setSendRequestTimeMs(final long sendRequestTimeMs) {
      super.setSendRequestTimeMs(sendRequestTimeMs);
    }

    @Override
    void setRecvResponseTimeMs(final long recvResponseTimeMs) {
      super.setRecvResponseTimeMs(recvResponseTimeMs);
    }

    @Override
    void setRequestId(final String requestId) {
      super.setRequestId(requestId);
    }

    @Override
    void setConnectionTimeMs(final long connectionTimeMs) {
      super.setConnectionTimeMs(connectionTimeMs);
    }

    @Override
    public String toString() {
      return super.toString();
    }

    @Override
    public String getLogString() {
      return super.getLogString();
    }

    @Override
    public String getMaskedUrl() {
      return super.getMaskedUrl();
    }

    @Override
    public String getMaskedEncodedUrl() {
      return super.getMaskedEncodedUrl();
    }

    @Override
    public void sendPayload(final byte[] buffer,
        final int offset,
        final int length)
        throws IOException {

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
    void parseResponse(final byte[] buffer, final int offset, final int length)
        throws IOException {
      super.parseResponse(buffer, offset, length);
    }

    @Override
    InputStream getContentInputStream() throws IOException {
      return null;
    }

    @Override
    protected void processStorageErrorResponse() {
      super.processStorageErrorResponse();
    }

    @Override
    protected InputStream getErrorStream() throws IOException {
      return null;
    }

    @Override
    protected void parseListFilesResponse(final InputStream stream)
        throws IOException {
      super.parseListFilesResponse(stream);
    }

    @Override
    long elapsedTimeMs(final long startTime) {
      return super.elapsedTimeMs(startTime);
    }

    @Override
    boolean isNullInputStream(final InputStream stream) {
      return super.isNullInputStream(stream);
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
    String getConnRequestMethod() {
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
    boolean getConnectionDisconnectedOnError() {
      return false;
    }

    @Override
    public String getTracingContextSuffix() {
      return null;
    }

    @Override
    public long getSendLatency() {
      return super.getSendLatency();
    }

    @Override
    public long getRecvLatency() {
      return super.getRecvLatency();
    }

    @Override
    public String getResponseHeader(final String httpHeader) {
      return "";
    }

  }
}
