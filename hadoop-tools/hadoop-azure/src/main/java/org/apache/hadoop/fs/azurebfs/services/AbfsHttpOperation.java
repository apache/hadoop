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
import java.io.UnsupportedEncodingException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.util.List;
import java.util.UUID;

import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLSocketFactory;

import org.apache.hadoop.security.ssl.DelegatingSSLSocketFactory;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.JsonToken;
import org.codehaus.jackson.map.ObjectMapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants;
import org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations;
import org.apache.hadoop.fs.azurebfs.contracts.services.AbfsPerfLoggable;
import org.apache.hadoop.fs.azurebfs.contracts.services.ListResultSchema;

/**
 * Represents an HTTP operation.
 */
public class AbfsHttpOperation implements AbfsPerfLoggable {
  private static final Logger LOG = LoggerFactory.getLogger(AbfsHttpOperation.class);

  public static final String SIGNATURE_QUERY_PARAM_KEY = "sig=";

  private static final int CONNECT_TIMEOUT = 30 * 1000;
  private static final int READ_TIMEOUT = 30 * 1000;

  private static final int CLEAN_UP_BUFFER_SIZE = 64 * 1024;

  private static final int ONE_THOUSAND = 1000;
  private static final int ONE_MILLION = ONE_THOUSAND * ONE_THOUSAND;

  private final String method;
  private final URL url;
  private String maskedUrl;
  private String maskedEncodedUrl;

  private HttpURLConnection connection;
  private int statusCode;
  private String statusDescription;
  private String storageErrorCode = "";
  private String storageErrorMessage  = "";
  private String clientRequestId = "";
  private String requestId  = "";
  private String expectedAppendPos = "";
  private ListResultSchema listResultSchema = null;

  // metrics
  private int bytesSent;
  private long bytesReceived;

  // optional trace enabled metrics
  private final boolean isTraceEnabled;
  private long connectionTimeMs;
  private long sendRequestTimeMs;
  private long recvResponseTimeMs;

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
    this.isTraceEnabled = LOG.isTraceEnabled();
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
    return clientRequestId;
  }

  public String getExpectedAppendPos() {
    return expectedAppendPos;
  }

  public String getRequestId() {
    return requestId;
  }

  public int getBytesSent() {
    return bytesSent;
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
    sb.append(clientRequestId);
    sb.append(",rid=");
    sb.append(requestId);
    if (isTraceEnabled) {
      sb.append(",connMs=");
      sb.append(connectionTimeMs);
      sb.append(",sendMs=");
      sb.append(sendRequestTimeMs);
      sb.append(",recvMs=");
      sb.append(recvResponseTimeMs);
    }
    sb.append(",sent=");
    sb.append(bytesSent);
    sb.append(",recv=");
    sb.append(bytesReceived);
    sb.append(",");
    sb.append(method);
    sb.append(",");
    sb.append(getSignatureMaskedUrl());
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
      .append(clientRequestId)
      .append(" ri=")
      .append(requestId);

    if (isTraceEnabled) {
      sb.append(" ct=")
        .append(connectionTimeMs)
        .append(" st=")
        .append(sendRequestTimeMs)
        .append(" rt=")
        .append(recvResponseTimeMs);
    }

    sb.append(" bs=")
      .append(bytesSent)
      .append(" br=")
      .append(bytesReceived)
      .append(" m=")
      .append(method)
      .append(" u=")
      .append(getSignatureMaskedEncodedUrl());

    return sb.toString();
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
    this.isTraceEnabled = LOG.isTraceEnabled();
    this.url = url;
    this.method = method;
    this.clientRequestId = UUID.randomUUID().toString();

    this.connection = openConnection();
    if (this.connection instanceof HttpsURLConnection) {
      HttpsURLConnection secureConn = (HttpsURLConnection) this.connection;
      SSLSocketFactory sslSocketFactory = DelegatingSSLSocketFactory.getDefaultFactory();
      if (sslSocketFactory != null) {
        secureConn.setSSLSocketFactory(sslSocketFactory);
      }
    }

    this.connection.setConnectTimeout(CONNECT_TIMEOUT);
    this.connection.setReadTimeout(READ_TIMEOUT);

    this.connection.setRequestMethod(method);

    for (AbfsHttpHeader header : requestHeaders) {
      this.connection.setRequestProperty(header.getName(), header.getValue());
    }

    this.connection.setRequestProperty(HttpHeaderConfigurations.X_MS_CLIENT_REQUEST_ID, clientRequestId);
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
    if (this.isTraceEnabled) {
      startTime = System.nanoTime();
    }
    try (OutputStream outputStream = this.connection.getOutputStream()) {
      // update bytes sent before they are sent so we may observe
      // attempted sends as well as successful sends via the
      // accompanying statusCode
      this.bytesSent = length;
      outputStream.write(buffer, offset, length);
    } finally {
      if (this.isTraceEnabled) {
        this.sendRequestTimeMs = elapsedTimeMs(startTime);
      }
    }
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
    if (this.isTraceEnabled) {
      startTime = System.nanoTime();
    }

    this.statusCode = this.connection.getResponseCode();

    if (this.isTraceEnabled) {
      this.recvResponseTimeMs = elapsedTimeMs(startTime);
    }

    this.statusDescription = this.connection.getResponseMessage();

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

    if (this.isTraceEnabled) {
      startTime = System.nanoTime();
    }

    if (statusCode >= HttpURLConnection.HTTP_BAD_REQUEST) {
      processStorageErrorResponse();
      if (this.isTraceEnabled) {
        this.recvResponseTimeMs += elapsedTimeMs(startTime);
      }
      this.bytesReceived = this.connection.getHeaderFieldLong(HttpHeaderConfigurations.CONTENT_LENGTH, 0);
    } else {
      // consume the input stream to release resources
      int totalBytesRead = 0;

      try (InputStream stream = this.connection.getInputStream()) {
        if (isNullInputStream(stream)) {
          return;
        }
        boolean endOfStream = false;

        // this is a list operation and need to retrieve the data
        // need a better solution
        if (AbfsHttpConstants.HTTP_METHOD_GET.equals(this.method) && buffer == null) {
          parseListFilesResponse(stream);
        } else {
          if (buffer != null) {
            while (totalBytesRead < length) {
              int bytesRead = stream.read(buffer, offset + totalBytesRead, length - totalBytesRead);
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
        LOG.error("UnexpectedError: ", ex);
        throw ex;
      } finally {
        if (this.isTraceEnabled) {
          this.recvResponseTimeMs += elapsedTimeMs(startTime);
        }
        this.bytesReceived = totalBytesRead;
      }
    }
  }


  /**
   * Open the HTTP connection.
   *
   * @throws IOException if an error occurs.
   */
  private HttpURLConnection openConnection() throws IOException {
    if (!isTraceEnabled) {
      return (HttpURLConnection) url.openConnection();
    }
    long start = System.nanoTime();
    try {
      return (HttpURLConnection) url.openConnection();
    } finally {
      connectionTimeMs = elapsedTimeMs(start);
    }
  }

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
    try (InputStream stream = connection.getErrorStream()) {
      if (stream == null) {
        return;
      }
      JsonFactory jf = new JsonFactory();
      try (JsonParser jp = jf.createJsonParser(stream)) {
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
      LOG.debug("ExpectedError: ", ex);
    }
  }

  /**
   * Returns the elapsed time in milliseconds.
   */
  private long elapsedTimeMs(final long startTime) {
    return (System.nanoTime() - startTime) / ONE_MILLION;
  }

  /**
   * Parse the list file response
   *
   * @param stream InputStream contains the list results.
   * @throws IOException
   */
  private void parseListFilesResponse(final InputStream stream) throws IOException {
    if (stream == null) {
      return;
    }

    if (listResultSchema != null) {
      // already parse the response
      return;
    }

    try {
      final ObjectMapper objectMapper = new ObjectMapper();
      this.listResultSchema = objectMapper.readValue(stream, ListResultSchema.class);
    } catch (IOException ex) {
      LOG.error("Unable to deserialize list results", ex);
      throw ex;
    }
  }

  /**
   * Check null stream, this is to pass findbugs's redundant check for NULL
   * @param stream InputStream
   */
  private boolean isNullInputStream(InputStream stream) {
    return stream == null ? true : false;
  }

  public static String getSignatureMaskedUrl(String url) {
    int qpStrIdx = url.indexOf('?' + SIGNATURE_QUERY_PARAM_KEY);
    if (qpStrIdx == -1) {
      qpStrIdx = url.indexOf('&' + SIGNATURE_QUERY_PARAM_KEY);
    }
    if (qpStrIdx == -1) {
      return url;
    }
    final int sigStartIdx = qpStrIdx + SIGNATURE_QUERY_PARAM_KEY.length() + 1;
    final int ampIdx = url.indexOf("&", sigStartIdx);
    final int sigEndIndex = (ampIdx != -1) ? ampIdx : url.length();
    String signature = url.substring(sigStartIdx, sigEndIndex);
    return url.replace(signature, "XXXX");
  }

  public static String encodedUrlStr(String url) {
    try {
      return URLEncoder.encode(url, "UTF-8");
    } catch (UnsupportedEncodingException e) {
      return "https%3A%2F%2Ffailed%2Fto%2Fencode%2Furl";
    }
  }

  public String getSignatureMaskedUrl() {
    if (this.maskedUrl == null) {
      this.maskedUrl = getSignatureMaskedUrl(this.url.toString());
    }
    return this.maskedUrl;
  }

  public String getSignatureMaskedEncodedUrl() {
    if (this.maskedEncodedUrl == null) {
      this.maskedEncodedUrl = encodedUrlStr(getSignatureMaskedUrl());
    }
    return this.maskedEncodedUrl;
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
