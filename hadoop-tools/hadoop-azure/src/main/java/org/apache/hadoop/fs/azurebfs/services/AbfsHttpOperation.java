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
import java.net.URL;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpRequest.BodyPublisher;
import java.net.http.HttpResponse.BodyHandler;
import java.util.List;

import com.sun.xml.bind.v2.TODO;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AzureBlobFileSystemException;
import org.apache.hadoop.fs.azurebfs.http.AbfsHttpClient;
import org.apache.hadoop.fs.azurebfs.http.AbfsHttpRequestBuilder;
import org.apache.hadoop.fs.azurebfs.http.AbfsHttpResponse;
import org.apache.hadoop.fs.azurebfs.http.AbfsHttpStatusCodes;
import org.apache.hadoop.fs.azurebfs.utils.TimeUtils;
import org.apache.hadoop.fs.azurebfs.utils.UriUtils;

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

/**
 * Represents an HTTP operation.
 */
public class AbfsHttpOperation implements AbfsPerfLoggable {
  private static final Logger LOG = LoggerFactory.getLogger(AbfsHttpOperation.class);

  private static final int CLEAN_UP_BUFFER_SIZE = 64 * 1024;

  private final AbfsHttpClient abfsHttpClient;

  private final String method;
  private final URL url;
  private String maskedUrl;
  private String maskedEncodedUrl;
  private AbfsHttpRequestBuilder httpRequestBuilder;

  private AbfsHttpResponse httpResponse;
  private int statusCode;
  private String statusDescription;
  private String storageErrorCode = "";
  private String storageErrorMessage  = "";
  private String requestId  = "";
  private String expectedAppendPos = "";
  private ListResultSchema listResultSchema = null;

  // metrics
  private int bytesSent;
  private int expectedBytesToBeSent;
  private long bytesReceived;

  private long sendRequestTimeMs;
  private long recvResponseTimeMs;
  private boolean shouldMask = false;

  public static AbfsHttpOperation getAbfsHttpOperationWithFixedResult(
      final AbfsHttpClient abfsHttpClient,
      final URL url,
      final String method,
      final int httpStatus) {
    AbfsHttpOperationWithFixedResult httpOp
        = new AbfsHttpOperationWithFixedResult(abfsHttpClient, url, method, httpStatus);
    return httpOp;
  }

  /**
   * Constructor for FixedResult instance, avoiding connection init.
   * @param url request url
   * @param method Http method
   * @param httpStatus HttpStatus
   */
  protected AbfsHttpOperation(
          final AbfsHttpClient abfsHttpClient,
          final URL url,
      final String method,
      final int httpStatus) {
    this.abfsHttpClient = abfsHttpClient;
    this.url = url;
    this.method = method;
    this.statusCode = httpStatus;
  }

  /**
   * Initializes a new HTTP operation.
   *
   * @param url The full URL including query string parameters.
   * @param method The HTTP method (PUT, PATCH, POST, GET, HEAD, or DELETE).
   * @param requestHeaders The HTTP request headers.READ_TIMEOUT
   *
   * @throws IOException if an error occurs.
   */
  public AbfsHttpOperation(final AbfsHttpClient abfsHttpClient,
                           final URL url, final String method,
                           final List<AbfsHttpHeader> requestHeaders,
                           BodyPublisher bodyPublisher)
          throws IOException {
    this.abfsHttpClient = abfsHttpClient;
    this.url = url;
    this.method = method;
    this.httpRequestBuilder = abfsHttpClient.createRequestBuilder(url, method, requestHeaders, bodyPublisher);
  }

  protected AbfsHttpRequestBuilder getHttpRequestBuilder() {
    return httpRequestBuilder;
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
    return this.httpRequestBuilder
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
    return httpResponse.getHeaderField(httpHeader);
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
    sb.append(httpRequestBuilder.connectionTimeDetails());
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

            // TODO ARNAUD
//      .append(" ct=")
//      .append(connectionTimeMs)
//      .append(" st=")
//      .append(sendRequestTimeMs)
//      .append(" rt=")
//      .append(recvResponseTimeMs)

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
  public <T> void sendRequest(byte[] buffer, int offset, int length,
                              BodyHandler<T> bodyHandler) throws IOException {
    if (buffer == null) {
      // An empty buffer is sent to set the "Content-Length: 0" header, which
      // is required by our endpoint.
      buffer = new byte[]{};
      offset = 0;
      length = 0;
    }
    // Updates the expected bytes to be sent based on length.
    this.expectedBytesToBeSent = length;

    // send the request body
    long startTime = 0;
    startTime = System.nanoTime();

    // TODO ARNAUD
    AbfsHttpResponse abfsHttpResponse;
    try {
        abfsHttpResponse = this.abfsHttpClient.sendRequest(this.httpRequestBuilder, bodyHandler);
    } catch (InterruptedException e) {
        throw new RuntimeException("Interrupted", e);
    }

//    OutputStream outputStream = null;
//    try {
//      // this.connection.setDoOutput(true);
//      // this.connection.setFixedLengthStreamingMode(length);
//      try {
//        /* Without expect header enabled, if getOutputStream() throws
//           an exception, it gets caught by the restOperation. But with
//           expect header enabled we return back without throwing an exception
//           for the correct response code processing.
//         */
//        outputStream = getConnOutputStream();
//      } catch (IOException e) {
//        /* If getOutputStream fails with an exception and expect header
//           is enabled, we return back without throwing an exception to
//           the caller. The caller is responsible for setting the correct status code.
//           If expect header is not enabled, we throw back the exception.
//         */
//        String expectHeader = getConnProperty(EXPECT);
//        if (expectHeader != null && expectHeader.equals(HUNDRED_CONTINUE)) {
//          LOG.debug("Getting output stream failed with expect header enabled, returning back ", e);
//          return;
//        } else {
//          LOG.debug("Getting output stream failed without expect header enabled, throwing exception ", e);
//          throw e;
//        }
//      }
//
//      // If this fails with or without expect header enabled,
//      // it throws an IOException.
//      outputStream.write(buffer, offset, length);
//    } finally {
//      // Closing the opened output stream
//      if (outputStream != null) {
//        outputStream.close();
//      }
//    }

    // update bytes sent for successful as well as failed attempts via the
    // accompanying statusCode.
    this.bytesSent = length;

    this.sendRequestTimeMs = TimeUtils.elapsedTimeMs(startTime);

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
    AbfsHttpResponse resp = this.httpResponse; // TODO ARNAUD

    // get the response
    long startTime = 0;
    startTime = System.nanoTime();

    this.statusCode = resp.getResponseCode(); // getConnResponseCode();
    this.recvResponseTimeMs = TimeUtils.elapsedTimeMs(startTime);

    this.statusDescription = resp.getResponseMessage(); // getConnResponseMessage();

    this.requestId = this.httpResponse.getHeaderField(HttpHeaderConfigurations.X_MS_REQUEST_ID);
    if (this.requestId == null) {
      this.requestId = AbfsHttpConstants.EMPTY_STRING;
    }
    // dump the headers
    AbfsIoUtils.dumpHeadersToDebugLog("Response Headers",
            httpResponse.getHeaderFields());

    if (AbfsHttpConstants.HTTP_METHOD_HEAD.equals(this.method)) {
      // If it is HEAD, and it is ERROR
      return;
    }

    startTime = System.nanoTime();

    if (statusCode >= AbfsHttpStatusCodes.BAD_REQUEST) {
      processStorageErrorResponse();
      this.recvResponseTimeMs += TimeUtils.elapsedTimeMs(startTime);
      this.bytesReceived = this.httpResponse.getHeaderFieldLong(HttpHeaderConfigurations.CONTENT_LENGTH, 0);
    } else {
      // consume the input stream to release resources
      int totalBytesRead = 0;

      try (InputStream stream = resp.getBodyInputStream()) {
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
        LOG.warn("IO/Network error: {} {}: {}",
            method, getMaskedUrl(), ex.getMessage());
        LOG.debug("IO Error: ", ex);
        throw ex;
      } finally {
        this.recvResponseTimeMs += TimeUtils.elapsedTimeMs(startTime);
        this.bytesReceived = totalBytesRead;
      }
    }
  }

  public void setRequestProperty(String key, String value) {
    this.httpRequestBuilder.setRequestProperty(key, value);
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
    // connection.getErrorStream()
    try (InputStream stream = httpResponse.getBodyInputStream()) {
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
      LOG.debug("ExpectedError: ", ex);
    }
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

  /**
   * Gets the connection request property for a key.
   * @param key The request property key.
   * @return request peoperty value.
   */
  String getConnProperty(String key) {
    return httpRequestBuilder.getRequestProperty(key);
  }

  /**
   * Gets the connection url.
   * @return url.
   */
  URL getConnUrl() {
    return httpRequestBuilder.getURL();
  }

  /**
   * Gets the connection request method.
   * @return request method.
   */
  String getConnRequestMethod() {
    return httpRequestBuilder.getMethod();
  }

  /**
   * Gets the connection response code.
   * @return response code.
   * @throws IOException
   */
  Integer getConnResponseCode() throws IOException {
    return httpResponse.getResponseCode();
  }

  /**
   * Gets the connection response message.
   * @return response message.
   * @throws IOException
   */
  String getConnResponseMessage() throws IOException {
    return httpResponse.getResponseMessage();
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
    public AbfsHttpOperationWithFixedResult(final AbfsHttpClient abfsHttpClient, final URL url,
        final String method,
        final int httpStatus) {
      super(abfsHttpClient, url, method, httpStatus);
    }

    @Override
    public String getResponseHeader(final String httpHeader) {
      return "";
    }
  }
}
