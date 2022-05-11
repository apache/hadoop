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

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.List;
import java.util.Map;

import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLSocketFactory;

import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.JsonToken;
import org.codehaus.jackson.map.ObjectMapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.security.ssl.DelegatingSSLSocketFactory;
import org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants;
import org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations;
import org.apache.hadoop.fs.azurebfs.contracts.services.ListResultSchema;

public class AbfsHttpConnection extends AbfsHttpOperation {
  private static final Logger LOG = LoggerFactory.getLogger(AbfsHttpOperation.class);
  private HttpURLConnection connection;
  private ListResultSchema listResultSchema = null;

  public AbfsHttpConnection(final URL url,
      final String method,
      List<AbfsHttpHeader> requestHeaders) throws IOException {
    super(url, method);
    init(method, requestHeaders);
  }

  /**
   * Initializes a new HTTP request and opens the connection.
   *
   * @param method The HTTP method (PUT, PATCH, POST, GET, HEAD, or DELETE).
   * @param requestHeaders The HTTP request headers.READ_TIMEOUT
   *
   * @throws IOException if an error occurs.
   */
  private void init(final String method, List<AbfsHttpHeader> requestHeaders)
      throws IOException {
    connection = openConnection();
    if (connection instanceof HttpsURLConnection) {
      HttpsURLConnection secureConn = (HttpsURLConnection) connection;
      SSLSocketFactory sslSocketFactory = DelegatingSSLSocketFactory.getDefaultFactory();
      if (sslSocketFactory != null) {
        secureConn.setSSLSocketFactory(sslSocketFactory);
      }
    }

    connection.setConnectTimeout(getConnectTimeout());
    connection.setReadTimeout(getReadTimeout());

    connection.setRequestMethod(method);

    for (AbfsHttpHeader header : requestHeaders) {
      connection.setRequestProperty(header.getName(), header.getValue());
    }
  }

  public HttpURLConnection getConnection() {
    return connection;
  }

  public ListResultSchema getListResultSchema() {
    return listResultSchema;
  }

  public String getResponseHeader(String httpHeader) {
    return connection.getHeaderField(httpHeader);
  }

  public void setHeader(String header, String value) {
    getConnection().setRequestProperty(header, value);
  }

  public Map<String, List<String>> getRequestHeaders() {
    return getConnection().getRequestProperties();
  }

  public String getRequestHeader(String header) {
    return getConnection().getRequestProperty(header);
  }

  public String getClientRequestId() {
    return connection
        .getRequestProperty(HttpHeaderConfigurations.X_MS_CLIENT_REQUEST_ID);
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
    connection.setDoOutput(true);
    connection.setFixedLengthStreamingMode(length);
    if (buffer == null) {
      // An empty buffer is sent to set the "Content-Length: 0" header, which
      // is required by our endpoint.
      buffer = new byte[]{};
      offset = 0;
      length = 0;
    }

    // send the request body

    long startTime = 0;
    if (isTraceEnabled()) {
      startTime = System.nanoTime();
    }
    try (OutputStream outputStream = connection.getOutputStream()) {
      // update bytes sent before they are sent so we may observe
      // attempted sends as well as successful sends via the
      // accompanying statusCode
      setBytesSent(length);
      outputStream.write(buffer, offset, length);
    } finally {
      if (isTraceEnabled()) {
        setSendRequestTimeMs(elapsedTimeMs(startTime));
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
  public void processResponse(byte[] buffer, final int offset,
      final int length) throws IOException {
    // get the response
    long startTime = 0;
    if (isTraceEnabled()) {
      startTime = System.nanoTime();
    }

    setStatusCode(connection.getResponseCode());

    if (isTraceEnabled()) {
      setRecvResponseTimeMs(elapsedTimeMs(startTime));
    }

    setStatusDescription(connection.getResponseMessage());

    setRequestId(connection.getHeaderField(
        HttpHeaderConfigurations.X_MS_REQUEST_ID));
    if (getRequestId() == null) {
      setRequestId(AbfsHttpConstants.EMPTY_STRING);
    }
    // dump the headers
    AbfsIoUtils.dumpHeadersToDebugLog("Response Headers",
        connection.getHeaderFields());

    if (AbfsHttpConstants.HTTP_METHOD_HEAD.equals(getMethod())) {
      // If it is HEAD, and it is ERROR
      return;
    }

    if (isTraceEnabled()) {
      startTime = System.nanoTime();
    }

    if (getStatusCode() >= HttpURLConnection.HTTP_BAD_REQUEST) {
      processStorageErrorResponse();
      if (isTraceEnabled()) {
        setRecvResponseTimeMs(getRecvResponseTimeMs() + elapsedTimeMs(startTime));
      }
      setBytesReceived(connection.getHeaderFieldLong(
          HttpHeaderConfigurations.CONTENT_LENGTH, 0));
    } else {
      // consume the input stream to release resources
      int totalBytesRead = 0;

      try (InputStream stream = connection.getInputStream()) {
        if (isNullInputStream(stream)) {
          return;
        }
        boolean endOfStream = false;

        // this is a list operation and need to retrieve the data
        // need a better solution
        if (AbfsHttpConstants.HTTP_METHOD_GET.equals(getMethod())
            && buffer == null) {
          parseListFilesResponse(stream);
        } else if (AbfsHttpConstants.HTTP_METHOD_POST.equals(getMethod())) {
          int contentLen = connection.getContentLength();
          if (contentLen != 0) {
            try (DataInputStream dis = new DataInputStream(stream)) {
              byte[] contentBuffer  = new byte[contentLen];
              dis.readFully(contentBuffer);
              setResponseContentBuffer(contentBuffer);
              totalBytesRead += contentLen;
            }
          }
        } else {
          if (buffer != null) {
            while (totalBytesRead < length) {
              int bytesRead = stream.read(buffer, offset + totalBytesRead,
                  length - totalBytesRead);
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
            byte[] b = new byte[getCleanUpBufferSize()];
            while ((bytesRead = stream.read(b)) >= 0) {
              totalBytesRead += bytesRead;
            }
          }
        }
      } catch (IOException ex) {
        LOG.warn("IO/Network error: {} {}: {}",
            getMethod(), getMaskedUrl(), ex.getMessage());
        LOG.debug("IO Error: ", ex);
        throw ex;
      } finally {
        if (isTraceEnabled()) {
          setRecvResponseTimeMs(getRecvResponseTimeMs() + elapsedTimeMs(startTime));
        }

        setBytesReceived(totalBytesRead);
      }
    }
  }

  /**
   * Open the HTTP connection.
   *
   * @throws IOException if an error occurs.
   */
  private HttpURLConnection openConnection() throws IOException {
    if (!isTraceEnabled()) {
      return (HttpURLConnection) getUrl().openConnection();
    }
    long start = System.nanoTime();
    try {
      return (HttpURLConnection) getUrl().openConnection();
    } finally {
      setConnectionTimeMs(elapsedTimeMs(start));
    }
  }

  /**
   * When the request fails, this function is used to parse the response
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
              setStorageErrorCode(fieldValue);
              break;
            case "message":
              setStorageErrorMessage(fieldValue);
              break;
            case "ExpectedAppendPos":
              setExpectedAppendPos(fieldValue);
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
      listResultSchema = objectMapper.readValue(stream, ListResultSchema.class);
    } catch (IOException ex) {
      LOG.error("Unable to deserialize list results", ex);
      throw ex;
    }
  }
}