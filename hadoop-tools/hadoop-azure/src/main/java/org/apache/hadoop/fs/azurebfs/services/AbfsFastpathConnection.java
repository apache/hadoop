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
import java.net.URL;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.thirdparty.com.google.common.annotations.VisibleForTesting;

import com.azure.storage.fastpath.exceptions.FastpathException;
import com.azure.storage.fastpath.exceptions.FastpathRequestException;
import com.azure.storage.fastpath.FastpathConnection;
import com.azure.storage.fastpath.requestParameters.AccessTokenType;
import com.azure.storage.fastpath.requestParameters.FastpathCloseRequestParams;
import com.azure.storage.fastpath.requestParameters.FastpathOpenRequestParams;
import com.azure.storage.fastpath.requestParameters.FastpathReadRequestParams;
import com.azure.storage.fastpath.responseProviders.FastpathCloseResponse;
import com.azure.storage.fastpath.responseProviders.FastpathOpenResponse;
import com.azure.storage.fastpath.responseProviders.FastpathReadResponse;
import com.azure.storage.fastpath.responseProviders.FastpathResponse;

import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AbfsFastpathException;

import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.DEFAULT_TIMEOUT;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.EMPTY_STRING;
import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.DEFAULT_VALUE_UNKNOWN;
import static org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations.X_MS_CLIENT_REQUEST_ID;
import static org.apache.hadoop.fs.azurebfs.services.AuthType.OAuth;

/**
 * Represents a Fastpath operation.
 */
public class AbfsFastpathConnection extends AbfsHttpOperation {

  // Saving the http status code to string map
  // to avoid adding external dependency on commons-httpclient
  private static final Map<Integer, String> STATUS_CODE_MAP = new HashMap<Integer, String>() {{
    put(200, "OK");
    put(201, "CREATED");
    put(200, "OK");
    put(203, "NOT AUTHORITATIVE");
    put(204, "NO CONTENT");
    put(205, "RESET");
    put(206, "PARTIAL");
    put(300, "MULT CHOICE");
    put(301, "MOVED PERM");
    put(302, "MOVED TEMP");
    put(303, "SEE OTHER");
    put(304, "NOT MODIFIED");
    put(305, "USE PROXY");
    put(400, "BAD REQUEST");
    put(401, "UNAUTHORIZED");
    put(402, "PAYMENT REQUIRED");
    put(403, "FORBIDDEN");
    put(404, "NOT FOUND");
    put(405, "BAD METHOD");
    put(406, "NOT ACCEPTABLE");
    put(407, "PROXY AUTH");
    put(408, "CLIENT TIMEOUT");
    put(409, "CONFLICT");
    put(410, "GONE");
    put(411, "LENGTH REQUIRED");
    put(412, "PRECON FAILED");
    put(413, "ENTITY TOO LARGE");
    put(414, "REQ TOO LONG");
    put(415, "UNSUPPORTED TYPE");
    put(500, "SERVER ERROR");
    put(500, "INTERNAL ERROR");
    put(501, "NOT IMPLEMENTED");
    put(502, "BAD GATEWAY");
    put(503, "UNAVAILABLE");
    put(504, "GATEWAY TIMEOUT");
    put(505, "VERSION");
  }};

  int defaultTimeout = Integer.valueOf(DEFAULT_TIMEOUT);
  String fastpathFileHandle;
  FastpathResponse response = null;

  public String getFastpathFileHandle() {
    return fastpathFileHandle;
  }

  public AbfsFastpathConnection(final AbfsRestOperationType opType,
      final URL url,
      final String method,
      final AuthType authType,
      final String authToken,
      List<AbfsHttpHeader> requestHeaders,
      final String fastpathFileHandle) throws IOException {
    super(opType, url, method, authType, authToken, requestHeaders);
    this.authType = authType;
    this.authToken = authToken;
    this.fastpathFileHandle = fastpathFileHandle;
    this.requestHeaders = requestHeaders;
  }

  public String getResponseHeader(String httpHeader) {
    return response.getResponseHeaders().get(httpHeader);
  }

  public Map<String, List<String>> getRequestHeaders() {
    final Map<String, List<String>> headers
        = new HashMap<String, java.util.List<String>>();
    for (AbfsHttpHeader abfsHeader : this.requestHeaders) {
      headers.put(abfsHeader.getName(),
          Collections.singletonList(abfsHeader.getValue()));
    }

    headers.put(
        X_MS_CLIENT_REQUEST_ID,
        Collections.singletonList(this.clientRequestId));

    return headers;
  }

  public String getRequestHeader(String header) {
    String value = EMPTY_STRING;
    for (AbfsHttpHeader abfsHeader : this.requestHeaders) {
      if (abfsHeader.getName().equals(header)) {
        value = abfsHeader.getValue();
        break;
      }
    }

    return value;
  }

  public String getClientRequestId() {
    return getRequestHeader(X_MS_CLIENT_REQUEST_ID);
  }

  public void setHeader(String header, String value) {
    this.requestHeaders.add(new AbfsHttpHeader(header, value));
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
    switch (this.opType) {
    case FastpathOpen:
      long startTime = System.nanoTime();
      processFastpathOpenResponse();
      this.recvResponseTimeMs = elapsedTimeMs(startTime);
      break;
    case FastpathRead:
      startTime = System.nanoTime();
      processFastpathReadResponse(buffer, offset, length);
      this.recvResponseTimeMs = elapsedTimeMs(startTime);
      break;
    case FastpathClose:
      startTime = System.nanoTime();
      processFastpathCloseResponse();
      this.recvResponseTimeMs = elapsedTimeMs(startTime);
      break;
    default:
      throw new FastpathException("Invalid state");
    }
  }

  private void setStatusFromFastpathResponse(FastpathResponse response) {
    this.response = response;
    this.statusCode = response.getHttpStatus();
    this.statusDescription = (STATUS_CODE_MAP.containsKey(this.statusCode)
        ? STATUS_CODE_MAP.get(this.statusCode)
        : DEFAULT_VALUE_UNKNOWN);
    this.storageErrorCode = String.valueOf(response.getStoreErrorCode());
    this.storageErrorMessage = response.getStoreErrorDescription();
  }

  private AccessTokenType getAccessTokenType(AuthType authType)
      throws FastpathException {
    if (authType == OAuth) {
      return AccessTokenType.AadBearer;
    }

    throw new FastpathException("Unsupported authType for Fastpath connection");
  }

  private void processFastpathOpenResponse() throws AbfsFastpathException {
    FastpathOpenRequestParams openRequestParams;
    FastpathOpenResponse openResponse = null;
    try {
      openRequestParams = new FastpathOpenRequestParams(
          url,
          getAccessTokenType(authType),
          authToken,
          getRequestHeaders(),
          defaultTimeout);
      openResponse = triggerOpen(openRequestParams);
      setStatusFromFastpathResponse(openResponse);
    } catch (FastpathException ex) {
      handleFastpathException(ex);
    }

    if ((openResponse != null) && (openResponse.isSuccessResponse())) {
      this.fastpathFileHandle = openResponse.getFastpathFileHandle();
      LOG.debug("Fast path open successful [Handle={}]", this.fastpathFileHandle);
    }
  }

  @VisibleForTesting
  protected FastpathOpenResponse triggerOpen(FastpathOpenRequestParams openRequestParams)
      throws FastpathException {
    FastpathConnection conn = new FastpathConnection();
    return conn.open(openRequestParams);
  }

  private void processFastpathReadResponse(final byte[] buffer,
      final int buffOffset, final int length) throws AbfsFastpathException {
    FastpathReadRequestParams readRequestParams = null;
    FastpathReadResponse readResponse = null;
    try {
      readRequestParams = new FastpathReadRequestParams(url,
          getAccessTokenType(authType),
          authToken, getRequestHeaders(),
          defaultTimeout, buffOffset, fastpathFileHandle);
      readResponse = triggerRead(readRequestParams, buffer);
      setStatusFromFastpathResponse(readResponse);
    } catch (FastpathException ex) {
      handleFastpathException(ex);
    }

    if ((readResponse != null) && (readResponse.isSuccessResponse())) {
      this.bytesReceived = readResponse.getBytesRead();
      LOG.debug("Fast path read successful [Handle={}] - bytes received = {} ",
          this.fastpathFileHandle, this.bytesReceived);
    }
  }

  @VisibleForTesting
  protected FastpathReadResponse triggerRead(
      FastpathReadRequestParams readRequestParams,
      byte[] buffer) throws FastpathException {
    FastpathConnection conn = new FastpathConnection();
    return conn.read(readRequestParams, buffer);
  }

  private void processFastpathCloseResponse() throws AbfsFastpathException {
    FastpathCloseRequestParams closeRequestParams = null;
    FastpathCloseResponse closeResponse = null;
    try {
      closeRequestParams
          = new FastpathCloseRequestParams(url, getAccessTokenType(authType),
          authToken, getRequestHeaders(), defaultTimeout, fastpathFileHandle);
      closeResponse = triggerClose(closeRequestParams);
      setStatusFromFastpathResponse(closeResponse);
    } catch (FastpathException ex) {
      handleFastpathException(ex);
    }

    if ((closeResponse != null) && (closeResponse.isSuccessResponse())) {
      LOG.debug("Fast path close successful [Handle={}]", this.fastpathFileHandle);
    }
  }

  private void handleFastpathException(final FastpathException ex)
      throws AbfsFastpathException {
    if (ex instanceof FastpathRequestException) {
      throw new AbfsFastpathException(ex.getMessage(), ex);
    }

    throw new AbfsFastpathException(ex.getMessage(), ex);
  }


  @VisibleForTesting
  protected FastpathCloseResponse triggerClose(
      FastpathCloseRequestParams closeRequestParams) throws FastpathException {
    FastpathConnection conn = new FastpathConnection();
    return conn.close(closeRequestParams);
  }
}
