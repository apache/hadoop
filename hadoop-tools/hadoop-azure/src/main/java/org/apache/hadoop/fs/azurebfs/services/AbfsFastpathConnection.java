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
import static org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations.X_MS_CLIENT_REQUEST_ID;

/**
 * Represents a Fastpath operation.
 */
public class AbfsFastpathConnection extends AbfsHttpOperation {
  private int defaultTimeout = Integer.parseInt(DEFAULT_TIMEOUT);
  private AbfsFastpathSessionInfo fastpathSessionInfo;
  private FastpathResponse response = null;

  public String getFastpathFileHandle() {
    return fastpathSessionInfo.getFastpathFileHandle();
  }

  public AbfsFastpathConnection(final AbfsRestOperationType opType,
      final URL url,
      final String method,
      final AuthType authType,
      final String authToken,
      List<AbfsHttpHeader> requestHeaders,
      final AbfsFastpathSessionInfo fastpathSessionInfo) throws IOException {
    super(opType, url, method, authType, authToken, requestHeaders);
    setAuthType(authType);
    setAuthToken(authToken);
    this.fastpathSessionInfo = fastpathSessionInfo;
    setAbfsHttpHeaders(requestHeaders);
  }

  public String getResponseHeader(String httpHeader) {
    return response.getResponseHeaders().get(httpHeader);
  }

  public Map<String, List<String>> getRequestHeaders() {
    final Map<String, List<String>> headers
        = new HashMap<String, java.util.List<String>>();
    for (AbfsHttpHeader abfsHeader : getAbfsHttpHeaders()) {
      headers.put(abfsHeader.getName(),
          Collections.singletonList(abfsHeader.getValue()));
    }

    headers.put(
        X_MS_CLIENT_REQUEST_ID,
        Collections.singletonList(getClientRequestId()));

    return headers;
  }

  public String getRequestHeader(String header) {
    String value = EMPTY_STRING;
    for (AbfsHttpHeader abfsHeader : getAbfsHttpHeaders()) {
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
    getAbfsHttpHeaders().add(new AbfsHttpHeader(header, value));
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
    switch (getOpType()) {
    case FastpathOpen:
      long startTime = System.nanoTime();
      processFastpathOpenResponse();
      setRecvResponseTimeMs(elapsedTimeMs(startTime));
      break;
    case FastpathRead:
      startTime = System.nanoTime();
      processFastpathReadResponse(buffer, offset, length);
      setRecvResponseTimeMs(elapsedTimeMs(startTime));
      break;
    case FastpathClose:
      startTime = System.nanoTime();
      processFastpathCloseResponse();
      setRecvResponseTimeMs(elapsedTimeMs(startTime));
      break;
    default:
      throw new FastpathException("Invalid state");
    }
  }

  private void setStatusFromFastpathResponse(FastpathResponse response) {
    this.response = response;
    setStatusCode(response.getHttpStatus());
    setStatusDescription(response.getHttpStatusDescription());
    setStorageErrorCode(String.valueOf(response.getStoreErrorCode()));
    setStorageErrorMessage(response.getStoreErrorDescription());
  }

  private AccessTokenType getAccessTokenType() {
    return AccessTokenType.FastpathSession;
  }

  private void processFastpathOpenResponse() throws AbfsFastpathException {
    FastpathOpenRequestParams openRequestParams;
    FastpathOpenResponse openResponse = null;
    try {
      openRequestParams = new FastpathOpenRequestParams(
          getUrl(),
          getAccessTokenType(),
          fastpathSessionInfo.getSessionToken(),
          getRequestHeaders(),
          defaultTimeout);
      openResponse = triggerOpen(openRequestParams);
      if (openResponse != null) {
        if (openResponse.isSuccessResponse()) {
          this.fastpathSessionInfo.setFastpathFileHandle(
              openResponse.getFastpathFileHandle());
          LOG.debug("Fast path open successful [Handle={}]",
              this.fastpathSessionInfo.getFastpathFileHandle());
        }

        setStatusFromFastpathResponse(openResponse);
      }
    } catch (FastpathException ex) {
      handleFastpathException(ex);
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
      readRequestParams = new FastpathReadRequestParams(getUrl(),
          getAccessTokenType(),
          fastpathSessionInfo.getSessionToken(),
          getRequestHeaders(),
          defaultTimeout, buffOffset, fastpathSessionInfo.getFastpathFileHandle());
      readResponse = triggerRead(readRequestParams, buffer);
      if (readResponse != null) {
        if (readResponse.isSuccessResponse()) {
          setBytesReceived(readResponse.getBytesRead());
          LOG.debug(
              "Fast path read successful [Handle={}] - bytes received = {} ",
              this.fastpathSessionInfo.getFastpathFileHandle(),
              getBytesReceived());
          setResponseContentBuffer(buffer);
        }

        setStatusFromFastpathResponse(readResponse);
      }
    } catch (FastpathException ex) {
      handleFastpathException(ex);
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
          = new FastpathCloseRequestParams(getUrl(), getAccessTokenType(),
          fastpathSessionInfo.getSessionToken(), getRequestHeaders(),
          defaultTimeout, fastpathSessionInfo.getFastpathFileHandle());
      closeResponse = triggerClose(closeRequestParams);
      if (closeResponse != null) {
        if (closeResponse.isSuccessResponse()) {
          LOG.debug("Fast path close successful [Handle={}]",
              fastpathSessionInfo.getFastpathFileHandle());
        }

        setStatusFromFastpathResponse(closeResponse);
      }
    } catch (FastpathException ex) {
      handleFastpathException(ex);
    }
  }

  private void handleFastpathException(final FastpathException ex)
      throws AbfsFastpathException {
    LOG.debug("Received FastpathException: {}", ex.toString());
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
