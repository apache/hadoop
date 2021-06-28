/*
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
import java.util.List;

import com.azure.storage.fastpath.exceptions.FastpathException;
import org.apache.hadoop.fs.azurebfs.utils.MockFastpathConnection;
import com.azure.storage.fastpath.requestParameters.FastpathCloseRequestParams;
import com.azure.storage.fastpath.requestParameters.FastpathOpenRequestParams;
import com.azure.storage.fastpath.requestParameters.FastpathReadRequestParams;
import com.azure.storage.fastpath.responseProviders.FastpathCloseResponse;
import com.azure.storage.fastpath.responseProviders.FastpathOpenResponse;
import com.azure.storage.fastpath.responseProviders.FastpathReadResponse;

import org.apache.hadoop.fs.azurebfs.AbfsConfiguration;

public class MockAbfsFastpathConnection extends AbfsFastpathConnection {

  int errStatus = 0;
  boolean mockRequestException = false;
  boolean mockConnectionException = false;

  public MockAbfsFastpathConnection(final org.apache.hadoop.fs.azurebfs.services.AbfsRestOperationType opType,
      final URL url,
      final String method,
      final AuthType authType,
      final String authToken,
      final List<AbfsHttpHeader> requestHeaders,
      final String fastpathFileHandle) throws IOException {
    super(opType, url, method, authType, authToken, requestHeaders,
        fastpathFileHandle);
  }

  protected FastpathOpenResponse triggerOpen(FastpathOpenRequestParams openRequestParams)
      throws FastpathException {
    MockFastpathConnection conn = new MockFastpathConnection();
    signalErrorConditionToMockFastpathConn(conn);
    return conn.open(openRequestParams);
  }

  protected FastpathReadResponse triggerRead(FastpathReadRequestParams readRequestParams, byte[] buffer)
      throws FastpathException {
    MockFastpathConnection conn = new MockFastpathConnection();
    signalErrorConditionToMockFastpathConn(conn);
    return conn.read(readRequestParams, buffer);
  }

  protected FastpathCloseResponse triggerClose(FastpathCloseRequestParams closeRequestParams)
      throws FastpathException {
    MockFastpathConnection conn = new MockFastpathConnection();
    signalErrorConditionToMockFastpathConn(conn);
    return conn.close(closeRequestParams);
  }

  private void signalErrorConditionToMockFastpathConn(MockFastpathConnection conn) {
    if (errStatus != 0) {
      conn.induceError(errStatus);
    }

    if (mockRequestException) {
      conn.induceRequestException();
    }

    if (mockConnectionException) {
      conn.induceConnectionException();
    }
  }

  public void induceError(int httpStatus) {
    errStatus = httpStatus;
  }

  public void induceRequestException() {
    mockRequestException = true;
  }

  public void induceConnectionException() {
    mockConnectionException = true;
  }

  public void resetAllMockErrStates() {
    errStatus = 0;
    mockRequestException = false;
    mockConnectionException = false;
  }
}
