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

import static org.apache.hadoop.fs.azurebfs.constants.TestConfigurationKeys.FS_AZURE_TEST_FASTPATH_MOCK_SO_ENABLED;

public class MockAbfsRestOperation extends AbfsRestOperation {

  int errStatus = 0;
  boolean mockRequestException = false;
  boolean mockConnectionException = false;
  boolean fastpathMockSoEnabled = true;

  MockAbfsRestOperation(final AbfsRestOperationType operationType,
      final AbfsClient client,
      final String method,
      final URL url,
      final List<AbfsHttpHeader> requestHeaders) {
    super(operationType, client, method, url, requestHeaders);
    fastpathMockSoEnabled = client.getAbfsConfiguration().getRawConfiguration()
        .getBoolean(FS_AZURE_TEST_FASTPATH_MOCK_SO_ENABLED, true);
  }

  MockAbfsRestOperation(final AbfsRestOperationType operationType,
      final AbfsClient client,
      final String method,
      final URL url,
      final List<AbfsHttpHeader> requestHeaders,
      final String sasToken) {
    super(operationType, client, method, url, requestHeaders, sasToken);
    fastpathMockSoEnabled = client.getAbfsConfiguration().getRawConfiguration()
        .getBoolean(FS_AZURE_TEST_FASTPATH_MOCK_SO_ENABLED, true);
  }

  MockAbfsRestOperation(final AbfsRestOperationType operationType,
      final AbfsClient client,
      final String method,
      final URL url,
      final List<AbfsHttpHeader> requestHeaders,
      final String sasToken,
      final String fastpathFileHandle) {
    super(operationType, client, method, url, requestHeaders, sasToken, fastpathFileHandle);
    fastpathMockSoEnabled = client.getAbfsConfiguration().getRawConfiguration()
        .getBoolean(FS_AZURE_TEST_FASTPATH_MOCK_SO_ENABLED, true);
  }

  MockAbfsRestOperation(final AbfsRestOperationType operationType,
      final AbfsClient client,
      final String method,
      final URL url,
      final List<AbfsHttpHeader> requestHeaders,
      final AbfsRestIODataParameters ioDataParams,
      final String sasToken) {
    super(operationType, client, method, url, requestHeaders, ioDataParams, sasToken);
    fastpathMockSoEnabled = client.getAbfsConfiguration().getRawConfiguration()
        .getBoolean(FS_AZURE_TEST_FASTPATH_MOCK_SO_ENABLED, true);
  }

  protected AbfsFastpathConnection getFastpathConnection() throws IOException {
    return new MockAbfsFastpathConnection(operationType, url, method,
        client.getAuthType(), client.getAccessToken(), requestHeaders,
        fastpathFileHandle, fastpathMockSoEnabled);
  }

  // is this needed
  protected void processResponse(AbfsHttpOperation httpOperation) throws IOException {
    if (isAFastpathRequest()) {
      signalErrorConditionToMockAbfsFastpathConn((MockAbfsFastpathConnection)httpOperation);
      ((MockAbfsFastpathConnection)httpOperation).processResponse(buffer, bufferOffset, bufferLength);
    } else {
      httpOperation.processResponse(buffer, bufferOffset, bufferLength);
    }
  }

  private void signalErrorConditionToMockAbfsFastpathConn(MockAbfsFastpathConnection httpOperation) {
    if (errStatus != 0) {
      httpOperation.induceError(errStatus);
    }

    if (mockRequestException) {
      httpOperation.induceRequestException();
    }

    if (mockConnectionException) {
      httpOperation.induceConnectionException();
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
