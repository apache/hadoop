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

import org.apache.hadoop.fs.azurebfs.utils.MockFastpathConnection;

import static org.apache.hadoop.fs.azurebfs.constants.TestConfigurationKeys.FS_AZURE_TEST_FASTPATH_MOCK_SO_ENABLED;

public class MockAbfsRestOperation extends AbfsRestOperation {

  private int errStatus = 0;
  private boolean mockRequestException = false;
  private boolean mockConnectionException = false;

  MockAbfsRestOperation(final AbfsRestOperationType operationType,
      final AbfsClient client,
      final String method,
      final URL url,
      final List<AbfsHttpHeader> requestHeaders,
      final AbfsFastpathSessionInfo fastpathSessionInfo) {
    super(operationType, client, method, url, requestHeaders, fastpathSessionInfo);
  }

  MockAbfsRestOperation(AbfsRestOperationType operationType,
      AbfsClient client,
      String method,
      URL url,
      List<AbfsHttpHeader> requestHeaders,
      byte[] buffer,
      int bufferOffset,
      int bufferLength,
      AbfsFastpathSessionInfo fastpathSessionInfo) {
    super(operationType, client, method, url, requestHeaders, buffer,
        bufferOffset, bufferLength, fastpathSessionInfo);
  }

  protected AbfsFastpathConnection getFastpathConnection() throws IOException {
    return new MockAbfsFastpathConnection(operationType, url, method,
        client.getAuthType(), client.getAccessToken(), requestHeaders,
        fastpathSessionInfo);
  }

  private void setEffectiveMock() {
    MockFastpathConnection.setTestMock(client.getAbfsConfiguration()
        .getRawConfiguration()
        .getBoolean(FS_AZURE_TEST_FASTPATH_MOCK_SO_ENABLED, false));
  }

  protected void processResponse(AbfsHttpOperation httpOperation) throws IOException {
    if (isAFastpathRequest()) {
      setEffectiveMock();
      signalErrorConditionToMockAbfsFastpathConn((MockAbfsFastpathConnection) httpOperation);
      ((MockAbfsFastpathConnection) httpOperation).processResponse(buffer, bufferOffset, bufferLength);
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
