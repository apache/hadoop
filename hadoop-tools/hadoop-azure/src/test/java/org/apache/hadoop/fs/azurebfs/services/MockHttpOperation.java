/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.fs.azurebfs.services;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.List;

public class MockHttpOperation extends AbfsHttpOperation {

  private MockHttpOperationTestIntercept mockHttpOperationTestIntercept;

  public MockHttpOperation(final URL url,
      final String method,
      final List<AbfsHttpHeader> requestHeaders) throws IOException {
    super(url, method, requestHeaders);
  }

  @Override
  public void processResponse(final byte[] buffer,
      final int offset,
      final int length) throws IOException {
    MockHttpOperationTestInterceptResult result =
        mockHttpOperationTestIntercept.intercept(this, buffer, offset, length);
    statusCode = result.status;
    bytesReceived = result.bytesRead;
    if(result.exception != null) {
      throw result.exception;
    }
  }

  public void processResponseSuperCall(final byte[] buffer,
      final int offset,
      final int length) throws IOException {
    super.processResponse(buffer, offset, length);
  }

  public void setMockHttpOperationTestIntercept(final MockHttpOperationTestIntercept mockHttpOperationTestIntercept) {
    this.mockHttpOperationTestIntercept = mockHttpOperationTestIntercept;
  }

  @Override
  public HttpURLConnection getConnection() {
    return super.getConnection();
  }
}
