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
import java.net.URL;
import java.util.List;

public class MockAbfsRestOperation extends AbfsRestOperation {

  private MockHttpOperationTestIntercept mockHttpOperationTestIntercept;

  public MockAbfsRestOperation(final AbfsRestOperationType operationType,
      final AbfsClient client,
      final String method,
      final URL url,
      final List<AbfsHttpHeader> requestHeaders,
      final byte[] buffer,
      final int bufferOffset,
      final int bufferLength,
      final String sasToken) {
    super(operationType, client, method, url, requestHeaders, buffer,
        bufferOffset, bufferLength, sasToken);
  }

  @Override
  AbfsHttpOperation getHttpOperation() throws IOException {
    MockHttpOperation op = new MockHttpOperation(getUrl(), getMethod(),
        getRequestHeaders());
    op.setMockHttpOperationTestIntercept(mockHttpOperationTestIntercept);
    return op;
  }

  public void setMockHttpOperationTestIntercept(final MockHttpOperationTestIntercept mockHttpOperationTestIntercept) {
    this.mockHttpOperationTestIntercept = mockHttpOperationTestIntercept;
  }
}
