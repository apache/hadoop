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
    MockHttpOperation op = new MockHttpOperation(getUrl(), getMethod(), getRequestHeaders());
    op.setMockHttpOperationTestIntercept(mockHttpOperationTestIntercept);
    return op;
  }

  public void setMockHttpOperationTestIntercept(final MockHttpOperationTestIntercept mockHttpOperationTestIntercept) {
    this.mockHttpOperationTestIntercept = mockHttpOperationTestIntercept;
  }
}
