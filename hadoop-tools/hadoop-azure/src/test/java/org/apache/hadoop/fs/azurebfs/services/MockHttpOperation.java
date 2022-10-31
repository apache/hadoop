package org.apache.hadoop.fs.azurebfs.services;

import java.io.IOException;
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
//    super.processResponse(buffer, offset, length);
    MockHttpOperationTestInterceptResult result =
        mockHttpOperationTestIntercept.intercept();
    statusCode = result.status;
    bytesReceived = result.bytesRead;
  }

  public void setMockHttpOperationTestIntercept(final MockHttpOperationTestIntercept mockHttpOperationTestIntercept) {
    this.mockHttpOperationTestIntercept = mockHttpOperationTestIntercept;
  }
}
