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
