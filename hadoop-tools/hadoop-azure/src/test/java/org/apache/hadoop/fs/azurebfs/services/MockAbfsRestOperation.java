package org.apache.hadoop.fs.azurebfs.services;

import java.io.IOException;
import java.net.URL;
import java.util.List;

import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AzureBlobFileSystemException;
import org.apache.hadoop.fs.azurebfs.utils.TracingContext;

public class MockAbfsRestOperation extends AbfsRestOperation {

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
    return new MockHttpOperation(getUrl(), getMethod(), getRequestHeaders());
  }
}
