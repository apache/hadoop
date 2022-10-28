package org.apache.hadoop.fs.azurebfs.services;

import java.io.IOException;
import java.net.URL;
import java.util.List;

public class MockHttpOperation extends AbfsHttpOperation {

  public MockHttpOperation(final URL url,
      final String method,
      final List<AbfsHttpHeader> requestHeaders) throws IOException {
    super(url, method, requestHeaders);
  }
}
