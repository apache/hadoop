package org.apache.hadoop.fs.azurebfs.services;

import java.io.IOException;

public interface MockHttpOperationTestIntercept {
  MockHttpOperationTestInterceptResult intercept(final MockHttpOperation mockHttpOperation,
      final byte[] buffer,
      final int offset,
      final int length) throws IOException;
  int getCallCount();
}
