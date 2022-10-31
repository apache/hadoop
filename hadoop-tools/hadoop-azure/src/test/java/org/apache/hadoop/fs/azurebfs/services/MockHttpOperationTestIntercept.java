package org.apache.hadoop.fs.azurebfs.services;

import java.io.IOException;

public interface MockHttpOperationTestIntercept {
  MockHttpOperationTestInterceptResult intercept() throws IOException;
  int getCallCount();
}
