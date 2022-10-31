package org.apache.hadoop.fs.azurebfs.services;

import java.io.IOException;

public interface MockHttpOperationTestIntercept {
  void intercept() throws IOException;
}
