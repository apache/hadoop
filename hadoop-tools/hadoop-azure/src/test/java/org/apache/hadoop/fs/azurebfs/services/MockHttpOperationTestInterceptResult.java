package org.apache.hadoop.fs.azurebfs.services;

import java.io.IOException;

public class MockHttpOperationTestInterceptResult {
  public int bytesRead;
  public int status;
  public IOException exception;
}
