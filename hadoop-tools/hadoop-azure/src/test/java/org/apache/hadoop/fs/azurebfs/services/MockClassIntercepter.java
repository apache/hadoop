package org.apache.hadoop.fs.azurebfs.services;

import java.io.IOException;

import org.mockito.invocation.InvocationOnMock;

public interface MockClassIntercepter {
  public Object intercept(InvocationOnMock invocationOnMock, Object... objects)
      throws IOException;
}
