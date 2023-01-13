package org.apache.hadoop.fs.azurebfs.services;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;
import org.mockito.Mockito;

public class TestAbfsRestOperation {

  @Test
  public void testClientRequestIdForTimeoutRetry() throws Exception {
    List<AbfsHttpHeader> headers = new ArrayList<>();

    AbfsRestOperation abfsRestOperation = Mockito.spy(new AbfsRestOperation(
      AbfsRestOperationType.ReadFile,
        Mockito.mock(AbfsClient.class),
        "PUT",
        null,
        new ArrayList<>()
    ));

    AbfsHttpOperation httpOperation = Mockito.spy(new AbfsHttpOperation(null, "PUT", new ArrayList<>()));

  }
}
