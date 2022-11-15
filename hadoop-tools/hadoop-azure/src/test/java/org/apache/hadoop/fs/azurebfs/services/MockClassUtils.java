package org.apache.hadoop.fs.azurebfs.services;

import java.io.IOException;
import java.net.URL;
import java.util.List;

import org.mockito.Mockito;

public class MockClassUtils {

  private MockClassUtils() {
  }

  public static void mockAbfsClientGetAbfsRestOperation(MockClassIntercepter mockClassIntercepter,
      AbfsClient mockedClient) {
    Mockito.doAnswer(answer -> {
      return (AbfsRestOperation) mockClassIntercepter.intercept(answer,
          mockedClient);
    }).when(mockedClient).getAbfsRestOperation(
        Mockito.any(AbfsRestOperationType.class),
        Mockito.anyString(),
        Mockito.any(URL.class),
        Mockito.anyList(),
        Mockito.any(byte[].class),
        Mockito.anyInt(),
        Mockito.anyInt(),
        Mockito.anyString()
    );
  }

  public static void mockAbfsRestOperationGetHttpOperation(MockClassIntercepter mockClassIntercepter,
      AbfsRestOperation abfsRestOperation)
      throws IOException {
    Mockito.doAnswer(answer -> {
      return (AbfsHttpOperation) mockClassIntercepter.intercept(answer,
          abfsRestOperation);
    }).when(abfsRestOperation).getHttpOperation();
  }

  public static void mockAbfsHttpOperationProcessResponse(MockClassIntercepter mockClassIntercepter,
      AbfsHttpOperation httpOp) throws IOException {
    Mockito.doAnswer(answer -> {
      return mockClassIntercepter.intercept(answer);
    }).when(httpOp).processResponse(
        Mockito.any(byte[].class),
        Mockito.anyInt(),
        Mockito.anyInt()
    );
  }

  public static void setHttpOpStatus(int status, AbfsHttpOperation abfsHttpOperation) {
    abfsHttpOperation.setStatusCode(status);
  }

  public static void setHttpOpBytesReceived(int bytesReceived, AbfsHttpOperation httpOperation) {
    httpOperation.setBytesReceived(bytesReceived);
  }

  public static AbfsRestOperation createAbfsRestOperation(AbfsRestOperationType operationType,
      AbfsClient client,
      String method,
      URL url,
      List<AbfsHttpHeader> requestHeaders,
      byte[] buffer,
      int bufferOffset,
      int bufferLength,
      String sasToken) {
    return new AbfsRestOperation(operationType, client, method, url,
        requestHeaders, buffer, bufferOffset, bufferLength, sasToken);
  }

}
