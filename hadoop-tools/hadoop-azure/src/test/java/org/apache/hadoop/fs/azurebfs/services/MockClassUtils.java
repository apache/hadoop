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
    })
.when(mockedClient).getAbfsRestOperation(
        Mockito.nullable(AbfsRestOperationType.class),
        Mockito.nullable(String.class),
        Mockito.nullable(URL.class),
        Mockito.nullable(List.class),
        Mockito.nullable(byte[].class),
        Mockito.nullable(int.class),
        Mockito.nullable(int.class),
        Mockito.nullable(String.class)
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
      return mockClassIntercepter.intercept(answer, httpOp);
    }).when(httpOp).processResponse(
        Mockito.nullable(byte[].class),
        Mockito.nullable(Integer.class),
        Mockito.nullable(Integer.class)
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
