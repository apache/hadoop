/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.fs.azurebfs.services;

import java.io.IOException;
import java.net.URL;
import java.util.List;

import org.mockito.Mockito;

/**
 * Utility class to expose methods to attach mocking logic to package-protected
 * methods of
 * <ol>
 *   <li>{@link org.apache.hadoop.fs.azurebfs.services.AbfsClient}</li>
 *   <li>{@link org.apache.hadoop.fs.azurebfs.services.AbfsRestOperation}</li>
 * </ol>
 * */
public class MockClassUtils {

  private MockClassUtils() {
  }

  public static void mockAbfsClientGetAbfsRestOperation(MockClassInterceptor mockClassInterceptor,
      AbfsClient mockedClient) {
    Mockito.doAnswer(answer -> {
          return (AbfsRestOperation) mockClassInterceptor.intercept(answer,
              mockedClient);
        })
        .when(mockedClient).getAbfsRestOperation(
            Mockito.nullable(AbfsRestOperationType.class),
            Mockito.nullable(String.class),
            Mockito.nullable(URL.class),
            (List<AbfsHttpHeader>) Mockito.nullable(List.class),
            Mockito.nullable(byte[].class),
            Mockito.nullable(int.class),
            Mockito.nullable(int.class),
            Mockito.nullable(String.class)
        );
  }

  public static void mockAbfsRestOperationGetHttpOperation(MockClassInterceptor mockClassInterceptor,
      AbfsRestOperation abfsRestOperation)
      throws IOException {
    Mockito.doAnswer(answer -> {
      return (AbfsHttpOperation) mockClassInterceptor.intercept(answer,
          abfsRestOperation);
    }).when(abfsRestOperation).getHttpOperation();
  }

  public static void mockAbfsHttpOperationProcessResponse(MockClassInterceptor mockClassInterceptor,
      AbfsHttpOperation httpOp) throws IOException {
    Mockito.doAnswer(answer -> {
      return mockClassInterceptor.intercept(answer, httpOp);
    }).when(httpOp).processResponse(
        Mockito.nullable(byte[].class),
        Mockito.nullable(Integer.class),
        Mockito.nullable(Integer.class)
    );
  }

  public static void setHttpOpStatus(int status,
      AbfsHttpOperation abfsHttpOperation) {
    abfsHttpOperation.setStatusCode(status);
  }

  public static void setHttpOpBytesReceived(int bytesReceived,
      AbfsHttpOperation httpOperation) {
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
