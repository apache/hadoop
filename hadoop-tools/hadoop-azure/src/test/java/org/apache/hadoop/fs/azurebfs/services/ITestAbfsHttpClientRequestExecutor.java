/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.fs.azurebfs.services;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URL;

import org.assertj.core.api.Assertions;
import org.junit.Test;
import org.mockito.Mockito;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.azurebfs.AbstractAbfsIntegrationTest;
import org.apache.hadoop.fs.azurebfs.AzureBlobFileSystem;
import org.apache.hadoop.fs.azurebfs.AzureBlobFileSystemStore;
import org.apache.hadoop.fs.azurebfs.utils.TracingContext;
import org.apache.http.HttpClientConnection;
import org.apache.http.HttpEntityEnclosingRequest;
import org.apache.http.HttpException;
import org.apache.http.HttpRequest;

import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_NETWORKING_LIBRARY;
import static org.apache.hadoop.fs.azurebfs.services.HttpOperationType.APACHE_HTTP_CLIENT;
import static org.apache.hadoop.test.LambdaTestUtils.intercept;

public class ITestAbfsHttpClientRequestExecutor extends
    AbstractAbfsIntegrationTest {

  public ITestAbfsHttpClientRequestExecutor() throws Exception {
    super();
  }

  @Test
  public void testExpect100ContinueHandling() throws Exception {
    AzureBlobFileSystem fs = getFileSystem();
    Path path = new Path("/testExpect100ContinueHandling");

    Configuration conf = new Configuration(fs.getConf());
    conf.set(FS_AZURE_NETWORKING_LIBRARY, APACHE_HTTP_CLIENT.toString());
    AzureBlobFileSystem fs2 = Mockito.spy(
        (AzureBlobFileSystem) FileSystem.newInstance(conf));

    AzureBlobFileSystemStore store = Mockito.spy(fs2.getAbfsStore());
    Mockito.doReturn(store).when(fs2).getAbfsStore();

    AbfsClient client = Mockito.spy(store.getClient());
    Mockito.doReturn(client).when(store).getClient();

    final int[] invocation = {0};
    Mockito.doAnswer(answer -> {
      AbfsRestOperation op = Mockito.spy(
          (AbfsRestOperation) answer.callRealMethod());
      final int[] sendHeadersInvocation = {0};
      final int[] sendBodyInvocation = {0};
      final int[] receiveResponseInvocation = {0};
      Mockito.doAnswer(httpOpCreationAnswer -> {
        AbfsAHCHttpOperation httpOperation = Mockito.spy(
            (AbfsAHCHttpOperation) httpOpCreationAnswer.callRealMethod());
        Mockito.doAnswer(createContextAnswer -> {
              AbfsApacheHttpClient.AbfsHttpClientContext context = Mockito.spy(
                  (AbfsApacheHttpClient.AbfsHttpClientContext) createContextAnswer.callRealMethod());
              Mockito.doAnswer(connectionSpyIntercept -> {
                return interceptedConn(invocation[0], sendHeadersInvocation,
                    sendBodyInvocation, receiveResponseInvocation,
                    (HttpClientConnection) connectionSpyIntercept.getArgument(0));
              }).when(context).interceptConnectionActivity(Mockito.any(
                  HttpClientConnection.class));
              return context;
            })
            .when(httpOperation).setFinalAbfsClientContext(Mockito.anyString());
        return httpOperation;
      }).when(op).createHttpOperation();
      //TODO: keep checking what are connections state on the iteration of op.execute.
      Mockito.doAnswer(executeAnswer -> {
        Throwable throwable = intercept(IOException.class, () -> {
          try {
            executeAnswer.callRealMethod();
          } catch (IOException ex) {
            //This exception is expected to be thrown by the op.execute() method.
            throw ex;
          } catch (Throwable interceptedAssertedThrowable) {
            //Any other throwable thrown by Mockito's callRealMethod would be
            //considered as an assertion error.
          }
        });
        invocation[0]++;
        /*
         * The first call would be with expect headers, and expect 100 continue assertion has to happen which would fail.
         * For expect100 assertion to happen, header IO happens before body IO. If assertion fails, no body IO happens.
         * The second call would not be using expect headers.
         *
         * The third call would be with expect headers, and expect 100 continue assertion has to happen which would pass.
         */
        if (invocation[0] == 1) {
          Assertions.assertThat(sendHeadersInvocation[0]).isEqualTo(1);
          Assertions.assertThat(sendBodyInvocation[0]).isEqualTo(0);
          Assertions.assertThat(receiveResponseInvocation[0]).isEqualTo(1);
        }
        if (invocation[0] == 2) {
          Assertions.assertThat(sendHeadersInvocation[0]).isEqualTo(1);
          Assertions.assertThat(sendBodyInvocation[0]).isEqualTo(1);
          Assertions.assertThat(receiveResponseInvocation[0]).isEqualTo(1);
        }
        if (invocation[0] == 3) {
          Assertions.assertThat(sendHeadersInvocation[0]).isEqualTo(1);
          Assertions.assertThat(sendBodyInvocation[0]).isEqualTo(1);
          Assertions.assertThat(receiveResponseInvocation[0]).isEqualTo(1);
        }
        Assertions.assertThat(invocation[0]).isLessThanOrEqualTo(3);
        if (throwable != null) {
          throw throwable;
        }
        return null;
      }).when(op).execute(Mockito.any(TracingContext.class));
      return op;
    }).when(client).getAbfsRestOperation(
        Mockito.any(AbfsRestOperationType.class),
        Mockito.anyString(),
        Mockito.any(URL.class),
        Mockito.anyList(),
        Mockito.any(byte[].class),
        Mockito.anyInt(),
        Mockito.anyInt(),
        Mockito.nullable(String.class));

    final OutputStream os = fs2.create(path);
    fs.delete(path, true);
    intercept(FileNotFoundException.class, () -> {
      os.write(1);
      os.close();
    });

    final OutputStream os2 = fs2.create(path);
    os2.write(1);
    os.close();
  }

  private HttpClientConnection interceptedConn(final int i,
      final int[] sendHeadersInvocation,
      final int[] sendBodyInvocation,
      final int[] receiveResponseInvocation,
      final HttpClientConnection connection) throws IOException, HttpException {
    HttpClientConnection interceptedConn = Mockito.spy(connection);
    Mockito.doAnswer(answer -> {
      sendHeadersInvocation[0]++;
      return answer.callRealMethod();
    }).when(interceptedConn).sendRequestHeader(Mockito.any(HttpRequest.class));
    Mockito.doAnswer(answer -> {
      sendBodyInvocation[0]++;
      return answer.callRealMethod();
    }).when(interceptedConn).sendRequestEntity(Mockito.any(
        HttpEntityEnclosingRequest.class));
    Mockito.doAnswer(answer -> {
      receiveResponseInvocation[0]++;
      return answer.callRealMethod();
    }).when(interceptedConn).receiveResponseHeader();
    return interceptedConn;
  }
}
