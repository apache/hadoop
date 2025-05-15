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
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URL;

import org.assertj.core.api.Assertions;
import org.junit.Assume;
import org.junit.Test;
import org.mockito.Mockito;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.azurebfs.AbstractAbfsIntegrationTest;
import org.apache.hadoop.fs.azurebfs.AzureBlobFileSystem;
import org.apache.hadoop.fs.azurebfs.AzureBlobFileSystemStore;
import org.apache.hadoop.fs.azurebfs.constants.AbfsServiceType;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AbfsRestOperationException;
import org.apache.hadoop.fs.azurebfs.utils.TracingContext;
import org.apache.http.HttpClientConnection;
import org.apache.http.HttpEntityEnclosingRequest;
import org.apache.http.HttpException;
import org.apache.http.HttpRequest;
import org.apache.http.HttpResponse;
import org.apache.http.client.protocol.HttpClientContext;

import static java.net.HttpURLConnection.HTTP_PRECON_FAILED;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_NETWORKING_LIBRARY;
import static org.apache.hadoop.fs.azurebfs.constants.HttpOperationType.APACHE_HTTP_CLIENT;
import static org.apache.hadoop.test.LambdaTestUtils.intercept;

public class ITestAbfsHttpClientRequestExecutor extends
    AbstractAbfsIntegrationTest {

  public ITestAbfsHttpClientRequestExecutor() throws Exception {
    super();
  }

  /**
   * Verify the correctness of expect 100 continue handling by ApacheHttpClient
   * with AbfsManagedHttpRequestExecutor.
   */
  @Test
  public void testExpect100ContinueHandling() throws Exception {
    AzureBlobFileSystem fs = getFileSystem();
    Path path = new Path("/testExpect100ContinueHandling");
    if (isAppendBlobEnabled()) {
      Assume.assumeFalse("Not valid for AppendBlob with blob endpoint",
          getIngressServiceType() == AbfsServiceType.BLOB);
    }

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
      final ConnectionInfo connectionInfo = new ConnectionInfo();

      /*
       * Assert that correct actions are taking place over the connection to handle
       * expect100 assertions, failure and success.
       *
       * The test would make two calls to the server. The first two calls would
       * be because of attempt to write in a non-existing file. The first call would have
       * expect100 header, and the server would respond with 404. The second call would
       * be a retry from AbfsOutputStream, and would not have expect100 header.
       *
       * The third call would be because of attempt to write in an existing file. The call
       * would have expect100 assertion pass and would send the data.
       *
       * Following is the expectation from the first attempt:
       * 1. sendHeaders should be called once. This is for expect100 assertion invocation.
       * 2. receiveResponse should be called once. This is to receive expect100 assertion.
       * 2. sendBody should not be called.
       *
       * Following is the expectation from the second attempt:
       * 1. sendHeaders should be called once. This is not for expect100 assertion invocation.
       * 2. sendBody should be called once. It will not have any expect100 assertion.
       * Once headers are sent, body is sent.
       * 3. receiveResponse should be called once. This is to receive the response from the server.
       *
       * Following is the expectation from the third attempt:
       * 1. sendHeaders should be called once. This is for expect100 assertion invocation.
       * 2. receiveResponse should be called. This is to receive the response from the server for expect100 assertion.
       * 3. sendBody called as expect100 assertion is pass.
       * 4. receiveResponse should be called. This is to receive the response from the server.
       */
      mockHttpOperationBehavior(connectionInfo, op);
      Mockito.doAnswer(executeAnswer -> {
        invocation[0]++;
        final Throwable throwable;
        if (invocation[0] == 3) {
          executeAnswer.callRealMethod();
          throwable = null;
        } else {
          throwable = intercept(IOException.class, () -> {
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
        }
        /*
         * The first call would be with expect headers, and expect 100 continue assertion has to happen which would fail.
         * For expect100 assertion to happen, header IO happens before body IO. If assertion fails, no body IO happens.
         * The second call would not be using expect headers.
         *
         * The third call would be with expect headers, and expect 100 continue assertion has to happen which would pass.
         */
        if (invocation[0] == 1) {
          Assertions.assertThat(connectionInfo.getSendHeaderInvocation())
              .isEqualTo(1);
          Assertions.assertThat(connectionInfo.getSendBodyInvocation())
              .isEqualTo(0);
          Assertions.assertThat(connectionInfo.getReceiveResponseInvocation())
              .isEqualTo(1);
          Assertions.assertThat(
                  connectionInfo.getReceiveResponseBodyInvocation())
              .isEqualTo(1);
        }
        if (invocation[0] == 2) {
          Assertions.assertThat(connectionInfo.getSendHeaderInvocation())
              .isEqualTo(1);
          Assertions.assertThat(connectionInfo.getSendBodyInvocation())
              .isEqualTo(1);
          Assertions.assertThat(connectionInfo.getReceiveResponseInvocation())
              .isEqualTo(1);
          Assertions.assertThat(
                  connectionInfo.getReceiveResponseBodyInvocation())
              .isEqualTo(1);
        }
        if (invocation[0] == 3) {
          Assertions.assertThat(connectionInfo.getSendHeaderInvocation())
              .isEqualTo(1);
          Assertions.assertThat(connectionInfo.getSendBodyInvocation())
              .isEqualTo(1);
          Assertions.assertThat(connectionInfo.getReceiveResponseInvocation())
              .isEqualTo(2);
          Assertions.assertThat(
                  connectionInfo.getReceiveResponseBodyInvocation())
              .isEqualTo(1);
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
    AbfsOutputStream innerOs
        = (AbfsOutputStream) ((FSDataOutputStream) os).getWrappedStream();
    if (innerOs.getClientHandler()
        .getIngressClient() instanceof AbfsDfsClient) {
      intercept(FileNotFoundException.class, () -> {
        /*
         * This would lead to two server calls.
         * First call would be with expect headers, and expect 100 continue
         *  assertion has to happen which would fail with 404.
         * Second call would be a retry from AbfsOutputStream, and would not be using expect headers.
         */
        os.write(1);
        os.close();
      });
    } else {
      AbfsRestOperationException ex = intercept(
          AbfsRestOperationException.class, () -> {
            /*
             * This would lead to two server calls.
             * First call would be with expect headers, and expect 100 continue
             *  assertion has to happen which would fail with 404.
             * Second call would be a retry from AbfsOutputStream, and would not be using expect headers.
             */
            try {
              os.write(1);
              os.close();
            } catch (IOException e) {
              throw (IOException) e.getCause().getCause();
            }
          });
      Assertions.assertThat(ex.getStatusCode()).isEqualTo(HTTP_PRECON_FAILED);
    }
  }

  /**
   * Creates a mock of HttpOperation that would be returned for AbfsRestOperation
   * to use to execute server call. To make call via ApacheHttpClient, an object
   * of {@link HttpClientContext} is required. This method would create a mock
   * of HttpClientContext that would be able to register the actions taken on
   * {@link HttpClientConnection} object. This would help in asserting the
   * order of actions taken on the connection object for making an append call with
   * expect100 header.
   */
  private void mockHttpOperationBehavior(final ConnectionInfo connectionInfo,
      final AbfsRestOperation op) throws IOException {
    Mockito.doAnswer(httpOpCreationAnswer -> {
      AbfsAHCHttpOperation httpOperation = Mockito.spy(
          (AbfsAHCHttpOperation) httpOpCreationAnswer.callRealMethod());

      Mockito.doAnswer(createContextAnswer -> {
            AbfsManagedHttpClientContext context = Mockito.spy(
                (AbfsManagedHttpClientContext) createContextAnswer.callRealMethod());
            Mockito.doAnswer(connectionSpyIntercept -> {
              return interceptedConn(connectionInfo,
                  (HttpClientConnection) connectionSpyIntercept.getArgument(0));
            }).when(context).interceptConnectionActivity(Mockito.any(
                HttpClientConnection.class));
            return context;
          })
          .when(httpOperation).getHttpClientContext();
      return httpOperation;
    }).when(op).createHttpOperation();
  }

  private HttpClientConnection interceptedConn(final ConnectionInfo connectionInfo,
      final HttpClientConnection connection) throws IOException, HttpException {
    HttpClientConnection interceptedConn = Mockito.spy(connection);

    Mockito.doAnswer(answer -> {
      connectionInfo.incrementSendHeaderInvocation();
      long start = System.currentTimeMillis();
      Object result = answer.callRealMethod();
      connectionInfo.addSendTime(System.currentTimeMillis() - start);
      return result;
    }).when(interceptedConn).sendRequestHeader(Mockito.any(HttpRequest.class));

    Mockito.doAnswer(answer -> {
      connectionInfo.incrementSendBodyInvocation();
      long start = System.currentTimeMillis();
      Object result = answer.callRealMethod();
      connectionInfo.addSendTime(System.currentTimeMillis() - start);
      return result;
    }).when(interceptedConn).sendRequestEntity(Mockito.any(
        HttpEntityEnclosingRequest.class));

    Mockito.doAnswer(answer -> {
      connectionInfo.incrementReceiveResponseInvocation();
      long start = System.currentTimeMillis();
      Object result = answer.callRealMethod();
      connectionInfo.addReadTime(System.currentTimeMillis() - start);
      return result;
    }).when(interceptedConn).receiveResponseHeader();

    Mockito.doAnswer(answer -> {
      connectionInfo.incrementReceiveResponseBodyInvocation();
      long start = System.currentTimeMillis();
      Object result = answer.callRealMethod();
      connectionInfo.addReadTime(System.currentTimeMillis() - start);
      return result;
    }).when(interceptedConn).receiveResponseEntity(Mockito.any(
        HttpResponse.class));
    return interceptedConn;
  }

  @Test
  public void testConnectionReadRecords() throws Exception {
    AzureBlobFileSystem fs = getFileSystem();
    Path path = new Path("/testConnectionRecords");

    Configuration conf = new Configuration(fs.getConf());
    conf.set(FS_AZURE_NETWORKING_LIBRARY, APACHE_HTTP_CLIENT.toString());
    AzureBlobFileSystem fs2 = Mockito.spy(
        (AzureBlobFileSystem) FileSystem.newInstance(conf));

    AzureBlobFileSystemStore store = Mockito.spy(fs2.getAbfsStore());
    Mockito.doReturn(store).when(fs2).getAbfsStore();

    AbfsClient client = Mockito.spy(store.getClient());
    Mockito.doReturn(client).when(store).getClient();

    try (OutputStream os = fs.create(path)) {
      os.write(1);
    }

    InputStream is = fs2.open(path);

    Mockito.doAnswer(answer -> {
      AbfsRestOperation op = Mockito.spy(
          (AbfsRestOperation) answer.callRealMethod());
      final ConnectionInfo connectionInfo = new ConnectionInfo();
      mockHttpOperationBehavior(connectionInfo, op);
      Mockito.doAnswer(executeAnswer -> {
        executeAnswer.callRealMethod();
        Assertions.assertThat(connectionInfo.getSendHeaderInvocation())
            .isEqualTo(1);
        Assertions.assertThat(connectionInfo.getSendBodyInvocation())
            .isEqualTo(0);
        Assertions.assertThat(connectionInfo.getReceiveResponseInvocation())
            .isEqualTo(1);
        Assertions.assertThat(connectionInfo.getReceiveResponseBodyInvocation())
            .isEqualTo(1);
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

    is.read();
    is.close();
  }

  private static class ConnectionInfo {

    private long connectTime;

    private long readTime;

    private long sendTime;

    private int sendHeaderInvocation;

    private int sendBodyInvocation;

    private int receiveResponseInvocation;

    private int receiveResponseBodyInvocation;

    private void incrementSendHeaderInvocation() {
      sendHeaderInvocation++;
    }

    private void incrementSendBodyInvocation() {
      sendBodyInvocation++;
    }

    private void incrementReceiveResponseInvocation() {
      receiveResponseInvocation++;
    }

    private void incrementReceiveResponseBodyInvocation() {
      receiveResponseBodyInvocation++;
    }

    private void addConnectTime(long connectTime) {
      this.connectTime += connectTime;
    }

    private void addReadTime(long readTime) {
      this.readTime += readTime;
    }

    private void addSendTime(long sendTime) {
      this.sendTime += sendTime;
    }

    private long getConnectTime() {
      return connectTime;
    }

    private long getReadTime() {
      return readTime;
    }

    private long getSendTime() {
      return sendTime;
    }

    private int getSendHeaderInvocation() {
      return sendHeaderInvocation;
    }

    private int getSendBodyInvocation() {
      return sendBodyInvocation;
    }

    private int getReceiveResponseInvocation() {
      return receiveResponseInvocation;
    }

    private int getReceiveResponseBodyInvocation() {
      return receiveResponseBodyInvocation;
    }
  }
}
