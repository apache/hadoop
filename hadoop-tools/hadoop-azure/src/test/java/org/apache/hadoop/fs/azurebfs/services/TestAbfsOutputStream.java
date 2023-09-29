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

import java.io.IOException;
import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import org.mockito.ArgumentCaptor;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.azurebfs.AbfsConfiguration;
import org.apache.hadoop.fs.azurebfs.constants.FSOperationType;
import org.apache.hadoop.fs.azurebfs.contracts.services.AppendRequestParameters;
import org.apache.hadoop.fs.azurebfs.utils.TracingContext;
import org.apache.hadoop.fs.azurebfs.utils.TracingHeaderFormat;
import org.apache.hadoop.fs.store.DataBlocks;
import org.apache.hadoop.util.BlockingThreadPoolExecutorService;
import org.apache.hadoop.util.SemaphoredDelegatingExecutor;

import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.DATA_BLOCKS_BUFFER;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_BLOCK_UPLOAD_BUFFER_DIR;
import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.BLOCK_UPLOAD_ACTIVE_BLOCKS_DEFAULT;
import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.DATA_BLOCKS_BUFFER_DEFAULT;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.ArgumentMatchers.refEq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.anyBoolean;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.anyLong;

import static org.assertj.core.api.Assertions.assertThat;
import static org.apache.hadoop.fs.azurebfs.contracts.services.AppendRequestParameters.Mode.APPEND_MODE;

public final class TestAbfsOutputStream {

  private static final int BUFFER_SIZE = 4096;
  private static final int WRITE_SIZE = 1000;
  private static final String PATH = "~/testpath";
  private final String globalKey = "fs.azure.configuration";
  private final String accountName1 = "account1";
  private final String accountKey1 = globalKey + "." + accountName1;
  private final String accountValue1 = "one";

  private AbfsOutputStreamContext populateAbfsOutputStreamContext(
      int writeBufferSize,
      boolean isFlushEnabled,
      boolean disableOutputStreamFlush,
      boolean isAppendBlob,
      boolean isExpectHeaderEnabled,
      AbfsClient client,
      String path,
      TracingContext tracingContext,
      ExecutorService executorService) throws IOException,
      IllegalAccessException {
    AbfsConfiguration abfsConf = new AbfsConfiguration(new Configuration(),
        accountName1);
    String blockFactoryName =
        abfsConf.getRawConfiguration().getTrimmed(DATA_BLOCKS_BUFFER,
        DATA_BLOCKS_BUFFER_DEFAULT);
    DataBlocks.BlockFactory blockFactory =
        DataBlocks.createFactory(FS_AZURE_BLOCK_UPLOAD_BUFFER_DIR,
            abfsConf.getRawConfiguration(),
        blockFactoryName);

    return new AbfsOutputStreamContext(2)
            .withWriteBufferSize(writeBufferSize)
            .enableExpectHeader(isExpectHeaderEnabled)
            .enableFlush(isFlushEnabled)
            .disableOutputStreamFlush(disableOutputStreamFlush)
            .withStreamStatistics(new AbfsOutputStreamStatisticsImpl())
            .withAppendBlob(isAppendBlob)
            .withWriteMaxConcurrentRequestCount(abfsConf.getWriteMaxConcurrentRequestCount())
            .withMaxWriteRequestsToQueue(abfsConf.getMaxWriteRequestsToQueue())
            .withClient(client)
            .withPath(path)
            .withTracingContext(tracingContext)
            .withExecutorService(executorService)
            .withBlockFactory(blockFactory)
            .build();
  }

  /**
   * The test verifies OutputStream shortwrite case(2000bytes write followed by flush, hflush, hsync) is making correct HTTP calls to the server
   */
  @Test
  public void verifyShortWriteRequest() throws Exception {

    AbfsClient client = mock(AbfsClient.class);
    AbfsRestOperation op = mock(AbfsRestOperation.class);
    AbfsConfiguration abfsConf;
    final Configuration conf = new Configuration();
    conf.set(accountKey1, accountValue1);
    abfsConf = new AbfsConfiguration(conf, accountName1);
    AbfsPerfTracker tracker = new AbfsPerfTracker("test", accountName1, abfsConf);
    when(client.getAbfsPerfTracker()).thenReturn(tracker);
    when(client.append(anyString(), any(byte[].class),
        any(AppendRequestParameters.class), any(), any(TracingContext.class)))
        .thenReturn(op);
    when(client.flush(anyString(), anyLong(), anyBoolean(), anyBoolean(), any(),
        isNull(), any(TracingContext.class))).thenReturn(op);

    AbfsOutputStream out = new AbfsOutputStream(
        populateAbfsOutputStreamContext(
            BUFFER_SIZE,
            true,
            false,
            false,
            true,
            client,
            PATH,
            new TracingContext(abfsConf.getClientCorrelationId(), "test-fs-id",
                FSOperationType.WRITE, abfsConf.getTracingHeaderFormat(),
                null),
            createExecutorService(abfsConf)));
    final byte[] b = new byte[WRITE_SIZE];
    new Random().nextBytes(b);
    out.write(b);
    out.hsync();

    final byte[] b1 = new byte[2*WRITE_SIZE];
    new Random().nextBytes(b1);
    out.write(b1);
    out.flush();
    out.hflush();

    out.hsync();

    AppendRequestParameters firstReqParameters = new AppendRequestParameters(
        0, 0, WRITE_SIZE, APPEND_MODE, false, null, true);
    AppendRequestParameters secondReqParameters = new AppendRequestParameters(
        WRITE_SIZE, 0, 2 * WRITE_SIZE, APPEND_MODE, false, null, true);

    verify(client, times(1)).append(
        eq(PATH), any(byte[].class), refEq(firstReqParameters), any(),
        any(TracingContext.class));
    verify(client, times(1)).append(
        eq(PATH), any(byte[].class), refEq(secondReqParameters), any(), any(TracingContext.class));
    // confirm there were only 2 invocations in all
    verify(client, times(2)).append(
        eq(PATH), any(byte[].class), any(), any(), any(TracingContext.class));
  }

  /**
   * The test verifies OutputStream Write of WRITE_SIZE(1000 bytes) followed by a close is making correct HTTP calls to the server
   */
  @Test
  public void verifyWriteRequest() throws Exception {

    AbfsClient client = mock(AbfsClient.class);
    AbfsRestOperation op = mock(AbfsRestOperation.class);
    AbfsConfiguration abfsConf;
    final Configuration conf = new Configuration();
    conf.set(accountKey1, accountValue1);
    abfsConf = new AbfsConfiguration(conf, accountName1);
    AbfsPerfTracker tracker = new AbfsPerfTracker("test", accountName1, abfsConf);
    TracingContext tracingContext = new TracingContext("test-corr-id",
        "test-fs-id", FSOperationType.WRITE,
        TracingHeaderFormat.ALL_ID_FORMAT, null);

    when(client.getAbfsPerfTracker()).thenReturn(tracker);
    when(client.append(anyString(), any(byte[].class), any(AppendRequestParameters.class), any(), any(TracingContext.class))).thenReturn(op);
    when(client.flush(anyString(), anyLong(), anyBoolean(), anyBoolean(), any(), isNull(), any(TracingContext.class))).thenReturn(op);

    AbfsOutputStream out = new AbfsOutputStream(
        populateAbfsOutputStreamContext(
            BUFFER_SIZE,
            true,
            false,
            false,
            true,
            client,
            PATH,
            tracingContext,
            createExecutorService(abfsConf)));
    final byte[] b = new byte[WRITE_SIZE];
    new Random().nextBytes(b);

    for (int i = 0; i < 5; i++) {
      out.write(b);
    }
    out.close();

    AppendRequestParameters firstReqParameters = new AppendRequestParameters(
        0, 0, BUFFER_SIZE, APPEND_MODE, false, null, true);
    AppendRequestParameters secondReqParameters = new AppendRequestParameters(
        BUFFER_SIZE, 0, 5*WRITE_SIZE-BUFFER_SIZE, APPEND_MODE, false, null, true);

    verify(client, times(1)).append(
        eq(PATH), any(byte[].class), refEq(firstReqParameters), any(),
        any(TracingContext.class));
    verify(client, times(1)).append(
        eq(PATH), any(byte[].class), refEq(secondReqParameters), any(),
        any(TracingContext.class));
    // confirm there were only 2 invocations in all
    verify(client, times(2)).append(
        eq(PATH), any(byte[].class), any(), any(),
        any(TracingContext.class));

    ArgumentCaptor<String> acFlushPath = ArgumentCaptor.forClass(String.class);
    ArgumentCaptor<Long> acFlushPosition = ArgumentCaptor.forClass(Long.class);
    ArgumentCaptor<TracingContext> acTracingContext = ArgumentCaptor
        .forClass(TracingContext.class);
    ArgumentCaptor<Boolean> acFlushRetainUnCommittedData = ArgumentCaptor.forClass(Boolean.class);
    ArgumentCaptor<Boolean> acFlushClose = ArgumentCaptor.forClass(Boolean.class);
    ArgumentCaptor<String> acFlushSASToken = ArgumentCaptor.forClass(String.class);

    verify(client, times(1)).flush(acFlushPath.capture(), acFlushPosition.capture(), acFlushRetainUnCommittedData.capture(), acFlushClose.capture(),
        acFlushSASToken.capture(), isNull(), acTracingContext.capture());
    assertThat(Arrays.asList(PATH)).describedAs("path").isEqualTo(acFlushPath.getAllValues());
    assertThat(Arrays.asList(Long.valueOf(5*WRITE_SIZE))).describedAs("position").isEqualTo(acFlushPosition.getAllValues());
    assertThat(Arrays.asList(false)).describedAs("RetainUnCommittedData flag").isEqualTo(acFlushRetainUnCommittedData.getAllValues());
    assertThat(Arrays.asList(true)).describedAs("Close flag").isEqualTo(acFlushClose.getAllValues());
  }

  /**
   * The test verifies OutputStream Write of BUFFER_SIZE(4KB) followed by a close is making correct HTTP calls to the server
   */
  @Test
  public void verifyWriteRequestOfBufferSizeAndClose() throws Exception {

    AbfsClient client = mock(AbfsClient.class);
    AbfsRestOperation op = mock(AbfsRestOperation.class);
    AbfsHttpOperation httpOp = mock(AbfsHttpOperation.class);
    AbfsConfiguration abfsConf;
    final Configuration conf = new Configuration();
    conf.set(accountKey1, accountValue1);
    abfsConf = new AbfsConfiguration(conf, accountName1);
    AbfsPerfTracker tracker = new AbfsPerfTracker("test", accountName1, abfsConf);
    TracingContext tracingContext = new TracingContext(
        abfsConf.getClientCorrelationId(), "test-fs-id",
        FSOperationType.WRITE, abfsConf.getTracingHeaderFormat(), null);

    when(client.getAbfsPerfTracker()).thenReturn(tracker);
    when(client.append(anyString(), any(byte[].class), any(AppendRequestParameters.class), any(), any(TracingContext.class))).thenReturn(op);
    when(client.flush(anyString(), anyLong(), anyBoolean(), anyBoolean(), any(), isNull(), any(TracingContext.class))).thenReturn(op);
    when(op.getSasToken()).thenReturn("testToken");
    when(op.getResult()).thenReturn(httpOp);

    AbfsOutputStream out = new AbfsOutputStream(
        populateAbfsOutputStreamContext(
            BUFFER_SIZE,
            true,
            false,
            false,
            true,
            client,
            PATH,
            tracingContext,
            createExecutorService(abfsConf)));
    final byte[] b = new byte[BUFFER_SIZE];
    new Random().nextBytes(b);

    for (int i = 0; i < 2; i++) {
      out.write(b);
    }
    out.close();

    AppendRequestParameters firstReqParameters = new AppendRequestParameters(
        0, 0, BUFFER_SIZE, APPEND_MODE, false, null, true);
    AppendRequestParameters secondReqParameters = new AppendRequestParameters(
        BUFFER_SIZE, 0, BUFFER_SIZE, APPEND_MODE, false, null, true);

    verify(client, times(1)).append(
        eq(PATH), any(byte[].class), refEq(firstReqParameters), any(), any(TracingContext.class));
    verify(client, times(1)).append(
        eq(PATH), any(byte[].class), refEq(secondReqParameters), any(), any(TracingContext.class));
    // confirm there were only 2 invocations in all
    verify(client, times(2)).append(
        eq(PATH), any(byte[].class), any(), any(), any(TracingContext.class));

    ArgumentCaptor<String> acFlushPath = ArgumentCaptor.forClass(String.class);
    ArgumentCaptor<Long> acFlushPosition = ArgumentCaptor.forClass(Long.class);
    ArgumentCaptor<TracingContext> acTracingContext = ArgumentCaptor
        .forClass(TracingContext.class);
    ArgumentCaptor<Boolean> acFlushRetainUnCommittedData = ArgumentCaptor.forClass(Boolean.class);
    ArgumentCaptor<Boolean> acFlushClose = ArgumentCaptor.forClass(Boolean.class);
    ArgumentCaptor<String> acFlushSASToken = ArgumentCaptor.forClass(String.class);

    verify(client, times(1)).flush(acFlushPath.capture(), acFlushPosition.capture(), acFlushRetainUnCommittedData.capture(), acFlushClose.capture(),
        acFlushSASToken.capture(), isNull(), acTracingContext.capture());
    assertThat(Arrays.asList(PATH)).describedAs("path").isEqualTo(acFlushPath.getAllValues());
    assertThat(Arrays.asList(Long.valueOf(2*BUFFER_SIZE))).describedAs("position").isEqualTo(acFlushPosition.getAllValues());
    assertThat(Arrays.asList(false)).describedAs("RetainUnCommittedData flag").isEqualTo(acFlushRetainUnCommittedData.getAllValues());
    assertThat(Arrays.asList(true)).describedAs("Close flag").isEqualTo(acFlushClose.getAllValues());
  }

  /**
   * The test verifies OutputStream Write of BUFFER_SIZE(4KB) is making correct HTTP calls to the server
   */
  @Test
  public void verifyWriteRequestOfBufferSize() throws Exception {

    AbfsClient client = mock(AbfsClient.class);
    AbfsRestOperation op = mock(AbfsRestOperation.class);
    AbfsHttpOperation httpOp = mock(AbfsHttpOperation.class);
    AbfsConfiguration abfsConf;
    final Configuration conf = new Configuration();
    conf.set(accountKey1, accountValue1);
    abfsConf = new AbfsConfiguration(conf, accountName1);
    AbfsPerfTracker tracker = new AbfsPerfTracker("test", accountName1, abfsConf);

    when(client.getAbfsPerfTracker()).thenReturn(tracker);
    when(client.append(anyString(), any(byte[].class),
        any(AppendRequestParameters.class), any(), any(TracingContext.class)))
        .thenReturn(op);
    when(client.flush(anyString(), anyLong(), anyBoolean(), anyBoolean(),
        any(), isNull(), any(TracingContext.class))).thenReturn(op);
    when(op.getSasToken()).thenReturn("testToken");
    when(op.getResult()).thenReturn(httpOp);

    AbfsOutputStream out = new AbfsOutputStream(
        populateAbfsOutputStreamContext(
            BUFFER_SIZE,
            true,
            false,
            false,
            true,
            client,
            PATH,
            new TracingContext(abfsConf.getClientCorrelationId(), "test-fs-id",
                FSOperationType.WRITE, abfsConf.getTracingHeaderFormat(),
                null),
            createExecutorService(abfsConf)));
    final byte[] b = new byte[BUFFER_SIZE];
    new Random().nextBytes(b);

    for (int i = 0; i < 2; i++) {
      out.write(b);
    }
    Thread.sleep(1000);

    AppendRequestParameters firstReqParameters = new AppendRequestParameters(
        0, 0, BUFFER_SIZE, APPEND_MODE, false, null, true);
    AppendRequestParameters secondReqParameters = new AppendRequestParameters(
        BUFFER_SIZE, 0, BUFFER_SIZE, APPEND_MODE, false, null, true);

    verify(client, times(1)).append(
        eq(PATH), any(byte[].class), refEq(firstReqParameters), any(), any(TracingContext.class));
    verify(client, times(1)).append(
        eq(PATH), any(byte[].class), refEq(secondReqParameters), any(), any(TracingContext.class));
    // confirm there were only 2 invocations in all
    verify(client, times(2)).append(
        eq(PATH), any(byte[].class), any(), any(), any(TracingContext.class));
  }

  /**
   * The test verifies OutputStream Write of BUFFER_SIZE(4KB) on a AppendBlob based stream is making correct HTTP calls to the server
   */
  @Test
  public void verifyWriteRequestOfBufferSizeWithAppendBlob() throws Exception {

    AbfsClient client = mock(AbfsClient.class);
    AbfsRestOperation op = mock(AbfsRestOperation.class);
    AbfsConfiguration abfsConf;
    final Configuration conf = new Configuration();
    conf.set(accountKey1, accountValue1);
    abfsConf = new AbfsConfiguration(conf, accountName1);
    AbfsPerfTracker tracker = new AbfsPerfTracker("test", accountName1, abfsConf);

    when(client.getAbfsPerfTracker()).thenReturn(tracker);
    when(client.append(anyString(), any(byte[].class),
        any(AppendRequestParameters.class), any(), any(TracingContext.class)))
        .thenReturn(op);
    when(client.flush(anyString(), anyLong(), anyBoolean(), anyBoolean(), any(),
        isNull(), any(TracingContext.class))).thenReturn(op);

    AbfsOutputStream out = new AbfsOutputStream(
        populateAbfsOutputStreamContext(
            BUFFER_SIZE,
            true,
            false,
            true,
            true,
            client,
            PATH,
            new TracingContext(abfsConf.getClientCorrelationId(), "test-fs-id",
                FSOperationType.OPEN, abfsConf.getTracingHeaderFormat(),
                null),
            createExecutorService(abfsConf)));
    final byte[] b = new byte[BUFFER_SIZE];
    new Random().nextBytes(b);

    for (int i = 0; i < 2; i++) {
      out.write(b);
    }
    Thread.sleep(1000);

    AppendRequestParameters firstReqParameters = new AppendRequestParameters(
        0, 0, BUFFER_SIZE, APPEND_MODE, true, null, true);
    AppendRequestParameters secondReqParameters = new AppendRequestParameters(
        BUFFER_SIZE, 0, BUFFER_SIZE, APPEND_MODE, true, null, true);

    verify(client, times(1)).append(
        eq(PATH), any(byte[].class), refEq(firstReqParameters), any(), any(TracingContext.class));
    verify(client, times(1)).append(
        eq(PATH), any(byte[].class), refEq(secondReqParameters), any(), any(TracingContext.class));
    // confirm there were only 2 invocations in all
    verify(client, times(2)).append(
        eq(PATH), any(byte[].class), any(), any(), any(TracingContext.class));
  }

  /**
   * The test verifies OutputStream Write of BUFFER_SIZE(4KB)  followed by a hflush call is making correct HTTP calls to the server
   */
  @Test
  public void verifyWriteRequestOfBufferSizeAndHFlush() throws Exception {

    AbfsClient client = mock(AbfsClient.class);
    AbfsRestOperation op = mock(AbfsRestOperation.class);
    when(op.getSasToken()).thenReturn("");
    AbfsConfiguration abfsConf;
    final Configuration conf = new Configuration();
    conf.set(accountKey1, accountValue1);
    abfsConf = new AbfsConfiguration(conf, accountName1);
    AbfsPerfTracker tracker = new AbfsPerfTracker("test", accountName1, abfsConf);
    TracingContext tracingContext = new TracingContext(
        abfsConf.getClientCorrelationId(), "test-fs-id",
        FSOperationType.WRITE, abfsConf.getTracingHeaderFormat(), null);

    when(client.getAbfsPerfTracker()).thenReturn(tracker);
    when(client.append(anyString(), any(byte[].class),
        any(AppendRequestParameters.class), any(), any(TracingContext.class)))
        .thenReturn(op);
    when(client.flush(anyString(), anyLong(), anyBoolean(), anyBoolean(), any(),
        isNull(), any(TracingContext.class))).thenReturn(op);

    AbfsOutputStream out = new AbfsOutputStream(
        populateAbfsOutputStreamContext(
            BUFFER_SIZE,
            true,
            false,
            false,
            true,
            client,
            PATH,
            new TracingContext(abfsConf.getClientCorrelationId(), "test-fs-id",
                FSOperationType.OPEN, abfsConf.getTracingHeaderFormat(),
                null),
            createExecutorService(abfsConf)));
    final byte[] b = new byte[BUFFER_SIZE];
    new Random().nextBytes(b);

    for (int i = 0; i < 2; i++) {
      out.write(b);
    }
    out.hflush();

    AppendRequestParameters firstReqParameters = new AppendRequestParameters(
        0, 0, BUFFER_SIZE, APPEND_MODE, false, null, true);
    AppendRequestParameters secondReqParameters = new AppendRequestParameters(
        BUFFER_SIZE, 0, BUFFER_SIZE, APPEND_MODE, false, null, true);

    verify(client, times(1)).append(
        eq(PATH), any(byte[].class), refEq(firstReqParameters), any(), any(TracingContext.class));
    verify(client, times(1)).append(
        eq(PATH), any(byte[].class), refEq(secondReqParameters), any(), any(TracingContext.class));
    // confirm there were only 2 invocations in all
    verify(client, times(2)).append(
        eq(PATH), any(byte[].class), any(), any(), any(TracingContext.class));

    ArgumentCaptor<String> acFlushPath = ArgumentCaptor.forClass(String.class);
    ArgumentCaptor<Long> acFlushPosition = ArgumentCaptor.forClass(Long.class);
    ArgumentCaptor<TracingContext> acTracingContext = ArgumentCaptor
        .forClass(TracingContext.class);
    ArgumentCaptor<Boolean> acFlushRetainUnCommittedData = ArgumentCaptor.forClass(Boolean.class);
    ArgumentCaptor<Boolean> acFlushClose = ArgumentCaptor.forClass(Boolean.class);
    ArgumentCaptor<String> acFlushSASToken = ArgumentCaptor.forClass(String.class);

    verify(client, times(1)).flush(acFlushPath.capture(), acFlushPosition.capture(), acFlushRetainUnCommittedData.capture(), acFlushClose.capture(),
        acFlushSASToken.capture(), isNull(), acTracingContext.capture());
    assertThat(Arrays.asList(PATH)).describedAs("path").isEqualTo(acFlushPath.getAllValues());
    assertThat(Arrays.asList(Long.valueOf(2*BUFFER_SIZE))).describedAs("position").isEqualTo(acFlushPosition.getAllValues());
    assertThat(Arrays.asList(false)).describedAs("RetainUnCommittedData flag").isEqualTo(acFlushRetainUnCommittedData.getAllValues());
    assertThat(Arrays.asList(false)).describedAs("Close flag").isEqualTo(acFlushClose.getAllValues());
  }

  /**
   * The test verifies OutputStream Write of BUFFER_SIZE(4KB)  followed by a flush call is making correct HTTP calls to the server
   */
  @Test
  public void verifyWriteRequestOfBufferSizeAndFlush() throws Exception {

    AbfsClient client = mock(AbfsClient.class);
    AbfsRestOperation op = mock(AbfsRestOperation.class);
    AbfsConfiguration abfsConf;
    final Configuration conf = new Configuration();
    conf.set(accountKey1, accountValue1);
    abfsConf = new AbfsConfiguration(conf, accountName1);
    AbfsPerfTracker tracker = new AbfsPerfTracker("test", accountName1, abfsConf);
    when(client.getAbfsPerfTracker()).thenReturn(tracker);
    when(client.append(anyString(), any(byte[].class),
        any(AppendRequestParameters.class), any(), any(TracingContext.class)))
        .thenReturn(op);
    when(client.flush(anyString(), anyLong(), anyBoolean(), anyBoolean(), any(),
        isNull(), any(TracingContext.class))).thenReturn(op);

    AbfsOutputStream out = new AbfsOutputStream(
        populateAbfsOutputStreamContext(
            BUFFER_SIZE,
            true,
            false,
            false,
            true,
            client,
            PATH,
            new TracingContext(abfsConf.getClientCorrelationId(), "test-fs-id",
                FSOperationType.WRITE, abfsConf.getTracingHeaderFormat(),
                null),
            createExecutorService(abfsConf)));
    final byte[] b = new byte[BUFFER_SIZE];
    new Random().nextBytes(b);

    for (int i = 0; i < 2; i++) {
      out.write(b);
    }
    Thread.sleep(1000);
    out.flush();
    Thread.sleep(1000);

    AppendRequestParameters firstReqParameters = new AppendRequestParameters(
        0, 0, BUFFER_SIZE, APPEND_MODE, false, null, true);
    AppendRequestParameters secondReqParameters = new AppendRequestParameters(
        BUFFER_SIZE, 0, BUFFER_SIZE, APPEND_MODE, false, null, true);

    verify(client, times(1)).append(
        eq(PATH), any(byte[].class), refEq(firstReqParameters), any(), any(TracingContext.class));
    verify(client, times(1)).append(
        eq(PATH), any(byte[].class), refEq(secondReqParameters), any(), any(TracingContext.class));
    // confirm there were only 2 invocations in all
    verify(client, times(2)).append(
        eq(PATH), any(byte[].class), any(), any(), any(TracingContext.class));
  }

  /**
   * Method to create an executor Service for AbfsOutputStream.
   * @param abfsConf Configuration.
   * @return ExecutorService.
   */
  private ExecutorService createExecutorService(
      AbfsConfiguration abfsConf) {
    ExecutorService executorService =
        new SemaphoredDelegatingExecutor(BlockingThreadPoolExecutorService.newInstance(
            abfsConf.getWriteMaxConcurrentRequestCount(),
            abfsConf.getMaxWriteRequestsToQueue(),
            10L, TimeUnit.SECONDS,
            "abfs-test-bounded"),
            BLOCK_UPLOAD_ACTIVE_BLOCKS_DEFAULT, true);
    return executorService;
  }
}
