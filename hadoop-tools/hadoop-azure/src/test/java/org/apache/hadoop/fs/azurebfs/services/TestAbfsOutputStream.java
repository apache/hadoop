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

import java.util.Arrays;
import java.util.HashSet;
import java.util.Random;

import org.junit.Test;

import org.mockito.ArgumentCaptor;

import org.apache.hadoop.fs.azurebfs.AbfsConfiguration;
import org.apache.hadoop.conf.Configuration;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.anyBoolean;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.anyLong;

import static org.assertj.core.api.Assertions.assertThat;

public final class TestAbfsOutputStream {

  private static final int BUFFER_SIZE = 4096;
  private static final int WRITE_SIZE = 1000;
  private static final String PATH = "~/testpath";
  private final String globalKey = "fs.azure.configuration";
  private final String accountName1 = "account1";
  private final String accountKey1 = globalKey + "." + accountName1;
  private final String accountValue1 = "one";

  private AbfsOutputStreamContext populateAbfsOutputStreamContext(int writeBufferSize,
            boolean isFlushEnabled,
            boolean disableOutputStreamFlush,
            boolean isAppendBlob) {
    return new AbfsOutputStreamContext(2)
            .withWriteBufferSize(writeBufferSize)
            .enableFlush(isFlushEnabled)
            .disableOutputStreamFlush(disableOutputStreamFlush)
            .withStreamStatistics(new AbfsOutputStreamStatisticsImpl())
            .withAppendBlob(isAppendBlob)
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
    when(client.append(anyString(), anyLong(), any(byte[].class), anyInt(), anyInt(), any(), anyBoolean())).thenReturn(op);
    when(client.flush(anyString(), anyLong(), anyBoolean(), anyBoolean(), any())).thenReturn(op);

    AbfsOutputStream out = new AbfsOutputStream(client, null, PATH, 0, populateAbfsOutputStreamContext(BUFFER_SIZE, true, false, false));
    final byte[] b = new byte[WRITE_SIZE];
    new Random().nextBytes(b);
    out.write(b);
    out.hsync();
    ArgumentCaptor<String> acString = ArgumentCaptor.forClass(String.class);
    ArgumentCaptor<Long> acLong = ArgumentCaptor.forClass(Long.class);
    ArgumentCaptor<Integer> acBufferOffset = ArgumentCaptor.forClass(Integer.class);
    ArgumentCaptor<Integer> acBufferLength = ArgumentCaptor.forClass(Integer.class);
    ArgumentCaptor<byte[]> acByteArray = ArgumentCaptor.forClass(byte[].class);
    ArgumentCaptor<Boolean> acAppendBlobAppend = ArgumentCaptor.forClass(Boolean.class);
    ArgumentCaptor<String> acSASToken = ArgumentCaptor.forClass(String.class);


    final byte[] b1 = new byte[2*WRITE_SIZE];
    new Random().nextBytes(b1);
    out.write(b1);
    out.flush();
    out.hflush();

    out.hsync();

    verify(client, times(2)).append(acString.capture(), acLong.capture(), acByteArray.capture(), acBufferOffset.capture(), acBufferLength.capture(),
                                    acSASToken.capture(), acAppendBlobAppend.capture());
    assertThat(Arrays.asList(PATH, PATH)).describedAs("Path of the requests").isEqualTo(acString.getAllValues());
    assertThat(Arrays.asList(Long.valueOf(0), Long.valueOf(WRITE_SIZE))).describedAs("Write Position").isEqualTo(acLong.getAllValues());
    assertThat(Arrays.asList(0, 0)).describedAs("Buffer Offset").isEqualTo(acBufferOffset.getAllValues());
    assertThat(Arrays.asList(WRITE_SIZE, 2*WRITE_SIZE)).describedAs("Buffer length").isEqualTo(acBufferLength.getAllValues());

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

    when(client.getAbfsPerfTracker()).thenReturn(tracker);
    when(client.append(anyString(), anyLong(), any(byte[].class), anyInt(), anyInt(), any(), anyBoolean())).thenReturn(op);
    when(client.flush(anyString(), anyLong(), anyBoolean(), anyBoolean(), any())).thenReturn(op);

    AbfsOutputStream out = new AbfsOutputStream(client, null, PATH, 0, populateAbfsOutputStreamContext(BUFFER_SIZE, true, false, false));
    final byte[] b = new byte[WRITE_SIZE];
    new Random().nextBytes(b);

    for (int i = 0; i < 5; i++) {
      out.write(b);
    }
    out.close();

    ArgumentCaptor<String> acString = ArgumentCaptor.forClass(String.class);
    ArgumentCaptor<Long> acLong = ArgumentCaptor.forClass(Long.class);
    ArgumentCaptor<Integer> acBufferOffset = ArgumentCaptor.forClass(Integer.class);
    ArgumentCaptor<Integer> acBufferLength = ArgumentCaptor.forClass(Integer.class);
    ArgumentCaptor<byte[]> acByteArray = ArgumentCaptor.forClass(byte[].class);
    ArgumentCaptor<Boolean> acAppendBlobAppend = ArgumentCaptor.forClass(Boolean.class);
    ArgumentCaptor<String> acSASToken = ArgumentCaptor.forClass(String.class);

    verify(client, times(2)).append(acString.capture(), acLong.capture(), acByteArray.capture(), acBufferOffset.capture(), acBufferLength.capture(),
                                    acSASToken.capture(), acAppendBlobAppend.capture());
    assertThat(Arrays.asList(PATH, PATH)).describedAs("Path").isEqualTo(acString.getAllValues());
    assertThat(new HashSet<Long>(Arrays.asList(Long.valueOf(0), Long.valueOf(BUFFER_SIZE)))).describedAs("Position").isEqualTo(new HashSet<Long>(
               acLong.getAllValues()));
    assertThat(Arrays.asList(0, 0)).describedAs("Buffer Offset").isEqualTo(acBufferOffset.getAllValues());
    assertThat(new HashSet<Integer>(Arrays.asList(BUFFER_SIZE, 5*WRITE_SIZE-BUFFER_SIZE))).describedAs("Buffer Length").isEqualTo(new HashSet<Integer>(
               acBufferLength.getAllValues()));

    ArgumentCaptor<String> acFlushString = ArgumentCaptor.forClass(String.class);
    ArgumentCaptor<Long> acFlushLong = ArgumentCaptor.forClass(Long.class);
    ArgumentCaptor<Boolean> acFlushRetainUnCommittedData = ArgumentCaptor.forClass(Boolean.class);
    ArgumentCaptor<Boolean> acFlushClose = ArgumentCaptor.forClass(Boolean.class);
    ArgumentCaptor<String> acFlushSASToken = ArgumentCaptor.forClass(String.class);

    verify(client, times(1)).flush(acFlushString.capture(), acFlushLong.capture(), acFlushRetainUnCommittedData.capture(), acFlushClose.capture(),
                                   acFlushSASToken.capture());
    assertThat(Arrays.asList(PATH)).describedAs("path").isEqualTo(acFlushString.getAllValues());
    assertThat(Arrays.asList(Long.valueOf(5*WRITE_SIZE))).describedAs("position").isEqualTo(acFlushLong.getAllValues());
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

    when(client.getAbfsPerfTracker()).thenReturn(tracker);
    when(client.append(anyString(), anyLong(), any(byte[].class), anyInt(), anyInt(), any(), anyBoolean())).thenReturn(op);
    when(client.flush(anyString(), anyLong(), anyBoolean(), anyBoolean(), any())).thenReturn(op);
    when(op.getSasToken()).thenReturn("testToken");
    when(op.getResult()).thenReturn(httpOp);

    AbfsOutputStream out = new AbfsOutputStream(client, null, PATH, 0, populateAbfsOutputStreamContext(BUFFER_SIZE, true, false, false));
    final byte[] b = new byte[BUFFER_SIZE];
    new Random().nextBytes(b);

    for (int i = 0; i < 2; i++) {
      out.write(b);
    }
    out.close();

    ArgumentCaptor<String> acString = ArgumentCaptor.forClass(String.class);
    ArgumentCaptor<Long> acLong = ArgumentCaptor.forClass(Long.class);
    ArgumentCaptor<Integer> acBufferOffset = ArgumentCaptor.forClass(Integer.class);
    ArgumentCaptor<Integer> acBufferLength = ArgumentCaptor.forClass(Integer.class);
    ArgumentCaptor<byte[]> acByteArray = ArgumentCaptor.forClass(byte[].class);
    ArgumentCaptor<Boolean> acAppendBlobAppend = ArgumentCaptor.forClass(Boolean.class);
    ArgumentCaptor<String> acSASToken = ArgumentCaptor.forClass(String.class);

    verify(client, times(2)).append(acString.capture(), acLong.capture(), acByteArray.capture(), acBufferOffset.capture(), acBufferLength.capture(),
                                    acSASToken.capture(), acAppendBlobAppend.capture());
    assertThat(Arrays.asList(PATH, PATH)).describedAs("path").isEqualTo(acString.getAllValues());
    assertThat(new HashSet<Long>(Arrays.asList(Long.valueOf(0), Long.valueOf(BUFFER_SIZE)))).describedAs("Position").isEqualTo(new HashSet<Long>(
               acLong.getAllValues()));
    assertThat(Arrays.asList(0, 0)).describedAs("Buffer Offset").isEqualTo(acBufferOffset.getAllValues());
    assertThat(Arrays.asList(BUFFER_SIZE, BUFFER_SIZE)).describedAs("Buffer Length").isEqualTo(acBufferLength.getAllValues());

    ArgumentCaptor<String> acFlushString = ArgumentCaptor.forClass(String.class);
    ArgumentCaptor<Long> acFlushLong = ArgumentCaptor.forClass(Long.class);
    ArgumentCaptor<Boolean> acFlushRetainUnCommittedData = ArgumentCaptor.forClass(Boolean.class);
    ArgumentCaptor<Boolean> acFlushClose = ArgumentCaptor.forClass(Boolean.class);
    ArgumentCaptor<String> acFlushSASToken = ArgumentCaptor.forClass(String.class);

    verify(client, times(1)).flush(acFlushString.capture(), acFlushLong.capture(), acFlushRetainUnCommittedData.capture(), acFlushClose.capture(),
                                   acFlushSASToken.capture());
    assertThat(Arrays.asList(PATH)).describedAs("path").isEqualTo(acFlushString.getAllValues());
    assertThat(Arrays.asList(Long.valueOf(2*BUFFER_SIZE))).describedAs("position").isEqualTo(acFlushLong.getAllValues());
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
    when(client.append(anyString(), anyLong(), any(byte[].class), anyInt(), anyInt(), any(), anyBoolean())).thenReturn(op);
    when(client.flush(anyString(), anyLong(), anyBoolean(), anyBoolean(), any())).thenReturn(op);
    when(op.getSasToken()).thenReturn("testToken");
    when(op.getResult()).thenReturn(httpOp);

    AbfsOutputStream out = new AbfsOutputStream(client, null, PATH, 0, populateAbfsOutputStreamContext(BUFFER_SIZE, true, false, false));
    final byte[] b = new byte[BUFFER_SIZE];
    new Random().nextBytes(b);

    for (int i = 0; i < 2; i++) {
      out.write(b);
    }
    Thread.sleep(1000);

    ArgumentCaptor<String> acString = ArgumentCaptor.forClass(String.class);
    ArgumentCaptor<Long> acLong = ArgumentCaptor.forClass(Long.class);
    ArgumentCaptor<Integer> acBufferOffset = ArgumentCaptor.forClass(Integer.class);
    ArgumentCaptor<Integer> acBufferLength = ArgumentCaptor.forClass(Integer.class);
    ArgumentCaptor<byte[]> acByteArray = ArgumentCaptor.forClass(byte[].class);
    ArgumentCaptor<Boolean> acAppendBlobAppend = ArgumentCaptor.forClass(Boolean.class);
    ArgumentCaptor<String> acSASToken = ArgumentCaptor.forClass(String.class);

    verify(client, times(2)).append(acString.capture(), acLong.capture(), acByteArray.capture(), acBufferOffset.capture(), acBufferLength.capture(),
                                    acSASToken.capture(), acAppendBlobAppend.capture());
    assertThat(Arrays.asList(PATH, PATH)).describedAs("File Path").isEqualTo(acString.getAllValues());
    assertThat(new HashSet<Long>(Arrays.asList(Long.valueOf(0), Long.valueOf(BUFFER_SIZE)))).describedAs("Position in file").isEqualTo(
               new HashSet<Long>(acLong.getAllValues()));
    assertThat(Arrays.asList(0, 0)).describedAs("buffer offset").isEqualTo(acBufferOffset.getAllValues());
    assertThat(Arrays.asList(BUFFER_SIZE, BUFFER_SIZE)).describedAs("buffer length").isEqualTo(acBufferLength.getAllValues());

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
    when(client.append(anyString(), anyLong(), any(byte[].class), anyInt(), anyInt(), any(), anyBoolean())).thenReturn(op);
    when(client.flush(anyString(), anyLong(), anyBoolean(), anyBoolean(), any())).thenReturn(op);

    AbfsOutputStream out = new AbfsOutputStream(client, null, PATH, 0, populateAbfsOutputStreamContext(BUFFER_SIZE, true, false, true));
    final byte[] b = new byte[BUFFER_SIZE];
    new Random().nextBytes(b);

    for (int i = 0; i < 2; i++) {
      out.write(b);
    }
    Thread.sleep(1000);

    ArgumentCaptor<String> acString = ArgumentCaptor.forClass(String.class);
    ArgumentCaptor<Long> acLong = ArgumentCaptor.forClass(Long.class);
    ArgumentCaptor<Integer> acBufferOffset = ArgumentCaptor.forClass(Integer.class);
    ArgumentCaptor<Integer> acBufferLength = ArgumentCaptor.forClass(Integer.class);
    ArgumentCaptor<byte[]> acByteArray = ArgumentCaptor.forClass(byte[].class);
    ArgumentCaptor<Boolean> acAppendBlobAppend = ArgumentCaptor.forClass(Boolean.class);
    ArgumentCaptor<String> acSASToken = ArgumentCaptor.forClass(String.class);

    verify(client, times(2)).append(acString.capture(), acLong.capture(), acByteArray.capture(), acBufferOffset.capture(), acBufferLength.capture(),
                                    acSASToken.capture(), acAppendBlobAppend.capture());
    assertThat(Arrays.asList(PATH, PATH)).describedAs("File Path").isEqualTo(acString.getAllValues());
    assertThat(Arrays.asList(Long.valueOf(0), Long.valueOf(BUFFER_SIZE))).describedAs("File Position").isEqualTo(acLong.getAllValues());
    assertThat(Arrays.asList(0, 0)).describedAs("Buffer Offset").isEqualTo(acBufferOffset.getAllValues());
    assertThat(Arrays.asList(BUFFER_SIZE, BUFFER_SIZE)).describedAs("Buffer Length").isEqualTo(acBufferLength.getAllValues());
    assertThat(Arrays.asList(true, true)).describedAs("is AppendBlob Append").isEqualTo(acAppendBlobAppend.getAllValues());

  }

  /**
   * The test verifies OutputStream Write of BUFFER_SIZE(4KB)  followed by a hflush call is making correct HTTP calls to the server
   */
  @Test
  public void verifyWriteRequestOfBufferSizeAndHFlush() throws Exception {

    AbfsClient client = mock(AbfsClient.class);
    AbfsRestOperation op = mock(AbfsRestOperation.class);
    AbfsConfiguration abfsConf;
    final Configuration conf = new Configuration();
    conf.set(accountKey1, accountValue1);
    abfsConf = new AbfsConfiguration(conf, accountName1);
    AbfsPerfTracker tracker = new AbfsPerfTracker("test", accountName1, abfsConf);

    when(client.getAbfsPerfTracker()).thenReturn(tracker);
    when(client.append(anyString(), anyLong(), any(byte[].class), anyInt(), anyInt(), any(), anyBoolean())).thenReturn(op);
    when(client.flush(anyString(), anyLong(), anyBoolean(), anyBoolean(), any())).thenReturn(op);

    AbfsOutputStream out = new AbfsOutputStream(client, null, PATH, 0, populateAbfsOutputStreamContext(BUFFER_SIZE, true, false, false));
    final byte[] b = new byte[BUFFER_SIZE];
    new Random().nextBytes(b);

    for (int i = 0; i < 2; i++) {
      out.write(b);
    }
    out.hflush();

    ArgumentCaptor<String> acString = ArgumentCaptor.forClass(String.class);
    ArgumentCaptor<Long> acLong = ArgumentCaptor.forClass(Long.class);
    ArgumentCaptor<Integer> acBufferOffset = ArgumentCaptor.forClass(Integer.class);
    ArgumentCaptor<Integer> acBufferLength = ArgumentCaptor.forClass(Integer.class);
    ArgumentCaptor<byte[]> acByteArray = ArgumentCaptor.forClass(byte[].class);
    ArgumentCaptor<Boolean> acAppendBlobAppend = ArgumentCaptor.forClass(Boolean.class);
    ArgumentCaptor<String> acSASToken = ArgumentCaptor.forClass(String.class);

    verify(client, times(2)).append(acString.capture(), acLong.capture(), acByteArray.capture(), acBufferOffset.capture(), acBufferLength.capture(),
                                    acSASToken.capture(), acAppendBlobAppend.capture());
    assertThat(Arrays.asList(PATH, PATH)).describedAs("File Path").isEqualTo(acString.getAllValues());
    assertThat(new HashSet<Long>(Arrays.asList(Long.valueOf(0), Long.valueOf(BUFFER_SIZE)))).describedAs("File Position").isEqualTo(
               new HashSet<Long>(acLong.getAllValues()));
    assertThat(Arrays.asList(0, 0)).describedAs("Buffer Offset").isEqualTo(acBufferOffset.getAllValues());
    assertThat(Arrays.asList(BUFFER_SIZE, BUFFER_SIZE)).describedAs("Buffer Length").isEqualTo(acBufferLength.getAllValues());

    ArgumentCaptor<String> acFlushString = ArgumentCaptor.forClass(String.class);
    ArgumentCaptor<Long> acFlushLong = ArgumentCaptor.forClass(Long.class);
    ArgumentCaptor<Boolean> acFlushRetainUnCommittedData = ArgumentCaptor.forClass(Boolean.class);
    ArgumentCaptor<Boolean> acFlushClose = ArgumentCaptor.forClass(Boolean.class);
    ArgumentCaptor<String> acFlushSASToken = ArgumentCaptor.forClass(String.class);

    verify(client, times(1)).flush(acFlushString.capture(), acFlushLong.capture(), acFlushRetainUnCommittedData.capture(), acFlushClose.capture(),
                                   acFlushSASToken.capture());
    assertThat(Arrays.asList(PATH)).describedAs("path").isEqualTo(acFlushString.getAllValues());
    assertThat(Arrays.asList(Long.valueOf(2*BUFFER_SIZE))).describedAs("position").isEqualTo(acFlushLong.getAllValues());
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
    when(client.append(anyString(), anyLong(), any(byte[].class), anyInt(), anyInt(), any(), anyBoolean())).thenReturn(op);
    when(client.flush(anyString(), anyLong(), anyBoolean(), anyBoolean(), any())).thenReturn(op);

    AbfsOutputStream out = new AbfsOutputStream(client, null, PATH, 0, populateAbfsOutputStreamContext(BUFFER_SIZE, true, false, false));
    final byte[] b = new byte[BUFFER_SIZE];
    new Random().nextBytes(b);

    for (int i = 0; i < 2; i++) {
      out.write(b);
    }
    Thread.sleep(1000);
    out.flush();
    Thread.sleep(1000);

    ArgumentCaptor<String> acString = ArgumentCaptor.forClass(String.class);
    ArgumentCaptor<Long> acLong = ArgumentCaptor.forClass(Long.class);
    ArgumentCaptor<Integer> acBufferOffset = ArgumentCaptor.forClass(Integer.class);
    ArgumentCaptor<Integer> acBufferLength = ArgumentCaptor.forClass(Integer.class);
    ArgumentCaptor<byte[]> acByteArray = ArgumentCaptor.forClass(byte[].class);
    ArgumentCaptor<Boolean> acAppendBlobAppend = ArgumentCaptor.forClass(Boolean.class);
    ArgumentCaptor<String> acSASToken = ArgumentCaptor.forClass(String.class);

    verify(client, times(2)).append(acString.capture(), acLong.capture(), acByteArray.capture(), acBufferOffset.capture(), acBufferLength.capture(),
                                    acSASToken.capture(), acAppendBlobAppend.capture());
    assertThat(Arrays.asList(PATH, PATH)).describedAs("path").isEqualTo(acString.getAllValues());
    assertThat(new HashSet<Long>(Arrays.asList(Long.valueOf(0), Long.valueOf(BUFFER_SIZE)))).describedAs("Position").isEqualTo(
               new HashSet<Long>(acLong.getAllValues()));
    assertThat(Arrays.asList(0, 0)).describedAs("Buffer Offset").isEqualTo(acBufferOffset.getAllValues());
    assertThat(Arrays.asList(BUFFER_SIZE, BUFFER_SIZE)).describedAs("Buffer Length").isEqualTo(acBufferLength.getAllValues());

  }
}
