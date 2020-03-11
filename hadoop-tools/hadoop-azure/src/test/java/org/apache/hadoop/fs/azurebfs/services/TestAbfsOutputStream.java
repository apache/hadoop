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
import org.junit.Assert;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.apache.hadoop.fs.azurebfs.AbfsConfiguration;
import org.apache.hadoop.conf.Configuration;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.*;

public final class TestAbfsOutputStream {

  private static int bufferSize = 4096;
  private static int writeSize = 1000;
  private static String path = "~/testpath";
  private final String globalKey = "fs.azure.configuration";
  private final String accountName1 = "account1";
  private final String accountKey1 = globalKey + "." + accountName1;
  private final String accountValue1 = "one";

  @Test
  /**
   * The test verifies OutputStream shortwrite case(2000bytes write followed by flush, hflush, hsync) is making correct HTTP calls to the server
   */
  public void verifyShortWriteRequest() throws Exception {

    AbfsClient client = mock(AbfsClient.class);
    AbfsRestOperation op = mock(AbfsRestOperation.class);
    AbfsConfiguration abfsConf;
    final Configuration conf = new Configuration();
    conf.set(accountKey1, accountValue1);
    abfsConf = new AbfsConfiguration(conf, accountName1);
    AbfsPerfTracker tracker = new AbfsPerfTracker("test", accountName1, abfsConf);
    when(client.getAbfsPerfTracker()).thenReturn(tracker);
    when(client.append(anyString(), anyLong(), any(byte[].class), anyInt(), anyInt(), anyBoolean(), anyBoolean())).thenReturn(op);

    AbfsOutputStream out = new AbfsOutputStream(client, path, 0, bufferSize, true, false, true, false);
    final byte[] b = new byte[writeSize];
    new Random().nextBytes(b);
    out.write(b);
    out.hsync();
    ArgumentCaptor<String> acString = ArgumentCaptor.forClass(String.class);
    ArgumentCaptor<Long> acLong = ArgumentCaptor.forClass(Long.class);
    ArgumentCaptor<Integer> acInt = ArgumentCaptor.forClass(Integer.class);
    ArgumentCaptor<Boolean> acBool = ArgumentCaptor.forClass(Boolean.class);
    ArgumentCaptor<byte[]> acByteArray = ArgumentCaptor.forClass(byte[].class);

    final byte[] b1 = new byte[2*writeSize];
    new Random().nextBytes(b1);
    out.write(b1);
    out.flush();
    out.hflush();

    out.hsync();

    verify(client, times(2)).append(acString.capture(), acLong.capture(), acByteArray.capture(), acInt.capture(), acInt.capture(), acBool.capture(), acBool.capture());
    Assert.assertEquals(Arrays.asList(path, path) , acString.getAllValues());
    Assert.assertEquals(Arrays.asList(Long.valueOf(0), Long.valueOf(writeSize)), acLong.getAllValues());
    //flush=true, close=false, flush=true, close=false
    Assert.assertEquals(Arrays.asList(true, false, true, false), acBool.getAllValues());
    Assert.assertEquals(Arrays.asList(0,writeSize, 0, 2*writeSize), acInt.getAllValues());

  }

  @Test
  /**
   * The test verifies OutputStream Write of writeSize(1000 bytes) followed by a close is making correct HTTP calls to the server
   */
  public void verifyWriteRequest() throws Exception {

    AbfsClient client = mock(AbfsClient.class);
    AbfsRestOperation op = mock(AbfsRestOperation.class);
    AbfsConfiguration abfsConf;
    final Configuration conf = new Configuration();
    conf.set(accountKey1, accountValue1);
    abfsConf = new AbfsConfiguration(conf, accountName1);
    AbfsPerfTracker tracker = new AbfsPerfTracker("test", accountName1, abfsConf);

    when(client.getAbfsPerfTracker()).thenReturn(tracker);
    when(client.append(anyString(), anyLong(), any(byte[].class), anyInt(), anyInt(), anyBoolean(), anyBoolean())).thenReturn(op);

    AbfsOutputStream out = new AbfsOutputStream(client, path, 0, bufferSize, true, false, true, false);
    final byte[] b = new byte[writeSize];
    new Random().nextBytes(b);

    for (int i = 0; i < 5; i++) {
      out.write(b);
    }
    out.close();

    ArgumentCaptor<String> acString = ArgumentCaptor.forClass(String.class);
    ArgumentCaptor<Long> acLong = ArgumentCaptor.forClass(Long.class);
    ArgumentCaptor<Integer> acInt = ArgumentCaptor.forClass(Integer.class);
    ArgumentCaptor<Boolean> acBool = ArgumentCaptor.forClass(Boolean.class);
    ArgumentCaptor<byte[]> acByteArray = ArgumentCaptor.forClass(byte[].class);

    verify(client, times(2)).append(acString.capture(), acLong.capture(), acByteArray.capture(), acInt.capture(), acInt.capture(), acBool.capture(), acBool.capture());
    Assert.assertEquals(Arrays.asList(path, path) , acString.getAllValues());
    Assert.assertEquals(Arrays.asList(Long.valueOf(0), Long.valueOf(bufferSize)), acLong.getAllValues());
    //flush=false,close=false, flush=true,close=true
    Assert.assertEquals(Arrays.asList(false, false, true, true), acBool.getAllValues());
    Assert.assertEquals(Arrays.asList(0, bufferSize, 0, 5*writeSize-bufferSize), acInt.getAllValues());

  }

  @Test
  /**
   * The test verifies OutputStream Write of bufferSize(4KB) followed by a close is making correct HTTP calls to the server
   */
  public void verifyWriteRequestOfBufferSizeAndClose() throws Exception {

    AbfsClient client = mock(AbfsClient.class);
    AbfsRestOperation op = mock(AbfsRestOperation.class);
    AbfsConfiguration abfsConf;
    final Configuration conf = new Configuration();
    conf.set(accountKey1, accountValue1);
    abfsConf = new AbfsConfiguration(conf, accountName1);
    AbfsPerfTracker tracker = new AbfsPerfTracker("test", accountName1, abfsConf);

    when(client.getAbfsPerfTracker()).thenReturn(tracker);
    when(client.append(anyString(), anyLong(), any(byte[].class), anyInt(), anyInt(), anyBoolean(), anyBoolean())).thenReturn(op);
    when(client.flush(anyString(), anyLong(), anyBoolean(), anyBoolean())).thenReturn(op);

    AbfsOutputStream out = new AbfsOutputStream(client, path, 0, bufferSize, true, false, true, false);
    final byte[] b = new byte[bufferSize];
    new Random().nextBytes(b);

    for (int i = 0; i < 2; i++) {
      out.write(b);
    }
    out.close();

    ArgumentCaptor<String> acString = ArgumentCaptor.forClass(String.class);
    ArgumentCaptor<Long> acLong = ArgumentCaptor.forClass(Long.class);
    ArgumentCaptor<Integer> acInt = ArgumentCaptor.forClass(Integer.class);
    ArgumentCaptor<Boolean> acBool = ArgumentCaptor.forClass(Boolean.class);
    ArgumentCaptor<byte[]> acByteArray = ArgumentCaptor.forClass(byte[].class);

    verify(client, times(2)).append(acString.capture(), acLong.capture(), acByteArray.capture(), acInt.capture(), acInt.capture(), acBool.capture(), acBool.capture());
    Assert.assertEquals(Arrays.asList(path, path) , acString.getAllValues());
    Assert.assertEquals(new HashSet<Long>(Arrays.asList(Long.valueOf(0), Long.valueOf(bufferSize))), new HashSet<Long>(acLong.getAllValues()));
    //flush=false, close=false, flush=false, close=false
    Assert.assertEquals(Arrays.asList(false, false, false, false), acBool.getAllValues());
    Assert.assertEquals(Arrays.asList(0, bufferSize, 0, bufferSize), acInt.getAllValues());

    ArgumentCaptor<String> acFlushString = ArgumentCaptor.forClass(String.class);
    ArgumentCaptor<Long> acFlushLong = ArgumentCaptor.forClass(Long.class);
    ArgumentCaptor<Boolean> acFlushBool = ArgumentCaptor.forClass(Boolean.class);

    verify(client, times(1)).flush(acFlushString.capture(), acFlushLong.capture(), acFlushBool.capture(), acFlushBool.capture());
    Assert.assertEquals(Arrays.asList(path) , acFlushString.getAllValues());
    Assert.assertEquals(Arrays.asList(Long.valueOf(2*bufferSize)), acFlushLong.getAllValues());
    Assert.assertEquals(Arrays.asList(false, true), acFlushBool.getAllValues());

  }

  @Test
  /**
   * The test verifies OutputStream Write of bufferSize(4KB) is making correct HTTP calls to the server
   */
  public void verifyWriteRequestOfBufferSize() throws Exception {

    AbfsClient client = mock(AbfsClient.class);
    AbfsRestOperation op = mock(AbfsRestOperation.class);
    AbfsConfiguration abfsConf;
    final Configuration conf = new Configuration();
    conf.set(accountKey1, accountValue1);
    abfsConf = new AbfsConfiguration(conf, accountName1);
    AbfsPerfTracker tracker = new AbfsPerfTracker("test", accountName1, abfsConf);

    when(client.getAbfsPerfTracker()).thenReturn(tracker);
    when(client.append(anyString(), anyLong(), any(byte[].class), anyInt(), anyInt(), anyBoolean(), anyBoolean())).thenReturn(op);

    AbfsOutputStream out = new AbfsOutputStream(client, path, 0, bufferSize, true, false, true, false);
    final byte[] b = new byte[bufferSize];
    new Random().nextBytes(b);

    for (int i = 0; i < 2; i++) {
      out.write(b);
    }
    Thread.sleep(1000);

    ArgumentCaptor<String> acString = ArgumentCaptor.forClass(String.class);
    ArgumentCaptor<Long> acLong = ArgumentCaptor.forClass(Long.class);
    ArgumentCaptor<Integer> acInt = ArgumentCaptor.forClass(Integer.class);
    ArgumentCaptor<Boolean> acBool = ArgumentCaptor.forClass(Boolean.class);
    ArgumentCaptor<byte[]> acByteArray = ArgumentCaptor.forClass(byte[].class);

    verify(client, times(2)).append(acString.capture(), acLong.capture(), acByteArray.capture(), acInt.capture(), acInt.capture(), acBool.capture(), acBool.capture());
    Assert.assertEquals(Arrays.asList(path, path) , acString.getAllValues());
    Assert.assertEquals(new HashSet<Long>(Arrays.asList(Long.valueOf(0), Long.valueOf(bufferSize))), new HashSet<Long>(acLong.getAllValues()));
    //flush=false, close=false, flush=false, close=false
    Assert.assertEquals(Arrays.asList(false, false, false, false), acBool.getAllValues());
    Assert.assertEquals(Arrays.asList(0, bufferSize, 0, bufferSize), acInt.getAllValues());

  }

  @Test
  /**
   * The test verifies OutputStream Write of bufferSize(4KB) on a AppendBlob based stream is making correct HTTP calls to the server
   */
  public void verifyWriteRequestOfBufferSizeWithAppendBlob() throws Exception {

    AbfsClient client = mock(AbfsClient.class);
    AbfsRestOperation op = mock(AbfsRestOperation.class);
    AbfsConfiguration abfsConf;
    final Configuration conf = new Configuration();
    conf.set(accountKey1, accountValue1);
    abfsConf = new AbfsConfiguration(conf, accountName1);
    AbfsPerfTracker tracker = new AbfsPerfTracker("test", accountName1, abfsConf);

    when(client.getAbfsPerfTracker()).thenReturn(tracker);
    when(client.append(anyString(), anyLong(), any(byte[].class), anyInt(), anyInt(), anyBoolean(), anyBoolean())).thenReturn(op);

    AbfsOutputStream out = new AbfsOutputStream(client, path, 0, bufferSize, true, false, true, false);
    final byte[] b = new byte[bufferSize];
    new Random().nextBytes(b);

    for (int i = 0; i < 2; i++) {
      out.write(b);
    }
    Thread.sleep(1000);

    ArgumentCaptor<String> acString = ArgumentCaptor.forClass(String.class);
    ArgumentCaptor<Long> acLong = ArgumentCaptor.forClass(Long.class);
    ArgumentCaptor<Integer> acInt = ArgumentCaptor.forClass(Integer.class);
    ArgumentCaptor<Boolean> acBool = ArgumentCaptor.forClass(Boolean.class);
    ArgumentCaptor<byte[]> acByteArray = ArgumentCaptor.forClass(byte[].class);

    verify(client, times(2)).append(acString.capture(), acLong.capture(), acByteArray.capture(), acInt.capture(), acInt.capture(), acBool.capture(), acBool.capture());
    Assert.assertEquals(Arrays.asList(path, path) , acString.getAllValues());
    Assert.assertEquals(Arrays.asList(Long.valueOf(0), Long.valueOf(bufferSize)), acLong.getAllValues());
    //flush=false, close=false, flush=false, close=false
    Assert.assertEquals(Arrays.asList(false, false, false, false), acBool.getAllValues());
    Assert.assertEquals(Arrays.asList(0, bufferSize, 0, bufferSize), acInt.getAllValues());

  }

  @Test
  /**
   * The test verifies OutputStream Write of bufferSize(4KB)  followed by a hflush call is making correct HTTP calls to the server
   */
  public void verifyWriteRequestOfBufferSizeAndHFlush() throws Exception {

    AbfsClient client = mock(AbfsClient.class);
    AbfsRestOperation op = mock(AbfsRestOperation.class);
    AbfsConfiguration abfsConf;
    final Configuration conf = new Configuration();
    conf.set(accountKey1, accountValue1);
    abfsConf = new AbfsConfiguration(conf, accountName1);
    AbfsPerfTracker tracker = new AbfsPerfTracker("test", accountName1, abfsConf);

    when(client.getAbfsPerfTracker()).thenReturn(tracker);
    when(client.append(anyString(), anyLong(), any(byte[].class), anyInt(), anyInt(), anyBoolean(), anyBoolean())).thenReturn(op);
    when(client.flush(anyString(), anyLong(), anyBoolean(), anyBoolean())).thenReturn(op);

    AbfsOutputStream out = new AbfsOutputStream(client, path, 0, bufferSize, true, false, true, false);
    final byte[] b = new byte[bufferSize];
    new Random().nextBytes(b);

    for (int i = 0; i < 2; i++) {
      out.write(b);
    }
    out.hflush();

    ArgumentCaptor<String> acString = ArgumentCaptor.forClass(String.class);
    ArgumentCaptor<Long> acLong = ArgumentCaptor.forClass(Long.class);
    ArgumentCaptor<Integer> acInt = ArgumentCaptor.forClass(Integer.class);
    ArgumentCaptor<Boolean> acBool = ArgumentCaptor.forClass(Boolean.class);
    ArgumentCaptor<byte[]> acByteArray = ArgumentCaptor.forClass(byte[].class);

    verify(client, times(2)).append(acString.capture(), acLong.capture(), acByteArray.capture(), acInt.capture(), acInt.capture(), acBool.capture(), acBool.capture());
    Assert.assertEquals(Arrays.asList(path, path) , acString.getAllValues());
    Assert.assertEquals(new HashSet<Long>(Arrays.asList(Long.valueOf(0), Long.valueOf(bufferSize))), new HashSet<Long>(acLong.getAllValues()));
    //flush=false, close=false, flush=false, close=false
    Assert.assertEquals(Arrays.asList(false, false, false, false), acBool.getAllValues());
    Assert.assertEquals(Arrays.asList(0, bufferSize, 0, bufferSize), acInt.getAllValues());

    ArgumentCaptor<String> acFlushString = ArgumentCaptor.forClass(String.class);
    ArgumentCaptor<Long> acFlushLong = ArgumentCaptor.forClass(Long.class);
    ArgumentCaptor<Boolean> acFlushBool = ArgumentCaptor.forClass(Boolean.class);

    verify(client, times(1)).flush(acFlushString.capture(), acFlushLong.capture(), acFlushBool.capture(), acFlushBool.capture());
    Assert.assertEquals(Arrays.asList(path) , acFlushString.getAllValues());
    Assert.assertEquals(Arrays.asList(Long.valueOf(2*bufferSize)), acFlushLong.getAllValues());
    Assert.assertEquals(Arrays.asList(false, false), acFlushBool.getAllValues());

  }

  @Test
  /**
   * The test verifies OutputStream Write of bufferSize(4KB)  followed by a flush call is making correct HTTP calls to the server
   */
  public void verifyWriteRequestOfBufferSizeAndFlush() throws Exception {

    AbfsClient client = mock(AbfsClient.class);
    AbfsRestOperation op = mock(AbfsRestOperation.class);
    AbfsConfiguration abfsConf;
    final Configuration conf = new Configuration();
    conf.set(accountKey1, accountValue1);
    abfsConf = new AbfsConfiguration(conf, accountName1);
    AbfsPerfTracker tracker = new AbfsPerfTracker("test", accountName1, abfsConf);
    when(client.getAbfsPerfTracker()).thenReturn(tracker);
    when(client.append(anyString(), anyLong(), any(byte[].class), anyInt(), anyInt(), anyBoolean(), anyBoolean())).thenReturn(op);

    AbfsOutputStream out = new AbfsOutputStream(client, path, 0, bufferSize, true, false, true, false);
    final byte[] b = new byte[bufferSize];
    new Random().nextBytes(b);

    for (int i = 0; i < 2; i++) {
      out.write(b);
    }
    out.flush();
    Thread.sleep(1000);

    ArgumentCaptor<String> acString = ArgumentCaptor.forClass(String.class);
    ArgumentCaptor<Long> acLong = ArgumentCaptor.forClass(Long.class);
    ArgumentCaptor<Integer> acInt = ArgumentCaptor.forClass(Integer.class);
    ArgumentCaptor<Boolean> acBool = ArgumentCaptor.forClass(Boolean.class);
    ArgumentCaptor<byte[]> acByteArray = ArgumentCaptor.forClass(byte[].class);

    verify(client, times(2)).append(acString.capture(), acLong.capture(), acByteArray.capture(), acInt.capture(), acInt.capture(), acBool.capture(), acBool.capture());
    Assert.assertEquals(Arrays.asList(path, path) , acString.getAllValues());
    Assert.assertEquals(new HashSet<Long>(Arrays.asList(Long.valueOf(0), Long.valueOf(bufferSize))), new HashSet<Long>(acLong.getAllValues()));
    //flush=false, close=false, flush=false, close=false
    Assert.assertEquals(Arrays.asList(false, false, false, false), acBool.getAllValues());
    Assert.assertEquals(Arrays.asList(0, bufferSize, 0, bufferSize), acInt.getAllValues());

  }
}
