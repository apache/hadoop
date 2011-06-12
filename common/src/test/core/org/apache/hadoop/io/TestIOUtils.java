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

package org.apache.hadoop.io;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.junit.Test;
import org.mockito.Mockito;

/**
 * Test cases for IOUtils.java
 */
public class TestIOUtils {

  @Test
  public void testCopyBytesShouldCloseStreamsWhenCloseIsTrue() throws Exception {
    InputStream inputStream = Mockito.mock(InputStream.class);
    OutputStream outputStream = Mockito.mock(OutputStream.class);
    Mockito.doReturn(-1).when(inputStream).read(new byte[1]);
    IOUtils.copyBytes(inputStream, outputStream, 1, true);
    Mockito.verify(inputStream, Mockito.atLeastOnce()).close();
    Mockito.verify(outputStream, Mockito.atLeastOnce()).close();
  }

  @Test
  public void testCopyBytesShouldCloseInputSteamWhenOutputStreamCloseThrowsException()
      throws Exception {
    InputStream inputStream = Mockito.mock(InputStream.class);
    OutputStream outputStream = Mockito.mock(OutputStream.class);
    Mockito.doReturn(-1).when(inputStream).read(new byte[1]);
    Mockito.doThrow(new IOException()).when(outputStream).close();
    try{
      IOUtils.copyBytes(inputStream, outputStream, 1, true);
    } catch (IOException e) {
    }
    Mockito.verify(inputStream, Mockito.atLeastOnce()).close();
    Mockito.verify(outputStream, Mockito.atLeastOnce()).close();
  }

  @Test
  public void testCopyBytesShouldNotCloseStreamsWhenCloseIsFalse()
      throws Exception {
    InputStream inputStream = Mockito.mock(InputStream.class);
    OutputStream outputStream = Mockito.mock(OutputStream.class);
    Mockito.doReturn(-1).when(inputStream).read(new byte[1]);
    IOUtils.copyBytes(inputStream, outputStream, 1, false);
    Mockito.verify(inputStream, Mockito.atMost(0)).close();
    Mockito.verify(outputStream, Mockito.atMost(0)).close();
  }
}
