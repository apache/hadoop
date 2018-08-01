/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdfs;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Test the BlockInputStream facade.
 */
@RunWith(MockitoJUnitRunner.class)
public class TestBlockInputStream {
  @Mock
  private BlockReader blockReaderMock;

  @Test
  public void testBlockInputStreamReadChar() {
    BlockInputStream is = new BlockInputStream(blockReaderMock);

    try {
      when(blockReaderMock.read(any(), eq(0), eq(1)))
          .thenReturn(32);
      // Making the mock perform the side effect of writing to buf is nasty.
      is.read();
      verify(blockReaderMock, times(1)).read(any(), eq(0), eq(1));
    } catch (IOException e) {
      fail("Could not even mock out read function.");
    }
  }

  @Test
  public void testBlockInputStreamReadBuf() {
    BlockInputStream is = new BlockInputStream(blockReaderMock);

    try {
      byte[] buf = new byte[1024];
      when(blockReaderMock.read(buf, 0, buf.length)).thenReturn(1024);
      is.read(buf, 0, buf.length);
      verify(blockReaderMock, times(1)).read(buf, 0, buf.length);
    } catch (IOException e) {
      fail("Could not even mock out read function.");
    }
  }

  @Test
  public void testBlockInputStreamSkip() {
    BlockInputStream is = new BlockInputStream(blockReaderMock);

    try {
      when(blockReaderMock.skip(10)).thenReturn(10L);
      long ret = is.skip(10);
      assertEquals(10, ret);
      verify(blockReaderMock, times(1)).skip(10L);
    } catch (IOException e) {
      fail("Could not even mock out skip function.");
    }
  }
}
