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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.io.InputStream;

import org.junit.Test;
import org.mockito.Mockito;

public class TestIOUtils {

  @Test
  public void testWrappedReadForCompressedData() throws IOException {
    byte[] buf = new byte[2];
    InputStream mockStream = Mockito.mock(InputStream.class);
    Mockito.when(mockStream.read(buf, 0, 1)).thenReturn(1);
    Mockito.when(mockStream.read(buf, 0, 2)).thenThrow(
        new java.lang.InternalError());

    try {
      assertEquals("Check expected value", 1,
          IOUtils.wrappedReadForCompressedData(mockStream, buf, 0, 1));
    } catch (IOException ioe) {
      fail("Unexpected error while reading");
    }
    try {
      IOUtils.wrappedReadForCompressedData(mockStream, buf, 0, 2);
    } catch (IOException ioe) {
      assertTrue("Unexpected exception caught",
          ioe.getMessage().contains("Error while reading compressed data"));
    }
  }
}
