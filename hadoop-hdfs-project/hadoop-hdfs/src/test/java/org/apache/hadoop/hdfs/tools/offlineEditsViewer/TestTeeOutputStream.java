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
package org.apache.hadoop.hdfs.tools.offlineEditsViewer;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.io.IOException;
import java.io.OutputStream;
import org.junit.jupiter.api.Test;

public class TestTeeOutputStream {

  @Test
  public void testCloseAttemptsEveryOutput() throws Exception {
    // close() must attempt every wrapped output even when an earlier one throws, rethrow the
    // first IOException, and attach later failures as suppressed exceptions.
    OutputStream first = mock(OutputStream.class);
    OutputStream second = mock(OutputStream.class);
    OutputStream third = mock(OutputStream.class);
    IOException firstFailure = new IOException("first");
    IOException secondFailure = new IOException("second");
    doThrow(firstFailure).when(first).close();
    doThrow(secondFailure).when(second).close();

    TeeOutputStream tee = new TeeOutputStream(new OutputStream[] {first, second, third});

    IOException thrown = assertThrows(IOException.class, tee::close);

    assertEquals(firstFailure, thrown);
    assertArrayEquals(new Throwable[] {secondFailure}, thrown.getSuppressed());
    verify(third).close();
  }
}
