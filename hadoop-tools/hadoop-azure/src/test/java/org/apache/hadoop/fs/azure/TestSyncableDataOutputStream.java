/*
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
package org.apache.hadoop.fs.azure;

import java.io.IOException;
import java.io.OutputStream;

import org.junit.Test;

import org.apache.hadoop.test.LambdaTestUtils;

public class TestSyncableDataOutputStream {

  @Test
  public void testCloseWhenFlushThrowingIOException() throws Exception {
    MockOutputStream out = new MockOutputStream();
    SyncableDataOutputStream sdos = new SyncableDataOutputStream(out);
    out.flushThrowIOE = true;
    LambdaTestUtils.intercept(IOException.class, "An IOE from flush", () -> sdos.close());
    MockOutputStream out2 = new MockOutputStream();
    out2.flushThrowIOE = true;
    LambdaTestUtils.intercept(IOException.class, "An IOE from flush", () -> {
      try (SyncableDataOutputStream sdos2 = new SyncableDataOutputStream(out2)) {
      }
    });
  }

  private static class MockOutputStream extends OutputStream {

    private boolean flushThrowIOE = false;
    private IOException lastException = null;

    @Override
    public void write(int arg0) throws IOException {

    }

    @Override
    public void flush() throws IOException {
      if (this.flushThrowIOE) {
        this.lastException = new IOException("An IOE from flush");
        throw this.lastException;
      }
    }

    @Override
    public void close() throws IOException {
      if (this.lastException != null) {
        throw this.lastException;
      }
    }
  }
}
