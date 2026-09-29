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

package org.apache.hadoop.fs.tosfs.object.tos;

import com.volcengine.tos.internal.util.aborthook.AbortInputStreamHook;
import com.volcengine.tos.model.object.GetObjectBasicOutput;
import com.volcengine.tos.model.object.GetObjectV2Output;
import org.apache.hadoop.fs.tosfs.object.Constants;
import org.apache.hadoop.fs.tosfs.util.TestUtility;
import org.apache.hadoop.thirdparty.com.google.common.io.ByteStreams;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestTOSInputStream {

  private static final int DATA_SIZE = 1 << 20;
  private static final byte[] DATA = TestUtility.rand(DATA_SIZE);

  @Test
  public void testForceClose() throws IOException {
    TOSInputStream stream = createStream(DATA, 0, DATA_SIZE - 1, 1024);
    stream.close();
    assertTrue(cast(stream).isForceClose(), "Expected force close");

    stream = createStream(DATA, 0, DATA_SIZE - 1, 1024);
    ByteStreams.skipFully(stream, DATA_SIZE - 1024 - 1);
    stream.close();
    assertTrue(cast(stream).isForceClose(), "Expected force close");

    stream = createStream(DATA, 0, -1, 1024);
    stream.close();
    assertTrue(cast(stream).isForceClose(), "Expected force close");

    stream = createStream(DATA, 0, -1, 1024);
    ByteStreams.skipFully(stream, DATA_SIZE - 1024 - 1);
    stream.close();
    assertTrue(cast(stream).isForceClose(), "Expected force close");

    stream = createStream(DATA, 0, -1, 1024);
    ByteStreams.skipFully(stream, DATA_SIZE - 1024);
    stream.close();
    assertTrue(cast(stream).isForceClose(), "Expected force close");
  }

  @Test
  public void testClose() throws IOException {
    TOSInputStream stream = createStream(DATA, 0, DATA_SIZE - 1, DATA_SIZE);
    stream.close();
    assertFalse(cast(stream).isForceClose(), "Expected close by skipping bytes");

    stream = createStream(DATA, 0, DATA_SIZE - 1, 1024);
    ByteStreams.skipFully(stream, DATA_SIZE - 1024);
    stream.close();
    assertFalse(cast(stream).isForceClose(), "Expected close by skipping bytes");

    stream = createStream(DATA, 0, DATA_SIZE - 1, 1024);
    ByteStreams.skipFully(stream, DATA_SIZE - 1023);
    stream.close();
    assertFalse(cast(stream).isForceClose(), "Expected close by skipping bytes");

    stream = createStream(DATA, 0, -1, DATA_SIZE + 1);
    stream.close();
    assertFalse(cast(stream).isForceClose(), "Expected close by skipping bytes");

    stream = createStream(DATA, 0, -1, 1024);
    ByteStreams.skipFully(stream, DATA_SIZE - 1023);
    stream.close();
    assertFalse(cast(stream).isForceClose(), "Expected close by skipping bytes");

    stream = createStream(DATA, 0, -1, 1024);
    ByteStreams.skipFully(stream, DATA_SIZE);
    stream.close();
    assertFalse(cast(stream).isForceClose(), "Expected close by skipping bytes");
  }

  private TestInputStream cast(TOSInputStream stream) throws IOException {
    InputStream content = stream.getObjectOutput().verifiedContent(Constants.MAGIC_CHECKSUM);
    assertTrue(content instanceof TestInputStream, "Not a TestInputStream");
    return (TestInputStream) content;
  }

  private TOSInputStream createStream(byte[] data, long startOff, long endOff, long maxDrainSize)
      throws IOException {
    TestInputStream stream =
        new TestInputStream(data, (int) startOff, (int) (data.length - startOff));
    GetObjectV2Output output = new GetObjectV2Output(new GetObjectBasicOutput(), stream).setHook(
        new ForceCloseHook(stream));

    return new TOSInputStream(new GetObjectOutput(output, Constants.MAGIC_CHECKSUM), startOff,
        endOff, maxDrainSize, Constants.MAGIC_CHECKSUM);
  }

  private final static class TestInputStream extends ByteArrayInputStream {
    // -1 means call close()
    //  0 means neither call close() nor forceClose()
    //  1 means call forceClose()
    private int cloeState = 0;

    private TestInputStream(byte[] buf, int off, int len) {
      super(buf, off, len);
    }

    @Override
    public void close() {
      cloeState = -1;
    }

    public void forceClose() {
      cloeState = 1;
    }

    boolean isForceClose() {
      assertTrue(cloeState == -1 || cloeState == 1, "Neither call close() nor forceClose()");
      return cloeState == 1;
    }
  }

  private final static class ForceCloseHook implements AbortInputStreamHook {
    private final TestInputStream in;

    private ForceCloseHook(TestInputStream in) {
      this.in = in;
    }

    @Override public void abort() {
      if (in != null) {
        in.forceClose();
      }
    }
  }
}
