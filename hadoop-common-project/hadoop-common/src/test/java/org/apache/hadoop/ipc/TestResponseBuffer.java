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

package org.apache.hadoop.ipc;

import static org.junit.Assert.assertEquals;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import org.apache.hadoop.ipc.ResponseBuffer;
import org.junit.Test;

/** Unit tests for ResponseBuffer. */
public class TestResponseBuffer {
  @Test
  public void testBuffer() throws IOException {
    final int startSize = 8;
    final String empty = "";
    ResponseBuffer buf = new ResponseBuffer(startSize);
    assertEquals(startSize, buf.capacity());

    // verify it's initially empty
    checkBuffer(buf, empty);
    // write "nothing" and re-verify it's empty
    buf.writeBytes(empty);
    checkBuffer(buf, empty);

    // write to the buffer twice and verify it's properly encoded
    String s1 = "testing123";
    buf.writeBytes(s1);
    checkBuffer(buf, s1);
    String s2 = "456!";
    buf.writeBytes(s2);
    checkBuffer(buf, s1 + s2);

    // reset should not change length of underlying byte array
    int length = buf.capacity();
    buf.reset();
    assertEquals(length, buf.capacity());
    checkBuffer(buf, empty);

    // setCapacity will change length of underlying byte array
    buf.setCapacity(startSize);
    assertEquals(startSize, buf.capacity());
    checkBuffer(buf, empty);

    // make sure it still works
    buf.writeBytes(s1);
    checkBuffer(buf, s1);
    buf.writeBytes(s2);
    checkBuffer(buf, s1 + s2);
  }

  private void checkBuffer(ResponseBuffer buf, String expected)
      throws IOException {
    // buffer payload length matches expected length
    int expectedLength = expected.getBytes().length;
    assertEquals(expectedLength, buf.size());
    // buffer has the framing bytes (int)
    byte[] framed = buf.toByteArray();
    assertEquals(expectedLength + 4, framed.length);

    // verify encoding of buffer: framing (int) + payload bytes
    DataInputStream dis =
        new DataInputStream(new ByteArrayInputStream(framed));
    assertEquals(expectedLength, dis.readInt());
    assertEquals(expectedLength, dis.available());
    byte[] payload = new byte[expectedLength];
    dis.readFully(payload);
    assertEquals(expected, new String(payload));
  }
}
