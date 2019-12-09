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
package org.apache.hadoop.mapred.nativetask.utils;

import org.junit.Test;
import org.junit.Assert;

import org.apache.hadoop.mapred.nativetask.util.ReadWriteBuffer;

public class TestReadWriteBuffer {

  private static byte[] bytes = new byte[] { '0', 'a', 'b', 'c', 'd', '9' };

  @Test
  public void testReadWriteBuffer() {

    final ReadWriteBuffer buffer = new ReadWriteBuffer();

    Assert.assertFalse(buffer.getBuff() == null);

    Assert.assertEquals(buffer.getWritePoint(), 0);
    Assert.assertEquals(buffer.getReadPoint(), 0);

    buffer.writeInt(3);

    buffer.writeString("goodboy");

    buffer.writeLong(10L);
    buffer.writeBytes(bytes, 0, bytes.length);
    buffer.writeLong(100L);

    Assert.assertEquals(buffer.getWritePoint(), 41);
    Assert.assertEquals(buffer.getReadPoint(), 0);
    Assert.assertTrue(buffer.getBuff().length >= 41);

    Assert.assertEquals(buffer.readInt(), 3);
    Assert.assertEquals(buffer.readString(), "goodboy");
    Assert.assertEquals(buffer.readLong(), 10L);

    final byte[] read = buffer.readBytes();
    for (int i = 0; i < bytes.length; i++) {
      Assert.assertEquals(bytes[i], read[i]);
    }

    Assert.assertEquals(100L, buffer.readLong());
    Assert.assertEquals(41, buffer.getReadPoint());
  }
}
