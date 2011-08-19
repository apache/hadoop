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

package org.apache.hadoop.hdfs.protocol;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;

import static org.junit.Assert.assertTrue;
import org.junit.Test;

import org.apache.hadoop.io.DataOutputBuffer;

public class TestCorruptFileBlocks {

  /**
   * Serialize the cfb given, deserialize and return the result.
   */
  static CorruptFileBlocks serializeAndDeserialize(CorruptFileBlocks cfb) 
    throws IOException {
    DataOutputBuffer buf = new DataOutputBuffer();
    cfb.write(buf);

    byte[] data = buf.getData();
    DataInputStream input = new DataInputStream(new ByteArrayInputStream(data));

    CorruptFileBlocks result = new CorruptFileBlocks();
    result.readFields(input);

    return result;
  }

  /**
   * Check whether cfb is unchanged after serialization and deserialization.
   */
  static boolean checkSerialize(CorruptFileBlocks cfb)
    throws IOException {
    return cfb.equals(serializeAndDeserialize(cfb));
  }

  /**
   * Test serialization and deserializaton of CorruptFileBlocks.
   */
  @Test
  public void testSerialization() throws IOException {
    {
      CorruptFileBlocks cfb = new CorruptFileBlocks();
      assertTrue("cannot serialize empty CFB", checkSerialize(cfb));
    }

    {
      String[] files = new String[0];
      CorruptFileBlocks cfb = new CorruptFileBlocks(files, "");
      assertTrue("cannot serialize CFB with empty cookie", checkSerialize(cfb));
    }

    {
      String[] files = { "a", "bb", "ccc" };
      CorruptFileBlocks cfb = new CorruptFileBlocks(files, "test");
      assertTrue("cannot serialize CFB", checkSerialize(cfb));
    }
  }
}