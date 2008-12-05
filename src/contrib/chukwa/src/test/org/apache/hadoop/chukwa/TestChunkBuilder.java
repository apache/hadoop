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
package org.apache.hadoop.chukwa;

import junit.framework.TestCase;

public class TestChunkBuilder extends TestCase {


  public void testChunkBuilder()
  {
    ChunkBuilder cb = new ChunkBuilder();
    cb.addRecord("foo".getBytes());
    cb.addRecord("bar".getBytes());
    cb.addRecord("baz".getBytes());
    Chunk chunk = cb.getChunk();
    assertEquals(3, chunk.getRecordOffsets().length);
    assertEquals(9, chunk.getSeqID());
    assertEquals(2, chunk.getRecordOffsets()[0]);
    assertEquals(5, chunk.getRecordOffsets()[1]);
    assertEquals(8, chunk.getRecordOffsets()[2]);
  }

}
