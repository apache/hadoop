/**
 * Copyright 2011 The Apache Software Foundation
 *
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

package org.apache.hadoop.hbase.io.hfile.slab;

import static org.junit.Assert.*;
import java.nio.ByteBuffer;
import org.junit.*;

/**Test cases for Slab.java*/
public class TestSlab {
  static final int BLOCKSIZE = 1000;
  static final int NUMBLOCKS = 100;
  Slab testSlab;
  ByteBuffer[] buffers = new ByteBuffer[NUMBLOCKS];

  @Before
  public void setUp() {
    testSlab = new Slab(BLOCKSIZE, NUMBLOCKS);
  }

  @After
  public void tearDown() {
    testSlab.shutdown();
  }

  @Test
  public void testBasicFunctionality() throws InterruptedException {
    for (int i = 0; i < NUMBLOCKS; i++) {
      buffers[i] = testSlab.alloc(BLOCKSIZE);
      assertEquals(BLOCKSIZE, buffers[i].limit());
    }

    // write an unique integer to each allocated buffer.
    for (int i = 0; i < NUMBLOCKS; i++) {
      buffers[i].putInt(i);
    }

    // make sure the bytebuffers remain unique (the slab allocator hasn't
    // allocated the same chunk of memory twice)
    for (int i = 0; i < NUMBLOCKS; i++) {
      buffers[i].putInt(i);
    }

    for (int i = 0; i < NUMBLOCKS; i++) {
      testSlab.free(buffers[i]); // free all the buffers.
    }

    for (int i = 0; i < NUMBLOCKS; i++) {
      buffers[i] = testSlab.alloc(BLOCKSIZE);
      assertEquals(BLOCKSIZE, buffers[i].limit());
    }
  }

}
