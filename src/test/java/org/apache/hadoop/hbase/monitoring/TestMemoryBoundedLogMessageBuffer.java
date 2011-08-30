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
package org.apache.hadoop.hbase.monitoring;

import static org.junit.Assert.*;

import java.io.PrintWriter;
import java.io.StringWriter;

import org.junit.Test;

/**
 * Test case for the MemoryBoundedLogMessageBuffer utility.
 * Ensures that it uses no more memory than it's supposed to,
 * and that it properly deals with multibyte encodings.
 */
public class TestMemoryBoundedLogMessageBuffer {

  private static final long TEN_KB = 10 * 1024;
  private static final String JP_TEXT = "こんにちは";
  
  @Test
  public void testBuffer() {
    MemoryBoundedLogMessageBuffer buf =
      new MemoryBoundedLogMessageBuffer(TEN_KB);
    
    for (int i = 0; i < 1000; i++) {
      buf.add("hello " + i);
    }
    assertTrue("Usage too big: " + buf.estimateHeapUsage(),
        buf.estimateHeapUsage() < TEN_KB);
    assertTrue("Too many retained: " + buf.getMessages().size(),
        buf.getMessages().size() < 100);
    StringWriter sw = new StringWriter();
    buf.dumpTo(new PrintWriter(sw));
    String dump = sw.toString();
    System.out.println(dump);
    assertFalse("The early log messages should be evicted",
        dump.contains("hello 1\n"));
    assertTrue("The late log messages should be retained",
        dump.contains("hello 999\n"));
  }
  
  @Test
  public void testNonAsciiEncoding() {
    MemoryBoundedLogMessageBuffer buf =
      new MemoryBoundedLogMessageBuffer(TEN_KB);
    
    buf.add(JP_TEXT);
    StringWriter sw = new StringWriter();
    buf.dumpTo(new PrintWriter(sw));
    String dump = sw.toString();
    assertTrue(dump.contains(JP_TEXT));
  }
}
