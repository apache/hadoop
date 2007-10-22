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

package org.apache.hadoop.fs;

import junit.framework.TestCase;

public class TestChecksumFileSystem extends TestCase {
  public void testgetChecksumLength() throws Exception {
    assertEquals(8, ChecksumFileSystem.getChecksumLength(0L, 512));
    assertEquals(12, ChecksumFileSystem.getChecksumLength(1L, 512));
    assertEquals(12, ChecksumFileSystem.getChecksumLength(512L, 512));
    assertEquals(16, ChecksumFileSystem.getChecksumLength(513L, 512));
    assertEquals(16, ChecksumFileSystem.getChecksumLength(1023L, 512));
    assertEquals(16, ChecksumFileSystem.getChecksumLength(1024L, 512));
    assertEquals(408, ChecksumFileSystem.getChecksumLength(100L, 1));
    assertEquals(4000000000008L,
                 ChecksumFileSystem.getChecksumLength(10000000000000L, 10));    
  }    
}
