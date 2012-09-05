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
package org.apache.hadoop.hdfs.util;

import java.io.File;
import java.io.IOException;
import java.util.Random;

import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.io.IOUtils;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

public class TestBestEffortLongFile {

  private static final File FILE = new File(MiniDFSCluster.getBaseDirectory() +
      File.separatorChar + "TestBestEffortLongFile");

  @Before
  public void cleanup() {
    if (FILE.exists()) {
      assertTrue(FILE.delete());
    }
    FILE.getParentFile().mkdirs();
  }
  
  @Test
  public void testGetSet() throws IOException {
    BestEffortLongFile f = new BestEffortLongFile(FILE, 12345L);
    try {
      // Before the file exists, should return default.
      assertEquals(12345L, f.get());
      
      // And first access should open it.
      assertTrue(FILE.exists());
  
      Random r = new Random();
      for (int i = 0; i < 100; i++) {
        long newVal = r.nextLong();
        // Changing the value should be reflected in the next get() call.
        f.set(newVal);
        assertEquals(newVal, f.get());
        
        // And should be reflected in a new instance (ie it actually got
        // written to the file)
        BestEffortLongFile f2 = new BestEffortLongFile(FILE, 999L);
        try {
          assertEquals(newVal, f2.get());
        } finally {
          IOUtils.closeStream(f2);
        }
      }
    } finally {
      IOUtils.closeStream(f);
    }
  }
  
  @Test
  public void testTruncatedFileReturnsDefault() throws IOException {
    assertTrue(FILE.createNewFile());
    assertEquals(0, FILE.length());
    BestEffortLongFile f = new BestEffortLongFile(FILE, 12345L);
    try {
      assertEquals(12345L, f.get());
    } finally {
      f.close();
    }
  }
}
