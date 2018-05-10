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
package org.apache.hadoop.mapred;

import org.junit.Test;

import java.io.File;
import java.io.RandomAccessFile;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Unit test for FadvisedChunkedFile.
 */
public class TestFadvisedChunkedFile {

  @Test
  public void testDoubleClose() throws Exception {
    File absoluteFile = new File("target",
        TestFadvisedChunkedFile.class.getSimpleName()).getAbsoluteFile();
    absoluteFile.deleteOnExit();
    try {
      try (RandomAccessFile f = new RandomAccessFile(
          absoluteFile.getAbsolutePath(), "rw")) {
        FadvisedChunkedFile af = new FadvisedChunkedFile(
            f, 0, 5, 2, true,
            10, null, "foo");

        assertTrue("fd not valid", f.getFD().valid());
        af.close();
        assertFalse("fd still valid", f.getFD().valid());
        af.close();
        assertFalse("fd still valid", f.getFD().valid());
      }
    } finally {
      absoluteFile.delete();
    }
  }
}
