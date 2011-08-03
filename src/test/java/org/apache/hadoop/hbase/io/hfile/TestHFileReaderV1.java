/*
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

package org.apache.hadoop.hbase.io.hfile;

import java.io.IOException;
import java.net.URL;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.util.Bytes;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

public class TestHFileReaderV1 {

  private static final HBaseTestingUtility TEST_UTIL =
    new HBaseTestingUtility();

  private Configuration conf;
  private FileSystem fs;

  private static final int N = 1000;

  @Before
  public void setUp() throws IOException {
    conf = TEST_UTIL.getConfiguration();
    fs = FileSystem.get(conf);
  }

  @Test
  public void testReadingExistingVersion1HFile() throws IOException {
    URL url = TestHFileReaderV1.class.getResource(
        "8e8ab58dcf39412da19833fcd8f687ac");
    Path existingHFilePath = new Path(url.getPath());
    HFile.Reader reader =
      HFile.createReader(fs, existingHFilePath, null, false, false);
    reader.loadFileInfo();
    FixedFileTrailer trailer = reader.getTrailer();

    assertEquals(N, reader.getEntries());
    assertEquals(N, trailer.getEntryCount());
    assertEquals(1, trailer.getVersion());
    assertEquals(Compression.Algorithm.GZ, trailer.getCompressionCodec());

    for (boolean pread : new boolean[] { false, true }) {
      int totalDataSize = 0;
      int n = 0;

      HFileScanner scanner = reader.getScanner(false, pread);
      assertTrue(scanner.seekTo());
      do {
        totalDataSize += scanner.getKey().limit() + scanner.getValue().limit()
            + Bytes.SIZEOF_INT * 2;
        ++n;
      } while (scanner.next());

      // Add magic record sizes, one per data block.
      totalDataSize += 8 * trailer.getDataIndexCount();

      assertEquals(N, n);
      assertEquals(trailer.getTotalUncompressedBytes(), totalDataSize);
    }
    reader.close();
  }

}
