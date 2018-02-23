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
package org.apache.hadoop.hdfs.server.common.blockaliasmap.impl;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.server.common.FileRegion;
import org.apache.hadoop.hdfs.server.common.blockaliasmap.BlockAliasMap;
import org.junit.Test;

import java.io.File;
import java.nio.file.Files;
import java.util.Iterator;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

/**
 * Tests for the {@link LevelDBFileRegionAliasMap}.
 */
public class TestLevelDBFileRegionAliasMap {

  private static final String BPID = "BPID-0";

  /**
   * A basic test to verify that we can write data and read it back again.
   * @throws Exception
   */
  @Test
  public void testReadBack() throws Exception {
    File dbFile = Files.createTempDirectory("fileregionformat")
        .toFile();
    try {
      LevelDBFileRegionAliasMap frf = new LevelDBFileRegionAliasMap();
      LevelDBFileRegionAliasMap.LevelDBOptions opts =
          new LevelDBFileRegionAliasMap.LevelDBOptions()
              .filename(dbFile.getAbsolutePath());
      BlockAliasMap.Writer<FileRegion> writer = frf.getWriter(opts, BPID);

      FileRegion fr = new FileRegion(1, new Path("/file"), 1, 1, 1);
      writer.store(fr);
      writer.close();

      BlockAliasMap.Reader<FileRegion> reader = frf.getReader(opts, BPID);
      FileRegion fr2 = reader.resolve(new Block(1, 1, 1)).get();
      assertEquals(fr, fr2);
      reader.close();
    } finally {
      dbFile.delete();
    }
  }

  @Test
  /**
   * A basic test to verify that we can read a bunch of data that we've written.
   */
  public void testIterate() throws Exception {
    FileRegion[] regions = new FileRegion[10];
    regions[0] = new FileRegion(1, new Path("/file1"), 0, 1024, 1);
    regions[1] = new FileRegion(2, new Path("/file1"), 1024, 1024, 1);
    regions[2] = new FileRegion(3, new Path("/file1"), 2048, 1024, 1);
    regions[3] = new FileRegion(4, new Path("/file2"), 0, 1024, 1);
    regions[4] = new FileRegion(5, new Path("/file2"), 1024, 1024, 1);
    regions[5] = new FileRegion(6, new Path("/file2"), 2048, 1024, 1);
    regions[6] = new FileRegion(7, new Path("/file2"), 3072, 1024, 1);
    regions[7] = new FileRegion(8, new Path("/file3"), 0, 1024, 1);
    regions[8] = new FileRegion(9, new Path("/file4"), 0, 1024, 1);
    regions[9] = new FileRegion(10, new Path("/file5"), 0, 1024,  1);
    File dbFile = Files.createTempDirectory("fileregionformat")
        .toFile();
    try {
      LevelDBFileRegionAliasMap frf = new LevelDBFileRegionAliasMap();
      LevelDBFileRegionAliasMap.LevelDBOptions opts =
          new LevelDBFileRegionAliasMap.LevelDBOptions()
              .filename(dbFile.getAbsolutePath());
      BlockAliasMap.Writer<FileRegion> writer = frf.getWriter(opts, BPID);

      for (FileRegion fr : regions) {
        writer.store(fr);
      }
      writer.close();

      BlockAliasMap.Reader<FileRegion> reader = frf.getReader(opts, BPID);
      Iterator<FileRegion> it = reader.iterator();
      int last = -1;
      int count = 0;
      while(it.hasNext()) {
        FileRegion fr = it.next();
        int blockId = (int)fr.getBlock().getBlockId();
        assertEquals(regions[blockId-1], fr);
        assertNotEquals(blockId, last);
        last = blockId;
        count++;
      }
      assertEquals(count, 10);

      reader.close();
    } finally {
      dbFile.delete();
    }
  }
}
