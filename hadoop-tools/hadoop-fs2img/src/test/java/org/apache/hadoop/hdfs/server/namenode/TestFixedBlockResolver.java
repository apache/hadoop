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
package org.apache.hadoop.hdfs.server.namenode;

import java.util.Iterator;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.BlockProto;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import static org.junit.Assert.*;

/**
 * Validate fixed-size block partitioning.
 */
public class TestFixedBlockResolver {

  @Rule public TestName name = new TestName();

  private final FixedBlockResolver blockId = new FixedBlockResolver();

  @Before
  public void setup() {
    Configuration conf = new Configuration(false);
    conf.setLong(FixedBlockResolver.BLOCKSIZE, 512L * (1L << 20));
    conf.setLong(FixedBlockResolver.START_BLOCK, 512L * (1L << 20));
    blockId.setConf(conf);
    System.out.println(name.getMethodName());
  }

  @Test
  public void testExactBlock() throws Exception {
    FileStatus f = file(512, 256);
    int nblocks = 0;
    for (BlockProto b : blockId.resolve(f)) {
      ++nblocks;
      assertEquals(512L * (1L << 20), b.getNumBytes());
    }
    assertEquals(1, nblocks);

    FileStatus g = file(1024, 256);
    nblocks = 0;
    for (BlockProto b : blockId.resolve(g)) {
      ++nblocks;
      assertEquals(512L * (1L << 20), b.getNumBytes());
    }
    assertEquals(2, nblocks);

    FileStatus h = file(5120, 256);
    nblocks = 0;
    for (BlockProto b : blockId.resolve(h)) {
      ++nblocks;
      assertEquals(512L * (1L << 20), b.getNumBytes());
    }
    assertEquals(10, nblocks);
  }

  @Test
  public void testEmpty() throws Exception {
    FileStatus f = file(0, 100);
    Iterator<BlockProto> b = blockId.resolve(f).iterator();
    assertTrue(b.hasNext());
    assertEquals(0, b.next().getNumBytes());
    assertFalse(b.hasNext());
  }

  @Test
  public void testRandomFile() throws Exception {
    Random r = new Random();
    long seed = r.nextLong();
    System.out.println("seed: " + seed);
    r.setSeed(seed);

    int len = r.nextInt(4096) + 512;
    int blk = r.nextInt(len - 128) + 128;
    FileStatus s = file(len, blk);
    long nbytes = 0;
    for (BlockProto b : blockId.resolve(s)) {
      nbytes += b.getNumBytes();
      assertTrue(512L * (1L << 20) >= b.getNumBytes());
    }
    assertEquals(s.getLen(), nbytes);
  }

  FileStatus file(long lenMB, long blocksizeMB) {
    Path p = new Path("foo://bar:4344/baz/dingo");
    return new FileStatus(
          lenMB * (1 << 20),       /* long length,             */
          false,                   /* boolean isdir,           */
          1,                       /* int block_replication,   */
          blocksizeMB * (1 << 20), /* long blocksize,          */
          0L,                      /* long modification_time,  */
          0L,                      /* long access_time,        */
          null,                    /* FsPermission permission, */
          "hadoop",                /* String owner,            */
          "hadoop",                /* String group,            */
          p);                      /* Path path                */
  }

}
