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
package org.apache.hadoop.hdfs.client.impl;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.Random;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.BlockReader;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

abstract public class TestBlockReaderBase {
  private BlockReaderTestUtil util;
  private byte[] blockData;
  private BlockReader reader;

  /**
   * if override this, make sure return array length is less than
   * block size.
   */
  byte [] getBlockData() {
    int length = 1 << 22;
    byte[] data = new byte[length];
    for (int i = 0; i < length; i++) {
      data[i] = (byte) (i % 133);
    }
    return data;
  }

  private BlockReader getBlockReader(LocatedBlock block) throws Exception {
    return util.getBlockReader(block, 0, blockData.length);
  }

  abstract HdfsConfiguration createConf();

  @Before
  public void setup() throws Exception {
    util = new BlockReaderTestUtil(1, createConf());
    blockData = getBlockData();
    DistributedFileSystem fs = util.getCluster().getFileSystem();
    Path testfile = new Path("/testfile");
    FSDataOutputStream fout = fs.create(testfile);
    fout.write(blockData);
    fout.close();
    LocatedBlock blk = util.getFileBlocks(testfile, blockData.length).get(0);
    reader = getBlockReader(blk);
  }

  @After
  public void shutdown() throws Exception {
    util.shutdown();
  }

  @Test(timeout=60000)
  public void testSkip() throws IOException {
    Random random = new Random();
    byte [] buf = new byte[1];
    for (int pos = 0; pos < blockData.length;) {
      long skip = random.nextInt(100) + 1;
      long skipped = reader.skip(skip);
      if (pos + skip >= blockData.length) {
        assertEquals(blockData.length, pos + skipped);
        break;
      } else {
        assertEquals(skip, skipped);
        pos += skipped;
        assertEquals(1, reader.read(buf, 0, 1));

        assertEquals(blockData[pos], buf[0]);
        pos += 1;
      }
    }
  }
}
