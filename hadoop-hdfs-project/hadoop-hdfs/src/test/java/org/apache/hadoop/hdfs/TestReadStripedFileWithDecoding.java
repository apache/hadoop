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
package org.apache.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static org.apache.hadoop.hdfs.StripedFileTestUtil.blockSize;
import static org.apache.hadoop.hdfs.StripedFileTestUtil.cellSize;
import static org.apache.hadoop.hdfs.StripedFileTestUtil.dataBlocks;
import static org.apache.hadoop.hdfs.StripedFileTestUtil.numDNs;

public class TestReadStripedFileWithDecoding {

  private MiniDFSCluster cluster;
  private FileSystem fs;

  @Before
  public void setup() throws IOException {
    Configuration conf = new HdfsConfiguration();
    conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, blockSize);
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(numDNs).build();
    cluster.getFileSystem().getClient().createErasureCodingZone("/",
        null, cellSize);
    fs = cluster.getFileSystem();
  }

  @After
  public void tearDown() throws IOException {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @Test
  public void testWritePreadWithDNFailure1() throws IOException {
    testWritePreadWithDNFailure("/foo", 0);
  }

  @Test
  public void testWritePreadWithDNFailure2() throws IOException {
    testWritePreadWithDNFailure("/foo", cellSize * 5);
  }

  private void testWritePreadWithDNFailure(String file, int startOffsetInFile)
      throws IOException {
    final int failedDNIdx = 2;
    final int length = cellSize * (dataBlocks + 2);
    Path testPath = new Path(file);
    final byte[] bytes = StripedFileTestUtil.generateBytes(length);
    DFSTestUtil.writeFile(fs, testPath, bytes);

    // shut down the DN that holds the last internal data block
    BlockLocation[] locs = fs.getFileBlockLocations(testPath, cellSize * 5,
        cellSize);
    String name = (locs[0].getNames())[failedDNIdx];
    for (DataNode dn : cluster.getDataNodes()) {
      int port = dn.getXferPort();
      if (name.contains(Integer.toString(port))) {
        dn.shutdown();
        break;
      }
    }

    // pread
    try (FSDataInputStream fsdis = fs.open(testPath)) {
      byte[] buf = new byte[length];
      int readLen = fsdis.read(startOffsetInFile, buf, 0, buf.length);
      Assert.assertEquals("The length of file should be the same to write size",
          length - startOffsetInFile, readLen);

      byte[] expected = new byte[readLen];
      for (int i = startOffsetInFile; i < length; i++) {
        expected[i - startOffsetInFile] = StripedFileTestUtil.getByte(i);
      }

      for (int i = startOffsetInFile; i < length; i++) {
        Assert.assertEquals("Byte at " + i + " should be the same",
            expected[i - startOffsetInFile], buf[i - startOffsetInFile]);
      }
    }
  }
}
