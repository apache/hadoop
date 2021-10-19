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
package org.apache.hadoop.hdfs.server.datanode;

import java.util.function.Supplier;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.*;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.test.LambdaTestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.RandomAccessFile;

import static org.junit.Assert.assertEquals;

/**
 * Tests to ensure that a block is not read successfully from a datanode
 * when it has a corrupt metadata file.
 */
public class TestCorruptMetadataFile {

  private MiniDFSCluster cluster;
  private MiniDFSCluster.Builder clusterBuilder;
  private Configuration conf;

  @Before
  public void setUp() throws Exception {
    conf = new HdfsConfiguration();
    // Reduce block acquire retries as we only have 1 DN and it allows the
    // test to run faster
    conf.setInt(
        HdfsClientConfigKeys.DFS_CLIENT_MAX_BLOCK_ACQUIRE_FAILURES_KEY, 1);
    clusterBuilder = new MiniDFSCluster.Builder(conf).numDataNodes(1);
  }

  @After
  public void tearDown() throws Exception {
    if (cluster != null) {
      cluster.shutdown();
      cluster = null;
    }
  }

  @Test(timeout=60000)
  public void testReadBlockFailsWhenMetaIsCorrupt() throws Exception {
    cluster = clusterBuilder.build();
    cluster.waitActive();
    FileSystem fs = cluster.getFileSystem();
    DataNode dn0 = cluster.getDataNodes().get(0);
    Path filePath = new Path("test.dat");
    FSDataOutputStream out = fs.create(filePath, (short) 1);
    out.write(1);
    out.hflush();
    out.close();

    ExtendedBlock block = DFSTestUtil.getFirstBlock(fs, filePath);
    File metadataFile = cluster.getBlockMetadataFile(0, block);

    // First ensure we can read the file OK
    FSDataInputStream in = fs.open(filePath);
    in.readByte();
    in.close();

    // Now truncate the meta file, and ensure the data is not read OK
    RandomAccessFile raFile = new RandomAccessFile(metadataFile, "rw");
    raFile.setLength(0);

    FSDataInputStream intrunc = fs.open(filePath);
    LambdaTestUtils.intercept(BlockMissingException.class,
        () -> intrunc.readByte());
    intrunc.close();

    // Write 11 bytes to the file, but an invalid header
    raFile.write("12345678901".getBytes());
    assertEquals(11, raFile.length());

    FSDataInputStream ininvalid = fs.open(filePath);
    LambdaTestUtils.intercept(BlockMissingException.class,
        () -> ininvalid.readByte());
    ininvalid.close();

    GenericTestUtils.waitFor(new Supplier<Boolean>() {
      @Override
      public Boolean get() {
        return cluster.getNameNode().getNamesystem()
            .getBlockManager().getCorruptBlocks() == 1;
      }
    }, 100, 5000);

    raFile.close();
  }

  /**
   * This test create a sample block meta file and then attempts to load it
   * using BlockMetadataHeader to ensure it can load a valid file and that it
   * throws a CorruptMetaHeaderException when the header is invalid.
   * @throws Exception
   */
  @Test
  public void testBlockMetaDataHeaderPReadHandlesCorruptMetaFile()
      throws Exception {
    File testDir = GenericTestUtils.getTestDir();
    RandomAccessFile raFile = new RandomAccessFile(
        new File(testDir, "metafile"), "rw");

    // Write a valid header into the file
    // Version
    raFile.writeShort((short)1);
    // Checksum type
    raFile.writeByte(1);
    // Bytes per checksum
    raFile.writeInt(512);
    // We should be able to get the header with no exceptions
    BlockMetadataHeader header =
        BlockMetadataHeader.preadHeader(raFile.getChannel());

    // Now truncate the meta file to zero and ensure an exception is raised
    raFile.setLength(0);
    LambdaTestUtils.intercept(CorruptMetaHeaderException.class,
        () -> BlockMetadataHeader.preadHeader(raFile.getChannel()));

    // Now write a partial valid header to sure an exception is thrown
    // if the header cannot be fully read
    // Version
    raFile.writeShort((short)1);
    // Checksum type
    raFile.writeByte(1);

    LambdaTestUtils.intercept(CorruptMetaHeaderException.class,
        () -> BlockMetadataHeader.preadHeader(raFile.getChannel()));

    // Finally write the expected 7 bytes, but invalid data
    raFile.setLength(0);
    raFile.write("1234567".getBytes());

    LambdaTestUtils.intercept(CorruptMetaHeaderException.class,
        () -> BlockMetadataHeader.preadHeader(raFile.getChannel()));

    raFile.close();
  }
}
