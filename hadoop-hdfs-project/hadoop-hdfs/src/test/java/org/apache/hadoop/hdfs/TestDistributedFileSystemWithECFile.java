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
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.hdfs.protocol.ErasureCodingPolicy;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertTrue;

/**
 * Testing correctness of FileSystem.getFileBlockLocations and
 * FileSystem.listFiles for erasure coded files.
 */
public class TestDistributedFileSystemWithECFile {
  private final ErasureCodingPolicy ecPolicy =
      StripedFileTestUtil.getDefaultECPolicy();
  private final int cellSize = ecPolicy.getCellSize();
  private final short dataBlocks = (short) ecPolicy.getNumDataUnits();
  private final short parityBlocks = (short) ecPolicy.getNumParityUnits();
  private final int numDNs = dataBlocks + parityBlocks;
  private final int stripesPerBlock = 4;
  private final int blockSize = stripesPerBlock * cellSize;
  private final int blockGroupSize = blockSize * dataBlocks;

  private MiniDFSCluster cluster;
  private FileContext fileContext;
  private DistributedFileSystem fs;
  private Configuration conf = new HdfsConfiguration();

  @Before
  public void setup() throws IOException {
    conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, blockSize);
    conf.setBoolean(DFSConfigKeys.DFS_NAMENODE_REDUNDANCY_CONSIDERLOAD_KEY,
        false);
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(numDNs).build();
    fileContext = FileContext.getFileContext(cluster.getURI(0), conf);
    fs = cluster.getFileSystem();
    fs.enableErasureCodingPolicy(
        StripedFileTestUtil.getDefaultECPolicy().getName());
    fs.mkdirs(new Path("/ec"));
    cluster.getFileSystem().getClient().setErasureCodingPolicy("/ec",
        StripedFileTestUtil.getDefaultECPolicy().getName());
  }

  @After
  public void tearDown() throws IOException {
    if (cluster != null) {
      cluster.shutdown();
      cluster = null;
    }
  }

  private void createFile(String path, int size) throws Exception {
    byte[] expected = StripedFileTestUtil.generateBytes(size);
    Path src = new Path(path);
    DFSTestUtil.writeFile(fs, src, new String(expected));
    StripedFileTestUtil.waitBlockGroupsReported(fs, src.toString());
    StripedFileTestUtil.verifyLength(fs, src, size);
  }

  @Test(timeout=60000)
  public void testListECFilesSmallerThanOneCell() throws Exception {
    createFile("/ec/smallcell", 1);
    final List<LocatedFileStatus> retVal = new ArrayList<>();
    final RemoteIterator<LocatedFileStatus> iter =
        cluster.getFileSystem().listFiles(new Path("/ec"), true);
    while (iter.hasNext()) {
      retVal.add(iter.next());
    }
    assertTrue(retVal.size() == 1);
    LocatedFileStatus fileStatus = retVal.get(0);
    assertSmallerThanOneCell(fileStatus.getBlockLocations());

    BlockLocation[] locations = cluster.getFileSystem().getFileBlockLocations(
        fileStatus, 0, fileStatus.getLen());
    assertSmallerThanOneCell(locations);

    //Test FileContext
    fileStatus = fileContext.listLocatedStatus(new Path("/ec")).next();
    assertSmallerThanOneCell(fileStatus.getBlockLocations());
    locations = fileContext.getFileBlockLocations(new Path("/ec/smallcell"),
        0, fileStatus.getLen());
    assertSmallerThanOneCell(locations);
  }

  private void assertSmallerThanOneCell(BlockLocation[] locations)
      throws IOException {
    assertTrue(locations.length == 1);
    BlockLocation blockLocation = locations[0];
    assertTrue(blockLocation.getOffset() == 0);
    assertTrue(blockLocation.getLength() == 1);
    assertTrue(blockLocation.getHosts().length == 1 + parityBlocks);
  }

  @Test(timeout=60000)
  public void testListECFilesSmallerThanOneStripe() throws Exception {
    int dataBlocksNum = 3;
    createFile("/ec/smallstripe", cellSize * dataBlocksNum);
    RemoteIterator<LocatedFileStatus> iter =
        cluster.getFileSystem().listFiles(new Path("/ec"), true);
    LocatedFileStatus fileStatus = iter.next();
    assertSmallerThanOneStripe(fileStatus.getBlockLocations(), dataBlocksNum);

    BlockLocation[] locations = cluster.getFileSystem().getFileBlockLocations(
        fileStatus, 0, fileStatus.getLen());
    assertSmallerThanOneStripe(locations, dataBlocksNum);

    //Test FileContext
    fileStatus = fileContext.listLocatedStatus(new Path("/ec")).next();
    assertSmallerThanOneStripe(fileStatus.getBlockLocations(), dataBlocksNum);
    locations = fileContext.getFileBlockLocations(new Path("/ec/smallstripe"),
        0, fileStatus.getLen());
    assertSmallerThanOneStripe(locations, dataBlocksNum);
  }

  private void assertSmallerThanOneStripe(BlockLocation[] locations,
      int dataBlocksNum) throws IOException {
    int expectedHostNum = dataBlocksNum + parityBlocks;
    assertTrue(locations.length == 1);
    BlockLocation blockLocation = locations[0];
    assertTrue(blockLocation.getHosts().length == expectedHostNum);
    assertTrue(blockLocation.getOffset() == 0);
    assertTrue(blockLocation.getLength() == dataBlocksNum * cellSize);
  }

  @Test(timeout=60000)
  public void testListECFilesMoreThanOneBlockGroup() throws Exception {
    createFile("/ec/group", blockGroupSize + 123);
    RemoteIterator<LocatedFileStatus> iter =
        cluster.getFileSystem().listFiles(new Path("/ec"), true);
    LocatedFileStatus fileStatus = iter.next();
    assertMoreThanOneBlockGroup(fileStatus.getBlockLocations(), 123);

    BlockLocation[] locations = cluster.getFileSystem().getFileBlockLocations(
        fileStatus, 0, fileStatus.getLen());
    assertMoreThanOneBlockGroup(locations, 123);

    //Test FileContext
    iter = fileContext.listLocatedStatus(new Path("/ec"));
    fileStatus = iter.next();
    assertMoreThanOneBlockGroup(fileStatus.getBlockLocations(), 123);
    locations = fileContext.getFileBlockLocations(new Path("/ec/group"),
        0, fileStatus.getLen());
    assertMoreThanOneBlockGroup(locations, 123);
  }

  private void assertMoreThanOneBlockGroup(BlockLocation[] locations,
      int lastBlockSize) throws IOException {
    assertTrue(locations.length == 2);
    BlockLocation fistBlockGroup = locations[0];
    assertTrue(fistBlockGroup.getHosts().length == numDNs);
    assertTrue(fistBlockGroup.getOffset() == 0);
    assertTrue(fistBlockGroup.getLength() == blockGroupSize);
    BlockLocation lastBlock = locations[1];
    assertTrue(lastBlock.getHosts().length == 1 + parityBlocks);
    assertTrue(lastBlock.getOffset() == blockGroupSize);
    assertTrue(lastBlock.getLength() == lastBlockSize);
  }
}
