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
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.hdfs.protocol.ErasureCodingPolicy;
import org.apache.hadoop.hdfs.protocol.SystemErasureCodingPolicies;
import org.apache.hadoop.io.IOUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Testing correctness of FileSystem.getFileBlockLocations and
 * FileSystem.listFiles for erasure coded files.
 */
public class TestDistributedFileSystemWithECFile {
  private ErasureCodingPolicy ecPolicy;
  private int cellSize;
  private short dataBlocks;
  private short parityBlocks;
  private int numDNs;
  private int stripesPerBlock;
  private int blockSize;
  private int blockGroupSize;

  private MiniDFSCluster cluster;
  private FileContext fileContext;
  private DistributedFileSystem fs;
  private Configuration conf = new HdfsConfiguration();

  public ErasureCodingPolicy getEcPolicy() {
    return StripedFileTestUtil.getDefaultECPolicy();
  }

  @Rule
  public final Timeout globalTimeout = new Timeout(60000 * 3);

  @Before
  public void setup() throws IOException {
    ecPolicy = getEcPolicy();
    cellSize = ecPolicy.getCellSize();
    dataBlocks = (short) ecPolicy.getNumDataUnits();
    parityBlocks = (short) ecPolicy.getNumParityUnits();
    numDNs = dataBlocks + parityBlocks;
    stripesPerBlock = 4;
    blockSize = stripesPerBlock * cellSize;
    blockGroupSize = blockSize * dataBlocks;

    conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, blockSize);
    conf.setBoolean(DFSConfigKeys.DFS_NAMENODE_REDUNDANCY_CONSIDERLOAD_KEY,
        false);
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(numDNs).build();
    fileContext = FileContext.getFileContext(cluster.getURI(0), conf);
    fs = cluster.getFileSystem();
    fs.enableErasureCodingPolicy(ecPolicy.getName());
    fs.mkdirs(new Path("/ec"));
    cluster.getFileSystem().getClient().setErasureCodingPolicy("/ec",
        ecPolicy.getName());
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
    int dataBlocksNum = dataBlocks;
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

  @Test(timeout=60000)
  public void testReplayEditLogsForReplicatedFile() throws Exception {
    cluster.shutdown();

    ErasureCodingPolicy rs63 = SystemErasureCodingPolicies.getByID(
        SystemErasureCodingPolicies.RS_6_3_POLICY_ID
    );
    ErasureCodingPolicy rs32 = SystemErasureCodingPolicies.getByID(
        SystemErasureCodingPolicies.RS_3_2_POLICY_ID
    );
    // Test RS(6,3) as default policy
    int numDataNodes = rs63.getNumDataUnits() + rs63.getNumParityUnits();
    cluster = new MiniDFSCluster.Builder(conf)
        .nnTopology(MiniDFSNNTopology.simpleHATopology())
        .numDataNodes(numDataNodes)
        .build();

    cluster.transitionToActive(0);
    fs = cluster.getFileSystem(0);
    fs.enableErasureCodingPolicy(rs63.getName());
    fs.enableErasureCodingPolicy(rs32.getName());

    Path dir = new Path("/ec");
    fs.mkdirs(dir);
    fs.setErasureCodingPolicy(dir, rs63.getName());

    // Create an erasure coded file with the default policy.
    Path ecFile = new Path(dir, "ecFile");
    createFile(ecFile.toString(), 10);
    // Create a replicated file.
    Path replicatedFile = new Path(dir, "replicated");
    try (FSDataOutputStream out = fs.createFile(replicatedFile)
      .replicate().build()) {
      out.write(123);
    }
    // Create an EC file with a different policy.
    Path ecFile2 = new Path(dir, "RS-3-2");
    try (FSDataOutputStream out = fs.createFile(ecFile2)
         .ecPolicyName(rs32.getName()).build()) {
      out.write(456);
    }

    cluster.transitionToStandby(0);
    cluster.transitionToActive(1);

    fs = cluster.getFileSystem(1);
    assertNull(fs.getErasureCodingPolicy(replicatedFile));
    assertEquals(rs63, fs.getErasureCodingPolicy(ecFile));
    assertEquals(rs32, fs.getErasureCodingPolicy(ecFile2));
  }

  @SuppressWarnings("deprecation")
  @Test
  public void testStatistics() throws Exception {
    final String fileName = "/ec/file";
    final int size = 3200;
    createFile(fileName, size);
    InputStream in = null;
    try {
      in = fs.open(new Path(fileName));
      IOUtils.copyBytes(in, System.out, 4096, false);
    } finally {
      IOUtils.closeStream(in);
    }

    // verify stats are correct
    Long totalBytesRead = 0L;
    for (FileSystem.Statistics stat : FileSystem.getAllStatistics()) {
      totalBytesRead += stat.getBytesRead();
    }
    assertEquals(Long.valueOf(size), totalBytesRead);

    // verify thread local stats are correct
    Long totalBytesReadThread = 0L;
    for (FileSystem.Statistics stat : FileSystem.getAllStatistics()) {
      FileSystem.Statistics.StatisticsData data = stat.getThreadStatistics();
      totalBytesReadThread += data.getBytesRead();
    }
    assertEquals(Long.valueOf(size), totalBytesReadThread);
  }
}
