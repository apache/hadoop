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
package org.apache.hadoop.hdfs.tools;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsDatasetSpi;
import org.apache.hadoop.io.IOUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.PrintStream;

import static org.apache.hadoop.hdfs.server.datanode.fsdataset.impl.FsDatasetTestUtil.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestDebugAdmin {

  static private final String TEST_ROOT_DIR =
      new File(System.getProperty("test.build.data", "/tmp"),
          TestDebugAdmin.class.getSimpleName()).getAbsolutePath();

  private MiniDFSCluster cluster;
  private DistributedFileSystem fs;
  private DebugAdmin admin;
  private DataNode datanode;

  @Before
  public void setUp() throws Exception {
    final File testRoot = new File(TEST_ROOT_DIR);
    testRoot.delete();
    testRoot.mkdirs();
    Configuration conf = new Configuration();
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(1).build();
    cluster.waitActive();
    fs = cluster.getFileSystem();
    admin = new DebugAdmin(conf);
    datanode = cluster.getDataNodes().get(0);
  }

  @After
  public void tearDown() throws Exception {
    if (cluster != null) {
      cluster.shutdown();
      cluster = null;
    }
  }

  private String runCmd(String[] cmd) throws Exception {
    final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    final PrintStream out = new PrintStream(bytes);
    final PrintStream oldErr = System.err;
    final PrintStream oldOut = System.out;
    System.setErr(out);
    System.setOut(out);
    int ret;
    try {
      ret = admin.run(cmd);
    } finally {
      System.setErr(oldErr);
      System.setOut(oldOut);
      IOUtils.closeStream(out);
    }
    return "ret: " + ret + ", " +
        bytes.toString().replaceAll(System.lineSeparator(), "");
  }

  @Test(timeout = 60000)
  public void testRecoverLease() throws Exception {
    assertEquals("ret: 1, You must supply a -path argument to recoverLease.",
        runCmd(new String[]{"recoverLease", "-retries", "1"}));
    FSDataOutputStream out = fs.create(new Path("/foo"));
    out.write(123);
    out.close();
    assertEquals("ret: 0, recoverLease SUCCEEDED on /foo",
        runCmd(new String[]{"recoverLease", "-path", "/foo"}));
  }

  @Test(timeout = 60000)
  public void testVerifyMetaCommand() throws Exception {
    DFSTestUtil.createFile(fs, new Path("/bar"), 1234, (short) 1, 0xdeadbeef);
    FsDatasetSpi<?> fsd = datanode.getFSDataset();
    ExtendedBlock block = DFSTestUtil.getFirstBlock(fs, new Path("/bar"));
    File blockFile = getBlockFile(fsd,
        block.getBlockPoolId(), block.getLocalBlock());
    assertEquals("ret: 1, You must specify a meta file with -meta", runCmd(
        new String[] {"verifyMeta", "-block", blockFile.getAbsolutePath()}));
    File metaFile = getMetaFile(fsd,
        block.getBlockPoolId(), block.getLocalBlock());
    assertEquals("ret: 0, Checksum type: " +
          "DataChecksum(type=CRC32C, chunkSize=512)",
        runCmd(new String[]{"verifyMeta",
            "-meta", metaFile.getAbsolutePath()}));
    assertEquals("ret: 0, Checksum type: " +
          "DataChecksum(type=CRC32C, chunkSize=512)" +
          "Checksum verification succeeded on block file " +
          blockFile.getAbsolutePath(),
        runCmd(new String[]{"verifyMeta",
            "-meta", metaFile.getAbsolutePath(),
            "-block", blockFile.getAbsolutePath()})
    );
  }

  @Test(timeout = 60000)
  public void testComputeMetaCommand() throws Exception {
    DFSTestUtil.createFile(fs, new Path("/bar"), 1234, (short) 1, 0xdeadbeef);
    FsDatasetSpi<?> fsd = datanode.getFSDataset();
    ExtendedBlock block = DFSTestUtil.getFirstBlock(fs, new Path("/bar"));
    File blockFile = getBlockFile(fsd,
        block.getBlockPoolId(), block.getLocalBlock());

    assertEquals("ret: 1, computeMeta -block <block-file> -out "
            + "<output-metadata-file>  Compute HDFS metadata from the specified"
            + " block file, and save it to  the specified output metadata file."
            + "**NOTE: Use at your own risk! If the block file is corrupt"
            + " and you overwrite it's meta file,  it will show up"
            + " as good in HDFS, but you can't read the data."
            + " Only use as a last measure, and when you are 100% certain"
            + " the block file is good.",
        runCmd(new String[] {"computeMeta"}));
    assertEquals("ret: 2, You must specify a block file with -block",
        runCmd(new String[] {"computeMeta", "-whatever"}));
    assertEquals("ret: 3, Block file <bla> does not exist or is not a file",
        runCmd(new String[] {"computeMeta", "-block", "bla"}));
    assertEquals("ret: 4, You must specify a output file with -out", runCmd(
        new String[] {"computeMeta", "-block", blockFile.getAbsolutePath()}));
    assertEquals("ret: 5, output file already exists!", runCmd(
        new String[] {"computeMeta", "-block", blockFile.getAbsolutePath(),
            "-out", blockFile.getAbsolutePath()}));

    File outFile = new File(TEST_ROOT_DIR, "out.meta");
    outFile.delete();
    assertEquals("ret: 0, Checksum calculation succeeded on block file " +
        blockFile.getAbsolutePath() + " saved metadata to meta file " +
        outFile.getAbsolutePath(), runCmd(new String[] {"computeMeta", "-block",
        blockFile.getAbsolutePath(), "-out", outFile.getAbsolutePath()}));

    assertTrue(outFile.exists());
    assertTrue(outFile.length() > 0);
  }

  @Test(timeout = 60000)
  public void testRecoverLeaseforFileNotFound() throws Exception {
    assertTrue(runCmd(new String[] {
        "recoverLease", "-path", "/foo", "-retries", "2" }).contains(
        "Giving up on recoverLease for /foo after 1 try"));
  }
}
