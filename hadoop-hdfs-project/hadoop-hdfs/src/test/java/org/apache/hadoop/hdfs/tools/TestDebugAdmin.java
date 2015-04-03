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

public class TestDebugAdmin {
  private MiniDFSCluster cluster;
  private DistributedFileSystem fs;
  private DebugAdmin admin;
  private DataNode datanode;

  @Before
  public void setUp() throws Exception {
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
        bytes.toString().replaceAll(System.getProperty("line.separator"), "");
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
  public void testVerifyBlockChecksumCommand() throws Exception {
    DFSTestUtil.createFile(fs, new Path("/bar"), 1234, (short) 1, 0xdeadbeef);
    FsDatasetSpi<?> fsd = datanode.getFSDataset();
    ExtendedBlock block = DFSTestUtil.getFirstBlock(fs, new Path("/bar"));
    File blockFile = getBlockFile(fsd,
        block.getBlockPoolId(), block.getLocalBlock());
    assertEquals("ret: 1, You must specify a meta file with -meta",
        runCmd(new String[]{"verify", "-block", blockFile.getAbsolutePath()}));
    File metaFile = getMetaFile(fsd,
        block.getBlockPoolId(), block.getLocalBlock());
    assertEquals("ret: 0, Checksum type: " +
          "DataChecksum(type=CRC32C, chunkSize=512)",
        runCmd(new String[]{"verify",
            "-meta", metaFile.getAbsolutePath()}));
    assertEquals("ret: 0, Checksum type: " +
          "DataChecksum(type=CRC32C, chunkSize=512)" +
          "Checksum verification succeeded on block file " +
          blockFile.getAbsolutePath(),
        runCmd(new String[]{"verify",
            "-meta", metaFile.getAbsolutePath(),
            "-block", blockFile.getAbsolutePath()})
    );
  }
}
