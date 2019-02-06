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

import static org.junit.Assert.fail;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.protocol.QuotaExceededException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Test abandoning blocks, which clients do on pipeline creation failure.
 */
public class TestAbandonBlock {
  public static final Logger LOG =
      LoggerFactory.getLogger(TestAbandonBlock.class);
  
  private static final Configuration CONF = new HdfsConfiguration();
  static final String FILE_NAME_PREFIX
      = "/" + TestAbandonBlock.class.getSimpleName() + "_"; 
  private MiniDFSCluster cluster;
  private DistributedFileSystem fs;

  @Before
  public void setUp() throws Exception {
    cluster = new MiniDFSCluster.Builder(CONF).numDataNodes(2).build();
    fs = cluster.getFileSystem();
    cluster.waitActive();
  }

  @After
  public void tearDown() throws Exception {
    if (fs != null) {
      fs.close();
      fs = null;
    }
    if (cluster != null) {
      cluster.shutdown();
      cluster = null;
    }
  }

  @Test
  /** Abandon a block while creating a file */
  public void testAbandonBlock() throws IOException {
    String src = FILE_NAME_PREFIX + "foo";

    // Start writing a file but do not close it
    FSDataOutputStream fout = fs.create(new Path(src), true, 4096, (short)1, 512L);
    for (int i = 0; i < 1024; i++) {
      fout.write(123);
    }
    fout.hflush();
    long fileId = ((DFSOutputStream)fout.getWrappedStream()).getFileId();

    // Now abandon the last block
    DFSClient dfsclient = DFSClientAdapter.getDFSClient(fs);
    LocatedBlocks blocks =
      dfsclient.getNamenode().getBlockLocations(src, 0, Integer.MAX_VALUE);
    int orginalNumBlocks = blocks.locatedBlockCount();
    LocatedBlock b = blocks.getLastLocatedBlock();
    dfsclient.getNamenode().abandonBlock(b.getBlock(), fileId, src,
        dfsclient.clientName);
    
    // call abandonBlock again to make sure the operation is idempotent
    dfsclient.getNamenode().abandonBlock(b.getBlock(), fileId, src,
        dfsclient.clientName);

    // And close the file
    fout.close();

    // Close cluster and check the block has been abandoned after restart
    cluster.restartNameNode();
    blocks = dfsclient.getNamenode().getBlockLocations(src, 0,
        Integer.MAX_VALUE);
    Assert.assertEquals("Blocks " + b + " has not been abandoned.",
        orginalNumBlocks, blocks.locatedBlockCount() + 1);
  }

  @Test
  /** Make sure that the quota is decremented correctly when a block is abandoned */
  public void testQuotaUpdatedWhenBlockAbandoned() throws IOException {
    // Setting diskspace quota to 3MB
    fs.setQuota(new Path("/"), HdfsConstants.QUOTA_DONT_SET, 3 * 1024 * 1024);

    // Start writing a file with 2 replicas to ensure each datanode has one.
    // Block Size is 1MB.
    String src = FILE_NAME_PREFIX + "test_quota1";
    FSDataOutputStream fout = fs.create(new Path(src), true, 4096, (short)2, 1024 * 1024);
    for (int i = 0; i < 1024; i++) {
      fout.writeByte(123);
    }

    // Shutdown one datanode, causing the block abandonment.
    cluster.getDataNodes().get(0).shutdown();

    // Close the file, new block will be allocated with 2MB pending size.
    try {
      fout.close();
    } catch (QuotaExceededException e) {
      fail("Unexpected quota exception when closing fout");
    }
  }
}
