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

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.server.datanode.*;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsDatasetSpi;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.Test;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.*;


/**
 * Test to verify that the DFSClient passes the expected block length to
 * the DataNode via DataTransferProtocol.
 */
public class TestWriteBlockGetsBlockLengthHint {
  static final long DEFAULT_BLOCK_LENGTH = 1024;
  static final long EXPECTED_BLOCK_LENGTH = DEFAULT_BLOCK_LENGTH * 2;

  @Test
  public void blockLengthHintIsPropagated() throws IOException {
    final String METHOD_NAME = GenericTestUtils.getMethodName();
    final Path path = new Path("/" + METHOD_NAME + ".dat");

    Configuration conf = new HdfsConfiguration();
    FsDatasetChecker.setFactory(conf);
    conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, DEFAULT_BLOCK_LENGTH);
    conf.setInt(DFSConfigKeys.DFS_DATANODE_SCAN_PERIOD_HOURS_KEY, -1);
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).numDataNodes(1).build();

    try {
      cluster.waitActive();

      // FsDatasetChecker#createRbw asserts during block creation if the test
      // fails.
      DFSTestUtil.createFile(
          cluster.getFileSystem(),
          path,
          4096,  // Buffer size.
          EXPECTED_BLOCK_LENGTH,
          EXPECTED_BLOCK_LENGTH,
          (short) 1,
          0x1BAD5EED);
    } finally {
      cluster.shutdown();
    }
  }

  static class FsDatasetChecker extends SimulatedFSDataset {
    static class Factory extends FsDatasetSpi.Factory<SimulatedFSDataset> {
      @Override
      public SimulatedFSDataset newInstance(DataNode datanode,
          DataStorage storage, Configuration conf) throws IOException {
        return new FsDatasetChecker(storage, conf);
      }

      @Override
      public boolean isSimulated() {
        return true;
      }
    }

    public static void setFactory(Configuration conf) {
      conf.set(DFSConfigKeys.DFS_DATANODE_FSDATASET_FACTORY_KEY,
               Factory.class.getName());
    }

    public FsDatasetChecker(DataStorage storage, Configuration conf) {
      super(storage, conf);
    }

    /**
     * Override createRbw to verify that the block length that is passed
     * is correct. This requires both DFSOutputStream and BlockReceiver to
     * correctly propagate the hint to FsDatasetSpi.
     */
    @Override
    public synchronized ReplicaInPipelineInterface createRbw(
        StorageType storageType, ExtendedBlock b) throws IOException {
      assertThat(b.getLocalBlock().getNumBytes(), is(EXPECTED_BLOCK_LENGTH));
      return super.createRbw(storageType, b);
    }
  }
}
