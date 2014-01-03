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

import java.io.*;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsDatasetSpi;
import org.junit.Test;

import static org.hamcrest.core.IsNot.not;
import static org.junit.Assert.*;


/**
 * Test to verify that the DataNode Uuid is correctly initialized before
 * FsDataSet initialization.
 */
public class TestDataNodeInitStorage {
  public static final Log LOG = LogFactory.getLog(TestDataNodeInitStorage.class);

  static private class SimulatedFsDatasetVerifier extends SimulatedFSDataset {
    static class Factory extends FsDatasetSpi.Factory<SimulatedFSDataset> {
      @Override
      public SimulatedFsDatasetVerifier newInstance(
          DataNode datanode, DataStorage storage,
          Configuration conf) throws IOException {
        return new SimulatedFsDatasetVerifier(storage, conf);
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

    // This constructor does the actual verification by ensuring that
    // the DatanodeUuid is initialized.
    public SimulatedFsDatasetVerifier(DataStorage storage, Configuration conf) {
      super(storage, conf);
      LOG.info("Assigned DatanodeUuid is " + storage.getDatanodeUuid());
      assert(storage.getDatanodeUuid() != null);
      assert(storage.getDatanodeUuid().length() != 0);
    }
  }


  @Test (timeout = 60000)
  public void testDataNodeInitStorage() throws Throwable {
    // Create configuration to use SimulatedFsDatasetVerifier#Factory.
    Configuration conf = new HdfsConfiguration();
    SimulatedFsDatasetVerifier.setFactory(conf);

    // Start a cluster so that SimulatedFsDatasetVerifier constructor is
    // invoked.
    MiniDFSCluster cluster =
        new MiniDFSCluster.Builder(conf).numDataNodes(1).build();
    cluster.waitActive();
    cluster.shutdown();
  }
}
