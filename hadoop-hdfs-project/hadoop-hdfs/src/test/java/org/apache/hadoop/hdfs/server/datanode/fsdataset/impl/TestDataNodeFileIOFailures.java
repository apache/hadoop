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

package org.apache.hadoop.hdfs.server.datanode.fsdataset.impl;

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_ENABLED_OPS_FILEIO_FAULT_INJECTION_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_ENABLE_FILEIO_FAULT_INJECTION_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_FILEIO_FAULT_PERCENTAGE_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_SYNCONCLOSE_KEY;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.EnumSet;

import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.StreamCapabilities;
import org.apache.hadoop.fs.Syncable;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.server.datanode.FileIoProvider;
import org.junit.jupiter.api.Test;

public class TestDataNodeFileIOFailures {

  @Test
  public void testFileHSyncFailure() throws Exception {

    HdfsConfiguration config = new HdfsConfiguration();
    config.setBoolean(DFS_DATANODE_SYNCONCLOSE_KEY, false);
    config.setBoolean(DFS_DATANODE_ENABLE_FILEIO_FAULT_INJECTION_KEY, true);
    config.set(DFS_DATANODE_ENABLED_OPS_FILEIO_FAULT_INJECTION_KEY,
        FileIoProvider.OPERATION.SYNC.name());
    // Fail 100% of the time
    config.set(DFS_DATANODE_FILEIO_FAULT_PERCENTAGE_KEY, "100");

    MiniDFSCluster cluster = new MiniDFSCluster.Builder(config)
        .numDataNodes(1).build();

    try {
      cluster.waitActive();
      FileSystem fs = cluster.getFileSystem();
      Path path = new Path("/testFileHSyncFailure");
      FSDataOutputStream os = fs.create(path);
      assertTrue(os.hasCapability(StreamCapabilities.HSYNC));
      assertTrue(os instanceof Syncable);
      os.writeUTF("test");
      IOException ioe = assertThrows(IOException.class, () -> os.hsync());
      assertTrue(ioe.getMessage().startsWith("All datanodes ") &&
          ioe.getMessage().endsWith(" are bad. Aborting..."));
      IOException ioe2 = assertThrows(IOException.class, () -> os.close());
      assertTrue(ioe2.getMessage().startsWith("All datanodes ") &&
          ioe2.getMessage().endsWith(" are bad. Aborting..."));
    } finally {
      cluster.shutdown();
    }
  }

  @Test
  public void testFileSyncCreateFlagFailure() throws Exception {

    HdfsConfiguration config = new HdfsConfiguration();
    config.setBoolean(DFS_DATANODE_SYNCONCLOSE_KEY, false);
    config.setBoolean(DFS_DATANODE_ENABLE_FILEIO_FAULT_INJECTION_KEY, true);
    config.set(DFS_DATANODE_ENABLED_OPS_FILEIO_FAULT_INJECTION_KEY,
        FileIoProvider.OPERATION.SYNC.name());
    // Fail 100% of the time
    config.set(DFS_DATANODE_FILEIO_FAULT_PERCENTAGE_KEY, "100");

    MiniDFSCluster cluster = new MiniDFSCluster.Builder(config)
        .numDataNodes(1).build();

    try {
      cluster.waitActive();
      FileSystem fs = cluster.getFileSystem();
      Path path = new Path("/testFileSyncCreateFlagFailure");
      EnumSet<CreateFlag> flags = EnumSet.of(CreateFlag.SYNC_BLOCK, CreateFlag.CREATE);
      IOException ioe = assertThrows(IOException.class, () -> {
        try (FSDataOutputStream os = fs.create(path, FsPermission.getDefault(), flags,
            1024, (short) 1, 8192, null)) {
          os.writeUTF("test");
        }
      });
      assertTrue(ioe.getMessage().startsWith("All datanodes ") &&
          ioe.getMessage().endsWith(" are bad. Aborting..."));
    } finally {
      cluster.shutdown();
    }
  }

  @Test
  public void testFileSyncOnCloseFailure() throws Exception {

    HdfsConfiguration config = new HdfsConfiguration();
    config.setBoolean(DFS_DATANODE_SYNCONCLOSE_KEY, true);
    config.setBoolean(DFS_DATANODE_ENABLE_FILEIO_FAULT_INJECTION_KEY, true);
    config.set(DFS_DATANODE_ENABLED_OPS_FILEIO_FAULT_INJECTION_KEY,
        FileIoProvider.OPERATION.SYNC.name());
    // Fail 100% of the time
    config.set(DFS_DATANODE_FILEIO_FAULT_PERCENTAGE_KEY, "100");

    MiniDFSCluster cluster = new MiniDFSCluster.Builder(config)
        .numDataNodes(1).build();

    try {
      cluster.waitActive();
      FileSystem fs = cluster.getFileSystem();
      Path path = new Path("/testFileSyncOnCloseFailure");
      IOException ioe = assertThrows(IOException.class, () -> {
        try (FSDataOutputStream os = fs.create(path)) {
          os.writeUTF("test");
        }
      });
      assertTrue(ioe.getMessage().startsWith("All datanodes ") &&
          ioe.getMessage().endsWith(" are bad. Aborting..."));
    } finally {
      cluster.shutdown();
    }
  }
}
