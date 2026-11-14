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

import org.junit.jupiter.api.Test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import static org.apache.hadoop.hdfs.client.HdfsClientConfigKeys.DFS_REPLICATION_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_REPLICATION_MAX_KEY;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Test that a client uses dfs.replication.CLUSTER for files it creates in
 * that cluster, and that the cluster accepts them when it has a lower
 * maximum replication than dfs.replication.
 */
public class TestPerClusterDefaultReplication {

  @Test
  public void testClusterReplicationUsedOnCreate() throws Exception {
    Configuration nnConf = new HdfsConfiguration();
    nnConf.setInt(DFS_REPLICATION_KEY, 1);
    nnConf.setInt(DFS_REPLICATION_MAX_KEY, 1);
    try (MiniDFSCluster cluster = new MiniDFSCluster.Builder(nnConf)
        .numDataNodes(1).build()) {
      cluster.waitActive();

      // the client config: 3 replicas by default, 1 for this cluster
      Configuration clientConf = new HdfsConfiguration(nnConf);
      clientConf.setInt(DFS_REPLICATION_KEY, 3);
      String host = cluster.getURI().getHost();
      clientConf.setInt(DFS_REPLICATION_KEY + "." + host, 1);

      try (FileSystem fs = FileSystem.newInstance(cluster.getURI(),
          clientConf)) {
        Path file = new Path("/test/file");
        assertEquals(1, fs.getDefaultReplication(file));
        // FileSystem.create() and the createFile() builder both use it;
        // without it the NameNode rejects a replication of 3.
        try (FSDataOutputStream out = fs.create(file, true)) {
          out.write(1);
        }
        assertEquals(1, fs.getFileStatus(file).getReplication());
        Path file2 = new Path("/test/file2");
        try (FSDataOutputStream out = fs.createFile(file2).recursive()
            .overwrite(true).build()) {
          out.write(1);
        }
        assertEquals(1, fs.getFileStatus(file2).getReplication());
      }
    }
  }
}
