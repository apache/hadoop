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

package org.apache.hadoop.yarn.server.resourcemanager.nodelabels;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.nodelabels.CommonNodeLabelsManager;
import org.junit.Assert;
import org.junit.Test;

public class TestNodeLabelFileReplication {

  @Test
  public void testNodeLabelFileReplication() throws IOException {
    int expectedReplication = 10;
    Configuration conf = new Configuration();
    conf.setBoolean(YarnConfiguration.NODE_LABELS_ENABLED, true);
    conf.setInt(YarnConfiguration.FS_STORE_FILE_REPLICATION,
        expectedReplication);
    MiniDFSCluster cluster = null;
    try {
      cluster = new MiniDFSCluster.Builder(new Configuration()).numDataNodes(1)
          .build();
      FileSystem fs = cluster.getFileSystem();
      String nodeLabelDir = fs.getUri().toString() + "/nodelabel";
      conf.set(YarnConfiguration.FS_NODE_LABELS_STORE_ROOT_DIR, nodeLabelDir);
      CommonNodeLabelsManager manager = new CommonNodeLabelsManager();
      manager.init(conf);
      manager.start();
      int fileReplication = fs
          .getFileStatus(new Path(nodeLabelDir, "nodelabel.mirror"))
          .getReplication();
      Assert.assertEquals(
          "Node label file replication should be " + expectedReplication,
          expectedReplication, fileReplication);
      manager.close();
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }
}
