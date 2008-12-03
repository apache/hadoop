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

import java.io.File;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.MiniDFSCluster;

import junit.framework.TestCase;

/** Test if a datanode can handle disk error correctly*/
public class TestDiskError extends TestCase {
  public void testShutdown() throws Exception {
    // bring up a cluster of 3
    Configuration conf = new Configuration();
    conf.setLong("dfs.block.size", 512L);
    conf.setInt("dfs.dataXceiver.timeoutInMS", 1000);
    MiniDFSCluster cluster = new MiniDFSCluster(conf, 3, true, null);
    cluster.waitActive();
    FileSystem fs = cluster.getFileSystem();
    final int dnIndex = 0;
    String dataDir = cluster.getDataDirectory();
    File dir1 = new File(new File(dataDir, "data"+(2*dnIndex+1)), "tmp");
    File dir2 = new File(new File(dataDir, "data"+(2*dnIndex+2)), "tmp");
    try {
      // make the data directory of the first datanode to be readonly
      assertTrue(dir1.setReadOnly());
      assertTrue(dir2.setReadOnly());

      // create files and make sure that first datanode will be down
      DataNode dn = cluster.getDataNodes().get(dnIndex);
      for (int i=0; DataNode.isDatanodeUp(dn); i++) {
        Path fileName = new Path("/test.txt"+i);
        DFSTestUtil.createFile(fs, fileName, 1024, (short)2, 1L);
        DFSTestUtil.waitReplication(fs, fileName, (short)2);
      }
    } finally {
      // restore its old permission
      dir1.setWritable(true);
      dir2.setWritable(true);
      cluster.shutdown();
    }
  }
}
