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
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.tools.DFSAdmin;
import org.junit.Assert;
import org.junit.Test;


/**
 * This class tests rolling upgrade.
 */
public class TestRollingUpgrade {
  /**
   * Test DFSAdmin Upgrade Command.
   */
  @Test
  public void testDFSAdminRollingUpgradeCommand() throws Exception {
    // start a cluster 
    final Configuration conf = new HdfsConfiguration();
    MiniDFSCluster cluster = null;
    try {
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(0).build();
      cluster.waitActive();

      final Path foo = new Path("/foo");
      final Path bar = new Path("/bar");

      {
        final DistributedFileSystem dfs = cluster.getFileSystem();
        final DFSAdmin dfsadmin = new DFSAdmin(conf);
        dfs.mkdirs(foo);

        {
          //start rolling upgrade
          final String[] args = {"-rollingUpgrade", "start"};
          dfsadmin.run(args);
        }

        dfs.mkdirs(bar);
        
        Assert.assertTrue(dfs.exists(foo));
        Assert.assertTrue(dfs.exists(bar));
      }
      
      cluster.restartNameNode();
      {
        final DistributedFileSystem dfs = cluster.getFileSystem();
        Assert.assertTrue(dfs.exists(foo));
        Assert.assertFalse(dfs.exists(bar));
      }
    } finally {
      if(cluster != null) cluster.shutdown();
    }
  }
}
