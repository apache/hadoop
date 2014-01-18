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
  public void testDFSAdminRollingUpgradeCommands() throws Exception {
    // start a cluster 
    final Configuration conf = new HdfsConfiguration();
    MiniDFSCluster cluster = null;
    try {
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(0).build();
      cluster.waitActive();

      final Path foo = new Path("/foo");
      final Path bar = new Path("/bar");
      final Path baz = new Path("/baz");

      {
        final DistributedFileSystem dfs = cluster.getFileSystem();
        final DFSAdmin dfsadmin = new DFSAdmin(conf);
        dfs.mkdirs(foo);

        {
          //illegal argument
          final String[] args = {"-rollingUpgrade", "abc"};
          Assert.assertTrue(dfsadmin.run(args) != 0);
        }

        {
          //query rolling upgrade
          final String[] args = {"-rollingUpgrade"};
          Assert.assertEquals(0, dfsadmin.run(args));
        }

        {
          //start rolling upgrade
          final String[] args = {"-rollingUpgrade", "start"};
          Assert.assertEquals(0, dfsadmin.run(args));
        }

        {
          //query rolling upgrade
          final String[] args = {"-rollingUpgrade", "query"};
          Assert.assertEquals(0, dfsadmin.run(args));
        }

        dfs.mkdirs(bar);
        
        {
          //finalize rolling upgrade
          final String[] args = {"-rollingUpgrade", "finalize"};
          Assert.assertEquals(0, dfsadmin.run(args));
        }

        dfs.mkdirs(baz);

        {
          //query rolling upgrade
          final String[] args = {"-rollingUpgrade"};
          Assert.assertEquals(0, dfsadmin.run(args));
        }

        Assert.assertTrue(dfs.exists(foo));
        Assert.assertTrue(dfs.exists(bar));
        Assert.assertTrue(dfs.exists(baz));
      }

      cluster.restartNameNode();
      {
        final DistributedFileSystem dfs = cluster.getFileSystem();
        Assert.assertTrue(dfs.exists(foo));
        Assert.assertTrue(dfs.exists(bar));
        Assert.assertTrue(dfs.exists(baz));
      }
    } finally {
      if(cluster != null) cluster.shutdown();
    }
  }
}
