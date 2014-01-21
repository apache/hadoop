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
package org.apache.hadoop.hdfs.server.namenode;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.RollingUpgradeException;
import org.junit.Assert;
import org.junit.Test;


/**
 * This class tests the edit log upgrade marker.
 */
public class TestEditLogUpgradeMarker {
  private static final Log LOG = LogFactory.getLog(TestEditLogUpgradeMarker.class);

  /**
   * Test edit log upgrade marker.
   */
  @Test
  public void testUpgradeMarker() throws IOException {
    // start a cluster 
    final Configuration conf = new HdfsConfiguration();
    MiniDFSCluster cluster = null;
    try {
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(0).build();
      cluster.waitActive();

      final FSNamesystem namesystem = cluster.getNamesystem();
      final Path foo = new Path("/foo");
      final Path bar = new Path("/bar");

      {
        final DistributedFileSystem dfs = cluster.getFileSystem();
        dfs.mkdirs(foo);
  
        //add marker
        namesystem.startRollingUpgrade();
  
        dfs.mkdirs(bar);
        
        Assert.assertTrue(dfs.exists(foo));
        Assert.assertTrue(dfs.exists(bar));
      }
      
      try {
        cluster.restartNameNode();
        Assert.fail();
      } catch(RollingUpgradeException e) {
        LOG.info("The exception is expected: ", e);
      }

      cluster.restartNameNode("-rollingUpgrade", "rollback");
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
