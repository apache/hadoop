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
package org.apache.hadoop.hdfs.server.namenode.ha;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.XAttr;
import org.apache.hadoop.fs.XAttrSetFlag;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.HAUtil;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.MiniDFSNNTopology;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Tests interaction of XAttrs with HA failover.
 */
public class TestXAttrsWithHA {
  private static final Path path = new Path("/file");
  
  // XAttrs
  protected static final String name1 = "user.a1";
  protected static final byte[] value1 = {0x31, 0x32, 0x33};
  protected static final byte[] newValue1 = {0x31, 0x31, 0x31};
  protected static final String name2 = "user.a2";
  protected static final byte[] value2 = {0x37, 0x38, 0x39};
  protected static final String name3 = "user.a3";
  
  private MiniDFSCluster cluster;
  private NameNode nn0;
  private NameNode nn1;
  private FileSystem fs;

  @Before
  public void setupCluster() throws Exception {
    Configuration conf = new Configuration();
    conf.setInt(DFSConfigKeys.DFS_HA_TAILEDITS_PERIOD_KEY, 1);
    HAUtil.setAllowStandbyReads(conf, true);
    
    cluster = new MiniDFSCluster.Builder(conf)
      .nnTopology(MiniDFSNNTopology.simpleHATopology())
      .numDataNodes(1)
      .waitSafeMode(false)
      .build();
    cluster.waitActive();
    
    nn0 = cluster.getNameNode(0);
    nn1 = cluster.getNameNode(1);
    fs = HATestUtil.configureFailoverFs(cluster, conf);
    
    cluster.transitionToActive(0);
  }
  
  @After
  public void shutdownCluster() throws IOException {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  /**
   * Test that xattrs are properly tracked by the standby
   */
  @Test(timeout = 60000)
  public void testXAttrsTrackedOnStandby() throws Exception {
    fs.create(path).close();
    fs.setXAttr(path, name1, value1, EnumSet.of(XAttrSetFlag.CREATE));
    fs.setXAttr(path, name2, value2, EnumSet.of(XAttrSetFlag.CREATE));

    HATestUtil.waitForStandbyToCatchUp(nn0, nn1);
    List<XAttr> xAttrs = nn1.getRpcServer().getXAttrs("/file", null);
    assertEquals(2, xAttrs.size());
    cluster.shutdownNameNode(0);
    
    // Failover the current standby to active.
    cluster.shutdownNameNode(0);
    cluster.transitionToActive(1);
    
    Map<String, byte[]> xattrs = fs.getXAttrs(path);
    Assert.assertEquals(xattrs.size(), 2);
    Assert.assertArrayEquals(value1, xattrs.get(name1));
    Assert.assertArrayEquals(value2, xattrs.get(name2));
    
    fs.delete(path, true);
  }

}
