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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.FileNotFoundException;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.protocol.BlockStoragePolicy;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockStoragePolicySuite;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestApplyingStoragePolicy {
  private static final short REPL = 1;
  private static final int SIZE = 128;

  private static Configuration conf;
  private static MiniDFSCluster cluster;
  private static DistributedFileSystem fs;

  @Before
  public void clusterSetUp() throws IOException {
    conf = new HdfsConfiguration();
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(REPL).build();
    cluster.waitActive();
    fs = cluster.getFileSystem();
  }

  @After
  public void clusterShutdown() throws IOException{
    if(fs != null) {
      fs.close();
      fs = null;
    }
    if(cluster != null) {
      cluster.shutdown();
      cluster = null;
    }
  }

  @Test
  public void testStoragePolicyByDefault() throws Exception {
    final Path foo = new Path("/foo");
    final Path bar = new Path(foo, "bar");
    final Path wow = new Path(bar, "wow");
    final Path fooz = new Path(bar, "/fooz");
    DFSTestUtil.createFile(fs, wow, SIZE, REPL, 0);

    final BlockStoragePolicySuite suite = BlockStoragePolicySuite
        .createDefaultSuite();
    final BlockStoragePolicy hot = suite.getPolicy("HOT");

    /*
     * test: storage policy is HOT by default or inherited from nearest
     * ancestor, if not explicitly specified for newly created dir/file.
     */
    assertEquals(fs.getStoragePolicy(foo), hot);
    assertEquals(fs.getStoragePolicy(bar), hot);
    assertEquals(fs.getStoragePolicy(wow), hot);
    try {
      fs.getStoragePolicy(fooz);
    } catch (Exception e) {
      assertTrue(e instanceof FileNotFoundException);
    }
  }

  @Test
  public void testSetAndUnsetStoragePolicy() throws Exception {
    final Path foo = new Path("/foo");
    final Path bar = new Path(foo, "bar");
    final Path wow = new Path(bar, "wow");
    final Path fooz = new Path(bar, "/fooz");
    DFSTestUtil.createFile(fs, wow, SIZE, REPL, 0);

    final BlockStoragePolicySuite suite = BlockStoragePolicySuite
        .createDefaultSuite();
    final BlockStoragePolicy warm = suite.getPolicy("WARM");
    final BlockStoragePolicy cold = suite.getPolicy("COLD");
    final BlockStoragePolicy hot = suite.getPolicy("HOT");

    /*
     * test: set storage policy
     */
    fs.setStoragePolicy(foo, warm.getName());
    fs.setStoragePolicy(bar, cold.getName());
    fs.setStoragePolicy(wow, hot.getName());
    try {
      fs.setStoragePolicy(fooz, warm.getName());
    } catch (Exception e) {
      assertTrue(e instanceof FileNotFoundException);
    }

    /*
     * test: get storage policy after set
     */
    assertEquals(fs.getStoragePolicy(foo), warm);
    assertEquals(fs.getStoragePolicy(bar), cold);
    assertEquals(fs.getStoragePolicy(wow), hot);
    try {
      fs.getStoragePolicy(fooz);
    } catch (Exception e) {
      assertTrue(e instanceof FileNotFoundException);
    }

    /*
     * test: unset storage policy
     */
    fs.unsetStoragePolicy(foo);
    fs.unsetStoragePolicy(bar);
    fs.unsetStoragePolicy(wow);
    try {
      fs.unsetStoragePolicy(fooz);
    } catch (Exception e) {
      assertTrue(e instanceof FileNotFoundException);
    }

    /*
     * test: get storage policy after unset
     */
    assertEquals(fs.getStoragePolicy(foo), hot);
    assertEquals(fs.getStoragePolicy(bar), hot);
    assertEquals(fs.getStoragePolicy(wow), hot);
    try {
      fs.getStoragePolicy(fooz);
    } catch (Exception e) {
      assertTrue(e instanceof FileNotFoundException);
    }
  }

  @Test
  public void testNestedStoragePolicy() throws Exception {
    final Path foo = new Path("/foo");
    final Path bar = new Path(foo, "bar");
    final Path wow = new Path(bar, "wow");
    final Path fooz = new Path("/foos");
    DFSTestUtil.createFile(fs, wow, SIZE, REPL, 0);

    final BlockStoragePolicySuite suite = BlockStoragePolicySuite
        .createDefaultSuite();
    final BlockStoragePolicy warm = suite.getPolicy("WARM");
    final BlockStoragePolicy cold = suite.getPolicy("COLD");
    final BlockStoragePolicy hot = suite.getPolicy("HOT");

    /*
     * test: set storage policy
     */
    fs.setStoragePolicy(foo, warm.getName());
    fs.setStoragePolicy(bar, cold.getName());
    fs.setStoragePolicy(wow, hot.getName());
    try {
      fs.setStoragePolicy(fooz, warm.getName());
    } catch (Exception e) {
      assertTrue(e instanceof FileNotFoundException);
    }

    /*
     * test: get storage policy after set
     */
    assertEquals(fs.getStoragePolicy(foo), warm);
    assertEquals(fs.getStoragePolicy(bar), cold);
    assertEquals(fs.getStoragePolicy(wow), hot);
    try {
      fs.getStoragePolicy(fooz);
    } catch (Exception e) {
      assertTrue(e instanceof FileNotFoundException);
    }

    /*
     * test: unset storage policy in the case of being nested
     */
    // unset wow
    fs.unsetStoragePolicy(wow);
    // inherit storage policy from wow's nearest ancestor
    assertEquals(fs.getStoragePolicy(wow), cold);
    // unset bar
    fs.unsetStoragePolicy(bar);
    // inherit storage policy from bar's nearest ancestor
    assertEquals(fs.getStoragePolicy(bar), warm);
    // unset foo
    fs.unsetStoragePolicy(foo);
    // default storage policy is applied, since no more available ancestors
    assertEquals(fs.getStoragePolicy(foo), hot);
    // unset fooz
    try {
      fs.unsetStoragePolicy(fooz);
    } catch (Exception e) {
      assertTrue(e instanceof FileNotFoundException);
    }

    /*
     * test: default storage policy is applied, since no explicit policies from
     * ancestors are available
     */
    assertEquals(fs.getStoragePolicy(foo), hot);
    assertEquals(fs.getStoragePolicy(bar), hot);
    assertEquals(fs.getStoragePolicy(wow), hot);
    try {
      fs.getStoragePolicy(fooz);
    } catch (Exception e) {
      assertTrue(e instanceof FileNotFoundException);
    }
  }

  @Test
  public void testSetAndGetStoragePolicy() throws IOException {
    final Path foo = new Path("/foo");
    final Path bar = new Path(foo, "bar");
    final Path fooz = new Path("/fooz");
    DFSTestUtil.createFile(fs, bar, SIZE, REPL, 0);

    final BlockStoragePolicySuite suite = BlockStoragePolicySuite
        .createDefaultSuite();
    final BlockStoragePolicy warm = suite.getPolicy("WARM");
    final BlockStoragePolicy cold = suite.getPolicy("COLD");
    final BlockStoragePolicy hot = suite.getPolicy("HOT");

    assertEquals(fs.getStoragePolicy(foo), hot);
    assertEquals(fs.getStoragePolicy(bar), hot);
    try {
      fs.getStoragePolicy(fooz);
    } catch (Exception e) {
      assertTrue(e instanceof FileNotFoundException);
    }

    /*
     * test: set storage policy
     */
    fs.setStoragePolicy(foo, warm.getName());
    fs.setStoragePolicy(bar, cold.getName());
    try {
      fs.setStoragePolicy(fooz, warm.getName());
    } catch (Exception e) {
      assertTrue(e instanceof FileNotFoundException);
    }

    /*
     * test: get storage policy after set
     */
    assertEquals(fs.getStoragePolicy(foo), warm);
    assertEquals(fs.getStoragePolicy(bar), cold);
    try {
      fs.getStoragePolicy(fooz);
    } catch (Exception e) {
      assertTrue(e instanceof FileNotFoundException);
    }
  }
}
