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
package org.apache.hadoop.hdfs.tools;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.BlockStoragePolicy;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockStoragePolicySuite;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Test StoragePolicyAdmin commands
 */
public class TestStoragePolicyCommands {
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
  public void testSetAndUnsetStoragePolicy() throws Exception {
    final Path foo = new Path("/foo");
    final Path bar = new Path(foo, "bar");
    final Path wow = new Path(bar, "wow");
    DFSTestUtil.createFile(fs, wow, SIZE, REPL, 0);

    /*
     * test: set storage policy
     */
    final StoragePolicyAdmin admin = new StoragePolicyAdmin(conf);
    DFSTestUtil.toolRun(admin, "-setStoragePolicy -path /foo -policy WARM", 0,
        "Set storage policy WARM on " + foo.toString());
    DFSTestUtil.toolRun(admin, "-setStoragePolicy -path /foo/bar -policy COLD",
        0, "Set storage policy COLD on " + bar.toString());
    DFSTestUtil.toolRun(admin, "-setStoragePolicy -path /foo/bar/wow -policy HOT",
        0, "Set storage policy HOT on " + wow.toString());
    DFSTestUtil.toolRun(admin, "-setStoragePolicy -path /fooz -policy WARM",
        2, "File/Directory does not exist: /fooz");

    /*
     * test: get storage policy after set
     */
    final BlockStoragePolicySuite suite = BlockStoragePolicySuite
        .createDefaultSuite();
    final BlockStoragePolicy warm = suite.getPolicy("WARM");
    final BlockStoragePolicy cold = suite.getPolicy("COLD");
    final BlockStoragePolicy hot = suite.getPolicy("HOT");
    DFSTestUtil.toolRun(admin, "-getStoragePolicy -path /foo", 0,
        "The storage policy of " + foo.toString() + ":\n" + warm);
    DFSTestUtil.toolRun(admin, "-getStoragePolicy -path /foo/bar", 0,
        "The storage policy of " + bar.toString() + ":\n" + cold);
    DFSTestUtil.toolRun(admin, "-getStoragePolicy -path /foo/bar/wow", 0,
        "The storage policy of " + wow.toString() + ":\n" + hot);
    DFSTestUtil.toolRun(admin, "-getStoragePolicy -path /fooz", 2,
        "File/Directory does not exist: /fooz");

    /*
     * test: unset storage policy
     */
    DFSTestUtil.toolRun(admin, "-unsetStoragePolicy -path /foo", 0,
        "Unset storage policy from " + foo.toString());
    DFSTestUtil.toolRun(admin, "-unsetStoragePolicy -path /foo/bar", 0,
        "Unset storage policy from " + bar.toString());
    DFSTestUtil.toolRun(admin, "-unsetStoragePolicy -path /foo/bar/wow", 0,
        "Unset storage policy from " + wow.toString());
    DFSTestUtil.toolRun(admin, "-unsetStoragePolicy -path /fooz", 2,
        "File/Directory does not exist: /fooz");

    /*
     * test: get storage policy after unset
     */
    DFSTestUtil.toolRun(admin, "-getStoragePolicy -path /foo", 0,
        "The storage policy of " + foo.toString() + " is unspecified");
    DFSTestUtil.toolRun(admin, "-getStoragePolicy -path /foo/bar", 0,
        "The storage policy of " + bar.toString() + " is unspecified");
    DFSTestUtil.toolRun(admin, "-getStoragePolicy -path /foo/bar/wow", 0,
        "The storage policy of " + wow.toString() + " is unspecified");
    DFSTestUtil.toolRun(admin, "-getStoragePolicy -path /fooz", 2,
        "File/Directory does not exist: /fooz");
  }

  @Test
  public void testSetAndGetStoragePolicy() throws Exception {
    final Path foo = new Path("/foo");
    final Path bar = new Path(foo, "bar");
    DFSTestUtil.createFile(fs, bar, SIZE, REPL, 0);

    final StoragePolicyAdmin admin = new StoragePolicyAdmin(conf);
    DFSTestUtil.toolRun(admin, "-getStoragePolicy -path /foo", 0,
        "The storage policy of " + foo.toString() + " is unspecified");
    DFSTestUtil.toolRun(admin, "-getStoragePolicy -path /foo/bar", 0,
        "The storage policy of " + bar.toString() + " is unspecified");

    DFSTestUtil.toolRun(admin, "-setStoragePolicy -path /foo -policy WARM", 0,
        "Set storage policy WARM on " + foo.toString());
    DFSTestUtil.toolRun(admin, "-setStoragePolicy -path /foo/bar -policy COLD",
        0, "Set storage policy COLD on " + bar.toString());
    DFSTestUtil.toolRun(admin, "-setStoragePolicy -path /fooz -policy WARM",
        2, "File/Directory does not exist: /fooz");

    final BlockStoragePolicySuite suite = BlockStoragePolicySuite
        .createDefaultSuite();
    final BlockStoragePolicy warm = suite.getPolicy("WARM");
    final BlockStoragePolicy cold = suite.getPolicy("COLD");
    DFSTestUtil.toolRun(admin, "-getStoragePolicy -path /foo", 0,
        "The storage policy of " + foo.toString() + ":\n" + warm);
    DFSTestUtil.toolRun(admin, "-getStoragePolicy -path /foo/bar", 0,
        "The storage policy of " + bar.toString() + ":\n" + cold);
    DFSTestUtil.toolRun(admin, "-getStoragePolicy -path /fooz", 2,
        "File/Directory does not exist: /fooz");
  }
}
