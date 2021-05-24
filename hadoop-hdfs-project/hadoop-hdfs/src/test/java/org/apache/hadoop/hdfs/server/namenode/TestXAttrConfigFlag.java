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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.namenode.NameNodeAdapter;
import org.apache.hadoop.io.IOUtils;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

/**
 * Tests that the configuration flag that controls support for XAttrs is off
 * and causes all attempted operations related to XAttrs to fail.  The
 * NameNode can still load XAttrs from fsimage or edits.
 */
public class TestXAttrConfigFlag {
  private static final Path PATH = new Path("/path");

  private MiniDFSCluster cluster;
  private DistributedFileSystem fs;

  @Rule
  public ExpectedException exception = ExpectedException.none();

  @After
  public void shutdown() throws Exception {
    IOUtils.cleanupWithLogger(null, fs);
    if (cluster != null) {
      cluster.shutdown();
      cluster = null;
    }
  }

  @Test
  public void testSetXAttr() throws Exception {
    initCluster(true, false);
    fs.mkdirs(PATH);
    expectException();
    fs.setXAttr(PATH, "user.foo", null);
  }
  
  @Test
  public void testGetXAttrs() throws Exception {
    initCluster(true, false);
    fs.mkdirs(PATH);
    expectException();
    fs.getXAttrs(PATH);
  }
  
  @Test
  public void testRemoveXAttr() throws Exception {
    initCluster(true, false);
    fs.mkdirs(PATH);
    expectException();
    fs.removeXAttr(PATH, "user.foo");
  }

  @Test
  public void testEditLog() throws Exception {
    // With XAttrs enabled, set an XAttr.
    initCluster(true, true);
    fs.mkdirs(PATH);
    fs.setXAttr(PATH, "user.foo", null);

    // Restart with XAttrs disabled.  Expect successful restart.
    restart(false, false);
  }

  @Test
  public void testFsImage() throws Exception {
    // With XAttrs enabled, set an XAttr.
    initCluster(true, true);
    fs.mkdirs(PATH);
    fs.setXAttr(PATH, "user.foo", null);

    // Save a new checkpoint and restart with XAttrs still enabled.
    restart(true, true);

    // Restart with XAttrs disabled.  Expect successful restart.
    restart(false, false);
  }

  /**
   * We expect an IOException, and we want the exception text to state the
   * configuration key that controls XAttr support.
   */
  private void expectException() {
    exception.expect(IOException.class);
    exception.expectMessage(DFSConfigKeys.DFS_NAMENODE_XATTRS_ENABLED_KEY);
  }

  /**
   * Initialize the cluster, wait for it to become active, and get FileSystem.
   *
   * @param format if true, format the NameNode and DataNodes before starting up
   * @param xattrsEnabled if true, XAttr support is enabled
   * @throws Exception if any step fails
   */
  private void initCluster(boolean format, boolean xattrsEnabled)
      throws Exception {
    Configuration conf = new Configuration();
    // not explicitly setting to false, should be false by default
    conf.setBoolean(DFSConfigKeys.DFS_NAMENODE_XATTRS_ENABLED_KEY, xattrsEnabled);
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(1).format(format)
      .build();
    cluster.waitActive();
    fs = cluster.getFileSystem();
  }

  /**
   * Restart the cluster, optionally saving a new checkpoint.
   *
   * @param checkpoint boolean true to save a new checkpoint
   * @param xattrsEnabled if true, XAttr support is enabled
   * @throws Exception if restart fails
   */
  private void restart(boolean checkpoint, boolean xattrsEnabled)
      throws Exception {
    NameNode nameNode = cluster.getNameNode();
    if (checkpoint) {
      NameNodeAdapter.enterSafeMode(nameNode, false);
      NameNodeAdapter.saveNamespace(nameNode);
    }
    shutdown();
    initCluster(false, xattrsEnabled);
  }
}
