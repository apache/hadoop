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
import java.util.EnumSet;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.XAttrSetFlag;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.SafeModeAction;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * 1) save xattrs, restart NN, assert xattrs reloaded from edit log, 
 * 2) save xattrs, create new checkpoint, restart NN, assert xattrs 
 * reloaded from fsimage
 */
public class TestFSImageWithXAttr {
  private static Configuration conf;
  private static MiniDFSCluster cluster;
  
  //xattrs
  private static final String name1 = "user.a1";
  private static final byte[] value1 = {0x31, 0x32, 0x33};
  private static final byte[] newValue1 = {0x31, 0x31, 0x31};
  private static final String name2 = "user.a2";
  private static final byte[] value2 = {0x37, 0x38, 0x39};
  private static final String name3 = "user.a3";
  private static final byte[] value3 = {};

  @BeforeClass
  public static void setUp() throws IOException {
    conf = new Configuration();
    conf.setBoolean(DFSConfigKeys.DFS_NAMENODE_XATTRS_ENABLED_KEY, true);
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(1).build();
    cluster.waitActive();
  }

  @AfterClass
  public static void tearDown() {
    cluster.shutdown();
  }

  private void testXAttr(boolean persistNamespace) throws IOException {
    Path path = new Path("/p");
    DistributedFileSystem fs = cluster.getFileSystem();
    fs.create(path).close();
    
    fs.setXAttr(path, name1, value1, EnumSet.of(XAttrSetFlag.CREATE));
    fs.setXAttr(path, name2, value2, EnumSet.of(XAttrSetFlag.CREATE));
    fs.setXAttr(path, name3, null, EnumSet.of(XAttrSetFlag.CREATE));
    
    restart(fs, persistNamespace);
    
    Map<String, byte[]> xattrs = fs.getXAttrs(path);
    Assert.assertEquals(xattrs.size(), 3);
    Assert.assertArrayEquals(value1, xattrs.get(name1));
    Assert.assertArrayEquals(value2, xattrs.get(name2));
    Assert.assertArrayEquals(value3, xattrs.get(name3));
    
    fs.setXAttr(path, name1, newValue1, EnumSet.of(XAttrSetFlag.REPLACE));
    
    restart(fs, persistNamespace);
    
    xattrs = fs.getXAttrs(path);
    Assert.assertEquals(xattrs.size(), 3);
    Assert.assertArrayEquals(newValue1, xattrs.get(name1));
    Assert.assertArrayEquals(value2, xattrs.get(name2));
    Assert.assertArrayEquals(value3, xattrs.get(name3));

    fs.removeXAttr(path, name1);
    fs.removeXAttr(path, name2);
    fs.removeXAttr(path, name3);

    restart(fs, persistNamespace);
    xattrs = fs.getXAttrs(path);
    Assert.assertEquals(xattrs.size(), 0);
  }

  @Test
  public void testPersistXAttr() throws IOException {
    testXAttr(true);
  }

  @Test
  public void testXAttrEditLog() throws IOException {
    testXAttr(false);
  }

  /**
   * Restart the NameNode, optionally saving a new checkpoint.
   *
   * @param fs DistributedFileSystem used for saving namespace
   * @param persistNamespace boolean true to save a new checkpoint
   * @throws IOException if restart fails
   */
  private void restart(DistributedFileSystem fs, boolean persistNamespace)
      throws IOException {
    if (persistNamespace) {
      fs.setSafeMode(SafeModeAction.SAFEMODE_ENTER);
      fs.saveNamespace();
      fs.setSafeMode(SafeModeAction.SAFEMODE_LEAVE);
    }

    cluster.restartNameNode();
    cluster.waitActive();
  }

}
