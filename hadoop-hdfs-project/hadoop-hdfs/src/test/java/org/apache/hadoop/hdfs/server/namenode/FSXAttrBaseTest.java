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
import java.util.List;
import java.util.Map;

import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.XAttrSetFlag;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.io.IOUtils;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Lists;

/**
 * Tests NameNode interaction for all XAttr APIs.
 */
public class FSXAttrBaseTest {
  
  protected static MiniDFSCluster dfsCluster;
  protected static Configuration conf;
  private static int pathCount = 0;
  private static Path path;
  
  //xattrs
  protected static final String name1 = "user.a1";
  protected static final byte[] value1 = {0x31, 0x32, 0x33};
  protected static final byte[] newValue1 = {0x31, 0x31, 0x31};
  protected static final String name2 = "user.a2";
  protected static final byte[] value2 = {0x37, 0x38, 0x39};
  protected static final String name3 = "user.a3";
  protected static final String name4 = "user.a4";

  protected FileSystem fs;

  @AfterClass
  public static void shutdown() {
    if (dfsCluster != null) {
      dfsCluster.shutdown();
    }
  }

  @Before
  public void setUp() throws Exception {
    pathCount += 1;
    path = new Path("/p" + pathCount);
    initFileSystem();
  }

  @After
  public void destroyFileSystems() {
    IOUtils.cleanup(null, fs);
    fs = null;
  }
  
  /**
   * Tests for creating xattr
   * 1. create xattr using XAttrSetFlag.CREATE flag.
   * 2. Assert exception of creating xattr which already exists.
   * 3. Create multiple xattrs
   */
  @Test
  public void testCreateXAttr() throws Exception {
    FileSystem.mkdirs(fs, path, FsPermission.createImmutable((short)0750));
    fs.setXAttr(path, name1, value1, EnumSet.of(XAttrSetFlag.CREATE));
    
    Map<String, byte[]> xattrs = fs.getXAttrs(path);
    Assert.assertEquals(xattrs.size(), 1);
    Assert.assertArrayEquals(value1, xattrs.get(name1));
    
    fs.removeXAttr(path, name1);
    
    xattrs = fs.getXAttrs(path);
    Assert.assertEquals(xattrs.size(), 0);
    
    //create xattr which already exists.
    fs.setXAttr(path, name1, value1, EnumSet.of(XAttrSetFlag.CREATE));
    try {
      fs.setXAttr(path, name1, value1, EnumSet.of(XAttrSetFlag.CREATE));
      Assert.fail("Creating xattr which already exists should fail.");
    } catch (IOException e) {
    }
    fs.removeXAttr(path, name1);
    
    //create two xattrs
    fs.setXAttr(path, name1, value1, EnumSet.of(XAttrSetFlag.CREATE));
    fs.setXAttr(path, name2, null, EnumSet.of(XAttrSetFlag.CREATE));
    xattrs = fs.getXAttrs(path);
    Assert.assertEquals(xattrs.size(), 2);
    Assert.assertArrayEquals(value1, xattrs.get(name1));
    Assert.assertArrayEquals(new byte[0], xattrs.get(name2));
    
    fs.removeXAttr(path, name1);
    fs.removeXAttr(path, name2);
  }
  
  /**
   * Tests for replacing xattr
   * 1. Replace xattr using XAttrSetFlag.REPLACE flag.
   * 2. Assert exception of replacing xattr which does not exist.
   * 3. Create multiple xattrs, and replace some.
   */
  @Test
  public void testReplaceXAttr() throws Exception {
    FileSystem.mkdirs(fs, path, FsPermission.createImmutable((short)0750));
    fs.setXAttr(path, name1, value1, EnumSet.of(XAttrSetFlag.CREATE));
    fs.setXAttr(path, name1, newValue1, EnumSet.of(XAttrSetFlag.REPLACE));
    
    Map<String, byte[]> xattrs = fs.getXAttrs(path);
    Assert.assertEquals(xattrs.size(), 1);
    Assert.assertArrayEquals(newValue1, xattrs.get(name1));
    
    fs.removeXAttr(path, name1);
    
    //replace xattr which does not exist.
    try {
      fs.setXAttr(path, name1, value1, EnumSet.of(XAttrSetFlag.REPLACE));
      Assert.fail("Replacing xattr which does not exist should fail.");
    } catch (IOException e) {
    }
    
    //create two xattrs, then replace one
    fs.setXAttr(path, name1, value1, EnumSet.of(XAttrSetFlag.CREATE));
    fs.setXAttr(path, name2, value2, EnumSet.of(XAttrSetFlag.CREATE));
    fs.setXAttr(path, name2, null, EnumSet.of(XAttrSetFlag.REPLACE));
    xattrs = fs.getXAttrs(path);
    Assert.assertEquals(xattrs.size(), 2);
    Assert.assertArrayEquals(value1, xattrs.get(name1));
    Assert.assertArrayEquals(new byte[0], xattrs.get(name2));
    
    fs.removeXAttr(path, name1);
    fs.removeXAttr(path, name2);
  }
  
  /**
   * Tests for setting xattr
   * 1. Set xattr with XAttrSetFlag.CREATE|XAttrSetFlag.REPLACE flag.
   * 2. Set xattr with illegal name
   * 3. Set xattr without XAttrSetFlag.
   * 4. Set xattr and total number exceeds max limit
   */
  @Test
  public void testSetXAttr() throws Exception {
    FileSystem.mkdirs(fs, path, FsPermission.createImmutable((short)0750));
    fs.setXAttr(path, name1, value1, EnumSet.of(XAttrSetFlag.CREATE, 
        XAttrSetFlag.REPLACE));
        
    Map<String, byte[]> xattrs = fs.getXAttrs(path);
    Assert.assertEquals(xattrs.size(), 1);
    Assert.assertArrayEquals(value1, xattrs.get(name1));
    fs.removeXAttr(path, name1);
    
    //set xattr with null name
    try {
      fs.setXAttr(path, null, value1, EnumSet.of(XAttrSetFlag.CREATE, 
          XAttrSetFlag.REPLACE));
      Assert.fail("Setting xattr with null name should fail.");
    } catch (NullPointerException e) {
    }
    
    //set xattr with empty name: "user."
    try {
      fs.setXAttr(path, "user.", value1, EnumSet.of(XAttrSetFlag.CREATE, 
          XAttrSetFlag.REPLACE));
      Assert.fail("Setting xattr with empty name should fail.");
    } catch (HadoopIllegalArgumentException e) {
    }
    
    //set xattr with invalid name: "a1"
    try {
      fs.setXAttr(path, "a1", value1, EnumSet.of(XAttrSetFlag.CREATE, 
          XAttrSetFlag.REPLACE));
      Assert.fail("Setting xattr with invalid name prefix or without " +
          "name prefix should fail.");
    } catch (HadoopIllegalArgumentException e) {
    }
    
    //set xattr without XAttrSetFlag
    fs.setXAttr(path, name1, value1);
    xattrs = fs.getXAttrs(path);
    Assert.assertEquals(xattrs.size(), 1);
    Assert.assertArrayEquals(value1, xattrs.get(name1));
    fs.removeXAttr(path, name1);
    
    //xattr exists, and replace it using CREATE|REPLACE flag.
    fs.setXAttr(path, name1, value1, EnumSet.of(XAttrSetFlag.CREATE));
    fs.setXAttr(path, name1, newValue1, EnumSet.of(XAttrSetFlag.CREATE, 
        XAttrSetFlag.REPLACE));
    
    xattrs = fs.getXAttrs(path);
    Assert.assertEquals(xattrs.size(), 1);
    Assert.assertArrayEquals(newValue1, xattrs.get(name1));
    
    fs.removeXAttr(path, name1);
    
    //Total number exceeds max limit
    fs.setXAttr(path, name1, value1);
    fs.setXAttr(path, name2, value2);
    fs.setXAttr(path, name3, null);
    try {
      fs.setXAttr(path, name4, null);
      Assert.fail("Setting xattr should fail if total number of xattrs " +
          "for inode exceeds max limit.");
    } catch (IOException e) {
    }
    fs.removeXAttr(path, name1);
    fs.removeXAttr(path, name2);
    fs.removeXAttr(path, name3);
  }
  
  /**
   * Tests for getting xattr
   * 1. To get xattr which does not exist.
   * 2. To get multiple xattrs.
   */
  @Test
  public void testGetXAttrs() throws Exception {
    FileSystem.mkdirs(fs, path, FsPermission.createImmutable((short)0750));
    fs.setXAttr(path, name1, value1, EnumSet.of(XAttrSetFlag.CREATE));
    fs.setXAttr(path, name2, value2, EnumSet.of(XAttrSetFlag.CREATE));
    
    //xattr does not exist.
    byte[] value = fs.getXAttr(path, name3);
    Assert.assertEquals(value, null);
    
    List<String> names = Lists.newArrayList();
    names.add(name1);
    names.add(name2);
    names.add(name3);
    Map<String, byte[]> xattrs = fs.getXAttrs(path, names);
    Assert.assertEquals(xattrs.size(), 2);
    Assert.assertArrayEquals(value1, xattrs.get(name1));
    Assert.assertArrayEquals(value2, xattrs.get(name2));
    
    fs.removeXAttr(path, name1);
    fs.removeXAttr(path, name2);
  }
  
  /**
   * Tests for removing xattr
   * 1. Remove xattr
   */
  @Test
  public void testRemoveXAttr() throws Exception {
    FileSystem.mkdirs(fs, path, FsPermission.createImmutable((short)0750));
    fs.setXAttr(path, name1, value1, EnumSet.of(XAttrSetFlag.CREATE));
    fs.setXAttr(path, name2, value2, EnumSet.of(XAttrSetFlag.CREATE));
    fs.setXAttr(path, name3, null, EnumSet.of(XAttrSetFlag.CREATE));
    
    fs.removeXAttr(path, name1);
    fs.removeXAttr(path, name2);
    
    Map<String, byte[]> xattrs = fs.getXAttrs(path);
    Assert.assertEquals(xattrs.size(), 1);
    Assert.assertArrayEquals(new byte[0], xattrs.get(name3));
    
    fs.removeXAttr(path, name3);
  }
  
  /**
   * Creates a FileSystem for the super-user.
   *
   * @return FileSystem for super-user
   * @throws Exception if creation fails
   */
  protected FileSystem createFileSystem() throws Exception {
    return dfsCluster.getFileSystem();
  }
  
  /**
   * Initializes all FileSystem instances used in the tests.
   *
   * @throws Exception if initialization fails
   */
  private void initFileSystem() throws Exception {
    fs = createFileSystem();
  }
  
  /**
   * Initialize the cluster, wait for it to become active, and get FileSystem
   * instances for our test users.
   *
   * @param format if true, format the NameNode and DataNodes before starting up
   * @throws Exception if any step fails
   */
  protected static void initCluster(boolean format) throws Exception {
    dfsCluster = new MiniDFSCluster.Builder(conf).numDataNodes(1).format(format)
      .build();
    dfsCluster.waitActive();
  }
}
