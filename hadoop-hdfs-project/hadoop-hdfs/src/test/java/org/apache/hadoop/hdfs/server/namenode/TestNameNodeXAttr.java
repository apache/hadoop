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

import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.junit.Assert;
import org.junit.Test;

/**
 * Tests NameNode interaction for all XAttr APIs.
 * This test suite covers restarting NN, saving new checkpoint, 
 * and also includes test of xattrs for symlinks. 
 */
public class TestNameNodeXAttr extends FSXAttrBaseTest {
  
  private static final Path linkParent = new Path("/symdir1");
  private static final Path targetParent = new Path("/symdir2");
  private static final Path link = new Path(linkParent, "link");
  private static final Path target = new Path(targetParent, "target");

  @Test(timeout = 120000)
  public void testXAttrSymlinks() throws Exception {
    fs.mkdirs(linkParent);
    fs.mkdirs(targetParent);
    DFSTestUtil.createFile(fs, target, 1024, (short)3, 0xBEEFl);
    fs.createSymlink(target, link, false);
    
    fs.setXAttr(target, name1, value1);
    fs.setXAttr(target, name2, value2);
    
    Map<String, byte[]> xattrs = fs.getXAttrs(link);
    Assert.assertEquals(xattrs.size(), 2);
    Assert.assertArrayEquals(value1, xattrs.get(name1));
    Assert.assertArrayEquals(value2, xattrs.get(name2));
    
    fs.setXAttr(link, name3, null);
    xattrs = fs.getXAttrs(target);
    Assert.assertEquals(xattrs.size(), 3);
    Assert.assertArrayEquals(value1, xattrs.get(name1));
    Assert.assertArrayEquals(value2, xattrs.get(name2));
    Assert.assertArrayEquals(new byte[0], xattrs.get(name3));
    
    fs.removeXAttr(link, name1);
    xattrs = fs.getXAttrs(target);
    Assert.assertEquals(xattrs.size(), 2);
    Assert.assertArrayEquals(value2, xattrs.get(name2));
    Assert.assertArrayEquals(new byte[0], xattrs.get(name3));
    
    fs.removeXAttr(target, name3);
    xattrs = fs.getXAttrs(link);
    Assert.assertEquals(xattrs.size(), 1);
    Assert.assertArrayEquals(value2, xattrs.get(name2));
    
    fs.delete(linkParent, true);
    fs.delete(targetParent, true);
  }
}
