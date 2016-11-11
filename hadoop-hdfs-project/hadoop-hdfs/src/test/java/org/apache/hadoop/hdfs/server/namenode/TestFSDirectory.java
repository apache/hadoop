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


import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.StringReader;
import java.util.EnumSet;
import java.util.List;
import java.util.Random;

import com.google.common.collect.ImmutableList;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ParentNotDirectoryException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.XAttr;
import org.apache.hadoop.fs.XAttrSetFlag;
import org.apache.hadoop.hdfs.protocol.NSQuotaExceededException;
import org.apache.hadoop.hdfs.server.namenode.FSDirectory.DirOp;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Lists;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Test {@link FSDirectory}, the in-memory namespace tree.
 */
public class TestFSDirectory {
  public static final Log LOG = LogFactory.getLog(TestFSDirectory.class);

  private static final long seed = 0;
  private static final short REPLICATION = 3;

  private final Path dir = new Path("/" + getClass().getSimpleName());
  
  private final Path sub1 = new Path(dir, "sub1");
  private final Path file1 = new Path(sub1, "file1");
  private final Path file2 = new Path(sub1, "file2");

  private final Path sub11 = new Path(sub1, "sub11");
  private final Path file3 = new Path(sub11, "file3");
  private final Path file5 = new Path(sub1, "z_file5");

  private final Path sub2 = new Path(dir, "sub2");
  private final Path file6 = new Path(sub2, "file6");

  private Configuration conf;
  private MiniDFSCluster cluster;
  private FSNamesystem fsn;
  private FSDirectory fsdir;

  private DistributedFileSystem hdfs;

  private static final int numGeneratedXAttrs = 256;
  private static final ImmutableList<XAttr> generatedXAttrs =
      ImmutableList.copyOf(generateXAttrs(numGeneratedXAttrs));

  @Before
  public void setUp() throws Exception {
    conf = new Configuration();
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_MAX_XATTRS_PER_INODE_KEY, 2);
    cluster = new MiniDFSCluster.Builder(conf)
      .numDataNodes(REPLICATION)
      .build();
    cluster.waitActive();
    
    fsn = cluster.getNamesystem();
    fsdir = fsn.getFSDirectory();
    
    hdfs = cluster.getFileSystem();
    DFSTestUtil.createFile(hdfs, file1, 1024, REPLICATION, seed);
    DFSTestUtil.createFile(hdfs, file2, 1024, REPLICATION, seed);
    DFSTestUtil.createFile(hdfs, file3, 1024, REPLICATION, seed);

    DFSTestUtil.createFile(hdfs, file5, 1024, REPLICATION, seed);
    hdfs.mkdirs(sub2);
  }

  @After
  public void tearDown() throws Exception {
    if (cluster != null) {
      cluster.shutdown();
      cluster = null;
    }
  }

  /** Dump the tree, make some changes, and then dump the tree again. */
  @Test
  public void testDumpTree() throws Exception {
    final INode root = fsdir.getINode("/");

    LOG.info("Original tree");
    final StringBuffer b1 = root.dumpTreeRecursively();
    System.out.println("b1=" + b1);

    final BufferedReader in = new BufferedReader(new StringReader(b1.toString()));
    
    String line = in.readLine();
    checkClassName(line);

    for(; (line = in.readLine()) != null; ) {
      line = line.trim();
      if (!line.isEmpty() && !line.contains("snapshot")) {
        assertTrue("line=" + line,
            line.startsWith(INodeDirectory.DUMPTREE_LAST_ITEM)
                || line.startsWith(INodeDirectory.DUMPTREE_EXCEPT_LAST_ITEM)
        );
        checkClassName(line);
      }
    }
  }
  
  @Test
  public void testSkipQuotaCheck() throws Exception {
    try {
      // set quota. nsQuota of 1 means no files can be created
      //  under this directory.
      hdfs.setQuota(sub2, 1, Long.MAX_VALUE);

      // create a file
      try {
        // this should fail
        DFSTestUtil.createFile(hdfs, file6, 1024, REPLICATION, seed);
        throw new IOException("The create should have failed.");
      } catch (NSQuotaExceededException qe) {
        // ignored
      }
      // disable the quota check and retry. this should succeed.
      fsdir.disableQuotaChecks();
      DFSTestUtil.createFile(hdfs, file6, 1024, REPLICATION, seed);

      // trying again after re-enabling the check.
      hdfs.delete(file6, false); // cleanup
      fsdir.enableQuotaChecks();
      try {
        // this should fail
        DFSTestUtil.createFile(hdfs, file6, 1024, REPLICATION, seed);
        throw new IOException("The create should have failed.");
      } catch (NSQuotaExceededException qe) {
        // ignored
      }
    } finally {
      hdfs.delete(file6, false); // cleanup, in case the test failed in the middle.
      hdfs.setQuota(sub2, Long.MAX_VALUE, Long.MAX_VALUE);
    }
  }
  
  static void checkClassName(String line) {
    int i = line.lastIndexOf('(');
    int j = line.lastIndexOf('@');
    final String classname = line.substring(i+1, j);
    assertTrue(classname.startsWith(INodeFile.class.getSimpleName())
        || classname.startsWith(INodeDirectory.class.getSimpleName()));
  }
  
  @Test
  public void testINodeXAttrsLimit() throws Exception {
    List<XAttr> existingXAttrs = Lists.newArrayListWithCapacity(2);
    XAttr xAttr1 = (new XAttr.Builder()).setNameSpace(XAttr.NameSpace.USER).
        setName("a1").setValue(new byte[]{0x31, 0x32, 0x33}).build();
    XAttr xAttr2 = (new XAttr.Builder()).setNameSpace(XAttr.NameSpace.USER).
        setName("a2").setValue(new byte[]{0x31, 0x31, 0x31}).build();
    existingXAttrs.add(xAttr1);
    existingXAttrs.add(xAttr2);
    
    // Adding system and raw namespace xAttrs aren't affected by inode
    // xAttrs limit.
    XAttr newSystemXAttr = (new XAttr.Builder()).
        setNameSpace(XAttr.NameSpace.SYSTEM).setName("a3").
        setValue(new byte[]{0x33, 0x33, 0x33}).build();
    XAttr newRawXAttr = (new XAttr.Builder()).setNameSpace(XAttr.NameSpace.RAW).
        setName("a3").setValue(new byte[]{0x33, 0x33, 0x33}).build();
    List<XAttr> newXAttrs = Lists.newArrayListWithCapacity(2);
    newXAttrs.add(newSystemXAttr);
    newXAttrs.add(newRawXAttr);
    List<XAttr> xAttrs = FSDirXAttrOp.setINodeXAttrs(fsdir, existingXAttrs,
                                                     newXAttrs, EnumSet.of(
            XAttrSetFlag.CREATE, XAttrSetFlag.REPLACE));
    assertEquals(xAttrs.size(), 4);
    
    // Adding a trusted namespace xAttr, is affected by inode xAttrs limit.
    XAttr newXAttr1 = (new XAttr.Builder()).setNameSpace(
        XAttr.NameSpace.TRUSTED).setName("a4").
        setValue(new byte[]{0x34, 0x34, 0x34}).build();
    newXAttrs.set(0, newXAttr1);
    try {
      FSDirXAttrOp.setINodeXAttrs(fsdir, existingXAttrs, newXAttrs,
                                  EnumSet.of(XAttrSetFlag.CREATE,
                                             XAttrSetFlag.REPLACE));
      fail("Setting user visible xattr on inode should fail if " +
          "reaching limit.");
    } catch (IOException e) {
      GenericTestUtils.assertExceptionContains("Cannot add additional XAttr " +
          "to inode, would exceed limit", e);
    }
  }

  /**
   * Verify that the first <i>num</i> generatedXAttrs are present in
   * newXAttrs.
   */
  private static void verifyXAttrsPresent(List<XAttr> newXAttrs,
      final int num) {
    assertEquals("Unexpected number of XAttrs after multiset", num,
        newXAttrs.size());
    for (int i=0; i<num; i++) {
      XAttr search = generatedXAttrs.get(i);
      assertTrue("Did not find set XAttr " + search + " + after multiset",
          newXAttrs.contains(search));
    }
  }

  private static List<XAttr> generateXAttrs(final int numXAttrs) {
    List<XAttr> generatedXAttrs = Lists.newArrayListWithCapacity(numXAttrs);
    for (int i=0; i<numXAttrs; i++) {
      XAttr xAttr = (new XAttr.Builder())
          .setNameSpace(XAttr.NameSpace.SYSTEM)
          .setName("a" + i)
          .setValue(new byte[] { (byte) i, (byte) (i + 1), (byte) (i + 2) })
          .build();
      generatedXAttrs.add(xAttr);
    }
    return generatedXAttrs;
  }

  /**
   * Test setting and removing multiple xattrs via single operations
   */
  @Test(timeout=300000)
  public void testXAttrMultiSetRemove() throws Exception {
    List<XAttr> existingXAttrs = Lists.newArrayListWithCapacity(0);

    // Keep adding a random number of xattrs and verifying until exhausted
    final Random rand = new Random(0xFEEDA);
    int numExpectedXAttrs = 0;
    while (numExpectedXAttrs < numGeneratedXAttrs) {
      LOG.info("Currently have " + numExpectedXAttrs + " xattrs");
      final int numToAdd = rand.nextInt(5)+1;

      List<XAttr> toAdd = Lists.newArrayListWithCapacity(numToAdd);
      for (int i = 0; i < numToAdd; i++) {
        if (numExpectedXAttrs >= numGeneratedXAttrs) {
          break;
        }
        toAdd.add(generatedXAttrs.get(numExpectedXAttrs));
        numExpectedXAttrs++;
      }
      LOG.info("Attempting to add " + toAdd.size() + " XAttrs");
      for (int i = 0; i < toAdd.size(); i++) {
        LOG.info("Will add XAttr " + toAdd.get(i));
      }
      List<XAttr> newXAttrs = FSDirXAttrOp.setINodeXAttrs(fsdir, existingXAttrs,
                                                          toAdd, EnumSet.of(
              XAttrSetFlag.CREATE));
      verifyXAttrsPresent(newXAttrs, numExpectedXAttrs);
      existingXAttrs = newXAttrs;
    }

    // Keep removing a random number of xattrs and verifying until all gone
    while (numExpectedXAttrs > 0) {
      LOG.info("Currently have " + numExpectedXAttrs + " xattrs");
      final int numToRemove = rand.nextInt(5)+1;
      List<XAttr> toRemove = Lists.newArrayListWithCapacity(numToRemove);
      for (int i = 0; i < numToRemove; i++) {
        if (numExpectedXAttrs == 0) {
          break;
        }
        toRemove.add(generatedXAttrs.get(numExpectedXAttrs-1));
        numExpectedXAttrs--;
      }
      final int expectedNumToRemove = toRemove.size();
      LOG.info("Attempting to remove " + expectedNumToRemove + " XAttrs");
      List<XAttr> removedXAttrs = Lists.newArrayList();
      List<XAttr> newXAttrs = FSDirXAttrOp.filterINodeXAttrs(existingXAttrs,
                                                             toRemove,
                                                             removedXAttrs);
      assertEquals("Unexpected number of removed XAttrs",
          expectedNumToRemove, removedXAttrs.size());
      verifyXAttrsPresent(newXAttrs, numExpectedXAttrs);
      existingXAttrs = newXAttrs;
    }
  }

  @Test(timeout=300000)
  public void testXAttrMultiAddRemoveErrors() throws Exception {

    // Test that the same XAttr can not be multiset twice
    List<XAttr> existingXAttrs = Lists.newArrayList();
    List<XAttr> toAdd = Lists.newArrayList();
    toAdd.add(generatedXAttrs.get(0));
    toAdd.add(generatedXAttrs.get(1));
    toAdd.add(generatedXAttrs.get(2));
    toAdd.add(generatedXAttrs.get(0));
    try {
      FSDirXAttrOp.setINodeXAttrs(fsdir, existingXAttrs, toAdd,
                                  EnumSet.of(XAttrSetFlag.CREATE));
      fail("Specified the same xattr to be set twice");
    } catch (IOException e) {
      GenericTestUtils.assertExceptionContains("Cannot specify the same " +
          "XAttr to be set", e);
    }

    // Test that CREATE and REPLACE flags are obeyed
    toAdd.remove(generatedXAttrs.get(0));
    existingXAttrs.add(generatedXAttrs.get(0));
    try {
      FSDirXAttrOp.setINodeXAttrs(fsdir, existingXAttrs, toAdd,
                                  EnumSet.of(XAttrSetFlag.CREATE));
      fail("Set XAttr that is already set without REPLACE flag");
    } catch (IOException e) {
      GenericTestUtils.assertExceptionContains("already exists", e);
    }
    try {
      FSDirXAttrOp.setINodeXAttrs(fsdir, existingXAttrs, toAdd,
                                  EnumSet.of(XAttrSetFlag.REPLACE));
      fail("Set XAttr that does not exist without the CREATE flag");
    } catch (IOException e) {
      GenericTestUtils.assertExceptionContains("does not exist", e);
    }

    // Sanity test for CREATE
    toAdd.remove(generatedXAttrs.get(0));
    List<XAttr> newXAttrs = FSDirXAttrOp.setINodeXAttrs(fsdir, existingXAttrs,
                                                        toAdd, EnumSet.of(
            XAttrSetFlag.CREATE));
    assertEquals("Unexpected toAdd size", 2, toAdd.size());
    for (XAttr x : toAdd) {
      assertTrue("Did not find added XAttr " + x, newXAttrs.contains(x));
    }
    existingXAttrs = newXAttrs;

    // Sanity test for REPLACE
    toAdd = Lists.newArrayList();
    for (int i=0; i<3; i++) {
      XAttr xAttr = (new XAttr.Builder())
          .setNameSpace(XAttr.NameSpace.SYSTEM)
          .setName("a" + i)
          .setValue(new byte[] { (byte) (i*2) })
          .build();
      toAdd.add(xAttr);
    }
    newXAttrs = FSDirXAttrOp.setINodeXAttrs(fsdir, existingXAttrs, toAdd,
                                            EnumSet.of(XAttrSetFlag.REPLACE));
    assertEquals("Unexpected number of new XAttrs", 3, newXAttrs.size());
    for (int i=0; i<3; i++) {
      assertArrayEquals("Unexpected XAttr value",
          new byte[] {(byte)(i*2)}, newXAttrs.get(i).getValue());
    }
    existingXAttrs = newXAttrs;

    // Sanity test for CREATE+REPLACE
    toAdd = Lists.newArrayList();
    for (int i=0; i<4; i++) {
      toAdd.add(generatedXAttrs.get(i));
    }
    newXAttrs = FSDirXAttrOp.setINodeXAttrs(fsdir, existingXAttrs, toAdd,
                                            EnumSet.of(XAttrSetFlag.CREATE,
                                                       XAttrSetFlag.REPLACE));
    verifyXAttrsPresent(newXAttrs, 4);
  }

  @Test
  public void testVerifyParentDir() throws Exception {
    hdfs.mkdirs(new Path("/dir1/dir2"));
    hdfs.createNewFile(new Path("/dir1/file"));
    hdfs.createNewFile(new Path("/dir1/dir2/file"));

    INodesInPath iip = fsdir.resolvePath(null, "/", DirOp.READ);
    fsdir.verifyParentDir(iip);

    iip = fsdir.resolvePath(null, "/dir1", DirOp.READ);
    fsdir.verifyParentDir(iip);

    iip = fsdir.resolvePath(null, "/dir1/file", DirOp.READ);
    fsdir.verifyParentDir(iip);

    iip = fsdir.resolvePath(null, "/dir-nonexist/file", DirOp.READ);
    try {
      fsdir.verifyParentDir(iip);
      fail("expected FNF");
    } catch (FileNotFoundException fnf) {
      // expected.
    }

    iip = fsdir.resolvePath(null, "/dir1/dir2", DirOp.READ);
    fsdir.verifyParentDir(iip);

    iip = fsdir.resolvePath(null, "/dir1/dir2/file", DirOp.READ);
    fsdir.verifyParentDir(iip);

    iip = fsdir.resolvePath(null, "/dir1/dir-nonexist/file", DirOp.READ);
    try {
      fsdir.verifyParentDir(iip);
      fail("expected FNF");
    } catch (FileNotFoundException fnf) {
      // expected.
    }

    try {
      iip = fsdir.resolvePath(null, "/dir1/file/fail", DirOp.READ);
      fail("expected ACE");
    } catch (AccessControlException ace) {
      assertTrue(ace.getMessage().contains("is not a directory"));
    }
    try {
      iip = fsdir.resolvePath(null, "/dir1/file/fail", DirOp.WRITE);
      fail("expected ACE");
    } catch (AccessControlException ace) {
      assertTrue(ace.getMessage().contains("is not a directory"));
    }
    try {
      iip = fsdir.resolvePath(null, "/dir1/file/fail", DirOp.CREATE);
      fail("expected PNDE");
    } catch (ParentNotDirectoryException pnde) {
      assertTrue(pnde.getMessage().contains("is not a directory"));
    }
  }
}
