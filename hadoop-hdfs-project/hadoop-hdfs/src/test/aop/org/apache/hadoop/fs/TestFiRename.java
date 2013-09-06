/*
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
package org.apache.hadoop.fs;

import java.io.File;
import java.io.IOException;
import java.util.EnumSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Options.Rename;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.test.PathUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static org.apache.hadoop.fs.FileContextTestHelper.*;

/**
 * Rename names src to dst. Rename is done using following steps:
 * <ul>
 * <li>Checks are made to ensure src exists and appropriate flags are being
 * passed to overwrite existing destination.
 * <li>src is removed.
 * <li>dst if it exists is removed.
 * <li>src is renamed and added to directory tree as dst.
 * </ul>
 * 
 * During any of the above steps, the state of src and dst is reverted back to
 * what it was prior to rename. This test ensures that the state is reverted
 * back.
 * 
 * This test uses AspectJ to simulate failures.
 */
public class TestFiRename {
  private static final Log LOG = LogFactory.getLog(TestFiRename.class);
  private static String removeChild = "";
  private static String addChild = "";
  private static byte[] data = { 0 };
  
  private static String TEST_ROOT_DIR = PathUtils.getTestDirName(TestFiRename.class);
  
  private static Configuration CONF = new Configuration();
  static {
    CONF.setInt("io.bytes.per.checksum", 1);
  }

  private MiniDFSCluster cluster = null;
  private FileContext fc = null;

  @Before
  public void setup() throws IOException {
    restartCluster(true);
  }

  @After
  public void teardown() throws IOException {
    if (fc != null) {
      fc.delete(getTestRootPath(), true);
    }
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  private void restartCluster(boolean format) throws IOException {
    if (cluster != null) {
      cluster.shutdown();
      cluster = null;
    }
    cluster = new MiniDFSCluster.Builder(CONF).format(format).build();
    cluster.waitClusterUp();
    fc = FileContext.getFileContext(cluster.getURI(0), CONF);
  }

  /**
   * Returns true to indicate an exception should be thrown to simulate failure
   * during removal of a node from directory tree.
   */
  public static boolean throwExceptionOnRemove(String child) {
    boolean status = removeChild.endsWith(child);
    if (status) {
      removeChild = "";
    }
    return status;
  }

  /**
   * Returns true to indicate an exception should be thrown to simulate failure
   * during addition of a node to directory tree.
   */
  public static boolean throwExceptionOnAdd(String child) {
    boolean status = addChild.endsWith(child);
    if (status) {
      addChild = "";
    }
    return status;
  }

  /** Set child name on removal of which failure should be simulated */
  public static void exceptionOnRemove(String child) {
    removeChild = child;
    addChild = "";
  }

  /** Set child name on addition of which failure should be simulated */
  public static void exceptionOnAdd(String child) {
    removeChild = "";
    addChild = child;
  }

  private Path getTestRootPath() {
    return fc.makeQualified(new Path(TEST_ROOT_DIR));
  }

  private Path getTestPath(String pathString) {
    return fc.makeQualified(new Path(TEST_ROOT_DIR, pathString));
  }

  private void createFile(Path path) throws IOException {
    FSDataOutputStream out = fc.create(path, EnumSet.of(CreateFlag.CREATE),
        Options.CreateOpts.createParent());
    out.write(data, 0, data.length);
    out.close();
  }

  /** Rename test when src exists and dst does not */
  @Test
  public void testFailureNonExistentDst() throws Exception {
    final Path src = getTestPath("testFailureNonExistenSrc/dir/src");
    final Path dst = getTestPath("testFailureNonExistenSrc/newdir/dst");
    createFile(src);

    // During rename, while removing src, an exception is thrown
    TestFiRename.exceptionOnRemove(src.toString());
    rename(src, dst, true, true, false, Rename.NONE);

    // During rename, while adding dst an exception is thrown
    TestFiRename.exceptionOnAdd(dst.toString());
    rename(src, dst, true, true, false, Rename.NONE);
  }

  /** Rename test when src and dst exist */
  @Test
  public void testFailuresExistingDst() throws Exception {
    final Path src = getTestPath("testFailuresExistingDst/dir/src");
    final Path dst = getTestPath("testFailuresExistingDst/newdir/dst");
    createFile(src);
    createFile(dst);

    // During rename, while removing src, an exception is thrown
    TestFiRename.exceptionOnRemove(src.toString());
    rename(src, dst, true, true, true, Rename.OVERWRITE);

    // During rename, while removing dst, an exception is thrown
    TestFiRename.exceptionOnRemove(dst.toString());
    rename(src, dst, true, true, true, Rename.OVERWRITE);

    // During rename, while adding dst an exception is thrown
    TestFiRename.exceptionOnAdd(dst.toString());
    rename(src, dst, true, true, true, Rename.OVERWRITE);
  }

  /** Rename test where both src and dst are files */
  @Test
  public void testDeletionOfDstFile() throws Exception {
    Path src = getTestPath("testDeletionOfDstFile/dir/src");
    Path dst = getTestPath("testDeletionOfDstFile/newdir/dst");
    createFile(src);
    createFile(dst);

    final FSNamesystem namesystem = cluster.getNamesystem();
    final long blocks = namesystem.getBlocksTotal();
    final long fileCount = namesystem.getFilesTotal();
    rename(src, dst, false, false, true, Rename.OVERWRITE);

    // After successful rename the blocks corresponing dst are deleted
    Assert.assertEquals(blocks - 1, namesystem.getBlocksTotal());

    // After successful rename dst file is deleted
    Assert.assertEquals(fileCount - 1, namesystem.getFilesTotal());

    // Restart the cluster to ensure new rename operation 
    // recorded in editlog is processed right
    restartCluster(false);
    int count = 0;
    boolean exception = true;
    src = getTestPath("testDeletionOfDstFile/dir/src");
    dst = getTestPath("testDeletionOfDstFile/newdir/dst");
    while (exception && count < 5) {
      try {
        exists(fc, src);
        exception = false;
      } catch (Exception e) {
        LOG.warn("Exception " + " count " + count + " " + e.getMessage());
        Thread.sleep(1000);
        count++;
      }
    }
    Assert.assertFalse(exists(fc, src));
    Assert.assertTrue(exists(fc, dst));
  }

  /** Rename test where both src and dst are directories */
  @Test
  public void testDeletionOfDstDirectory() throws Exception {
    Path src = getTestPath("testDeletionOfDstDirectory/dir/src");
    Path dst = getTestPath("testDeletionOfDstDirectory/newdir/dst");
    fc.mkdir(src, FileContext.DEFAULT_PERM, true);
    fc.mkdir(dst, FileContext.DEFAULT_PERM, true);

    FSNamesystem namesystem = cluster.getNamesystem();
    long fileCount = namesystem.getFilesTotal();
    rename(src, dst, false, false, true, Rename.OVERWRITE);

    // After successful rename dst directory is deleted
    Assert.assertEquals(fileCount - 1, namesystem.getFilesTotal());
    
    // Restart the cluster to ensure new rename operation 
    // recorded in editlog is processed right
    restartCluster(false);
    src = getTestPath("testDeletionOfDstDirectory/dir/src");
    dst = getTestPath("testDeletionOfDstDirectory/newdir/dst");
    int count = 0;
    boolean exception = true;
    while (exception && count < 5) {
      try {
        exists(fc, src);
        exception = false;
      } catch (Exception e) {
        LOG.warn("Exception " + " count " + count + " " + e.getMessage());
        Thread.sleep(1000);
        count++;
      }
    }
    Assert.assertFalse(exists(fc, src));
    Assert.assertTrue(exists(fc, dst));
  }

  private void rename(Path src, Path dst, boolean exception, boolean srcExists,
      boolean dstExists, Rename... options) throws IOException {
    try {
      fc.rename(src, dst, options);
      Assert.assertFalse("Expected exception is not thrown", exception);
    } catch (Exception e) {
      LOG.warn("Exception ", e);
      Assert.assertTrue(exception);
    }
    Assert.assertEquals(srcExists, exists(fc, src));
    Assert.assertEquals(dstExists, exists(fc, dst));
  }
}
