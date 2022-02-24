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

package org.apache.hadoop.tools;

import static org.apache.hadoop.test.GenericTestUtils.getMethodName;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsShell;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.util.ToolRunner;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * A JUnit test for copying files recursively.
 */

public class TestDistCpSystem {
  private static final Logger LOG =
      LoggerFactory.getLogger(TestDistCpSystem.class);

  private static final String SRCDAT = "srcdat";
  private static final String DSTDAT = "dstdat";
  private static final long BLOCK_SIZE = 1024;

  private static MiniDFSCluster cluster;
  private static Configuration conf;

  private class FileEntry {
    String path;
    boolean isDir;
    public FileEntry(String path, boolean isDir) {
      this.path = path;
      this.isDir = isDir;
    }

    String getPath() {
      return path;
    }

    boolean isDirectory() {
      return isDir;
    }
  }

  @BeforeClass
  public static void beforeClass() throws IOException {
    conf = new Configuration();
    conf.setLong(DFSConfigKeys.DFS_NAMENODE_MIN_BLOCK_SIZE_KEY, BLOCK_SIZE);
    conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, BLOCK_SIZE);
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(2).build();
    cluster.waitActive();
  }

  @AfterClass
  public static void afterClass() throws IOException {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  static String execCmd(FsShell shell, String... args) throws Exception {
    ByteArrayOutputStream baout = new ByteArrayOutputStream();
    PrintStream out = new PrintStream(baout, true);
    PrintStream old = System.out;
    System.setOut(out);
    shell.run(args);
    out.close();
    System.setOut(old);
    return baout.toString();
  }
  
  private void createFiles(DistributedFileSystem fs, String topdir,
      FileEntry[] entries, long chunkSize) throws IOException {
    long seed = System.currentTimeMillis();
    Random rand = new Random(seed);
    short replicationFactor = 2;
    for (FileEntry entry : entries) {
      Path newPath = new Path(topdir + "/" + entry.getPath());
      if (entry.isDirectory()) {
        fs.mkdirs(newPath);
      } else {
        long fileSize = BLOCK_SIZE *100;
        int bufSize = 128;
        if (chunkSize == -1) {
          DFSTestUtil.createFile(fs, newPath, bufSize,
              fileSize, BLOCK_SIZE, replicationFactor, seed);
        } else {
          // Create a variable length block file, by creating
          // one block of half block size at the chunk boundary
          long seg1 = chunkSize * BLOCK_SIZE - BLOCK_SIZE / 2;
          long seg2 = fileSize - seg1;
          DFSTestUtil.createFile(fs, newPath, bufSize,
              seg1, BLOCK_SIZE, replicationFactor, seed);
          DFSTestUtil.appendFileNewBlock(fs, newPath, (int)seg2);
        }
      }
      seed = System.currentTimeMillis() + rand.nextLong();
    }
  }

  private void createFiles(DistributedFileSystem fs, String topdir,
      FileEntry[] entries) throws IOException {
    createFiles(fs, topdir, entries, -1);
  }
   
  private static FileStatus[] getFileStatus(FileSystem fs,
      String topdir, FileEntry[] files) throws IOException {
      Path root = new Path(topdir);
      List<FileStatus> statuses = new ArrayList<FileStatus>();
      
      for (int idx = 0; idx < files.length; ++idx) {
        Path newpath = new Path(root, files[idx].getPath());
        statuses.add(fs.getFileStatus(newpath));
      }
      return statuses.toArray(new FileStatus[statuses.size()]);
    }
  

  /** delete directory and everything underneath it.*/
  private static void deldir(FileSystem fs, String topdir) throws IOException {
    fs.delete(new Path(topdir), true);
  }

  private void testPreserveUserHelper(String testRoot,
      FileEntry[] srcEntries,
      FileEntry[] dstEntries,
      boolean createSrcDir,
      boolean createTgtDir,
      boolean update) throws Exception {
    final String testSrcRel = SRCDAT;
    final String testSrc = testRoot + "/" + testSrcRel;
    final String testDstRel = DSTDAT;
    final String testDst = testRoot + "/" + testDstRel;

    String nnUri = FileSystem.getDefaultUri(conf).toString();
    DistributedFileSystem fs = (DistributedFileSystem)
        FileSystem.get(URI.create(nnUri), conf);
    fs.mkdirs(new Path(testRoot));
    if (createSrcDir) {
      fs.mkdirs(new Path(testSrc));
    }
    if (createTgtDir) {
      fs.mkdirs(new Path(testDst));
    }

    createFiles(fs, testRoot, srcEntries);
    FileStatus[] srcstats = getFileStatus(fs, testRoot, srcEntries);
    for(int i = 0; i < srcEntries.length; i++) {
      fs.setOwner(srcstats[i].getPath(), "u" + i, null);
    }
    String[] args = update? new String[]{"-pub", "-update", nnUri+testSrc,
        nnUri+testDst} : new String[]{"-pub", nnUri+testSrc, nnUri+testDst};

    ToolRunner.run(conf, new DistCp(), args);

    String realTgtPath = testDst;
    if (!createTgtDir) {
      realTgtPath = testRoot;
    }
    FileStatus[] dststat = getFileStatus(fs, realTgtPath, dstEntries);
    for(int i = 0; i < dststat.length; i++) {
      assertEquals("i=" + i, "u" + i, dststat[i].getOwner());
    }
    deldir(fs, testRoot);
  }

  private void compareFiles(FileSystem fs, FileStatus srcStat,
      FileStatus dstStat) throws Exception {
    LOG.info("Comparing " + srcStat + " and " + dstStat);
    assertEquals(srcStat.isDirectory(), dstStat.isDirectory());
    assertEquals(srcStat.getReplication(), dstStat.getReplication());
    assertEquals("File POSIX permission should match",
        srcStat.getPermission(), dstStat.getPermission());
    assertEquals("File user ownership should match",
        srcStat.getOwner(), dstStat.getOwner());
    assertEquals("File group ownership should match",
        srcStat.getGroup(), dstStat.getGroup());
    // TODO; check ACL attributes

    if (srcStat.isDirectory()) {
      return;
    }

    assertEquals("File length should match (" + srcStat.getPath() + ")",
        srcStat.getLen(), dstStat.getLen());

    FSDataInputStream srcIn = fs.open(srcStat.getPath());
    FSDataInputStream dstIn = fs.open(dstStat.getPath());
    try {
      byte[] readSrc = new byte[(int)
                                HdfsClientConfigKeys.DFS_BLOCK_SIZE_DEFAULT];
      byte[] readDst = new byte[(int)
                                HdfsClientConfigKeys.DFS_BLOCK_SIZE_DEFAULT];

      int srcBytesRead = 0, tgtBytesRead = 0;
      int srcIdx = 0, tgtIdx = 0;
      long totalComparedBytes = 0;
      while (true) {
        if (srcBytesRead == 0) {
          srcBytesRead = srcIn.read(readSrc);
          srcIdx = 0;
        }
        if (tgtBytesRead == 0) {
          tgtBytesRead = dstIn.read(readDst);
          tgtIdx = 0;
        }
        if (srcBytesRead == 0 || tgtBytesRead == 0) {
          LOG.info("______ compared src and dst files for "
              + totalComparedBytes + " bytes, content match.");
          if (srcBytesRead != tgtBytesRead) {
            Assert.fail("Read mismatching size, compared "
                + totalComparedBytes + " bytes between src and dst file "
                + srcStat + " and " + dstStat);
          }
          if (totalComparedBytes != srcStat.getLen()) {
            Assert.fail("Only read/compared " + totalComparedBytes +
                " bytes between src and dst file " + srcStat +
                " and " + dstStat);
          } else {
            // success
            break;
          }
        }
        for (; srcIdx < srcBytesRead && tgtIdx < tgtBytesRead;
            ++srcIdx, ++tgtIdx) {
          if (readSrc[srcIdx] != readDst[tgtIdx]) {
            Assert.fail("src and dst file does not match at "
                + totalComparedBytes + " between "
                + srcStat + " and " + dstStat);
          }
          ++totalComparedBytes;
        }
        LOG.info("______ compared src and dst files for "
            + totalComparedBytes + " bytes, content match. FileLength: "
            + srcStat.getLen());
        if (totalComparedBytes == srcStat.getLen()) {
          LOG.info("______ Final:" + srcIdx + " "
              + srcBytesRead + " " + tgtIdx + " " + tgtBytesRead);
          break;
        }
        if (srcIdx == srcBytesRead) {
          srcBytesRead = 0;
        }
        if (tgtIdx == tgtBytesRead) {
          tgtBytesRead = 0;
        }
      }
    } finally {
      if (srcIn != null) {
        srcIn.close();
      }
      if (dstIn != null) {
        dstIn.close();
      }
    }
  }

  // WC: needed because the current distcp does not create target dirs
  private void createDestDir(FileSystem fs, String testDst,
      FileStatus[] srcStats, FileEntry[] srcFiles) throws IOException {
    fs.mkdirs(new Path(testDst));

    for (int i=0; i<srcStats.length; i++) {
      FileStatus srcStat = srcStats[i];
      if (srcStat.isDirectory()) {
        Path dstPath = new Path(testDst, srcFiles[i].getPath());
        fs.mkdirs(dstPath);
        fs.setOwner(dstPath, srcStat.getOwner(), srcStat.getGroup());
      }
    }
  }

  private void copyAndVerify(final DistributedFileSystem fs,
      final FileEntry[] srcFiles, final FileStatus[] srcStats,
      final String testDst,
      final String[] args) throws Exception {
    final String testRoot = "/testdir";
    FsShell shell = new FsShell(fs.getConf());

    LOG.info("ls before distcp");
    LOG.info(execCmd(shell, "-lsr", testRoot));

    LOG.info("_____ running distcp: " + args[0] + " " + args[1]);
    ToolRunner.run(conf, new DistCp(), args);

    LOG.info("ls after distcp");
    LOG.info(execCmd(shell, "-lsr", testRoot));

    FileStatus[] dstStat = getFileStatus(fs, testDst, srcFiles);
    for (int i=0; i< dstStat.length; i++) {
      compareFiles(fs, srcStats[i], dstStat[i]);
    }
  }

  private void chunkCopy(FileEntry[] srcFiles) throws Exception {
    final String testRoot = "/testdir";
    final String testSrcRel = SRCDAT;
    final String testSrc = testRoot + "/" + testSrcRel;
    final String testDstRel = DSTDAT;
    final String testDst = testRoot + "/" + testDstRel;
    long chunkSize = 8;

    String nnUri = FileSystem.getDefaultUri(conf).toString();
    DistributedFileSystem fs = (DistributedFileSystem)
        FileSystem.get(URI.create(nnUri), conf);

    createFiles(fs, testRoot, srcFiles, chunkSize);

    FileStatus[] srcStats = getFileStatus(fs, testRoot, srcFiles);
    for (int i = 0; i < srcFiles.length; i++) {
      fs.setOwner(srcStats[i].getPath(), "u" + i,  "g" + i);
    }
    // get file status after updating owners
    srcStats = getFileStatus(fs, testRoot, srcFiles);

    createDestDir(fs, testDst, srcStats, srcFiles);

    String[] args = new String[] {"-pugp", "-blocksperchunk",
        String.valueOf(chunkSize),
        nnUri + testSrc, nnUri + testDst};

    copyAndVerify(fs, srcFiles, srcStats, testDst, args);
    // Do it again
    copyAndVerify(fs, srcFiles, srcStats, testDst, args);

    // modify last file and rerun distcp with -update option
    LOG.info("Modify a file and copy again");
    for(int i=srcFiles.length-1; i >=0; --i) {
      if (!srcFiles[i].isDirectory()) {
        LOG.info("Modifying " + srcStats[i].getPath());
        DFSTestUtil.appendFileNewBlock(fs, srcStats[i].getPath(),
            (int)BLOCK_SIZE * 3);
        break;
      }
    }
    // get file status after modifying file
    srcStats = getFileStatus(fs, testRoot, srcFiles);

    args = new String[] {"-pugp", "-update", "-blocksperchunk",
        String.valueOf(chunkSize),
        nnUri + testSrc, nnUri + testDst + "/" + testSrcRel};

    copyAndVerify(fs, srcFiles, srcStats, testDst, args);

    deldir(fs, testRoot);
  }

  @Test
  public void testRecursiveChunkCopy() throws Exception {
    FileEntry[] srcFiles = {
        new FileEntry(SRCDAT, true),
        new FileEntry(SRCDAT + "/file0", false),
        new FileEntry(SRCDAT + "/dir1", true),
        new FileEntry(SRCDAT + "/dir2", true),
        new FileEntry(SRCDAT + "/dir1/file1", false)
    };
    chunkCopy(srcFiles);
  }

  @Test
  public void testChunkCopyOneFile() throws Exception {
    FileEntry[] srcFiles = {
        new FileEntry(SRCDAT, true),
        new FileEntry(SRCDAT + "/file0", false)
    };
    chunkCopy(srcFiles);
  }

  @Test
  public void testDistcpLargeFile() throws Exception {
    FileEntry[] srcfiles = {
        new FileEntry(SRCDAT, true),
        new FileEntry(SRCDAT + "/file", false)
    };

    final String testRoot = "/testdir";
    final String testSrcRel = SRCDAT;
    final String testSrc = testRoot + "/" + testSrcRel;
    final String testDstRel = DSTDAT;
    final String testDst = testRoot + "/" + testDstRel;

    String nnUri = FileSystem.getDefaultUri(conf).toString();
    DistributedFileSystem fs =
        (DistributedFileSystem) FileSystem.get(URI.create(nnUri), conf);
    fs.mkdirs(new Path(testRoot));
    fs.mkdirs(new Path(testSrc));
    fs.mkdirs(new Path(testDst));
    long chunkSize = 6;
    createFiles(fs, testRoot, srcfiles, chunkSize);

    String srcFileName = testRoot + Path.SEPARATOR + srcfiles[1].getPath();
    Path srcfile = new Path(srcFileName);

    if(!cluster.getFileSystem().exists(srcfile)){
      throw new Exception("src not exist");
    }

    final long srcLen = fs.getFileStatus(srcfile).getLen();

    FileStatus[] srcstats = getFileStatus(fs, testRoot, srcfiles);
    for (int i = 0; i < srcfiles.length; i++) {
      fs.setOwner(srcstats[i].getPath(), "u" + i, null);
    }
    String[] args = new String[] {
        "-blocksperchunk",
        String.valueOf(chunkSize),
        nnUri + testSrc,
        nnUri + testDst
    };

    LOG.info("_____ running distcp: " + args[0] + " " + args[1]);
    ToolRunner.run(conf, new DistCp(), args);

    String realTgtPath = testDst;
    FileStatus[] dststat = getFileStatus(fs, realTgtPath, srcfiles);
    assertEquals("File length should match", srcLen,
        dststat[dststat.length - 1].getLen());

    this.compareFiles(fs,  srcstats[srcstats.length-1],
        dststat[dststat.length-1]);
    deldir(fs, testRoot);
  }

  @Test
  public void testPreserveUseNonEmptyDir() throws Exception {
    String testRoot = "/testdir." + getMethodName();
    FileEntry[] srcfiles = {
        new FileEntry(SRCDAT, true),
        new FileEntry(SRCDAT + "/a", false),
        new FileEntry(SRCDAT + "/b", true),
        new FileEntry(SRCDAT + "/b/c", false)
    };

    FileEntry[] dstfiles = {
        new FileEntry(DSTDAT, true),
        new FileEntry(DSTDAT + "/a", false),
        new FileEntry(DSTDAT + "/b", true),
        new FileEntry(DSTDAT + "/b/c", false)
    };

    testPreserveUserHelper(testRoot, srcfiles, srcfiles, false, true, false);
    testPreserveUserHelper(testRoot, srcfiles, dstfiles, false, false, false);
  }

  @Test
  public void testPreserveUserEmptyDir() throws Exception {
    String testRoot = "/testdir." + getMethodName();
    FileEntry[] srcfiles = {
        new FileEntry(SRCDAT, true)
    };
    
    FileEntry[] dstfiles = {
        new FileEntry(DSTDAT, true)
    };
    
    testPreserveUserHelper(testRoot, srcfiles, srcfiles, false, true, false);
    testPreserveUserHelper(testRoot, srcfiles, dstfiles, false, false, false);
  }

  @Test
  public void testPreserveUserSingleFile() throws Exception {
    String testRoot = "/testdir." + getMethodName();
    FileEntry[] srcfiles = {
        new FileEntry(SRCDAT, false)
    };
    FileEntry[] dstfiles = {
        new FileEntry(DSTDAT, false)
    };
    testPreserveUserHelper(testRoot, srcfiles, srcfiles, false, true, false);
    testPreserveUserHelper(testRoot, srcfiles, dstfiles, false, false, false);
  }

  @Test
  public void testPreserveUserNonEmptyDirWithUpdate() throws Exception {
    String testRoot = "/testdir." + getMethodName();
    FileEntry[] srcfiles = {
        new FileEntry(SRCDAT + "/a", false),
        new FileEntry(SRCDAT + "/b", true),
        new FileEntry(SRCDAT + "/b/c", false)
    };

    FileEntry[] dstfiles = {
        new FileEntry("a", false),
        new FileEntry("b", true),
        new FileEntry("b/c", false)
    };

    testPreserveUserHelper(testRoot, srcfiles, dstfiles, true, true, true);
  }

  @Test
  public void testSourceRoot() throws Exception {
    FileSystem fs = cluster.getFileSystem();

    String rootStr = fs.makeQualified(new Path("/")).toString();

    String testRoot = "/testdir." + getMethodName();

    // Case 1. The target does not exist.

    Path tgtPath = new Path(testRoot + "/nodir");
    String tgtStr = fs.makeQualified(tgtPath).toString();
    String[] args = new String[]{rootStr, tgtStr};
    Assert.assertThat(ToolRunner.run(conf, new DistCp(), args), is(0));

    // Case 2. The target exists.

    Path tgtPath2 = new Path(testRoot + "/dir");
    assertTrue(fs.mkdirs(tgtPath2));
    String tgtStr2 = fs.makeQualified(tgtPath2).toString();
    String[] args2 = new String[]{rootStr, tgtStr2};
    Assert.assertThat(ToolRunner.run(conf, new DistCp(), args2), is(0));
  }
}