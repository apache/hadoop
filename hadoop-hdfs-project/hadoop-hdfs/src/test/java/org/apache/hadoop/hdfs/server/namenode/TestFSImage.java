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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.EnumSet;

import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Assume;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSOutputStream;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.client.HdfsDataOutputStream.SyncFlag;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.SafeModeAction;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.BlockUCState;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.StartupOption;
import org.apache.hadoop.hdfs.server.namenode.LeaseManager.Lease;
import org.apache.hadoop.hdfs.server.namenode.NNStorage.NameNodeDirType;
import org.apache.hadoop.hdfs.server.namenode.FsImageProto.FileSummary.Section;
import org.apache.hadoop.hdfs.server.namenode.FSImageFormatProtobuf.SectionName;
import org.apache.hadoop.hdfs.util.MD5FileUtils;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.test.PathUtils;
import org.apache.hadoop.util.NativeCodeLoader;
import org.apache.hadoop.util.Time;
import org.junit.Test;

public class TestFSImage {

  private static final String HADOOP_2_7_ZER0_BLOCK_SIZE_TGZ =
      "image-with-zero-block-size.tar.gz";
  @Test
  public void testPersist() throws IOException {
    Configuration conf = new Configuration();
    testPersistHelper(conf);
  }

  @Test
  public void testCompression() throws IOException {
    Configuration conf = new Configuration();
    conf.setBoolean(DFSConfigKeys.DFS_IMAGE_COMPRESS_KEY, true);
    setCompressCodec(conf, "org.apache.hadoop.io.compress.DefaultCodec");
    setCompressCodec(conf, "org.apache.hadoop.io.compress.GzipCodec");
    setCompressCodec(conf, "org.apache.hadoop.io.compress.BZip2Codec");
  }

  @Test
  public void testNativeCompression() throws IOException {
    Assume.assumeTrue(NativeCodeLoader.isNativeCodeLoaded());
    Configuration conf = new Configuration();
    conf.setBoolean(DFSConfigKeys.DFS_IMAGE_COMPRESS_KEY, true);
    setCompressCodec(conf, "org.apache.hadoop.io.compress.Lz4Codec");
  }

  private void setCompressCodec(Configuration conf, String compressCodec)
      throws IOException {
    conf.set(DFSConfigKeys.DFS_IMAGE_COMPRESSION_CODEC_KEY, compressCodec);
    testPersistHelper(conf);
  }

  private void testPersistHelper(Configuration conf) throws IOException {
    MiniDFSCluster cluster = null;
    try {
      cluster = new MiniDFSCluster.Builder(conf).build();
      cluster.waitActive();
      FSNamesystem fsn = cluster.getNamesystem();
      DistributedFileSystem fs = cluster.getFileSystem();

      final Path dir = new Path("/abc/def");
      final Path file1 = new Path(dir, "f1");
      final Path file2 = new Path(dir, "f2");

      // create an empty file f1
      fs.create(file1).close();

      // create an under-construction file f2
      FSDataOutputStream out = fs.create(file2);
      out.writeBytes("hello");
      ((DFSOutputStream) out.getWrappedStream()).hsync(EnumSet
          .of(SyncFlag.UPDATE_LENGTH));

      // checkpoint
      fs.setSafeMode(SafeModeAction.SAFEMODE_ENTER);
      fs.saveNamespace();
      fs.setSafeMode(SafeModeAction.SAFEMODE_LEAVE);

      cluster.restartNameNode();
      cluster.waitActive();
      fs = cluster.getFileSystem();

      assertTrue(fs.isDirectory(dir));
      assertTrue(fs.exists(file1));
      assertTrue(fs.exists(file2));

      // check internals of file2
      INodeFile file2Node = fsn.dir.getINode4Write(file2.toString()).asFile();
      assertEquals("hello".length(), file2Node.computeFileSize());
      assertTrue(file2Node.isUnderConstruction());
      BlockInfo[] blks = file2Node.getBlocks();
      assertEquals(1, blks.length);
      assertEquals(BlockUCState.UNDER_CONSTRUCTION, blks[0].getBlockUCState());
      // check lease manager
      Lease lease = fsn.leaseManager.getLease(file2Node);
      Assert.assertNotNull(lease);
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

   /**
   * On checkpointing , stale fsimage checkpoint file should be deleted.
   */
  @Test
  public void testRemovalStaleFsimageCkpt() throws IOException {
    MiniDFSCluster cluster = null;
    SecondaryNameNode secondary = null;
    Configuration conf = new HdfsConfiguration();
    try {
      cluster = new MiniDFSCluster.Builder(conf).
          numDataNodes(1).format(true).build();
      conf.set(DFSConfigKeys.DFS_NAMENODE_SECONDARY_HTTP_ADDRESS_KEY,
          "0.0.0.0:0");
      secondary = new SecondaryNameNode(conf);
      // Do checkpointing
      secondary.doCheckpoint();
      NNStorage storage = secondary.getFSImage().storage;
      File currentDir = FSImageTestUtil.
          getCurrentDirs(storage, NameNodeDirType.IMAGE).get(0);
      // Create a stale fsimage.ckpt file
      File staleCkptFile = new File(currentDir.getPath() +
          "/fsimage.ckpt_0000000000000000002");
      staleCkptFile.createNewFile();
      assertTrue(staleCkptFile.exists());
      // After checkpoint stale fsimage.ckpt file should be deleted
      secondary.doCheckpoint();
      assertFalse(staleCkptFile.exists());
    } finally {
      if (secondary != null) {
        secondary.shutdown();
        secondary = null;
      }
      if (cluster != null) {
        cluster.shutdown();
        cluster = null;
      }
    }
  }

  /**
   * Ensure that the digest written by the saver equals to the digest of the
   * file.
   */
  @Test
  public void testDigest() throws IOException {
    Configuration conf = new Configuration();
    MiniDFSCluster cluster = null;
    try {
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(0).build();
      DistributedFileSystem fs = cluster.getFileSystem();
      fs.setSafeMode(SafeModeAction.SAFEMODE_ENTER);
      fs.saveNamespace();
      fs.setSafeMode(SafeModeAction.SAFEMODE_LEAVE);
      File currentDir = FSImageTestUtil.getNameNodeCurrentDirs(cluster, 0).get(
          0);
      File fsimage = FSImageTestUtil.findNewestImageFile(currentDir
          .getAbsolutePath());
      assertEquals(MD5FileUtils.readStoredMd5ForFile(fsimage),
          MD5FileUtils.computeMd5ForFile(fsimage));
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  /**
   * Ensure mtime and atime can be loaded from fsimage.
   */
  @Test(timeout=60000)
  public void testLoadMtimeAtime() throws Exception {
    Configuration conf = new Configuration();
    MiniDFSCluster cluster = null;
    try {
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(1).build();
      cluster.waitActive();
      DistributedFileSystem hdfs = cluster.getFileSystem();
      String userDir = hdfs.getHomeDirectory().toUri().getPath().toString();
      Path file = new Path(userDir, "file");
      Path dir = new Path(userDir, "/dir");
      Path link = new Path(userDir, "/link");
      hdfs.createNewFile(file);
      hdfs.mkdirs(dir);
      hdfs.createSymlink(file, link, false);

      long mtimeFile = hdfs.getFileStatus(file).getModificationTime();
      long atimeFile = hdfs.getFileStatus(file).getAccessTime();
      long mtimeDir = hdfs.getFileStatus(dir).getModificationTime();
      long mtimeLink = hdfs.getFileLinkStatus(link).getModificationTime();
      long atimeLink = hdfs.getFileLinkStatus(link).getAccessTime();

      // save namespace and restart cluster
      hdfs.setSafeMode(HdfsConstants.SafeModeAction.SAFEMODE_ENTER);
      hdfs.saveNamespace();
      hdfs.setSafeMode(HdfsConstants.SafeModeAction.SAFEMODE_LEAVE);
      cluster.shutdown();
      cluster = new MiniDFSCluster.Builder(conf).format(false)
          .numDataNodes(1).build();
      cluster.waitActive();
      hdfs = cluster.getFileSystem();
      
      assertEquals(mtimeFile, hdfs.getFileStatus(file).getModificationTime());
      assertEquals(atimeFile, hdfs.getFileStatus(file).getAccessTime());
      assertEquals(mtimeDir, hdfs.getFileStatus(dir).getModificationTime());
      assertEquals(mtimeLink, hdfs.getFileLinkStatus(link).getModificationTime());
      assertEquals(atimeLink, hdfs.getFileLinkStatus(link).getAccessTime());
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  /**
   * Ensure ctime is set during namenode formatting.
   */
  @Test(timeout=60000)
  public void testCtime() throws Exception {
    Configuration conf = new Configuration();
    MiniDFSCluster cluster = null;
    try {
      final long pre = Time.now();
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(1).build();
      cluster.waitActive();
      final long post = Time.now();
      final long ctime = cluster.getNamesystem().getCTime();

      assertTrue(pre <= ctime);
      assertTrue(ctime <= post);
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  /**
   * In this test case, I have created an image with a file having
   * preferredblockSize = 0. We are trying to read this image (since file with
   * preferredblockSize = 0 was allowed pre 2.1.0-beta version. The namenode 
   * after 2.6 version will not be able to read this particular file.
   * See HDFS-7788 for more information.
   * @throws Exception
   */
  @Test
  public void testZeroBlockSize() throws Exception {
    final Configuration conf = new HdfsConfiguration();
    String tarFile = System.getProperty("test.cache.data", "build/test/cache")
      + "/" + HADOOP_2_7_ZER0_BLOCK_SIZE_TGZ;
    String testDir = PathUtils.getTestDirName(getClass());
    File dfsDir = new File(testDir, "image-with-zero-block-size");
    if (dfsDir.exists() && !FileUtil.fullyDelete(dfsDir)) {
      throw new IOException("Could not delete dfs directory '" + dfsDir + "'");
    }
    FileUtil.unTar(new File(tarFile), new File(testDir));
    File nameDir = new File(dfsDir, "name");
    GenericTestUtils.assertExists(nameDir);
    conf.set(DFSConfigKeys.DFS_NAMENODE_NAME_DIR_KEY, 
        nameDir.getAbsolutePath());
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).numDataNodes(1)
        .format(false)
        .manageDataDfsDirs(false)
        .manageNameDfsDirs(false)
        .waitSafeMode(false)
        .startupOption(StartupOption.UPGRADE)
        .build();
    try {
      FileSystem fs = cluster.getFileSystem();
      Path testPath = new Path("/tmp/zeroBlockFile");
      assertTrue("File /tmp/zeroBlockFile doesn't exist ", fs.exists(testPath));
      assertTrue("Name node didn't come up", cluster.isNameNodeUp(0));
    } finally {
      cluster.shutdown();
      //Clean up
      FileUtil.fullyDelete(dfsDir);
    }
  }

  private ArrayList<Section> getSubSectionsOfName(ArrayList<Section> sections,
      FSImageFormatProtobuf.SectionName name) {
    ArrayList<Section> subSec = new ArrayList<>();
    for (Section s : sections) {
      if (s.getName().equals(name.toString())) {
        subSec.add(s);
      }
    }
    return subSec;
  }

  private MiniDFSCluster createAndLoadParallelFSImage(Configuration conf)
      throws IOException {
    conf.set(DFSConfigKeys.DFS_IMAGE_PARALLEL_LOAD_KEY, "true");
    conf.set(DFSConfigKeys.DFS_IMAGE_PARALLEL_INODE_THRESHOLD_KEY, "1");
    conf.set(DFSConfigKeys.DFS_IMAGE_PARALLEL_TARGET_SECTIONS_KEY, "4");
    conf.set(DFSConfigKeys.DFS_IMAGE_PARALLEL_THREADS_KEY, "4");

    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).build();
    cluster.waitActive();
    DistributedFileSystem fs = cluster.getFileSystem();

    // Create 10 directories, each containing 5 files
    String baseDir = "/abc/def";
    for (int i=0; i<10; i++) {
      Path dir = new Path(baseDir+"/"+i);
      for (int j=0; j<5; j++) {
        Path f = new Path(dir, Integer.toString(j));
        FSDataOutputStream os = fs.create(f);
        os.write(1);
        os.close();
      }
    }

    // checkpoint
    fs.setSafeMode(SafeModeAction.SAFEMODE_ENTER);
    fs.saveNamespace();
    fs.setSafeMode(SafeModeAction.SAFEMODE_LEAVE);

    cluster.restartNameNode();
    cluster.waitActive();
    fs = cluster.getFileSystem();

    // Ensure all the files created above exist, proving they were loaded
    // correctly
    for (int i=0; i<10; i++) {
      Path dir = new Path(baseDir+"/"+i);
      assertTrue(fs.getFileStatus(dir).isDirectory());
      for (int j=0; j<5; j++) {
        Path f = new Path(dir, Integer.toString(j));
        assertTrue(fs.exists(f));
      }
    }
    return cluster;
  }

  @Test
  public void testParallelSaveAndLoad() throws IOException {
    Configuration conf = new Configuration();

    MiniDFSCluster cluster = null;
    try {
      cluster = createAndLoadParallelFSImage(conf);

      // Obtain the image summary section to check the sub-sections
      // are being correctly created when the image is saved.
      FsImageProto.FileSummary summary = FSImageTestUtil.
          getLatestImageSummary(cluster);
      ArrayList<Section> sections = Lists.newArrayList(
          summary.getSectionsList());

      ArrayList<Section> inodeSubSections =
          getSubSectionsOfName(sections, SectionName.INODE_SUB);
      ArrayList<Section> dirSubSections =
          getSubSectionsOfName(sections, SectionName.INODE_DIR_SUB);
      Section inodeSection =
          getSubSectionsOfName(sections, SectionName.INODE).get(0);
      Section dirSection = getSubSectionsOfName(sections,
          SectionName.INODE_DIR).get(0);

      // Expect 4 sub-sections for inodes and directories as target Sections
      // is 4
      assertEquals(4, inodeSubSections.size());
      assertEquals(4, dirSubSections.size());

      // Expect the sub-section offset and lengths do not overlap and cover a
      // continuous range of the file. They should also line up with the parent
      ensureSubSectionsAlignWithParent(inodeSubSections, inodeSection);
      ensureSubSectionsAlignWithParent(dirSubSections, dirSection);
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  @Test
  public void testNoParallelSectionsWithCompressionEnabled()
      throws IOException {
    Configuration conf = new Configuration();
    conf.setBoolean(DFSConfigKeys.DFS_IMAGE_COMPRESS_KEY, true);
    conf.set(DFSConfigKeys.DFS_IMAGE_COMPRESSION_CODEC_KEY,
        "org.apache.hadoop.io.compress.GzipCodec");

    MiniDFSCluster cluster = null;
    try {
      cluster = createAndLoadParallelFSImage(conf);

      // Obtain the image summary section to check the sub-sections
      // are being correctly created when the image is saved.
      FsImageProto.FileSummary summary = FSImageTestUtil.
          getLatestImageSummary(cluster);
      ArrayList<Section> sections = Lists.newArrayList(
          summary.getSectionsList());

      ArrayList<Section> inodeSubSections =
          getSubSectionsOfName(sections, SectionName.INODE_SUB);
      ArrayList<Section> dirSubSections =
          getSubSectionsOfName(sections, SectionName.INODE_DIR_SUB);

      // As compression is enabled, there should be no sub-sections in the
      // image header
      assertEquals(0, inodeSubSections.size());
      assertEquals(0, dirSubSections.size());
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  private void ensureSubSectionsAlignWithParent(ArrayList<Section> subSec,
                                                Section parent) {
    // For each sub-section, check its offset + length == the next section
    // offset
    for (int i=0; i<subSec.size()-1; i++) {
      Section s = subSec.get(i);
      long endOffset = s.getOffset() + s.getLength();
      assertEquals(subSec.get(i+1).getOffset(), endOffset);
    }
    // The last sub-section should align with the parent section
    Section lastSubSection = subSec.get(subSec.size()-1);
    assertEquals(parent.getLength()+parent.getOffset(),
        lastSubSection.getLength() + lastSubSection.getOffset());
    // The first sub-section and parent section should have the same offset
    assertEquals(parent.getOffset(), subSec.get(0).getOffset());
  }
}
