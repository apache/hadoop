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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.server.common.blockaliasmap.BlockAliasMap;
import org.apache.hadoop.hdfs.server.common.blockaliasmap.impl.TextFileRegionAliasMap;
import org.apache.hadoop.hdfs.server.namenode.mountmanager.FSMultiRootTreeWalk;
import org.apache.hadoop.hdfs.server.namenode.mountmanager.FsUGIResolver;
import org.apache.hadoop.hdfs.server.namenode.mountmanager.UGIResolver;
import org.apache.hadoop.util.ToolRunner;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_IMAGE_WRITER_UGI_CLASS;
import static org.apache.hadoop.hdfs.server.common.blockaliasmap.impl.TextFileRegionAliasMap.fileNameFromBlockPoolID;
import static org.apache.hadoop.hdfs.server.namenode.ITestProvidedImplementation.createFiles;
import static org.apache.hadoop.hdfs.server.namenode.ITestProvidedImplementation.startCluster;
import static org.apache.hadoop.hdfs.server.namenode.ITestProvidedImplementation.verifyPaths;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * Integration test for {@link FSMultiRootTreeWalk}.
 */
public class TestMultiRootProvidedCluster {

  public static final Logger LOG =
      LoggerFactory.getLogger(TestMultiRootProvidedCluster.class);

  private final File fBase = new File(MiniDFSCluster.getBaseDirectory());
  private final Path base = new Path(fBase.toURI().toString());
  private final Path providedPath = new Path(base, "providedDir");
  private final Path nnDirPath = new Path(base, "nnDir");
  private final int baseFileLen = 1024;
  private Configuration conf;
  private final String providedBlockPoolID = "BP-PROVIDED-1234-127.0.0.0-1234";

  @Before
  public void setup() throws Exception {
    conf = new HdfsConfiguration();

    conf.setBoolean(DFSConfigKeys.DFS_NAMENODE_PROVIDED_ENABLED, true);
    conf.setClass(DFSConfigKeys.DFS_PROVIDED_ALIASMAP_CLASS,
        TextFileRegionAliasMap.class, BlockAliasMap.class);
    conf.set(DFSConfigKeys.DFS_PROVIDED_ALIASMAP_TEXT_WRITE_DIR,
            nnDirPath.toString());
    conf.set(DFSConfigKeys.DFS_PROVIDED_ALIASMAP_TEXT_READ_FILE,
        new Path(nnDirPath,
            fileNameFromBlockPoolID(providedBlockPoolID)).toString());
    conf.setClass(DFS_IMAGE_WRITER_UGI_CLASS, FsUGIResolver.class,
        UGIResolver.class);
    File imageDir = new File(providedPath.toUri());
    if (!imageDir.exists()) {
      LOG.info("Creating directory: " + imageDir);
      imageDir.mkdirs();
    }

    File nnDir = new File(nnDirPath.toUri());
    if (!nnDir.exists()) {
      nnDir.mkdirs();
    }
    fBase.deleteOnExit();
  }

  @Test
  public void testSingleRootCluster() throws Exception {
    // create files in remote path.
    createFiles(providedPath, 10, baseFileLen, "file", ".dat");

    int ret = ToolRunner.run(conf, new FileSystemImage(),
        new String[] {"-o", nnDirPath.toString(), "-bpid", providedBlockPoolID,
            providedPath.toString()});
    assertEquals(0, ret);
    LOG.info("Created image");
    MiniDFSCluster cluster = startCluster(nnDirPath, 3,
        new StorageType[] {StorageType.PROVIDED, StorageType.DISK},
        null, false, conf);
    // verify all mount points and paths exist.
    verifyPaths(cluster, conf, new Path("/"), providedPath);
    cluster.shutdown(true, true);
  }

  @Test
  public void testMultipleMounts() throws Exception {
    int numFiles = 10;
    int numSubDirs = 3;
    int numMountPoints = 3;
    List<String[]> mountPairs = new ArrayList<>();
    String hdfsCommonPath = "/testHDFS/user/alice";
    String[] cmdLineArgs = new String[numMountPoints + 4];
    cmdLineArgs[0] = "-o";
    cmdLineArgs[1] = nnDirPath.toString();
    cmdLineArgs[2] = "-bpid";
    cmdLineArgs[3] = providedBlockPoolID;

    // create mount points
    for (int i = 0; i < numMountPoints; i++) {
      String[] mountPair = new String[2];
      // local path
      mountPair[0] = hdfsCommonPath + "/remote" + i;
      // remote path
      Path remotePath = new Path(providedPath, "dir" + i);
      mountPair[1] = remotePath.toString();
      // create files on remote paths
      createSubDirectoriesFiles(remotePath, numSubDirs, numFiles * i);
      mountPairs.add(mountPair);
      cmdLineArgs[4 + i] = mountPair[0] + "," + mountPair[1];
    }

    // run tool to create image.
    int ret = ToolRunner.run(conf, new FileSystemImage(), cmdLineArgs);
    assertEquals(0, ret);
    LOG.info("Created image");
    MiniDFSCluster cluster = startCluster(nnDirPath, 3,
        new StorageType[] {StorageType.PROVIDED, StorageType.DISK},
        null, false, conf);
    // verify all mount points and paths exist.
    for (String[] mountPair: mountPairs) {
      verifyPaths(cluster, conf, new Path(mountPair[0]),
          new Path(mountPair[1]));
    }
    cluster.shutdown(true, true);
  }

  @Test
  public void testOverlappingMounts() throws Exception {
    List<String[]> mountPairs = new ArrayList<>();
    String hdfsCommonPath = "/testHDFS/user/bob";

    Path remotePath1 = new Path(providedPath, "dir1");
    Path remotePath2 = new Path(providedPath, "dir2");
    // create files on remote paths
    createSubDirectoriesFiles(remotePath1, 2, 10);
    createSubDirectoriesFiles(remotePath2, 1, 10);

    mountPairs.add(new String[] {hdfsCommonPath, remotePath1.toString()});
    mountPairs.add(new String[] {hdfsCommonPath, remotePath2.toString()});
    try {
      ToolRunner.run(conf, new FileSystemImage(),
          new String[] {"-o", nnDirPath.toString(),
              hdfsCommonPath + "," + remotePath1.toString(),
              hdfsCommonPath + "," + remotePath2.toString()});
      fail("Exception expected when running tool with nested mounts");
    } catch (Exception e) {
      LOG.info("Expected exception : " + e);
    }
  }

  private void createSubDirectoriesFiles(Path basePath, int numSubDirs,
      int numFiles) throws IOException {
    File nnDir = new File(basePath.toUri());
    if (!nnDir.exists()) {
      nnDir.mkdirs();
    }
    for (int dirCount = 0; dirCount < numSubDirs; dirCount++) {
      Path dir = new Path(basePath, "subDir" + dirCount);
      File fDir = new File(dir.toUri());
      if (!fDir.exists()) {
        fDir.mkdirs();
      }
      createFiles(dir, numFiles, baseFileLen, "file", ".dat");
    }
  }
}