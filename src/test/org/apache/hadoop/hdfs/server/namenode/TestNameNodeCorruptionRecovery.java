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

import static org.junit.Assert.*;

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.server.namenode.FSImage.NameNodeDirType;
import org.apache.hadoop.hdfs.server.namenode.FSImage.NameNodeFile;
import org.apache.hadoop.io.IOUtils;
import org.junit.After;
import org.junit.Test;

/**
 * Test the name node's ability to recover from partially corrupted storage
 * directories.
 */
public class TestNameNodeCorruptionRecovery {

  private static final Log LOG = LogFactory.getLog(
    TestNameNodeCorruptionRecovery.class);
  
  private MiniDFSCluster cluster;
  
  @After
  public void tearDownCluster() {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  /**
   * Test that a corrupted fstime file in a single storage directory does not
   * prevent the NN from starting up.
   */
  @Test
  public void testFsTimeFileCorrupt() throws IOException, InterruptedException {
    cluster = new MiniDFSCluster(new Configuration(), 0, true, null);
    cluster.waitActive();
    assertEquals(cluster.getNameDirs().size(), 2);
    // Get the first fstime file and truncate it.
    truncateStorageDirFile(cluster, NameNodeFile.TIME, 0);
    // Make sure we can start up despite the fact the fstime file is corrupted.
    cluster.restartNameNode();
  }

  /**
   * Tests that a cluster's image is not damaged if checkpoint fails after
   * writing checkpoint time to the image directory but before writing checkpoint
   * time to the edits directory.  This is a very rare failure scenario that can
   * only occur if the namenode is configured with separate directories for image
   * and edits.  This test simulates the failure by forcing the fstime file for
   * edits to contain 0, so that it appears the checkpoint time for edits is less
   * than the checkpoint time for image.
   */
  @Test
  public void testEditsFsTimeLessThanImageFsTime() throws Exception {
    // Create a cluster with separate directories for image and edits.
    Configuration conf = new Configuration();
    File testDir = new File(System.getProperty("test.build.data",
      "build/test/data"), "dfs/");
    conf.set("dfs.name.dir", new File(testDir, "name").getPath());
    conf.set("dfs.name.edits.dir", new File(testDir, "edits").getPath());
    cluster = new MiniDFSCluster(0, conf, 1, true, false, true, null, null, null,
      null);
    cluster.waitActive();

    // Create several files to generate some edits.
    createFile("one");
    createFile("two");
    createFile("three");
    assertTrue(checkFileExists("one"));
    assertTrue(checkFileExists("two"));
    assertTrue(checkFileExists("three"));

    // Restart to force a checkpoint.
    cluster.restartNameNode();

    // Shutdown so that we can safely modify the fstime file.
    File[] editsFsTime = cluster.getNameNode().getFSImage().getFileNames(
      NameNodeFile.TIME, NameNodeDirType.EDITS);
    assertTrue("expected exactly one edits directory containing fstime file",
      editsFsTime.length == 1);
    cluster.shutdown();

    // Write 0 into the fstime file for the edits directory.
    FileOutputStream fos = null;
    DataOutputStream dos = null;
    try {
      fos = new FileOutputStream(editsFsTime[0]);
      dos = new DataOutputStream(fos);
      dos.writeLong(0);
    } finally {
      IOUtils.cleanup(LOG, dos, fos);
    }

    // Restart to force another checkpoint, which should discard the old edits.
    cluster = new MiniDFSCluster(0, conf, 1, false, false, true, null, null,
      null, null);
    cluster.waitActive();

    // Restart one more time.  If all of the prior checkpoints worked correctly,
    // then we expect to load the image successfully and find the files.
    cluster.restartNameNode();
    assertTrue(checkFileExists("one"));
    assertTrue(checkFileExists("two"));
    assertTrue(checkFileExists("three"));
  }

  /**
   * Checks that a file exists in the cluster.
   * 
   * @param file String name of file to check
   * @return boolean true if file exists
   * @throws IOException thrown if there is an I/O error
   */
  private boolean checkFileExists(String file) throws IOException {
    return cluster.getFileSystem().exists(new Path(file));
  }

  /**
   * Creates a new, empty file in the cluster.
   * 
   * @param file String name of file to create
   * @throws IOException thrown if there is an I/O error
   */
  private void createFile(String file) throws IOException {
    cluster.getFileSystem().create(new Path(file)).close();
  }

  private static void truncateStorageDirFile(MiniDFSCluster cluster,
      NameNodeFile f, int storageDirIndex) throws IOException {
    File currentDir = cluster.getNameNode().getFSImage()
        .getStorageDir(storageDirIndex).getCurrentDir();
    File nameNodeFile = new File(currentDir, f.getName());
    assertTrue(nameNodeFile.isFile());
    assertTrue(nameNodeFile.delete());
    assertTrue(nameNodeFile.createNewFile());
  }
}
