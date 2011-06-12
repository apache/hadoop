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

import junit.framework.TestCase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.server.common.Storage.StorageDirectory;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.protocol.FSConstants.SafeModeAction;
import org.apache.hadoop.util.PureJavaCrc32;
import org.apache.hadoop.hdfs.server.namenode.NNStorage.NameNodeDirType;
import org.apache.hadoop.hdfs.server.namenode.NNStorage.NameNodeFile;

import java.util.Iterator;
import java.util.List;
import java.util.ArrayList;

import java.io.File;
import java.io.FileInputStream;

/**
 * A JUnit test for checking if restarting DFS preserves integrity.
 * Specifically with FSImage being written in parallel
 */
public class TestParallelImageWrite extends TestCase {
  private static final int NUM_DATANODES = 4;
  /** check if DFS remains in proper condition after a restart */
  public void testRestartDFS() throws Exception {
    final Configuration conf = new HdfsConfiguration();
    MiniDFSCluster cluster = null;
    FSNamesystem fsn = null;
    int numNamenodeDirs;
    DFSTestUtil files = new DFSTestUtil("TestRestartDFS", 200, 3, 8*1024);

    final String dir = "/srcdat";
    final Path rootpath = new Path("/");
    final Path dirpath = new Path(dir);

    long rootmtime;
    FileStatus rootstatus;
    FileStatus dirstatus;

    try {
      cluster = new MiniDFSCluster.Builder(conf).format(true)
          .numDataNodes(NUM_DATANODES).build();
      String[] nameNodeDirs = conf.getStrings(
          DFSConfigKeys.DFS_NAMENODE_NAME_DIR_KEY, new String[] {});
      numNamenodeDirs = nameNodeDirs.length;
      assertTrue("failed to get number of Namenode StorageDirs", 
          numNamenodeDirs != 0);
      FileSystem fs = cluster.getFileSystem();
      files.createFiles(fs, dir);

      rootmtime = fs.getFileStatus(rootpath).getModificationTime();
      rootstatus = fs.getFileStatus(dirpath);
      dirstatus = fs.getFileStatus(dirpath);

      fs.setOwner(rootpath, rootstatus.getOwner() + "_XXX", null);
      fs.setOwner(dirpath, null, dirstatus.getGroup() + "_XXX");
    } finally {
      if (cluster != null) { cluster.shutdown(); }
    }
    try {
      // Here we restart the MiniDFScluster without formatting namenode
      cluster = new MiniDFSCluster.Builder(conf).format(false)
          .numDataNodes(NUM_DATANODES).build();
      fsn = cluster.getNamesystem();
      FileSystem fs = cluster.getFileSystem();
      assertTrue("Filesystem corrupted after restart.",
                 files.checkFiles(fs, dir));

      final FileStatus newrootstatus = fs.getFileStatus(rootpath);
      assertEquals(rootmtime, newrootstatus.getModificationTime());
      assertEquals(rootstatus.getOwner() + "_XXX", newrootstatus.getOwner());
      assertEquals(rootstatus.getGroup(), newrootstatus.getGroup());

      final FileStatus newdirstatus = fs.getFileStatus(dirpath);
      assertEquals(dirstatus.getOwner(), newdirstatus.getOwner());
      assertEquals(dirstatus.getGroup() + "_XXX", newdirstatus.getGroup());
      rootmtime = fs.getFileStatus(rootpath).getModificationTime();

      final long checkAfterRestart = checkImages(fsn, numNamenodeDirs);
      
      // Modify the system and then perform saveNamespace
      files.cleanup(fs, dir);
      files.createFiles(fs, dir);
      fsn.setSafeMode(SafeModeAction.SAFEMODE_ENTER);
      cluster.getNameNode().saveNamespace();
      final long checkAfterModify = checkImages(fsn, numNamenodeDirs);
      assertTrue("Modified namespace doesn't change fsimage contents",
          checkAfterRestart != checkAfterModify);
      fsn.setSafeMode(SafeModeAction.SAFEMODE_LEAVE);
      files.cleanup(fs, dir);
    } finally {
      if (cluster != null) { cluster.shutdown(); }
    }
  }
  
  /**
   * Confirm that FSImage files in all StorageDirectory are the same,
   * and non-empty, and there are the expected number of them.
   * @param fsn - the FSNamesystem being checked.
   * @param numImageDirs - the configured number of StorageDirectory of type IMAGE. 
   * @return - the checksum of the FSImage files, which must all be the same.
   * @throws AssertionFailedError if image files are empty or different,
   *     if less than two StorageDirectory are provided, or if the
   *     actual number of StorageDirectory is less than configured.
   */
  public static long checkImages(FSNamesystem fsn, int numImageDirs) throws Exception {
    NNStorage stg = fsn.getFSImage().getStorage();
    //any failed StorageDirectory is removed from the storageDirs list
    assertEquals("Some StorageDirectories failed Upgrade",
        numImageDirs, stg.getNumStorageDirs(NameNodeDirType.IMAGE));
    assertTrue("Not enough fsimage copies in MiniDFSCluster " + 
        "to test parallel write", numImageDirs > 1);
    //checksum the FSImage stored in each storageDir
    Iterator<StorageDirectory> iter = stg.dirIterator(NameNodeDirType.IMAGE);
    List<Long> checksums = new ArrayList<Long>();
    while (iter.hasNext()) {
      StorageDirectory sd = iter.next();
      File fsImage = NNStorage.getStorageFile(sd, NameNodeFile.IMAGE);
      PureJavaCrc32 crc = new PureJavaCrc32();
      FileInputStream in = new FileInputStream(fsImage);
      byte[] buff = new byte[4096];
      int read = 0;
      while ((read = in.read(buff)) != -1) {
       crc.update(buff, 0, read);
      }
      long val = crc.getValue();
      checksums.add(val);
    }
    assertEquals(numImageDirs, checksums.size());
    PureJavaCrc32 crc = new PureJavaCrc32();
    long emptyCrc = crc.getValue();
    assertTrue("Empty fsimage file", checksums.get(0) != emptyCrc);
    for (int i = 1; i < numImageDirs; i++) {
      assertEquals(checksums.get(i - 1), checksums.get(i));
    }
    return checksums.get(0);
  }
}

