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
import org.apache.hadoop.hdfs.server.namenode.FSImage;
import org.apache.hadoop.hdfs.server.common.Storage.StorageDirectory;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.protocol.FSConstants.SafeModeAction;
import org.apache.hadoop.util.PureJavaCrc32;

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
  /** check if DFS remains in proper condition after a restart */
  public void testRestartDFS() throws Exception {
    final Configuration conf = new HdfsConfiguration();
    MiniDFSCluster cluster = null;
    FSNamesystem fsn = null;
    DFSTestUtil files = new DFSTestUtil("TestRestartDFS", 200, 3, 8*1024);

    final String dir = "/srcdat";
    final Path rootpath = new Path("/");
    final Path dirpath = new Path(dir);

    long rootmtime;
    FileStatus rootstatus;
    FileStatus dirstatus;

    try {
      cluster = new MiniDFSCluster.Builder(conf).format(true).numDataNodes(4).build();
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
      cluster = new MiniDFSCluster.Builder(conf).format(false).numDataNodes(4).build();
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

      checkImages(fsn);

      // Modify the system and then perform saveNamespace
      files.cleanup(fs, dir);
      files.createFiles(fs, dir);
      fsn.setSafeMode(SafeModeAction.SAFEMODE_ENTER);
      cluster.getNameNode().saveNamespace();
      checkImages(fsn);
      fsn.setSafeMode(SafeModeAction.SAFEMODE_LEAVE);
      files.cleanup(fs, dir);
    } finally {
      if (cluster != null) { cluster.shutdown(); }
    }
  }
  
  private void checkImages(FSNamesystem fsn) throws Exception {
    Iterator<StorageDirectory> iter = fsn.
            getFSImage().dirIterator(FSImage.NameNodeDirType.IMAGE);
    List<Long> checksums = new ArrayList<Long>();
    while (iter.hasNext()) {
      StorageDirectory sd = iter.next();
      File fsImage = FSImage.getImageFile(sd, FSImage.NameNodeFile.IMAGE);
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
    assertTrue("Not enough fsimage copies in MiniDFSCluster " + 
               "to test parallel write", checksums.size() > 1);
    for (int i = 1; i < checksums.size(); i++) {
      assertEquals(checksums.get(i - 1), checksums.get(i));
    }
  }
}

