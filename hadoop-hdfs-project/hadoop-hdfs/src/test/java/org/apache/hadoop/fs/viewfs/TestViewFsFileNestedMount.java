/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.fs.viewfs;


import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import javax.security.auth.login.LoginException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.hdfs.MiniDFSCluster;

import org.junit.BeforeClass;
import org.junit.Test;


/**
 * Test Nested Mount for view file system.
 */
public class TestViewFsFileNestedMount {

  private static MiniDFSCluster cluster;
  private static final Configuration CONF = new Configuration();
  private static FileSystem fHdfs;
  private static FileSystem vfs;

  private static final String ROOT = "/";
  private static final String BASE_ROOT = "/home";
  private static final String MIDDLE_DIR = BASE_ROOT + "/middle";
  private static final String SON1 = BASE_ROOT + "/middle/son1";
  private static final String SON2 = BASE_ROOT + "/middle/son2";
  private static final String SON1_FILE = "son1File";
  private static final String SON2_FILE = "son2File";
  private static final String MIDDLE_FILE = "middleFile";
  private static final String BASE_ROOT_FILE1 = "baseRootFile";
  private final byte[] data = new byte[1];

  private static Path rootPath;
  private static Path son1Path;
  private static Path son2Path;
  private static Path middlePath;
  private static Path baseRootPath;

  private static Path son1FilePath;
  private static Path son2FilePath;
  private static Path middleFilePath;
  private static Path baseRootFilePath;


  @BeforeClass
  public static void clusterSetup() throws IOException,
      LoginException, URISyntaxException {
    cluster = new MiniDFSCluster.Builder(CONF).numDataNodes(2).build();
    cluster.waitClusterUp();
    fHdfs = cluster.getFileSystem();

    rootPath = new Path(ROOT);
    son1Path = new Path(SON1);
    son2Path = new Path(SON2);
    baseRootPath = new Path(BASE_ROOT);
    middlePath = new Path(MIDDLE_DIR);
    son1FilePath = new Path(SON1 + "/" + SON1_FILE);
    son2FilePath = new Path(SON2 + "/" + SON2_FILE);
    middleFilePath = new Path(MIDDLE_DIR + "/" + MIDDLE_FILE);
    baseRootFilePath = new Path(BASE_ROOT + "/" + BASE_ROOT_FILE1);

    // Setup the ViewFS to be used for all tests.
    Configuration conf = ViewFileSystemTestSetup.createConfig();
    ConfigUtil.addLink(conf, SON1, new URI(fHdfs.getUri() + SON1));
    ConfigUtil.addLink(conf, SON2, new URI(fHdfs.getUri() + SON2));
    ConfigUtil.addLink(conf, BASE_ROOT, new URI(fHdfs.getUri() + BASE_ROOT));
    vfs = FileSystem.get(FsConstants.VIEWFS_URI, conf);
    assertEquals(ViewFileSystem.class, vfs.getClass());

  }

  public void create(
      FileSystem fs, Path path, short replication, byte[] data)
      throws IOException {
    FSDataOutputStream out = fs.create(path, replication);
    out.write(data);
    out.close();
  }

  public void initAndTestCreateFile() throws IOException {
    // mkdir son1 mount point dir
    fHdfs.mkdirs(son1Path);
    assertTrue(fHdfs.exists(son1Path));
    // mkdir son2 mount point dir
    fHdfs.mkdirs(son2Path);
    assertTrue(fHdfs.exists(son2Path));

    // Create a file at the child mount point
    create(vfs, son1FilePath, (short) 1, data);
    assertTrue(vfs.exists(son1FilePath));
    create(vfs, son2FilePath, (short) 1, data);
    assertTrue(vfs.exists(son2FilePath));

    // Create a file at middle path
    // VFS allows you to create files in MIDDLE
    create(vfs, middleFilePath, (short) 1, data);
    assertTrue(vfs.exists(middleFilePath));

    // Create a file at the parent mount point
    create(vfs, baseRootFilePath, (short) 1, data);
    assertTrue(vfs.exists(baseRootFilePath));
  }

  public void cleanup() throws IOException {
    fHdfs.delete(baseRootPath, true);
    assertFalse(vfs.exists(baseRootPath));
  }

  @Test
  public void testListStatus() throws IOException {
    initAndTestCreateFile();
    // root dir fileStatuses
    FileStatus[] fileStatuses = vfs.listStatus(rootPath);
    assertEquals(1, fileStatuses.length);
    assertEquals(baseRootPath.getName(), fileStatuses[0].getPath().getName());
    // the parent mount point
    fileStatuses = vfs.listStatus(baseRootPath);
    assertEquals(2, fileStatuses.length);
    assertEquals(BASE_ROOT_FILE1, fileStatuses[0].getPath().getName());
    assertEquals(middlePath.getName(), fileStatuses[1].getPath().getName());
    // The intermediate directory is matching to the parent mount point
    fileStatuses = vfs.listStatus(middleFilePath);
    assertEquals(1, fileStatuses.length);
    assertEquals(middleFilePath.getName(), fileStatuses[0].getPath().getName());
    // Nested mount points
    fileStatuses = vfs.listStatus(son1Path);
    assertEquals(1, fileStatuses.length);
    assertEquals(son1FilePath.getName(), fileStatuses[0].getPath().getName());

    cleanup();
  }

  @Test
  public void testRename() throws IOException {
    initAndTestCreateFile();
    // Same NS different mount point rename
    vfs.delete(middleFilePath);
    assertFalse(vfs.exists(middleFilePath));

    vfs.rename(son1FilePath, middlePath);
    FileStatus[] fileStatuses = vfs.listStatus(son1Path);
    assertEquals(0, fileStatuses.length);
    fileStatuses = vfs.listStatus(middlePath);
    assertEquals(3, fileStatuses.length);
    for (FileStatus file : fileStatuses) {
      if (file.isFile()) {
        assertEquals(SON1_FILE, file.getPath().getName());
      }
    }
    cleanup();
  }

  @Test
  public void testGetContentSummary() throws IOException {
    initAndTestCreateFile();
    ContentSummary contentSummary = vfs.getContentSummary(baseRootPath);
    assertEquals(4, contentSummary.getSpaceConsumed());
    assertEquals(4, contentSummary.getFileCount());

    contentSummary = vfs.getContentSummary(son1Path);
    assertEquals(1, contentSummary.getSpaceConsumed());
    assertEquals(1, contentSummary.getFileCount());
    cleanup();
  }
}
