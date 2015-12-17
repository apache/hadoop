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

package org.apache.hadoop.fs;

import java.io.IOException;
import java.net.URISyntaxException;

import javax.security.auth.login.LoginException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.util.StringUtils;
import static org.apache.hadoop.fs.FileContextTestHelper.*;
import org.apache.log4j.Level;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestFcHdfsSetUMask {
  
  private static final FileContextTestHelper fileContextTestHelper =
      new FileContextTestHelper("/tmp/TestFcHdfsSetUMask");
  private static MiniDFSCluster cluster;
  private static Path defaultWorkingDirectory;
  private static FileContext fc;

  // rwxrwx---
  private static final FsPermission USER_GROUP_OPEN_PERMISSIONS = FsPermission
      .createImmutable((short) 0770);

  private static final FsPermission USER_GROUP_OPEN_FILE_PERMISSIONS = 
      FsPermission.createImmutable((short) 0660);

  private static final FsPermission USER_GROUP_OPEN_TEST_UMASK = FsPermission
      .createImmutable((short) (0770 ^ 0777));

  // ---------
  private static final FsPermission BLANK_PERMISSIONS = FsPermission
      .createImmutable((short) 0000);

  // parent directory permissions when creating a directory with blank (000)
  // permissions - it always add the -wx------ bits to the parent so that
  // it can create the child
  private static final FsPermission PARENT_PERMS_FOR_BLANK_PERMISSIONS = 
      FsPermission.createImmutable((short) 0300);

  private static final FsPermission BLANK_TEST_UMASK = FsPermission
      .createImmutable((short) (0000 ^ 0777));
  
  // rwxrwxrwx
  private static final FsPermission WIDE_OPEN_PERMISSIONS = FsPermission
      .createImmutable((short) 0777);

  private static final FsPermission WIDE_OPEN_FILE_PERMISSIONS = 
      FsPermission.createImmutable((short) 0666);

  private static final FsPermission WIDE_OPEN_TEST_UMASK = FsPermission
      .createImmutable((short) (0777 ^ 0777));
  
  @BeforeClass
  public static void clusterSetupAtBegining()
        throws IOException, LoginException, URISyntaxException  {
    Configuration conf = new HdfsConfiguration();
    // set permissions very restrictive
    conf.set(CommonConfigurationKeys.FS_PERMISSIONS_UMASK_KEY,  "077");
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(2).build();
    fc = FileContext.getFileContext(cluster.getURI(0), conf);
    defaultWorkingDirectory = fc.makeQualified( new Path("/user/" + 
        UserGroupInformation.getCurrentUser().getShortUserName()));
    fc.mkdir(defaultWorkingDirectory, FileContext.DEFAULT_PERM, true);
  }

  @AfterClass
  public static void ClusterShutdownAtEnd() throws Exception {
    if (cluster != null) {
      cluster.shutdown();
    }
  }
  
  {
    try {
      GenericTestUtils.setLogLevel(FileSystem.LOG, Level.DEBUG);
    }
    catch(Exception e) {
      System.out.println("Cannot change log level\n"
          + StringUtils.stringifyException(e));
    }
  }

  @Before
  public void setUp() throws Exception {
    fc.setUMask(WIDE_OPEN_TEST_UMASK);
    fc.mkdir(fileContextTestHelper.getTestRootPath(fc), FileContext.DEFAULT_PERM, true);
  }

  @After
  public void tearDown() throws Exception {
    fc.delete(fileContextTestHelper.getTestRootPath(fc), true);
  }

  @Test
  public void testMkdirWithExistingDirClear() throws IOException {
    testMkdirWithExistingDir(BLANK_TEST_UMASK, BLANK_PERMISSIONS);
  }

  @Test
  public void testMkdirWithExistingDirOpen() throws IOException {
    testMkdirWithExistingDir(WIDE_OPEN_TEST_UMASK, WIDE_OPEN_PERMISSIONS);
  }

  @Test
  public void testMkdirWithExistingDirMiddle() throws IOException {
    testMkdirWithExistingDir(USER_GROUP_OPEN_TEST_UMASK,
        USER_GROUP_OPEN_PERMISSIONS);
  }
  
  @Test
  public void testMkdirRecursiveWithNonExistingDirClear() throws IOException {
    // by default parent directories have -wx------ bits set
    testMkdirRecursiveWithNonExistingDir(BLANK_TEST_UMASK, BLANK_PERMISSIONS, 
        PARENT_PERMS_FOR_BLANK_PERMISSIONS);
  }

  @Test
  public void testMkdirRecursiveWithNonExistingDirOpen() throws IOException {
    testMkdirRecursiveWithNonExistingDir(WIDE_OPEN_TEST_UMASK, 
        WIDE_OPEN_PERMISSIONS, WIDE_OPEN_PERMISSIONS);
  }

  @Test
  public void testMkdirRecursiveWithNonExistingDirMiddle() throws IOException {
    testMkdirRecursiveWithNonExistingDir(USER_GROUP_OPEN_TEST_UMASK,
        USER_GROUP_OPEN_PERMISSIONS, USER_GROUP_OPEN_PERMISSIONS);
  }


  @Test
  public void testCreateRecursiveWithExistingDirClear() throws IOException {
    testCreateRecursiveWithExistingDir(BLANK_TEST_UMASK, BLANK_PERMISSIONS);
  }

  @Test
  public void testCreateRecursiveWithExistingDirOpen() throws IOException {
    testCreateRecursiveWithExistingDir(WIDE_OPEN_TEST_UMASK,
        WIDE_OPEN_FILE_PERMISSIONS);
  }

  @Test
  public void testCreateRecursiveWithExistingDirMiddle() throws IOException {
    testCreateRecursiveWithExistingDir(USER_GROUP_OPEN_TEST_UMASK,
        USER_GROUP_OPEN_FILE_PERMISSIONS);
  }


  @Test
  public void testCreateRecursiveWithNonExistingDirClear() throws IOException {
    // directory permission inherited from parent so this must match the @Before
    // set of umask
    testCreateRecursiveWithNonExistingDir(BLANK_TEST_UMASK,
        WIDE_OPEN_PERMISSIONS, BLANK_PERMISSIONS);
  }

  @Test
  public void testCreateRecursiveWithNonExistingDirOpen() throws IOException {
    // directory permission inherited from parent so this must match the @Before
    // set of umask
    testCreateRecursiveWithNonExistingDir(WIDE_OPEN_TEST_UMASK,
        WIDE_OPEN_PERMISSIONS, WIDE_OPEN_FILE_PERMISSIONS);
  }

  @Test
  public void testCreateRecursiveWithNonExistingDirMiddle() throws IOException {
    // directory permission inherited from parent so this must match the @Before
    // set of umask
    testCreateRecursiveWithNonExistingDir(USER_GROUP_OPEN_TEST_UMASK, 
        WIDE_OPEN_PERMISSIONS, USER_GROUP_OPEN_FILE_PERMISSIONS);
  }


  public void testMkdirWithExistingDir(FsPermission umask, 
      FsPermission expectedPerms) throws IOException {
    Path f = fileContextTestHelper.getTestRootPath(fc, "aDir");
    fc.setUMask(umask);
    fc.mkdir(f, FileContext.DEFAULT_PERM, true);
    Assert.assertTrue(isDir(fc, f));
    Assert.assertEquals("permissions on directory are wrong",  
        expectedPerms, fc.getFileStatus(f).getPermission());
  }
  
  public void testMkdirRecursiveWithNonExistingDir(FsPermission umask,
      FsPermission expectedPerms, FsPermission expectedParentPerms) 
      throws IOException {
    Path f = fileContextTestHelper.getTestRootPath(fc, "NonExistant2/aDir");
    fc.setUMask(umask);
    fc.mkdir(f, FileContext.DEFAULT_PERM, true);
    Assert.assertTrue(isDir(fc, f));
    Assert.assertEquals("permissions on directory are wrong",  
        expectedPerms, fc.getFileStatus(f).getPermission());
    Path fParent = fileContextTestHelper.getTestRootPath(fc, "NonExistant2");
    Assert.assertEquals("permissions on parent directory are wrong",  
        expectedParentPerms, fc.getFileStatus(fParent).getPermission());
  }


  public void testCreateRecursiveWithExistingDir(FsPermission umask,
      FsPermission expectedPerms) throws IOException {
    Path f = fileContextTestHelper.getTestRootPath(fc,"foo");
    fc.setUMask(umask);
    createFile(fc, f);
    Assert.assertTrue(isFile(fc, f));
    Assert.assertEquals("permissions on file are wrong",  
        expectedPerms , fc.getFileStatus(f).getPermission());
  }
  
  
  public void testCreateRecursiveWithNonExistingDir(FsPermission umask,
      FsPermission expectedDirPerms, FsPermission expectedFilePerms) 
      throws IOException {
    Path f = fileContextTestHelper.getTestRootPath(fc,"NonExisting/foo");
    Path fParent = fileContextTestHelper.getTestRootPath(fc, "NonExisting");
    Assert.assertFalse(exists(fc, fParent));
    fc.setUMask(umask);
    createFile(fc, f);
    Assert.assertTrue(isFile(fc, f));
    Assert.assertEquals("permissions on file are wrong",  
        expectedFilePerms, fc.getFileStatus(f).getPermission());
    Assert.assertEquals("permissions on parent directory are wrong",  
        expectedDirPerms, fc.getFileStatus(fParent).getPermission());
  }
 
}
