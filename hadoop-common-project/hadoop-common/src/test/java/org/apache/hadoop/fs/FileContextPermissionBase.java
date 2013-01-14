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
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

import junit.framework.Assert;

import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.Shell;
import org.apache.hadoop.util.StringUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.apache.hadoop.fs.FileContextTestHelper.*;
import static org.junit.Assert.assertEquals;

/**
 * <p>
 * A collection of permission tests for the {@link FileContext}.
 * This test should be used for testing an instance of FileContext
 *  that has been initialized to a specific default FileSystem such a
 *  LocalFileSystem, HDFS,S3, etc.
 * </p>
 * <p>
 * To test a given {@link FileSystem} implementation create a subclass of this
 * test and override {@link #setUp()} to initialize the <code>fc</code> 
 * {@link FileContext} instance variable.
 * 
 * Since this a junit 4 you can also do a single setup before 
 * the start of any tests.
 * E.g.
 *     @BeforeClass   public static void clusterSetupAtBegining()
 *     @AfterClass    public static void ClusterShutdownAtEnd()
 * </p>
 */
public abstract class FileContextPermissionBase {  
  
  {
    try {
      ((org.apache.commons.logging.impl.Log4JLogger)FileSystem.LOG).getLogger()
      .setLevel(org.apache.log4j.Level.DEBUG);
    }
    catch(Exception e) {
      System.out.println("Cannot change log level\n"
          + StringUtils.stringifyException(e));
    }
  }
  
  protected static FileContext fc;

  @Before
  public void setUp() throws Exception {
    fc.mkdir(getTestRootPath(fc), FileContext.DEFAULT_PERM, true);
  }

  @After
  public void tearDown() throws Exception {
    fc.delete(getTestRootPath(fc), true);
  }
  
  private void cleanupFile(FileContext fc, Path name) throws IOException {
    Assert.assertTrue(exists(fc, name));
    fc.delete(name, true);
    Assert.assertTrue(!exists(fc, name));
  }

  @Test
  public void testCreatePermission() throws IOException {
    if (Path.WINDOWS) {
      System.out.println("Cannot run test for Windows");
      return;
    }
    String filename = "foo";
    Path f = getTestRootPath(fc, filename);
    createFile(fc, filename);
    doFilePermissionCheck(FileContext.FILE_DEFAULT_PERM.applyUMask(fc.getUMask()),
                        fc.getFileStatus(f).getPermission());
  }
  
  
  @Test
  public void testSetPermission() throws IOException {
    if (Path.WINDOWS) {
      System.out.println("Cannot run test for Windows");
      return;
    }

    String filename = "foo";
    Path f = getTestRootPath(fc, filename);
    createFile(fc, f);

    try {
      // create files and manipulate them.
      FsPermission all = new FsPermission((short)0777);
      FsPermission none = new FsPermission((short)0);

      fc.setPermission(f, none);
      doFilePermissionCheck(none, fc.getFileStatus(f).getPermission());

      fc.setPermission(f, all);
      doFilePermissionCheck(all, fc.getFileStatus(f).getPermission());
    }
    finally {cleanupFile(fc, f);}
  }

  @Test
  public void testSetOwner() throws IOException {
    if (Path.WINDOWS) {
      System.out.println("Cannot run test for Windows");
      return;
    }

    String filename = "bar";
    Path f = getTestRootPath(fc, filename);
    createFile(fc, f);
    List<String> groups = null;
    try {
      groups = getGroups();
      System.out.println(filename + ": " + fc.getFileStatus(f).getPermission());
    }
    catch(IOException e) {
      System.out.println(StringUtils.stringifyException(e));
      System.out.println("Cannot run test");
      return;
    }
    if (groups == null || groups.size() < 1) {
      System.out.println("Cannot run test: need at least one group.  groups="
                         + groups);
      return;
    }

    // create files and manipulate them.
    try {
      String g0 = groups.get(0);
      fc.setOwner(f, null, g0);
      Assert.assertEquals(g0, fc.getFileStatus(f).getGroup());

      if (groups.size() > 1) {
        String g1 = groups.get(1);
        fc.setOwner(f, null, g1);
        Assert.assertEquals(g1, fc.getFileStatus(f).getGroup());
      } else {
        System.out.println("Not testing changing the group since user " +
                           "belongs to only one group.");
      }
    } 
    finally {cleanupFile(fc, f);}
  }
  
  @Test
  public void testUgi() throws IOException, InterruptedException {
    
    UserGroupInformation otherUser = UserGroupInformation
        .createRemoteUser("otherUser");
    FileContext newFc = otherUser.doAs(new PrivilegedExceptionAction<FileContext>() {

      @Override
      public FileContext run() throws Exception {
        FileContext newFc = FileContext.getFileContext();
        return newFc;
      }
      
    });
    assertEquals("otherUser",newFc.getUgi().getUserName());
  }

  static List<String> getGroups() throws IOException {
    List<String> a = new ArrayList<String>();
    String s = Shell.execCommand(Shell.getGroupsCommand());
    for(StringTokenizer t = new StringTokenizer(s); t.hasMoreTokens(); ) {
      a.add(t.nextToken());
    }
    return a;
  }
  
  
  void doFilePermissionCheck(FsPermission expectedPerm, FsPermission actualPerm) {
  Assert.assertEquals(expectedPerm.applyUMask(getFileMask()), actualPerm);
  }
  
  
  /*
   * Some filesystem like HDFS ignore the "x" bit if the permission.
   * Others like localFs does not.
   * Override the method below if the file system being tested masks our
   * certain bits for file masks.
   */
  static final FsPermission FILE_MASK_ZERO = new FsPermission((short) 0);
  FsPermission getFileMask() {
    return FILE_MASK_ZERO;
  }
}
