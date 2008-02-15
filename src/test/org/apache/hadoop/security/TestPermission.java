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
package org.apache.hadoop.security;

import java.io.IOException;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.impl.Log4JLogger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.dfs.MiniDFSCluster;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.permission.*;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.log4j.Level;

import junit.framework.TestCase;

/** Unit tests for permission */
public class TestPermission extends TestCase {
  public static final Log LOG = LogFactory.getLog(TestPermission.class);

  {
    ((Log4JLogger)UserGroupInformation.LOG).getLogger().setLevel(Level.ALL);
  }

  final private static Path ROOT_PATH = new Path("/data");
  final private static Path CHILD_DIR1 = new Path(ROOT_PATH, "web1");
  final private static Path CHILD_DIR2 = new Path(ROOT_PATH, "web2");
  final private static Path CHILD_FILE1 = new Path(ROOT_PATH, "file1");
  final private static Path CHILD_FILE2 = new Path(ROOT_PATH, "file2");

  final private static int FILE_LEN = 100;
  final private static String PERMISSION_EXCEPTION_NAME =
    AccessControlException.class.getName();

  final private static String USER_NAME = "Who";
  final private static String GROUP1_NAME = "group1";
  final private static String GROUP2_NAME = "group2";
  final private static String[] GROUP_NAMES = {GROUP1_NAME, GROUP2_NAME};

  static FsPermission checkPermission(FileSystem fs,
      String path, FsPermission expected) throws IOException {
    FileStatus s = fs.getFileStatus(new Path(path));
    LOG.info(s.getPath() + ": " + s.isDir() + " " + s.getPermission()
        + ":" + s.getOwner() + ":" + s.getGroup());
    if (expected != null) {
      assertEquals(expected, s.getPermission());
      assertEquals(expected.toShort(), s.getPermission().toShort());
    }
    return s.getPermission();
  }

  public void testCreate() throws Exception {
    Configuration conf = new Configuration();
    conf.setBoolean("dfs.permissions", true);
    conf.setInt(FsPermission.UMASK_LABEL, 0);
    MiniDFSCluster cluster = new MiniDFSCluster(conf, 3, true, null);
    cluster.waitActive();
    FileSystem fs = FileSystem.get(conf);

    try {
      FsPermission rootPerm = checkPermission(fs, "/", null);
      FsPermission inheritPerm = FsPermission.createImmutable(
          (short)(rootPerm.toShort() | 0300));

      FsPermission dirPerm = new FsPermission((short)0777);
      fs.mkdirs(new Path("/a1/a2/a3"), dirPerm);
      checkPermission(fs, "/a1", inheritPerm);
      checkPermission(fs, "/a1/a2", inheritPerm);
      checkPermission(fs, "/a1/a2/a3", dirPerm);

      FsPermission filePerm = new FsPermission((short)0444);
      FSDataOutputStream out = fs.create(new Path("/b1/b2/b3.txt"), filePerm,
          true, conf.getInt("io.file.buffer.size", 4096),
          fs.getDefaultReplication(), fs.getDefaultBlockSize(), null);
      out.write(123);
      out.close();
      checkPermission(fs, "/b1", inheritPerm);
      checkPermission(fs, "/b1/b2", inheritPerm);
      checkPermission(fs, "/b1/b2/b3.txt", filePerm);
      
      conf.setInt(FsPermission.UMASK_LABEL, 0022);
      FsPermission permission = 
        FsPermission.createImmutable((short)0666);
      FileSystem.mkdirs(fs, new Path("/c1"), new FsPermission(permission));
      FileSystem.create(fs, new Path("/c1/c2.txt"),
          new FsPermission(permission));
      checkPermission(fs, "/c1", permission);
      checkPermission(fs, "/c1/c2.txt", permission);
    }
    finally {
      try{fs.close();} catch(Exception e) {}
      try{cluster.shutdown();} catch(Exception e) {}
    }
  }

  public void testFilePermision() throws Exception {
    Configuration conf = new Configuration();
    conf.setBoolean("dfs.permissions", true);
    MiniDFSCluster cluster = new MiniDFSCluster(conf, 3, true, null);
    cluster.waitActive();
    FileSystem fs = FileSystem.get(conf);

    try {
      // following dir/file creations are legal
      fs.mkdirs(CHILD_DIR1);
      FSDataOutputStream out = fs.create(CHILD_FILE1);
      byte data[] = new byte[FILE_LEN];
      Random r = new Random();
      r.nextBytes(data);
      out.write(data);
      out.close();
      fs.setPermission(CHILD_FILE1, new FsPermission((short)0700));

      // following read is legal
      byte dataIn[] = new byte[FILE_LEN];
      FSDataInputStream fin = fs.open(CHILD_FILE1);
      fin.read(dataIn);
      for(int i=0; i<FILE_LEN; i++) {
        assertEquals(data[i], dataIn[i]);
      }
      fs.close();

      // test illegal file/dir creation
      UnixUserGroupInformation userGroupInfo = new UnixUserGroupInformation(
          USER_NAME, GROUP_NAMES );
      conf.set(UnixUserGroupInformation.UGI_PROPERTY_NAME,
          userGroupInfo.toString());
      fs = FileSystem.get(conf);

      // illegal mkdir
      assertTrue(!canMkdirs(fs, CHILD_DIR2));

      // illegal file creation
      assertTrue(!canCreate(fs, CHILD_FILE2));

      // illegal file open
      assertTrue(!canOpen(fs, CHILD_FILE1));
    }
    finally {
      try{fs.close();} catch(Exception e) {}
      try{cluster.shutdown();} catch(Exception e) {}
    }
  }

  static boolean canMkdirs(FileSystem fs, Path p) throws IOException {
    try {
      fs.mkdirs(p);
      return true;
    } catch(RemoteException e) {
      assertEquals(PERMISSION_EXCEPTION_NAME, e.getClassName());
      return false;
    }
  }

  static boolean canCreate(FileSystem fs, Path p) throws IOException {
    try {
      fs.create(p);
      return true;
    } catch(RemoteException e) {
      assertEquals(PERMISSION_EXCEPTION_NAME, e.getClassName());
      return false;
    }
  }

  static boolean canOpen(FileSystem fs, Path p) throws IOException {
    try {
      fs.open(p);
      return true;
    } catch(RemoteException e) {
      assertEquals(PERMISSION_EXCEPTION_NAME, e.getClassName());
      return false;
    }
  }
}
