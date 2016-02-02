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

package org.apache.hadoop.hdfs;

import static org.junit.Assert.assertEquals;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.net.URI;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Arrays;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileSystemTestHelper;
import org.apache.hadoop.fs.FsShell;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.Test;

/**
 * This test covers privilege related aspects of FsShell
 *
 */
public class TestFsShellPermission {

  static private final String TEST_ROOT = "/testroot";

  static UserGroupInformation createUGI(String ownername, String groupName) {
    return UserGroupInformation.createUserForTesting(ownername,
        new String[]{groupName});
  }

  private class FileEntry {
    private String path;
    private boolean isDir;
    private String owner;
    private String group;
    private String permission;
    public FileEntry(String path, boolean isDir,
        String owner, String group, String permission) {
      this.path = path;
      this.isDir = isDir;
      this.owner = owner;
      this.group = group;
      this.permission = permission;
    }
    String getPath() { return path; }
    boolean isDirectory() { return isDir; }
    String getOwner() { return owner; }
    String getGroup() { return group; }
    String getPermission() { return permission; }
  }

  private void createFiles(FileSystem fs, String topdir,
      FileEntry[] entries) throws IOException {
    for (FileEntry entry : entries) {
      String newPathStr = topdir + "/" + entry.getPath();
      Path newPath = new Path(newPathStr);
      if (entry.isDirectory()) {
        fs.mkdirs(newPath);
      } else {
        FileSystemTestHelper.createFile(fs,  newPath);
      }
      fs.setPermission(newPath, new FsPermission(entry.getPermission()));
      fs.setOwner(newPath, entry.getOwner(), entry.getGroup());
    }
  }

  /** delete directory and everything underneath it.*/
  private static void deldir(FileSystem fs, String topdir) throws IOException {
    fs.delete(new Path(topdir), true);
  }

  static String execCmd(FsShell shell, final String[] args) throws Exception {
    ByteArrayOutputStream baout = new ByteArrayOutputStream();
    PrintStream out = new PrintStream(baout, true);
    PrintStream old = System.out;
    int ret;
    try {
      System.setOut(out);
      ret = shell.run(args);
      out.close();
    } finally {
      System.setOut(old);
    }
    return String.valueOf(ret);
  }

  /*
   * Each instance of TestDeleteHelper captures one testing scenario.
   *
   * To create all files listed in fileEntries, and then delete as user
   * doAsuser the deleteEntry with command+options specified in cmdAndOptions.
   *
   * When expectedToDelete is true, the deleteEntry is expected to be deleted;
   * otherwise, it's not expected to be deleted. At the end of test,
   * the existence of deleteEntry is checked against expectedToDelete
   * to ensure the command is finished with expected result
   */
  private class TestDeleteHelper {
    private FileEntry[] fileEntries;
    private FileEntry deleteEntry;
    private String cmdAndOptions;
    private boolean expectedToDelete;

    final String doAsGroup;
    final UserGroupInformation userUgi;

    public TestDeleteHelper(
        FileEntry[] fileEntries,
        FileEntry deleteEntry,
        String cmdAndOptions,
        String doAsUser,
        boolean expectedToDelete) {
      this.fileEntries = fileEntries;
      this.deleteEntry = deleteEntry;
      this.cmdAndOptions = cmdAndOptions;
      this.expectedToDelete = expectedToDelete;

      doAsGroup = doAsUser.equals("hdfs")? "supergroup" : "users";
      userUgi = createUGI(doAsUser, doAsGroup);
    }

    public void execute(Configuration conf, FileSystem fs) throws Exception {
      fs.mkdirs(new Path(TEST_ROOT));

      createFiles(fs, TEST_ROOT, fileEntries);
      final FsShell fsShell = new FsShell(conf);
      final String deletePath =  TEST_ROOT + "/" + deleteEntry.getPath();

      String[] tmpCmdOpts = StringUtils.split(cmdAndOptions);
      ArrayList<String> tmpArray = new ArrayList<String>(Arrays.asList(tmpCmdOpts));
      tmpArray.add(deletePath);
      final String[] cmdOpts = tmpArray.toArray(new String[tmpArray.size()]);
      userUgi.doAs(new PrivilegedExceptionAction<String>() {
        public String run() throws Exception {
          return execCmd(fsShell, cmdOpts);
        }
      });

      boolean deleted = !fs.exists(new Path(deletePath));
      assertEquals(expectedToDelete, deleted);

      deldir(fs, TEST_ROOT);
    }
  }

  private TestDeleteHelper genDeleteEmptyDirHelper(final String cmdOpts,
      final String targetPerm,
      final String asUser,
      boolean expectedToDelete) {
    FileEntry[] files = {
        new FileEntry("userA", true, "userA", "users", "755"),
        new FileEntry("userA/userB", true, "userB", "users", targetPerm)
    };
    FileEntry deleteEntry = files[1];
    return new TestDeleteHelper(files, deleteEntry, cmdOpts, asUser,
        expectedToDelete);
  }

  // Expect target to be deleted
  private TestDeleteHelper genRmrEmptyDirWithReadPerm() {
    return genDeleteEmptyDirHelper("-rm -r", "744", "userA", true);
  }

  // Expect target to be deleted
  private TestDeleteHelper genRmrEmptyDirWithNoPerm() {
    return genDeleteEmptyDirHelper("-rm -r", "700", "userA", true);
  }

  // Expect target to be deleted
  private TestDeleteHelper genRmrfEmptyDirWithNoPerm() {
    return genDeleteEmptyDirHelper("-rm -r -f", "700", "userA", true);
  }

  private TestDeleteHelper genDeleteNonEmptyDirHelper(final String cmd,
      final String targetPerm,
      final String asUser,
      boolean expectedToDelete) {
    FileEntry[] files = {
        new FileEntry("userA", true, "userA", "users", "755"),
        new FileEntry("userA/userB", true, "userB", "users", targetPerm),
        new FileEntry("userA/userB/xyzfile", false, "userB", "users",
            targetPerm)
    };
    FileEntry deleteEntry = files[1];
    return new TestDeleteHelper(files, deleteEntry, cmd, asUser,
        expectedToDelete);
  }

  // Expect target not to be deleted
  private TestDeleteHelper genRmrNonEmptyDirWithReadPerm() {
    return genDeleteNonEmptyDirHelper("-rm -r", "744", "userA", false);
  }

  // Expect target not to be deleted
  private TestDeleteHelper genRmrNonEmptyDirWithNoPerm() {
    return genDeleteNonEmptyDirHelper("-rm -r", "700", "userA", false);
  }

  // Expect target to be deleted
  private TestDeleteHelper genRmrNonEmptyDirWithAllPerm() {
    return genDeleteNonEmptyDirHelper("-rm -r", "777", "userA", true);
  }

  // Expect target not to be deleted
  private TestDeleteHelper genRmrfNonEmptyDirWithNoPerm() {
    return genDeleteNonEmptyDirHelper("-rm -r -f", "700", "userA", false);
  }

  // Expect target to be deleted
  public TestDeleteHelper genDeleteSingleFileNotAsOwner() throws Exception {
    FileEntry[] files = {
        new FileEntry("userA", true, "userA", "users", "755"),
        new FileEntry("userA/userB", false, "userB", "users", "700")
    };
    FileEntry deleteEntry = files[1];
    return new TestDeleteHelper(files, deleteEntry, "-rm -r", "userA", true);
  }

  @Test
  public void testDelete() throws Exception {
    Configuration conf = null;
    MiniDFSCluster cluster = null;
    try {
      conf = new Configuration();
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(2).build();

      String nnUri = FileSystem.getDefaultUri(conf).toString();
      FileSystem fs = FileSystem.get(URI.create(nnUri), conf);

      ArrayList<TestDeleteHelper> ta = new ArrayList<TestDeleteHelper>();

      // Add empty dir tests
      ta.add(genRmrEmptyDirWithReadPerm());
      ta.add(genRmrEmptyDirWithNoPerm());
      ta.add(genRmrfEmptyDirWithNoPerm());

      // Add non-empty dir tests
      ta.add(genRmrNonEmptyDirWithReadPerm());
      ta.add(genRmrNonEmptyDirWithNoPerm());
      ta.add(genRmrNonEmptyDirWithAllPerm());
      ta.add(genRmrfNonEmptyDirWithNoPerm());

      // Add single tile test
      ta.add(genDeleteSingleFileNotAsOwner());

      // Run all tests
      for(TestDeleteHelper t : ta) {
        t.execute(conf,  fs);
      }
    } finally {
      if (cluster != null) { cluster.shutdown(); }
    }
  }
}
