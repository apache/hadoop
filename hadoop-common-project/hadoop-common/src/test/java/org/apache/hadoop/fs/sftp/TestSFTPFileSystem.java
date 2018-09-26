/*
 * Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package org.apache.hadoop.fs.sftp;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.test.GenericTestUtils;

import org.apache.sshd.server.SshServer;
import org.apache.sshd.common.NamedFactory;
import org.apache.sshd.server.Command;
import org.apache.sshd.server.auth.password.PasswordAuthenticator;
import org.apache.sshd.server.auth.UserAuth;
import org.apache.sshd.server.auth.password.UserAuthPasswordFactory;
import org.apache.sshd.server.keyprovider.SimpleGeneratorHostKeyProvider;
import org.apache.sshd.server.session.ServerSession;

import org.apache.sshd.server.subsystem.sftp.SftpSubsystemFactory;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import static org.apache.hadoop.test.PlatformAssumptions.assumeNotWindows;
import static org.junit.Assert.*;

public class TestSFTPFileSystem {

  private static final String TEST_SFTP_DIR = "testsftp";
  private static final String TEST_ROOT_DIR =
      GenericTestUtils.getTestDir().getAbsolutePath();

  @Rule public TestName name = new TestName();

  private static final String connection = "sftp://user:password@localhost";
  private static Path localDir = null;
  private static FileSystem localFs = null;
  private static FileSystem sftpFs = null;
  private static SshServer sshd = null;
  private static int port;

  private static void startSshdServer() throws IOException {
    sshd = SshServer.setUpDefaultServer();
    // ask OS to assign a port
    sshd.setPort(0);
    sshd.setKeyPairProvider(new SimpleGeneratorHostKeyProvider());

    List<NamedFactory<UserAuth>> userAuthFactories =
        new ArrayList<NamedFactory<UserAuth>>();
    userAuthFactories.add(new UserAuthPasswordFactory());

    sshd.setUserAuthFactories(userAuthFactories);

    sshd.setPasswordAuthenticator(new PasswordAuthenticator() {
      @Override
      public boolean authenticate(String username, String password,
          ServerSession session) {
        if (username.equals("user") && password.equals("password")) {
          return true;
        }
        return false;
      }
    });

    sshd.setSubsystemFactories(
        Arrays.<NamedFactory<Command>>asList(new SftpSubsystemFactory()));

    sshd.start();
    port = sshd.getPort();
  }

  @BeforeClass
  public static void setUp() throws Exception {
    // skip all tests if running on Windows
    assumeNotWindows();

    startSshdServer();

    Configuration conf = new Configuration();
    conf.setClass("fs.sftp.impl", SFTPFileSystem.class, FileSystem.class);
    conf.setInt("fs.sftp.host.port", port);
    conf.setBoolean("fs.sftp.impl.disable.cache", true);

    localFs = FileSystem.getLocal(conf);
    localDir = localFs.makeQualified(new Path(TEST_ROOT_DIR, TEST_SFTP_DIR));
    if (localFs.exists(localDir)) {
      localFs.delete(localDir, true);
    }
    localFs.mkdirs(localDir);

    sftpFs = FileSystem.get(URI.create(connection), conf);
  }

  @AfterClass
  public static void tearDown() {
    if (localFs != null) {
      try {
        localFs.delete(localDir, true);
        localFs.close();
      } catch (IOException e) {
        // ignore
      }
    }
    if (sftpFs != null) {
      try {
        sftpFs.close();
      } catch (IOException e) {
        // ignore
      }
    }
    if (sshd != null) {
      try {
        sshd.stop(true);
      } catch (IOException e) {
        // ignore
      }
    }
  }

  private static final Path touch(FileSystem fs, String filename)
      throws IOException {
    return touch(fs, filename, null);
  }

  private static final Path touch(FileSystem fs, String filename, byte[] data)
      throws IOException {
    Path lPath = new Path(localDir.toUri().getPath(), filename);
    FSDataOutputStream out = null;
    try {
      out = fs.create(lPath);
      if (data != null) {
        out.write(data);
      }
    } finally {
      if (out != null) {
        out.close();
      }
    }
    return lPath;
  }

  /**
   * Creates a file and deletes it.
   *
   * @throws Exception
   */
  @Test
  public void testCreateFile() throws Exception {
    Path file = touch(sftpFs, name.getMethodName().toLowerCase());
    assertTrue(localFs.exists(file));
    assertTrue(sftpFs.delete(file, false));
    assertFalse(localFs.exists(file));
  }

  /**
   * Checks if a new created file exists.
   *
   * @throws Exception
   */
  @Test
  public void testFileExists() throws Exception {
    Path file = touch(localFs, name.getMethodName().toLowerCase());
    assertTrue(sftpFs.exists(file));
    assertTrue(localFs.exists(file));
    assertTrue(sftpFs.delete(file, false));
    assertFalse(sftpFs.exists(file));
    assertFalse(localFs.exists(file));
  }

  /**
   * Test writing to a file and reading its value.
   *
   * @throws Exception
   */
  @Test
  public void testReadFile() throws Exception {
    byte[] data = "yaks".getBytes();
    Path file = touch(localFs, name.getMethodName().toLowerCase(), data);
    FSDataInputStream is = null;
    try {
      is = sftpFs.open(file);
      byte[] b = new byte[data.length];
      is.read(b);
      assertArrayEquals(data, b);
    } finally {
      if (is != null) {
        is.close();
      }
    }
    assertTrue(sftpFs.delete(file, false));
  }

  /**
   * Test getting the status of a file.
   *
   * @throws Exception
   */
  @Test
  public void testStatFile() throws Exception {
    byte[] data = "yaks".getBytes();
    Path file = touch(localFs, name.getMethodName().toLowerCase(), data);

    FileStatus lstat = localFs.getFileStatus(file);
    FileStatus sstat = sftpFs.getFileStatus(file);
    assertNotNull(sstat);

    assertEquals(lstat.getPath().toUri().getPath(),
                 sstat.getPath().toUri().getPath());
    assertEquals(data.length, sstat.getLen());
    assertEquals(lstat.getLen(), sstat.getLen());
    assertTrue(sftpFs.delete(file, false));
  }

  /**
   * Test deleting a non empty directory.
   *
   * @throws Exception
   */
  @Test(expected=java.io.IOException.class)
  public void testDeleteNonEmptyDir() throws Exception {
    Path file = touch(localFs, name.getMethodName().toLowerCase());
    sftpFs.delete(localDir, false);
  }

  /**
   * Test deleting a file that does not exist.
   *
   * @throws Exception
   */
  @Test
  public void testDeleteNonExistFile() throws Exception {
    Path file = new Path(localDir, name.getMethodName().toLowerCase());
    assertFalse(sftpFs.delete(file, false));
  }

  /**
   * Test renaming a file.
   *
   * @throws Exception
   */
  @Test
  public void testRenameFile() throws Exception {
    byte[] data = "dingos".getBytes();
    Path file1 = touch(localFs, name.getMethodName().toLowerCase() + "1");
    Path file2 = new Path(localDir, name.getMethodName().toLowerCase() + "2");

    assertTrue(sftpFs.rename(file1, file2));

    assertTrue(sftpFs.exists(file2));
    assertFalse(sftpFs.exists(file1));

    assertTrue(localFs.exists(file2));
    assertFalse(localFs.exists(file1));

    assertTrue(sftpFs.delete(file2, false));
  }

  /**
   * Test renaming a file that does not exist.
   *
   * @throws Exception
   */
  @Test(expected=java.io.IOException.class)
  public void testRenameNonExistFile() throws Exception {
    Path file1 = new Path(localDir, name.getMethodName().toLowerCase() + "1");
    Path file2 = new Path(localDir, name.getMethodName().toLowerCase() + "2");
    sftpFs.rename(file1, file2);
  }

  /**
   * Test renaming a file onto an existing file.
   *
   * @throws Exception
   */
  @Test(expected=java.io.IOException.class)
  public void testRenamingFileOntoExistingFile() throws Exception {
    Path file1 = touch(localFs, name.getMethodName().toLowerCase() + "1");
    Path file2 = touch(localFs, name.getMethodName().toLowerCase() + "2");
    sftpFs.rename(file1, file2);
  }

  @Test
  public void testGetAccessTime() throws IOException {
    Path file = touch(localFs, name.getMethodName().toLowerCase());
    LocalFileSystem local = (LocalFileSystem)localFs;
    java.nio.file.Path path = (local).pathToFile(file).toPath();
    long accessTime1 = Files.readAttributes(path, BasicFileAttributes.class)
        .lastAccessTime().toMillis();
    accessTime1 = (accessTime1 / 1000) * 1000;
    long accessTime2 = sftpFs.getFileStatus(file).getAccessTime();
    assertEquals(accessTime1, accessTime2);
  }

  @Test
  public void testGetModifyTime() throws IOException {
    Path file = touch(localFs, name.getMethodName().toLowerCase() + "1");
    java.io.File localFile = ((LocalFileSystem) localFs).pathToFile(file);
    long modifyTime1 = localFile.lastModified();
    long modifyTime2 = sftpFs.getFileStatus(file).getModificationTime();
    assertEquals(modifyTime1, modifyTime2);
  }

}
