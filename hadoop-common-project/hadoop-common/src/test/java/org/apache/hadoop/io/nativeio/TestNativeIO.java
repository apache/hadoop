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
package org.apache.hadoop.io.nativeio;

import java.io.File;
import java.io.FileDescriptor;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.ArrayList;
import java.util.List;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assume.*;
import static org.junit.Assert.*;

import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.NativeCodeLoader;
import org.apache.hadoop.util.Time;

public class TestNativeIO {
  static final Log LOG = LogFactory.getLog(TestNativeIO.class);

  static final File TEST_DIR = new File(
    System.getProperty("test.build.data"), "testnativeio");

  @Before
  public void checkLoaded() {
    assumeTrue(NativeCodeLoader.isNativeCodeLoaded());
  }

  @Before
  public void setupTestDir() {
    FileUtil.fullyDelete(TEST_DIR);
    TEST_DIR.mkdirs();
  }

  @Test
  public void testFstat() throws Exception {
    FileOutputStream fos = new FileOutputStream(
      new File(TEST_DIR, "testfstat"));
    NativeIO.Stat stat = NativeIO.getFstat(fos.getFD());
    fos.close();
    LOG.info("Stat: " + String.valueOf(stat));

    assertEquals(System.getProperty("user.name"), stat.getOwner());
    assertNotNull(stat.getGroup());
    assertTrue(!stat.getGroup().isEmpty());
    assertEquals("Stat mode field should indicate a regular file",
      NativeIO.Stat.S_IFREG, stat.getMode() & NativeIO.Stat.S_IFMT);
  }

  /**
   * Test for races in fstat usage
   *
   * NOTE: this test is likely to fail on RHEL 6.0 which has a non-threadsafe
   * implementation of getpwuid_r.
   */
  @Test
  public void testMultiThreadedFstat() throws Exception {
    final FileOutputStream fos = new FileOutputStream(
      new File(TEST_DIR, "testfstat"));

    final AtomicReference<Throwable> thrown =
      new AtomicReference<Throwable>();
    List<Thread> statters = new ArrayList<Thread>();
    for (int i = 0; i < 10; i++) {
      Thread statter = new Thread() {
        @Override
        public void run() {
          long et = Time.now() + 5000;
          while (Time.now() < et) {
            try {
              NativeIO.Stat stat = NativeIO.getFstat(fos.getFD());
              assertEquals(System.getProperty("user.name"), stat.getOwner());
              assertNotNull(stat.getGroup());
              assertTrue(!stat.getGroup().isEmpty());
              assertEquals("Stat mode field should indicate a regular file",
                NativeIO.Stat.S_IFREG, stat.getMode() & NativeIO.Stat.S_IFMT);
            } catch (Throwable t) {
              thrown.set(t);
            }
          }
        }
      };
      statters.add(statter);
      statter.start();
    }
    for (Thread t : statters) {
      t.join();
    }

    fos.close();
    
    if (thrown.get() != null) {
      throw new RuntimeException(thrown.get());
    }
  }

  @Test
  public void testFstatClosedFd() throws Exception {
    FileOutputStream fos = new FileOutputStream(
      new File(TEST_DIR, "testfstat2"));
    fos.close();
    try {
      NativeIO.Stat stat = NativeIO.getFstat(fos.getFD());
    } catch (NativeIOException nioe) {
      LOG.info("Got expected exception", nioe);
      assertEquals(Errno.EBADF, nioe.getErrno());
    }
  }

  @Test
  public void testOpenMissingWithoutCreate() throws Exception {
    LOG.info("Open a missing file without O_CREAT and it should fail");
    try {
      FileDescriptor fd = NativeIO.open(
        new File(TEST_DIR, "doesntexist").getAbsolutePath(),
        NativeIO.O_WRONLY, 0700);
      fail("Able to open a new file without O_CREAT");
    } catch (NativeIOException nioe) {
      LOG.info("Got expected exception", nioe);
      assertEquals(Errno.ENOENT, nioe.getErrno());
    }
  }

  @Test
  public void testOpenWithCreate() throws Exception {
    LOG.info("Test creating a file with O_CREAT");
    FileDescriptor fd = NativeIO.open(
      new File(TEST_DIR, "testWorkingOpen").getAbsolutePath(),
      NativeIO.O_WRONLY | NativeIO.O_CREAT, 0700);
    assertNotNull(true);
    assertTrue(fd.valid());
    FileOutputStream fos = new FileOutputStream(fd);
    fos.write("foo".getBytes());
    fos.close();

    assertFalse(fd.valid());

    LOG.info("Test exclusive create");
    try {
      fd = NativeIO.open(
        new File(TEST_DIR, "testWorkingOpen").getAbsolutePath(),
        NativeIO.O_WRONLY | NativeIO.O_CREAT | NativeIO.O_EXCL, 0700);
      fail("Was able to create existing file with O_EXCL");
    } catch (NativeIOException nioe) {
      LOG.info("Got expected exception for failed exclusive create", nioe);
      assertEquals(Errno.EEXIST, nioe.getErrno());
    }
  }

  /**
   * Test that opens and closes a file 10000 times - this would crash with
   * "Too many open files" if we leaked fds using this access pattern.
   */
  @Test
  public void testFDDoesntLeak() throws IOException {
    for (int i = 0; i < 10000; i++) {
      FileDescriptor fd = NativeIO.open(
        new File(TEST_DIR, "testNoFdLeak").getAbsolutePath(),
        NativeIO.O_WRONLY | NativeIO.O_CREAT, 0700);
      assertNotNull(true);
      assertTrue(fd.valid());
      FileOutputStream fos = new FileOutputStream(fd);
      fos.write("foo".getBytes());
      fos.close();
    }
  }

  /**
   * Test basic chmod operation
   */
  @Test
  public void testChmod() throws Exception {
    try {
      NativeIO.chmod("/this/file/doesnt/exist", 777);
      fail("Chmod of non-existent file didn't fail");
    } catch (NativeIOException nioe) {
      assertEquals(Errno.ENOENT, nioe.getErrno());
    }

    File toChmod = new File(TEST_DIR, "testChmod");
    assertTrue("Create test subject",
               toChmod.exists() || toChmod.mkdir());
    NativeIO.chmod(toChmod.getAbsolutePath(), 0777);
    assertPermissions(toChmod, 0777);
    NativeIO.chmod(toChmod.getAbsolutePath(), 0000);
    assertPermissions(toChmod, 0000);
    NativeIO.chmod(toChmod.getAbsolutePath(), 0644);
    assertPermissions(toChmod, 0644);
  }


  @Test
  public void testPosixFadvise() throws Exception {
    FileInputStream fis = new FileInputStream("/dev/zero");
    try {
      NativeIO.posix_fadvise(fis.getFD(), 0, 0,
                             NativeIO.POSIX_FADV_SEQUENTIAL);
    } catch (UnsupportedOperationException uoe) {
      // we should just skip the unit test on machines where we don't
      // have fadvise support
      assumeTrue(false);
    } catch (NativeIOException nioe) {
      // ignore this error as FreeBSD returns EBADF even if length is zero
    }
      finally {
      fis.close();
    }

    try {
      NativeIO.posix_fadvise(fis.getFD(), 0, 1024,
                             NativeIO.POSIX_FADV_SEQUENTIAL);

      fail("Did not throw on bad file");
    } catch (NativeIOException nioe) {
      assertEquals(Errno.EBADF, nioe.getErrno());
    }
    
    try {
      NativeIO.posix_fadvise(null, 0, 1024,
                             NativeIO.POSIX_FADV_SEQUENTIAL);

      fail("Did not throw on null file");
    } catch (NullPointerException npe) {
      // expected
    }
  }

  @Test
  public void testSyncFileRange() throws Exception {
    FileOutputStream fos = new FileOutputStream(
      new File(TEST_DIR, "testSyncFileRange"));
    try {
      fos.write("foo".getBytes());
      NativeIO.sync_file_range(fos.getFD(), 0, 1024,
                               NativeIO.SYNC_FILE_RANGE_WRITE);
      // no way to verify that this actually has synced,
      // but if it doesn't throw, we can assume it worked
    } catch (UnsupportedOperationException uoe) {
      // we should just skip the unit test on machines where we don't
      // have fadvise support
      assumeTrue(false);
    } finally {
      fos.close();
    }
    try {
      NativeIO.sync_file_range(fos.getFD(), 0, 1024,
                               NativeIO.SYNC_FILE_RANGE_WRITE);
      fail("Did not throw on bad file");
    } catch (NativeIOException nioe) {
      assertEquals(Errno.EBADF, nioe.getErrno());
    }
  }

  private void assertPermissions(File f, int expected) throws IOException {
    FileSystem localfs = FileSystem.getLocal(new Configuration());
    FsPermission perms = localfs.getFileStatus(
      new Path(f.getAbsolutePath())).getPermission();
    assertEquals(expected, perms.toShort());
  }

  @Test
  public void testGetUserName() throws IOException {
    assertFalse(NativeIO.getUserName(0).isEmpty());
  }

  @Test
  public void testGetGroupName() throws IOException {
    assertFalse(NativeIO.getGroupName(0).isEmpty());
  }

  @Test
  public void testRenameTo() throws Exception {
    final File TEST_DIR = new File(new File(
        System.getProperty("test.build.data","build/test/data")), "renameTest");
    assumeTrue(TEST_DIR.mkdirs());
    File nonExistentFile = new File(TEST_DIR, "nonexistent");
    File targetFile = new File(TEST_DIR, "target");
    // Test attempting to rename a nonexistent file.
    try {
      NativeIO.renameTo(nonExistentFile, targetFile);
      Assert.fail();
    } catch (NativeIOException e) {
      Assert.assertEquals(e.getErrno(), Errno.ENOENT);
    }
    
    // Test renaming a file to itself.  It should succeed and do nothing.
    File sourceFile = new File(TEST_DIR, "source");
    Assert.assertTrue(sourceFile.createNewFile());
    NativeIO.renameTo(sourceFile, sourceFile);

    // Test renaming a source to a destination.
    NativeIO.renameTo(sourceFile, targetFile);

    // Test renaming a source to a path which uses a file as a directory.
    sourceFile = new File(TEST_DIR, "source");
    Assert.assertTrue(sourceFile.createNewFile());
    File badTarget = new File(targetFile, "subdir");
    try {
      NativeIO.renameTo(sourceFile, badTarget);
      Assert.fail();
    } catch (NativeIOException e) {
      Assert.assertEquals(e.getErrno(), Errno.ENOTDIR);
    }

    FileUtils.deleteQuietly(TEST_DIR);
  }
}
