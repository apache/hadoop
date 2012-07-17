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
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assume.*;
import static org.junit.Assert.*;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.NativeCodeLoader;

public class TestNativeIO {
  static final Log LOG = LogFactory.getLog(TestNativeIO.class);

  static final File TEST_DIR = new File(
    System.getProperty("test.build.data"), "testnativeio");

  @Before
  public void checkLoaded() {
    assumeTrue(NativeCodeLoader.isNativeCodeLoaded());
  }

  @Before
  public void setupTestDir() throws IOException {
    FileUtil.fullyDelete(TEST_DIR);
    TEST_DIR.mkdirs();
  }

  @Test
  public void testFstat() throws Exception {
    FileOutputStream fos = new FileOutputStream(
      new File(TEST_DIR, "testfstat"));
    NativeIO.Stat stat = NativeIO.fstat(fos.getFD());
    fos.close();
    LOG.info("Stat: " + String.valueOf(stat));

    assertEquals(System.getProperty("user.name"), stat.getOwner());
    assertEquals(NativeIO.Stat.S_IFREG, stat.getMode() & NativeIO.Stat.S_IFMT);
  }
  
  @Test
  public void testGetOwner() throws Exception {
    FileOutputStream fos = new FileOutputStream(
      new File(TEST_DIR, "testfstat"));
    String owner = NativeIO.getOwner(fos.getFD());
    fos.close();
    LOG.info("Owner: " + owner);

    assertEquals(System.getProperty("user.name"), owner);
  }

  @Test
  public void testFstatClosedFd() throws Exception {
    FileOutputStream fos = new FileOutputStream(
      new File(TEST_DIR, "testfstat2"));
    fos.close();
    try {
      NativeIO.Stat stat = NativeIO.fstat(fos.getFD());
    } catch (IOException e) {
      LOG.info("Got expected exception", e);
    }
  }

  @Test
  public void testOpen() throws Exception {
    LOG.info("Open a missing file without O_CREAT and it should fail");
    try {
      FileDescriptor fd = NativeIO.open(
        new File(TEST_DIR, "doesntexist").getAbsolutePath(),
        NativeIO.O_WRONLY, 0700);
      fail("Able to open a new file without O_CREAT");
    } catch (IOException ioe) {
      // expected
    }

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
    } catch (IOException ioe) {
      // expected
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
    } finally {
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

}
