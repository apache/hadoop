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
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.util.Random;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.commons.io.FileUtils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathIOException;
import org.apache.hadoop.fs.contract.ContractTestUtils;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.test.LambdaTestUtils;
import org.apache.hadoop.test.StatUtils;
import org.apache.hadoop.util.NativeCodeLoader;
import org.apache.hadoop.util.Time;
import static org.apache.hadoop.io.nativeio.NativeIO.POSIX.*;
import static org.apache.hadoop.io.nativeio.NativeIO.POSIX.Stat.*;
import static org.apache.hadoop.test.PlatformAssumptions.assumeNotWindows;
import static org.apache.hadoop.test.PlatformAssumptions.assumeWindows;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assume.*;
import static org.junit.Assert.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestNativeIO {
  static final Logger LOG = LoggerFactory.getLogger(TestNativeIO.class);

  static final File TEST_DIR = GenericTestUtils.getTestDir("testnativeio");

  @Before
  public void checkLoaded() {
    assumeTrue(NativeCodeLoader.isNativeCodeLoaded());
  }

  @Before
  public void setupTestDir() {
    FileUtil.fullyDelete(TEST_DIR);
    TEST_DIR.mkdirs();
  }

  @Test (timeout = 30000)
  public void testFstat() throws Exception {
    FileOutputStream fos = new FileOutputStream(
      new File(TEST_DIR, "testfstat"));
    NativeIO.POSIX.Stat stat = NativeIO.POSIX.getFstat(fos.getFD());
    fos.close();
    LOG.info("Stat: " + String.valueOf(stat));

    String owner = stat.getOwner();
    String expectedOwner = System.getProperty("user.name");
    if (Path.WINDOWS) {
      UserGroupInformation ugi =
          UserGroupInformation.createRemoteUser(expectedOwner);
      final String adminsGroupString = "Administrators";
      if (Arrays.asList(ugi.getGroupNames()).contains(adminsGroupString)) {
        expectedOwner = adminsGroupString;
      }
    }
    assertEquals(expectedOwner, owner);
    assertNotNull(stat.getGroup());
    assertTrue(!stat.getGroup().isEmpty());
    assertEquals("Stat mode field should indicate a regular file", S_IFREG,
      stat.getMode() & S_IFMT);
  }

  /**
   * Test for races in fstat usage
   *
   * NOTE: this test is likely to fail on RHEL 6.0 which has a non-threadsafe
   * implementation of getpwuid_r.
   */
  @Test (timeout = 30000)
  public void testMultiThreadedFstat() throws Exception {
    assumeNotWindows();

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
              NativeIO.POSIX.Stat stat = NativeIO.POSIX.getFstat(fos.getFD());
              assertEquals(System.getProperty("user.name"), stat.getOwner());
              assertNotNull(stat.getGroup());
              assertTrue(!stat.getGroup().isEmpty());
              assertEquals("Stat mode field should indicate a regular file",
                S_IFREG, stat.getMode() & S_IFMT);
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

  @Test (timeout = 30000)
  public void testFstatClosedFd() throws Exception {
    FileOutputStream fos = new FileOutputStream(
      new File(TEST_DIR, "testfstat2"));
    fos.close();
    try {
      NativeIO.POSIX.Stat stat = NativeIO.POSIX.getFstat(fos.getFD());
    } catch (NativeIOException nioe) {
      LOG.info("Got expected exception", nioe);
      assertEquals(Errno.EBADF, nioe.getErrno());
    }
  }

  @Test (timeout = 30000)
  public void testStat() throws Exception {
    Configuration conf = new Configuration();
    FileSystem fileSystem = FileSystem.getLocal(conf).getRawFileSystem();
    Path path = new Path(TEST_DIR.getPath(), "teststat2");
    fileSystem.createNewFile(path);
    String testFilePath = path.toString();

    try {
      doStatTest(testFilePath);
      LOG.info("testStat() is successful.");
    } finally {
      ContractTestUtils.cleanup("cleanup test file: " + path.toString(),
          fileSystem, path);
    }
  }

  private boolean doStatTest(String testFilePath) throws Exception {
    NativeIO.POSIX.Stat stat = NativeIO.POSIX.getStat(testFilePath);
    String owner = stat.getOwner();
    String group = stat.getGroup();
    int mode = stat.getMode();

    // direct check with System
    String expectedOwner = System.getProperty("user.name");
    assertEquals(expectedOwner, owner);
    assertNotNull(group);
    assertTrue(!group.isEmpty());

    // cross check with ProcessBuilder
    StatUtils.Permission expected =
        StatUtils.getPermissionFromProcess(testFilePath);
    StatUtils.Permission permission =
        new StatUtils.Permission(owner, group, new FsPermission(mode));

    assertEquals(expected.getOwner(), permission.getOwner());
    assertEquals(expected.getGroup(), permission.getGroup());
    assertEquals(expected.getFsPermission(), permission.getFsPermission());

    LOG.info("Load permission test is successful for path: {}, stat: {}",
        testFilePath, stat);
    LOG.info("On mask, stat is owner: {}, group: {}, permission: {}",
        owner, group, permission.getFsPermission().toOctal());
    return true;
  }

  @Test
  public void testStatOnError() throws Exception {
    final String testNullFilePath = null;
    LambdaTestUtils.intercept(IOException.class,
            "Path is null",
            () -> NativeIO.POSIX.getStat(testNullFilePath));

    final String testInvalidFilePath = "C:\\nonexisting_path\\nonexisting_file";
    LambdaTestUtils.intercept(IOException.class,
            PathIOException.class.getName(),
            () -> NativeIO.POSIX.getStat(testInvalidFilePath));
  }

  @Test (timeout = 30000)
  public void testMultiThreadedStat() throws Exception {
    Configuration conf = new Configuration();
    FileSystem fileSystem = FileSystem.getLocal(conf).getRawFileSystem();
    Path path = new Path(TEST_DIR.getPath(), "teststat2");
    fileSystem.createNewFile(path);
    String testFilePath = path.toString();

    int numOfThreads = 10;
    ExecutorService executorService =
        Executors.newFixedThreadPool(numOfThreads);
    executorService.awaitTermination(1000, TimeUnit.MILLISECONDS);
    try {
      for (int i = 0; i < numOfThreads; i++){
        Future<Boolean> result =
            executorService.submit(() -> doStatTest(testFilePath));
        assertTrue(result.get());
      }
      LOG.info("testMultiThreadedStat() is successful.");
    } finally {
      executorService.shutdown();
      ContractTestUtils.cleanup("cleanup test file: " + path.toString(),
          fileSystem, path);
    }
  }

  @Test
  public void testMultiThreadedStatOnError() throws Exception {
    final String testInvalidFilePath = "C:\\nonexisting_path\\nonexisting_file";

    int numOfThreads = 10;
    ExecutorService executorService =
        Executors.newFixedThreadPool(numOfThreads);
    for (int i = 0; i < numOfThreads; i++) {
      try {
        Future<Boolean> result =
            executorService.submit(() -> doStatTest(testInvalidFilePath));
        result.get();
      } catch (Exception e) {
        assertTrue(e.getCause() instanceof PathIOException);
      }
    }
    executorService.shutdown();
  }

  @Test (timeout = 30000)
  public void testSetFilePointer() throws Exception {
    assumeWindows();

    LOG.info("Set a file pointer on Windows");
    try {
      File testfile = new File(TEST_DIR, "testSetFilePointer");
      assertTrue("Create test subject",
          testfile.exists() || testfile.createNewFile());
      FileWriter writer = new FileWriter(testfile);
      try {
        for (int i = 0; i < 200; i++)
          if (i < 100)
            writer.write('a');
          else
            writer.write('b');
        writer.flush();
      } catch (Exception writerException) {
        fail("Got unexpected exception: " + writerException.getMessage());
      } finally {
        writer.close();
      }

      FileDescriptor fd = NativeIO.Windows.createFile(
          testfile.getCanonicalPath(),
          NativeIO.Windows.GENERIC_READ,
          NativeIO.Windows.FILE_SHARE_READ |
          NativeIO.Windows.FILE_SHARE_WRITE |
          NativeIO.Windows.FILE_SHARE_DELETE,
          NativeIO.Windows.OPEN_EXISTING);
      NativeIO.Windows.setFilePointer(fd, 120, NativeIO.Windows.FILE_BEGIN);
      FileReader reader = new FileReader(fd);
      try {
        int c = reader.read();
        assertTrue("Unexpected character: " + c, c == 'b');
      } catch (Exception readerException) {
        fail("Got unexpected exception: " + readerException.getMessage());
      } finally {
        reader.close();
      }
    } catch (Exception e) {
      fail("Got unexpected exception: " + e.getMessage());
    }
  }

  @Test (timeout = 30000)
  public void testCreateFile() throws Exception {
    assumeWindows();

    LOG.info("Open a file on Windows with SHARE_DELETE shared mode");
    try {
      File testfile = new File(TEST_DIR, "testCreateFile");
      assertTrue("Create test subject",
          testfile.exists() || testfile.createNewFile());

      FileDescriptor fd = NativeIO.Windows.createFile(
          testfile.getCanonicalPath(),
          NativeIO.Windows.GENERIC_READ,
          NativeIO.Windows.FILE_SHARE_READ |
          NativeIO.Windows.FILE_SHARE_WRITE |
          NativeIO.Windows.FILE_SHARE_DELETE,
          NativeIO.Windows.OPEN_EXISTING);

      FileInputStream fin = new FileInputStream(fd);
      try {
        fin.read();

        File newfile = new File(TEST_DIR, "testRenamedFile");

        boolean renamed = testfile.renameTo(newfile);
        assertTrue("Rename failed.", renamed);

        fin.read();
      } catch (Exception e) {
        fail("Got unexpected exception: " + e.getMessage());
      }
      finally {
        fin.close();
      }
    } catch (Exception e) {
      fail("Got unexpected exception: " + e.getMessage());
    }

  }

  /** Validate access checks on Windows */
  @Test (timeout = 30000)
  public void testAccess() throws Exception {
    assumeWindows();

    File testFile = new File(TEST_DIR, "testfileaccess");
    assertTrue(testFile.createNewFile());

    // Validate ACCESS_READ
    FileUtil.setReadable(testFile, false);
    assertFalse(NativeIO.Windows.access(testFile.getAbsolutePath(),
        NativeIO.Windows.AccessRight.ACCESS_READ));

    FileUtil.setReadable(testFile, true);
    assertTrue(NativeIO.Windows.access(testFile.getAbsolutePath(),
        NativeIO.Windows.AccessRight.ACCESS_READ));

    // Validate ACCESS_WRITE
    FileUtil.setWritable(testFile, false);
    assertFalse(NativeIO.Windows.access(testFile.getAbsolutePath(),
        NativeIO.Windows.AccessRight.ACCESS_WRITE));

    FileUtil.setWritable(testFile, true);
    assertTrue(NativeIO.Windows.access(testFile.getAbsolutePath(),
        NativeIO.Windows.AccessRight.ACCESS_WRITE));

    // Validate ACCESS_EXECUTE
    FileUtil.setExecutable(testFile, false);
    assertFalse(NativeIO.Windows.access(testFile.getAbsolutePath(),
        NativeIO.Windows.AccessRight.ACCESS_EXECUTE));

    FileUtil.setExecutable(testFile, true);
    assertTrue(NativeIO.Windows.access(testFile.getAbsolutePath(),
        NativeIO.Windows.AccessRight.ACCESS_EXECUTE));

    // Validate that access checks work as expected for long paths

    // Assemble a path longer then 260 chars (MAX_PATH)
    String testFileRelativePath = "";
    for (int i = 0; i < 15; ++i) {
      testFileRelativePath += "testfileaccessfolder\\";
    }
    testFileRelativePath += "testfileaccess";
    testFile = new File(TEST_DIR, testFileRelativePath);
    assertTrue(testFile.getParentFile().mkdirs());
    assertTrue(testFile.createNewFile());

    // Validate ACCESS_READ
    FileUtil.setReadable(testFile, false);
    assertFalse(NativeIO.Windows.access(testFile.getAbsolutePath(),
        NativeIO.Windows.AccessRight.ACCESS_READ));

    FileUtil.setReadable(testFile, true);
    assertTrue(NativeIO.Windows.access(testFile.getAbsolutePath(),
        NativeIO.Windows.AccessRight.ACCESS_READ));

    // Validate ACCESS_WRITE
    FileUtil.setWritable(testFile, false);
    assertFalse(NativeIO.Windows.access(testFile.getAbsolutePath(),
        NativeIO.Windows.AccessRight.ACCESS_WRITE));

    FileUtil.setWritable(testFile, true);
    assertTrue(NativeIO.Windows.access(testFile.getAbsolutePath(),
        NativeIO.Windows.AccessRight.ACCESS_WRITE));

    // Validate ACCESS_EXECUTE
    FileUtil.setExecutable(testFile, false);
    assertFalse(NativeIO.Windows.access(testFile.getAbsolutePath(),
        NativeIO.Windows.AccessRight.ACCESS_EXECUTE));

    FileUtil.setExecutable(testFile, true);
    assertTrue(NativeIO.Windows.access(testFile.getAbsolutePath(),
        NativeIO.Windows.AccessRight.ACCESS_EXECUTE));
  }

  @Test (timeout = 30000)
  public void testOpenMissingWithoutCreate() throws Exception {
    assumeNotWindows();

    LOG.info("Open a missing file without O_CREAT and it should fail");
    try {
      FileDescriptor fd = NativeIO.POSIX.open(
        new File(TEST_DIR, "doesntexist").getAbsolutePath(), O_WRONLY, 0700);
      fail("Able to open a new file without O_CREAT");
    } catch (NativeIOException nioe) {
      LOG.info("Got expected exception", nioe);
      assertEquals(Errno.ENOENT, nioe.getErrno());
    }
  }

  @Test (timeout = 30000)
  public void testOpenWithCreate() throws Exception {
    assumeNotWindows();

    LOG.info("Test creating a file with O_CREAT");
    FileDescriptor fd = NativeIO.POSIX.open(
      new File(TEST_DIR, "testWorkingOpen").getAbsolutePath(),
      O_WRONLY | O_CREAT, 0700);
    assertNotNull(true);
    assertTrue(fd.valid());
    FileOutputStream fos = new FileOutputStream(fd);
    fos.write("foo".getBytes());
    fos.close();

    assertFalse(fd.valid());

    LOG.info("Test exclusive create");
    try {
      fd = NativeIO.POSIX.open(
        new File(TEST_DIR, "testWorkingOpen").getAbsolutePath(),
        O_WRONLY | O_CREAT | O_EXCL, 0700);
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
  @Test (timeout = 30000)
  public void testFDDoesntLeak() throws IOException {
    assumeNotWindows();

    for (int i = 0; i < 10000; i++) {
      FileDescriptor fd = NativeIO.POSIX.open(
        new File(TEST_DIR, "testNoFdLeak").getAbsolutePath(),
        O_WRONLY | O_CREAT, 0700);
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
  @Test (timeout = 30000)
  public void testChmod() throws Exception {
    assumeNotWindows();

    try {
      NativeIO.POSIX.chmod("/this/file/doesnt/exist", 777);
      fail("Chmod of non-existent file didn't fail");
    } catch (NativeIOException nioe) {
      assertEquals(Errno.ENOENT, nioe.getErrno());
    }

    File toChmod = new File(TEST_DIR, "testChmod");
    assertTrue("Create test subject",
               toChmod.exists() || toChmod.mkdir());
    NativeIO.POSIX.chmod(toChmod.getAbsolutePath(), 0777);
    assertPermissions(toChmod, 0777);
    NativeIO.POSIX.chmod(toChmod.getAbsolutePath(), 0000);
    assertPermissions(toChmod, 0000);
    NativeIO.POSIX.chmod(toChmod.getAbsolutePath(), 0644);
    assertPermissions(toChmod, 0644);
  }


  @Test (timeout = 30000)
  public void testPosixFadvise() throws Exception {
    assumeNotWindows();

    FileInputStream fis = new FileInputStream("/dev/zero");
    try {
      NativeIO.POSIX.posix_fadvise(
          fis.getFD(), 0, 0, POSIX_FADV_SEQUENTIAL);
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
      NativeIO.POSIX.posix_fadvise(fis.getFD(), 0, 1024, POSIX_FADV_SEQUENTIAL);
      fail("Did not throw on bad file");
    } catch (NativeIOException nioe) {
      assertEquals(Errno.EBADF, nioe.getErrno());
    }
    
    try {
      NativeIO.POSIX.posix_fadvise(null, 0, 1024, POSIX_FADV_SEQUENTIAL);
      fail("Did not throw on null file");
    } catch (NullPointerException npe) {
      // expected
    }
  }

  @Test (timeout = 30000)
  public void testSyncFileRange() throws Exception {
    FileOutputStream fos = new FileOutputStream(
      new File(TEST_DIR, "testSyncFileRange"));
    try {
      fos.write("foo".getBytes());
      NativeIO.POSIX.sync_file_range(fos.getFD(), 0, 1024,
        SYNC_FILE_RANGE_WRITE);
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
      NativeIO.POSIX.sync_file_range(fos.getFD(), 0, 1024,
	   SYNC_FILE_RANGE_WRITE);
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

  @Test (timeout = 30000)
  public void testGetUserName() throws IOException {
    assumeNotWindows();
    assertFalse(NativeIO.POSIX.getUserName(0).isEmpty());
  }

  @Test (timeout = 30000)
  public void testGetGroupName() throws IOException {
    assumeNotWindows();
    assertFalse(NativeIO.POSIX.getGroupName(0).isEmpty());
  }

  @Test (timeout = 30000)
  public void testRenameTo() throws Exception {
    final File TEST_DIR = GenericTestUtils.getTestDir("renameTest") ;
    assumeTrue(TEST_DIR.mkdirs());
    File nonExistentFile = new File(TEST_DIR, "nonexistent");
    File targetFile = new File(TEST_DIR, "target");
    // Test attempting to rename a nonexistent file.
    try {
      NativeIO.renameTo(nonExistentFile, targetFile);
      Assert.fail();
    } catch (NativeIOException e) {
      if (Path.WINDOWS) {
        Assert.assertEquals(
          String.format("The system cannot find the file specified.%n"),
          e.getMessage());
      } else {
        Assert.assertEquals(Errno.ENOENT, e.getErrno());
      }
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
      if (Path.WINDOWS) {
        Assert.assertEquals(
          String.format("The parameter is incorrect.%n"),
          e.getMessage());
      } else {
        Assert.assertEquals(Errno.ENOTDIR, e.getErrno());
      }
    }

    // Test renaming to an existing file
    assertTrue(targetFile.exists());
    NativeIO.renameTo(sourceFile, targetFile);
  }

  @Test(timeout=10000)
  public void testMlock() throws Exception {
    assumeTrue(NativeIO.isAvailable());
    final File TEST_FILE = GenericTestUtils.getTestDir("testMlockFile");
    final int BUF_LEN = 12289;
    byte buf[] = new byte[BUF_LEN];
    int bufSum = 0;
    for (int i = 0; i < buf.length; i++) {
      buf[i] = (byte)(i % 60);
      bufSum += buf[i];
    }
    FileOutputStream fos = new FileOutputStream(TEST_FILE);
    try {
      fos.write(buf);
      fos.getChannel().force(true);
    } finally {
      fos.close();
    }
    
    FileInputStream fis = null;
    FileChannel channel = null;
    try {
      // Map file into memory
      fis = new FileInputStream(TEST_FILE);
      channel = fis.getChannel();
      long fileSize = channel.size();
      MappedByteBuffer mapbuf = channel.map(MapMode.READ_ONLY, 0, fileSize);
      // mlock the buffer
      NativeIO.POSIX.mlock(mapbuf, fileSize);
      // Read the buffer
      int sum = 0;
      for (int i=0; i<fileSize; i++) {
        sum += mapbuf.get(i);
      }
      assertEquals("Expected sums to be equal", bufSum, sum);
      // munmap the buffer, which also implicitly unlocks it
      NativeIO.POSIX.munmap(mapbuf);
    } finally {
      if (channel != null) {
        channel.close();
      }
      if (fis != null) {
        fis.close();
      }
    }
  }

  @Test(timeout=10000)
  public void testGetMemlockLimit() throws Exception {
    assumeTrue(NativeIO.isAvailable());
    NativeIO.getMemlockLimit();
  }

  @Test (timeout = 30000)
  public void testCopyFileUnbuffered() throws Exception {
    final String METHOD_NAME = GenericTestUtils.getMethodName();
    File srcFile = new File(TEST_DIR, METHOD_NAME + ".src.dat");
    File dstFile = new File(TEST_DIR, METHOD_NAME + ".dst.dat");
    final int fileSize = 0x8000000; // 128 MB
    final int SEED = 0xBEEF;
    final int batchSize = 4096;
    final int numBatches = fileSize / batchSize;
    Random rb = new Random(SEED);
    FileChannel channel = null;
    RandomAccessFile raSrcFile = null;
    try {
      raSrcFile = new RandomAccessFile(srcFile, "rw");
      channel = raSrcFile.getChannel();
      byte bytesToWrite[] = new byte[batchSize];
      MappedByteBuffer mapBuf;
      mapBuf = channel.map(MapMode.READ_WRITE, 0, fileSize);
      for (int i = 0; i < numBatches; i++) {
        rb.nextBytes(bytesToWrite);
        mapBuf.put(bytesToWrite);
      }
      NativeIO.copyFileUnbuffered(srcFile, dstFile);
      Assert.assertEquals(srcFile.length(), dstFile.length());
    } finally {
      IOUtils.cleanupWithLogger(LOG, channel);
      IOUtils.cleanupWithLogger(LOG, raSrcFile);
      FileUtils.deleteQuietly(TEST_DIR);
    }
  }

  @Test (timeout=10000)
  public void testNativePosixConsts() {
    assumeNotWindows("Native POSIX constants not required for Windows");
    assertTrue("Native 0_RDONLY const not set", O_RDONLY >= 0);
    assertTrue("Native 0_WRONLY const not set", O_WRONLY >= 0);
    assertTrue("Native 0_RDWR const not set", O_RDWR >= 0);
    assertTrue("Native 0_CREAT const not set", O_CREAT >= 0);
    assertTrue("Native 0_EXCL const not set", O_EXCL >= 0);
    assertTrue("Native 0_NOCTTY const not set", O_NOCTTY >= 0);
    assertTrue("Native 0_TRUNC const not set", O_TRUNC >= 0);
    assertTrue("Native 0_APPEND const not set", O_APPEND >= 0);
    assertTrue("Native 0_NONBLOCK const not set", O_NONBLOCK >= 0);
    assertTrue("Native 0_SYNC const not set", O_SYNC >= 0);
    assertTrue("Native S_IFMT const not set", S_IFMT >= 0);
    assertTrue("Native S_IFIFO const not set", S_IFIFO >= 0);
    assertTrue("Native S_IFCHR const not set", S_IFCHR >= 0);
    assertTrue("Native S_IFDIR const not set", S_IFDIR >= 0);
    assertTrue("Native S_IFBLK const not set", S_IFBLK >= 0);
    assertTrue("Native S_IFREG const not set", S_IFREG >= 0);
    assertTrue("Native S_IFLNK const not set", S_IFLNK >= 0);
    assertTrue("Native S_IFSOCK const not set", S_IFSOCK >= 0);
    assertTrue("Native S_ISUID const not set", S_ISUID >= 0);
    assertTrue("Native S_ISGID const not set", S_ISGID >= 0);
    assertTrue("Native S_ISVTX const not set", S_ISVTX >= 0);
    assertTrue("Native S_IRUSR const not set", S_IRUSR >= 0);
    assertTrue("Native S_IWUSR const not set", S_IWUSR >= 0);
    assertTrue("Native S_IXUSR const not set", S_IXUSR >= 0);
  }

  @Test (timeout=10000)
  public void testNativeFadviseConsts() {
    assumeTrue("Fadvise constants not supported", fadvisePossible);
    assertTrue("Native POSIX_FADV_NORMAL const not set",
      POSIX_FADV_NORMAL >= 0);
    assertTrue("Native POSIX_FADV_RANDOM const not set",
      POSIX_FADV_RANDOM >= 0);
    assertTrue("Native POSIX_FADV_SEQUENTIAL const not set",
      POSIX_FADV_SEQUENTIAL >= 0);
    assertTrue("Native POSIX_FADV_WILLNEED const not set",
      POSIX_FADV_WILLNEED >= 0);
    assertTrue("Native POSIX_FADV_DONTNEED const not set",
      POSIX_FADV_DONTNEED >= 0);
    assertTrue("Native POSIX_FADV_NOREUSE const not set",
      POSIX_FADV_NOREUSE >= 0);
  }
}
