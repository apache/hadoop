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

import java.io.File;
import java.io.IOException;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.test.StatUtils;
import org.apache.hadoop.util.Shell;

import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test filesystem contracts with {@link RawLocalFileSystem}.
 * Root directory related tests from super class will work into target
 * directory since we have no permission to write / on local filesystem.
 */
public class TestRawLocalFileSystemContract extends FileSystemContractBaseTest {

  private static final Logger LOG =
      LoggerFactory.getLogger(TestRawLocalFileSystemContract.class);
  private final static Path TEST_BASE_DIR =
      new Path(GenericTestUtils.getRandomizedTestDir().getAbsolutePath());

  // These are the string values that DF sees as "Filesystem" for a
  // Docker container accessing a Mac or Windows host's filesystem.
  private static final String FS_TYPE_MAC = "osxfs";
  private static boolean looksLikeMac(String filesys) {
    return filesys.toLowerCase().contains(FS_TYPE_MAC.toLowerCase());
  }
  private static final Pattern HAS_DRIVE_LETTER_SPECIFIER =
      Pattern.compile("^/?[a-zA-Z]:");
  private static boolean looksLikeWindows(String filesys) {
    return HAS_DRIVE_LETTER_SPECIFIER.matcher(filesys).find();
  }

  @Before
  public void setUp() throws Exception {
    Configuration conf = new Configuration();
    fs = FileSystem.getLocal(conf).getRawFileSystem();
  }

  /**
   * Actually rename is supported in RawLocalFileSystem but
   * it works different as the other filesystems. Short term we do not test it.
   * Please check HADOOP-13082.
   * @return true if rename supported so rename related tests will run
   */
  @Override
  protected boolean renameSupported() {
    return false;
  }

  /**
   * Disabling testing root operation.
   *
   * Writing to root directory on the local file system may get permission
   * denied exception, or even worse, delete/overwrite files accidentally.
   */
  @Override
  protected boolean rootDirTestEnabled() {
    return false;
  }

  @Override
  public String getDefaultWorkingDirectory() {
    return fs.getWorkingDirectory().toUri().getPath();
  }

  @Override
  protected Path getTestBaseDir() {
    return TEST_BASE_DIR;
  }

  @Override
  protected boolean filesystemIsCaseSensitive() {
    if (Shell.WINDOWS || Shell.MAC) {
      return false;
    }
    // osType is linux or unix-like, but it might be in a container mounting a
    // Mac or Windows volume. Use DF to try to determine if this is the case.
    String rfsPathStr = "uninitialized";
    String rfsType;
    try {
      RawLocalFileSystem rfs = new RawLocalFileSystem();
      Configuration conf = new Configuration();
      rfs.initialize(rfs.getUri(), conf);
      rfsPathStr = Path.getPathWithoutSchemeAndAuthority(
          rfs.getWorkingDirectory()).toString();
      File rfsPath = new File(rfsPathStr);
      // DF.getFilesystem() only provides indirect info about FS type, but it's
      // the best we have.  `df -T` would be better, but isn't cross-platform.
      rfsType = (new DF(rfsPath, conf)).getFilesystem();
      LOG.info("DF.Filesystem is {} for path {}", rfsType, rfsPath);
    } catch (IOException ex) {
      LOG.error("DF failed on path {}", rfsPathStr);
      rfsType = Shell.osType.toString();
    }
    return !(looksLikeMac(rfsType) || looksLikeWindows(rfsType));
  }

  // cross-check getPermission using both native/non-native
  @Test
  @SuppressWarnings("deprecation")
  public void testPermission() throws Exception {
    Path testDir = getTestBaseDir();
    String testFilename = "teststat2File";
    Path path = new Path(testDir, testFilename);

    RawLocalFileSystem rfs = new RawLocalFileSystem();
    Configuration conf = new Configuration();
    rfs.initialize(rfs.getUri(), conf);
    rfs.createNewFile(path);

    File file = rfs.pathToFile(path);
    long defaultBlockSize = rfs.getDefaultBlockSize(path);

    //
    // test initial permission
    //
    RawLocalFileSystem.DeprecatedRawLocalFileStatus fsNIO =
        new RawLocalFileSystem.DeprecatedRawLocalFileStatus(
            file, defaultBlockSize, rfs);
    fsNIO.loadPermissionInfoByNativeIO();
    RawLocalFileSystem.DeprecatedRawLocalFileStatus fsnonNIO =
        new RawLocalFileSystem.DeprecatedRawLocalFileStatus(
            file, defaultBlockSize, rfs);
    fsnonNIO.loadPermissionInfoByNonNativeIO();

    assertEquals(fsNIO.getOwner(), fsnonNIO.getOwner());
    assertEquals(fsNIO.getGroup(), fsnonNIO.getGroup());
    assertEquals(fsNIO.getPermission(), fsnonNIO.getPermission());

    LOG.info("owner: {}, group: {}, permission: {}, isSticky: {}",
        fsNIO.getOwner(), fsNIO.getGroup(), fsNIO.getPermission(),
        fsNIO.getPermission().getStickyBit());

    //
    // test normal chmod - no sticky bit
    //
    StatUtils.setPermissionFromProcess("644", file.getPath());
    fsNIO.loadPermissionInfoByNativeIO();
    fsnonNIO.loadPermissionInfoByNonNativeIO();
    assertEquals(fsNIO.getPermission(), fsnonNIO.getPermission());
    assertEquals(644, fsNIO.getPermission().toOctal());
    assertFalse(fsNIO.getPermission().getStickyBit());
    assertFalse(fsnonNIO.getPermission().getStickyBit());

    //
    // test sticky bit
    // unfortunately, cannot be done in Windows environments
    //
    if (!Shell.WINDOWS) {
      //
      // add sticky bit
      //
      StatUtils.setPermissionFromProcess("1644", file.getPath());
      fsNIO.loadPermissionInfoByNativeIO();
      fsnonNIO.loadPermissionInfoByNonNativeIO();
      assertEquals(fsNIO.getPermission(), fsnonNIO.getPermission());
      assertEquals(1644, fsNIO.getPermission().toOctal());
      assertEquals(true, fsNIO.getPermission().getStickyBit());
      assertEquals(true, fsnonNIO.getPermission().getStickyBit());

      //
      // remove sticky bit
      //
      StatUtils.setPermissionFromProcess("-t", file.getPath());
      fsNIO.loadPermissionInfoByNativeIO();
      fsnonNIO.loadPermissionInfoByNonNativeIO();
      assertEquals(fsNIO.getPermission(), fsnonNIO.getPermission());
      assertEquals(644, fsNIO.getPermission().toOctal());
      assertEquals(false, fsNIO.getPermission().getStickyBit());
      assertEquals(false, fsnonNIO.getPermission().getStickyBit());
    }
  }

}
