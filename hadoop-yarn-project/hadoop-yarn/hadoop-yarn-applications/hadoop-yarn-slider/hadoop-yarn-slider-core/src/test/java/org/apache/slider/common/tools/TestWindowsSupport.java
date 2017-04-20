/*
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

package org.apache.slider.common.tools;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ChecksumFileSystem;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.service.ServiceStateException;
import org.apache.hadoop.util.Shell;
import org.apache.slider.utils.YarnMiniClusterTestBase;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.net.URI;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

/**
 * Test windows support.
 */
public class TestWindowsSupport extends YarnMiniClusterTestBase {
  private static final Logger LOG =
      LoggerFactory.getLogger(TestWindowsSupport.class);

  private static final Pattern HAS_DRIVE_LETTER_SPECIFIER =
      Pattern.compile("^/?[a-zA-Z]:");
  public static final String WINDOWS_FILE =
      "C:\\Users\\Administrator\\AppData\\Local\\Temp" +
      "\\junit3180177850133852404\\testpkg\\appdef_1.zip";


  private static boolean hasWindowsDrive(String path) {
    return HAS_DRIVE_LETTER_SPECIFIER.matcher(path).find();
  }

  private static int startPositionWithoutWindowsDrive(String path) {
    if (hasWindowsDrive(path)) {
      return path.charAt(0) == '/' ? 3 : 2;
    } else {
      return 0;
    }
  }

  @Test
  public void testHasWindowsDrive() throws Throwable {
    assertTrue(hasWindowsDrive(WINDOWS_FILE));
  }

  @Test
  public void testStartPosition() throws Throwable {
    assertEquals(2, startPositionWithoutWindowsDrive(WINDOWS_FILE));
  }

  @Test
  public void testPathHandling() throws Throwable {
    assumeWindows();

    Path path = new Path(WINDOWS_FILE);
    URI uri = path.toUri();
    //    assert "file" == uri.scheme
    assertNull(uri.getAuthority());

    Configuration conf = new Configuration();

    FileSystem localfs = FileSystem.get(uri, conf);
    assertTrue(localfs instanceof ChecksumFileSystem);
    try {
      FileStatus stat = localfs.getFileStatus(path);
      fail("expected an exception, got " + stat);
    } catch (FileNotFoundException fnfe) {
      // expected
    }

    try {
      FSDataInputStream appStream = localfs.open(path);
    } catch (FileNotFoundException fnfe) {
      // expected
    }
  }

  @Test
  public void testExecNonexistentBinary() throws Throwable {
    assumeWindows();
    List<String> commands = Arrays.asList("undefined-application", "--version");
    try {
      exec(0, commands);
      fail("expected an exception");
    } catch (ServiceStateException e) {
      if (!(e.getCause() instanceof FileNotFoundException)) {
        throw e;
      }
    }
  }
  @Test
  public void testExecNonexistentBinary2() throws Throwable {
    assumeWindows();
    assertFalse(doesAppExist(Arrays.asList("undefined-application",
        "--version")));
  }

  @Test
  public void testEmitKillCommand() throws Throwable {

    int result = killJavaProcesses("regionserver", 9);
    // we know the exit code if there is no supported kill operation
    assertTrue(getKillSupported() || result == -1);
  }

  @Test
  public void testHadoopHomeDefined() throws Throwable {
    assumeWindows();
    String hadoopHome = Shell.getHadoopHome();
    LOG.info("HADOOP_HOME={}", hadoopHome);
  }

  @Test
  public void testHasWinutils() throws Throwable {
    assumeWindows();
    SliderUtils.maybeVerifyWinUtilsValid();
  }

  @Test
  public void testExecWinutils() throws Throwable {
    assumeWindows();
    String winUtilsPath = Shell.getWinUtilsPath();
    assertTrue(SliderUtils.isSet(winUtilsPath));
    File winUtils = new File(winUtilsPath);
    LOG.debug("Winutils is at {}", winUtils);

    exec(0, Arrays.asList(winUtilsPath, "systeminfo"));
  }

  @Test
  public void testPath() throws Throwable {
    String path = extractPath();
    LOG.info("Path value = {}", path);
  }

  @Test
  public void testFindJavac() throws Throwable {
    String name = Shell.WINDOWS ? "javac.exe" : "javac";
    assertNotNull(locateExecutable(name));
  }

  @Test
  public void testHadoopDLL() throws Throwable {
    assumeWindows();
    // split the path
    File exepath = locateExecutable("HADOOP.DLL");
    assertNotNull(exepath);
    LOG.info("Hadoop DLL at: {}", exepath);
  }

}
