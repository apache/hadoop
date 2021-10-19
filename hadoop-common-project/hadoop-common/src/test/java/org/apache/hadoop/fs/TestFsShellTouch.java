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

import java.text.ParseException;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.shell.TouchCommands.Touch;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.util.StringUtils;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.assertj.core.api.Assertions.assertThat;

public class TestFsShellTouch {
  static final Logger LOG = LoggerFactory.getLogger(TestFsShellTouch.class);

  static FsShell shell;
  static LocalFileSystem lfs;
  static Path testRootDir;

  @BeforeClass
  public static void setup() throws Exception {
    Configuration conf = new Configuration();
    shell = new FsShell(conf);
    lfs = FileSystem.getLocal(conf);
    testRootDir = lfs.makeQualified(
        new Path(GenericTestUtils.getTempPath("testFsShell")));

    lfs.mkdirs(testRootDir);
    lfs.setWorkingDirectory(testRootDir);
  }

  @Before
  public void prepFiles() throws Exception {
    lfs.setVerifyChecksum(true);
    lfs.setWriteChecksum(true);
  }

  private int shellRun(String... args) throws Exception {
    int exitCode = shell.run(args);
    LOG.info("exit " + exitCode + " - " + StringUtils.join(" ", args));
    return exitCode;
  }

  @Test
  public void testTouchz() throws Exception {
    // Ensure newFile does not exist
    final String newFileName = "newFile";
    final Path newFile = new Path(newFileName);
    lfs.delete(newFile, true);
    assertThat(lfs.exists(newFile)).isFalse();

    assertThat(shellRun("-touchz", newFileName))
        .as("Expected successful touchz on a new file").isEqualTo(0);
    shellRun("-ls", newFileName);

    assertThat(shellRun("-touchz", newFileName))
        .as("Expected successful touchz on an existing zero-length file")
        .isEqualTo(0);

    // Ensure noDir does not exist
    final String noDirName = "noDir";
    final Path noDir = new Path(noDirName);
    lfs.delete(noDir, true);
    assertThat(lfs.exists(noDir)).isFalse();

    assertThat(shellRun("-touchz", noDirName + "/foo"))
        .as("Expected failed touchz in a non-existent directory")
        .isNotEqualTo(0);
  }

  @Test
  public void testTouch() throws Exception {
    // Ensure newFile2 does not exist
    final String newFileName = "newFile2";
    final Path newFile = new Path(newFileName);
    lfs.delete(newFile, true);
    assertThat(lfs.exists(newFile)).isFalse();

    {
      assertThat(shellRun("-touch", "-c", newFileName))
          .as("Expected successful touch on a non-existent file" +
              " with -c option")
          .isEqualTo(0);
      assertThat(lfs.exists(newFile)).isFalse();
    }

    {
      String strTime = formatTimestamp(System.currentTimeMillis());
      Date dateObj = parseTimestamp(strTime);

      assertThat(shellRun("-touch", "-t", strTime, newFileName))
          .as("Expected successful touch on a new file" +
              " with a specified timestamp")
          .isEqualTo(0);
      FileStatus new_status = lfs.getFileStatus(newFile);
      assertThat(new_status.getAccessTime()).isEqualTo(dateObj.getTime());
      assertThat(new_status.getModificationTime())
          .isEqualTo(dateObj.getTime());
    }

    FileStatus fstatus = lfs.getFileStatus(newFile);

    {
      String strTime = formatTimestamp(System.currentTimeMillis());
      Date dateObj = parseTimestamp(strTime);

      assertThat(shellRun("-touch", "-a", "-t", strTime, newFileName))
          .as("Expected successful touch with a specified access time")
          .isEqualTo(0);
      FileStatus new_status = lfs.getFileStatus(newFile);
      // Verify if access time is recorded correctly (and modification time
      // remains unchanged).
      assertThat(new_status.getAccessTime()).isEqualTo(dateObj.getTime());
      assertThat(new_status.getModificationTime())
          .isEqualTo(fstatus.getModificationTime());
    }

    fstatus = lfs.getFileStatus(newFile);

    {
      String strTime = formatTimestamp(System.currentTimeMillis());
      Date dateObj = parseTimestamp(strTime);

      assertThat(shellRun("-touch", "-m", "-t", strTime, newFileName))
          .as("Expected successful touch with a specified modification time")
          .isEqualTo(0);
      // Verify if modification time is recorded correctly (and access time
      // remains unchanged).
      FileStatus new_status = lfs.getFileStatus(newFile);
      assertThat(new_status.getAccessTime())
          .isEqualTo(fstatus.getAccessTime());
      assertThat(new_status.getModificationTime())
          .isEqualTo(dateObj.getTime());
    }

    {
      String strTime = formatTimestamp(System.currentTimeMillis());
      Date dateObj = parseTimestamp(strTime);

      assertThat(shellRun("-touch", "-t", strTime, newFileName))
          .as("Expected successful touch with a specified timestamp")
          .isEqualTo(0);

      // Verify if both modification and access times are recorded correctly
      FileStatus new_status = lfs.getFileStatus(newFile);
      assertThat(new_status.getAccessTime()).isEqualTo(dateObj.getTime());
      assertThat(new_status.getModificationTime())
          .isEqualTo(dateObj.getTime());
    }

    {
      String strTime = formatTimestamp(System.currentTimeMillis());
      Date dateObj = parseTimestamp(strTime);

      assertThat(shellRun("-touch", "-a", "-m", "-t", strTime, newFileName))
          .as("Expected successful touch with a specified timestamp")
          .isEqualTo(0);

      // Verify if both modification and access times are recorded correctly
      FileStatus new_status = lfs.getFileStatus(newFile);
      assertThat(new_status.getAccessTime()).isEqualTo(dateObj.getTime());
      assertThat(new_status.getModificationTime())
          .isEqualTo(dateObj.getTime());
    }

    {
      assertThat(shellRun("-touch", "-t", newFileName))
          .as("Expected failed touch with a missing timestamp")
          .isNotEqualTo(0);
    }

    // Verify -c option when file exists.
    String strTime = formatTimestamp(System.currentTimeMillis());
    Date dateObj = parseTimestamp(strTime);
    assertThat(shellRun("-touch", "-c", "-t", strTime, newFileName))
        .as("Expected successful touch on a non-existent file with -c option")
        .isEqualTo(0);
    FileStatus fileStatus = lfs.getFileStatus(newFile);
    assertThat(fileStatus.getAccessTime()).isEqualTo(dateObj.getTime());
    assertThat(fileStatus.getModificationTime()).isEqualTo(dateObj.getTime());
  }

  private String formatTimestamp(long timeInMillis) {
    return (new Touch()).getDateFormat().format(new Date(timeInMillis));
  }

  private Date parseTimestamp(String tstamp) throws ParseException {
    return (new Touch()).getDateFormat().parse(tstamp);
  }
}
