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

import java.io.IOException;
import java.text.ParseException;
import java.util.Date;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FsShell;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.shell.TouchCommands;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.util.StringUtils;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests to perform Touch operations on DFS.
 */
public class TestDFSShellTouch {

  private static final Logger LOG = LoggerFactory.getLogger(TestDFSShellTouch.class);

  private static MiniDFSCluster miniCluster;
  private static DistributedFileSystem dfs;
  private static FsShell shell;

  @BeforeClass
  public static void setup() throws IOException {
    final Configuration conf = new Configuration();
    conf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR,
        GenericTestUtils.getTestDir("TestDFSShellTouch").getAbsolutePath());

    miniCluster = new MiniDFSCluster.Builder(conf).numDataNodes(2).build();
    miniCluster.waitActive();
    dfs = miniCluster.getFileSystem();
    shell = new FsShell(dfs.getConf());
  }

  @AfterClass
  public static void tearDown() {
    if (miniCluster != null) {
      miniCluster.shutdown(true, true);
    }
  }

  @Test
  public void testTouch() throws Exception {
    final String newFileName = "newFile1";
    final Path newFile = new Path(newFileName);
    dfs.delete(newFile, true);
    assertThat(dfs.exists(newFile)).isFalse();
    dfs.create(newFile);
    assertThat(dfs.exists(newFile)).isTrue();

    String strTime = formatTimestamp(System.currentTimeMillis());
    Date dateObj = parseTimestamp(strTime);

    assertThat(shellRun("-touch", "-t", strTime, newFileName)).as(
        "Expected successful touch on a new file" + " with a specified timestamp").isEqualTo(0);
    FileStatus newStatus = dfs.getFileStatus(newFile);
    assertThat(newStatus.getAccessTime()).isEqualTo(dateObj.getTime());
    assertThat(newStatus.getModificationTime()).isEqualTo(dateObj.getTime());

    FileStatus fileStatus = dfs.getFileStatus(newFile);
    Thread.sleep(500);

    strTime = formatTimestamp(System.currentTimeMillis());
    dateObj = parseTimestamp(strTime);

    assertThat(shellRun("-touch", "-a", "-t", strTime, newFileName)).as(
        "Expected successful touch with a specified access time").isEqualTo(0);
    newStatus = dfs.getFileStatus(newFile);
    // Verify if access time is recorded correctly (and modification time
    // remains unchanged).
    assertThat(newStatus.getAccessTime()).isEqualTo(dateObj.getTime());
    assertThat(newStatus.getModificationTime()).isEqualTo(fileStatus.getModificationTime());

    fileStatus = dfs.getFileStatus(newFile);
    Thread.sleep(500);

    strTime = formatTimestamp(System.currentTimeMillis());
    dateObj = parseTimestamp(strTime);

    assertThat(shellRun("-touch", "-m", "-t", strTime, newFileName)).as(
        "Expected successful touch with a specified modification time").isEqualTo(0);
    // Verify if modification time is recorded correctly (and access time
    // remains unchanged).
    newStatus = dfs.getFileStatus(newFile);
    assertThat(newStatus.getAccessTime()).isEqualTo(fileStatus.getAccessTime());
    assertThat(newStatus.getModificationTime()).isEqualTo(dateObj.getTime());

    strTime = formatTimestamp(System.currentTimeMillis());
    dateObj = parseTimestamp(strTime);

    assertThat(shellRun("-touch", "-t", strTime, newFileName)).as(
        "Expected successful touch with a specified timestamp").isEqualTo(0);

    // Verify if both modification and access times are recorded correctly
    newStatus = dfs.getFileStatus(newFile);
    assertThat(newStatus.getAccessTime()).isEqualTo(dateObj.getTime());
    assertThat(newStatus.getModificationTime()).isEqualTo(dateObj.getTime());

    strTime = formatTimestamp(System.currentTimeMillis());
    dateObj = parseTimestamp(strTime);

    assertThat(shellRun("-touch", "-a", "-m", "-t", strTime, newFileName)).as(
        "Expected successful touch with a specified timestamp").isEqualTo(0);

    // Verify if both modification and access times are recorded correctly
    newStatus = dfs.getFileStatus(newFile);
    assertThat(newStatus.getAccessTime()).isEqualTo(dateObj.getTime());
    assertThat(newStatus.getModificationTime()).isEqualTo(dateObj.getTime());

    assertThat(shellRun("-touch", "-t", newFileName)).as(
        "Expected failed touch with a missing timestamp").isNotEqualTo(0);

    strTime = formatTimestamp(System.currentTimeMillis());
    dateObj = parseTimestamp(strTime);
    assertThat(shellRun("-touch", "-c", "-t", strTime, newFileName)).as(
        "Expected successful touch on a non-existent file with -c option").isEqualTo(0);
    fileStatus = dfs.getFileStatus(newFile);
    assertThat(fileStatus.getAccessTime()).isEqualTo(dateObj.getTime());
    assertThat(fileStatus.getModificationTime()).isEqualTo(dateObj.getTime());

    dfs.delete(newFile, true);
    assertThat(dfs.exists(newFile)).isFalse();

  }

  @Test
  public void testTouchDirs() throws IOException, ParseException, InterruptedException {
    final String newFileName = "dir2/newFile2";
    final Path newFile = new Path(newFileName);
    FileStatus newStatus;
    FileStatus fileStatus;
    String strTime;
    Date dateObj;
    Path dirPath = new Path("dir2");
    dfs.mkdirs(dirPath);
    dfs.delete(newFile, true);
    assertThat(dfs.exists(newFile)).isFalse();

    strTime = formatTimestamp(System.currentTimeMillis());
    dateObj = parseTimestamp(strTime);

    assertThat(shellRun("-touch", "-t", strTime, newFileName)).as(
        "Expected successful touch on a new file with a specified timestamp").isEqualTo(0);
    newStatus = dfs.getFileStatus(newFile);
    assertThat(newStatus.getAccessTime()).isEqualTo(dateObj.getTime());
    assertThat(newStatus.getModificationTime()).isEqualTo(dateObj.getTime());

    Thread.sleep(500);
    strTime = formatTimestamp(System.currentTimeMillis());
    dateObj = parseTimestamp(strTime);

    assertThat(shellRun("-touch", "-m", "-a", "-t", strTime, "dir2")).as(
        "Expected successful touch with a specified modification time").isEqualTo(0);

    newStatus = dfs.getFileStatus(dirPath);
    // Verify if both modification and access times are recorded correctly
    assertThat(newStatus.getAccessTime()).isEqualTo(dateObj.getTime());
    assertThat(newStatus.getModificationTime()).isEqualTo(dateObj.getTime());

    fileStatus = dfs.getFileStatus(dirPath);
    Thread.sleep(500);

    strTime = formatTimestamp(System.currentTimeMillis());
    dateObj = parseTimestamp(strTime);

    assertThat(shellRun("-touch", "-m", "-t", strTime, "dir2")).as(
        "Expected successful touch with a specified modification time").isEqualTo(0);

    newStatus = dfs.getFileStatus(dirPath);
    // Verify if modification time is recorded correctly (and access time
    // remains unchanged).
    assertThat(newStatus.getAccessTime()).isEqualTo(fileStatus.getAccessTime());
    assertThat(newStatus.getModificationTime()).isEqualTo(dateObj.getTime());

    fileStatus = dfs.getFileStatus(dirPath);
    Thread.sleep(500);

    strTime = formatTimestamp(System.currentTimeMillis());
    dateObj = parseTimestamp(strTime);

    assertThat(shellRun("-touch", "-a", "-t", strTime, "dir2")).as(
        "Expected successful touch with a specified modification time").isEqualTo(0);

    newStatus = dfs.getFileStatus(dirPath);
    // Verify if access time is recorded correctly (and modification time
    // remains unchanged).
    assertThat(newStatus.getAccessTime()).isEqualTo(dateObj.getTime());
    assertThat(newStatus.getModificationTime()).isEqualTo(fileStatus.getModificationTime());

    dfs.delete(newFile, true);
    dfs.delete(dirPath, true);
    assertThat(dfs.exists(newFile)).isFalse();
    assertThat(dfs.exists(dirPath)).isFalse();
  }

  private int shellRun(String... args) {
    int exitCode = shell.run(args);
    LOG.info("exit " + exitCode + " - " + StringUtils.join(" ", args));
    return exitCode;
  }

  private String formatTimestamp(long timeInMillis) {
    return (new TouchCommands.Touch()).getDateFormat().format(new Date(timeInMillis));
  }

  private Date parseTimestamp(String tstamp) throws ParseException {
    return (new TouchCommands.Touch()).getDateFormat().parse(tstamp);
  }

}
