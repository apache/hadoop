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

package org.apache.hadoop.mapred.uploader;

import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.util.Lists;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.zip.GZIPInputStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.apache.hadoop.fs.FileSystem.FS_DEFAULT_NAME_KEY;

/**
 * Unit test class for FrameworkUploader.
 */
public class TestFrameworkUploader {
  private static String testDir;

  @BeforeEach
  public void setUp() {
    String testRootDir =
        new File(System.getProperty("test.build.data", "/tmp"))
            .getAbsolutePath()
            .replace(' ', '+');
    Random random = new Random(System.currentTimeMillis());
    testDir = testRootDir + File.separatorChar +
        Long.toString(random.nextLong());
  }

  /**
   * Test requesting command line help.
   * @throws IOException test failure
   */
  @Test
  void testHelp() throws IOException {
    String[] args = new String[]{"-help"};
    FrameworkUploader uploader = new FrameworkUploader();
    boolean success = uploader.parseArguments(args);
    assertFalse(success, "Expected to print help");
    assertThat(uploader.input)
        .withFailMessage("Expected ignore run")
        .isNull();
    assertThat(uploader.whitelist)
        .withFailMessage("Expected ignore run")
        .isNull();
    assertThat(uploader.target)
        .withFailMessage("Expected ignore run")
        .isNull();
  }

  /**
   * Test invalid argument parsing.
   * @throws IOException test failure
   */
  @Test
  void testWrongArgument() throws IOException {
    String[] args = new String[]{"-unexpected"};
    FrameworkUploader uploader = new FrameworkUploader();
    boolean success = uploader.parseArguments(args);
    assertFalse(success, "Expected to print help");
  }

  /**
   * Test normal argument passing.
   * @throws IOException test failure
   */
  @Test
  void testArguments() throws IOException {
    String[] args =
        new String[]{
            "-input", "A",
            "-whitelist", "B",
            "-blacklist", "C",
            "-fs", "hdfs://C:8020",
            "-target", "D",
            "-initialReplication", "100",
            "-acceptableReplication", "120",
            "-finalReplication", "140",
            "-timeout", "10"};
    FrameworkUploader uploader = new FrameworkUploader();
    boolean success = uploader.parseArguments(args);
    assertTrue(success, "Expected to print help");
    assertEquals("A",
        uploader.input,
        "Input mismatch");
    assertEquals("B",
        uploader.whitelist,
        "Whitelist mismatch");
    assertEquals("C",
        uploader.blacklist,
        "Blacklist mismatch");
    assertEquals("hdfs://C:8020/D",
        uploader.target,
        "Target mismatch");
    assertEquals(100,
        uploader.initialReplication,
        "Initial replication mismatch");
    assertEquals(120,
        uploader.acceptableReplication,
        "Acceptable replication mismatch");
    assertEquals(140,
        uploader.finalReplication,
        "Final replication mismatch");
    assertEquals(10,
        uploader.timeout,
        "Timeout mismatch");
  }

  /**
   * Test the default ways how to specify filesystems.
   */
  @Test
  void testNoFilesystem() throws IOException {
    FrameworkUploader uploader = new FrameworkUploader();
    boolean success = uploader.parseArguments(new String[]{});
    assertTrue(success, "Expected to parse arguments");
    assertEquals(
        "file:////usr/lib/mr-framework.tar.gz#mr-framework", uploader.target, "Expected");
  }

  /**
   * Test the default ways how to specify filesystems.
   */
  @Test
  void testDefaultFilesystem() throws IOException {
    FrameworkUploader uploader = new FrameworkUploader();
    Configuration conf = new Configuration();
    conf.set(FS_DEFAULT_NAME_KEY, "hdfs://namenode:555");
    uploader.setConf(conf);
    boolean success = uploader.parseArguments(new String[]{});
    assertTrue(success, "Expected to parse arguments");
    assertEquals(
        "hdfs://namenode:555/usr/lib/mr-framework.tar.gz#mr-framework",
        uploader.target,
        "Expected");
  }

  /**
   * Test the explicit filesystem specification.
   */
  @Test
  void testExplicitFilesystem() throws IOException {
    FrameworkUploader uploader = new FrameworkUploader();
    Configuration conf = new Configuration();
    uploader.setConf(conf);
    boolean success = uploader.parseArguments(new String[]{
        "-target",
        "hdfs://namenode:555/usr/lib/mr-framework.tar.gz#mr-framework"
    });
    assertTrue(success, "Expected to parse arguments");
    assertEquals(
        "hdfs://namenode:555/usr/lib/mr-framework.tar.gz#mr-framework",
        uploader.target,
        "Expected");
  }

  /**
   * Test the conflicting filesystem specification.
   */
  @Test
  void testConflictingFilesystem() throws IOException {
    FrameworkUploader uploader = new FrameworkUploader();
    Configuration conf = new Configuration();
    conf.set(FS_DEFAULT_NAME_KEY, "hdfs://namenode:555");
    uploader.setConf(conf);
    boolean success = uploader.parseArguments(new String[]{
        "-target",
        "file:///usr/lib/mr-framework.tar.gz#mr-framework"
    });
    assertTrue(success, "Expected to parse arguments");
    assertEquals(
        "file:///usr/lib/mr-framework.tar.gz#mr-framework",
        uploader.target,
        "Expected");
  }

  /**
   * Test whether we can filter a class path properly.
   * @throws IOException test failure
   */
  @Test
  void testCollectPackages() throws IOException, UploaderException {
    File parent = new File(testDir);
    try {
      parent.deleteOnExit();
      assertTrue(parent.mkdirs(), "Directory creation failed");
      File dirA = new File(parent, "A");
      assertTrue(dirA.mkdirs());
      File dirB = new File(parent, "B");
      assertTrue(dirB.mkdirs());
      File jarA = new File(dirA, "a.jar");
      assertTrue(jarA.createNewFile());
      File jarB = new File(dirA, "b.jar");
      assertTrue(jarB.createNewFile());
      File jarC = new File(dirA, "c.jar");
      assertTrue(jarC.createNewFile());
      File txtD = new File(dirA, "d.txt");
      assertTrue(txtD.createNewFile());
      File jarD = new File(dirB, "d.jar");
      assertTrue(jarD.createNewFile());
      File txtE = new File(dirB, "e.txt");
      assertTrue(txtE.createNewFile());

      FrameworkUploader uploader = new FrameworkUploader();
      uploader.whitelist = ".*a\\.jar,.*b\\.jar,.*d\\.jar";
      uploader.blacklist = ".*b\\.jar";
      uploader.input = dirA.getAbsolutePath() + File.separatorChar + "*" +
          File.pathSeparatorChar +
          dirB.getAbsolutePath() + File.separatorChar + "*";
      uploader.collectPackages();
      assertEquals(3,
          uploader.whitelistedFiles.size(),
          "Whitelist count error");
      assertEquals(1,
          uploader.blacklistedFiles.size(),
          "Blacklist count error");

      assertTrue(uploader.filteredInputFiles.contains(jarA.getAbsolutePath()),
          "File not collected");
      assertFalse(uploader.filteredInputFiles.contains(jarB.getAbsolutePath()),
          "File collected");
      assertTrue(uploader.filteredInputFiles.contains(jarD.getAbsolutePath()),
          "File not collected");
      assertEquals(2,
          uploader.filteredInputFiles.size(),
          "Too many whitelists");
    } finally {
      FileUtils.deleteDirectory(parent);
    }
  }

  /**
   * Test building a tarball from source jars.
   */
  @Test
  void testBuildTarBall()
      throws IOException, UploaderException, InterruptedException {
    String[] testFiles = {"upload.tar", "upload.tar.gz"};
    for (String testFile : testFiles) {
      File parent = new File(testDir);
      try {
        parent.deleteOnExit();
        FrameworkUploader uploader = prepareTree(parent);

        File gzipFile =
            new File(parent.getAbsolutePath() + "/" + testFile);
        gzipFile.deleteOnExit();

        uploader.target =
            "file:///" + gzipFile.getAbsolutePath();
        uploader.beginUpload();
        uploader.buildPackage();
        InputStream stream = new FileInputStream(gzipFile);
        if (gzipFile.getName().endsWith(".gz")) {
          stream = new GZIPInputStream(stream);
        }

        TarArchiveInputStream result = null;
        try {
          result =
              new TarArchiveInputStream(stream);
          Set<String> fileNames = new HashSet<>();
          Set<Long> sizes = new HashSet<>();
          TarArchiveEntry entry1 = result.getNextTarEntry();
          fileNames.add(entry1.getName());
          sizes.add(entry1.getSize());
          TarArchiveEntry entry2 = result.getNextTarEntry();
          fileNames.add(entry2.getName());
          sizes.add(entry2.getSize());
          assertTrue(
              fileNames.contains("a.jar"), "File name error");
          assertTrue(
              sizes.contains((long) 13), "File size error");
          assertTrue(
              fileNames.contains("b.jar"), "File name error");
          assertTrue(
              sizes.contains((long) 14), "File size error");
        } finally {
          if (result != null) {
            result.close();
          }
        }
      } finally {
        FileUtils.deleteDirectory(parent);
      }
    }
  }

  /**
   * Test upload to HDFS.
   */
  @Test
  void testUpload()
      throws IOException, UploaderException, InterruptedException {
    final String fileName = "/upload.tar.gz";
    File parent = new File(testDir);
    try {
      parent.deleteOnExit();

      FrameworkUploader uploader = prepareTree(parent);

      uploader.target = "file://" + parent.getAbsolutePath() + fileName;

      uploader.buildPackage();
      try (TarArchiveInputStream archiveInputStream = new TarArchiveInputStream(
          new GZIPInputStream(
              new FileInputStream(
                  parent.getAbsolutePath() + fileName)))) {
        Set<String> fileNames = new HashSet<>();
        Set<Long> sizes = new HashSet<>();
        TarArchiveEntry entry1 = archiveInputStream.getNextTarEntry();
        fileNames.add(entry1.getName());
        sizes.add(entry1.getSize());
        TarArchiveEntry entry2 = archiveInputStream.getNextTarEntry();
        fileNames.add(entry2.getName());
        sizes.add(entry2.getSize());
        assertTrue(
            fileNames.contains("a.jar"), "File name error");
        assertTrue(
            sizes.contains((long) 13), "File size error");
        assertTrue(
            fileNames.contains("b.jar"), "File name error");
        assertTrue(
            sizes.contains((long) 14), "File size error");
      }
    } finally {
      FileUtils.deleteDirectory(parent);
    }
  }

  /**
   * Prepare a mock directory tree to compress and upload.
   */
  private FrameworkUploader prepareTree(File parent)
      throws FileNotFoundException {
    assertTrue(parent.mkdirs());
    File dirA = new File(parent, "A");
    assertTrue(dirA.mkdirs());
    File jarA = new File(parent, "a.jar");
    PrintStream printStream = new PrintStream(new FileOutputStream(jarA));
    printStream.println("Hello World!");
    printStream.close();
    File jarB = new File(dirA, "b.jar");
    printStream = new PrintStream(new FileOutputStream(jarB));
    printStream.println("Hello Galaxy!");
    printStream.close();

    FrameworkUploader uploader = new FrameworkUploader();
    uploader.filteredInputFiles.add(jarA.getAbsolutePath());
    uploader.filteredInputFiles.add(jarB.getAbsolutePath());

    return uploader;
  }

  /**
   * Test regex pattern matching and environment variable replacement.
   */
  @Test
  void testEnvironmentReplacement() throws UploaderException {
    String input = "C/$A/B,$B,D";
    Map<String, String> map = new HashMap<>();
    map.put("A", "X");
    map.put("B", "Y");
    map.put("C", "Z");
    FrameworkUploader uploader = new FrameworkUploader();
    String output = uploader.expandEnvironmentVariables(input, map);
    assertEquals("C/X/B,Y,D", output, "Environment not expanded");

  }

  /**
   * Test regex pattern matching and environment variable replacement.
   */
  @Test
  void testRecursiveEnvironmentReplacement()
      throws UploaderException {
    String input = "C/$A/B,$B,D";
    Map<String, String> map = new HashMap<>();
    map.put("A", "X");
    map.put("B", "$C");
    map.put("C", "Y");
    FrameworkUploader uploader = new FrameworkUploader();
    String output = uploader.expandEnvironmentVariables(input, map);
    assertEquals("C/X/B,Y,D", output, "Environment not expanded");

  }

  /**
   * Test native IO.
   */
  @Test
  void testNativeIO() throws IOException {
    FrameworkUploader uploader = new FrameworkUploader();
    File parent = new File(testDir);
    try {
      // Create a parent directory
      parent.deleteOnExit();
      assertTrue(parent.mkdirs());

      // Create a target file
      File targetFile = new File(parent, "a.txt");
      try (FileOutputStream os = new FileOutputStream(targetFile)) {
        IOUtils.writeLines(Lists.newArrayList("a", "b"), null, os, StandardCharsets.UTF_8);
      }
      assertFalse(uploader.checkSymlink(targetFile));

      // Create a symlink to the target
      File symlinkToTarget = new File(parent, "symlinkToTarget.txt");
      try {
        Files.createSymbolicLink(
            Paths.get(symlinkToTarget.getAbsolutePath()),
            Paths.get(targetFile.getAbsolutePath()));
      } catch (UnsupportedOperationException e) {
        // Symlinks are not supported, so ignore the test
        Assumptions.assumeTrue(false);
      }
      assertTrue(uploader.checkSymlink(symlinkToTarget));

      // Create a symlink to the target with /./ in the path
      symlinkToTarget = new File(parent.getAbsolutePath() +
          "/./symlinkToTarget2.txt");
      try {
        Files.createSymbolicLink(
            Paths.get(symlinkToTarget.getAbsolutePath()),
            Paths.get(targetFile.getAbsolutePath()));
      } catch (UnsupportedOperationException e) {
        // Symlinks are not supported, so ignore the test
        Assumptions.assumeTrue(false);
      }
      assertTrue(uploader.checkSymlink(symlinkToTarget));

      // Create a symlink outside the current directory
      File symlinkOutside = new File(parent, "symlinkToParent.txt");
      try {
        Files.createSymbolicLink(
            Paths.get(symlinkOutside.getAbsolutePath()),
            Paths.get(parent.getAbsolutePath()));
      } catch (UnsupportedOperationException e) {
        // Symlinks are not supported, so ignore the test
        Assumptions.assumeTrue(false);
      }
      assertFalse(uploader.checkSymlink(symlinkOutside));
    } finally {
      FileUtils.forceDelete(parent);
    }

  }

  @Test
  void testPermissionSettingsOnRestrictiveUmask()
      throws Exception {
    File parent = new File(testDir);
    parent.deleteOnExit();
    MiniDFSCluster cluster = null;

    try {
      assertTrue(parent.mkdirs(), "Directory creation failed");
      Configuration hdfsConf = new HdfsConfiguration();
      String namenodeDir = new File(MiniDFSCluster.getBaseDirectory(),
          "name").getAbsolutePath();
      hdfsConf.set(DFSConfigKeys.DFS_NAMENODE_NAME_DIR_KEY, namenodeDir);
      hdfsConf.set(DFSConfigKeys.DFS_NAMENODE_EDITS_DIR_KEY, namenodeDir);
      hdfsConf.set(CommonConfigurationKeys.FS_PERMISSIONS_UMASK_KEY, "027");
      cluster = new MiniDFSCluster.Builder(hdfsConf)
          .numDataNodes(1).build();
      DistributedFileSystem dfs = cluster.getFileSystem();
      cluster.waitActive();

      File file1 = new File(parent, "a.jar");
      file1.createNewFile();
      File file2 = new File(parent, "b.jar");
      file2.createNewFile();
      File file3 = new File(parent, "c.jar");
      file3.createNewFile();

      FrameworkUploader uploader = new FrameworkUploader();
      uploader.whitelist = "";
      uploader.blacklist = "";
      uploader.input = parent.getAbsolutePath() + File.separatorChar + "*";
      String hdfsUri = hdfsConf.get(FS_DEFAULT_NAME_KEY);
      String targetPath = "/test.tar.gz";
      uploader.target = hdfsUri + targetPath;
      uploader.acceptableReplication = 1;
      uploader.setConf(hdfsConf);

      uploader.collectPackages();
      uploader.buildPackage();

      FileStatus fileStatus = dfs.getFileStatus(new Path(targetPath));
      FsPermission perm = fileStatus.getPermission();
      assertEquals(new FsPermission(0644), perm, "Permissions");
    } finally {
      if (cluster != null) {
        cluster.close();
      }
      FileUtils.deleteDirectory(parent);
    }
  }
}
