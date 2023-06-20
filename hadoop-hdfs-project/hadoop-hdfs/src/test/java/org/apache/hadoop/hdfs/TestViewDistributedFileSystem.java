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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.CommonPathCapabilities;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LeaseRecoverable;
import org.apache.hadoop.fs.Options;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathHandle;
import org.apache.hadoop.fs.SafeMode;
import org.apache.hadoop.fs.SafeModeAction;
import org.apache.hadoop.fs.viewfs.ConfigUtil;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.test.Whitebox;
import org.apache.hadoop.util.functional.ConsumerRaisingIOE;

import org.assertj.core.api.Assertions;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;

import static org.apache.hadoop.fs.CommonPathCapabilities.LEASE_RECOVERABLE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TestViewDistributedFileSystem extends TestDistributedFileSystem{
  @Override
  HdfsConfiguration getTestConfiguration() {
    HdfsConfiguration conf = super.getTestConfiguration();
    conf.set("fs.hdfs.impl", ViewDistributedFileSystem.class.getName());
    return conf;
  }

  @Override
  public void testStatistics() throws IOException {
    FileSystem.getStatistics(HdfsConstants.HDFS_URI_SCHEME,
        ViewDistributedFileSystem.class).reset();
    @SuppressWarnings("unchecked")
    ThreadLocal<FileSystem.Statistics.StatisticsData> data =
        (ThreadLocal<FileSystem.Statistics.StatisticsData>) Whitebox
            .getInternalState(FileSystem
                .getStatistics(HdfsConstants.HDFS_URI_SCHEME,
                    ViewDistributedFileSystem.class), "threadData");
    data.set(null);
    super.testStatistics();
  }

  @Test
  public void testOpenWithPathHandle() throws Exception {
    Configuration conf = getTestConfiguration();
    MiniDFSCluster cluster = null;
    try {
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(1).build();
      FileSystem fileSys = cluster.getFileSystem();
      Path openTestPath = new Path("/testOpen");
      fileSys.create(openTestPath).close();
      PathHandle pathHandle =
          fileSys.getPathHandle(fileSys.getFileStatus(openTestPath));
      fileSys.open(pathHandle, 1024).close();
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  @Override
  public void testEmptyDelegationToken() throws IOException {
    Configuration conf = getTestConfiguration();
    MiniDFSCluster cluster = null;
    try {
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(0).build();
      URI defaultUri =
          URI.create(conf.get(CommonConfigurationKeys.FS_DEFAULT_NAME_KEY));
      ConfigUtil.addLinkFallback(conf, defaultUri.getHost(), defaultUri);
      try (FileSystem fileSys = FileSystem.get(conf)) {
        fileSys.getDelegationToken("");
      }
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  @Test
  public void testRenameWithOptions() throws IOException {
    Configuration conf = getTestConfiguration();
    MiniDFSCluster cluster = null;
    try {
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(0).build();
      URI defaultUri =
          URI.create(conf.get(CommonConfigurationKeys.FS_DEFAULT_NAME_KEY));
      conf.set("fs.viewfs.mounttable." + defaultUri.getHost() + ".linkFallback",
          defaultUri.toString());
      conf.setLong(CommonConfigurationKeys.FS_TRASH_INTERVAL_KEY, 30000);
      try (ViewDistributedFileSystem fileSystem =
          (ViewDistributedFileSystem) FileSystem.get(conf)) {
        final Path testDir = new Path("/test");
        final Path renameDir = new Path("/testRename");
        fileSystem.mkdirs(testDir);
        fileSystem.rename(testDir, renameDir, Options.Rename.TO_TRASH);
        Assert.assertTrue(fileSystem.exists(renameDir));
        Assert.assertFalse(fileSystem.exists(testDir));
      }
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  @Test
  public void testRenameWithOptionsWithMountEntries() throws IOException {
    Configuration conf = getTestConfiguration();
    MiniDFSCluster cluster = null;
    try {
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(1).build();
      URI defaultUri =
          URI.create(conf.get(CommonConfigurationKeys.FS_DEFAULT_NAME_KEY));
      conf.set("fs.viewfs.mounttable." + defaultUri.getHost() + ".linkFallback",
          defaultUri.toString());
      Path target = new Path(defaultUri.toString(), "/src");
      ConfigUtil.addLink(conf, defaultUri.getHost(), "/source",
          target.toUri());
      FileSystem defaultFs = FileSystem.get(defaultUri, conf);
      defaultFs.mkdirs(target);
      try (ViewDistributedFileSystem fileSystem = (ViewDistributedFileSystem) FileSystem
          .get(conf)) {
        final Path testDir = new Path("/source");
        Path filePath = new Path(testDir, "file");
        Path renamedFilePath = new Path(testDir, "fileRename");
        // Create a file.
        fileSystem.create(filePath).close();
        // Check the file exists before rename is called.
        assertTrue(fileSystem.exists(filePath));
        fileSystem.rename(filePath, renamedFilePath, Options.Rename.NONE);
        // Check the file is not present at source location post a rename.
        assertFalse(fileSystem.exists(filePath));
        // Check the file is there at target location post rename.
        assertTrue(fileSystem.exists(renamedFilePath));
      }
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  @Test
  public void testQuota() throws IOException {
    Configuration conf = getTestConfiguration();
    MiniDFSCluster cluster = null;
    try {
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(0).build();
      URI defaultUri =
          URI.create(conf.get(CommonConfigurationKeys.FS_DEFAULT_NAME_KEY));
      conf.set("fs.viewfs.mounttable." + defaultUri.getHost() + ".linkFallback",
          defaultUri.toString());
      Path target = new Path(defaultUri.toString(), "/src");
      // /source -> /src
      ConfigUtil.addLink(conf, defaultUri.getHost(), "/source",
          target.toUri());
      FileSystem defaultFs = FileSystem.get(defaultUri, conf);
      defaultFs.mkdirs(target);
      try (ViewDistributedFileSystem fileSystem = (ViewDistributedFileSystem) FileSystem
          .get(conf)) {
        final Path testDir = new Path("/source");
        // Set Quota via ViewDFS
        fileSystem.setQuota(testDir, 10L, 10L);
        // Check quota through actual DFS
        assertEquals(10,
            defaultFs.getQuotaUsage(target).getSpaceQuota());
      }
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  @Test
  public void testPathCapabilities() throws IOException {
    try (MiniDFSCluster cluster = new MiniDFSCluster.Builder(getViewFsConfiguration())
        .numDataNodes(0).build();
        ViewDistributedFileSystem fileSystem = (ViewDistributedFileSystem) FileSystem.get(
            cluster.getConfiguration(0))) {
      final Path testFile = new Path("/test");
      assertTrue("ViewDfs supports truncate",
          fileSystem.hasPathCapability(testFile, CommonPathCapabilities.FS_TRUNCATE));
      final boolean isLeaseRecoverable = fileSystem.hasPathCapability(testFile, LEASE_RECOVERABLE);
      assertThat(isLeaseRecoverable).describedAs("path capabilities %s=%s in %s",
          LEASE_RECOVERABLE, fileSystem.hasPathCapability(testFile, LEASE_RECOVERABLE),
          fileSystem).isTrue();
      assertThat(fileSystem).describedAs("filesystem %s", fileSystem)
          .isInstanceOf(LeaseRecoverable.class);
      assertThat(fileSystem).describedAs("filesystem %s", fileSystem).isInstanceOf(SafeMode.class);
    }
  }

  @Test
  public void testSafeMode() throws IOException {
    testSafeMode(this::executeAssertionsWithSafeMode);
  }

  @Test
  public void testSafeModeWithDeprecatedAPIs() throws IOException {
    testSafeMode(this::executeAssertionsWithDeprecatedAPIs);
  }

  private void testSafeMode(ConsumerRaisingIOE<ViewDistributedFileSystem> executeAssertionsFunction)
      throws IOException {
    try (MiniDFSCluster cluster = new MiniDFSCluster.Builder(getViewFsConfiguration())
        .numDataNodes(0).build();
        ViewDistributedFileSystem fileSystem = (ViewDistributedFileSystem) FileSystem.get(
            cluster.getConfiguration(0))) {
      executeAssertionsFunction.accept(fileSystem);
    }
  }

  private SafeMode verifyAndGetSafeModeInstance(FileSystem fs) {
    Assertions.assertThat(fs)
        .describedAs("File system %s must be an instance of %s", fs, SafeMode.class.getClass())
        .isInstanceOf(SafeMode.class);
    return (SafeMode) fs;
  }

  private void executeAssertionsWithSafeMode(ViewDistributedFileSystem fileSystem)
      throws IOException {
    SafeMode fsWithSafeMode = verifyAndGetSafeModeInstance(fileSystem);
    assertSafeModeStatus(fsWithSafeMode, SafeModeAction.GET, false,
        "Getting the status of safe mode before entering should be off.");
    assertSafeModeStatus(fsWithSafeMode, SafeModeAction.ENTER, true,
        "Entering Safe mode and safe mode turns on.");
    assertSafeModeStatus(fsWithSafeMode, SafeModeAction.GET, true,
        "Getting the status of safe mode after entering, safe mode should be on.");
    assertSafeModeStatus(fsWithSafeMode, SafeModeAction.LEAVE, false,
        "Leaving safe mode, and safe mode switches off.");
    assertSafeModeStatus(fsWithSafeMode, SafeModeAction.FORCE_EXIT, false,
        "Force exist safe mode at any time, safe mode should always switches off.");
  }

  private void executeAssertionsWithDeprecatedAPIs(ViewDistributedFileSystem fileSystem)
      throws IOException {
    assertSafeModeStatus(fileSystem, HdfsConstants.SafeModeAction.SAFEMODE_GET, false,
        "Getting the status of safe mode before entering should be off.");
    assertSafeModeStatus(fileSystem, HdfsConstants.SafeModeAction.SAFEMODE_ENTER, true,
        "Entering Safe mode and safe mode turns on.");
    assertSafeModeStatus(fileSystem, HdfsConstants.SafeModeAction.SAFEMODE_GET, true,
        "Getting the status of safe mode after entering, safe mode should be on.");
    assertSafeModeStatus(fileSystem, HdfsConstants.SafeModeAction.SAFEMODE_LEAVE, false,
        "Leaving safe mode, and safe mode switches off.");
    assertSafeModeStatus(fileSystem, HdfsConstants.SafeModeAction.SAFEMODE_FORCE_EXIT, false,
        "Force exist safe mode at any time, safe mode should always switches off.");
  }

  private void assertSafeModeStatus(SafeMode fsWithSafeMode, SafeModeAction action,
      boolean expectedStatus, String message) throws IOException {
    Assertions.assertThat(fsWithSafeMode.setSafeMode(action)).describedAs(message)
        .isEqualTo(expectedStatus);
  }

  private void assertSafeModeStatus(ViewDistributedFileSystem fileSystem,
      HdfsConstants.SafeModeAction action, boolean expectedStatus, String message)
      throws IOException {
    Assertions.assertThat(fileSystem.setSafeMode(action)).describedAs(message)
        .isEqualTo(expectedStatus);
  }

  private Configuration getViewFsConfiguration() {
    Configuration conf = getTestConfiguration();
    URI defaultUri = URI.create(conf.get(CommonConfigurationKeys.FS_DEFAULT_NAME_KEY));
    conf.set("fs.viewfs.mounttable." + defaultUri.getHost() + ".linkFallback",
        defaultUri.toString());
    return conf;
  }

}
