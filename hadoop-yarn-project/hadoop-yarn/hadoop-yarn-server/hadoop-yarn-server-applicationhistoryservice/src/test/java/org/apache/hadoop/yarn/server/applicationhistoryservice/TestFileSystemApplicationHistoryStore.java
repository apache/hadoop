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

package org.apache.hadoop.yarn.server.applicationhistoryservice;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.applicationhistoryservice.records.ApplicationAttemptHistoryData;
import org.apache.hadoop.yarn.server.applicationhistoryservice.records.ApplicationHistoryData;
import org.apache.hadoop.yarn.server.applicationhistoryservice.records.ContainerHistoryData;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class TestFileSystemApplicationHistoryStore extends
    ApplicationHistoryStoreTestUtils {

  private static final Logger LOG = LoggerFactory
      .getLogger(TestFileSystemApplicationHistoryStore.class.getName());

  private FileSystem fs;
  private Path fsWorkingPath;

  @BeforeEach
  public void setup() throws Exception {
    fs = new RawLocalFileSystem();
    initAndStartStore(fs);
  }

  private void initAndStartStore(final FileSystem fs) throws IOException,
      URISyntaxException {
    Configuration conf = new Configuration();
    fs.initialize(new URI("/"), conf);
    fsWorkingPath =
        new Path("target",
          TestFileSystemApplicationHistoryStore.class.getSimpleName());
    fs.delete(fsWorkingPath, true);
    conf.set(YarnConfiguration.FS_APPLICATION_HISTORY_STORE_URI, fsWorkingPath.toString());
    store = new FileSystemApplicationHistoryStore() {
      @Override
      protected FileSystem getFileSystem(Path path, Configuration conf) {
        return fs;
      }
    };
    store.init(conf);
    store.start();
  }

  @AfterEach
  public void tearDown() throws Exception {
    store.stop();
    fs.delete(fsWorkingPath, true);
    fs.close();
  }

  @Test
  void testReadWriteHistoryData() throws IOException {
    LOG.info("Starting testReadWriteHistoryData");
    testWriteHistoryData(5);
    testReadHistoryData(5);
  }

  private void testWriteHistoryData(int num) throws IOException {
    testWriteHistoryData(num, false, false);
  }
  
  private void testWriteHistoryData(
      int num, boolean missingContainer, boolean missingApplicationAttempt)
          throws IOException {
    // write application history data
    for (int i = 1; i <= num; ++i) {
      ApplicationId appId = ApplicationId.newInstance(0, i);
      writeApplicationStartData(appId);

      // write application attempt history data
      for (int j = 1; j <= num; ++j) {
        ApplicationAttemptId appAttemptId =
            ApplicationAttemptId.newInstance(appId, j);
        writeApplicationAttemptStartData(appAttemptId);

        if (missingApplicationAttempt && j == num) {
          continue;
        }
        // write container history data
        for (int k = 1; k <= num; ++k) {
          ContainerId containerId = ContainerId.newContainerId(appAttemptId, k);
          writeContainerStartData(containerId);
          if (missingContainer && k == num) {
            continue;
          }
          writeContainerFinishData(containerId);
        }
        writeApplicationAttemptFinishData(appAttemptId);
      }
      writeApplicationFinishData(appId);
    }
  }

  private void testReadHistoryData(int num) throws IOException {
    testReadHistoryData(num, false, false);
  }

  @SuppressWarnings("deprecation")
  private void testReadHistoryData(
      int num, boolean missingContainer, boolean missingApplicationAttempt)
          throws IOException {
    // read application history data
    assertEquals(num, store.getAllApplications().size());
    for (int i = 1; i <= num; ++i) {
      ApplicationId appId = ApplicationId.newInstance(0, i);
      ApplicationHistoryData appData = store.getApplication(appId);
      assertNotNull(appData);
      assertEquals(appId.toString(), appData.getApplicationName());
      assertEquals(appId.toString(), appData.getDiagnosticsInfo());

      // read application attempt history data
      assertEquals(num, store.getApplicationAttempts(appId).size());
      for (int j = 1; j <= num; ++j) {
        ApplicationAttemptId appAttemptId =
            ApplicationAttemptId.newInstance(appId, j);
        ApplicationAttemptHistoryData attemptData =
            store.getApplicationAttempt(appAttemptId);
        assertNotNull(attemptData);
        assertEquals(appAttemptId.toString(), attemptData.getHost());
        
        if (missingApplicationAttempt && j == num) {
          assertNull(attemptData.getDiagnosticsInfo());
          continue;
        } else {
          assertEquals(appAttemptId.toString(),
              attemptData.getDiagnosticsInfo());
        }

        // read container history data
        assertEquals(num, store.getContainers(appAttemptId).size());
        for (int k = 1; k <= num; ++k) {
          ContainerId containerId = ContainerId.newContainerId(appAttemptId, k);
          ContainerHistoryData containerData = store.getContainer(containerId);
          assertNotNull(containerData);
          assertEquals(Priority.newInstance(containerId.getId()), containerData.getPriority());
          if (missingContainer && k == num) {
            assertNull(containerData.getDiagnosticsInfo());
          } else {
            assertEquals(containerId.toString(),
                containerData.getDiagnosticsInfo());
          }
        }
        ContainerHistoryData masterContainer =
            store.getAMContainer(appAttemptId);
        assertNotNull(masterContainer);
        assertEquals(ContainerId.newContainerId(appAttemptId, 1), masterContainer.getContainerId());
      }
    }
  }

  @Test
  void testWriteAfterApplicationFinish() throws IOException {
    LOG.info("Starting testWriteAfterApplicationFinish");
    ApplicationId appId = ApplicationId.newInstance(0, 1);
    writeApplicationStartData(appId);
    writeApplicationFinishData(appId);
    // write application attempt history data
    ApplicationAttemptId appAttemptId =
        ApplicationAttemptId.newInstance(appId, 1);
    try {
      writeApplicationAttemptStartData(appAttemptId);
      fail();
    } catch (IOException e) {
      assertTrue(e.getMessage().contains("is not opened"));
    }
    try {
      writeApplicationAttemptFinishData(appAttemptId);
      fail();
    } catch (IOException e) {
      assertTrue(e.getMessage().contains("is not opened"));
    }
    // write container history data
    ContainerId containerId = ContainerId.newContainerId(appAttemptId, 1);
    try {
      writeContainerStartData(containerId);
      fail();
    } catch (IOException e) {
      assertTrue(e.getMessage().contains("is not opened"));
    }
    try {
      writeContainerFinishData(containerId);
      fail();
    } catch (IOException e) {
      assertTrue(e.getMessage().contains("is not opened"));
    }
  }

  @Test
  void testMassiveWriteContainerHistoryData() throws IOException {
    LOG.info("Starting testMassiveWriteContainerHistoryData");
    long mb = 1024 * 1024;
    long usedDiskBefore = fs.getContentSummary(fsWorkingPath).getLength() / mb;
    ApplicationId appId = ApplicationId.newInstance(0, 1);
    writeApplicationStartData(appId);
    ApplicationAttemptId appAttemptId =
        ApplicationAttemptId.newInstance(appId, 1);
    for (int i = 1; i <= 100000; ++i) {
      ContainerId containerId = ContainerId.newContainerId(appAttemptId, i);
      writeContainerStartData(containerId);
      writeContainerFinishData(containerId);
    }
    writeApplicationFinishData(appId);
    long usedDiskAfter = fs.getContentSummary(fsWorkingPath).getLength() / mb;
    assertTrue((usedDiskAfter - usedDiskBefore) < 20);
  }

  @Test
  void testMissingContainerHistoryData() throws IOException {
    LOG.info("Starting testMissingContainerHistoryData");
    testWriteHistoryData(3, true, false);
    testReadHistoryData(3, true, false);
  }

  @Test
  void testMissingApplicationAttemptHistoryData() throws IOException {
    LOG.info("Starting testMissingApplicationAttemptHistoryData");
    testWriteHistoryData(3, false, true);
    testReadHistoryData(3, false, true);
  }

  @Test
  void testInitExistingWorkingDirectoryInSafeMode() throws Exception {
    LOG.info("Starting testInitExistingWorkingDirectoryInSafeMode");
    tearDown();

    // Setup file system to inject startup conditions
    FileSystem fileSystem = spy(new RawLocalFileSystem());
    FileStatus fileStatus = Mockito.mock(FileStatus.class);
    doReturn(true).when(fileStatus).isDirectory();
    doReturn(fileStatus).when(fileSystem).getFileStatus(any(Path.class));

    try {
      initAndStartStore(fileSystem);
    } catch (Exception e) {
      fail("Exception should not be thrown: " + e);
    }

    // Make sure that directory creation was not attempted
    verify(fileStatus, never()).isDirectory();
    verify(fileSystem, times(1)).mkdirs(any(Path.class));
  }

  @Test
  void testInitNonExistingWorkingDirectoryInSafeMode() throws Exception {
    LOG.info("Starting testInitNonExistingWorkingDirectoryInSafeMode");
    tearDown();

    // Setup file system to inject startup conditions
    FileSystem fileSystem = spy(new RawLocalFileSystem());
    FileStatus fileStatus = Mockito.mock(FileStatus.class);
    doReturn(false).when(fileStatus).isDirectory();
    doReturn(fileStatus).when(fileSystem).getFileStatus(any(Path.class));
    doThrow(new IOException()).when(fileSystem).mkdirs(any(Path.class));

    try {
      initAndStartStore(fileSystem);
      fail("Exception should have been thrown");
    } catch (Exception e) {
      // Expected failure
    }

    // Make sure that directory creation was attempted
    verify(fileStatus, never()).isDirectory();
    verify(fileSystem, times(1)).mkdirs(any(Path.class));
  }
}
