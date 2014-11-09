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

import org.junit.Assert;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
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
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestFileSystemApplicationHistoryStore extends
    ApplicationHistoryStoreTestUtils {

  private static Log LOG = LogFactory
    .getLog(TestFileSystemApplicationHistoryStore.class.getName());

  private FileSystem fs;
  private Path fsWorkingPath;

  @Before
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
    conf.set(YarnConfiguration.FS_APPLICATION_HISTORY_STORE_URI,
      fsWorkingPath.toString());
    store = new FileSystemApplicationHistoryStore() {
      @Override
      protected FileSystem getFileSystem(Path path, Configuration conf) {
        return fs;
      }
    };
    store.init(conf);
    store.start();
  }

  @After
  public void tearDown() throws Exception {
    store.stop();
    fs.delete(fsWorkingPath, true);
    fs.close();
  }

  @Test
  public void testReadWriteHistoryData() throws IOException {
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
    Assert.assertEquals(num, store.getAllApplications().size());
    for (int i = 1; i <= num; ++i) {
      ApplicationId appId = ApplicationId.newInstance(0, i);
      ApplicationHistoryData appData = store.getApplication(appId);
      Assert.assertNotNull(appData);
      Assert.assertEquals(appId.toString(), appData.getApplicationName());
      Assert.assertEquals(appId.toString(), appData.getDiagnosticsInfo());

      // read application attempt history data
      Assert.assertEquals(num, store.getApplicationAttempts(appId).size());
      for (int j = 1; j <= num; ++j) {
        ApplicationAttemptId appAttemptId =
            ApplicationAttemptId.newInstance(appId, j);
        ApplicationAttemptHistoryData attemptData =
            store.getApplicationAttempt(appAttemptId);
        Assert.assertNotNull(attemptData);
        Assert.assertEquals(appAttemptId.toString(), attemptData.getHost());
        
        if (missingApplicationAttempt && j == num) {
          Assert.assertNull(attemptData.getDiagnosticsInfo());
          continue;
        } else {
          Assert.assertEquals(appAttemptId.toString(),
              attemptData.getDiagnosticsInfo());
        }

        // read container history data
        Assert.assertEquals(num, store.getContainers(appAttemptId).size());
        for (int k = 1; k <= num; ++k) {
          ContainerId containerId = ContainerId.newContainerId(appAttemptId, k);
          ContainerHistoryData containerData = store.getContainer(containerId);
          Assert.assertNotNull(containerData);
          Assert.assertEquals(Priority.newInstance(containerId.getId()),
            containerData.getPriority());
          if (missingContainer && k == num) {
            Assert.assertNull(containerData.getDiagnosticsInfo());
          } else {
            Assert.assertEquals(containerId.toString(),
                containerData.getDiagnosticsInfo());
          }
        }
        ContainerHistoryData masterContainer =
            store.getAMContainer(appAttemptId);
        Assert.assertNotNull(masterContainer);
        Assert.assertEquals(ContainerId.newContainerId(appAttemptId, 1),
          masterContainer.getContainerId());
      }
    }
  }

  @Test
  public void testWriteAfterApplicationFinish() throws IOException {
    LOG.info("Starting testWriteAfterApplicationFinish");
    ApplicationId appId = ApplicationId.newInstance(0, 1);
    writeApplicationStartData(appId);
    writeApplicationFinishData(appId);
    // write application attempt history data
    ApplicationAttemptId appAttemptId =
        ApplicationAttemptId.newInstance(appId, 1);
    try {
      writeApplicationAttemptStartData(appAttemptId);
      Assert.fail();
    } catch (IOException e) {
      Assert.assertTrue(e.getMessage().contains("is not opened"));
    }
    try {
      writeApplicationAttemptFinishData(appAttemptId);
      Assert.fail();
    } catch (IOException e) {
      Assert.assertTrue(e.getMessage().contains("is not opened"));
    }
    // write container history data
    ContainerId containerId = ContainerId.newContainerId(appAttemptId, 1);
    try {
      writeContainerStartData(containerId);
      Assert.fail();
    } catch (IOException e) {
      Assert.assertTrue(e.getMessage().contains("is not opened"));
    }
    try {
      writeContainerFinishData(containerId);
      Assert.fail();
    } catch (IOException e) {
      Assert.assertTrue(e.getMessage().contains("is not opened"));
    }
  }

  @Test
  public void testMassiveWriteContainerHistoryData() throws IOException {
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
    Assert.assertTrue((usedDiskAfter - usedDiskBefore) < 20);
  }

  @Test
  public void testMissingContainerHistoryData() throws IOException {
    LOG.info("Starting testMissingContainerHistoryData");
    testWriteHistoryData(3, true, false);
    testReadHistoryData(3, true, false);
  }
  
  @Test
  public void testMissingApplicationAttemptHistoryData() throws IOException {
    LOG.info("Starting testMissingApplicationAttemptHistoryData");
    testWriteHistoryData(3, false, true);
    testReadHistoryData(3, false, true);
  }

  @Test
  public void testInitExistingWorkingDirectoryInSafeMode() throws Exception {
    LOG.info("Starting testInitExistingWorkingDirectoryInSafeMode");
    tearDown();

    // Setup file system to inject startup conditions
    FileSystem fs = spy(new RawLocalFileSystem());
    doReturn(true).when(fs).isDirectory(any(Path.class));

    try {
      initAndStartStore(fs);
    } catch (Exception e) {
      Assert.fail("Exception should not be thrown: " + e);
    }

    // Make sure that directory creation was not attempted
    verify(fs, times(1)).isDirectory(any(Path.class));
    verify(fs, times(0)).mkdirs(any(Path.class));
  }

  @Test
  public void testInitNonExistingWorkingDirectoryInSafeMode() throws Exception {
    LOG.info("Starting testInitNonExistingWorkingDirectoryInSafeMode");
    tearDown();

    // Setup file system to inject startup conditions
    FileSystem fs = spy(new RawLocalFileSystem());
    doReturn(false).when(fs).isDirectory(any(Path.class));
    doThrow(new IOException()).when(fs).mkdirs(any(Path.class));

    try {
      initAndStartStore(fs);
      Assert.fail("Exception should have been thrown");
    } catch (Exception e) {
      // Expected failure
    }

    // Make sure that directory creation was attempted
    verify(fs, times(1)).isDirectory(any(Path.class));
    verify(fs, times(1)).mkdirs(any(Path.class));
  }
}
