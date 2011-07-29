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
package org.apache.hadoop.hdfs.server.namenode;

import static org.junit.Assert.*;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hdfs.server.common.Storage.StorageDirType;
import org.apache.hadoop.hdfs.server.common.Storage.StorageDirectory;
import org.apache.hadoop.hdfs.server.namenode.NNStorage.NameNodeDirType;
import static org.apache.hadoop.hdfs.server.namenode.NNStorage.getInProgressEditsFileName;
import static org.apache.hadoop.hdfs.server.namenode.NNStorage.getFinalizedEditsFileName;
import static org.apache.hadoop.hdfs.server.namenode.NNStorage.getImageFileName;

import org.apache.hadoop.hdfs.server.namenode.FSImageTransactionalStorageInspector.FoundEditLog;
import org.apache.hadoop.hdfs.server.namenode.FSImageTransactionalStorageInspector.FoundFSImage;
import org.apache.hadoop.hdfs.server.namenode.FSImageTransactionalStorageInspector.TransactionalLoadPlan;
import org.apache.hadoop.hdfs.server.namenode.FSImageTransactionalStorageInspector.LogGroup;
import org.apache.hadoop.hdfs.server.namenode.FSImageStorageInspector.LoadPlan;
import org.junit.Test;
import org.mockito.Mockito;

public class TestFSImageStorageInspector {
  private static final Log LOG = LogFactory.getLog(
      TestFSImageStorageInspector.class);

  /**
   * Simple test with image, edits, and inprogress edits
   */
  @Test
  public void testCurrentStorageInspector() throws IOException {
    FSImageTransactionalStorageInspector inspector = 
        new FSImageTransactionalStorageInspector();
    
    StorageDirectory mockDir = mockDirectory(
        NameNodeDirType.IMAGE_AND_EDITS,
        false,
        "/foo/current/" + getImageFileName(123),
        "/foo/current/" + getFinalizedEditsFileName(123, 456),
        "/foo/current/" + getImageFileName(456),
        "/foo/current/" + getInProgressEditsFileName(457));

    inspector.inspectDirectory(mockDir);
    mockLogValidation(inspector,
        "/foo/current/" + getInProgressEditsFileName(457), 10);
    
    assertEquals(2, inspector.foundEditLogs.size());
    assertEquals(2, inspector.foundImages.size());
    assertTrue(inspector.foundEditLogs.get(1).isInProgress());
    
    FoundFSImage latestImage = inspector.getLatestImage();
    assertEquals(456, latestImage.txId);
    assertSame(mockDir, latestImage.sd);
    assertTrue(inspector.isUpgradeFinalized());
    
    LoadPlan plan = inspector.createLoadPlan();
    LOG.info("Plan: " + plan);
    
    assertEquals(new File("/foo/current/"+getImageFileName(456)), 
                 plan.getImageFile());
    assertArrayEquals(new File[] {
        new File("/foo/current/" + getInProgressEditsFileName(457)) },
        plan.getEditsFiles().toArray(new File[0]));
  }
  
  /**
   * Test that we check for gaps in txids when devising a load plan.
   */
  @Test
  public void testPlanWithGaps() throws IOException {
    FSImageTransactionalStorageInspector inspector =
        new FSImageTransactionalStorageInspector();
    
    StorageDirectory mockDir = mockDirectory(
        NameNodeDirType.IMAGE_AND_EDITS,
        false,
        "/foo/current/" + getImageFileName(123),
        "/foo/current/" + getImageFileName(456),
        "/foo/current/" + getFinalizedEditsFileName(457,900),
        "/foo/current/" + getFinalizedEditsFileName(901,950),
        "/foo/current/" + getFinalizedEditsFileName(952,1000)); // <-- missing edit 951!

    inspector.inspectDirectory(mockDir);
    try {
      inspector.createLoadPlan();
      fail("Didn't throw IOE trying to load with gaps in edits");
    } catch (IOException ioe) {
      assertTrue(ioe.getMessage().contains(
          "would start at txid 951 but starts at txid 952"));
    }
  }
  
  /**
   * Test the case where an in-progress log comes in the middle of a sequence
   * of logs
   */
  @Test
  public void testPlanWithInProgressInMiddle() throws IOException {
    FSImageTransactionalStorageInspector inspector =
        new FSImageTransactionalStorageInspector();
    
    StorageDirectory mockDir = mockDirectory(
        NameNodeDirType.IMAGE_AND_EDITS,
        false,
        "/foo/current/" + getImageFileName(123),
        "/foo/current/" + getImageFileName(456),
        "/foo/current/" + getFinalizedEditsFileName(457,900),
        "/foo/current/" + getInProgressEditsFileName(901), // <-- inprogress in middle
        "/foo/current/" + getFinalizedEditsFileName(952,1000));

    inspector.inspectDirectory(mockDir);
    mockLogValidation(inspector,
        "/foo/current/" + getInProgressEditsFileName(901), 51);

    LoadPlan plan = inspector.createLoadPlan();
    LOG.info("Plan: " + plan);
    
    assertEquals(new File("/foo/current/" + getImageFileName(456)), 
                 plan.getImageFile());
    assertArrayEquals(new File[] {
        new File("/foo/current/" + getFinalizedEditsFileName(457,900)),
        new File("/foo/current/" + getInProgressEditsFileName(901)),
        new File("/foo/current/" + getFinalizedEditsFileName(952,1000)) },
        plan.getEditsFiles().toArray(new File[0]));

  }

  
  /**
   * Test case for the usual case where no recovery of a log group is necessary
   * (i.e all logs have the same start and end txids and finalized)
   */
  @Test
  public void testLogGroupRecoveryNoop() throws IOException {
    FSImageTransactionalStorageInspector inspector =
        new FSImageTransactionalStorageInspector();

    inspector.inspectDirectory(
        mockDirectoryWithEditLogs("/foo1/current/" 
                                  + getFinalizedEditsFileName(123,456)));
    inspector.inspectDirectory(
        mockDirectoryWithEditLogs("/foo2/current/"
                                  + getFinalizedEditsFileName(123,456)));
    inspector.inspectDirectory(
        mockDirectoryWithEditLogs("/foo3/current/"
                                  + getFinalizedEditsFileName(123,456)));
    LogGroup lg = inspector.logGroups.get(123L);
    assertEquals(3, lg.logs.size());
    
    lg.planRecovery();
    
    assertFalse(lg.logs.get(0).isCorrupt());
    assertFalse(lg.logs.get(1).isCorrupt());
    assertFalse(lg.logs.get(2).isCorrupt());
  }
  
  /**
   * Test case where we have some in-progress and some finalized logs
   * for a given txid.
   */
  @Test
  public void testLogGroupRecoveryMixed() throws IOException {
    FSImageTransactionalStorageInspector inspector =
        new FSImageTransactionalStorageInspector();

    inspector.inspectDirectory(
        mockDirectoryWithEditLogs("/foo1/current/"
                                  + getFinalizedEditsFileName(123,456)));
    inspector.inspectDirectory(
        mockDirectoryWithEditLogs("/foo2/current/"
                                  + getFinalizedEditsFileName(123,456)));
    inspector.inspectDirectory(
        mockDirectoryWithEditLogs("/foo3/current/"
                                  + getInProgressEditsFileName(123)));
    inspector.inspectDirectory(mockDirectory(
        NameNodeDirType.IMAGE,
        false,
        "/foo4/current/" + getImageFileName(122)));

    LogGroup lg = inspector.logGroups.get(123L);
    assertEquals(3, lg.logs.size());
    FoundEditLog inProgressLog = lg.logs.get(2);
    assertTrue(inProgressLog.isInProgress());
    
    LoadPlan plan = inspector.createLoadPlan();

    // Check that it was marked corrupt.
    assertFalse(lg.logs.get(0).isCorrupt());
    assertFalse(lg.logs.get(1).isCorrupt());
    assertTrue(lg.logs.get(2).isCorrupt());

    
    // Calling recover should move it aside
    inProgressLog = spy(inProgressLog);
    Mockito.doNothing().when(inProgressLog).moveAsideCorruptFile();
    lg.logs.set(2, inProgressLog);
    
    plan.doRecovery();
    
    Mockito.verify(inProgressLog).moveAsideCorruptFile();
  }
  
  /**
   * Test case where we have finalized logs with different end txids
   */
  @Test
  public void testLogGroupRecoveryInconsistentEndTxIds() throws IOException {
    FSImageTransactionalStorageInspector inspector =
        new FSImageTransactionalStorageInspector();
    inspector.inspectDirectory(
        mockDirectoryWithEditLogs("/foo1/current/"
                                  + getFinalizedEditsFileName(123,456)));
    inspector.inspectDirectory(
        mockDirectoryWithEditLogs("/foo2/current/"
                                  + getFinalizedEditsFileName(123,678)));

    LogGroup lg = inspector.logGroups.get(123L);
    assertEquals(2, lg.logs.size());

    try {
      lg.planRecovery();
      fail("Didn't throw IOE on inconsistent end txids");
    } catch (IOException ioe) {
      assertTrue(ioe.getMessage().contains("More than one ending txid"));
    }
  }

  /**
   * Test case where we have only in-progress logs and need to synchronize
   * based on valid length.
   */
  @Test
  public void testLogGroupRecoveryInProgress() throws IOException {
    String paths[] = new String[] {
        "/foo1/current/" + getInProgressEditsFileName(123),
        "/foo2/current/" + getInProgressEditsFileName(123),
        "/foo3/current/" + getInProgressEditsFileName(123)
    };
    FSImageTransactionalStorageInspector inspector =
        new FSImageTransactionalStorageInspector();
    inspector.inspectDirectory(mockDirectoryWithEditLogs(paths[0]));
    inspector.inspectDirectory(mockDirectoryWithEditLogs(paths[1]));
    inspector.inspectDirectory(mockDirectoryWithEditLogs(paths[2]));

    // Inject spies to return the valid counts we would like to see
    mockLogValidation(inspector, paths[0], 2000);
    mockLogValidation(inspector, paths[1], 2000);
    mockLogValidation(inspector, paths[2], 1000);

    LogGroup lg = inspector.logGroups.get(123L);
    assertEquals(3, lg.logs.size());
    
    lg.planRecovery();
    
    // Check that the short one was marked corrupt
    assertFalse(lg.logs.get(0).isCorrupt());
    assertFalse(lg.logs.get(1).isCorrupt());
    assertTrue(lg.logs.get(2).isCorrupt());
    
    // Calling recover should move it aside
    FoundEditLog badLog = lg.logs.get(2);
    Mockito.doNothing().when(badLog).moveAsideCorruptFile();
    Mockito.doNothing().when(lg.logs.get(0)).finalizeLog();
    Mockito.doNothing().when(lg.logs.get(1)).finalizeLog();
    
    lg.recover();
    
    Mockito.verify(badLog).moveAsideCorruptFile();
    Mockito.verify(lg.logs.get(0)).finalizeLog();
    Mockito.verify(lg.logs.get(1)).finalizeLog();
  }

  /**
   * Mock out the log at the given path to return a specified number
   * of transactions upon validation.
   */
  private void mockLogValidation(
      FSImageTransactionalStorageInspector inspector,
      String path, int numValidTransactions) throws IOException {
    
    for (LogGroup lg : inspector.logGroups.values()) {
      List<FoundEditLog> logs = lg.logs;
      for (int i = 0; i < logs.size(); i++) {
        FoundEditLog log = logs.get(i);
        if (log.file.getPath().equals(path)) {
          // mock out its validation
          FoundEditLog spyLog = spy(log);
          doReturn(new FSEditLogLoader.EditLogValidation(-1, numValidTransactions))
            .when(spyLog).validateLog();
          logs.set(i, spyLog);
          return;
        }
      }
    }
    fail("No log found to mock out at " + path);
  }

  /**
   * Test when edits and image are in separate directories.
   */
  @Test
  public void testCurrentSplitEditsAndImage() throws IOException {
    FSImageTransactionalStorageInspector inspector =
        new FSImageTransactionalStorageInspector();
    
    StorageDirectory mockImageDir = mockDirectory(
        NameNodeDirType.IMAGE,
        false,
        "/foo/current/" + getImageFileName(123));
    StorageDirectory mockImageDir2 = mockDirectory(
        NameNodeDirType.IMAGE,
        false,
        "/foo2/current/" + getImageFileName(456));
    StorageDirectory mockEditsDir = mockDirectory(
        NameNodeDirType.EDITS,
        false,
        "/foo3/current/" + getFinalizedEditsFileName(123, 456),
        "/foo3/current/" + getInProgressEditsFileName(457));
    
    inspector.inspectDirectory(mockImageDir);
    inspector.inspectDirectory(mockEditsDir);
    inspector.inspectDirectory(mockImageDir2);
    
    mockLogValidation(inspector,
        "/foo3/current/" + getInProgressEditsFileName(457), 2);

    assertEquals(2, inspector.foundEditLogs.size());
    assertEquals(2, inspector.foundImages.size());
    assertTrue(inspector.foundEditLogs.get(1).isInProgress());
    assertTrue(inspector.isUpgradeFinalized());    

    // Check plan
    TransactionalLoadPlan plan =
      (TransactionalLoadPlan)inspector.createLoadPlan();
    FoundFSImage pickedImage = plan.image;
    assertEquals(456, pickedImage.txId);
    assertSame(mockImageDir2, pickedImage.sd);
    assertEquals(new File("/foo2/current/" + getImageFileName(456)),
                 plan.getImageFile());
    assertArrayEquals(new File[] {
        new File("/foo3/current/" + getInProgressEditsFileName(457))
      }, plan.getEditsFiles().toArray(new File[0]));

    // Check log manifest
    assertEquals("[[123,456]]", inspector.getEditLogManifest(123).toString());
    assertEquals("[[123,456]]", inspector.getEditLogManifest(456).toString());
    assertEquals("[]", inspector.getEditLogManifest(457).toString());
  }
  
  @Test
  public void testLogManifest() throws IOException { 
    FSImageTransactionalStorageInspector inspector =
        new FSImageTransactionalStorageInspector();
    inspector.inspectDirectory(
        mockDirectoryWithEditLogs("/foo1/current/" 
                                  + getFinalizedEditsFileName(1,1),
                                  "/foo1/current/"
                                  + getFinalizedEditsFileName(2,200)));
    inspector.inspectDirectory(
        mockDirectoryWithEditLogs("/foo2/current/" 
                                  + getInProgressEditsFileName(1),
                                  "/foo2/current/"
                                  + getFinalizedEditsFileName(201, 400)));
    inspector.inspectDirectory(
        mockDirectoryWithEditLogs("/foo3/current/"
                                  + getFinalizedEditsFileName(1, 1),
                                  "/foo3/current/"
                                  + getFinalizedEditsFileName(2,200)));
    
    assertEquals("[[1,1], [2,200], [201,400]]",
                 inspector.getEditLogManifest(1).toString());
    assertEquals("[[2,200], [201,400]]",
                 inspector.getEditLogManifest(2).toString());
    assertEquals("[[2,200], [201,400]]",
                 inspector.getEditLogManifest(10).toString());
    assertEquals("[[201,400]]",
                 inspector.getEditLogManifest(201).toString());
  }  

  /**
   * Test case where an in-progress log is in an earlier name directory
   * than a finalized log. Previously, getEditLogManifest wouldn't
   * see this log.
   */
  @Test
  public void testLogManifestInProgressComesFirst() throws IOException { 
    FSImageTransactionalStorageInspector inspector =
        new FSImageTransactionalStorageInspector();
    inspector.inspectDirectory(
        mockDirectoryWithEditLogs("/foo1/current/" 
                                  + getFinalizedEditsFileName(2622,2623),
                                  "/foo1/current/"
                                  + getFinalizedEditsFileName(2624,2625),
                                  "/foo1/current/"
                                  + getInProgressEditsFileName(2626)));
    inspector.inspectDirectory(
        mockDirectoryWithEditLogs("/foo2/current/"
                                  + getFinalizedEditsFileName(2622,2623),
                                  "/foo2/current/"
                                  + getFinalizedEditsFileName(2624,2625),
                                  "/foo2/current/"
                                  + getFinalizedEditsFileName(2626,2627),
                                  "/foo2/current/"
                                  + getFinalizedEditsFileName(2628,2629)));
    
    assertEquals("[[2622,2623], [2624,2625], [2626,2627], [2628,2629]]",
                 inspector.getEditLogManifest(2621).toString());
  }  
  
  private StorageDirectory mockDirectoryWithEditLogs(String... fileNames) {
    return mockDirectory(NameNodeDirType.EDITS, false, fileNames);
  }
  
  /**
   * Make a mock storage directory that returns some set of file contents.
   * @param type type of storage dir
   * @param previousExists should we mock that the previous/ dir exists?
   * @param fileNames the names of files contained in current/
   */
  static StorageDirectory mockDirectory(
      StorageDirType type,
      boolean previousExists,
      String...  fileNames) {
    StorageDirectory sd = mock(StorageDirectory.class);
    
    doReturn(type).when(sd).getStorageDirType();

    // Version file should always exist
    doReturn(FSImageTestUtil.mockFile(true)).when(sd).getVersionFile();
    
    // Previous dir optionally exists
    doReturn(FSImageTestUtil.mockFile(previousExists))
      .when(sd).getPreviousDir();   

    // Return a mock 'current' directory which has the given paths
    File[] files = new File[fileNames.length];
    for (int i = 0; i < fileNames.length; i++) {
      files[i] = new File(fileNames[i]);
    }
    
    File mockDir = Mockito.spy(new File("/dir/current"));
    doReturn(files).when(mockDir).listFiles();
    doReturn(mockDir).when(sd).getCurrentDir();
    
    return sd;
  }
}
