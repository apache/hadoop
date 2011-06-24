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

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.server.common.Storage.StorageDirectory;
import org.apache.hadoop.hdfs.server.namenode.FSImageTransactionalStorageInspector.FoundEditLog;
import org.apache.hadoop.hdfs.server.namenode.FSImageTransactionalStorageInspector.FoundFSImage;
import org.apache.hadoop.hdfs.server.namenode.NNStorage.NameNodeDirType;
import org.apache.hadoop.hdfs.server.namenode.NNStorageArchivalManager.StorageArchiver;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;


public class TestNNStorageArchivalManager {
  /**
   * Test the "easy case" where we have more images in the
   * directory than we need to keep. Should archive the
   * old ones.
   */
  @Test
  public void testArchiveEasyCase() throws IOException {
    TestCaseDescription tc = new TestCaseDescription();
    tc.addRoot("/foo1", NameNodeDirType.IMAGE_AND_EDITS);
    tc.addImage("/foo1/current/fsimage_100", true);
    tc.addImage("/foo1/current/fsimage_200", true);
    tc.addImage("/foo1/current/fsimage_300", false);
    tc.addImage("/foo1/current/fsimage_400", false);
    tc.addLog("/foo1/current/edits_101-200", true);
    tc.addLog("/foo1/current/edits_201-300", true);
    tc.addLog("/foo1/current/edits_301-400", false);
    tc.addLog("/foo1/current/edits_inprogress_401", false);
    
    // Test that other files don't get archived
    tc.addLog("/foo1/current/VERSION", false);
    runTest(tc);
  }
  
  /**
   * Same as above, but across multiple directories
   */
  @Test
  public void testArchiveMultipleDirs() throws IOException {
    TestCaseDescription tc = new TestCaseDescription();
    tc.addRoot("/foo1", NameNodeDirType.IMAGE_AND_EDITS);
    tc.addRoot("/foo2", NameNodeDirType.IMAGE_AND_EDITS);
    tc.addImage("/foo1/current/fsimage_100", true);
    tc.addImage("/foo1/current/fsimage_200", true);
    tc.addImage("/foo2/current/fsimage_200", true);
    tc.addImage("/foo1/current/fsimage_300", false);
    tc.addImage("/foo1/current/fsimage_400", false);
    tc.addLog("/foo1/current/edits_101-200", true);
    tc.addLog("/foo1/current/edits_201-300", true);
    tc.addLog("/foo2/current/edits_201-300", true);
    tc.addLog("/foo1/current/edits_301-400", false);
    tc.addLog("/foo2/current/edits_301-400", false);
    tc.addLog("/foo1/current/edits_inprogress_401", false);
    runTest(tc);
  }
  
  /**
   * Test that if we have fewer fsimages than the configured
   * retention, we don't archive any of them
   */
  @Test
  public void testArchiveLessThanRetention() throws IOException {
    TestCaseDescription tc = new TestCaseDescription();
    tc.addRoot("/foo1", NameNodeDirType.IMAGE_AND_EDITS);
    tc.addImage("/foo1/current/fsimage_100", false);
    tc.addLog("/foo1/current/edits_101-200", false);
    tc.addLog("/foo1/current/edits_201-300", false);
    tc.addLog("/foo1/current/edits_301-400", false);
    tc.addLog("/foo1/current/edits_inprogress_401", false);
    runTest(tc);
  }

  /**
   * Check for edge case with no logs present at all.
   */
  @Test
  public void testNoLogs() throws IOException {
    TestCaseDescription tc = new TestCaseDescription();
    tc.addRoot("/foo1", NameNodeDirType.IMAGE_AND_EDITS);
    tc.addImage("/foo1/current/fsimage_100", true);
    tc.addImage("/foo1/current/fsimage_200", true);
    tc.addImage("/foo1/current/fsimage_300", false);
    tc.addImage("/foo1/current/fsimage_400", false);
    runTest(tc);
  }
  
  /**
   * Check for edge case with no logs or images present at all.
   */
  @Test
  public void testEmptyDir() throws IOException {
    TestCaseDescription tc = new TestCaseDescription();
    tc.addRoot("/foo1", NameNodeDirType.IMAGE_AND_EDITS);
    runTest(tc);
  }

  /**
   * Test that old in-progress logs are properly archived
   */
  @Test
  public void testOldInProgress() throws IOException {
    TestCaseDescription tc = new TestCaseDescription();
    tc.addRoot("/foo1", NameNodeDirType.IMAGE_AND_EDITS);
    tc.addImage("/foo1/current/fsimage_100", true);
    tc.addImage("/foo1/current/fsimage_200", true);
    tc.addImage("/foo1/current/fsimage_300", false);
    tc.addImage("/foo1/current/fsimage_400", false);
    tc.addLog("/foo1/current/edits_inprogress_101", true);
    runTest(tc);
  }

  @Test
  public void testSeparateEditDirs() throws IOException {
    TestCaseDescription tc = new TestCaseDescription();
    tc.addRoot("/foo1", NameNodeDirType.IMAGE);
    tc.addRoot("/foo2", NameNodeDirType.EDITS);
    tc.addImage("/foo1/current/fsimage_100", true);
    tc.addImage("/foo1/current/fsimage_200", true);
    tc.addImage("/foo1/current/fsimage_300", false);
    tc.addImage("/foo1/current/fsimage_400", false);
    tc.addLog("/foo2/current/edits_101-200", true);
    tc.addLog("/foo2/current/edits_201-300", true);
    tc.addLog("/foo2/current/edits_301-400", false);
    tc.addLog("/foo2/current/edits_inprogress_401", false);
    runTest(tc);    
  }
  
  private void runTest(TestCaseDescription tc) throws IOException {
    Configuration conf = new Configuration();

    StorageArchiver mockArchiver =
      Mockito.mock(NNStorageArchivalManager.StorageArchiver.class);
    ArgumentCaptor<FoundFSImage> imagesArchivedCaptor =
      ArgumentCaptor.forClass(FoundFSImage.class);    
    ArgumentCaptor<FoundEditLog> logsArchivedCaptor =
      ArgumentCaptor.forClass(FoundEditLog.class);    

    // Ask the manager to archive files we don't need any more
    new NNStorageArchivalManager(conf,
        tc.mockStorage(), tc.mockEditLog(), mockArchiver)
      .archiveOldStorage();
    
    // Verify that it asked the archiver to remove the correct files
    Mockito.verify(mockArchiver, Mockito.atLeast(0))
      .archiveImage(imagesArchivedCaptor.capture());
    Mockito.verify(mockArchiver, Mockito.atLeast(0))
      .archiveLog(logsArchivedCaptor.capture());

    // Check images
    Set<String> archivedPaths = Sets.newHashSet();
    for (FoundFSImage archived : imagesArchivedCaptor.getAllValues()) {
      archivedPaths.add(archived.getFile().toString());
    }    
    Assert.assertEquals(Joiner.on(",").join(tc.expectedArchivedImages),
        Joiner.on(",").join(archivedPaths));

    // Check images
    archivedPaths.clear();
    for (FoundEditLog archived : logsArchivedCaptor.getAllValues()) {
      archivedPaths.add(archived.getFile().toString());
    }    
    Assert.assertEquals(Joiner.on(",").join(tc.expectedArchivedLogs),
        Joiner.on(",").join(archivedPaths));
  }
  
  private static class TestCaseDescription {
    private Map<String, FakeRoot> dirRoots = Maps.newHashMap();
    private Set<String> expectedArchivedLogs = Sets.newHashSet();
    private Set<String> expectedArchivedImages = Sets.newHashSet();
    
    private static class FakeRoot {
      NameNodeDirType type;
      List<String> files;
      
      FakeRoot(NameNodeDirType type) {
        this.type = type;
        files = Lists.newArrayList();
      }

      StorageDirectory mockStorageDir() {
        return TestFSImageStorageInspector.mockDirectory(
            type, false,
            files.toArray(new String[0]));
      }
    }

    void addRoot(String root, NameNodeDirType dir) {
      dirRoots.put(root, new FakeRoot(dir));
    }

    private void addFile(String path) {
      for (Map.Entry<String, FakeRoot> entry : dirRoots.entrySet()) {
        if (path.startsWith(entry.getKey())) {
          entry.getValue().files.add(path);
        }
      }
    }
    
    void addLog(String path, boolean expectArchive) {
      addFile(path);
      if (expectArchive) {
        expectedArchivedLogs.add(path);
      }
    }
    
    void addImage(String path, boolean expectArchive) {
      addFile(path);
      if (expectArchive) {
        expectedArchivedImages.add(path);
      }
    }
    
    NNStorage mockStorage() throws IOException {
      List<StorageDirectory> sds = Lists.newArrayList();
      for (FakeRoot root : dirRoots.values()) {
        sds.add(root.mockStorageDir());
      }
      return mockStorageForDirs(sds.toArray(new StorageDirectory[0]));
    }
    
    public FSEditLog mockEditLog() {
      final List<JournalManager> jms = Lists.newArrayList();
      for (FakeRoot root : dirRoots.values()) {
        if (!root.type.isOfType(NameNodeDirType.EDITS)) continue;
        
        FileJournalManager fjm = new FileJournalManager(
            root.mockStorageDir());
        jms.add(fjm);
      }

      FSEditLog mockLog = Mockito.mock(FSEditLog.class);
      Mockito.doAnswer(new Answer<Void>() {

        @Override
        public Void answer(InvocationOnMock invocation) throws Throwable {
          Object[] args = invocation.getArguments();
          assert args.length == 2;
          long txId = (Long) args[0];
          StorageArchiver archiver = (StorageArchiver) args[1];
          
          for (JournalManager jm : jms) {
            jm.archiveLogsOlderThan(txId, archiver);
          }
          return null;
        }
      }).when(mockLog).archiveLogsOlderThan(
          Mockito.anyLong(), (StorageArchiver) Mockito.anyObject());
      return mockLog;
    }
  }

  private static NNStorage mockStorageForDirs(final StorageDirectory ... mockDirs)
      throws IOException {
    NNStorage mockStorage = Mockito.mock(NNStorage.class);
    Mockito.doAnswer(new Answer<Void>() {
      @Override
      public Void answer(InvocationOnMock invocation) throws Throwable {
        FSImageStorageInspector inspector =
          (FSImageStorageInspector) invocation.getArguments()[0];
        for (StorageDirectory sd : mockDirs) {
          inspector.inspectDirectory(sd);
        }
        return null;
      }
    }).when(mockStorage).inspectStorageDirs(
        Mockito.<FSImageStorageInspector>anyObject());
    return mockStorage;
  }
}
