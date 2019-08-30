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

import static org.apache.hadoop.hdfs.server.namenode.NNStorage.getFinalizedEditsFileName;
import static org.apache.hadoop.hdfs.server.namenode.NNStorage.getImageFileName;
import static org.apache.hadoop.hdfs.server.namenode.NNStorage.getInProgressEditsFileName;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.ToLongFunction;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.common.Storage.StorageDirectory;
import org.apache.hadoop.hdfs.server.namenode.FSImageStorageInspector.FSImageFile;
import org.apache.hadoop.hdfs.server.namenode.FileJournalManager.EditLogFile;
import org.apache.hadoop.hdfs.server.namenode.NNStorage.NameNodeDirType;
import org.apache.hadoop.hdfs.server.namenode.NNStorage.NameNodeFile;
import org.apache.hadoop.hdfs.server.namenode.NNStorageRetentionManager.StoragePurger;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;


public class TestNNStorageRetentionManager {
  final Configuration conf = new Configuration();

  /**
   * For the purpose of this test, purge as many edits as we can 
   * with no extra "safety cushion"
   */
  @Before
  public void setNoExtraEditRetention() {
    conf.setLong(DFSConfigKeys.DFS_NAMENODE_NUM_EXTRA_EDITS_RETAINED_KEY, 0);
  }
  
  /**
   * Test the "easy case" where we have more images in the
   * directory than we need to keep. Should purge the
   * old ones.
   */
  @Test
  public void testPurgeEasyCase() throws IOException {
    TestCaseDescription tc = new TestCaseDescription();
    tc.addRoot("/foo1", NameNodeDirType.IMAGE_AND_EDITS);
    tc.addImage("/foo1/current/" + getImageFileName(100), true);
    tc.addImage("/foo1/current/" + getImageFileName(200), true);
    tc.addImage("/foo1/current/" + getImageFileName(300), false);
    tc.addImage("/foo1/current/" + getImageFileName(400), false);
    tc.addLog("/foo1/current/" + getFinalizedEditsFileName(101,200), true);
    tc.addLog("/foo1/current/" + getFinalizedEditsFileName(201,300), true);
    tc.addLog("/foo1/current/" + getFinalizedEditsFileName(301,400), false);
    tc.addLog("/foo1/current/" + getInProgressEditsFileName(401), false);
    
    // Test that other files don't get purged
    tc.addLog("/foo1/current/VERSION", false);
    runTest(tc);
  }
  
  /**
   * Same as above, but across multiple directories
   */
  @Test
  public void testPurgeMultipleDirs() throws IOException {
    TestCaseDescription tc = new TestCaseDescription();
    tc.addRoot("/foo1", NameNodeDirType.IMAGE_AND_EDITS);
    tc.addRoot("/foo2", NameNodeDirType.IMAGE_AND_EDITS);
    tc.addImage("/foo1/current/" + getImageFileName(100), true);
    tc.addImage("/foo1/current/" + getImageFileName(200), true);
    tc.addImage("/foo2/current/" + getImageFileName(200), true);
    tc.addImage("/foo1/current/" + getImageFileName(300), false);
    tc.addImage("/foo1/current/" + getImageFileName(400), false);
    tc.addLog("/foo1/current/" + getFinalizedEditsFileName(101, 200), true);
    tc.addLog("/foo1/current/" + getFinalizedEditsFileName(201, 300), true);
    tc.addLog("/foo2/current/" + getFinalizedEditsFileName(201, 300), true);
    tc.addLog("/foo1/current/" + getFinalizedEditsFileName(301, 400), false);
    tc.addLog("/foo2/current/" + getFinalizedEditsFileName(301, 400), false);
    tc.addLog("/foo1/current/" + getInProgressEditsFileName(401), false);
    runTest(tc);
  }
  
  /**
   * Test that if we have fewer fsimages than the configured
   * retention, we don't purge any of them
   */
  @Test
  public void testPurgeLessThanRetention() throws IOException {
    TestCaseDescription tc = new TestCaseDescription();
    tc.addRoot("/foo1", NameNodeDirType.IMAGE_AND_EDITS);
    tc.addImage("/foo1/current/" + getImageFileName(100), false);
    tc.addLog("/foo1/current/" + getFinalizedEditsFileName(101,200), false);
    tc.addLog("/foo1/current/" + getFinalizedEditsFileName(201,300), false);
    tc.addLog("/foo1/current/" + getFinalizedEditsFileName(301,400), false);
    tc.addLog("/foo1/current/" + getInProgressEditsFileName(401), false);
    runTest(tc);
  }

  /**
   * Check for edge case with no logs present at all.
   */
  @Test
  public void testNoLogs() throws IOException {
    TestCaseDescription tc = new TestCaseDescription();
    tc.addRoot("/foo1", NameNodeDirType.IMAGE_AND_EDITS);
    tc.addImage("/foo1/current/" + getImageFileName(100), true);
    tc.addImage("/foo1/current/" + getImageFileName(200), true);
    tc.addImage("/foo1/current/" + getImageFileName(300), false);
    tc.addImage("/foo1/current/" + getImageFileName(400), false);
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
   * Test that old in-progress logs are properly purged
   */
  @Test
  public void testOldInProgress() throws IOException {
    TestCaseDescription tc = new TestCaseDescription();
    tc.addRoot("/foo1", NameNodeDirType.IMAGE_AND_EDITS);
    tc.addImage("/foo1/current/" + getImageFileName(100), true);
    tc.addImage("/foo1/current/" + getImageFileName(200), true);
    tc.addImage("/foo1/current/" + getImageFileName(300), false);
    tc.addImage("/foo1/current/" + getImageFileName(400), false);
    tc.addLog("/foo1/current/" + getInProgressEditsFileName(101), true);
    runTest(tc);
  }

  @Test
  public void testSeparateEditDirs() throws IOException {
    TestCaseDescription tc = new TestCaseDescription();
    tc.addRoot("/foo1", NameNodeDirType.IMAGE);
    tc.addRoot("/foo2", NameNodeDirType.EDITS);
    tc.addImage("/foo1/current/" + getImageFileName(100), true);
    tc.addImage("/foo1/current/" + getImageFileName(200), true);
    tc.addImage("/foo1/current/" + getImageFileName(300), false);
    tc.addImage("/foo1/current/" + getImageFileName(400), false);

    tc.addLog("/foo2/current/" + getFinalizedEditsFileName(101, 200), true);
    tc.addLog("/foo2/current/" + getFinalizedEditsFileName(201, 300), true);
    tc.addLog("/foo2/current/" + getFinalizedEditsFileName(301, 400), false);
    tc.addLog("/foo2/current/" + getInProgressEditsFileName(401), false);
    runTest(tc);    
  }
  
  @Test
  public void testRetainExtraLogs() throws IOException {
    conf.setLong(DFSConfigKeys.DFS_NAMENODE_NUM_EXTRA_EDITS_RETAINED_KEY,
        50);
    TestCaseDescription tc = new TestCaseDescription();
    tc.addRoot("/foo1", NameNodeDirType.IMAGE);
    tc.addRoot("/foo2", NameNodeDirType.EDITS);
    tc.addImage("/foo1/current/" + getImageFileName(100), true);
    tc.addImage("/foo1/current/" + getImageFileName(200), true);
    tc.addImage("/foo1/current/" + getImageFileName(300), false);
    tc.addImage("/foo1/current/" + getImageFileName(400), false);

    tc.addLog("/foo2/current/" + getFinalizedEditsFileName(101, 200), true);
    // Since we need 50 extra edits, *do* retain the 201-300 segment 
    tc.addLog("/foo2/current/" + getFinalizedEditsFileName(201, 300), false);
    tc.addLog("/foo2/current/" + getFinalizedEditsFileName(301, 400), false);
    tc.addLog("/foo2/current/" + getInProgressEditsFileName(401), false);
    runTest(tc);
  }
  
  @Test
  public void testRetainExtraLogsLimitedSegments() throws IOException {
    conf.setLong(DFSConfigKeys.DFS_NAMENODE_NUM_EXTRA_EDITS_RETAINED_KEY,
        150);
    conf.setLong(DFSConfigKeys.DFS_NAMENODE_MAX_EXTRA_EDITS_SEGMENTS_RETAINED_KEY, 2);
    TestCaseDescription tc = new TestCaseDescription();
    tc.addRoot("/foo1", NameNodeDirType.IMAGE);
    tc.addRoot("/foo2", NameNodeDirType.EDITS);
    tc.addImage("/foo1/current/" + getImageFileName(100), true);
    tc.addImage("/foo1/current/" + getImageFileName(200), true);
    tc.addImage("/foo1/current/" + getImageFileName(300), false);
    tc.addImage("/foo1/current/" + getImageFileName(400), false);

    // Segments containing txns upto txId 250 are extra and should be purged.
    tc.addLog("/foo2/current/" + getFinalizedEditsFileName(1, 100), true);
    tc.addLog("/foo2/current/" + getInProgressEditsFileName(101) + ".trash",
        true);
    tc.addLog("/foo2/current/" + getFinalizedEditsFileName(101, 175), true);
    tc.addLog("/foo2/current/" + getInProgressEditsFileName(176) + ".empty",
        true);
    tc.addLog("/foo2/current/" + getFinalizedEditsFileName(176, 200), true);
    tc.addLog("/foo2/current/" + getFinalizedEditsFileName(201, 225), true);
    tc.addLog("/foo2/current/" + getInProgressEditsFileName(226) + ".corrupt",
        true);
    tc.addLog("/foo2/current/" + getFinalizedEditsFileName(226, 240), true);
    // Only retain 2 extra segments. The 301-350 and 351-400 segments are
    // considered required, not extra.
    tc.addLog("/foo2/current/" + getFinalizedEditsFileName(241, 275), false);
    tc.addLog("/foo2/current/" + getInProgressEditsFileName(276) + ".trash",
        false);
    tc.addLog("/foo2/current/" + getFinalizedEditsFileName(276, 300), false);
    tc.addLog("/foo2/current/" + getInProgressEditsFileName(301) + ".empty",
        false);
    tc.addLog("/foo2/current/" + getFinalizedEditsFileName(301, 350), false);
    tc.addLog("/foo2/current/" + getInProgressEditsFileName(351) + ".corrupt",
        false);
    tc.addLog("/foo2/current/" + getFinalizedEditsFileName(351, 400), false);
    tc.addLog("/foo2/current/" + getInProgressEditsFileName(401), false);
    runTest(tc);
  }

  /* We are checking here the JournalNode environment hence added the paxos
   * directory, but as the test here is about the FileJournalManager it happens
   * via the NNStorageRetentionManager and that needs the fsImage files as well
   * to be present in the folder to calculate the minimum transaction id we want
   * to keep based on the config.
   */
  @Test
  public void testExtraInprogressFilesAreRemovedOrMarkedStale()
      throws IOException {
    conf.setLong(DFSConfigKeys.DFS_NAMENODE_NUM_EXTRA_EDITS_RETAINED_KEY, 150);
    TestCaseDescription tc = new TestCaseDescription();
    tc.addRoot("/foo", NameNodeDirType.IMAGE_AND_EDITS);
    final String PATH = "/foo/current/";

    tc.addImage(PATH + getImageFileName(200), true);
    tc.addImage(PATH + getImageFileName(300), false);
    tc.addImage(PATH + getImageFileName(400), false);

    File file = Mockito.spy(new File(PATH + "paxos"));
    Mockito.when(file.isDirectory()).thenReturn(true);
    tc.addFile(file);

    tc.addLog(PATH + getFinalizedEditsFileName(1,75), true);
    tc.addLog(PATH + getInProgressEditsFileName(76), true);
    tc.addLog(PATH + getFinalizedEditsFileName(76, 120), true);
    tc.addLog(PATH + getInProgressEditsFileName(121) + ".stale", true);
    tc.addLog(PATH + getFinalizedEditsFileName(121, 150), true);
    // everything down from here should be kept.
    tc.addLog(PATH + getInProgressEditsFileName(151), false, true);
    tc.addLog(PATH + getFinalizedEditsFileName(151, 320), false);
    tc.addLog(PATH + getInProgressEditsFileName(321), false, true);
    tc.addLog(PATH + getFinalizedEditsFileName(321, 430), false);
    tc.addLog(PATH + getInProgressEditsFileName(431), false);

    runTest(tc);
  }
  
  private void runTest(TestCaseDescription tc) throws IOException {
    StoragePurger mockPurger =
      Mockito.mock(NNStorageRetentionManager.StoragePurger.class);
    ArgumentCaptor<FSImageFile> imagesPurgedCaptor =
      ArgumentCaptor.forClass(FSImageFile.class);
    ArgumentCaptor<EditLogFile> logsPurgedCaptor =
      ArgumentCaptor.forClass(EditLogFile.class);
    ArgumentCaptor<EditLogFile> staleLogsCaptor =
      ArgumentCaptor.forClass(EditLogFile.class);

    // Ask the manager to purge files we don't need any more
    new NNStorageRetentionManager(conf,
        tc.mockStorage(), tc.mockEditLog(mockPurger), mockPurger)
      .purgeOldStorage(NameNodeFile.IMAGE);
    
    // Verify that it asked the purger to remove the correct files
    Mockito.verify(mockPurger, Mockito.atLeast(0))
      .purgeImage(imagesPurgedCaptor.capture());
    Mockito.verify(mockPurger, Mockito.atLeast(0))
      .purgeLog(logsPurgedCaptor.capture());
    Mockito.verify(mockPurger, Mockito.atLeast(0))
      .markStale(staleLogsCaptor.capture());

    Set<String> capturedPaths = Sets.newLinkedHashSet();
    // Check images
    for (FSImageFile captured : imagesPurgedCaptor.getAllValues()) {
      capturedPaths.add(fileToPath(captured.getFile()));
    }
    Assert.assertEquals("Image file check.",
      Joiner.on(",").join(filesToPaths(tc.expectedPurgedImages)),
      Joiner.on(",").join(capturedPaths));

    capturedPaths.clear();
    // Check edit logs, and also in progress edits older than minTxIdToKeep
    for (EditLogFile captured : logsPurgedCaptor.getAllValues()) {
      capturedPaths.add(fileToPath(captured.getFile()));
    }
    Assert.assertEquals("Check old edits are removed.",
      Joiner.on(",").join(filesToPaths(tc.expectedPurgedLogs)),
      Joiner.on(",").join(capturedPaths));

    capturedPaths.clear();
    // Check in progress edits to keep are marked as stale
    for (EditLogFile captured : staleLogsCaptor.getAllValues()) {
      capturedPaths.add(fileToPath(captured.getFile()));
    }
    Assert.assertEquals("Check unnecessary but kept edits are marked stale",
      Joiner.on(",").join(filesToPaths(tc.expectedStaleLogs)),
      Joiner.on(",").join(capturedPaths));
  }

  private class TestCaseDescription {
    private final Map<File, FakeRoot> dirRoots = Maps.newLinkedHashMap();
    private final Set<File> expectedPurgedLogs = Sets.newLinkedHashSet();
    private final Set<File> expectedPurgedImages = Sets.newLinkedHashSet();
    private final Set<File> expectedStaleLogs = Sets.newLinkedHashSet();

    private class FakeRoot {
      final NameNodeDirType type;
      final List<File> files;
      
      FakeRoot(NameNodeDirType type) {
        this.type = type;
        files = Lists.newArrayList();
      }

      StorageDirectory mockStorageDir() {
        return FSImageTestUtil.mockStorageDirectory(
            type, false,
            filesToPaths(files).toArray(new String[0]));
      }
    }

    void addRoot(String root, NameNodeDirType dir) {
      dirRoots.put(new File(root), new FakeRoot(dir));
    }

    private void addFile(File file) {
      for (Map.Entry<File, FakeRoot> entry : dirRoots.entrySet()) {
        if (fileToPath(file).startsWith(fileToPath(entry.getKey()))) {
          entry.getValue().files.add(file);
        }
      }
    }

    void addLog(String path, boolean expectPurge) {
      addLog(path, expectPurge, false);
    }

    void addLog(String path, boolean expectPurge, boolean expectStale) {
      File file = new File(path);
      addFile(file);
      if (expectPurge) {
        expectedPurgedLogs.add(file);
      }
      if (expectStale) {
        expectedStaleLogs.add(file);
      }
    }
    
    void addImage(String path, boolean expectPurge) {
      File file = new File(path);
      addFile(file);
      if (expectPurge) {
        expectedPurgedImages.add(file);
      }
    }
    
    NNStorage mockStorage() throws IOException {
      List<StorageDirectory> sds = Lists.newArrayList();
      for (FakeRoot root : dirRoots.values()) {
        sds.add(root.mockStorageDir());
      }
      return mockStorageForDirs(sds.toArray(new StorageDirectory[0]));
    }

    private File findLastInProgressEdit(FakeRoot root){
      Pattern p = Pattern.compile(
          NameNodeFile.EDITS_INPROGRESS.getName() + "_(\\d+)");
      ToLongFunction<File> fileNameToTxId =
          f -> {
            Matcher m = p.matcher(f.getName());
            return m.matches() ?
                Long.parseLong(m.group(1)):
                HdfsServerConstants.INVALID_TXID;
          };
      return root.files.stream().
          sorted(Comparator.comparingLong(fileNameToTxId).reversed()).
          findFirst().orElse(null);
    }

    @SuppressWarnings("unchecked")
    public FSEditLog mockEditLog(StoragePurger purger) throws IOException {
      final List<JournalManager> jms = Lists.newArrayList();
      final JournalSet journalSet = new JournalSet(0);
      for (FakeRoot root : dirRoots.values()) {
        if (!root.type.isOfType(NameNodeDirType.EDITS)) continue;
        
        // passing null NNStorage for unit test because it does not use it
        FileJournalManager fjm = new FileJournalManager(conf,
            root.mockStorageDir(), null);
        fjm.currentInProgress = findLastInProgressEdit(root);
        fjm.purger = purger;
        jms.add(fjm);
        journalSet.add(fjm, false);
      }

      FSEditLog mockLog = Mockito.mock(FSEditLog.class);
      Mockito.doAnswer(invocation -> {
        Object[] args = invocation.getArguments();
        assert args.length == 1;
        long txId = (Long) args[0];
        for (JournalManager jm : jms) {
          jm.purgeLogsOlderThan(txId);
        }
        return null;
      }).when(mockLog).purgeLogsOlderThan(Mockito.anyLong());
      
      Mockito.doAnswer(invocation -> {
        Object[] args = invocation.getArguments();
        journalSet.selectInputStreams((Collection<EditLogInputStream>)args[0],
            (Long)args[1], (Boolean)args[2], (Boolean)args[3]);
        return null;
      }).when(mockLog).selectInputStreams(Mockito.anyCollection(),
          Mockito.anyLong(), Mockito.anyBoolean(), Mockito.anyBoolean());
      return mockLog;
    }
  }

  /**
   * Converts a file to a platform-agnostic URI path.
   * 
   * @param file File to convert
   * @return String path
   */
  private static String fileToPath(File file) {
    return file.toURI().getPath();
  }

  /**
   * Converts multiple files to platform-agnostic URI paths.
   * 
   * @param files Collection<File> files to convert
   * @return Collection<String> paths
   */
  private static Collection<String> filesToPaths(Collection<File> files) {
    List<String> paths = Lists.newArrayList();
    for (File file: files) {
      paths.add(fileToPath(file));
    }
    return paths;
  }

  private static NNStorage mockStorageForDirs(final StorageDirectory... mockDirs)
      throws IOException {
    NNStorage mockStorage = Mockito.mock(NNStorage.class);
    Mockito.doAnswer(invocation -> {
      FSImageStorageInspector inspector =
          (FSImageStorageInspector) invocation.getArguments()[0];
      for (StorageDirectory sd : mockDirs) {
        inspector.inspectDirectory(sd);
      }
      return null;
    }).when(mockStorage).inspectStorageDirs(Mockito.any());
    return mockStorage;
  }
}
