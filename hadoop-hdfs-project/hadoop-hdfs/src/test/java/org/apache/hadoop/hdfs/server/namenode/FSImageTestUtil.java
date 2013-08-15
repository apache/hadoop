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

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.server.common.Storage.StorageDirType;
import org.apache.hadoop.hdfs.server.common.Storage.StorageDirectory;
import org.apache.hadoop.hdfs.server.namenode.FSImageStorageInspector.FSImageFile;
import org.apache.hadoop.hdfs.server.namenode.FileJournalManager.EditLogFile;
import org.apache.hadoop.hdfs.server.namenode.NNStorage.NameNodeDirType;
import org.apache.hadoop.hdfs.util.Holder;
import org.apache.hadoop.hdfs.util.MD5FileUtils;
import org.apache.hadoop.io.IOUtils;
import org.mockito.Matchers;
import org.mockito.Mockito;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.io.Files;

/**
 * Utility functions for testing fsimage storage.
 */
public abstract class FSImageTestUtil {
  
  public static final Log LOG = LogFactory.getLog(FSImageTestUtil.class);

  /**
   * The position in the fsimage header where the txid is
   * written.
   */
  private static final long IMAGE_TXID_POS = 24;

  /**
   * This function returns a md5 hash of a file.
   * 
   * @param file input file
   * @return The md5 string
   */
  public static String getFileMD5(File file) throws IOException {
    return MD5FileUtils.computeMd5ForFile(file).toString();
  }
  
  /**
   * Calculate the md5sum of an image after zeroing out the transaction ID
   * field in the header. This is useful for tests that want to verify
   * that two checkpoints have identical namespaces.
   */
  public static String getImageFileMD5IgnoringTxId(File imageFile)
      throws IOException {
    File tmpFile = File.createTempFile("hadoop_imagefile_tmp", "fsimage");
    tmpFile.deleteOnExit();
    try {
      Files.copy(imageFile, tmpFile);
      RandomAccessFile raf = new RandomAccessFile(tmpFile, "rw");
      try {
        raf.seek(IMAGE_TXID_POS);
        raf.writeLong(0);
      } finally {
        IOUtils.closeStream(raf);
      }
      return getFileMD5(tmpFile);
    } finally {
      tmpFile.delete();
    }
  }
  
  public static StorageDirectory mockStorageDirectory(
      File currentDir, NameNodeDirType type) {
    // Mock the StorageDirectory interface to just point to this file
    StorageDirectory sd = Mockito.mock(StorageDirectory.class);
    Mockito.doReturn(type)
      .when(sd).getStorageDirType();
    Mockito.doReturn(currentDir).when(sd).getCurrentDir();
    Mockito.doReturn(currentDir).when(sd).getRoot();
    Mockito.doReturn(mockFile(true)).when(sd).getVersionFile();
    Mockito.doReturn(mockFile(false)).when(sd).getPreviousDir();
    return sd;
  }
  
  /**
   * Make a mock storage directory that returns some set of file contents.
   * @param type type of storage dir
   * @param previousExists should we mock that the previous/ dir exists?
   * @param fileNames the names of files contained in current/
   */
  static StorageDirectory mockStorageDirectory(
      StorageDirType type,
      boolean previousExists,
      String...  fileNames) {
    StorageDirectory sd = mock(StorageDirectory.class);
    
    doReturn(type).when(sd).getStorageDirType();
  
    // Version file should always exist
    doReturn(mockFile(true)).when(sd).getVersionFile();
    doReturn(mockFile(true)).when(sd).getRoot();

    // Previous dir optionally exists
    doReturn(mockFile(previousExists))
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
  
  static File mockFile(boolean exists) {
    File mockFile = mock(File.class);
    doReturn(exists).when(mockFile).exists();
    return mockFile;
  }
  
  public static FSImageTransactionalStorageInspector inspectStorageDirectory(
      File dir, NameNodeDirType dirType) throws IOException {
    FSImageTransactionalStorageInspector inspector =
      new FSImageTransactionalStorageInspector();
    inspector.inspectDirectory(mockStorageDirectory(dir, dirType));
    return inspector;
  }

  
  /**
   * Return a standalone instance of FSEditLog that will log into the given
   * log directory. The returned instance is not yet opened.
   */
  public static FSEditLog createStandaloneEditLog(File logDir)
      throws IOException {
    assertTrue(logDir.mkdirs() || logDir.exists());
    if (!FileUtil.fullyDeleteContents(logDir)) {
      throw new IOException("Unable to delete contents of " + logDir);
    }
    NNStorage storage = Mockito.mock(NNStorage.class);
    StorageDirectory sd 
      = FSImageTestUtil.mockStorageDirectory(logDir, NameNodeDirType.EDITS);
    List<StorageDirectory> sds = Lists.newArrayList(sd);
    Mockito.doReturn(sds).when(storage).dirIterable(NameNodeDirType.EDITS);
    Mockito.doReturn(sd).when(storage)
      .getStorageDirectory(Matchers.<URI>anyObject());

    FSEditLog editLog = new FSEditLog(new Configuration(), 
                         storage,
                         ImmutableList.of(logDir.toURI()));
    editLog.initJournalsForWrite();
    return editLog;
  }
  

  /**
   * Create an aborted in-progress log in the given directory, containing
   * only a specified number of "mkdirs" operations.
   */
  public static void createAbortedLogWithMkdirs(File editsLogDir, int numDirs,
      long firstTxId, long newInodeId) throws IOException {
    FSEditLog editLog = FSImageTestUtil.createStandaloneEditLog(editsLogDir);
    editLog.setNextTxId(firstTxId);
    editLog.openForWrite();
    
    PermissionStatus perms = PermissionStatus.createImmutable("fakeuser", "fakegroup",
        FsPermission.createImmutable((short)0755));
    for (int i = 1; i <= numDirs; i++) {
      String dirName = "dir" + i;
      INodeDirectory dir = new INodeDirectory(newInodeId + i - 1,
          DFSUtil.string2Bytes(dirName), perms, 0L);
      editLog.logMkDir("/" + dirName, dir);
    }
    editLog.logSync();
    editLog.abortCurrentLogSegment();
  }

  /**
   * @param editLog a path of an edit log file
   * @return the count of each type of operation in the log file
   * @throws Exception if there is an error reading it
   */
  public static EnumMap<FSEditLogOpCodes,Holder<Integer>> countEditLogOpTypes(
      File editLog) throws Exception {
    EditLogInputStream elis = new EditLogFileInputStream(editLog);
    try {
      return countEditLogOpTypes(elis);
    } finally {
      IOUtils.closeStream(elis);
    }
  }

  /**
   * @see #countEditLogOpTypes(File)
   */
  public static EnumMap<FSEditLogOpCodes, Holder<Integer>> countEditLogOpTypes(
      EditLogInputStream elis) throws IOException {
    EnumMap<FSEditLogOpCodes, Holder<Integer>> opCounts =
        new EnumMap<FSEditLogOpCodes, Holder<Integer>>(FSEditLogOpCodes.class);
    
    FSEditLogOp op;
    while ((op = elis.readOp()) != null) {
      Holder<Integer> i = opCounts.get(op.opCode);
      if (i == null) {
        i = new Holder<Integer>(0);
        opCounts.put(op.opCode, i);
      }
      i.held++;
    }
    return opCounts;
  }

  /**
   * Assert that all of the given directories have the same newest filename
   * for fsimage that they hold the same data.
   */
  public static void assertSameNewestImage(List<File> dirs) throws Exception {
    if (dirs.size() < 2) return;
    
    long imageTxId = -1;
    
    List<File> imageFiles = new ArrayList<File>();
    for (File dir : dirs) {
      FSImageTransactionalStorageInspector inspector =
        inspectStorageDirectory(dir, NameNodeDirType.IMAGE);
      List<FSImageFile> latestImages = inspector.getLatestImages();
      assert(!latestImages.isEmpty());
      long thisTxId = latestImages.get(0).getCheckpointTxId();
      if (imageTxId != -1 && thisTxId != imageTxId) {
        fail("Storage directory " + dir + " does not have the same " +
            "last image index " + imageTxId + " as another");
      }
      imageTxId = thisTxId;
      imageFiles.add(inspector.getLatestImages().get(0).getFile());
    }
    
    assertFileContentsSame(imageFiles.toArray(new File[0]));
  }
  
  /**
   * Given a list of directories, assert that any files that are named
   * the same thing have the same contents. For example, if a file
   * named "fsimage_1" shows up in more than one directory, then it must
   * be the same.
   * @throws Exception 
   */
  public static void assertParallelFilesAreIdentical(List<File> dirs,
      Set<String> ignoredFileNames) throws Exception {
    HashMap<String, List<File>> groupedByName = new HashMap<String, List<File>>();
    for (File dir : dirs) {
      for (File f : dir.listFiles()) {
        if (ignoredFileNames.contains(f.getName())) {
          continue;
        }
        
        List<File> fileList = groupedByName.get(f.getName());
        if (fileList == null) {
          fileList = new ArrayList<File>();
          groupedByName.put(f.getName(), fileList);
        }
        fileList.add(f);
      }
    }
    
    for (List<File> sameNameList : groupedByName.values()) {
      if (sameNameList.get(0).isDirectory()) {
        // recurse
        assertParallelFilesAreIdentical(sameNameList, ignoredFileNames);
      } else {
        if ("VERSION".equals(sameNameList.get(0).getName())) {
          assertPropertiesFilesSame(sameNameList.toArray(new File[0]));
        } else {
          assertFileContentsSame(sameNameList.toArray(new File[0]));
        }
      }
    }  
  }
  
  /**
   * Assert that a set of properties files all contain the same data.
   * We cannot simply check the md5sums here, since Properties files
   * contain timestamps -- thus, two properties files from the same
   * saveNamespace operation may actually differ in md5sum.
   * @param propFiles the files to compare
   * @throws IOException if the files cannot be opened or read
   * @throws AssertionError if the files differ
   */
  public static void assertPropertiesFilesSame(File[] propFiles)
      throws IOException {
    Set<Map.Entry<Object, Object>> prevProps = null;
    
    for (File f : propFiles) {
      Properties props;
      FileInputStream is = new FileInputStream(f);
      try {
        props = new Properties();
        props.load(is);
      } finally {
        IOUtils.closeStream(is);
      }
      if (prevProps == null) {
        prevProps = props.entrySet();
      } else {
        Set<Entry<Object,Object>> diff =
          Sets.symmetricDifference(prevProps, props.entrySet());
        if (!diff.isEmpty()) {
          fail("Properties file " + f + " differs from " + propFiles[0]);
        }
      }
    }
  }

  /**
   * Assert that all of the given paths have the exact same
   * contents 
   */
  public static void assertFileContentsSame(File... files) throws Exception {
    if (files.length < 2) return;
    
    Map<File, String> md5s = getFileMD5s(files);
    if (Sets.newHashSet(md5s.values()).size() > 1) {
      fail("File contents differed:\n  " +
          Joiner.on("\n  ")
            .withKeyValueSeparator("=")
            .join(md5s));
    }
  }
  
  /**
   * Assert that the given files are not all the same, and in fact that
   * they have <code>expectedUniqueHashes</code> unique contents.
   */
  public static void assertFileContentsDifferent(
      int expectedUniqueHashes,
      File... files) throws Exception
  {
    Map<File, String> md5s = getFileMD5s(files);
    if (Sets.newHashSet(md5s.values()).size() != expectedUniqueHashes) {
      fail("Expected " + expectedUniqueHashes + " different hashes, got:\n  " +
          Joiner.on("\n  ")
            .withKeyValueSeparator("=")
            .join(md5s));
    }
  }
  
  public static Map<File, String> getFileMD5s(File... files) throws Exception {
    Map<File, String> ret = Maps.newHashMap();
    for (File f : files) {
      assertTrue("Must exist: " + f, f.exists());
      ret.put(f, getFileMD5(f));
    }
    return ret;
  }

  /**
   * @return a List which contains the "current" dir for each storage
   * directory of the given type. 
   */
  public static List<File> getCurrentDirs(NNStorage storage,
      NameNodeDirType type) {
    List<File> ret = Lists.newArrayList();
    for (StorageDirectory sd : storage.dirIterable(type)) {
      ret.add(sd.getCurrentDir());
    }
    return ret;
  }

  /**
   * @return the fsimage file with the most recent transaction ID in the
   * given storage directory.
   */
  public static File findLatestImageFile(StorageDirectory sd)
  throws IOException {
    FSImageTransactionalStorageInspector inspector =
      new FSImageTransactionalStorageInspector();
    inspector.inspectDirectory(sd);
    
    return inspector.getLatestImages().get(0).getFile();
  }

  /**
   * @return the fsimage file with the most recent transaction ID in the
   * given 'current/' directory.
   */
  public static File findNewestImageFile(String currentDirPath) throws IOException {
    StorageDirectory sd = FSImageTestUtil.mockStorageDirectory(
        new File(currentDirPath), NameNodeDirType.IMAGE);

    FSImageTransactionalStorageInspector inspector =
      new FSImageTransactionalStorageInspector();
    inspector.inspectDirectory(sd);

    List<FSImageFile> latestImages = inspector.getLatestImages();
    return (latestImages.isEmpty()) ? null : latestImages.get(0).getFile();
  }

  /**
   * Assert that the NameNode has checkpoints at the expected
   * transaction IDs.
   */
  public static void assertNNHasCheckpoints(MiniDFSCluster cluster,
      List<Integer> txids) {
    assertNNHasCheckpoints(cluster, 0, txids);
  }
  
  public static void assertNNHasCheckpoints(MiniDFSCluster cluster,
      int nnIdx, List<Integer> txids) {

    for (File nameDir : getNameNodeCurrentDirs(cluster, nnIdx)) {
      LOG.info("examining name dir with files: " +
          Joiner.on(",").join(nameDir.listFiles()));
      // Should have fsimage_N for the three checkpoints
      LOG.info("Examining storage dir " + nameDir + " with contents: "
          + StringUtils.join(nameDir.listFiles(), ", "));
      for (long checkpointTxId : txids) {
        File image = new File(nameDir,
                              NNStorage.getImageFileName(checkpointTxId));
        assertTrue("Expected non-empty " + image, image.length() > 0);
      }
    }
  }

  public static List<File> getNameNodeCurrentDirs(MiniDFSCluster cluster, int nnIdx) {
    List<File> nameDirs = Lists.newArrayList();
    for (URI u : cluster.getNameDirs(nnIdx)) {
      nameDirs.add(new File(u.getPath(), "current"));
    }
    return nameDirs;
  }

  /**
   * @return the latest edits log, finalized or otherwise, from the given
   * storage directory.
   */
  public static EditLogFile findLatestEditsLog(StorageDirectory sd)
  throws IOException {
    File currentDir = sd.getCurrentDir();
    List<EditLogFile> foundEditLogs 
      = Lists.newArrayList(FileJournalManager.matchEditLogs(currentDir));
    return Collections.max(foundEditLogs, EditLogFile.COMPARE_BY_START_TXID);
  }

  /**
   * Corrupt the given VERSION file by replacing a given
   * key with a new value and re-writing the file.
   * 
   * @param versionFile the VERSION file to corrupt
   * @param key the key to replace
   * @param value the new value for this key
   */
  public static void corruptVersionFile(File versionFile, String key, String value)
      throws IOException {
    Properties props = new Properties();
    FileInputStream fis = new FileInputStream(versionFile);
    FileOutputStream out = null;
    try {
      props.load(fis);
      IOUtils.closeStream(fis);
  
      if (value == null || value.isEmpty()) {
        props.remove(key);
      } else {
        props.setProperty(key, value);
      }
      
      out = new FileOutputStream(versionFile);
      props.store(out, null);
      
    } finally {
      IOUtils.cleanup(null, fis, out);
    }    
  }

  public static void assertReasonableNameCurrentDir(File curDir)
      throws IOException {
    assertTrue(curDir.isDirectory());
    assertTrue(new File(curDir, "VERSION").isFile());
    assertTrue(new File(curDir, "seen_txid").isFile());
    File image = findNewestImageFile(curDir.toString());
    assertNotNull(image);
  }

  public static void logStorageContents(Log LOG, NNStorage storage) {
    LOG.info("current storages and corresponding sizes:");
    for (StorageDirectory sd : storage.dirIterable(null)) {
      File curDir = sd.getCurrentDir();
      LOG.info("In directory " + curDir);
      File[] files = curDir.listFiles();
      Arrays.sort(files);
      for (File f : files) {
        LOG.info("  file " + f.getAbsolutePath() + "; len = " + f.length());  
      }
    }
  }
  
  /** get the fsImage*/
  public static FSImage getFSImage(NameNode node) {
    return node.getFSImage();
  }
  
  public static void assertNNFilesMatch(MiniDFSCluster cluster) throws Exception {
    List<File> curDirs = Lists.newArrayList();
    curDirs.addAll(FSImageTestUtil.getNameNodeCurrentDirs(cluster, 0));
    curDirs.addAll(FSImageTestUtil.getNameNodeCurrentDirs(cluster, 1));
    
    // Ignore seen_txid file, since the newly bootstrapped standby
    // will have a higher seen_txid than the one it bootstrapped from.
    Set<String> ignoredFiles = ImmutableSet.of("seen_txid");
    FSImageTestUtil.assertParallelFilesAreIdentical(curDirs,
        ignoredFiles);
  }
}
