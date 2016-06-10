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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.TreeMap;
import java.util.zip.CRC32;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.inotify.Event;
import org.apache.hadoop.hdfs.inotify.EventBatch;
import org.apache.hadoop.hdfs.protocol.DirectoryListing;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.StartupOption;
import org.apache.hadoop.hdfs.server.namenode.FSImage;
import org.apache.hadoop.hdfs.server.namenode.FSImageFormat;
import org.apache.hadoop.hdfs.server.namenode.FSImageTestUtil;
import org.apache.hadoop.hdfs.server.namenode.IllegalReservedPathException;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.util.StringUtils;
import org.apache.log4j.Logger;
import org.junit.Test;

import static org.apache.hadoop.hdfs.inotify.Event.CreateEvent;
import static org.junit.Assert.*;

/**
 * This tests data transfer protocol handling in the Datanode. It sends
 * various forms of wrong data and verifies that Datanode handles it well.
 * 
 * This test uses the following items from src/test/.../dfs directory :
 *   1) hadoop-22-dfs-dir.tgz and other tarred pre-upgrade NN / DN 
 *      directory images
 *   2) hadoop-dfs-dir.txt : checksums that are compared in this test.
 * Please read hadoop-dfs-dir.txt for more information.  
 */
public class TestDFSUpgradeFromImage {
  
  private static final Log LOG = LogFactory
      .getLog(TestDFSUpgradeFromImage.class);
  private static final File TEST_ROOT_DIR =
                      new File(MiniDFSCluster.getBaseDirectory());
  private static final String HADOOP_DFS_DIR_TXT = "hadoop-dfs-dir.txt";
  private static final String HADOOP22_IMAGE = "hadoop-22-dfs-dir.tgz";
  private static final String HADOOP1_BBW_IMAGE = "hadoop1-bbw.tgz";
  private static final String HADOOP1_RESERVED_IMAGE = "hadoop-1-reserved.tgz";
  private static final String HADOOP023_RESERVED_IMAGE =
      "hadoop-0.23-reserved.tgz";
  private static final String HADOOP2_RESERVED_IMAGE = "hadoop-2-reserved.tgz";
  private static final String HADOOP252_IMAGE = "hadoop-252-dfs-dir.tgz";

  private static class ReferenceFileInfo {
    String path;
    long checksum;
  }
  
  static final Configuration upgradeConf;
  
  static {
    upgradeConf = new HdfsConfiguration();
    upgradeConf.setInt(DFSConfigKeys.DFS_DATANODE_SCAN_PERIOD_HOURS_KEY, -1); // block scanning off
    if (System.getProperty("test.build.data") == null) { // to allow test to be run outside of Maven
      System.setProperty("test.build.data", "build/test/data");
    }
  }
  
  public interface ClusterVerifier {
    public void verifyClusterPostUpgrade(final MiniDFSCluster cluster) throws IOException;
  }

  final LinkedList<ReferenceFileInfo> refList = new LinkedList<ReferenceFileInfo>();
  Iterator<ReferenceFileInfo> refIter;
  
  boolean printChecksum = false;
  
  void unpackStorage(String tarFileName, String referenceName)
      throws IOException {
    String tarFile = System.getProperty("test.cache.data", "build/test/cache")
        + "/" + tarFileName;
    String dataDir = System.getProperty("test.build.data", "build/test/data");
    File dfsDir = new File(dataDir, "dfs");
    if ( dfsDir.exists() && !FileUtil.fullyDelete(dfsDir) ) {
      throw new IOException("Could not delete dfs directory '" + dfsDir + "'");
    }
    LOG.info("Unpacking " + tarFile);
    FileUtil.unTar(new File(tarFile), new File(dataDir));
    //Now read the reference info
    
    BufferedReader reader = new BufferedReader(new FileReader(
        System.getProperty("test.cache.data", "build/test/cache")
            + "/" + referenceName));
    String line;
    while ( (line = reader.readLine()) != null ) {
      
      line = line.trim();
      if (line.length() <= 0 || line.startsWith("#")) {
        continue;
      }
      String[] arr = line.split("\\s+");
      if (arr.length < 1) {
        continue;
      }
      if (arr[0].equals("printChecksums")) {
        printChecksum = true;
        break;
      }
      if (arr.length < 2) {
        continue;
      }
      ReferenceFileInfo info = new ReferenceFileInfo();
      info.path = arr[0];
      info.checksum = Long.parseLong(arr[1]);
      refList.add(info);
    }
    reader.close();
  }

  private void verifyChecksum(String path, long checksum) throws IOException {
    if ( refIter == null ) {
      refIter = refList.iterator();
    }
    
    if ( printChecksum ) {
      LOG.info("CRC info for reference file : " + path + " \t " + checksum);
    } else {
      if ( !refIter.hasNext() ) {
        throw new IOException("Checking checksum for " + path +
                              "Not enough elements in the refList");
      }
      ReferenceFileInfo info = refIter.next();
      // The paths are expected to be listed in the same order 
      // as they are traversed here.
      assertEquals(info.path, path);
      assertEquals("Checking checksum for " + path, info.checksum, checksum);
    }
  }
  
  /**
   * Try to open a file for reading several times.
   * 
   * If we fail because lease recovery hasn't completed, retry the open.
   */
  private static FSInputStream dfsOpenFileWithRetries(DistributedFileSystem dfs,
      String pathName) throws IOException {
    IOException exc = null;
    for (int tries = 0; tries < 30; tries++) {
      try {
        return dfs.dfs.open(pathName);
      } catch (IOException e) {
        exc = e;
      }
      if (!exc.getMessage().contains("Cannot obtain " +
          "block length for LocatedBlock")) {
        throw exc;
      }
      try {
        LOG.info("Open failed. " + tries + " times. Retrying.");
        Thread.sleep(1000);
      } catch (InterruptedException ignored) {}
    }
    throw exc;
  }
  
  private void verifyDir(DistributedFileSystem dfs, Path dir,
      CRC32 overallChecksum) throws IOException {
    FileStatus[] fileArr = dfs.listStatus(dir);
    TreeMap<Path, Boolean> fileMap = new TreeMap<Path, Boolean>();
    
    for(FileStatus file : fileArr) {
      fileMap.put(file.getPath(), Boolean.valueOf(file.isDirectory()));
    }
    
    for(Iterator<Path> it = fileMap.keySet().iterator(); it.hasNext();) {
      Path path = it.next();
      boolean isDir = fileMap.get(path);
      
      String pathName = path.toUri().getPath();
      overallChecksum.update(pathName.getBytes());
      
      if ( isDir ) {
        verifyDir(dfs, path, overallChecksum);
      } else {
        // this is not a directory. Checksum the file data.
        CRC32 fileCRC = new CRC32();
        FSInputStream in = dfsOpenFileWithRetries(dfs, pathName);
        byte[] buf = new byte[4096];
        int nRead = 0;
        while ( (nRead = in.read(buf, 0, buf.length)) > 0 ) {
          fileCRC.update(buf, 0, nRead);
        }
        
        verifyChecksum(pathName, fileCRC.getValue());
      }
    }
  }
  
  private void verifyFileSystem(DistributedFileSystem dfs) throws IOException {
  
    CRC32 overallChecksum = new CRC32();
    verifyDir(dfs, new Path("/"), overallChecksum);
    
    verifyChecksum("overallCRC", overallChecksum.getValue());
    
    if ( printChecksum ) {
      throw new IOException("Checksums are written to log as requested. " +
                            "Throwing this exception to force an error " +
                            "for this test.");
    }
  }
  
  /**
   * Test that sets up a fake image from Hadoop 0.3.0 and tries to start a
   * NN, verifying that the correct error message is thrown.
   */
  @Test
  public void testFailOnPreUpgradeImage() throws IOException {
    Configuration conf = new HdfsConfiguration();

    File namenodeStorage = new File(TEST_ROOT_DIR, "nnimage-0.3.0");
    conf.set(DFSConfigKeys.DFS_NAMENODE_NAME_DIR_KEY, namenodeStorage.toString());

    // Set up a fake NN storage that looks like an ancient Hadoop dir circa 0.3.0
    FileUtil.fullyDelete(namenodeStorage);
    assertTrue("Make " + namenodeStorage, namenodeStorage.mkdirs());
    File imageDir = new File(namenodeStorage, "image");
    assertTrue("Make " + imageDir, imageDir.mkdirs());

    // Hex dump of a formatted image from Hadoop 0.3.0
    File imageFile = new File(imageDir, "fsimage");
    byte[] imageBytes = StringUtils.hexStringToByte(
      "fffffffee17c0d2700000000");
    FileOutputStream fos = new FileOutputStream(imageFile);
    try {
      fos.write(imageBytes);
    } finally {
      fos.close();
    }

    // Now try to start an NN from it

    MiniDFSCluster cluster = null;
    try {
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(0)
        .format(false)
        .manageDataDfsDirs(false)
        .manageNameDfsDirs(false)
        .startupOption(StartupOption.REGULAR)
        .build();
      fail("Was able to start NN from 0.3.0 image");
    } catch (IOException ioe) {
      if (!ioe.toString().contains("Old layout version is 'too old'")) {
        throw ioe;
      }
    } finally {
      // We expect startup to fail, but just in case it didn't, shutdown now.
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }
  
  /**
   * Test upgrade from 0.22 image
   */
  @Test
  public void testUpgradeFromRel22Image() throws IOException {
    unpackStorage(HADOOP22_IMAGE, HADOOP_DFS_DIR_TXT);
    upgradeAndVerify(new MiniDFSCluster.Builder(upgradeConf).
        numDataNodes(4), null);
  }
  
  /**
   * Test upgrade from 0.22 image with corrupt md5, make sure it
   * fails to upgrade
   */
  @Test
  public void testUpgradeFromCorruptRel22Image() throws IOException {
    unpackStorage(HADOOP22_IMAGE, HADOOP_DFS_DIR_TXT);
    
    // Overwrite the md5 stored in the VERSION files
    File[] nnDirs = MiniDFSCluster.getNameNodeDirectory(MiniDFSCluster.getBaseDirectory(), 0, 0);
    FSImageTestUtil.corruptVersionFile(
        new File(nnDirs[0], "current/VERSION"),
        "imageMD5Digest", "22222222222222222222222222222222");
    FSImageTestUtil.corruptVersionFile(
        new File(nnDirs[1], "current/VERSION"),
        "imageMD5Digest", "22222222222222222222222222222222");
    
    // Attach our own log appender so we can verify output
    final LogVerificationAppender appender = new LogVerificationAppender();
    final Logger logger = Logger.getRootLogger();
    logger.addAppender(appender);

    // Upgrade should now fail
    try {
      upgradeAndVerify(new MiniDFSCluster.Builder(upgradeConf).
          numDataNodes(4), null);
      fail("Upgrade did not fail with bad MD5");
    } catch (IOException ioe) {
      String msg = StringUtils.stringifyException(ioe);
      if (!msg.contains("Failed to load FSImage file")) {
        throw ioe;
      }
      int md5failures = appender.countExceptionsWithMessage(
          " is corrupt with MD5 checksum of ");
      assertEquals("Upgrade did not fail with bad MD5", 1, md5failures);
    }
  }

  /**
   * Test upgrade from a branch-1.2 image with reserved paths
   */
  @Test
  public void testUpgradeFromRel1ReservedImage() throws Exception {
    unpackStorage(HADOOP1_RESERVED_IMAGE, HADOOP_DFS_DIR_TXT);
    MiniDFSCluster cluster = null;
    // Try it once without setting the upgrade flag to ensure it fails
    final Configuration conf = new Configuration();
    // Try it again with a custom rename string
    try {
      FSImageFormat.setRenameReservedPairs(
          ".snapshot=.user-snapshot," +
              ".reserved=.my-reserved");
      cluster =
          new MiniDFSCluster.Builder(conf)
              .format(false)
              .startupOption(StartupOption.UPGRADE)
              .numDataNodes(0).build();
      DistributedFileSystem dfs = cluster.getFileSystem();
      // Make sure the paths were renamed as expected
      // Also check that paths are present after a restart, checks that the
      // upgraded fsimage has the same state.
      final String[] expected = new String[] {
          "/.my-reserved",
          "/.user-snapshot",
          "/.user-snapshot/.user-snapshot",
          "/.user-snapshot/open",
          "/dir1",
          "/dir1/.user-snapshot",
          "/dir2",
          "/dir2/.user-snapshot",
          "/user",
          "/user/andrew",
          "/user/andrew/.user-snapshot",
      };
      for (int i=0; i<2; i++) {
        // Restart the second time through this loop
        if (i==1) {
          cluster.finalizeCluster(conf);
          cluster.restartNameNode(true);
        }
        ArrayList<Path> toList = new ArrayList<Path>();
        toList.add(new Path("/"));
        ArrayList<String> found = new ArrayList<String>();
        while (!toList.isEmpty()) {
          Path p = toList.remove(0);
          FileStatus[] statuses = dfs.listStatus(p);
          for (FileStatus status: statuses) {
            final String path = status.getPath().toUri().getPath();
            System.out.println("Found path " + path);
            found.add(path);
            if (status.isDirectory()) {
              toList.add(status.getPath());
            }
          }
        }
        for (String s: expected) {
          assertTrue("Did not find expected path " + s, found.contains(s));
        }
        assertEquals("Found an unexpected path while listing filesystem",
            found.size(), expected.length);
      }
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  /**
   * Test upgrade from a 0.23.11 image with reserved paths
   */
  @Test
  public void testUpgradeFromRel023ReservedImage() throws Exception {
    unpackStorage(HADOOP023_RESERVED_IMAGE, HADOOP_DFS_DIR_TXT);
    MiniDFSCluster cluster = null;
    // Try it once without setting the upgrade flag to ensure it fails
    final Configuration conf = new Configuration();
    // Try it again with a custom rename string
    try {
      FSImageFormat.setRenameReservedPairs(
          ".snapshot=.user-snapshot," +
              ".reserved=.my-reserved");
      cluster =
          new MiniDFSCluster.Builder(conf)
              .format(false)
              .startupOption(StartupOption.UPGRADE)
              .numDataNodes(0).build();
      DistributedFileSystem dfs = cluster.getFileSystem();
      // Make sure the paths were renamed as expected
      // Also check that paths are present after a restart, checks that the
      // upgraded fsimage has the same state.
      final String[] expected = new String[] {
          "/.user-snapshot",
          "/dir1",
          "/dir1/.user-snapshot",
          "/dir2",
          "/dir2/.user-snapshot"
      };
      for (int i=0; i<2; i++) {
        // Restart the second time through this loop
        if (i==1) {
          cluster.finalizeCluster(conf);
          cluster.restartNameNode(true);
        }
        ArrayList<Path> toList = new ArrayList<Path>();
        toList.add(new Path("/"));
        ArrayList<String> found = new ArrayList<String>();
        while (!toList.isEmpty()) {
          Path p = toList.remove(0);
          FileStatus[] statuses = dfs.listStatus(p);
          for (FileStatus status: statuses) {
            final String path = status.getPath().toUri().getPath();
            System.out.println("Found path " + path);
            found.add(path);
            if (status.isDirectory()) {
              toList.add(status.getPath());
            }
          }
        }
        for (String s: expected) {
          assertTrue("Did not find expected path " + s, found.contains(s));
        }
        assertEquals("Found an unexpected path while listing filesystem",
            found.size(), expected.length);
      }
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  /**
   * Test upgrade from 2.0 image with a variety of .snapshot and .reserved
   * paths to test renaming on upgrade
   */
  @Test
  public void testUpgradeFromRel2ReservedImage() throws Exception {
    unpackStorage(HADOOP2_RESERVED_IMAGE, HADOOP_DFS_DIR_TXT);
    MiniDFSCluster cluster = null;
    // Try it once without setting the upgrade flag to ensure it fails
    final Configuration conf = new Configuration();
    try {
      cluster =
          new MiniDFSCluster.Builder(conf)
              .format(false)
              .startupOption(StartupOption.UPGRADE)
              .numDataNodes(0).build();
    } catch (IOException ioe) {
        Throwable cause = ioe.getCause();
        if (cause != null && cause instanceof IllegalReservedPathException) {
          GenericTestUtils.assertExceptionContains(
              "reserved path component in this version",
              cause);
        } else {
          throw ioe;
        }
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
    // Try it again with a custom rename string
    try {
      FSImageFormat.setRenameReservedPairs(
          ".snapshot=.user-snapshot," +
          ".reserved=.my-reserved");
      cluster =
          new MiniDFSCluster.Builder(conf)
              .format(false)
              .startupOption(StartupOption.UPGRADE)
              .numDataNodes(0).build();
      DistributedFileSystem dfs = cluster.getFileSystem();
      // Make sure the paths were renamed as expected
      // Also check that paths are present after a restart, checks that the
      // upgraded fsimage has the same state.
      final String[] expected = new String[] {
          "/edits",
          "/edits/.reserved",
          "/edits/.user-snapshot",
          "/edits/.user-snapshot/editsdir",
          "/edits/.user-snapshot/editsdir/editscontents",
          "/edits/.user-snapshot/editsdir/editsdir2",
          "/image",
          "/image/.reserved",
          "/image/.user-snapshot",
          "/image/.user-snapshot/imagedir",
          "/image/.user-snapshot/imagedir/imagecontents",
          "/image/.user-snapshot/imagedir/imagedir2",
          "/.my-reserved",
          "/.my-reserved/edits-touch",
          "/.my-reserved/image-touch"
      };
      for (int i=0; i<2; i++) {
        // Restart the second time through this loop
        if (i==1) {
          cluster.finalizeCluster(conf);
          cluster.restartNameNode(true);
        }
        ArrayList<Path> toList = new ArrayList<Path>();
        toList.add(new Path("/"));
        ArrayList<String> found = new ArrayList<String>();
        while (!toList.isEmpty()) {
          Path p = toList.remove(0);
          FileStatus[] statuses = dfs.listStatus(p);
          for (FileStatus status: statuses) {
            final String path = status.getPath().toUri().getPath();
            System.out.println("Found path " + path);
            found.add(path);
            if (status.isDirectory()) {
              toList.add(status.getPath());
            }
          }
        }
        for (String s: expected) {
          assertTrue("Did not find expected path " + s, found.contains(s));
        }
        assertEquals("Found an unexpected path while listing filesystem",
            found.size(), expected.length);
      }
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }
    
  static void recoverAllLeases(DFSClient dfs, 
      Path path) throws IOException {
    String pathStr = path.toString();
    HdfsFileStatus status = dfs.getFileInfo(pathStr);
    if (!status.isDir()) {
      for (int retries = 10; retries > 0; retries--) {
        if (dfs.recoverLease(pathStr)) {
          return;
        } else {
          try {
            Thread.sleep(1000);
          } catch (InterruptedException ignored) {
          }
        }
      }
      throw new IOException("Failed to recover lease of " + path);
    }
    byte prev[] = HdfsFileStatus.EMPTY_NAME;
    DirectoryListing dirList;
    do {
      dirList = dfs.listPaths(pathStr, prev);
      HdfsFileStatus files[] = dirList.getPartialListing();
      for (HdfsFileStatus f : files) {
        recoverAllLeases(dfs, f.getFullPath(path));
      }
      prev = dirList.getLastName();
    } while (dirList.hasMore());
  }
  
  void upgradeAndVerify(MiniDFSCluster.Builder bld, ClusterVerifier verifier)
      throws IOException {
    MiniDFSCluster cluster = null;
    try {
      bld.format(false).startupOption(StartupOption.UPGRADE)
        .clusterId("testClusterId");
      cluster = bld.build();
      cluster.waitActive();
      DistributedFileSystem dfs = cluster.getFileSystem();
      DFSClient dfsClient = dfs.dfs;
      //Safemode will be off only after upgrade is complete. Wait for it.
      while ( dfsClient.setSafeMode(HdfsConstants.SafeModeAction.SAFEMODE_GET) ) {
        LOG.info("Waiting for SafeMode to be OFF.");
        try {
          Thread.sleep(1000);
        } catch (InterruptedException ignored) {}
      }
      recoverAllLeases(dfsClient, new Path("/"));
      verifyFileSystem(dfs);

      if (verifier != null) {
        verifier.verifyClusterPostUpgrade(cluster);
      }
    } finally {
      if (cluster != null) { cluster.shutdown(); }
    } 
  }

  /**
   * Test upgrade from a 1.x image with some blocksBeingWritten
   */
  @Test
  public void testUpgradeFromRel1BBWImage() throws IOException {
    unpackStorage(HADOOP1_BBW_IMAGE, HADOOP_DFS_DIR_TXT);
    Configuration conf = new Configuration(upgradeConf);
    conf.set(DFSConfigKeys.DFS_DATANODE_DATA_DIR_KEY, 
        System.getProperty("test.build.data") + File.separator + 
        "dfs" + File.separator + 
        "data" + File.separator + 
        "data1");
    upgradeAndVerify(new MiniDFSCluster.Builder(conf).
          numDataNodes(1).enableManagedDfsDirsRedundancy(false).
          manageDataDfsDirs(false), null);
  }

  @Test
  public void testPreserveEditLogs() throws Exception {
    unpackStorage(HADOOP252_IMAGE, HADOOP_DFS_DIR_TXT);
    /**
     * The pre-created image has the following edits:
     * mkdir /input; mkdir /input/dir1~5
     * copyFromLocal randome_file_1 /input/dir1
     * copyFromLocal randome_file_2 /input/dir2
     * mv /input/dir1/randome_file_1 /input/dir3/randome_file_3
     * rmdir /input/dir1
     */
    Configuration conf = new HdfsConfiguration();
    conf = UpgradeUtilities.initializeStorageStateConf(1, conf);
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).numDataNodes(0)
        .format(false)
        .manageDataDfsDirs(false)
        .manageNameDfsDirs(false)
        .startupOption(StartupOption.UPGRADE)
        .build();
    DFSInotifyEventInputStream ieis =
        cluster.getFileSystem().getInotifyEventStream(0);

    EventBatch batch;
    Event.CreateEvent ce;
    Event.RenameEvent re;

    // mkdir /input
    batch = TestDFSInotifyEventInputStream.waitForNextEvents(ieis);
    assertEquals(1, batch.getEvents().length);
    assertTrue(batch.getEvents()[0].getEventType() == Event.EventType.CREATE);
    ce = (Event.CreateEvent) batch.getEvents()[0];
    assertEquals(ce.getPath(), "/input");

    // mkdir /input/dir1~5
    for (int i = 1; i <= 5; i++) {
      batch = TestDFSInotifyEventInputStream.waitForNextEvents(ieis);
      assertEquals(1, batch.getEvents().length);
      assertTrue(batch.getEvents()[0].getEventType() == Event.EventType.CREATE);
      ce = (Event.CreateEvent) batch.getEvents()[0];
      assertEquals(ce.getPath(), "/input/dir" + i);
    }
    // copyFromLocal randome_file_1~2 /input/dir1~2
    for (int i = 1; i <= 2; i++) {
      batch = TestDFSInotifyEventInputStream.waitForNextEvents(ieis);
      assertEquals(1, batch.getEvents().length);
      if (batch.getEvents()[0].getEventType() != Event.EventType.CREATE) {
        FSImage.LOG.debug("");
      }
      assertTrue(batch.getEvents()[0].getEventType() == Event.EventType.CREATE);

      // copyFromLocal randome_file_1 /input/dir1, CLOSE
      batch = TestDFSInotifyEventInputStream.waitForNextEvents(ieis);
      assertEquals(1, batch.getEvents().length);
      assertTrue(batch.getEvents()[0].getEventType() == Event.EventType.CLOSE);

      // copyFromLocal randome_file_1 /input/dir1, CLOSE
      batch = TestDFSInotifyEventInputStream.waitForNextEvents(ieis);
      assertEquals(1, batch.getEvents().length);
      assertTrue(batch.getEvents()[0].getEventType() ==
          Event.EventType.RENAME);
      re = (Event.RenameEvent) batch.getEvents()[0];
      assertEquals(re.getDstPath(), "/input/dir" + i + "/randome_file_" + i);
    }

    // mv /input/dir1/randome_file_1 /input/dir3/randome_file_3
    long txIDBeforeRename = batch.getTxid();
    batch = TestDFSInotifyEventInputStream.waitForNextEvents(ieis);
    assertEquals(1, batch.getEvents().length);
    assertTrue(batch.getEvents()[0].getEventType() == Event.EventType.RENAME);
    re = (Event.RenameEvent) batch.getEvents()[0];
    assertEquals(re.getDstPath(), "/input/dir3/randome_file_3");


    // rmdir /input/dir1
    batch = TestDFSInotifyEventInputStream.waitForNextEvents(ieis);
    assertEquals(1, batch.getEvents().length);
    assertTrue(batch.getEvents()[0].getEventType() == Event.EventType.UNLINK);
    assertEquals(((Event.UnlinkEvent) batch.getEvents()[0]).getPath(),
        "/input/dir1");
    long lastTxID = batch.getTxid();

    // Start inotify from the tx before rename /input/dir1/randome_file_1
    ieis = cluster.getFileSystem().getInotifyEventStream(txIDBeforeRename);
    batch = TestDFSInotifyEventInputStream.waitForNextEvents(ieis);
    assertEquals(1, batch.getEvents().length);
    assertTrue(batch.getEvents()[0].getEventType() == Event.EventType.RENAME);
    re = (Event.RenameEvent) batch.getEvents()[0];
    assertEquals(re.getDstPath(), "/input/dir3/randome_file_3");

    // Try to read beyond available edits
    ieis = cluster.getFileSystem().getInotifyEventStream(lastTxID + 1);
    assertNull(ieis.poll());

    cluster.shutdown();
  }
}
