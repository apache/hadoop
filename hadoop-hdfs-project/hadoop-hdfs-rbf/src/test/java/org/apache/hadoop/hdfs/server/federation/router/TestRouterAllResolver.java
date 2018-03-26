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
package org.apache.hadoop.hdfs.server.federation.router;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.server.federation.RouterConfigBuilder;
import org.apache.hadoop.hdfs.server.federation.MiniRouterDFSCluster.NamenodeContext;
import org.apache.hadoop.hdfs.server.federation.MiniRouterDFSCluster.RouterContext;
import org.apache.hadoop.hdfs.server.federation.StateStoreDFSCluster;
import org.apache.hadoop.hdfs.server.federation.resolver.MountTableManager;
import org.apache.hadoop.hdfs.server.federation.resolver.MultipleDestinationMountTableResolver;
import org.apache.hadoop.hdfs.server.federation.resolver.order.DestinationOrder;
import org.apache.hadoop.hdfs.server.federation.store.StateStoreService;
import org.apache.hadoop.hdfs.server.federation.store.protocol.AddMountTableEntryRequest;
import org.apache.hadoop.hdfs.server.federation.store.protocol.AddMountTableEntryResponse;
import org.apache.hadoop.hdfs.server.federation.store.protocol.GetMountTableEntriesRequest;
import org.apache.hadoop.hdfs.server.federation.store.protocol.GetMountTableEntriesResponse;
import org.apache.hadoop.hdfs.server.federation.store.records.MountTable;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Tests the use of the resolvers that write in all subclusters from the
 * Router. It supports:
 * <li>HashResolver
 * <li>RandomResolver.
 */
public class TestRouterAllResolver {

  /** Directory that will be in a HASH_ALL mount point. */
  private static final String TEST_DIR_HASH_ALL = "/hashall";
  /** Directory that will be in a RANDOM mount point. */
  private static final String TEST_DIR_RANDOM = "/random";
  /** Directory that will be in a SPACE mount point. */
  private static final String TEST_DIR_SPACE = "/space";

  /** Number of namespaces. */
  private static final int NUM_NAMESPACES = 2;


  /** Mini HDFS clusters with Routers and State Store. */
  private static StateStoreDFSCluster cluster;
  /** Router for testing. */
  private static RouterContext routerContext;
  /** Router/federated filesystem. */
  private static FileSystem routerFs;
  /** Filesystem for each namespace. */
  private static List<FileSystem> nsFss = new LinkedList<>();


  @Before
  public void setup() throws Exception {
    // 2 nameservices with 1 namenode each (no HA needed for this test)
    cluster = new StateStoreDFSCluster(
        false, NUM_NAMESPACES, MultipleDestinationMountTableResolver.class);

    // Start NNs and DNs and wait until ready
    cluster.startCluster();

    // Build and start a Router with: State Store + Admin + RPC
    Configuration routerConf = new RouterConfigBuilder()
        .stateStore()
        .admin()
        .rpc()
        .build();
    cluster.addRouterOverrides(routerConf);
    cluster.startRouters();
    routerContext = cluster.getRandomRouter();

    // Register and verify all NNs with all routers
    cluster.registerNamenodes();
    cluster.waitNamenodeRegistration();

    // Setup the test mount point
    createMountTableEntry(TEST_DIR_HASH_ALL, DestinationOrder.HASH_ALL);
    createMountTableEntry(TEST_DIR_RANDOM, DestinationOrder.RANDOM);
    createMountTableEntry(TEST_DIR_SPACE, DestinationOrder.SPACE);

    // Get filesystems for federated and each namespace
    routerFs = routerContext.getFileSystem();
    for (String nsId : cluster.getNameservices()) {
      List<NamenodeContext> nns = cluster.getNamenodes(nsId);
      for (NamenodeContext nn : nns) {
        FileSystem nnFs = nn.getFileSystem();
        nsFss.add(nnFs);
      }
    }
    assertEquals(NUM_NAMESPACES, nsFss.size());
  }

  @After
  public void cleanup() {
    cluster.shutdown();
    cluster = null;
    routerContext = null;
    routerFs = null;
    nsFss.clear();
  }

  @Test
  public void testHashAll() throws Exception {
    testAll(TEST_DIR_HASH_ALL);
  }

  @Test
  public void testRandomAll() throws Exception {
    testAll(TEST_DIR_RANDOM);
  }

  @Test
  public void testSpaceAll() throws Exception {
    testAll(TEST_DIR_SPACE);
  }

  /**
   * Tests that the resolver spreads files across subclusters in the whole
   * tree.
   * @throws Exception If the resolver is not working.
   */
  private void testAll(final String path) throws Exception {

    // Create directories in different levels
    routerFs.mkdirs(new Path(path + "/dir0"));
    routerFs.mkdirs(new Path(path + "/dir1"));
    routerFs.mkdirs(new Path(path + "/dir2/dir20"));
    routerFs.mkdirs(new Path(path + "/dir2/dir21"));
    routerFs.mkdirs(new Path(path + "/dir2/dir22"));
    routerFs.mkdirs(new Path(path + "/dir2/dir22/dir220"));
    routerFs.mkdirs(new Path(path + "/dir2/dir22/dir221"));
    routerFs.mkdirs(new Path(path + "/dir2/dir22/dir222"));
    assertDirsEverywhere(path, 9);

    // Create 14 files at different levels of the tree
    createTestFile(routerFs, path + "/dir0/file1.txt");
    createTestFile(routerFs, path + "/dir0/file2.txt");
    createTestFile(routerFs, path + "/dir1/file2.txt");
    createTestFile(routerFs, path + "/dir1/file3.txt");
    createTestFile(routerFs, path + "/dir2/dir20/file4.txt");
    createTestFile(routerFs, path + "/dir2/dir20/file5.txt");
    createTestFile(routerFs, path + "/dir2/dir21/file6.txt");
    createTestFile(routerFs, path + "/dir2/dir21/file7.txt");
    createTestFile(routerFs, path + "/dir2/dir22/file8.txt");
    createTestFile(routerFs, path + "/dir2/dir22/file9.txt");
    createTestFile(routerFs, path + "/dir2/dir22/dir220/file10.txt");
    createTestFile(routerFs, path + "/dir2/dir22/dir220/file11.txt");
    createTestFile(routerFs, path + "/dir2/dir22/dir220/file12.txt");
    createTestFile(routerFs, path + "/dir2/dir22/dir220/file13.txt");
    assertDirsEverywhere(path, 9);
    assertFilesDistributed(path, 14);

    // Test append
    String testFile = path + "/dir2/dir22/dir220/file-append.txt";
    createTestFile(routerFs, testFile);
    Path testFilePath = new Path(testFile);
    assertTrue("Created file is too small",
        routerFs.getFileStatus(testFilePath).getLen() > 50);
    appendTestFile(routerFs, testFile);
    assertTrue("Append file is too small",
        routerFs.getFileStatus(testFilePath).getLen() > 110);
    assertDirsEverywhere(path, 9);
    assertFilesDistributed(path, 15);

    // Removing a directory should remove it from every subcluster
    routerFs.delete(new Path(path + "/dir2/dir22/dir220"), true);
    assertDirsEverywhere(path, 8);
    assertFilesDistributed(path, 10);

    // Removing all sub directories
    routerFs.delete(new Path(path + "/dir0"), true);
    routerFs.delete(new Path(path + "/dir1"), true);
    routerFs.delete(new Path(path + "/dir2"), true);
    assertDirsEverywhere(path, 0);
    assertFilesDistributed(path, 0);
  }

  /**
   * Directories in HASH_ALL mount points must be in every namespace.
   * @param path Path to check under.
   * @param expectedNumDirs Expected number of directories.
   * @throws IOException If it cannot check the directories.
   */
  private void assertDirsEverywhere(String path, int expectedNumDirs)
      throws IOException {

    // Check for the directories in each filesystem
    List<FileStatus> files = listRecursive(routerFs, path);
    int numDirs = 0;
    for (FileStatus file : files) {
      if (file.isDirectory()) {
        numDirs++;

        Path dirPath = file.getPath();
        Path checkPath = getRelativePath(dirPath);
        for (FileSystem nsFs : nsFss) {
          FileStatus fileStatus1 = nsFs.getFileStatus(checkPath);
          assertTrue(file + " should be a directory",
              fileStatus1.isDirectory());
        }
      }
    }
    assertEquals(expectedNumDirs, numDirs);
  }

  /**
   * Check that the files are somewhat spread across namespaces.
   * @param path Path to check under.
   * @param expectedNumFiles Number of files expected.
   * @throws IOException If the files cannot be checked.
   */
  private void assertFilesDistributed(String path, int expectedNumFiles)
      throws IOException {

    // Check where the files went
    List<FileStatus> routerFiles = listRecursive(routerFs, path);
    List<List<FileStatus>> nssFiles = new LinkedList<>();
    for (FileSystem nsFs : nsFss) {
      List<FileStatus> nsFiles = listRecursive(nsFs, path);
      nssFiles.add(nsFiles);
    }

    // We should see all the files in the federated view
    int numRouterFiles = getNumTxtFiles(routerFiles);
    assertEquals(numRouterFiles, expectedNumFiles);

    // All the files should be spread somewhat evenly across subclusters
    List<Integer> numNsFiles = new LinkedList<>();
    int sumNsFiles = 0;
    for (int i = 0; i < NUM_NAMESPACES; i++) {
      List<FileStatus> nsFiles = nssFiles.get(i);
      int numFiles = getNumTxtFiles(nsFiles);
      numNsFiles.add(numFiles);
      sumNsFiles += numFiles;
    }
    assertEquals(numRouterFiles, sumNsFiles);
    if (expectedNumFiles > 0) {
      for (int numFiles : numNsFiles) {
        assertTrue("Files not distributed: " + numNsFiles, numFiles > 0);
      }
    }
  }

  /**
   * Create a test file in the filesystem and check if it was written.
   * @param fs Filesystem.
   * @param filename Name of the file to create.
   * @throws IOException If it cannot create the file.
   */
  private static void createTestFile(
      final FileSystem fs, final String filename)throws IOException {

    final Path path = new Path(filename);

    // Write the data
    FSDataOutputStream os = fs.create(path);
    os.writeUTF("Test data " + filename);
    os.close();

    // Read the data and check
    FSDataInputStream is = fs.open(path);
    String read = is.readUTF();
    assertEquals("Test data " + filename, read);
    is.close();
  }

  /**
   * Append to a test file in the filesystem and check if we appended.
   * @param fs Filesystem.
   * @param filename Name of the file to append to.
   * @throws IOException
   */
  private static void appendTestFile(
      final FileSystem fs, final String filename) throws IOException {
    final Path path = new Path(filename);

    // Write the data
    FSDataOutputStream os = fs.append(path);
    os.writeUTF("Test append data " + filename);
    os.close();

    // Read the data previous data
    FSDataInputStream is = fs.open(path);
    String read = is.readUTF();
    assertEquals(read, "Test data " + filename);
    // Read the new data and check
    read = is.readUTF();
    assertEquals(read, "Test append data " + filename);
    is.close();
  }

  /**
   * Count the number of text files in a list.
   * @param files File list.
   * @return Number of .txt files.
   */
  private static int getNumTxtFiles(final List<FileStatus> files) {
    int numFiles = 0;
    for (FileStatus file : files) {
      if (file.getPath().getName().endsWith(".txt")) {
        numFiles++;
      }
    }
    return numFiles;
  }

  /**
   * Get the relative path within a filesystem (removes the filesystem prefix).
   * @param path Path to check.
   * @return File within the filesystem.
   */
  private static Path getRelativePath(final Path path) {
    URI uri = path.toUri();
    String uriPath = uri.getPath();
    return new Path(uriPath);
  }

  /**
   * Get the list the files/dirs under a path.
   * @param fs Filesystem to check in.
   * @param path Path to check for.
   * @return List of files.
   * @throws IOException If it cannot list the files.
   */
  private List<FileStatus> listRecursive(
      final FileSystem fs, final String path) throws IOException {
    List<FileStatus> ret = new LinkedList<>();
    List<Path> temp = new LinkedList<>();
    temp.add(new Path(path));
    while (!temp.isEmpty()) {
      Path p = temp.remove(0);
      for (FileStatus fileStatus : fs.listStatus(p)) {
        ret.add(fileStatus);
        if (fileStatus.isDirectory()) {
          temp.add(fileStatus.getPath());
        }
      }
    }
    return ret;
  }

  /**
   * Add a mount table entry in all nameservices and wait until it is
   * available in all routers.
   * @param mountPoint Name of the mount point.
   * @param order Order of the mount table entry.
   * @throws Exception If the entry could not be created.
   */
  private void createMountTableEntry(
      final String mountPoint, final DestinationOrder order) throws Exception {

    RouterClient admin = routerContext.getAdminClient();
    MountTableManager mountTable = admin.getMountTableManager();
    Map<String, String> destMap = new HashMap<>();
    for (String nsId : cluster.getNameservices()) {
      destMap.put(nsId, mountPoint);
    }
    MountTable newEntry = MountTable.newInstance(mountPoint, destMap);
    newEntry.setDestOrder(order);
    AddMountTableEntryRequest addRequest =
        AddMountTableEntryRequest.newInstance(newEntry);
    AddMountTableEntryResponse addResponse =
        mountTable.addMountTableEntry(addRequest);
    boolean created = addResponse.getStatus();
    assertTrue(created);

    // Refresh the caches to get the mount table
    Router router = routerContext.getRouter();
    StateStoreService stateStore = router.getStateStore();
    stateStore.refreshCaches(true);

    // Check for the path
    GetMountTableEntriesRequest getRequest =
        GetMountTableEntriesRequest.newInstance(mountPoint);
    GetMountTableEntriesResponse getResponse =
        mountTable.getMountTableEntries(getRequest);
    List<MountTable> entries = getResponse.getEntries();
    assertEquals(1, entries.size());
    assertEquals(mountPoint, entries.get(0).getSourcePath());
  }
}