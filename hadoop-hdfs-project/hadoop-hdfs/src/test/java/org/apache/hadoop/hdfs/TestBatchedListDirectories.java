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

import org.apache.hadoop.thirdparty.com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonPathCapabilities;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.PartialListing;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.UserGroupInformation;
import org.hamcrest.core.StringContains;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Tests for the batched listing API.
 */
public class TestBatchedListDirectories {

  private static MiniDFSCluster cluster;
  private static Configuration conf;
  private static DistributedFileSystem dfs;

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  private static final List<Path> SUBDIR_PATHS = Lists.newArrayList();
  private static final List<Path> FILE_PATHS = Lists.newArrayList();
  private static final int FIRST_LEVEL_DIRS = 2;
  private static final int SECOND_LEVEL_DIRS = 3;
  private static final int FILES_PER_DIR = 5;
  private static final Path EMPTY_DIR_PATH = new Path("/emptydir");
  private static final Path DATA_FILE_PATH = new Path("/datafile");
  private static final Path INACCESSIBLE_DIR_PATH = new Path("/noperms");
  private static final Path INACCESSIBLE_FILE_PATH =
      new Path(INACCESSIBLE_DIR_PATH, "nopermsfile");

  private static Path getSubDirName(int i, int j) {
    return new Path(String.format("/dir%d/subdir%d", i, j));
  }

  private static Path getFileName(int i, int j, int k) {
    Path dirPath = getSubDirName(i, j);
    return new Path(dirPath, "file" + k);
  }

  private static void assertSubDirEquals(int i, int j, Path p) {
    assertTrue(p.toString().startsWith("hdfs://"));
    Path expected = getSubDirName(i, j);
    assertEquals("Unexpected subdir name",
        expected.toString(), p.toUri().getPath());
  }

  private static void assertFileEquals(int i, int j, int k, Path p) {
    assertTrue(p.toString().startsWith("hdfs://"));
    Path expected = getFileName(i, j, k);
    assertEquals("Unexpected file name",
        expected.toString(), p.toUri().getPath());
  }

  private static void loadData() throws Exception {
    for (int i = 0; i < FIRST_LEVEL_DIRS; i++) {
      for (int j = 0; j < SECOND_LEVEL_DIRS; j++) {
        Path dirPath = getSubDirName(i, j);
        dfs.mkdirs(dirPath);
        SUBDIR_PATHS.add(dirPath);
        for (int k = 0; k < FILES_PER_DIR; k++) {
          Path filePath = getFileName(i, j, k);
          dfs.create(filePath, (short)1).close();
          FILE_PATHS.add(filePath);
        }
      }
    }
    dfs.mkdirs(EMPTY_DIR_PATH);
    FSDataOutputStream fsout = dfs.create(DATA_FILE_PATH, (short)1);
    fsout.write(123);
    fsout.close();

    dfs.mkdirs(INACCESSIBLE_DIR_PATH);
    dfs.create(INACCESSIBLE_FILE_PATH, (short)1).close();
    dfs.setPermission(INACCESSIBLE_DIR_PATH, new FsPermission(0000));
  }

  @BeforeClass
  public static void beforeClass() throws Exception {
    conf = new HdfsConfiguration();
    conf.setInt(DFSConfigKeys.DFS_LIST_LIMIT, 7);
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_BATCHED_LISTING_LIMIT,
        FIRST_LEVEL_DIRS * SECOND_LEVEL_DIRS * FILES_PER_DIR);
    cluster = new MiniDFSCluster.Builder(conf)
        .numDataNodes(1)
        .build();
    dfs = cluster.getFileSystem();
    loadData();
  }

  @AfterClass
  public static void afterClass() {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  private static List<PartialListing<FileStatus>> getListings(List<Path> paths)
      throws IOException {
    List<PartialListing<FileStatus>> returned = Lists.newArrayList();
    RemoteIterator<PartialListing<FileStatus>> it =
        dfs.batchedListStatusIterator(paths);
    while (it.hasNext()) {
      returned.add(it.next());
    }
    return returned;
  }

  private static List<FileStatus> listingsToStatuses(
      List<PartialListing<FileStatus>> listings) throws IOException {
    List<FileStatus> returned = Lists.newArrayList();
    for (PartialListing<FileStatus> listing : listings) {
      returned.addAll(listing.get());
    }
    return returned;
  }

  private static List<FileStatus> getStatuses(List<Path> paths)
      throws IOException {
    List<PartialListing<FileStatus>> listings = getListings(paths);
    return listingsToStatuses(listings);
  }

  @Test
  public void testEmptyPath() throws Exception {
    thrown.expect(FileNotFoundException.class);
    List<Path> paths = Lists.newArrayList();
    getStatuses(paths);
  }

  @Test
  public void testEmptyDir() throws Exception {
    List<Path> paths = Lists.newArrayList(EMPTY_DIR_PATH);
    List<PartialListing<FileStatus>> listings = getListings(paths);
    assertEquals(1, listings.size());
    PartialListing<FileStatus> listing = listings.get(0);
    assertEquals(EMPTY_DIR_PATH, listing.getListedPath());
    assertEquals(0, listing.get().size());
  }
  @Test
  public void listOneFile() throws Exception {
    List<Path> paths = Lists.newArrayList();
    paths.add(FILE_PATHS.get(0));
    List<FileStatus> statuses = getStatuses(paths);
    assertEquals(1, statuses.size());
    assertFileEquals(0, 0, 0, statuses.get(0).getPath());
  }

  @Test
  public void listDoesNotExist() throws Exception {
    thrown.expect(FileNotFoundException.class);
    List<Path> paths = Lists.newArrayList();
    paths.add(new Path("/does/not/exist"));
    getStatuses(paths);
  }

  @Test
  public void listSomeDoNotExist() throws Exception {
    List<Path> paths = Lists.newArrayList();
    paths.add(new Path("/does/not/exist"));
    paths.addAll(SUBDIR_PATHS.subList(0, FIRST_LEVEL_DIRS));
    paths.add(new Path("/does/not/exist"));
    paths.addAll(SUBDIR_PATHS.subList(0, FIRST_LEVEL_DIRS));
    paths.add(new Path("/does/not/exist"));
    List<PartialListing<FileStatus>> listings = getListings(paths);
    for (int i = 0; i < listings.size(); i++) {
      PartialListing<FileStatus> partial = listings.get(i);
      if (partial.getListedPath().toString().equals("/does/not/exist")) {
        try {
          partial.get();
          fail("Expected exception");
        } catch (FileNotFoundException e) {
          assertTrue(e.getMessage().contains("/does/not/exist"));
        }
      } else {
        partial.get();
      }
    }
    try {
      listings.get(listings.size()-1).get();
      fail("Expected exception");
    } catch (FileNotFoundException e) {
      assertTrue(e.getMessage().contains("/does/not/exist"));
    }
  }

  @Test
  public void listDirRelative() throws Exception {
    dfs.setWorkingDirectory(new Path("/dir0"));
    List<Path> paths = Lists.newArrayList(new Path("."));
    List<FileStatus> statuses = getStatuses(paths);
    assertEquals("Wrong number of items",
        SECOND_LEVEL_DIRS, statuses.size());
    for (int i = 0; i < SECOND_LEVEL_DIRS; i++) {
      FileStatus stat = statuses.get(i);
      assertSubDirEquals(0, i, stat.getPath());
    }
  }

  @Test
  public void listFilesRelative() throws Exception {
    dfs.setWorkingDirectory(new Path("/dir0"));
    List<Path> paths = Lists.newArrayList(new Path("subdir0"));
    List<FileStatus> statuses = getStatuses(paths);
    assertEquals("Wrong number of items",
        FILES_PER_DIR, statuses.size());
    for (int i = 0; i < FILES_PER_DIR; i++) {
      FileStatus stat = statuses.get(i);
      assertFileEquals(0, 0, i, stat.getPath());
    }
  }

  @Test
  public void testDFSHasCapability() throws Throwable {
    assertTrue("FS does not declare PathCapability support",
        dfs.hasPathCapability(new Path("/"),
            CommonPathCapabilities.FS_EXPERIMENTAL_BATCH_LISTING));
  }

  private void listFilesInternal(int numFiles) throws Exception {
    List<Path> paths = FILE_PATHS.subList(0, numFiles);
    List<FileStatus> statuses = getStatuses(paths);
    assertEquals(paths.size(), statuses.size());
    for (int i = 0; i < paths.size(); i++) {
      Path p = paths.get(i);
      FileStatus stat = statuses.get(i);
      assertEquals(p.toUri().getPath(), stat.getPath().toUri().getPath());
    }
  }

  @Test
  public void listOneFiles() throws Exception {
    listFilesInternal(1);
  }

  @Test
  public void listSomeFiles() throws Exception {
    listFilesInternal(FILE_PATHS.size() / 2);
  }

  @Test
  public void listAllFiles() throws Exception {
    listFilesInternal(FILE_PATHS.size());
  }

  private void listDirectoriesInternal(int numDirs) throws Exception {
    List<Path> paths = SUBDIR_PATHS.subList(0, numDirs);
    List<PartialListing<FileStatus>> listings = getListings(paths);

    LinkedHashMap<Path, List<FileStatus>> listing = new LinkedHashMap<>();
    for (PartialListing<FileStatus> partialListing : listings) {
      Path parent = partialListing.getListedPath();
      if (!listing.containsKey(parent)) {
        listing.put(parent, Lists.newArrayList());
      }
      listing.get(parent).addAll(partialListing.get());
    }

    assertEquals(paths.size(), listing.size());
    int pathIdx = 0;
    for (Map.Entry<Path, List<FileStatus>> entry : listing.entrySet()) {
      Path expected = paths.get(pathIdx++);
      Path parent = entry.getKey();
      List<FileStatus> children = entry.getValue();
      assertEquals(expected, parent);
      assertEquals(FILES_PER_DIR, children.size());
    }
  }

  @Test
  public void listOneDirectory() throws Exception {
    listDirectoriesInternal(1);
  }

  @Test
  public void listSomeDirectories() throws Exception {
    listDirectoriesInternal(SUBDIR_PATHS.size() / 2);
  }

  @Test
  public void listAllDirectories() throws Exception {
    listDirectoriesInternal(SUBDIR_PATHS.size());
  }

  @Test
  public void listTooManyDirectories() throws Exception {
    thrown.expect(RemoteException.class);
    thrown.expectMessage(
        StringContains.containsString("Too many source paths"));
    List<Path> paths = Lists.newArrayList(FILE_PATHS);
    paths.add(SUBDIR_PATHS.get(0));
    getStatuses(paths);
  }

  @Test
  public void listDirsAndEmpty() throws Exception {
    List<Path> paths = Lists.newArrayList();
    paths.add(EMPTY_DIR_PATH);
    paths.add(FILE_PATHS.get(0));
    paths.add(EMPTY_DIR_PATH);
    List<PartialListing<FileStatus>> listings = getListings(paths);
    assertEquals(3, listings.size());
    assertEquals(0, listings.get(0).get().size());
    assertEquals(1, listings.get(1).get().size());
    assertEquals(FILE_PATHS.get(0).toString(),
        listings.get(1).get().get(0).getPath().toUri().getPath());
    assertEquals(0, listings.get(2).get().size());
  }

  @Test
  public void listSamePaths() throws Exception {
    List<Path> paths = Lists.newArrayList();
    paths.add(SUBDIR_PATHS.get(0));
    paths.add(SUBDIR_PATHS.get(0));
    paths.add(FILE_PATHS.get(0));
    paths.add(FILE_PATHS.get(0));
    List<FileStatus> statuses = getStatuses(paths);
    assertEquals(FILES_PER_DIR*2 + 2, statuses.size());
    List<FileStatus> slice = statuses.subList(0, FILES_PER_DIR);
    for (int i = 0; i < FILES_PER_DIR; i++) {
      assertFileEquals(0, 0, i, slice.get(i).getPath());
    }
    slice = statuses.subList(FILES_PER_DIR, FILES_PER_DIR*2);
    for (int i = 0; i < FILES_PER_DIR; i++) {
      assertFileEquals(0, 0, i, slice.get(i).getPath());
    }
    assertFileEquals(0, 0, 0, statuses.get(FILES_PER_DIR*2).getPath());
    assertFileEquals(0, 0, 0, statuses.get(FILES_PER_DIR*2+1).getPath());
  }

  @Test
  public void listLocatedStatus() throws Exception {
    List<Path> paths = Lists.newArrayList();
    paths.add(DATA_FILE_PATH);
    RemoteIterator<PartialListing<LocatedFileStatus>> it =
        dfs.batchedListLocatedStatusIterator(paths);
    PartialListing<LocatedFileStatus> listing = it.next();
    List<LocatedFileStatus> statuses = listing.get();
    assertEquals(1, statuses.size());
    assertTrue(statuses.get(0).getBlockLocations().length > 0);
  }

  private void listAsNormalUser(List<Path> paths)
      throws IOException, InterruptedException {
    final UserGroupInformation ugi = UserGroupInformation
        .createRemoteUser("tiffany");
    ugi.doAs(new PrivilegedExceptionAction<Void>() {
      @Override
      public Void run() throws Exception {
        // try renew with long name
        DistributedFileSystem fs = (DistributedFileSystem)
            FileSystem.get(cluster.getURI(), conf);
        RemoteIterator<PartialListing<FileStatus>> it =
            fs.batchedListStatusIterator(paths);
        PartialListing<FileStatus> listing = it.next();
        listing.get();
        return null;
      }
    });
  }

  @Test
  public void listInaccessibleDir() throws Exception {
    thrown.expect(AccessControlException.class);
    List<Path> paths = Lists.newArrayList(INACCESSIBLE_DIR_PATH);
    listAsNormalUser(paths);
  }

  @Test
  public void listInaccessibleFile() throws Exception {
    thrown.expect(AccessControlException.class);
    List<Path> paths = Lists.newArrayList(INACCESSIBLE_FILE_PATH);
    listAsNormalUser(paths);
  }
}
