/*
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

package org.apache.hadoop.fs.s3a.s3guard;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

/**
 * Main test class for MetadataStore implementations.
 * Implementations should each create a test by subclassing this and
 * overriding {@link #createContract()}.
 */
public abstract class MetadataStoreTestBase extends Assert {

  private static final Logger LOG =
      LoggerFactory.getLogger(MetadataStoreTestBase.class);

  /** Some dummy values for sanity-checking FileStatus contents. */
  protected static final long BLOCK_SIZE = 32 * 1024 * 1024;
  protected static final int REPLICATION = 1;
  private static final FsPermission PERMISSION = new FsPermission((short)0755);
  private static final String OWNER = "bob";
  private static final String GROUP = "uncles";
  private final long accessTime = System.currentTimeMillis();
  private final long modTime = accessTime - 5000;

  /**
   * Each test should override this.
   * @return Contract which specifies the MetadataStore under test plus config.
   */
  public abstract AbstractMSContract createContract();

  /** The MetadataStore contract used to test against. */
  private AbstractMSContract contract;

  private MetadataStore ms;

  @Before
  public void setUp() throws Exception {
    LOG.debug("== Setup. ==");
    contract = createContract();
    ms = contract.getMetadataStore();
    assertNotNull("null MetadataStore", ms);
    assertNotNull("null FileSystem", contract.getFileSystem());
    ms.initialize(contract.getFileSystem());
  }

  @After
  public void tearDown() throws Exception {
    LOG.debug("== Tear down. ==");
    if (ms != null) {
      ms.close();
      ms = null;
    }
  }

  @Test
  public void testPutNew() throws Exception {
    /* create three dirs /da1, /da2, /da3 */
    createNewDirs("/da1", "/da2", "/da3");

    /* It is caller's responsibility to set up ancestor entries beyond the
     * containing directory.  We only track direct children of the directory.
     * Thus this will not affect entry for /da1.
     */
    ms.put(new PathMetadata(makeFileStatus("/da1/db1/fc1", 100)));

    assertEmptyDirs("/da1", "/da2", "/da3");
    assertDirectorySize("/da1/db1", 1);

    /* Check contents of dir status. */
    PathMetadata dirMeta = ms.get(new Path("/da1"));
    verifyDirStatus(dirMeta);

    /* This already exists, and should silently replace it. */
    ms.put(new PathMetadata(makeDirStatus("/da1/db1")));

    /* If we had putNew(), and used it above, this would be empty again. */
    assertDirectorySize("/da1", 1);

    assertEmptyDirs("/da2", "/da3");

    /* Ensure new files update correct parent dirs. */
    ms.put(new PathMetadata(makeFileStatus("/da1/db1/fc1", 100)));
    ms.put(new PathMetadata(makeFileStatus("/da1/db1/fc2", 200)));
    assertDirectorySize("/da1", 1);
    assertDirectorySize("/da1/db1", 2);
    assertEmptyDirs("/da2", "/da3");
    PathMetadata meta = ms.get(new Path("/da1/db1/fc2"));
    assertNotNull("Get file after put new.", meta);
    assertEquals("Cached file size correct.", 200,
        meta.getFileStatus().getLen());
  }

  @Test
  public void testPutOverwrite() throws Exception {
    final String filePath = "/a1/b1/c1/some_file";
    final String dirPath = "/a1/b1/c1/d1";
    ms.put(new PathMetadata(makeFileStatus(filePath, 100)));
    ms.put(new PathMetadata(makeDirStatus(dirPath)));
    PathMetadata meta = ms.get(new Path(filePath));
    verifyBasicFileStatus(meta);

    ms.put(new PathMetadata(basicFileStatus(new Path(filePath), 9999, false)));
    meta = ms.get(new Path(filePath));
    assertEquals("Updated size", 9999, meta.getFileStatus().getLen());
  }

  @Test
  public void testRootDirPutNew() throws Exception {
    Path rootPath = new Path("/");

    ms.put(new PathMetadata(makeFileStatus("/file1", 100)));
    DirListingMetadata dir = ms.listChildren(rootPath);
    assertNotNull("Root dir cached", dir);
    assertFalse("Root not fully cached", dir.isAuthoritative());
    assertNotNull("have root dir file listing", dir.getListing());
    assertEquals("One file in root dir", 1, dir.getListing().size());
    assertEquals("file1 in root dir", "/file1", dir.getListing().iterator()
        .next().getFileStatus().getPath().toString());
  }

  @Test
  public void testDelete() throws Exception {
    setUpDeleteTest();

    ms.delete(new Path("/ADirectory1/db1/file2"));

    /* Ensure delete happened. */
    assertDirectorySize("/ADirectory1/db1", 1);
    PathMetadata meta = ms.get(new Path("/ADirectory1/db1/file2"));
    assertNull("File deleted", meta);
  }

  @Test
  public void testDeleteSubtree() throws Exception {
    setUpDeleteTest();
    createNewDirs("/ADirectory1/db1/dc1", "/ADirectory1/db1/dc1/dd1");
    ms.put(new PathMetadata(
        makeFileStatus("/ADirectory1/db1/dc1/dd1/deepFile", 100)));
    ms.deleteSubtree(new Path("/ADirectory1/db1"));

    assertEmptyDirectory("/ADirectory1");
    assertNotCached("/ADirectory1/file1");
    assertNotCached("/ADirectory1/file2");
    assertNotCached("/ADirectory1/db1/dc1/dd1/deepFile");
    assertEmptyDirectory("/ADirectory2");
  }


  /*
   * Some implementations might not support this.  It was useful to test
   * correctness of the LocalMetadataStore implementation, but feel free to
   * override this to be a no-op.
   */
  @Test
  public void testDeleteRecursiveRoot() throws Exception {
    setUpDeleteTest();

    ms.deleteSubtree(new Path("/"));
    assertNotCached("/ADirectory1");
    assertNotCached("/ADirectory2");
    assertNotCached("/ADirectory2/db1");
    assertNotCached("/ADirectory2/db1/file1");
    assertNotCached("/ADirectory2/db1/file2");
  }

  @Test
  public void testDeleteNonExisting() throws Exception {
    // Path doesn't exist, but should silently succeed
    ms.delete(new Path("/bobs/your/uncle"));

    // Ditto.
    ms.deleteSubtree(new Path("/internets"));
  }


  private void setUpDeleteTest() throws IOException {
    createNewDirs("/ADirectory1", "/ADirectory2", "/ADirectory1/db1");
    ms.put(new PathMetadata(makeFileStatus("/ADirectory1/db1/file1", 100)));
    ms.put(new PathMetadata(makeFileStatus("/ADirectory1/db1/file2", 100)));

    PathMetadata meta = ms.get(new Path("/ADirectory1/db1/file2"));
    assertNotNull("Found test file", meta);

    assertDirectorySize("/ADirectory1/db1", 2);
  }

  @Test
  public void testGet() throws Exception {
    final String filePath = "/a1/b1/c1/some_file";
    final String dirPath = "/a1/b1/c1/d1";
    ms.put(new PathMetadata(makeFileStatus(filePath, 100)));
    ms.put(new PathMetadata(makeDirStatus(dirPath)));
    PathMetadata meta = ms.get(new Path(filePath));
    assertNotNull("Get found file", meta);
    verifyBasicFileStatus(meta);

    meta = ms.get(new Path(dirPath));
    assertNotNull("Get found file (dir)", meta);
    assertTrue("Found dir", meta.getFileStatus().isDirectory());

    meta = ms.get(new Path("bollocks"));
    assertNull("Don't get non-existent file", meta);
  }


  @Test
  public void testListChildren() throws Exception {
    setupListStatus();

    DirListingMetadata dirMeta;
    try {
      dirMeta = ms.listChildren(new Path("/"));
      if (dirMeta != null) {
      /* Cache has no way of knowing it has all entries for root unless we
       * specifically tell it via put() with
       * DirListingMetadata.isAuthoritative = true */
        assertFalse("Root dir is not cached, or partially cached",
            dirMeta.isAuthoritative());
        assertListingsEqual(dirMeta.getListing(), "/a1", "/a2");
      }
    } catch (FileNotFoundException f) { }

    dirMeta = ms.listChildren(new Path("/a1"));
    assertListingsEqual(dirMeta.getListing(), "/a1/b1", "/a1/b2");

    // TODO
    // 1. Add properties query to MetadataStore interface
    // supportsAuthoritativeDirectories() or something.
    // 2. Add "isNew" flag to MetadataStore.put(DirListingMetadata)
    // 3. If #1 is true, assert that directory is still fully cached here.
    // assertTrue("Created dir is fully cached", dirMeta.isAuthoritative());

    dirMeta = ms.listChildren(new Path("/a1/b1"));
    assertListingsEqual(dirMeta.getListing(), "/a1/b1/file1", "/a1/b1/file2",
        "/a1/b1/c1");
  }

  @Test
  public void testDirListingRoot() throws Exception {
    commonTestPutListStatus("/");
  }

  @Test
  public void testPutDirListing() throws Exception {
    commonTestPutListStatus("/a");
  }

  @Test
  public void testInvalidListChildren() throws Exception {
    setupListStatus();
    assertNull("missing path returns null",
        ms.listChildren(new Path("/a1/b1x")));
  }

  @Ignore
  public void testMove() throws Exception {
    // TODO
    throw new RuntimeException("TODO: implement move and tests.");
  }


  /*
   * Helper functions.
   */

  /** Modifies paths input array and returns it. */
  private String[] buildPaths(String parent, String ...paths) {
    for (int i = 0; i < paths.length; i++) {
      Path p = new Path(parent, paths[i]);
      paths[i] = p.toString();
    }
    return paths;
  }

  private void commonTestPutListStatus(final String parent) throws IOException {
    putListStatusFiles(parent, true, buildPaths(parent, "file1", "file2",
        "file3"));
    DirListingMetadata dirMeta = ms.listChildren(new Path(parent));
    assertNotNull("list after putListStatus", dirMeta);
    Collection<PathMetadata> entries = dirMeta.getListing();
    assertNotNull("listStatus has entries", entries);
    assertListingsEqual(entries, buildPaths(parent, "file1", "file2", "file3"));
  }

  private void setupListStatus() throws IOException {
    createNewDirs("/a1", "/a2", "/a1/b1", "/a1/b2", "/a1/b1/c1",
        "/a1/b1/c1/d1");
    ms.put(new PathMetadata(makeFileStatus("/a1/b1/file1", 100)));
    ms.put(new PathMetadata(makeFileStatus("/a1/b1/file2", 100)));
  }

  private void assertListingsEqual(Collection<PathMetadata> listing,
      String ...pathStrs) {
    Set<Path> a = new HashSet<>();
    for (PathMetadata meta : listing) {
      a.add(meta.getFileStatus().getPath());
    }

    Set<Path> b = new HashSet<>();
    for (String ps : pathStrs) {
      b.add(new Path(ps));
    }
    assertTrue("Same set of files", a.equals(b));
  }

  private void putListStatusFiles(String dirPath, boolean authoritative,
      String... filenames) throws IOException {
    ArrayList<PathMetadata> metas = new ArrayList<>(filenames .length);
    for (int i = 0; i < filenames.length; i++) {
      metas.add(new PathMetadata(makeFileStatus(filenames[i], 100)));
    }
    DirListingMetadata dirMeta =
        new DirListingMetadata(new Path(dirPath), metas, authoritative);
    ms.put(dirMeta);
  }

  private void createNewDirs(String... dirs)
      throws IOException {
    for (String pathStr : dirs) {
      ms.put(new PathMetadata(makeDirStatus(pathStr)));
    }
  }

  private void assertDirectorySize(String pathStr, int size)
      throws IOException {
    DirListingMetadata dirMeta = ms.listChildren(new Path(pathStr));
    assertNotNull("Directory " + pathStr + " in cache", dirMeta);
    assertEquals("Number of entries in dir " + pathStr, size,
        nonDeleted(dirMeta.getListing()).size());
  }

  /** @return only file statuses which are *not* marked deleted. */
  private Collection<PathMetadata> nonDeleted(
      Collection<PathMetadata> statuses) {
    /* TODO: filter out paths marked for deletion. */
    return statuses;
  }

  private void assertNotCached(String pathStr) throws IOException {
    // TODO this should return an entry with deleted flag set
    PathMetadata meta = ms.get(new Path(pathStr));
    assertNull(pathStr + " not cached.", meta);
  }

  private void assertEmptyDirectory(String pathStr) throws IOException {
    assertDirectorySize(pathStr, 0);
  }

  private void assertEmptyDirs(String ...dirs) throws IOException {
    for (String pathStr : dirs) {
      assertEmptyDirectory(pathStr);
    }
  }

  private FileStatus basicFileStatus(Path path, int size, boolean isDir) {
    return new FileStatus(size, isDir, REPLICATION, BLOCK_SIZE, modTime,
        accessTime, PERMISSION, OWNER, GROUP, path);
  }

  private FileStatus makeFileStatus(String pathStr, int size) {
    return basicFileStatus(new Path(pathStr), size, false);
  }

  private void verifyBasicFileStatus(PathMetadata meta) {
    FileStatus status = meta.getFileStatus();
    assertFalse("Not a dir", status.isDirectory());
    assertEquals("Replication value", REPLICATION, status.getReplication());
    assertEquals("Access time", accessTime, status.getAccessTime());
    assertEquals("Mod time", modTime, status.getModificationTime());
    assertEquals("Block size", BLOCK_SIZE, status.getBlockSize());
    assertEquals("Owner", OWNER, status.getOwner());
    assertEquals("Group", GROUP, status.getGroup());
    assertEquals("Permission", PERMISSION, status.getPermission());
  }

  private FileStatus makeDirStatus(String pathStr) {
    return basicFileStatus(new Path(pathStr), 0, true);
  }

  private void verifyDirStatus(PathMetadata meta) {
    FileStatus status = meta.getFileStatus();
    assertTrue("Is a dir", status.isDirectory());
    assertEquals("zero length", 0, status.getLen());
    assertEquals("Replication value", REPLICATION, status.getReplication());
    assertEquals("Access time", accessTime, status.getAccessTime());
    assertEquals("Mod time", modTime, status.getModificationTime());
    assertEquals("Owner", OWNER, status.getOwner());
    assertEquals("Group", GROUP, status.getGroup());
    assertEquals("Permission", PERMISSION, status.getPermission());
  }
}