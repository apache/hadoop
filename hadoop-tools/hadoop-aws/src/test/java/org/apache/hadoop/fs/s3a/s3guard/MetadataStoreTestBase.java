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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.assertj.core.api.Assertions;
import org.junit.After;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.s3a.S3AFileStatus;
import org.apache.hadoop.fs.s3a.S3ATestConstants;
import org.apache.hadoop.fs.s3a.S3ATestUtils;
import org.apache.hadoop.fs.s3a.Tristate;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.test.HadoopTestBase;

import static org.apache.hadoop.thirdparty.com.google.common.base.Preconditions.checkNotNull;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.metadataStorePersistsAuthoritativeBit;

/**
 * Main test class for MetadataStore implementations.
 * Implementations should each create a test by subclassing this and
 * overriding {@link #createContract()}.
 * If your implementation may return missing results for recently set paths,
 * override {@link MetadataStoreTestBase#allowMissing()}.
 */
public abstract class MetadataStoreTestBase extends HadoopTestBase {

  private static final Logger LOG =
      LoggerFactory.getLogger(MetadataStoreTestBase.class);

  /** Some dummy values for sanity-checking FileStatus contents. */
  static final long BLOCK_SIZE = 32 * 1024 * 1024;
  static final int REPLICATION = 1;
  static final String OWNER = "bob";
  private final long modTime = System.currentTimeMillis() - 5000;

  // attributes not supported by S3AFileStatus
  static final FsPermission PERMISSION = null;
  static final String GROUP = null;
  private final long accessTime = 0;
  private static ITtlTimeProvider ttlTimeProvider;

  private static final List<Path> EMPTY_LIST = Collections.emptyList();

  /**
   * Each test should override this.  Will use a new Configuration instance.
   * @return Contract which specifies the MetadataStore under test plus config.
   */
  public abstract AbstractMSContract createContract() throws IOException;

  /**
   * Each test should override this.
   * @param conf Base configuration instance to use.
   * @return Contract which specifies the MetadataStore under test plus config.
   */
  public abstract AbstractMSContract createContract(Configuration conf)
      throws IOException;

  /**
   * Tests assume that implementations will return recently set results.  If
   * your implementation does not always hold onto metadata (e.g. LRU or
   * time-based expiry) you can override this to return false.
   * @return true if the test should succeed when null results are returned
   *  from the MetadataStore under test.
   */
  public boolean allowMissing() {
    return false;
  }

  /**
   * Pruning is an optional feature for metadata store implementations.
   * Tests will only check that functionality if it is expected to work.
   * @return true if the test should expect pruning to work.
   */
  public boolean supportsPruning() {
    return true;
  }

  /** The MetadataStore contract used to test against. */
  private AbstractMSContract contract;

  protected MetadataStore ms;

  /**
   * @return reference to the test contract.
   */
  protected AbstractMSContract getContract() {
    return contract;
  }

  @Before
  public void setUp() throws Exception {
    Thread.currentThread().setName("setup");
    LOG.debug("== Setup. ==");
    contract = createContract();
    ms = contract.getMetadataStore();
    assertNotNull("null MetadataStore", ms);
    assertNotNull("null FileSystem", contract.getFileSystem());
    ms.initialize(contract.getFileSystem(),
        new S3Guard.TtlTimeProvider(contract.getFileSystem().getConf()));
    ttlTimeProvider =
        new S3Guard.TtlTimeProvider(contract.getFileSystem().getConf());
  }

  @After
  public void tearDown() throws Exception {
    Thread.currentThread().setName("teardown");
    LOG.debug("== Tear down. ==");
    if (ms != null) {
      try {
        ms.destroy();
      } catch (Exception e) {
        LOG.warn("Failed to destroy tables in teardown", e);
      }
      IOUtils.closeStream(ms);
      ms = null;
    }
  }

  /**
   * Describe a test in the logs.
   * @param text text to print
   * @param args arguments to format in the printing
   */
  protected void describe(String text, Object... args) {
    LOG.info("\n\n{}: {}\n",
        getMethodName(),
        String.format(text, args));
  }

  /**
   * Helper function for verifying DescendantsIterator and
   * MetadataStoreListFilesIterator behavior.
   * @param createNodes List of paths to create
   * @param checkNodes List of paths that the iterator should return
   */
  private void doTestDescendantsIterator(
      Class implementation, String[] createNodes,
      String[] checkNodes) throws Exception {
    // we set up the example file system tree in metadata store
    for (String pathStr : createNodes) {
      final S3AFileStatus status = pathStr.contains("file")
          ? basicFileStatus(strToPath(pathStr), 100, false)
          : basicFileStatus(strToPath(pathStr), 0, true);
      ms.put(new PathMetadata(status), null);
    }

    final PathMetadata rootMeta = new PathMetadata(makeDirStatus("/"));
    RemoteIterator<S3AFileStatus> iterator;
    if (implementation == DescendantsIterator.class) {
      iterator = new DescendantsIterator(ms, rootMeta);
    } else if (implementation == MetadataStoreListFilesIterator.class) {
      iterator = new MetadataStoreListFilesIterator(ms, rootMeta, false);
    } else {
      throw new UnsupportedOperationException("Unrecognized class");
    }

    final Set<String> actual = new HashSet<>();
    while (iterator.hasNext()) {
      final Path p = iterator.next().getPath();
      actual.add(Path.getPathWithoutSchemeAndAuthority(p).toString());
    }
    LOG.info("We got {} by iterating DescendantsIterator", actual);

    if (!allowMissing()) {
      Assertions.assertThat(actual)
          .as("files listed through DescendantsIterator")
          .containsExactlyInAnyOrder(checkNodes);
    }
  }

  /**
   * Test that we can get the whole sub-tree by iterating DescendantsIterator.
   *
   * The tree is similar to or same as the example in code comment.
   */
  @Test
  public void testDescendantsIterator() throws Exception {
    final String[] tree = new String[] {
        "/dir1",
        "/dir1/dir2",
        "/dir1/dir3",
        "/dir1/dir2/file1",
        "/dir1/dir2/file2",
        "/dir1/dir3/dir4",
        "/dir1/dir3/dir5",
        "/dir1/dir3/dir4/file3",
        "/dir1/dir3/dir5/file4",
        "/dir1/dir3/dir6"
    };
    doTestDescendantsIterator(DescendantsIterator.class,
        tree, tree);
  }

  /**
   * Test that we can get the correct subset of the tree with
   * MetadataStoreListFilesIterator.
   *
   * The tree is similar to or same as the example in code comment.
   */
  @Test
  public void testMetadataStoreListFilesIterator() throws Exception {
    final String[] wholeTree = new String[] {
        "/dir1",
        "/dir1/dir2",
        "/dir1/dir3",
        "/dir1/dir2/file1",
        "/dir1/dir2/file2",
        "/dir1/dir3/dir4",
        "/dir1/dir3/dir5",
        "/dir1/dir3/dir4/file3",
        "/dir1/dir3/dir5/file4",
        "/dir1/dir3/dir6"
    };
    final String[] leafNodes = new String[] {
        "/dir1/dir2/file1",
        "/dir1/dir2/file2",
        "/dir1/dir3/dir4/file3",
        "/dir1/dir3/dir5/file4"
    };
    doTestDescendantsIterator(MetadataStoreListFilesIterator.class, wholeTree,
        leafNodes);
  }

  @Test
  public void testPutNew() throws Exception {
    /* create three dirs /da1, /da2, /da3 */
    createNewDirs("/da1", "/da2", "/da3");

    /* It is caller's responsibility to set up ancestor entries beyond the
     * containing directory.  We only track direct children of the directory.
     * Thus this will not affect entry for /da1.
     */
    ms.put(new PathMetadata(makeFileStatus("/da1/db1/fc1", 100)), null);

    assertEmptyDirs("/da2", "/da3");
    assertDirectorySize("/da1/db1", 1);

    /* Check contents of dir status. */
    PathMetadata dirMeta = ms.get(strToPath("/da1"));
    if (!allowMissing() || dirMeta != null) {
      verifyDirStatus(dirMeta.getFileStatus());
    }

    /* This already exists, and should silently replace it. */
    ms.put(new PathMetadata(makeDirStatus("/da1/db1")), null);

    /* If we had putNew(), and used it above, this would be empty again. */
    assertDirectorySize("/da1", 1);

    assertEmptyDirs("/da2", "/da3");

    /* Ensure new files update correct parent dirs. */
    ms.put(new PathMetadata(makeFileStatus("/da1/db1/fc1", 100)), null);
    ms.put(new PathMetadata(makeFileStatus("/da1/db1/fc2", 200)), null);
    assertDirectorySize("/da1", 1);
    assertDirectorySize("/da1/db1", 2);
    assertEmptyDirs("/da2", "/da3");
    PathMetadata meta = ms.get(strToPath("/da1/db1/fc2"));
    if (!allowMissing() || meta != null) {
      assertNotNull("Get file after put new.", meta);
      verifyFileStatus(meta.getFileStatus(), 200);
    }
  }

  @Test
  public void testPutOverwrite() throws Exception {
    final String filePath = "/a1/b1/c1/some_file";
    final String dirPath = "/a1/b1/c1/d1";
    ms.put(new PathMetadata(makeFileStatus(filePath, 100)), null);
    ms.put(new PathMetadata(makeDirStatus(dirPath)), null);
    PathMetadata meta = ms.get(strToPath(filePath));
    if (!allowMissing() || meta != null) {
      verifyFileStatus(meta.getFileStatus(), 100);
    }

    ms.put(new PathMetadata(basicFileStatus(strToPath(filePath), 9999, false)),
        null);
    meta = ms.get(strToPath(filePath));
    if (!allowMissing() || meta != null) {
      verifyFileStatus(meta.getFileStatus(), 9999);
    }
  }

  @Test
  public void testRootDirPutNew() throws Exception {
    Path rootPath = strToPath("/");

    ms.put(new PathMetadata(makeFileStatus("/file1", 100)), null);
    DirListingMetadata dir = ms.listChildren(rootPath);
    if (!allowMissing() || dir != null) {
      assertNotNull("Root dir cached", dir);
      assertFalse("Root not fully cached", dir.isAuthoritative());
      final Collection<PathMetadata> listing = dir.getListing();
      Assertions.assertThat(listing)
          .describedAs("Root dir listing")
          .isNotNull()
          .extracting(p -> p.getFileStatus().getPath())
          .containsExactly(strToPath("/file1"));
    }
  }

  @Test
  public void testDelete() throws Exception {
    setUpDeleteTest();

    ms.delete(strToPath("/ADirectory1/db1/file2"), null);

    /* Ensure delete happened. */
    assertDirectorySize("/ADirectory1/db1", 1);
    PathMetadata meta = ms.get(strToPath("/ADirectory1/db1/file2"));
    assertTrue("File deleted", meta == null || meta.isDeleted());
  }

  @Test
  public void testDeleteSubtree() throws Exception {
    deleteSubtreeHelper("");
  }

  @Test
  public void testDeleteSubtreeHostPath() throws Exception {
    deleteSubtreeHelper(contract.getFileSystem().getUri().toString());
  }

  private void deleteSubtreeHelper(String pathPrefix) throws Exception {

    String p = pathPrefix;
    setUpDeleteTest(p);
    createNewDirs(p + "/ADirectory1/db1/dc1", p + "/ADirectory1/db1/dc1/dd1");
    ms.put(new PathMetadata(
        makeFileStatus(p + "/ADirectory1/db1/dc1/dd1/deepFile", 100)),
        null);
    if (!allowMissing()) {
      assertCached(p + "/ADirectory1/db1");
    }
    ms.deleteSubtree(strToPath(p + "/ADirectory1/db1/"), null);

    assertEmptyDirectory(p + "/ADirectory1");
    assertDeleted(p + "/ADirectory1/db1");
    assertDeleted(p + "/ADirectory1/file1");
    assertDeleted(p + "/ADirectory1/file2");
    assertDeleted(p + "/ADirectory1/db1/dc1/dd1/deepFile");
    assertEmptyDirectory(p + "/ADirectory2");
  }


  /*
   * Some implementations might not support this.  It was useful to test
   * correctness of the LocalMetadataStore implementation, but feel free to
   * override this to be a no-op.
   */
  @Test
  public void testDeleteRecursiveRoot() throws Exception {
    setUpDeleteTest();

    ms.deleteSubtree(strToPath("/"), null);
    assertDeleted("/ADirectory1");
    assertDeleted("/ADirectory2");
    assertDeleted("/ADirectory2/db1");
    assertDeleted("/ADirectory2/db1/file1");
    assertDeleted("/ADirectory2/db1/file2");
  }

  @Test
  public void testDeleteNonExisting() throws Exception {
    // Path doesn't exist, but should silently succeed
    ms.delete(strToPath("/bobs/your/uncle"), null);

    // Ditto.
    ms.deleteSubtree(strToPath("/internets"), null);
  }


  private void setUpDeleteTest() throws IOException {
    setUpDeleteTest("");
  }

  private void setUpDeleteTest(String prefix) throws IOException {
    createNewDirs(prefix + "/ADirectory1", prefix + "/ADirectory2",
        prefix + "/ADirectory1/db1");
    ms.put(new PathMetadata(makeFileStatus(prefix + "/ADirectory1/db1/file1",
        100)),
        null);
    ms.put(new PathMetadata(makeFileStatus(prefix + "/ADirectory1/db1/file2",
        100)),
        null);

    PathMetadata meta = ms.get(strToPath(prefix + "/ADirectory1/db1/file2"));
    if (!allowMissing() || meta != null) {
      assertNotNull("Found test file", meta);
      assertDirectorySize(prefix + "/ADirectory1/db1", 2);
    }
  }

  @Test
  public void testGet() throws Exception {
    final String filePath = "/a1/b1/c1/some_file";
    final String dirPath = "/a1/b1/c1/d1";
    ms.put(new PathMetadata(makeFileStatus(filePath, 100)), null);
    ms.put(new PathMetadata(makeDirStatus(dirPath)), null);
    PathMetadata meta = ms.get(strToPath(filePath));
    if (!allowMissing() || meta != null) {
      assertNotNull("Get found file", meta);
      verifyFileStatus(meta.getFileStatus(), 100);
    }

    if (!(ms instanceof NullMetadataStore)) {
      ms.delete(strToPath(filePath), null);
      meta = ms.get(strToPath(filePath));
      assertTrue("Tombstone not left for deleted file", meta.isDeleted());
    }

    meta = ms.get(strToPath(dirPath));
    if (!allowMissing() || meta != null) {
      assertNotNull("Get found file (dir)", meta);
      assertTrue("Found dir", meta.getFileStatus().isDirectory());
    }

    meta = ms.get(strToPath("/bollocks"));
    assertNull("Don't get non-existent file", meta);
  }

  @Test
  public void testGetEmptyDir() throws Exception {
    final String dirPath = "/a1/b1/c1/d1";
    // Creates /a1/b1/c1/d1 as an empty dir
    setupListStatus();

    // 1. Tell MetadataStore (MS) that there are zero children
    putListStatusFiles(dirPath, true /* authoritative */
        /* zero children */);

    // 2. Request a file status for dir, including whether or not the dir
    // is empty.
    PathMetadata meta = ms.get(strToPath(dirPath), true);

    // 3. Check that either (a) the MS doesn't track whether or not it is
    // empty (which is allowed), or (b) the MS knows the dir is empty.
    if (!allowMissing() || meta != null) {
      assertNotNull("Get should find meta for dir", meta);
      assertNotEquals("Dir is empty or unknown", Tristate.FALSE,
          meta.isEmptyDirectory());
    }
  }

  @Test
  public void testGetNonEmptyDir() throws Exception {
    final String dirPath = "/a1/b1/c1";
    // Creates /a1/b1/c1 as an non-empty dir
    setupListStatus();

    // Request a file status for dir, including whether or not the dir
    // is empty.
    PathMetadata meta = ms.get(strToPath(dirPath), true);

    // MetadataStore knows /a1/b1/c1 has at least one child.  It is valid
    // for it to answer either (a) UNKNOWN: the MS doesn't track whether
    // or not the dir is empty, or (b) the MS knows the dir is non-empty.
    if (!allowMissing() || meta != null) {
      assertNotNull("Get should find meta for dir", meta);
      assertNotEquals("Dir is non-empty or unknown", Tristate.TRUE,
          meta.isEmptyDirectory());
    }
  }

  @Test
  public void testGetDirUnknownIfEmpty() throws Exception {
    final String dirPath = "/a1/b1/c1/d1";
    // 1. Create /a1/b1/c1/d1 as an empty dir, but do not tell MetadataStore
    // (MS) whether or not it has any children.
    setupListStatus();

    // 2. Request a file status for dir, including whether or not the dir
    // is empty.
    PathMetadata meta = ms.get(strToPath(dirPath), true);

    // 3. Assert MS reports isEmptyDir as UNKONWN: We haven't told MS
    // whether or not the directory has any children.
    if (!allowMissing() || meta != null) {
      assertNotNull("Get should find meta for dir", meta);
      assertEquals("Dir empty is unknown", Tristate.UNKNOWN,
          meta.isEmptyDirectory());
    }
  }

  @Test
  public void testListChildren() throws Exception {
    setupListStatus();

    DirListingMetadata dirMeta;
    dirMeta = ms.listChildren(strToPath("/"));
    if (!allowMissing()) {
      assertNotNull(dirMeta);
        /* Cache has no way of knowing it has all entries for root unless we
         * specifically tell it via put() with
         * DirListingMetadata.isAuthoritative = true */
      assertFalse("Root dir is not cached, or partially cached",
          dirMeta.isAuthoritative());
      assertListingsEqual(dirMeta.getListing(), "/a1", "/a2");
    }

    dirMeta = ms.listChildren(strToPath("/a1"));
    if (!allowMissing() || dirMeta != null) {
      dirMeta = dirMeta.withoutTombstones();
      assertListingsEqual(dirMeta.getListing(), "/a1/b1", "/a1/b2");
    }

    dirMeta = ms.listChildren(strToPath("/a1/b1"));
    if (!allowMissing() || dirMeta != null) {
      assertListingsEqual(dirMeta.getListing(), "/a1/b1/file1", "/a1/b1/file2",
          "/a1/b1/c1");
    }
  }



  @Test
  public void testListChildrenAuthoritative() throws IOException {
    Assume.assumeTrue("MetadataStore should be capable for authoritative "
        + "storage of directories to run this test.",
        metadataStorePersistsAuthoritativeBit(ms));

    setupListStatus();

    DirListingMetadata dirMeta = ms.listChildren(strToPath("/a1/b1"));
    dirMeta.setAuthoritative(true);
    dirMeta.put(new PathMetadata(
        makeFileStatus("/a1/b1/file_new", 100)));
    ms.put(dirMeta, EMPTY_LIST, null);

    dirMeta = ms.listChildren(strToPath("/a1/b1"));
    assertListingsEqual(dirMeta.getListing(), "/a1/b1/file1", "/a1/b1/file2",
        "/a1/b1/c1", "/a1/b1/file_new");
    assertTrue(dirMeta.isAuthoritative());
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
        ms.listChildren(strToPath("/a1/b1x")));
  }

  @Test
  public void testMove() throws Exception {
    // Create test dir structure
    createNewDirs("/a1", "/a2", "/a3");
    createNewDirs("/a1/b1", "/a1/b2");
    putListStatusFiles("/a1/b1", false, "/a1/b1/file1", "/a1/b1/file2");

    // Assert root listing as expected
    Collection<PathMetadata> entries;
    DirListingMetadata dirMeta = ms.listChildren(strToPath("/"));
    if (!allowMissing() || dirMeta != null) {
      dirMeta = dirMeta.withoutTombstones();
      assertNotNull("Listing root", dirMeta);
      entries = dirMeta.getListing();
      assertListingsEqual(entries, "/a1", "/a2", "/a3");
    }

    // Assert src listing as expected
    dirMeta = ms.listChildren(strToPath("/a1/b1"));
    if (!allowMissing() || dirMeta != null) {
      assertNotNull("Listing /a1/b1", dirMeta);
      entries = dirMeta.getListing();
      assertListingsEqual(entries, "/a1/b1/file1", "/a1/b1/file2");
    }

    // Do the move(): rename(/a1/b1, /b1)
    Collection<Path> srcPaths = Arrays.asList(strToPath("/a1/b1"),
        strToPath("/a1/b1/file1"), strToPath("/a1/b1/file2"));

    ArrayList<PathMetadata> destMetas = new ArrayList<>();
    destMetas.add(new PathMetadata(makeDirStatus("/b1")));
    destMetas.add(new PathMetadata(makeFileStatus("/b1/file1", 100)));
    destMetas.add(new PathMetadata(makeFileStatus("/b1/file2", 100)));
    ms.move(srcPaths, destMetas, null);

    // Assert src is no longer there
    dirMeta = ms.listChildren(strToPath("/a1"));
    if (!allowMissing() || dirMeta != null) {
      assertNotNull("Listing /a1", dirMeta);
      entries = dirMeta.withoutTombstones().getListing();
      assertListingsEqual(entries, "/a1/b2");
    }

    PathMetadata meta = ms.get(strToPath("/a1/b1/file1"));
    assertTrue("Src path deleted", meta == null || meta.isDeleted());

    // Assert dest looks right
    meta = ms.get(strToPath("/b1/file1"));
    if (!allowMissing() || meta != null) {
      assertNotNull("dest file not null", meta);
      verifyFileStatus(meta.getFileStatus(), 100);
    }

    dirMeta = ms.listChildren(strToPath("/b1"));
    if (!allowMissing() || dirMeta != null) {
      assertNotNull("dest listing not null", dirMeta);
      entries = dirMeta.getListing();
      assertListingsEqual(entries, "/b1/file1", "/b1/file2");
    }
  }

  /**
   * Test that the MetadataStore differentiates between the same path in two
   * different buckets.
   */
  @Test
  public void testMultiBucketPaths() throws Exception {
    String p1 = "s3a://bucket-a/path1";
    String p2 = "s3a://bucket-b/path2";

    // Make sure we start out empty
    PathMetadata meta = ms.get(new Path(p1));
    assertNull("Path should not be present yet.", meta);
    meta = ms.get(new Path(p2));
    assertNull("Path2 should not be present yet.", meta);

    // Put p1, assert p2 doesn't match
    ms.put(new PathMetadata(makeFileStatus(p1, 100)), null);
    meta = ms.get(new Path(p2));
    assertNull("Path 2 should not match path 1.", meta);

    // Make sure delete is correct as well
    if (!allowMissing()) {
      ms.delete(new Path(p2), null);
      meta = ms.get(new Path(p1));
      assertNotNull("Path should not have been deleted", meta);
    }
    ms.delete(new Path(p1), null);
  }

  @Test
  public void testPruneFiles() throws Exception {
    Assume.assumeTrue(supportsPruning());
    createNewDirs("/pruneFiles");

    long oldTime = getTime();
    ms.put(new PathMetadata(makeFileStatus("/pruneFiles/old", 1, oldTime)),
        null);
    DirListingMetadata ls2 = ms.listChildren(strToPath("/pruneFiles"));
    if (!allowMissing()) {
      assertListingsEqual(ls2.getListing(), "/pruneFiles/old");
    }

    // It's possible for the Local implementation to get from /pruneFiles/old's
    // modification time to here in under 1ms, causing it to not get pruned
    Thread.sleep(1);
    long cutoff = System.currentTimeMillis();
    long newTime = getTime();
    ms.put(new PathMetadata(makeFileStatus("/pruneFiles/new", 1, newTime)),
        null);

    DirListingMetadata ls;
    ls = ms.listChildren(strToPath("/pruneFiles"));
    if (!allowMissing()) {
      assertListingsEqual(ls.getListing(), "/pruneFiles/new",
          "/pruneFiles/old");
    }
    ms.prune(MetadataStore.PruneMode.ALL_BY_MODTIME, cutoff);
    ls = ms.listChildren(strToPath("/pruneFiles"));
    if (allowMissing()) {
      assertDeleted("/pruneFiles/old");
    } else {
      assertListingsEqual(ls.getListing(), "/pruneFiles/new");
    }
  }

  @Test
  public void testPruneDirs() throws Exception {
    Assume.assumeTrue(supportsPruning());

    // We only test that files, not dirs, are removed during prune.
    // We specifically allow directories to remain, as it is more robust
    // for DynamoDBMetadataStore's prune() implementation: If a
    // file was created in a directory while it was being pruned, it would
    // violate the invariant that all ancestors of a file exist in the table.

    createNewDirs("/pruneDirs/dir");

    long oldTime = getTime();
    ms.put(new PathMetadata(makeFileStatus("/pruneDirs/dir/file",
        1, oldTime)), null);

    // It's possible for the Local implementation to get from the old
    // modification time to here in under 1ms, causing it to not get pruned
    Thread.sleep(1);
    long cutoff = getTime();

    ms.prune(MetadataStore.PruneMode.ALL_BY_MODTIME, cutoff);

    assertDeleted("/pruneDirs/dir/file");
  }

  @Test
  public void testPruneUnsetsAuthoritative() throws Exception {
    String rootDir = "/unpruned-root-dir";
    String grandparentDir = rootDir + "/pruned-grandparent-dir";
    String parentDir = grandparentDir + "/pruned-parent-dir";
    String staleFile = parentDir + "/stale-file";
    String freshFile = rootDir + "/fresh-file";
    String[] directories = {rootDir, grandparentDir, parentDir};

    createNewDirs(rootDir, grandparentDir, parentDir);
    long time = System.currentTimeMillis();
    ms.put(new PathMetadata(
        basicFileStatus(0, false, 0, time - 1, strToPath(staleFile)),
        Tristate.FALSE, false),
        null);
    ms.put(new PathMetadata(
        basicFileStatus(0, false, 0, time + 1, strToPath(freshFile)),
        Tristate.FALSE, false),
        null);

    // set parent dir as authoritative
    if (!allowMissing()) {
      DirListingMetadata parentDirMd = ms.listChildren(strToPath(parentDir));
      parentDirMd.setAuthoritative(true);
      ms.put(parentDirMd, EMPTY_LIST, null);
    }

    ms.prune(MetadataStore.PruneMode.ALL_BY_MODTIME, time);
    DirListingMetadata listing;
    for (String directory : directories) {
      Path path = strToPath(directory);
      if (ms.get(path) != null) {
        listing = ms.listChildren(path);
        assertFalse(listing.isAuthoritative());
      }
    }
  }

  @Test
  public void testPrunePreservesAuthoritative() throws Exception {
    String rootDir = "/unpruned-root-dir";
    String grandparentDir = rootDir + "/pruned-grandparent-dir";
    String parentDir = grandparentDir + "/pruned-parent-dir";
    String staleFile = parentDir + "/stale-file";
    String freshFile = rootDir + "/fresh-file";
    String[] directories = {rootDir, grandparentDir, parentDir};

    // create dirs
    createNewDirs(rootDir, grandparentDir, parentDir);
    long time = System.currentTimeMillis();
    ms.put(new PathMetadata(
        basicFileStatus(0, false, 0, time + 1, strToPath(staleFile)),
        Tristate.FALSE, false),
        null);
    ms.put(new PathMetadata(
        basicFileStatus(0, false, 0, time + 1, strToPath(freshFile)),
        Tristate.FALSE, false),
        null);

    if (!allowMissing()) {
      // set parent dir as authoritative
      DirListingMetadata parentDirMd = ms.listChildren(strToPath(parentDir));
      parentDirMd.setAuthoritative(true);
      ms.put(parentDirMd, EMPTY_LIST, null);

      // prune the ms
      ms.prune(MetadataStore.PruneMode.ALL_BY_MODTIME, time);

      // get the directory listings
      DirListingMetadata rootDirMd = ms.listChildren(strToPath(rootDir));
      DirListingMetadata grandParentDirMd =
          ms.listChildren(strToPath(grandparentDir));
      parentDirMd = ms.listChildren(strToPath(parentDir));

      // assert that parent dir is still authoritative (no removed elements
      // during prune)
      assertFalse(rootDirMd.isAuthoritative());
      assertFalse(grandParentDirMd.isAuthoritative());
      assertTrue(parentDirMd.isAuthoritative());
    }
  }

  @Test
  public void testPutDirListingMetadataPutsFileMetadata()
      throws IOException {
    boolean authoritative = true;
    String[] filenames = {"/dir1/file1", "/dir1/file2", "/dir1/file3"};
    String dirPath = "/dir1";

    ArrayList<PathMetadata> metas = new ArrayList<>(filenames.length);
    for (String filename : filenames) {
      metas.add(new PathMetadata(makeFileStatus(filename, 100)));
    }
    DirListingMetadata dirMeta =
        new DirListingMetadata(strToPath(dirPath), metas, authoritative);
    ms.put(dirMeta, EMPTY_LIST, null);

    if (!allowMissing()) {
      assertDirectorySize(dirPath, filenames.length);
      PathMetadata metadata;
      for(String fileName : filenames){
        metadata = ms.get(strToPath(fileName));
        assertNotNull(String.format(
            "PathMetadata for file %s should not be null.", fileName),
            metadata);
      }
    }
  }

  @Test
  public void testPutRetainsIsDeletedInParentListing() throws Exception {
    final Path path = strToPath("/a/b");
    final S3AFileStatus fileStatus = basicFileStatus(path, 0, false);
    PathMetadata pm = new PathMetadata(fileStatus);
    pm.setIsDeleted(true);
    ms.put(pm, null);
    if(!allowMissing()) {
      final PathMetadata pathMetadata =
          ms.listChildren(path.getParent()).get(path);
      assertTrue("isDeleted should be true on the parent listing",
          pathMetadata.isDeleted());
    }
  }

  @Test
  public void testPruneExpiredTombstones() throws Exception {
    List<String> keepFilenames = new ArrayList<>(
        Arrays.asList("/dir1/fileK1", "/dir1/fileK2", "/dir1/fileK3"));
    List<String> removeFilenames = new ArrayList<>(
        Arrays.asList("/dir1/fileR1", "/dir1/fileR2", "/dir1/fileR3"));

    long cutoff = 9001;

    for(String fN : keepFilenames) {
      final PathMetadata pathMetadata = new PathMetadata(makeFileStatus(fN, 1));
      pathMetadata.setLastUpdated(9002L);
      ms.put(pathMetadata);
    }

    for(String fN : removeFilenames) {
      final PathMetadata pathMetadata = new PathMetadata(makeFileStatus(fN, 1));
      pathMetadata.setLastUpdated(9000L);
      // tombstones are the deleted files!
      pathMetadata.setIsDeleted(true);
      ms.put(pathMetadata);
    }

    ms.prune(MetadataStore.PruneMode.TOMBSTONES_BY_LASTUPDATED, cutoff);

    if (!allowMissing()) {
      for (String fN : keepFilenames) {
        final PathMetadata pathMetadata = ms.get(strToPath(fN));
        assertNotNull("Kept files should be in the metastore after prune",
            pathMetadata);
      }
    }

    for(String fN : removeFilenames) {
      final PathMetadata pathMetadata = ms.get(strToPath(fN));
      assertNull("Expired tombstones should be removed from metastore after "
          + "the prune.", pathMetadata);
    }
  }

  @Test
  public void testPruneExpiredTombstonesSpecifiedPath() throws Exception {
    List<String> keepFilenames = new ArrayList<>(
        Arrays.asList("/dir1/fileK1", "/dir1/fileK2", "/dir1/fileK3"));
    List<String> removeFilenames = new ArrayList<>(
        Arrays.asList("/dir2/fileR1", "/dir2/fileR2", "/dir2/fileR3"));

    long cutoff = 9001;

    // Both are expired. Difference is it will only delete the specified one.
    for (String fN : keepFilenames) {
      final PathMetadata pathMetadata = new PathMetadata(makeFileStatus(fN, 1));
      pathMetadata.setLastUpdated(9002L);
      ms.put(pathMetadata);
    }

    for (String fN : removeFilenames) {
      final PathMetadata pathMetadata = new PathMetadata(makeFileStatus(fN, 1));
      pathMetadata.setLastUpdated(9000L);
      // tombstones are the deleted files!
      pathMetadata.setIsDeleted(true);
      ms.put(pathMetadata);
    }

    final String prunePath = getPathStringForPrune("/dir2");
    ms.prune(MetadataStore.PruneMode.TOMBSTONES_BY_LASTUPDATED, cutoff,
        prunePath);

    if (!allowMissing()) {
      for (String fN : keepFilenames) {
        final PathMetadata pathMetadata = ms.get(strToPath(fN));
        assertNotNull("Kept files should be in the metastore after prune",
            pathMetadata);
      }
    }

    for (String fN : removeFilenames) {
      final PathMetadata pathMetadata = ms.get(strToPath(fN));
      assertNull("Expired tombstones should be removed from metastore after "
          + "the prune.", pathMetadata);
    }
  }

  /*
   * Helper functions.
   */

  /** Modifies paths input array and returns it. */
  private String[] buildPathStrings(String parent, String... paths)
      throws IOException {
    for (int i = 0; i < paths.length; i++) {
      Path p = new Path(strToPath(parent), paths[i]);
      paths[i] = p.toString();
    }
    return paths;
  }


  /**
   * The prune operation needs the path with the bucket name as a string in
   * {@link DynamoDBMetadataStore}, but not for {@link LocalMetadataStore}.
   * This is an implementation detail of the ms, so this should be
   * implemented in the subclasses.
   */
  protected abstract String getPathStringForPrune(String path)
      throws Exception;

  private void commonTestPutListStatus(final String parent) throws IOException {
    putListStatusFiles(parent, true, buildPathStrings(parent, "file1", "file2",
        "file3"));
    DirListingMetadata dirMeta = ms.listChildren(strToPath(parent));
    if (!allowMissing() || dirMeta != null) {
      dirMeta = dirMeta.withoutTombstones();
      assertNotNull("list after putListStatus", dirMeta);
      Collection<PathMetadata> entries = dirMeta.getListing();
      assertNotNull("listStatus has entries", entries);
      assertListingsEqual(entries,
          buildPathStrings(parent, "file1", "file2", "file3"));
    }
  }

  private void setupListStatus() throws IOException {
    createNewDirs("/a1", "/a2", "/a1/b1", "/a1/b2", "/a1/b1/c1",
        "/a1/b1/c1/d1");
    ms.put(new PathMetadata(makeFileStatus("/a1/b1/file1", 100)), null);
    ms.put(new PathMetadata(makeFileStatus("/a1/b1/file2", 100)), null);
  }

  private void assertListingsEqual(Collection<PathMetadata> listing,
      String ...pathStrs) throws IOException {
    Set<Path> a = new HashSet<>();
    for (PathMetadata meta : listing) {
      a.add(meta.getFileStatus().getPath());
    }

    Set<Path> b = new HashSet<>();
    for (String ps : pathStrs) {
      b.add(strToPath(ps));
    }
    Assertions.assertThat(a)
        .as("Directory Listing")
        .containsExactlyInAnyOrderElementsOf(b);
  }

  protected void putListStatusFiles(String dirPath, boolean authoritative,
      String... filenames) throws IOException {
    ArrayList<PathMetadata> metas = new ArrayList<>(filenames .length);
    for (String filename : filenames) {
      metas.add(new PathMetadata(makeFileStatus(filename, 100)));
    }
    DirListingMetadata dirMeta =
        new DirListingMetadata(strToPath(dirPath), metas, authoritative);
    ms.put(dirMeta, EMPTY_LIST, null);
  }

  protected void createNewDirs(String... dirs)
      throws IOException {
    for (String pathStr : dirs) {
      ms.put(new PathMetadata(makeDirStatus(pathStr)), null);
    }
  }

  private void assertDirectorySize(String pathStr, int size)
      throws IOException {
    DirListingMetadata dirMeta = ms.listChildren(strToPath(pathStr));
    if (!allowMissing()) {
      assertNotNull("Directory " + pathStr + " is null in cache", dirMeta);
    }
    if (!allowMissing() || dirMeta != null) {
      dirMeta = dirMeta.withoutTombstones();
      Assertions.assertThat(nonDeleted(dirMeta.getListing()))
          .as("files in directory %s", pathStr)
          .hasSize(size);
    }
  }

  /** @return only file statuses which are *not* marked deleted. */
  private Collection<PathMetadata> nonDeleted(
      Collection<PathMetadata> statuses) {
    Collection<PathMetadata> currentStatuses = new ArrayList<>();
    for (PathMetadata status : statuses) {
      if (!status.isDeleted()) {
        currentStatuses.add(status);
      }
    }
    return currentStatuses;
  }

  protected PathMetadata get(final String pathStr) throws IOException {
    Path path = strToPath(pathStr);
    return ms.get(path);
  }

  protected PathMetadata getNonNull(final String pathStr) throws IOException {
    return checkNotNull(get(pathStr), "No metastore entry for %s", pathStr);
  }

  /**
   * Assert that either a path has no entry or that it is marked as deleted.
   * @param pathStr path
   * @throws IOException IO failure.
   */
  protected void assertDeleted(String pathStr) throws IOException {
    PathMetadata meta = get(pathStr);
    boolean cached = meta != null && !meta.isDeleted();
    assertFalse(pathStr + " should not be cached: " + meta, cached);
  }

  protected void assertCached(String pathStr) throws IOException {
    verifyCached(pathStr);
  }

  /**
   * Get an entry which must exist and not be deleted.
   * @param pathStr path
   * @return the entry
   * @throws IOException IO failure.
   */
  protected PathMetadata verifyCached(final String pathStr) throws IOException {
    PathMetadata meta = getNonNull(pathStr);
    assertFalse(pathStr + " was found but marked deleted: "+ meta,
        meta.isDeleted());
    return meta;
  }

  /**
   * Assert that an entry exists and is a file.
   * @param pathStr path
   * @throws IOException IO failure.
   */
  protected PathMetadata verifyIsFile(String pathStr) throws IOException {
    PathMetadata md = verifyCached(pathStr);
    assertTrue("Not a file: " + md,
        md.getFileStatus().isFile());
    return md;
  }

  /**
   * Assert that an entry exists and is a tombstone.
   * @param pathStr path
   * @throws IOException IO failure.
   */
  protected void assertIsTombstone(String pathStr) throws IOException {
    PathMetadata meta = getNonNull(pathStr);
    assertTrue(pathStr + " must be a tombstone: " + meta, meta.isDeleted());
  }

  /**
   * Assert that an entry does not exist.
   * @param pathStr path
   * @throws IOException IO failure.
   */
  protected void assertNotFound(String pathStr) throws IOException {
    PathMetadata meta = get(pathStr);
    assertNull("Unexpectedly found entry at path " + pathStr,
        meta);
  }

  /**
   * Get an entry which must be a file.
   * @param pathStr path
   * @return the entry
   * @throws IOException IO failure.
   */
  protected PathMetadata getFile(final String pathStr) throws IOException {
    PathMetadata meta = verifyCached(pathStr);
    assertFalse(pathStr + " is not a file: " + meta,
        meta.getFileStatus().isDirectory());
    return meta;
  }

  /**
   * Get an entry which must be a directory.
   * @param pathStr path
   * @return the entry
   * @throws IOException IO failure.
   */
  protected PathMetadata getDirectory(final String pathStr) throws IOException {
    PathMetadata meta = verifyCached(pathStr);
    assertTrue(pathStr + " is not a directory: " + meta,
        meta.getFileStatus().isDirectory());
    return meta;
  }

  /**
   * Get an entry which must not be marked as an empty directory:
   * its empty directory field must be FALSE or UNKNOWN.
   * @param pathStr path
   * @return the entry
   * @throws IOException IO failure.
   */
  protected PathMetadata getNonEmptyDirectory(final String pathStr)
      throws IOException {
    PathMetadata meta = getDirectory(pathStr);
    assertNotEquals("Path " + pathStr
            + " is considered an empty dir " + meta,
        Tristate.TRUE,
        meta.isEmptyDirectory());
    return meta;
  }

  /**
   * Get an entry which must be an empty directory.
   * its empty directory field must be TRUE.
   * @param pathStr path
   * @return the entry
   * @throws IOException IO failure.
   */
  protected PathMetadata getEmptyDirectory(final String pathStr)
      throws IOException {
    PathMetadata meta = getDirectory(pathStr);
    assertEquals("Path " + pathStr
            + " is not considered an empty dir " + meta,
        Tristate.TRUE,
        meta.isEmptyDirectory());
    return meta;
  }

  /**
   * Convenience to create a fully qualified Path from string.
   */
  protected Path strToPath(String p) throws IOException {
    final Path path = new Path(p);
    assertTrue("Non-absolute path: " + path,  path.isAbsolute());
    return path.makeQualified(contract.getFileSystem().getUri(), null);
  }

  protected void assertEmptyDirectory(String pathStr) throws IOException {
    assertDirectorySize(pathStr, 0);
  }

  protected void assertEmptyDirs(String...dirs) throws IOException {
    for (String pathStr : dirs) {
      assertEmptyDirectory(pathStr);
    }
  }

  protected S3AFileStatus basicFileStatus(Path path, int size, boolean isDir)
      throws IOException {
    return basicFileStatus(path, size, isDir, modTime);
  }

  public static S3AFileStatus basicFileStatus(int size, boolean isDir,
      long blockSize, long modificationTime, Path path) {
    if (isDir) {
      return new S3AFileStatus(Tristate.UNKNOWN, path, null);
    } else {
      return new S3AFileStatus(size, modificationTime, path, blockSize, null,
          null, null);
    }
  }

  public static S3AFileStatus basicFileStatus(Path path, int size,
      boolean isDir, long newModTime) throws IOException {
    if (isDir) {
      return new S3AFileStatus(Tristate.UNKNOWN, path, OWNER);
    } else {
      return new S3AFileStatus(size, newModTime, path, BLOCK_SIZE, OWNER,
          "etag", "version");
    }
  }

  protected S3AFileStatus makeFileStatus(String pathStr, int size) throws
      IOException {
    return makeFileStatus(pathStr, size, modTime);
  }

  protected S3AFileStatus makeFileStatus(String pathStr, int size,
      long newModTime) throws IOException {
    return basicFileStatus(strToPath(pathStr), size, false,
        newModTime);
  }

  protected void verifyFileStatus(FileStatus status, long size) {
    S3ATestUtils.verifyFileStatus(status, size, BLOCK_SIZE, modTime);
  }

  protected S3AFileStatus makeDirStatus(String pathStr) throws IOException {
    return basicFileStatus(strToPath(pathStr), 0, true, modTime);
  }

  /**
   * Verify the directory file status. Subclass may verify additional fields.
   */
  protected void verifyDirStatus(S3AFileStatus status) {
    assertTrue("Is a dir", status.isDirectory());
    assertEquals("zero length", 0, status.getLen());
  }

  long getModTime() {
    return modTime;
  }

  long getAccessTime() {
    return accessTime;
  }

  protected static long getTime() {
    return System.currentTimeMillis();
  }

  protected static ITtlTimeProvider getTtlTimeProvider() {
    return ttlTimeProvider;
  }

  /**
   * Put a file to the shared DDB table.
   * @param key key
   * @param time timestamp.
   * @param operationState ongoing state
   * @return the entry
   * @throws IOException IO failure
   */
  protected PathMetadata putFile(
      final String key,
      final long time,
      BulkOperationState operationState) throws IOException {
    PathMetadata meta = new PathMetadata(makeFileStatus(key, 1, time));
    meta.setLastUpdated(time);
    ms.put(meta, operationState);
    return meta;
  }

  /**
   * Put a dir to the shared DDB table.
   * @param key key
   * @param time timestamp.
   * @param operationState ongoing state
   * @return the entry
   * @throws IOException IO failure
   */
  protected PathMetadata putDir(
      final String key,
      final long time,
      BulkOperationState operationState) throws IOException {
    PathMetadata meta = new PathMetadata(
        basicFileStatus(strToPath(key), 0, true, time));
    meta.setLastUpdated(time);
    ms.put(meta, operationState);
    return meta;
  }

  /**
   * Put a tombstone to the shared DDB table.
   * @param key key
   * @param time timestamp.
   * @param operationState ongoing state
   * @return the entry
   * @throws IOException IO failure
   */
  protected PathMetadata putTombstone(
      final String key,
      final long time,
      BulkOperationState operationState) throws IOException {
    PathMetadata meta = tombstone(strToPath(key), time);
    ms.put(meta, operationState);
    return meta;
  }

  /**
   * Create a tombstone from the timestamp.
   * @param path path to tombstone
   * @param time timestamp.
   * @return the entry.
   */
  public static PathMetadata tombstone(Path path, long time) {
    S3AFileStatus s3aStatus = new S3AFileStatus(0,
        time, path, 0, null,
        null, null);
    return new PathMetadata(s3aStatus, Tristate.UNKNOWN, true);
  }

  @Override
  protected Timeout retrieveTestTimeout() {
    return Timeout.millis(S3ATestConstants.S3A_TEST_TIMEOUT);
  }

}
