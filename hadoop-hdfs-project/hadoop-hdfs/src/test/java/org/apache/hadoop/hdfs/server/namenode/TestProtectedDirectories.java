/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hdfs.server.namenode;

import com.google.common.base.Joiner;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.hdfs.server.namenode.FSDirectory;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.FS_PROTECTED_DIRECTORIES;

/**
 * Verify that the dfs.namenode.protected.directories setting is respected.
 */
public class TestProtectedDirectories {
  static final Logger LOG = LoggerFactory.getLogger(
      TestProtectedDirectories.class);

  @Rule
  public Timeout timeout = new Timeout(300000);

  /**
   * Start a namenode-only 'cluster' which is configured to protect
   * the given list of directories.
   * @param conf
   * @param protectedDirs
   * @param unProtectedDirs
   * @return
   * @throws IOException
   */
  public MiniDFSCluster setupTestCase(Configuration conf,
                                      Collection<Path> protectedDirs,
                                      Collection<Path> unProtectedDirs)
      throws Throwable {
    // Initialize the configuration.
    conf.set(
        CommonConfigurationKeys.FS_PROTECTED_DIRECTORIES,
        Joiner.on(",").skipNulls().join(protectedDirs));

    // Start the cluster.
    MiniDFSCluster cluster =
        new MiniDFSCluster.Builder(conf).numDataNodes(0).build();

    // Create all the directories.
    try {
      cluster.waitActive();
      FileSystem fs = cluster.getFileSystem();
      for (Path path : Iterables.concat(protectedDirs, unProtectedDirs)) {
        fs.mkdirs(path);
      }
      return cluster;
    } catch (Throwable t) {
      cluster.shutdown();
      throw t;
    }
  }

  /**
   * Initialize a collection of file system layouts that will be used
   * as the test matrix.
   *
   * @return
   */
  private Collection<TestMatrixEntry> createTestMatrix() {
    Collection<TestMatrixEntry> matrix = new ArrayList<TestMatrixEntry>();

    // single empty unprotected dir.
    matrix.add(TestMatrixEntry.get()
        .addUnprotectedDir("/1", true));

    // Single empty protected dir.
    matrix.add(TestMatrixEntry.get()
        .addProtectedDir("/1", true));

    // Nested unprotected dirs.
    matrix.add(TestMatrixEntry.get()
        .addUnprotectedDir("/1", true)
        .addUnprotectedDir("/1/2", true)
        .addUnprotectedDir("/1/2/3", true)
        .addUnprotectedDir("/1/2/3/4", true));

    // Non-empty protected dir.
    matrix.add(TestMatrixEntry.get()
        .addProtectedDir("/1", false)
        .addUnprotectedDir("/1/2", true));

    // Protected empty child of unprotected parent.
    matrix.add(TestMatrixEntry.get()
        .addProtectedDir("/1/2", true)
        .addUnprotectedDir("/1/2", true));

    // Protected empty child of protected parent.
    // We should not be able to delete the parent.
    matrix.add(TestMatrixEntry.get()
        .addProtectedDir("/1", false)
        .addProtectedDir("/1/2", true));

    // One of each, non-nested.
    matrix.add(TestMatrixEntry.get()
        .addProtectedDir("/1", true)
        .addUnprotectedDir("/a", true));

    // Protected non-empty child of unprotected parent.
    // Neither should be deletable.
    matrix.add(TestMatrixEntry.get()
        .addProtectedDir("/1/2", false)
        .addUnprotectedDir("/1/2/3", true)
        .addUnprotectedDir("/1", false));

    // Protected non-empty child has unprotected siblings.
    matrix.add(TestMatrixEntry.get()
        .addProtectedDir("/1/2.2", false)
        .addUnprotectedDir("/1/2.2/3", true)
        .addUnprotectedDir("/1/2.1", true)
        .addUnprotectedDir("/1/2.3", true)
        .addUnprotectedDir("/1", false));

    // Deeply nested protected child.
    matrix.add(TestMatrixEntry.get()
        .addProtectedDir("/1/2/3/4/5", false)
        .addUnprotectedDir("/1/2/3/4/5/6", true)
        .addUnprotectedDir("/1", false)
        .addUnprotectedDir("/1/2", false)
        .addUnprotectedDir("/1/2/3", false)
        .addUnprotectedDir("/1/2/3/4", false));

    // Disjoint trees.
    matrix.add(TestMatrixEntry.get()
        .addProtectedDir("/1/2", false)
        .addProtectedDir("/a/b", false)
        .addUnprotectedDir("/1/2/3", true)
        .addUnprotectedDir("/a/b/c", true));

    // The following tests exercise special cases in the path prefix
    // checks and handling of trailing separators.

    // A disjoint non-empty protected dir has the same string prefix as the
    // directory we are trying to delete.
    matrix.add(TestMatrixEntry.get()
        .addProtectedDir("/a1", false)
        .addUnprotectedDir("/a1/a2", true)
        .addUnprotectedDir("/a", true));

    // The directory we are trying to delete has a non-empty protected
    // child and we try to delete it with a trailing separator.
    matrix.add(TestMatrixEntry.get()
        .addProtectedDir("/a/b", false)
        .addUnprotectedDir("/a/b/c", true)
        .addUnprotectedDir("/a/", false));

    // The directory we are trying to delete has an empty protected
    // child and we try to delete it with a trailing separator.
    matrix.add(TestMatrixEntry.get()
        .addProtectedDir("/a/b", true)
        .addUnprotectedDir("/a/", true));

    return matrix;
  }

  @Test
  public void testReconfigureProtectedPaths() throws Throwable {
    Configuration conf = new HdfsConfiguration();
    Collection<Path> protectedPaths = Arrays.asList(new Path("/a"), new Path(
        "/b"), new Path("/c"));
    Collection<Path> unprotectedPaths = Arrays.asList();

    MiniDFSCluster cluster = setupTestCase(conf, protectedPaths,
        unprotectedPaths);

    SortedSet<String> protectedPathsNew = new TreeSet<>(
        FSDirectory.normalizePaths(Arrays.asList("/aa", "/bb", "/cc"),
            FS_PROTECTED_DIRECTORIES));

    String protectedPathsStrNew = "/aa,/bb,/cc";

    NameNode nn = cluster.getNameNode();

    // change properties
    nn.reconfigureProperty(FS_PROTECTED_DIRECTORIES, protectedPathsStrNew);

    FSDirectory fsDirectory = nn.getNamesystem().getFSDirectory();
    // verify change
    assertEquals(String.format("%s has wrong value", FS_PROTECTED_DIRECTORIES),
        protectedPathsNew, fsDirectory.getProtectedDirectories());

    assertEquals(String.format("%s has wrong value", FS_PROTECTED_DIRECTORIES),
        protectedPathsStrNew, nn.getConf().get(FS_PROTECTED_DIRECTORIES));

    // revert to default
    nn.reconfigureProperty(FS_PROTECTED_DIRECTORIES, null);

    // verify default
    assertEquals(String.format("%s has wrong value", FS_PROTECTED_DIRECTORIES),
        new TreeSet<String>(), fsDirectory.getProtectedDirectories());

    assertEquals(String.format("%s has wrong value", FS_PROTECTED_DIRECTORIES),
        null, nn.getConf().get(FS_PROTECTED_DIRECTORIES));
  }

  @Test
  public void testAll() throws Throwable {
    for (TestMatrixEntry testMatrixEntry : createTestMatrix()) {
      Configuration conf = new HdfsConfiguration();
      MiniDFSCluster cluster = setupTestCase(
          conf, testMatrixEntry.getProtectedPaths(),
          testMatrixEntry.getUnprotectedPaths());

      try {
        LOG.info("Running {}", testMatrixEntry);
        FileSystem fs = cluster.getFileSystem();
        for (Path path : testMatrixEntry.getAllPathsToBeDeleted()) {
          final long countBefore = cluster.getNamesystem().getFilesTotal();
          assertThat(
              testMatrixEntry + ": Testing whether " + path + " can be deleted",
              deletePath(fs, path),
              is(testMatrixEntry.canPathBeDeleted(path)));
          final long countAfter = cluster.getNamesystem().getFilesTotal();

          if (!testMatrixEntry.canPathBeDeleted(path)) {
            assertThat(
                "Either all paths should be deleted or none",
                countAfter, is(countBefore));
          }
        }
      } finally {
        cluster.shutdown();
      }
    }
  }

  /**
   * Verify that configured paths are normalized by removing
   * redundant separators.
   */
  @Test
  public void testProtectedDirNormalization1() {
    Configuration conf = new HdfsConfiguration();
    conf.set(
        CommonConfigurationKeys.FS_PROTECTED_DIRECTORIES,
        "/foo//bar");
    Collection<String> paths = FSDirectory.parseProtectedDirectories(conf);
    assertThat(paths.size(), is(1));
    assertThat(paths.iterator().next(), is("/foo/bar"));
  }

  /**
   * Verify that configured paths are normalized by removing
   * trailing separators.
   */
  @Test
  public void testProtectedDirNormalization2() {
    Configuration conf = new HdfsConfiguration();
    conf.set(
        CommonConfigurationKeys.FS_PROTECTED_DIRECTORIES,
        "/a/b/,/c,/d/e/f/");
    Collection<String> paths = FSDirectory.parseProtectedDirectories(conf);

    for (String path : paths) {
      assertFalse(path.endsWith("/"));
    }
  }

  /**
   * Verify that configured paths are canonicalized.
   */
  @Test
  public void testProtectedDirIsCanonicalized() {
    Configuration conf = new HdfsConfiguration();
    conf.set(
        CommonConfigurationKeys.FS_PROTECTED_DIRECTORIES,
        "/foo/../bar/");
    Collection<String> paths = FSDirectory.parseProtectedDirectories(conf);
    assertThat(paths.size(), is(1));
    assertThat(paths.iterator().next(), is("/bar"));   
  }

  /**
   * Verify that the root directory in the configuration is correctly handled.
   */
  @Test
  public void testProtectedRootDirectory() {
    Configuration conf = new HdfsConfiguration();
    conf.set(
        CommonConfigurationKeys.FS_PROTECTED_DIRECTORIES, "/");
    Collection<String> paths = FSDirectory.parseProtectedDirectories(conf);
    assertThat(paths.size(), is(1));
    assertThat(paths.iterator().next(), is("/"));
  }

  /**
   * Verify that invalid paths in the configuration are filtered out.
   * (Path with scheme, reserved path).
   */
  @Test
  public void testBadPathsInConfig() {
    Configuration conf = new HdfsConfiguration();
    conf.set(
        CommonConfigurationKeys.FS_PROTECTED_DIRECTORIES,
        "hdfs://foo/,/.reserved/foo");
    Collection<String> paths = FSDirectory.parseProtectedDirectories(conf);
    assertThat("Unexpected directories " + paths,
        paths.size(), is(0));
  }

  /**
   * Return true if the path was successfully deleted. False if it
   * failed with AccessControlException. Any other exceptions are
   * propagated to the caller.
   *
   * @param fs
   * @param path
   * @return
   */
  private boolean deletePath(FileSystem fs, Path path) throws IOException {
    try {
      fs.delete(path, true);
      return true;
    } catch (AccessControlException ace) {
      return false;
    }
  }

  private static class TestMatrixEntry {
    // true if the path can be deleted.
    final Map<Path, Boolean> protectedPaths = Maps.newHashMap();
    final Map<Path, Boolean> unProtectedPaths = Maps.newHashMap();

    private TestMatrixEntry() {
    }

    public static TestMatrixEntry get() {
      return new TestMatrixEntry();
    }

    public Collection<Path> getProtectedPaths() {
      return protectedPaths.keySet();
    }

    public Collection<Path> getUnprotectedPaths() {
      return unProtectedPaths.keySet();
    }

    /**
     * Get all paths to be deleted in sorted order.
     * @return sorted collection of paths to be deleted.
     */
    @SuppressWarnings("unchecked") // Path implements Comparable incorrectly
    public Iterable<Path> getAllPathsToBeDeleted() {
      // Sorting ensures deletion of parents is attempted first.
      ArrayList<Path> combined = new ArrayList<>();
      combined.addAll(protectedPaths.keySet());
      combined.addAll(unProtectedPaths.keySet());
      Collections.sort(combined);
      return combined;
    }

    public boolean canPathBeDeleted(Path path) {
      return protectedPaths.containsKey(path) ?
          protectedPaths.get(path) : unProtectedPaths.get(path);
    }


    public TestMatrixEntry addProtectedDir(String dir, boolean canBeDeleted) {
      protectedPaths.put(new Path(dir), canBeDeleted);
      return this;
    }

    public TestMatrixEntry addUnprotectedDir(String dir, boolean canBeDeleted) {
      unProtectedPaths.put(new Path(dir), canBeDeleted);
      return this;
    }

    @Override
    public String toString() {
      return "TestMatrixEntry - ProtectedPaths=[" +
          Joiner.on(", ").join(protectedPaths.keySet()) +
          "]; UnprotectedPaths=[" +
          Joiner.on(", ").join(unProtectedPaths.keySet()) + "]";
    }
  }
}
