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

import org.apache.hadoop.thirdparty.com.google.common.base.Joiner;
import org.apache.hadoop.thirdparty.com.google.common.collect.Iterables;
import org.apache.hadoop.thirdparty.com.google.common.collect.Maps;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.Trash;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.hdfs.server.namenode.FSDirectory;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.*;

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_PROTECTED_SUBDIRECTORIES_ENABLE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.FS_PROTECTED_DIRECTORIES;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verify that the dfs.namenode.protected.directories setting is respected.
 */
@Timeout(300)
public class TestProtectedDirectories {
  static final Logger LOG = LoggerFactory.getLogger(
      TestProtectedDirectories.class);

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

  private Collection<TestMatrixEntry> createTestMatrixForProtectSubDirs() {
    Collection<TestMatrixEntry> matrix = new ArrayList<TestMatrixEntry>();

    // Nested unprotected dirs.
    matrix.add(TestMatrixEntry.get()
            .addUnprotectedDir("/1", true)
            .addUnprotectedDir("/1/2", true)
            .addUnprotectedDir("/1/2/3", true)
            .addUnprotectedDir("/1/2/3/4", true));

    // Non-empty protected dir.
    matrix.add(TestMatrixEntry.get()
            .addProtectedDir("/1", false)
            .addUnprotectedDir("/1/2", false)
            .addUnprotectedDir("/1/2/3", false)
            .addUnprotectedDir("/1/2/3/4", true));
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
    assertEquals(protectedPathsNew, fsDirectory.getProtectedDirectories(),
        String.format("%s has wrong value", FS_PROTECTED_DIRECTORIES));

    assertEquals(protectedPathsStrNew, nn.getConf().get(FS_PROTECTED_DIRECTORIES),
        String.format("%s has wrong value", FS_PROTECTED_DIRECTORIES));

    // revert to default
    nn.reconfigureProperty(FS_PROTECTED_DIRECTORIES, null);

    // verify default
    assertEquals(new TreeSet<String>(), fsDirectory.getProtectedDirectories(),
        String.format("%s has wrong value", FS_PROTECTED_DIRECTORIES));

    assertEquals(null, nn.getConf().get(FS_PROTECTED_DIRECTORIES),
        String.format("%s has wrong value", FS_PROTECTED_DIRECTORIES));
  }

  @Test
  public void testDelete() throws Throwable {
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
          assertThat(deletePath(fs, path))
              .as(testMatrixEntry + ": Testing whether " + path + " can be deleted")
              .isEqualTo(testMatrixEntry.canPathBeDeleted(path));
          final long countAfter = cluster.getNamesystem().getFilesTotal();

          if (!testMatrixEntry.canPathBeDeleted(path)) {
            assertThat(countAfter)
                .as("Either all paths should be deleted or none")
                .isEqualTo(countBefore);
          }
        }
      } finally {
        cluster.shutdown();
      }
    }
  }

  @Test
  public void testMoveToTrash() throws Throwable {
    for (TestMatrixEntry testMatrixEntry : createTestMatrix()) {
      Configuration conf = new HdfsConfiguration();
      conf.setInt(DFSConfigKeys.FS_TRASH_INTERVAL_KEY, 3600);
      MiniDFSCluster cluster = setupTestCase(
          conf, testMatrixEntry.getProtectedPaths(),
          testMatrixEntry.getUnprotectedPaths());

      try {
        LOG.info("Running {}", testMatrixEntry);
        FileSystem fs = cluster.getFileSystem();
        for (Path path : testMatrixEntry.getAllPathsToBeDeleted()) {
          assertThat(
              moveToTrash(fs, path, conf))
              .as(testMatrixEntry + ": Testing whether " + path +
                  " can be moved to trash")
              .isEqualTo(testMatrixEntry.canPathBeDeleted(path));
        }
      } finally {
        cluster.shutdown();
      }
    }
  }

  /*
   * Verify that protected directories could not be renamed.
   */
  @Test
  public void testRename() throws Throwable {
    for (TestMatrixEntry testMatrixEntry : createTestMatrix()) {
      Configuration conf = new HdfsConfiguration();
      MiniDFSCluster cluster = setupTestCase(
          conf, testMatrixEntry.getProtectedPaths(),
          testMatrixEntry.getUnprotectedPaths());

      try {
        LOG.info("Running {}", testMatrixEntry);
        FileSystem fs = cluster.getFileSystem();
        for (Path srcPath : testMatrixEntry.getAllPathsToBeDeleted()) {
          assertThat(
              renamePath(fs, srcPath,
                  new Path(srcPath.toString() + "_renamed")))
              .as(testMatrixEntry + ": Testing whether "
                  + srcPath + " can be renamed")
              .isEqualTo(testMatrixEntry.canPathBeRenamed(srcPath));
        }
      } finally {
        cluster.shutdown();
      }
    }
  }

  @Test
  public void testRenameProtectSubDirs() throws Throwable {
    for (TestMatrixEntry testMatrixEntry :
            createTestMatrixForProtectSubDirs()) {
      Configuration conf = new HdfsConfiguration();
      conf.setBoolean(DFS_PROTECTED_SUBDIRECTORIES_ENABLE, true);
      MiniDFSCluster cluster = setupTestCase(
              conf, testMatrixEntry.getProtectedPaths(),
              testMatrixEntry.getUnprotectedPaths());

      try {
        LOG.info("Running {}", testMatrixEntry);
        FileSystem fs = cluster.getFileSystem();
        for (Path srcPath : testMatrixEntry.getAllPathsToBeDeleted()) {
          assertThat(
              renamePath(fs, srcPath,
                  new Path(srcPath.toString() + "_renamed")))
              .as(testMatrixEntry + ": Testing whether "
                  + srcPath + " can be renamed")
              .isEqualTo(testMatrixEntry.canPathBeRenamed(srcPath));
        }
      } finally {
        cluster.shutdown();
      }
    }
  }

  @Test
  public void testMoveProtectedSubDirsToTrash() throws Throwable {
    for (TestMatrixEntry testMatrixEntry :
        createTestMatrixForProtectSubDirs()) {
      Configuration conf = new HdfsConfiguration();
      conf.setBoolean(DFS_PROTECTED_SUBDIRECTORIES_ENABLE, true);
      conf.setInt(DFSConfigKeys.FS_TRASH_INTERVAL_KEY, 3600);
      MiniDFSCluster cluster = setupTestCase(
          conf, testMatrixEntry.getProtectedPaths(),
          testMatrixEntry.getUnprotectedPaths());

      try {
        LOG.info("Running {}", testMatrixEntry);
        FileSystem fs = cluster.getFileSystem();
        for (Path srcPath : testMatrixEntry.getAllPathsToBeDeleted()) {
          assertThat(
              moveToTrash(fs, srcPath, conf))
              .as(testMatrixEntry + ": Testing whether "
                  + srcPath + " can be moved to trash")
              .isEqualTo(moveToTrash(fs, srcPath, conf));
        }
      } finally {
        cluster.shutdown();
      }
    }
  }

  @Test
  public void testDeleteProtectSubDirs() throws Throwable {
    for (TestMatrixEntry testMatrixEntry :
            createTestMatrixForProtectSubDirs()) {
      Configuration conf = new HdfsConfiguration();
      conf.setBoolean(DFS_PROTECTED_SUBDIRECTORIES_ENABLE, true);
      MiniDFSCluster cluster = setupTestCase(
              conf, testMatrixEntry.getProtectedPaths(),
              testMatrixEntry.getUnprotectedPaths());

      try {
        LOG.info("Running {}", testMatrixEntry);
        FileSystem fs = cluster.getFileSystem();
        for (Path path : testMatrixEntry.getAllPathsToBeDeleted()) {
          final long countBefore = cluster.getNamesystem().getFilesTotal();
          assertThat(deletePath(fs, path))
              .as(testMatrixEntry + ": Testing whether "
                  + path + " can be deleted")
              .isEqualTo(testMatrixEntry.canPathBeDeleted(path));
          final long countAfter = cluster.getNamesystem().getFilesTotal();

          if (!testMatrixEntry.canPathBeDeleted(path)) {
            assertThat(countAfter)
                .as("Either all paths should be deleted or none")
                .isEqualTo(countBefore);
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
    assertThat(paths.size()).isEqualTo(1);
    assertThat(paths.iterator().next()).isEqualTo("/foo/bar");
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
    assertThat(paths.size()).isEqualTo(1);
    assertThat(paths.iterator().next()).isEqualTo("/bar");
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
    assertThat(paths.size()).isEqualTo(1);
    assertThat(paths.iterator().next()).isEqualTo("/");
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
    assertThat(paths.size())
        .as("Unexpected directories " + paths)
        .isEqualTo(0);
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

  private boolean moveToTrash(FileSystem fs, Path path, Configuration conf) {
    try {
      return Trash.moveToAppropriateTrash(fs, path, conf);
    } catch (FileNotFoundException fnf) {
      // fs.delete(...) does not throw an exception if the file does not exist.
      // The deletePath method in this class, will therefore return true if
      // there is an attempt to delete a file which does not exist. Therefore
      // catching this exception and returning true to keep it consistent and
      // allow tests to work with the same test matrix.
      return true;
    } catch (IOException ace) {
      return false;
    }
  }

  /**
   * Return true if the path was successfully renamed. False if it
   * failed with AccessControlException. Any other exceptions are
   * propagated to the caller.
   *
   * @param fs
   * @param srcPath
   * @param dstPath
   * @return
   */
  private boolean renamePath(FileSystem fs, Path srcPath, Path dstPath)
      throws IOException {
    try {
      fs.rename(srcPath, dstPath);
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

    public boolean canPathBeRenamed(Path path) {
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
