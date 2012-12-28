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
package org.apache.hadoop.hdfs.server.namenode.snapshot;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.server.namenode.INode;

/**
 * Helper for writing snapshot related tests
 */
public class SnapshotTestHelper {
  public static final Log LOG = LogFactory.getLog(SnapshotTestHelper.class);

  private SnapshotTestHelper() {
    // Cannot be instantinatied
  }

  public static Path getSnapshotRoot(Path snapshottedDir, String snapshotName) {
    return new Path(snapshottedDir, HdfsConstants.DOT_SNAPSHOT_DIR + "/"
        + snapshotName);
  }

  public static Path getSnapshotPath(Path snapshottedDir, String snapshotName,
      String fileLocalName) {
    return new Path(getSnapshotRoot(snapshottedDir, snapshotName),
        fileLocalName);
  }

  /**
   * Create snapshot for a dir using a given snapshot name
   * 
   * @param hdfs DistributedFileSystem instance
   * @param snapshottedDir The dir to be snapshotted
   * @param snapshotName The name of the snapshot
   * @return The path of the snapshot root
   */
  public static Path createSnapshot(DistributedFileSystem hdfs,
      Path snapshottedDir, String snapshotName) throws Exception {
    assertTrue(hdfs.exists(snapshottedDir));
    hdfs.allowSnapshot(snapshottedDir.toString());
    hdfs.createSnapshot(snapshotName, snapshottedDir.toString());
    return SnapshotTestHelper.getSnapshotRoot(snapshottedDir, snapshotName);
  }

  /**
   * Check the functionality of a snapshot.
   * 
   * @param hdfs DistributedFileSystem instance
   * @param snapshotRoot The root of the snapshot
   * @param snapshottedDir The snapshotted directory
   */
  public static void checkSnapshotCreation(DistributedFileSystem hdfs,
      Path snapshotRoot, Path snapshottedDir) throws Exception {
    // Currently we only check if the snapshot was created successfully
    assertTrue(hdfs.exists(snapshotRoot));
    // Compare the snapshot with the current dir
    FileStatus[] currentFiles = hdfs.listStatus(snapshottedDir);
    FileStatus[] snapshotFiles = hdfs.listStatus(snapshotRoot);
    assertEquals(currentFiles.length, snapshotFiles.length);
  }

  /**
   * Generate the path for a snapshot file.
   * 
   * @param fs FileSystem instance
   * @param snapshotRoot of format
   *          {@literal <snapshottble_dir>/.snapshot/<snapshot_name>}
   * @param file path to a file
   * @return The path of the snapshot of the file assuming the file has a
   *         snapshot under the snapshot root of format
   *         {@literal <snapshottble_dir>/.snapshot/<snapshot_name>/<path_to_file_inside_snapshot>}
   *         . Null if the file is not under the directory associated with the
   *         snapshot root.
   */
  static Path getSnapshotFile(FileSystem fs, Path snapshotRoot, Path file) {
    Path rootParent = snapshotRoot.getParent();
    if (rootParent != null && rootParent.getName().equals(".snapshot")) {
      Path snapshotDir = rootParent.getParent();
      if (file.toString().contains(snapshotDir.toString())) {
        String fileName = file.toString().substring(
            snapshotDir.toString().length() + 1);
        Path snapshotFile = new Path(snapshotRoot, fileName);
        return snapshotFile;
      }
    }
    return null;
  }

  /**
   * A class creating directories trees for snapshot testing. For simplicity,
   * the directory tree is a binary tree, i.e., each directory has two children
   * as snapshottable directories.
   */
  static class TestDirectoryTree {
    /** Height of the directory tree */
    final int height;
    final FileSystem fs;
    /** Top node of the directory tree */
    final Node topNode;
    /** A map recording nodes for each tree level */
    final Map<Integer, ArrayList<Node>> levelMap;

    /**
     * Constructor to build a tree of given {@code height}
     */
    TestDirectoryTree(int height, FileSystem fs) throws Exception {
      this.height = height;
      this.fs = fs;
      this.topNode = new Node(new Path("/TestSnapshot"), 0,
          null, fs);
      this.levelMap = new HashMap<Integer, ArrayList<Node>>();
      addDirNode(topNode, 0);
      genChildren(topNode, height - 1);
    }

    /**
     * Add a node into the levelMap
     */
    private void addDirNode(Node node, int atLevel) {
      ArrayList<Node> list = levelMap.get(atLevel);
      if (list == null) {
        list = new ArrayList<Node>();
        levelMap.put(atLevel, list);
      }
      list.add(node);
    }

    int id = 0;
    /**
     * Recursively generate the tree based on the height.
     * 
     * @param parent The parent node
     * @param level The remaining levels to generate
     * @throws Exception
     */
    void genChildren(Node parent, int level) throws Exception {
      if (level == 0) {
        return;
      }
      parent.leftChild = new Node(new Path(parent.nodePath,
          "left" + ++id), height - level, parent, fs);
      parent.rightChild = new Node(new Path(parent.nodePath,
          "right" + ++id), height - level, parent, fs);
      addDirNode(parent.leftChild, parent.leftChild.level);
      addDirNode(parent.rightChild, parent.rightChild.level);
      genChildren(parent.leftChild, level - 1);
      genChildren(parent.rightChild, level - 1);
    }

    /**
     * Randomly retrieve a node from the directory tree.
     * 
     * @param random A random instance passed by user.
     * @param excludedList Excluded list, i.e., the randomly generated node
     *          cannot be one of the nodes in this list.
     * @return a random node from the tree.
     */
    Node getRandomDirNode(Random random,
        ArrayList<Node> excludedList) {
      while (true) {
        int level = random.nextInt(height);
        ArrayList<Node> levelList = levelMap.get(level);
        int index = random.nextInt(levelList.size());
        Node randomNode = levelList.get(index);
        if (!excludedList.contains(randomNode)) {
          return randomNode;
        }
      }
    }

    /**
     * The class representing a node in {@link TestDirectoryTree}.
     * <br>
     * This contains:
     * <ul>
     * <li>Two children representing the two snapshottable directories</li>
     * <li>A list of files for testing, so that we can check snapshots
     * after file creation/deletion/modification.</li>
     * <li>A list of non-snapshottable directories, to test snapshots with
     * directory creation/deletion. Note that this is needed because the
     * deletion of a snapshottale directory with snapshots is not allowed.</li>
     * </ul>
     */
    static class Node {
      /** The level of this node in the directory tree */
      final int level;

      /** Children */
      Node leftChild;
      Node rightChild;

      /** Parent node of the node */
      final Node parent;

      /** File path of the node */
      final Path nodePath;

      /**
       * The file path list for testing snapshots before/after file
       * creation/deletion/modification
       */
      ArrayList<Path> fileList;

      /**
       * Each time for testing snapshots with file creation, since we do not
       * want to insert new files into the fileList, we always create the file
       * that was deleted last time. Thus we record the index for deleted file
       * in the fileList, and roll the file modification forward in the list.
       */
      int nullFileIndex = 0;

      /**
       * A list of non-snapshottable directories for testing snapshots with
       * directory creation/deletion
       */
      final ArrayList<Node> nonSnapshotChildren;
      final FileSystem fs;

      Node(Path path, int level, Node parent,
          FileSystem fs) throws Exception {
        this.nodePath = path;
        this.level = level;
        this.parent = parent;
        this.nonSnapshotChildren = new ArrayList<Node>();
        this.fs = fs;
        fs.mkdirs(nodePath);
      }

      /**
       * Create files and add them in the fileList. Initially the last element
       * in the fileList is set to null (where we start file creation).
       */
      void initFileList(String namePrefix, long fileLen, short replication, long seed, int numFiles)
          throws Exception {
        fileList = new ArrayList<Path>(numFiles);
        for (int i = 0; i < numFiles; i++) {
          Path file = new Path(nodePath, namePrefix + "-f" + i);
          fileList.add(file);
          if (i < numFiles - 1) {
            DFSTestUtil.createFile(fs, file, fileLen, replication, seed);
          }
        }
        nullFileIndex = numFiles - 1;
      }

      @Override
      public boolean equals(Object o) {
        if (o != null && o instanceof Node) {
          Node node = (Node) o;
          return node.nodePath.equals(nodePath);
        }
        return false;
      }

      @Override
      public int hashCode() {
        return nodePath.hashCode();
      }
    }
  }

  static void dumpTreeRecursively(INode inode) {
    if (INode.LOG.isDebugEnabled()) {
      inode.dumpTreeRecursively(
          new PrintWriter(System.out, true), new StringBuilder(), null);
    }
  }
}
