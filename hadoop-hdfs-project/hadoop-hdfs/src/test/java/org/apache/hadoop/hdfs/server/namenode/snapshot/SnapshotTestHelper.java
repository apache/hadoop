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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.impl.Log4JLogger;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.UnresolvedLinkException;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfoUnderConstruction;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManager;
import org.apache.hadoop.hdfs.server.datanode.BlockPoolSliceStorage;
import org.apache.hadoop.hdfs.server.datanode.DataBlockScanner;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.datanode.DirectoryScanner;
import org.apache.hadoop.hdfs.server.namenode.FSDirectory;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.INode;
import org.apache.hadoop.hdfs.server.namenode.INodeDirectory;
import org.apache.hadoop.hdfs.server.namenode.INodeFile;
import org.apache.hadoop.hdfs.server.namenode.LeaseManager;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.http.HttpServer2;
import org.apache.hadoop.ipc.ProtobufRpcEngine.Server;
import org.apache.hadoop.metrics2.impl.MetricsSystemImpl;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.log4j.Level;
import org.junit.Assert;

/**
 * Helper for writing snapshot related tests
 */
public class SnapshotTestHelper {
  public static final Log LOG = LogFactory.getLog(SnapshotTestHelper.class);

  /** Disable the logs that are not very useful for snapshot related tests. */
  public static void disableLogs() {
    final String[] lognames = {
        "org.apache.hadoop.hdfs.server.datanode.BlockPoolSliceScanner",
        "org.apache.hadoop.hdfs.server.datanode.fsdataset.impl.FsDatasetImpl",
        "org.apache.hadoop.hdfs.server.datanode.fsdataset.impl.FsDatasetAsyncDiskService",
    };
    for(String n : lognames) {
      setLevel2OFF(LogFactory.getLog(n));
    }
    
    setLevel2OFF(LogFactory.getLog(UserGroupInformation.class));
    setLevel2OFF(LogFactory.getLog(BlockManager.class));
    setLevel2OFF(LogFactory.getLog(FSNamesystem.class));
    setLevel2OFF(LogFactory.getLog(DirectoryScanner.class));
    setLevel2OFF(LogFactory.getLog(MetricsSystemImpl.class));
    
    setLevel2OFF(DataBlockScanner.LOG);
    setLevel2OFF(HttpServer2.LOG);
    setLevel2OFF(DataNode.LOG);
    setLevel2OFF(BlockPoolSliceStorage.LOG);
    setLevel2OFF(LeaseManager.LOG);
    setLevel2OFF(NameNode.stateChangeLog);
    setLevel2OFF(NameNode.blockStateChangeLog);
    setLevel2OFF(DFSClient.LOG);
    setLevel2OFF(Server.LOG);
  }

  static void setLevel2OFF(Object log) {
    ((Log4JLogger)log).getLogger().setLevel(Level.OFF);
  }

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
   * @param snapshotRoot The dir to be snapshotted
   * @param snapshotName The name of the snapshot
   * @return The path of the snapshot root
   */
  public static Path createSnapshot(DistributedFileSystem hdfs,
      Path snapshotRoot, String snapshotName) throws Exception {
    LOG.info("createSnapshot " + snapshotName + " for " + snapshotRoot);
    assertTrue(hdfs.exists(snapshotRoot));
    hdfs.allowSnapshot(snapshotRoot);
    hdfs.createSnapshot(snapshotRoot, snapshotName);
    // set quota to a large value for testing counts
    hdfs.setQuota(snapshotRoot, Long.MAX_VALUE-1, Long.MAX_VALUE-1);
    return SnapshotTestHelper.getSnapshotRoot(snapshotRoot, snapshotName);
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
    assertEquals("snapshottedDir=" + snapshottedDir
        + ", snapshotRoot=" + snapshotRoot,
        currentFiles.length, snapshotFiles.length);
  }
  
  /**
   * Compare two dumped trees that are stored in two files. The following is an
   * example of the dumped tree:
   * 
   * <pre>
   * information of root
   * +- the first child of root (e.g., /foo)
   *   +- the first child of /foo
   *   ...
   *   \- the last child of /foo (e.g., /foo/bar)
   *     +- the first child of /foo/bar
   *     ...
   *   snapshots of /foo
   *   +- snapshot s_1
   *   ...
   *   \- snapshot s_n
   * +- second child of root
   *   ...
   * \- last child of root
   * 
   * The following information is dumped for each inode:
   * localName (className@hashCode) parent permission group user
   * 
   * Specific information for different types of INode: 
   * {@link INodeDirectory}:childrenSize 
   * {@link INodeFile}: fileSize, block list. Check {@link BlockInfo#toString()} 
   * and {@link BlockInfoUnderConstruction#toString()} for detailed information.
   * {@link FileWithSnapshot}: next link
   * </pre>
   * @see INode#dumpTreeRecursively()
   */
  public static void compareDumpedTreeInFile(File file1, File file2,
      boolean compareQuota) throws IOException {
    try {
      compareDumpedTreeInFile(file1, file2, compareQuota, false);
    } catch(Throwable t) {
      LOG.info("FAILED compareDumpedTreeInFile(" + file1 + ", " + file2 + ")", t);
      compareDumpedTreeInFile(file1, file2, compareQuota, true);
    }
  }

  private static void compareDumpedTreeInFile(File file1, File file2,
      boolean compareQuota, boolean print) throws IOException {
    if (print) {
      printFile(file1);
      printFile(file2);
    }

    BufferedReader reader1 = new BufferedReader(new FileReader(file1));
    BufferedReader reader2 = new BufferedReader(new FileReader(file2));
    try {
      String line1 = "";
      String line2 = "";
      while ((line1 = reader1.readLine()) != null
          && (line2 = reader2.readLine()) != null) {
        if (print) {
          System.out.println();
          System.out.println("1) " + line1);
          System.out.println("2) " + line2);
        }
        // skip the hashCode part of the object string during the comparison,
        // also ignore the difference between INodeFile/INodeFileWithSnapshot
        line1 = line1.replaceAll("INodeFileWithSnapshot", "INodeFile");
        line2 = line2.replaceAll("INodeFileWithSnapshot", "INodeFile");
        line1 = line1.replaceAll("@[\\dabcdef]+", "");
        line2 = line2.replaceAll("@[\\dabcdef]+", "");
        
        // skip the replica field of the last block of an
        // INodeFileUnderConstruction
        line1 = line1.replaceAll("replicas=\\[.*\\]", "replicas=[]");
        line2 = line2.replaceAll("replicas=\\[.*\\]", "replicas=[]");
        
        if (!compareQuota) {
          line1 = line1.replaceAll("Quota\\[.*\\]", "Quota[]");
          line2 = line2.replaceAll("Quota\\[.*\\]", "Quota[]");
        }
        
        // skip the specific fields of BlockInfoUnderConstruction when the node
        // is an INodeFileSnapshot or an INodeFileUnderConstructionSnapshot
        if (line1.contains("(INodeFileSnapshot)")
            || line1.contains("(INodeFileUnderConstructionSnapshot)")) {
          line1 = line1.replaceAll(
           "\\{blockUCState=\\w+, primaryNodeIndex=[-\\d]+, replicas=\\[\\]\\}",
           "");
          line2 = line2.replaceAll(
           "\\{blockUCState=\\w+, primaryNodeIndex=[-\\d]+, replicas=\\[\\]\\}",
           "");
        }
        
        assertEquals(line1, line2);
      }
      Assert.assertNull(reader1.readLine());
      Assert.assertNull(reader2.readLine());
    } finally {
      reader1.close();
      reader2.close();
    }
  }

  static void printFile(File f) throws IOException {
    System.out.println();
    System.out.println("File: " + f);
    BufferedReader in = new BufferedReader(new FileReader(f));
    try {
      for(String line; (line = in.readLine()) != null; ) {
        System.out.println(line);
      }
    } finally {
      in.close();
    }
  }

  public static void dumpTree2File(FSDirectory fsdir, File f) throws IOException{
    final PrintWriter out = new PrintWriter(new FileWriter(f, false), true);
    fsdir.getINode("/").dumpTreeRecursively(out, new StringBuilder(),
        Snapshot.CURRENT_STATE_ID);
    out.close();
  }

  /**
   * Generate the path for a snapshot file.
   * 
   * @param snapshotRoot of format
   *          {@literal <snapshottble_dir>/.snapshot/<snapshot_name>}
   * @param file path to a file
   * @return The path of the snapshot of the file assuming the file has a
   *         snapshot under the snapshot root of format
   *         {@literal <snapshottble_dir>/.snapshot/<snapshot_name>/<path_to_file_inside_snapshot>}
   *         . Null if the file is not under the directory associated with the
   *         snapshot root.
   */
  static Path getSnapshotFile(Path snapshotRoot, Path file) {
    Path rootParent = snapshotRoot.getParent();
    if (rootParent != null && rootParent.getName().equals(".snapshot")) {
      Path snapshotDir = rootParent.getParent();
      if (file.toString().contains(snapshotDir.toString())
          && !file.equals(snapshotDir)) {
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
    /** Top node of the directory tree */
    final Node topNode;
    /** A map recording nodes for each tree level */
    final Map<Integer, ArrayList<Node>> levelMap;

    /**
     * Constructor to build a tree of given {@code height}
     */
    TestDirectoryTree(int height, FileSystem fs) throws Exception {
      this.height = height;
      this.topNode = new Node(new Path("/TestSnapshot"), 0,
          null, fs);
      this.levelMap = new HashMap<Integer, ArrayList<Node>>();
      addDirNode(topNode, 0);
      genChildren(topNode, height - 1, fs);
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
     * @param fs The FileSystem where to generate the files/dirs
     * @throws Exception
     */
    private void genChildren(Node parent, int level, FileSystem fs)
        throws Exception {
      if (level == 0) {
        return;
      }
      parent.leftChild = new Node(new Path(parent.nodePath,
          "left" + ++id), height - level, parent, fs);
      parent.rightChild = new Node(new Path(parent.nodePath,
          "right" + ++id), height - level, parent, fs);
      addDirNode(parent.leftChild, parent.leftChild.level);
      addDirNode(parent.rightChild, parent.rightChild.level);
      genChildren(parent.leftChild, level - 1, fs);
      genChildren(parent.rightChild, level - 1, fs);
    }

    /**
     * Randomly retrieve a node from the directory tree.
     * 
     * @param random A random instance passed by user.
     * @param excludedList Excluded list, i.e., the randomly generated node
     *          cannot be one of the nodes in this list.
     * @return a random node from the tree.
     */
    Node getRandomDirNode(Random random, List<Node> excludedList) {
      while (true) {
        int level = random.nextInt(height);
        ArrayList<Node> levelList = levelMap.get(level);
        int index = random.nextInt(levelList.size());
        Node randomNode = levelList.get(index);
        if (excludedList == null || !excludedList.contains(randomNode)) {
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

      Node(Path path, int level, Node parent,
          FileSystem fs) throws Exception {
        this.nodePath = path;
        this.level = level;
        this.parent = parent;
        this.nonSnapshotChildren = new ArrayList<Node>();
        fs.mkdirs(nodePath);
      }

      /**
       * Create files and add them in the fileList. Initially the last element
       * in the fileList is set to null (where we start file creation).
       */
      void initFileList(FileSystem fs, String namePrefix, long fileLen,
          short replication, long seed, int numFiles) throws Exception {
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
  
  public static void dumpTree(String message, MiniDFSCluster cluster
      ) throws UnresolvedLinkException {
    System.out.println("XXX " + message);
    cluster.getNameNode().getNamesystem().getFSDirectory().getINode("/"
        ).dumpTreeRecursively(System.out);
  }
}
