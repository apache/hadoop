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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsShell;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.TrashPolicyDefault;
import org.apache.hadoop.fs.UnresolvedLinkException;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManager;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockUnderConstructionFeature;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeDescriptor;
import org.apache.hadoop.hdfs.server.datanode.*;
import org.apache.hadoop.hdfs.server.datanode.checker.DatasetVolumeChecker;
import org.apache.hadoop.hdfs.server.datanode.checker.ThrottledAsyncChecker;
import org.apache.hadoop.hdfs.server.namenode.*;
import org.apache.hadoop.hdfs.server.namenode.top.metrics.TopMetrics;
import org.apache.hadoop.http.HttpRequestLog;
import org.apache.hadoop.http.HttpServer2;
import org.apache.hadoop.ipc.ProtobufRpcEngine2.Server;
import org.apache.hadoop.metrics2.impl.MetricsSystemImpl;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.util.GSet;
import org.apache.log4j.Appender;
import org.apache.log4j.Layout;
import org.apache.log4j.LogManager;
import org.apache.log4j.PatternLayout;
import org.apache.log4j.WriterAppender;
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Helper for writing snapshot related tests
 */
public class SnapshotTestHelper {
  static final Logger LOG = LoggerFactory.getLogger(SnapshotTestHelper.class);

  /** Disable the logs that are not very useful for snapshot related tests. */
  public static void disableLogs() {
    final String[] lognames = {
        "org.eclipse.jetty",
        "org.apache.hadoop.ipc",
        "org.apache.hadoop.net",
        "org.apache.hadoop.security",

        "org.apache.hadoop.hdfs.server.blockmanagement",
        "org.apache.hadoop.hdfs.server.common.Util",
        "org.apache.hadoop.hdfs.server.datanode",
        "org.apache.hadoop.hdfs.server.namenode.FileJournalManager",
        "org.apache.hadoop.hdfs.server.namenode.NNStorageRetentionManager",
        "org.apache.hadoop.hdfs.server.namenode.FSImageFormatProtobuf",
        "org.apache.hadoop.hdfs.server.namenode.FSEditLog",
    };
    for(String n : lognames) {
      GenericTestUtils.disableLog(LoggerFactory.getLogger(n));
    }
    
    GenericTestUtils.disableLog(LoggerFactory.getLogger(
        UserGroupInformation.class));
    GenericTestUtils.disableLog(LoggerFactory.getLogger(BlockManager.class));
    GenericTestUtils.disableLog(LoggerFactory.getLogger(FSNamesystem.class));
    GenericTestUtils.disableLog(LoggerFactory.getLogger(
        DirectoryScanner.class));
    GenericTestUtils.disableLog(LoggerFactory.getLogger(
        MetricsSystemImpl.class));

    GenericTestUtils.disableLog(DatasetVolumeChecker.LOG);
    GenericTestUtils.disableLog(DatanodeDescriptor.LOG);
    GenericTestUtils.disableLog(GSet.LOG);
    GenericTestUtils.disableLog(TopMetrics.LOG);
    GenericTestUtils.disableLog(HttpRequestLog.LOG);
    GenericTestUtils.disableLog(ThrottledAsyncChecker.LOG);
    GenericTestUtils.disableLog(VolumeScanner.LOG);
    GenericTestUtils.disableLog(BlockScanner.LOG);
    GenericTestUtils.disableLog(HttpServer2.LOG);
    GenericTestUtils.disableLog(DataNode.LOG);
    GenericTestUtils.disableLog(BlockPoolSliceStorage.LOG);
    GenericTestUtils.disableLog(LeaseManager.LOG);
    GenericTestUtils.disableLog(NameNode.stateChangeLog);
    GenericTestUtils.disableLog(NameNode.blockStateChangeLog);
    GenericTestUtils.disableLog(DFSClient.LOG);
    GenericTestUtils.disableLog(Server.LOG);
  }

  static class MyCluster {
    private final MiniDFSCluster cluster;
    private final FSNamesystem fsn;
    private final FSDirectory fsdir;
    private final DistributedFileSystem hdfs;
    private final FsShell shell = new FsShell();

    private final Path snapshotDir = new Path("/");
    private final AtomicInteger snapshotCount = new AtomicInteger();
    private final AtomicInteger trashMoveCount = new AtomicInteger();
    private final AtomicInteger printTreeCount = new AtomicInteger();
    private final AtomicBoolean printTree = new AtomicBoolean();

    MyCluster(Configuration conf) throws Exception {
      cluster = new MiniDFSCluster.Builder(conf)
          .numDataNodes(1)
          .format(true)
          .build();
      fsn = cluster.getNamesystem();
      fsdir = fsn.getFSDirectory();

      cluster.waitActive();
      hdfs = cluster.getFileSystem();
      hdfs.allowSnapshot(snapshotDir);
      createSnapshot();

      shell.setConf(cluster.getConfiguration(0));
      runShell("-mkdir", "-p", ".Trash");
    }

    void setPrintTree(boolean print) {
      printTree.set(print);
    }

    boolean getPrintTree() {
      return printTree.get();
    }

    Path getTrashPath(Path p) throws Exception {
      final Path trash = hdfs.getTrashRoot(p);
      final Path resolved = hdfs.resolvePath(p);
      return new Path(trash, "Current/" + resolved.toUri().getPath());
    }

    int runShell(String... argv) {
      return shell.run(argv);
    }

    String createSnapshot()
        throws Exception {
      final String name = "s" + snapshotCount.getAndIncrement();
      SnapshotTestHelper.createSnapshot(hdfs, snapshotDir, name);
      return name;
    }

    void deleteSnapshot(String snapshotName) throws Exception {
      LOG.info("Before delete snapshot " + snapshotName);
      hdfs.deleteSnapshot(snapshotDir, snapshotName);
    }

    boolean assertExists(Path path) throws Exception {
      if (path == null) {
        return false;
      }
      if (!hdfs.exists(path)) {
        final String err = "Path not found: " + path;
        printFs(err);
        throw new AssertionError(err);
      }
      return true;
    }

    void printFs(String label) {
      final PrintStream out = System.out;
      out.println();
      out.println();
      out.println("XXX " + printTreeCount.getAndIncrement() + ": " + label);
      if (printTree.get()) {
        fsdir.getRoot().dumpTreeRecursively(out);
      }
    }

    void shutdown() {
      LOG.info("snapshotCount: {}", snapshotCount);
      cluster.shutdown();
    }

    Path mkdirs(String dir) throws Exception {
      return mkdirs(new Path(dir));
    }

    Path mkdirs(Path dir) throws Exception {
      final String label = "mkdirs " + dir;
      LOG.info(label);
      hdfs.mkdirs(dir);
      Assert.assertTrue(label, hdfs.exists(dir));
      return dir;
    }

    Path createFile(String file) throws Exception {
      return createFile(new Path(file));
    }

    Path createFile(Path file) throws Exception {
      final String label = "createFile " + file;
      LOG.info(label);
      DFSTestUtil.createFile(hdfs, file, 0, (short)1, 0L);
      Assert.assertTrue(label, hdfs.exists(file));
      return file;
    }

    String rename(Path src, Path dst) throws Exception {
      assertExists(src);
      final String snapshot = createSnapshot();

      final String label = "rename " + src + " -> " + dst;
      final boolean renamed = hdfs.rename(src, dst);
      LOG.info("{}: success? {}", label, renamed);
      Assert.assertTrue(label, renamed);
      return snapshot;
    }

    Path moveToTrash(Path path, boolean printFs) throws Exception {
      return moveToTrash(path.toString(), printFs);
    }

    Path moveToTrash(String path, boolean printFs) throws Exception {
      final Log4jRecorder recorder = Log4jRecorder.record(
          LoggerFactory.getLogger(TrashPolicyDefault.class));
      runShell("-rm", "-r", path);
      final String label = "moveToTrash-" + trashMoveCount.getAndIncrement() + " " + path;
      if (printFs) {
        printFs(label);
      } else {
        LOG.info(label);
      }
      final String recorded = recorder.getRecorded();
      LOG.info("Recorded: {}", recorded);

      final String pattern = " to trash at: ";
      final int i = recorded.indexOf(pattern);
      if (i > 0) {
        final String sub = recorded.substring(i + pattern.length());
        return new Path(sub.trim());
      }
      return null;
    }
  }

  /** Records log messages from a Log4j logger. */
  public static final class Log4jRecorder {
    static Log4jRecorder record(org.slf4j.Logger logger) {
      return new Log4jRecorder(toLog4j(logger), getLayout());
    }

    static org.apache.log4j.Logger toLog4j(org.slf4j.Logger logger) {
      return LogManager.getLogger(logger.getName());
    }

    static Layout getLayout() {
      final org.apache.log4j.Logger root
          = org.apache.log4j.Logger.getRootLogger();
      Appender a = root.getAppender("stdout");
      if (a == null) {
        a = root.getAppender("console");
      }
      return a == null? new PatternLayout() : a.getLayout();
    }

    private final StringWriter stringWriter = new StringWriter();
    private final WriterAppender appender;
    private final org.apache.log4j.Logger logger;

    private Log4jRecorder(org.apache.log4j.Logger logger, Layout layout) {
      this.appender = new WriterAppender(layout, stringWriter);
      this.logger = logger;
      this.logger.addAppender(this.appender);
    }

    public String getRecorded() {
      return stringWriter.toString();
    }

    public void stop() {
      logger.removeAppender(appender);
    }

    public void clear() {
      stringWriter.getBuffer().setLength(0);
    }
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
   * and {@link BlockUnderConstructionFeature#toString()} for detailed information.
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
        
        // skip the specific fields of BlockUnderConstructionFeature when the
        // node is an INodeFileSnapshot or INodeFileUnderConstructionSnapshot
        if (line1.contains("(INodeFileSnapshot)")
            || line1.contains("(INodeFileUnderConstructionSnapshot)")) {
          line1 = line1.replaceAll(
           "\\{blockUCState=\\w+, primaryNodeIndex=[-\\d]+, replicas=\\[\\]\\}",
           "");
          line2 = line2.replaceAll(
           "\\{blockUCState=\\w+, primaryNodeIndex=[-\\d]+, replicas=\\[\\]\\}",
           "");
        }
        assertEquals(line1.trim(), line2.trim());
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
    try {
      cluster.getNameNode().getNamesystem().getFSDirectory().getINode("/"
          ).dumpTreeRecursively(System.out);
    } catch (UnresolvedLinkException ule) {
      throw ule;
    } catch (IOException ioe) {
      throw new RuntimeException(ioe);
    }
  }
}
