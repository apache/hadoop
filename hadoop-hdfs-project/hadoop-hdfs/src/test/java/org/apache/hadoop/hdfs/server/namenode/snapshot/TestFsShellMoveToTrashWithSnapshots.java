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
package org.apache.hadoop.hdfs.server.namenode.snapshot;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

/**
 * Testing snapshots with FsShell move-to-trash feature.
 */
public class TestFsShellMoveToTrashWithSnapshots {
  static {
    SnapshotTestHelper.disableLogs();
  }

  private static final Logger LOG =
      LoggerFactory.getLogger("XXX");

  private static final String TMP = ".tmp";
  private static final String WAREHOUSE_DIR = "/warehouse/sub/";
  private static final String TO_BE_REMOVED = "TMP/";

  private static SnapshotTestHelper.MyCluster cluster;

  @Before
  public void setUp() throws Exception {
    final Configuration conf = new Configuration();
    conf.setInt(CommonConfigurationKeys.FS_TRASH_INTERVAL_KEY, 100);
    cluster = new SnapshotTestHelper.MyCluster(conf);
  }

  @After
  public void tearDown() throws Exception {
    if (cluster != null) {
      cluster.shutdown();
      cluster = null;
    }
  }

  static class MyDirs {
    private final Path base;
    private final boolean[] moved;
    private final List<Integer> renames = new ArrayList<>();

    MyDirs(Path base, int depth) {
      this.base = base;
      this.moved = new boolean[depth];

      for (int i = 0; i < depth; i++) {
        renames.add(i);
      }
      Collections.shuffle(renames);
    }

    int depth() {
      return moved.length;
    }

    DeleteSnapshotOp rename() throws Exception {
      final int i = renames.remove(renames.size() - 1);
      final String snapshot = cluster.rename(getSubPath(i + 1), getSubPath(i));
      moved[i] = true;
      return new DeleteSnapshotOp(snapshot);
    }

    Path getSubPath(int n) {
      if (n == 0) {
        return base;
      }
      final StringBuilder b = new StringBuilder();
      for (int i = 0; i < n; i++) {
        if (!moved[i]) {
          b.append(TO_BE_REMOVED);
        }
        b.append("dir").append(i).append("/");
      }
      return new Path(base, b.toString());
    }

    Path getPath() {
      return getSubPath(moved.length);
    }
  }

  static class MyFile {
    private final Path tmp;
    private Path dst;
    private Path trash;

    MyFile(String filePath) {
      this.tmp = new Path(filePath + TMP);
    }

    @Override
    public String toString() {
      return "MyFile{" +
          "tmp=" + tmp +
          ", dst=" + dst +
          ", trash=" + trash +
          '}';
    }

    synchronized Path getPath() {
      return trash != null ? trash
          : dst != null ? dst
          : tmp;
    }

    synchronized String moveFromTmp2Dst(Path dstDir) throws Exception {
      final String tmpName = tmp.getName();
      dst = new Path(dstDir, tmpName.substring(0, tmpName.length() - 4));
      final String snapshot = cluster.rename(tmp, dst);
      trash = cluster.getTrashPath(dst);
      return snapshot;
    }
  }

  MyFile createTmp(String filePath) throws Exception {
    final MyFile f = new MyFile(filePath);
    cluster.createFile(f.tmp);
    return f;
  }

  DeleteSnapshotOp moveFromTmp2Dst(MyFile file, Path dstDir) throws Exception {
    final String snapshot = file.moveFromTmp2Dst(dstDir);
    return new DeleteSnapshotOp(snapshot);
  }

  List<MyFile> runTestMoveToTrashWithShell(
      Path dbDir, Path tmpDir, int numFiles)
      throws Exception {
    return runTestMoveToTrashWithShell(dbDir, tmpDir, numFiles, 4, null);
  }

  List<MyFile> runTestMoveToTrashWithShell(
      Path dbDir, Path tmpDir, int numFiles, int depth, Integer randomSleepMaxMs)
      throws Exception {
    LOG.info("dbDir={}", dbDir);
    LOG.info("tmpDir={}", tmpDir);
    LOG.info("numFiles={}, depth={}, randomSleepMaxMs={}", numFiles, depth, randomSleepMaxMs);
    cluster.setPrintTree(numFiles < 10);

    final List<Op> ops = new ArrayList<>();
    createSnapshot(ops);

    //swap sub1 and sub2
    Path sub1 = cluster.mkdirs(new Path(dbDir, "sub1"));
    Path sub2 = cluster.mkdirs(new Path(sub1, "sub2"));

    ops.add(new DeleteSnapshotOp(cluster.rename(sub2, dbDir)));
    sub2 = new Path(dbDir, "sub2");
    ops.add(new DeleteSnapshotOp(cluster.rename(sub1, sub2)));
    sub1 = new Path(sub2, "sub1");

    final MyDirs dirs = new MyDirs(sub1, depth);
    cluster.mkdirs(dirs.getPath());
    final List<MyFile> buckets = new ArrayList<>();

    for (int i = 0; i < dirs.depth() / 2; i++) {
      ops.add(dirs.rename());
    }
    final int offset = numFiles / 4;
    for (int i = 0; i < numFiles; i++) {
      final String bucket = tmpDir + String.format("/bucket_%04d", i);
      createSnapshot(ops);
      buckets.add(createTmp(bucket));
      if (i >= offset) {
        final int j = i - offset;
        ops.add(moveFromTmp2Dst(buckets.get(j), dirs.getPath()));
      }
      if (randomSleepMaxMs != null) {
        Thread.sleep(ThreadLocalRandom.current().nextInt(randomSleepMaxMs));
      }
    }

    for (int i = dirs.depth() / 2; i < dirs.depth(); i++) {
      ops.add(dirs.rename());
    }

    ops.add(new DeleteSnapshotOp(cluster.rename(dirs.getSubPath(1), sub2)));
    ops.add(new DeleteSnapshotOp(cluster.rename(sub1, dbDir)));
    sub1 = new Path(dbDir, "sub1");
    ops.add(new DeleteSnapshotOp(cluster.rename(sub2, sub1)));
    sub2 = new Path(sub1, "sub2");
    ops.add(new DeleteSnapshotOp(cluster.rename(sub2, new Path(sub1, "sub1"))));
    ops.add(new DeleteSnapshotOp(cluster.rename(sub1, new Path(dbDir, "sub2"))));

    final MoveToTrashOp m = new MoveToTrashOp(dbDir);
    m.trashPath.thenAccept(p -> updateTrashPath(p, buckets));
    ops.add(m);

    LOG.info("ops count: {}", ops.size());
    while (!ops.isEmpty()) {
      runOneOp(ops);
    }
    cluster.printFs("END");
    return buckets;
  }

  static Path removeSubstring(Path p) {
    if (p == null) {
      return null;
    }
    return new Path(p.toUri().getPath().replace(TO_BE_REMOVED, ""));
  }

  void updateTrashPath(String trashPathPrefix, List<MyFile> files) {
    final String commonPrefix;
    final int j = trashPathPrefix.lastIndexOf('/');
    commonPrefix = trashPathPrefix.substring(0, j + 1);

    for (MyFile f : files) {
      final String original = f.trash.toUri().getPath();
      if (!original.startsWith(trashPathPrefix)) {
        Assert.assertTrue(original.startsWith(commonPrefix));

        final int i = original.indexOf('/', commonPrefix.length());
        final String suffix = original.substring(i + 1);
        f.trash = new Path(trashPathPrefix, suffix);
      }
    }
  }

  @Test(timeout = 300_000)
  public void test100tasks20files() throws Exception {
    runMultipleTasks(100, 20);
  }

  @Test(timeout = 300_000)
  public void test10tasks200files() throws Exception {
    runMultipleTasks(10, 200);
  }

  void runMultipleTasks(int numTasks, int filesPerTask) throws Exception {
    final List<Future<List<MyFile>>> futures = new ArrayList<>();
    final List<MyFile> buckets = new ArrayList<>();

    final ExecutorService executor = Executors.newFixedThreadPool(10);
    try {
      for (int i = 0; i < numTasks; i++) {
        final String db = "db" + i;
        final String tmp = "tmp" + i;
        futures.add(executor.submit(() -> {
          final Path dbDir = cluster.mkdirs(WAREHOUSE_DIR + db);
          final Path tmpDir = cluster.mkdirs(WAREHOUSE_DIR + tmp);
          return runTestMoveToTrashWithShell(dbDir, tmpDir, filesPerTask, 4, 100);
        }));
      }

      for (Future<List<MyFile>> f : futures) {
        buckets.addAll(f.get());
      }
    } finally {
      executor.shutdown();
    }
    assertExists(buckets, f -> removeSubstring(f.getPath()));
  }

  @Test(timeout = 100_000)
  public void test4files() throws Exception {
    final Path dbDir = cluster.mkdirs(WAREHOUSE_DIR + "db");
    final Path tmpDir = cluster.mkdirs(WAREHOUSE_DIR + "tmp");
    final List<MyFile> buckets = runTestMoveToTrashWithShell(
        dbDir, tmpDir, 4, 2, null);
    assertExists(buckets, f -> removeSubstring(f.getPath()));
  }

  @Test(timeout = 300_000)
  public void test200files() throws Exception {
    final Path dbDir = cluster.mkdirs(WAREHOUSE_DIR + "db");
    final Path tmpDir = cluster.mkdirs(WAREHOUSE_DIR + "tmp");
    final List<MyFile> buckets = runTestMoveToTrashWithShell(
        dbDir, tmpDir, 200);
    assertExists(buckets, f -> removeSubstring(f.getPath()));
  }

  @Test(timeout = 300_000)
  public void test50files10times() throws Exception {
    final Path tmpDir = cluster.mkdirs(WAREHOUSE_DIR + "tmp");
    final List<MyFile> buckets = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      final Path dbDir = cluster.mkdirs(WAREHOUSE_DIR + "db");
      buckets.addAll(runTestMoveToTrashWithShell(dbDir, tmpDir, 50));
    }
    cluster.setPrintTree(true);
    cluster.printFs("test_10files_10times");
    assertExists(buckets, f -> removeSubstring(f.getPath()));
  }

  static void createSnapshot(List<Op> ops) throws Exception {
    if (ThreadLocalRandom.current().nextBoolean()) {
      ops.add(new DeleteSnapshotOp(cluster.createSnapshot()));
    }
  }

  void runOneOp(List<Op> ops) throws Exception {
    Collections.shuffle(ops);

    final Op op = ops.remove(ops.size() - 1);
    if (op instanceof MoveToTrashOp) {
      createSnapshot(ops);
    }
    op.execute();
  }

  static abstract class Op {
    private final AtomicBoolean executed = new AtomicBoolean();

    final void execute() throws Exception {
      if (executed.compareAndSet(false, true)) {
        executeImpl();
      }
    }

    final boolean isExecuted() {
      return executed.get();
    }

    abstract void executeImpl() throws Exception;
  }

  static class MoveToTrashOp extends Op {
    private final Path path;
    private final CompletableFuture<String> trashPath = new CompletableFuture<>();

    MoveToTrashOp(Path path) {
      this.path = path;
    }

    @Override
    public void executeImpl() throws Exception {
      final Path p = cluster.moveToTrash(path, true);
      LOG.info("MoveToTrash: {} -> {}", path, p);
      trashPath.complete(p.toUri().getPath());
    }
  }

  static class DeleteSnapshotOp extends Op {
    private final String name;

    DeleteSnapshotOp(String name) {
      this.name = name;
    }

    @Override
    void executeImpl() throws Exception {
      cluster.deleteSnapshot(name);
    }
  }

  void assertExists(List<MyFile> files, Function<MyFile, Path> getPath)
      throws Exception {
    for (MyFile f : files) {
      final Path p = getPath.apply(f);
      final boolean exists = cluster.assertExists(p);
      if (cluster.getPrintTree()) {
        LOG.info("{} exists? {}, {}", p, exists, f);
      }
    }
  }
}