/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.tools.fedbalance;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.tools.fedbalance.DistCpProcedure.Stage;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.tools.fedbalance.procedure.BalanceJob;
import org.apache.hadoop.tools.fedbalance.procedure.BalanceProcedure.RetryException;
import org.apache.hadoop.tools.fedbalance.procedure.BalanceProcedureScheduler;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import java.io.IOException;
import java.io.OutputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.DataOutput;
import java.io.DataInputStream;
import java.io.ByteArrayInputStream;
import java.net.URI;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import static junit.framework.TestCase.assertTrue;
import static org.apache.hadoop.tools.fedbalance.FedBalanceConfigs.SCHEDULER_JOURNAL_URI;
import static org.apache.hadoop.test.GenericTestUtils.getMethodName;
import static org.apache.hadoop.test.LambdaTestUtils.intercept;
import static org.apache.hadoop.tools.fedbalance.FedBalanceConfigs.CURRENT_SNAPSHOT_NAME;
import static org.apache.hadoop.tools.fedbalance.FedBalanceConfigs.LAST_SNAPSHOT_NAME;
import static org.apache.hadoop.tools.fedbalance.FedBalanceConfigs.TrashOption;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

/**
 * Test DistCpProcedure.
 */
public class TestDistCpProcedure {
  private static MiniDFSCluster cluster;
  private static Configuration conf;
  static final String MOUNT = "mock_mount_point";
  private static final String SRCDAT = "srcdat";
  private static final String DSTDAT = "dstdat";
  private static final long BLOCK_SIZE = 1024;
  private static final long FILE_SIZE = BLOCK_SIZE * 100;
  private FileEntry[] srcfiles =
      {new FileEntry(SRCDAT, true), new FileEntry(SRCDAT + "/a", false),
          new FileEntry(SRCDAT + "/b", true),
          new FileEntry(SRCDAT + "/b/c", false)};
  private static String nnUri;

  @Rule
  // There are multiple unit tests with different timeouts that fail multiple times because of
  // DataStreamer#waitAndQueuePacket, so we set a larger global timeout.
  public Timeout globalTimeout = new Timeout(180000, TimeUnit.MILLISECONDS);

  @BeforeClass
  public static void beforeClass() throws IOException {
    DistCpProcedure.enableForTest();
    conf = new Configuration();
    conf.setLong(DFSConfigKeys.DFS_NAMENODE_MIN_BLOCK_SIZE_KEY, BLOCK_SIZE);
    conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, BLOCK_SIZE);
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(2).build();
    cluster.waitActive();

    String workPath =
        "hdfs://" + cluster.getNameNode().getHostAndPort() + "/procedure";
    conf.set(SCHEDULER_JOURNAL_URI, workPath);

    nnUri = FileSystem.getDefaultUri(conf).toString();
  }

  @AfterClass
  public static void afterClass() {
    DistCpProcedure.disableForTest();
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @Test
  public void testSuccessfulDistCpProcedure() throws Exception {
    String testRoot = nnUri + "/user/foo/testdir." + getMethodName();
    DistributedFileSystem fs =
        (DistributedFileSystem) FileSystem.get(URI.create(nnUri), conf);
    createFiles(fs, testRoot, srcfiles);

    Path src = new Path(testRoot, SRCDAT);
    Path dst = new Path(testRoot, DSTDAT);
    FsPermission originalPerm = new FsPermission(777);
    fs.setPermission(src, originalPerm);
    FedBalanceContext context = buildContext(src, dst, MOUNT);
    DistCpProcedure dcProcedure =
        new DistCpProcedure("distcp-procedure", null, 1000, context);
    BalanceProcedureScheduler scheduler = new BalanceProcedureScheduler(conf);
    scheduler.init(true);

    BalanceJob balanceJob =
        new BalanceJob.Builder<>().nextProcedure(dcProcedure).build();
    scheduler.submit(balanceJob);
    scheduler.waitUntilDone(balanceJob);
    assertTrue(balanceJob.isJobDone());
    if (balanceJob.getError() != null) {
      throw balanceJob.getError();
    }
    assertNull(balanceJob.getError());
    assertTrue(fs.exists(dst));
    assertFalse(
        fs.exists(new Path(context.getSrc(), HdfsConstants.DOT_SNAPSHOT_DIR)));
    assertFalse(
        fs.exists(new Path(context.getDst(), HdfsConstants.DOT_SNAPSHOT_DIR)));
    assertEquals(originalPerm, fs.getFileStatus(dst).getPermission());
    assertEquals(0, fs.getFileStatus(src).getPermission().toShort());
    for (FileEntry e : srcfiles) { // verify file len.
      if (!e.isDir) {
        Path targetFile = new Path(testRoot, e.path.replace(SRCDAT, DSTDAT));
        assertEquals(FILE_SIZE, fs.getFileStatus(targetFile).getLen());
      }
    }
    cleanup(fs, new Path(testRoot));
  }

  @Test
  public void testInitDistCp() throws Exception {
    String testRoot = nnUri + "/user/foo/testdir." + getMethodName();
    DistributedFileSystem fs =
        (DistributedFileSystem) FileSystem.get(URI.create(nnUri), conf);
    createFiles(fs, testRoot, srcfiles);

    Path src = new Path(testRoot, SRCDAT);
    Path dst = new Path(testRoot, DSTDAT);
    // set permission.
    fs.setPermission(src, FsPermission.createImmutable((short) 020));

    FedBalanceContext context = buildContext(src, dst, MOUNT);
    DistCpProcedure dcProcedure =
        new DistCpProcedure("distcp-procedure", null, 1000, context);

    // submit distcp.
    try {
      dcProcedure.initDistCp();
    } catch (RetryException e) {
    }
    fs.delete(new Path(src, "a"), true);
    // wait until job done.
    executeProcedure(dcProcedure, Stage.DIFF_DISTCP,
        () -> dcProcedure.initDistCp());
    assertTrue(fs.exists(dst));
    // Because we used snapshot, the file should be copied.
    assertTrue(fs.exists(new Path(dst, "a")));
    cleanup(fs, new Path(testRoot));
  }

  @Test
  public void testDiffThreshold() throws Exception {
    String testRoot = nnUri + "/user/foo/testdir." + getMethodName();
    DistributedFileSystem fs =
        (DistributedFileSystem) FileSystem.get(URI.create(nnUri), conf);
    createFiles(fs, testRoot, srcfiles);
    Path src = new Path(testRoot, SRCDAT);
    Path dst = new Path(testRoot, DSTDAT);

    FedBalanceContext context = buildContext(src, dst, MOUNT, 10);
    DistCpProcedure dcProcedure =
        new DistCpProcedure("distcp-procedure", null, 1000, context);
    executeProcedure(dcProcedure, Stage.DIFF_DISTCP,
        () -> dcProcedure.initDistCp());
    // Test distcp with diff entries number no greater than threshold.
    Path lastPath = new Path(src, "a");
    for (int i = 0; i < 5; i++) {
      Path newPath = new Path(src, "a-" + i);
      fs.rename(lastPath, newPath);
      lastPath = newPath;
      assertTrue(dcProcedure.diffDistCpStageDone());
      executeProcedure(dcProcedure, Stage.DISABLE_WRITE,
          () -> dcProcedure.diffDistCp());
    }
    cleanup(fs, new Path(testRoot));
  }

  @Test
  public void testDiffDistCp() throws Exception {
    String testRoot = nnUri + "/user/foo/testdir." + getMethodName();
    DistributedFileSystem fs =
        (DistributedFileSystem) FileSystem.get(URI.create(nnUri), conf);
    createFiles(fs, testRoot, srcfiles);
    Path src = new Path(testRoot, SRCDAT);
    Path dst = new Path(testRoot, DSTDAT);

    FedBalanceContext context = buildContext(src, dst, MOUNT);
    DistCpProcedure dcProcedure =
        new DistCpProcedure("distcp-procedure", null, 1000, context);
    executeProcedure(dcProcedure, Stage.DIFF_DISTCP,
        () -> dcProcedure.initDistCp());
    assertTrue(fs.exists(dst));

    // move file out of src and test distcp.
    fs.rename(new Path(src, "a"), new Path("/a"));
    executeProcedure(dcProcedure, Stage.FINISH,
        () -> dcProcedure.finalDistCp());
    assertFalse(fs.exists(new Path(dst, "a")));
    // move back file src/a and test distcp.
    fs.rename(new Path("/a"), new Path(src, "a"));
    executeProcedure(dcProcedure, Stage.FINISH,
        () -> dcProcedure.finalDistCp());
    assertTrue(fs.exists(new Path(dst, "a")));
    // append file src/a and test.
    OutputStream out = fs.append(new Path(src, "a"));
    out.write("hello".getBytes());
    out.close();
    long len = fs.getFileStatus(new Path(src, "a")).getLen();
    executeProcedure(dcProcedure, Stage.FINISH,
        () -> dcProcedure.finalDistCp());
    assertEquals(len, fs.getFileStatus(new Path(dst, "a")).getLen());
    cleanup(fs, new Path(testRoot));
  }

  @Test
  public void testStageFinalDistCp() throws Exception {
    String testRoot = nnUri + "/user/foo/testdir." + getMethodName();
    DistributedFileSystem fs =
        (DistributedFileSystem) FileSystem.get(URI.create(nnUri), conf);
    createFiles(fs, testRoot, srcfiles);

    Path src = new Path(testRoot, SRCDAT);
    Path dst = new Path(testRoot, DSTDAT);
    // open files.
    OutputStream out = fs.append(new Path(src, "a"));

    FedBalanceContext context = buildContext(src, dst, MOUNT);
    DistCpProcedure dcProcedure =
        new DistCpProcedure("distcp-procedure", null, 1000, context);
    executeProcedure(dcProcedure, Stage.DIFF_DISTCP,
        () -> dcProcedure.initDistCp());
    executeProcedure(dcProcedure, Stage.FINISH,
        () -> dcProcedure.finalDistCp());
    // Verify all the open files have been closed.
    intercept(RemoteException.class, "LeaseExpiredException",
        "Expect RemoteException(LeaseExpiredException).", () -> out.close());
    cleanup(fs, new Path(testRoot));
  }

  @Test
  public void testStageFinish() throws Exception {
    String testRoot = nnUri + "/user/foo/testdir." + getMethodName();
    DistributedFileSystem fs =
        (DistributedFileSystem) FileSystem.get(URI.create(nnUri), conf);
    Path src = new Path(testRoot, SRCDAT);
    Path dst = new Path(testRoot, DSTDAT);
    fs.mkdirs(src);
    fs.mkdirs(dst);
    fs.allowSnapshot(src);
    fs.allowSnapshot(dst);
    fs.createSnapshot(src, LAST_SNAPSHOT_NAME);
    fs.createSnapshot(src, CURRENT_SNAPSHOT_NAME);
    fs.createSnapshot(dst, LAST_SNAPSHOT_NAME);
    FsPermission originalPerm = new FsPermission(777);
    fs.setPermission(src, originalPerm);

    // Test the finish stage.
    FedBalanceContext context = buildContext(src, dst, MOUNT);
    DistCpProcedure dcProcedure =
        new DistCpProcedure("distcp-procedure", null, 1000, context);
    dcProcedure.disableWrite(context);
    dcProcedure.finish();

    // Verify path and permission.
    assertTrue(fs.exists(dst));
    assertFalse(fs.exists(new Path(src, HdfsConstants.DOT_SNAPSHOT_DIR)));
    assertFalse(fs.exists(new Path(dst, HdfsConstants.DOT_SNAPSHOT_DIR)));
    assertEquals(originalPerm, fs.getFileStatus(dst).getPermission());
    assertEquals(0, fs.getFileStatus(src).getPermission().toShort());
    cleanup(fs, new Path(testRoot));
  }

  @Test
  public void testRecoveryByStage() throws Exception {
    String testRoot = nnUri + "/user/foo/testdir." + getMethodName();
    DistributedFileSystem fs =
        (DistributedFileSystem) FileSystem.get(URI.create(nnUri), conf);
    createFiles(fs, testRoot, srcfiles);

    Path src = new Path(testRoot, SRCDAT);
    Path dst = new Path(testRoot, DSTDAT);

    FedBalanceContext context = buildContext(src, dst, MOUNT);
    final DistCpProcedure[] dcp = new DistCpProcedure[1];
    dcp[0] = new DistCpProcedure("distcp-procedure", null, 1000, context);

    // Doing serialization and deserialization before each stage to monitor the
    // recovery.
    dcp[0] = serializeProcedure(dcp[0]);
    executeProcedure(dcp[0], Stage.INIT_DISTCP, () -> dcp[0].preCheck());
    dcp[0] = serializeProcedure(dcp[0]);
    executeProcedure(dcp[0], Stage.DIFF_DISTCP, () -> dcp[0].initDistCp());
    fs.delete(new Path(src, "a"), true); // make some difference.
    dcp[0] = serializeProcedure(dcp[0]);
    executeProcedure(dcp[0], Stage.DISABLE_WRITE, () -> dcp[0].diffDistCp());
    dcp[0] = serializeProcedure(dcp[0]);
    executeProcedure(dcp[0], Stage.FINAL_DISTCP,
        () -> dcp[0].disableWrite(context));
    dcp[0] = serializeProcedure(dcp[0]);
    OutputStream out = fs.append(new Path(src, "b/c"));
    executeProcedure(dcp[0], Stage.FINISH, () -> dcp[0].finalDistCp());
    intercept(RemoteException.class, "LeaseExpiredException",
        "Expect RemoteException(LeaseExpiredException).", () -> out.close());
    dcp[0] = serializeProcedure(dcp[0]);
    assertTrue(dcp[0].execute());
    assertTrue(fs.exists(dst));
    assertFalse(
        fs.exists(new Path(context.getSrc(), HdfsConstants.DOT_SNAPSHOT_DIR)));
    assertFalse(
        fs.exists(new Path(context.getDst(), HdfsConstants.DOT_SNAPSHOT_DIR)));
    cleanup(fs, new Path(testRoot));
  }

  @Test
  public void testShutdown() throws Exception {
    String testRoot = nnUri + "/user/foo/testdir." + getMethodName();
    DistributedFileSystem fs =
        (DistributedFileSystem) FileSystem.get(URI.create(nnUri), conf);
    createFiles(fs, testRoot, srcfiles);

    Path src = new Path(testRoot, SRCDAT);
    Path dst = new Path(testRoot, DSTDAT);
    FedBalanceContext context = buildContext(src, dst, MOUNT);
    DistCpProcedure dcProcedure =
        new DistCpProcedure("distcp-procedure", null, 1000, context);
    BalanceProcedureScheduler scheduler = new BalanceProcedureScheduler(conf);
    scheduler.init(true);

    BalanceJob balanceJob =
        new BalanceJob.Builder<>().nextProcedure(dcProcedure).build();
    scheduler.submit(balanceJob);

    long sleep = Math.abs(new Random().nextLong()) % 10000;
    Thread.sleep(sleep);
    scheduler.shutDown();
    cleanup(fs, new Path(testRoot));
  }

  @Test
  public void testDisableWrite() throws Exception {
    String testRoot = nnUri + "/user/foo/testdir." + getMethodName();
    DistributedFileSystem fs =
        (DistributedFileSystem) FileSystem.get(URI.create(nnUri), conf);
    createFiles(fs, testRoot, srcfiles);
    Path src = new Path(testRoot, SRCDAT);
    Path dst = new Path(testRoot, DSTDAT);

    FedBalanceContext context = buildContext(src, dst, MOUNT);
    DistCpProcedure dcProcedure =
        new DistCpProcedure("distcp-procedure", null, 1000, context);
    assertNotEquals(0, fs.getFileStatus(src).getPermission().toShort());
    executeProcedure(dcProcedure, Stage.FINAL_DISTCP,
        () -> dcProcedure.disableWrite(context));
    assertEquals(0, fs.getFileStatus(src).getPermission().toShort());
    cleanup(fs, new Path(testRoot));
  }

  private FedBalanceContext buildContext(Path src, Path dst, String mount) {
    return buildContext(src, dst, mount, 0);
  }

  private FedBalanceContext buildContext(Path src, Path dst, String mount,
      int diffThreshold) {
    return new FedBalanceContext.Builder(src, dst, mount, conf).setMapNum(10)
        .setBandwidthLimit(1).setTrash(TrashOption.TRASH).setDelayDuration(1000)
        .setDiffThreshold(diffThreshold).build();
  }

  protected interface Call {
    void execute() throws IOException, RetryException;
  }

  /**
   * Execute the procedure until its stage is updated to the target stage.
   *
   * @param procedure the procedure to be executed and verified.
   * @param target the target stage.
   * @param call the function executing the procedure.
   */
  protected static void executeProcedure(DistCpProcedure procedure,
      Stage target, Call call) throws IOException {
    Stage stage = Stage.PRE_CHECK;
    procedure.updateStage(stage);
    while (stage != target) {
      try {
        call.execute();
      } catch (RetryException e) {
      } finally {
        stage = procedure.getStage();
      }
    }
  }

  static class FileEntry {
    private String path;
    private boolean isDir;

    FileEntry(String path, boolean isDir) {
      this.path = path;
      this.isDir = isDir;
    }

    String getPath() {
      return path;
    }

    boolean isDirectory() {
      return isDir;
    }
  }

  /**
   * Create directories and files with random data.
   *
   * @param fs the file system obj.
   * @param topdir the base dir of the directories and files.
   * @param entries the directory and file entries to be created.
   */
  private void createFiles(DistributedFileSystem fs, String topdir,
      FileEntry[] entries) throws IOException {
    long seed = System.currentTimeMillis();
    Random rand = new Random(seed);
    short replicationFactor = 2;
    for (FileEntry entry : entries) {
      Path newPath = new Path(topdir + "/" + entry.getPath());
      if (entry.isDirectory()) {
        fs.mkdirs(newPath);
      } else {
        int bufSize = 128;
        DFSTestUtil.createFile(fs, newPath, bufSize, FILE_SIZE, BLOCK_SIZE,
            replicationFactor, seed);
      }
      seed = System.currentTimeMillis() + rand.nextLong();
    }
  }

  private DistCpProcedure serializeProcedure(DistCpProcedure dcp)
      throws IOException {
    ByteArrayOutputStream bao = new ByteArrayOutputStream();
    DataOutput dataOut = new DataOutputStream(bao);
    dcp.write(dataOut);
    dcp = new DistCpProcedure();
    dcp.readFields(
        new DataInputStream(new ByteArrayInputStream(bao.toByteArray())));
    return dcp;
  }

  private void cleanup(DistributedFileSystem dfs, Path root)
      throws IOException {
    Path src = new Path(root, SRCDAT);
    Path dst = new Path(root, DSTDAT);
    DistCpProcedure.cleanupSnapshot(dfs, src);
    DistCpProcedure.cleanupSnapshot(dfs, dst);
    dfs.delete(root, true);
  }
}
