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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileWriter;
import java.io.PrintStream;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.util.ToolRunner;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Tests for {@link HdfsStressTest} against a {@link MiniDFSCluster}, covering
 * the end-to-end Tool run as well as the individual pre-test (cold-read corpus
 * generation), write and read building blocks.
 */
public class TestHdfsStressTest {

  private MiniDFSCluster cluster;
  private Configuration conf;

  @Before
  public void setUp() throws Exception {
    conf = new HdfsConfiguration();
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(3).build();
    cluster.waitActive();
  }

  @After
  public void tearDown() {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @Test(timeout = 120_000)
  public void testWriteAndColdReadWorkload() throws Exception {
    File props = File.createTempFile("hdfs-stress", ".properties",
        GenericTestUtils.getTestDir());
    props.deleteOnExit();
    try (FileWriter fw = new FileWriter(props)) {
      fw.write("replication=1\n");
      fw.write("blockSizeMB=1\n");
      fw.write("testWriteDirectory=/stress/write\n");
      fw.write("writeThroughputMB=8\n");
      fw.write("testReadDirectories=/stress/read\n");
      fw.write("readThroughputMB=8\n");
      // Bounded by preTestWriteDurationSeconds, so only a handful of files.
      fw.write("testReadFileSizeGB=1\n");
      fw.write("preTestWriteThroughputMB=32\n");
      fw.write("preTestWriteDurationSeconds=2\n");
      fw.write("testDurationSeconds=2\n");
    }

    int rc = ToolRunner.run(conf, new HdfsStressTest(),
        new String[] {props.getAbsolutePath()});
    assertEquals("HdfsStressTest should exit successfully", 0, rc);

    assertTrue("write directory should contain stress files",
        cluster.getFileSystem().exists(new Path("/stress/write")));
    assertTrue("read corpus should have been created",
        cluster.getFileSystem().exists(new Path("/stress/read")));
    assertTrue("at least one cold-read file should exist",
        cluster.getFileSystem().listStatus(new Path("/stress/read")).length > 0);
  }

  /**
   * The write building block must produce a file that is exactly one block
   * long, with the configured replication and block size, filled with data.
   */
  @Test(timeout = 60_000)
  public void testWriteBlockFileProducesExactBlockSizedReplicatedFile()
      throws Exception {
    HdfsStressTest tool = newConfiguredTool(
        "replication=2",
        "blockSizeMB=2");
    DistributedFileSystem dfs = cluster.getFileSystem();

    Path f = new Path("/unit/write/block-0.dat");
    tool.writeBlockFile(dfs, f);

    FileStatus st = dfs.getFileStatus(f);
    long twoMb = 2L * 1024 * 1024;
    assertEquals("file should be exactly one block long", twoMb, st.getLen());
    assertEquals("block size should match config", twoMb, st.getBlockSize());
    assertEquals("replication should match config", 2, st.getReplication());
    try (FSDataInputStream in = dfs.open(f)) {
      assertEquals("payload should be written", 'a', in.read());
    }
  }

  /**
   * The read building block must read a whole file to EOF without error, for
   * every byte of a full block.
   */
  @Test(timeout = 60_000)
  public void testReadWholeFileReadsEntireFile() throws Exception {
    HdfsStressTest tool = newConfiguredTool(
        "replication=1",
        "blockSizeMB=1");
    DistributedFileSystem dfs = cluster.getFileSystem();

    Path f = new Path("/unit/read/block-0.dat");
    tool.writeBlockFile(dfs, f);
    long oneMb = 1024L * 1024;
    assertEquals("precondition: file is one block", oneMb,
        dfs.getFileStatus(f).getLen());

    // Should consume the whole file without throwing.
    tool.readWholeFile(dfs, f);

    // Independently confirm every byte is readable and is the written payload.
    long read = 0;
    try (FSDataInputStream in = dfs.open(f)) {
      byte[] buf = new byte[64 * 1024];
      int n;
      while ((n = in.read(buf)) != -1) {
        for (int i = 0; i < n; i++) {
          assertEquals("payload byte mismatch", 'a', buf[i]);
        }
        read += n;
      }
    }
    assertEquals("read path should see the full block", oneMb, read);
  }

  /**
   * The pre-test phase must create a cold-read corpus of block-sized files,
   * round-robining across every configured read directory.
   */
  @Test(timeout = 120_000)
  public void testPreTestCreatesColdReadCorpusAcrossDirectories()
      throws Exception {
    HdfsStressTest tool = newConfiguredTool(
        "replication=1",
        "blockSizeMB=1",
        "testReadDirectories=/unit/pre/a,/unit/pre/b",
        "readThroughputMB=8",
        "testReadFileSizeGB=1",
        "preTestWriteThroughputMB=64",
        "preTestWriteDurationSeconds=3");
    DistributedFileSystem dfs = cluster.getFileSystem();

    List<Path> created = tool.preTestCreateReadFiles(dfs);

    assertFalse("pre-test should create at least one file", created.isEmpty());
    long oneMb = 1024L * 1024;
    for (Path p : created) {
      assertTrue("corpus file should be named coldread-*",
          p.getName().startsWith("coldread-"));
      assertEquals("each corpus file should be one block long", oneMb,
          dfs.getFileStatus(p).getLen());
    }

    int inA = dfs.listStatus(new Path("/unit/pre/a")).length;
    int inB = dfs.listStatus(new Path("/unit/pre/b")).length;
    assertEquals("all created files should be on disk", created.size(),
        inA + inB);
    if (created.size() >= 2) {
      assertTrue("first directory should receive files", inA > 0);
      assertTrue("second directory should receive files", inB > 0);
    }
  }

  /**
   * The pre-test phase must stop at the duration cap rather than creating the
   * full target corpus.
   */
  @Test(timeout = 60_000)
  public void testPreTestStopsAtDurationCap() throws Exception {
    // Target is 1 GB / 1 MB blocks = 1024 files, but only 1 second is allowed,
    // which a MiniDFSCluster cannot fill, so the run must be duration-bounded.
    HdfsStressTest tool = newConfiguredTool(
        "replication=1",
        "blockSizeMB=1",
        "testReadDirectories=/unit/precap",
        "readThroughputMB=8",
        "testReadFileSizeGB=1",
        "preTestWriteThroughputMB=1024",
        "preTestWriteDurationSeconds=1");
    DistributedFileSystem dfs = cluster.getFileSystem();

    // Capture stderr so we can assert the tool warns that the capped corpus is
    // smaller than requested (so its cold-read guarantee may not hold).
    PrintStream origErr = System.err;
    ByteArrayOutputStream errBuf = new ByteArrayOutputStream();
    List<Path> created;
    try {
      System.setErr(new PrintStream(errBuf, true, "UTF-8"));
      created = tool.preTestCreateReadFiles(dfs);
    } finally {
      System.setErr(origErr);
    }

    assertFalse("some files should be created before the cap", created.isEmpty());
    assertTrue("duration cap should stop well below the 1024-file target",
        created.size() < 1024);
    String err = errBuf.toString("UTF-8");
    assertTrue("a short (capped) corpus must warn about page-cache reads, was: "
        + err, err.contains("WARNING") && err.contains("smaller than requested"));
  }

  /**
   * A write-only run (no read directories) drives the write path end-to-end via
   * the Tool interface and must not create any read corpus.
   */
  @Test(timeout = 120_000)
  public void testWriteOnlyWorkloadCreatesOnlyWriteFiles() throws Exception {
    File props = writeProps(
        "replication=1",
        "blockSizeMB=1",
        "testWriteDirectory=/wo/write",
        "writeThroughputMB=8",
        "testDurationSeconds=2");

    int rc = ToolRunner.run(conf, new HdfsStressTest(),
        new String[] {props.getAbsolutePath()});
    assertEquals("write-only run should succeed", 0, rc);

    DistributedFileSystem dfs = cluster.getFileSystem();
    FileStatus[] written = dfs.listStatus(new Path("/wo/write"));
    assertTrue("write-only run should produce write files", written.length > 0);
    long oneMb = 1024L * 1024;
    for (FileStatus st : written) {
      assertTrue("write files should be named stress-write-*",
          st.getPath().getName().startsWith("stress-write-"));
      assertEquals("each write file should be one block long", oneMb,
          st.getLen());
      assertEquals("write replication should match config", 1,
          st.getReplication());
    }
    assertFalse("no read corpus should be created for a write-only run",
        dfs.exists(new Path("/wo/read")));
  }

  /**
   * A non-positive {@code preTestWriteDurationSeconds} must mean "no time cap":
   * the pre-test keeps building the corpus (previously a 0 duration set the
   * deadline to "now", created zero files, and silently disabled reads).
   */
  @Test(timeout = 60_000)
  public void testPreTestZeroDurationIsUnbounded() throws Exception {
    final HdfsStressTest tool = newConfiguredTool(
        "replication=1",
        "blockSizeMB=1",
        "testReadDirectories=/unit/unbounded",
        "readThroughputMB=8",
        "testReadFileSizeGB=1",             // 1024-file target
        "preTestWriteThroughputMB=16",      // paced so it will not finish fast
        "preTestWriteDurationSeconds=0");   // unbounded (regression under test)
    final DistributedFileSystem dfs = cluster.getFileSystem();
    final Path dir = new Path("/unit/unbounded");

    Thread worker = new Thread(() -> {
      try {
        tool.preTestCreateReadFiles(dfs);
      } catch (Exception ignored) {
        // Interrupted/aborted by the test once enough files exist.
      }
    }, "pretest-unbounded");
    worker.setDaemon(true);
    worker.start();

    // With the bug, deadline == now, so zero files are ever created and this
    // wait would time out. With the fix the corpus keeps growing.
    GenericTestUtils.waitFor(() -> {
      try {
        return dfs.exists(dir) && dfs.listStatus(dir).length >= 3;
      } catch (Exception e) {
        return false;
      }
    }, 200, 30_000);

    worker.interrupt();
    worker.join(20_000);
    assertTrue("zero-duration pre-test must keep creating files, not stop at 0",
        dfs.listStatus(dir).length >= 3);
  }

  /**
   * A failure inside a workload worker must abort the run with a non-zero
   * result instead of being swallowed and reported as success.
   */
  @Test(timeout = 60_000)
  public void testWorkerFailurePropagatesAsError() throws Exception {
    HdfsStressTest tool = new HdfsStressTest() {
      @Override
      void writeBlockFile(DistributedFileSystem dfs, Path file)
          throws java.io.IOException {
        throw new java.io.IOException("injected write failure");
      }
    };
    tool.setConf(conf);
    tool.loadConfig(writeProps(
        "replication=1",
        "blockSizeMB=1",
        "testWriteDirectory=/we/write",
        "writeThroughputMB=64",
        "writeThreads=1",
        "testDurationSeconds=5").getAbsolutePath());
    DistributedFileSystem dfs = cluster.getFileSystem();

    try {
      tool.runTestPhase(dfs, java.util.Collections.emptyList());
      org.junit.Assert.fail("runTestPhase should surface the worker failure");
    } catch (java.io.IOException e) {
      assertTrue("should report the stress workload failure but was: "
          + e.getMessage(), e.getMessage().contains("Stress workload failed"));
    }
  }

  /** Build a tool whose config fields are populated from the given lines. */
  private HdfsStressTest newConfiguredTool(String... lines) throws Exception {
    HdfsStressTest tool = new HdfsStressTest();
    tool.setConf(conf);
    tool.loadConfig(writeProps(lines).getAbsolutePath());
    return tool;
  }

  /** Write the given {@code key=value} lines to a temp properties file. */
  private File writeProps(String... lines) throws Exception {
    File props = File.createTempFile("hdfs-stress", ".properties",
        GenericTestUtils.getTestDir());
    props.deleteOnExit();
    try (FileWriter fw = new FileWriter(props)) {
      for (String line : lines) {
        fw.write(line);
        fw.write('\n');
      }
    }
    return props;
  }
}
