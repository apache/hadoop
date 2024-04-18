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
package org.apache.hadoop.hdfs.tools;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.HdfsLocatedFileStatus;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsDatasetSpi;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestVerifyReadable {
  private Configuration conf = new Configuration();
  private MiniDFSCluster cluster;
  private DebugAdmin admin;
  private static final short REPLICATION = 2;
  private static final long BLOCK_SIZE = 1024;

  @Before
  public void setUp() throws Exception {
    conf.setInt(DFSConfigKeys.DFS_REPLICATION_KEY, REPLICATION);
    conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, BLOCK_SIZE);
    conf.setBoolean(DFSConfigKeys.DFS_BLOCK_ACCESS_TOKEN_ENABLE_KEY, true);
    // Faster timeout for testing
    conf.setInt(CommonConfigurationKeysPublic.IPC_CLIENT_CONNECT_MAX_RETRIES_KEY, 1);
    conf.setLong(CommonConfigurationKeysPublic.IPC_CLIENT_CONNECT_RETRY_INTERVAL_KEY, 300);
    admin = new DebugAdmin(conf);
  }

  @After
  public void tearDown() throws Exception {
    if (cluster != null) {
      cluster.shutdown();
      cluster = null;
    }
  }

  private int runDebugCommand(Path path, String input, String output, int concurrency) {
    List<String> args = new ArrayList<>();
    args.add("verifyReadable");
    if (path != null) {
      args.add("-path");
      args.add(path.toString());
    }
    if (input != null) {
      args.add("-input");
      args.add(input);
    }
    if (output != null) {
      args.add("-output");
      args.add(output);
    }
    if (concurrency > 1) {
      args.add("-concurrency");
      args.add(String.valueOf(concurrency));
    }
    return admin.run(args.toArray(new String[0]));
  }

  @Test
  public void testReadable() throws Exception {
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(3).build();
    cluster.waitActive();
    DistributedFileSystem fs = cluster.getFileSystem();

    // Successful case with various block lengths and replications
    Path testPath = new Path("/testReadable1Repl.txt");
    DFSTestUtil.createFile(fs, testPath, BLOCK_SIZE, (short) 1, 1234);
    Assert.assertEquals(0, runDebugCommand(testPath, null, null, 1));

    testPath = new Path("/testReadable3Repl.txt");
    DFSTestUtil.createFile(fs, testPath, BLOCK_SIZE, (short) 3, 1234);
    Assert.assertEquals(0, runDebugCommand(testPath, null, null, 1));

    testPath = new Path("/testReadableLong3Repl.txt");
    DFSTestUtil.createFile(fs, testPath, BLOCK_SIZE * 16, (short) 3, 1234);
    Assert.assertEquals(0, runDebugCommand(testPath, null, null, 1));

    // Simple failure cases
    // File not found
    testPath = new Path("/test404.txt");
    Assert.assertEquals(1, runDebugCommand(testPath, null, null, 1));

    // Missing replicas
    // Deleted replica
    testPath = new Path("/testMissingBlocks.txt");
    DFSTestUtil.createFile(fs, testPath, BLOCK_SIZE * 3, (short) 2, 1234);
    // Still readable with 1 replica left
    deleteReplica(fs, testPath, 1, 1);
    Assert.assertEquals(0, runDebugCommand(testPath, null, null, 1));
    // Unreadable when all replicas are gone for a block
    deleteReplica(fs, testPath, 1, 0);
    Assert.assertEquals(1, runDebugCommand(testPath, null, null, 1));

    // Down DNs
    testPath = new Path("/testMissingDNs.txt");
    DFSTestUtil.createFile(fs, testPath, BLOCK_SIZE * 3, (short) 2, 1234);
    // Still readable with 1 replica left
    shutdownDn(fs, testPath, 1, 1);
    Assert.assertEquals(0, runDebugCommand(testPath, null, null, 1));
    // Unreadable when all replicas are gone for a block
    shutdownDn(fs, testPath, 1, 0);
    Assert.assertEquals(1, runDebugCommand(testPath, null, null, 1));
  }

  @Test
  public void testReadableWithInputOutput() throws Exception {
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(3).build();
    cluster.waitActive();
    DistributedFileSystem fs = cluster.getFileSystem();

    // Run the same test in testReadable but with input and output file
    File inputPath = File.createTempFile("testReadableIn", ".txt");
    File outputPath = File.createTempFile("testReadableOut", ".txt");
    BufferedWriter inputWriter =
        new BufferedWriter(new OutputStreamWriter(Files.newOutputStream(inputPath.toPath())));

    // Successful case with various block lengths and replications
    Path testPath = new Path("/testReadable1Repl.txt");
    DFSTestUtil.createFile(fs, testPath, BLOCK_SIZE, (short) 1, 1234);
    inputWriter.write("/testReadable1Repl.txt\n");

    testPath = new Path("/testReadable3Repl.txt");
    DFSTestUtil.createFile(fs, testPath, BLOCK_SIZE, (short) 3, 1234);
    inputWriter.write("/testReadable3Repl.txt\n");

    testPath = new Path("/testReadableLong3Repl.txt");
    DFSTestUtil.createFile(fs, testPath, BLOCK_SIZE * 16, (short) 3, 1234);
    inputWriter.write("/testReadableLong3Repl.txt\n");

    // File not found
    inputWriter.write("/test404.txt\n");

    // Missing replicas
    // Deleted replica
    testPath = new Path("/testMissingBlocks.txt");
    DFSTestUtil.createFile(fs, testPath, BLOCK_SIZE * 3, (short) 2, 1234);
    deleteReplica(fs, testPath, 1, 1);
    deleteReplica(fs, testPath, 1, 0);
    inputWriter.write("/testMissingBlocks.txt\n");

    inputWriter.flush();
    inputWriter.close();
    runDebugCommand(null, inputPath.getAbsolutePath(), outputPath.getAbsolutePath(), 2);
    Map<String, Integer> results = new HashMap<>();

    BufferedReader outputReader =
        new BufferedReader(new InputStreamReader(Files.newInputStream(outputPath.toPath())));
    String line;
    while ((line = outputReader.readLine()) != null) {
      String[] split = line.split("\\s");
      results.put(split[0], Integer.parseInt(split[1]));
    }
    outputReader.close();

    Assert.assertEquals(0, results.get("/testReadable1Repl.txt").intValue());
    Assert.assertEquals(0, results.get("/testReadable3Repl.txt").intValue());
    Assert.assertEquals(0, results.get("/testReadableLong3Repl.txt").intValue());
    Assert.assertEquals(1, results.get("/test404.txt").intValue());
    Assert.assertEquals(1, results.get("/testMissingBlocks.txt").intValue());
  }

  private void deleteReplica(FileSystem fs, Path path, int blkIdx, int dnIndex) throws IOException {
    HdfsLocatedFileStatus locs = (HdfsLocatedFileStatus) fs.listFiles(path, true).next();
    String dnToInvalidate =
        locs.getLocatedBlocks().get(blkIdx).getLocations()[dnIndex].getDatanodeUuid();
    DataNode matchedDn = null;
    for (DataNode dn : cluster.getDataNodes()) {
      if (dn.getDatanodeUuid().equals(dnToInvalidate)) {
        matchedDn = dn;
        break;
      }
    }
    FsDatasetSpi fsdataset = matchedDn.getFSDataset();
    fsdataset.invalidate(cluster.getNamesystem().getBlockPoolId(),
        new Block[] {locs.getLocatedBlocks().get(blkIdx).getBlock().getLocalBlock()});
  }

  private void shutdownDn(FileSystem fs, Path path, int blkIdx, int dnIndex) throws IOException {
    HdfsLocatedFileStatus locs = (HdfsLocatedFileStatus) fs.listFiles(path, true).next();
    String dnToShutdown =
        locs.getLocatedBlocks().get(blkIdx).getLocations()[dnIndex].getDatanodeUuid();
    int idx = 0;
    for (DataNode dn : cluster.getDataNodes()) {
      if (dn.getDatanodeUuid().equals(dnToShutdown)) {
        cluster.shutdownDataNode(idx);
        break;
      }
      idx++;
    }
  }
}
