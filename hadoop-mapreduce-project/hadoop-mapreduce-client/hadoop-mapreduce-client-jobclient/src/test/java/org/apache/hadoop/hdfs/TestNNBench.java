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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.HadoopTestCase;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.util.ToolRunner;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

public class TestNNBench extends HadoopTestCase {
  private static final String BASE_DIR =
      new File(System.getProperty("test.build.data", "build/test/data"),
          "NNBench").getAbsolutePath();
  private static final String CONTROL_DIR_NAME = "control";


  public TestNNBench() throws IOException {
    super(LOCAL_MR, LOCAL_FS, 1, 1);
  }

  @AfterEach
  public void tearDown() throws Exception {
    getFileSystem().delete(new Path(BASE_DIR), true);
    getFileSystem().delete(new Path(NNBench.DEFAULT_RES_FILE_NAME), true);
    super.tearDown();
  }

  @Test
  @Timeout(value = 30)
  public void testNNBenchCreateReadAndDelete() throws Exception {
    runNNBench(createJobConf(), "create_write");
    Path path = new Path(BASE_DIR + "/data/file_0_0");
    assertTrue(getFileSystem().exists(path), "create_write should create the file");
    runNNBench(createJobConf(), "open_read");
    runNNBench(createJobConf(), "delete");
    assertFalse(getFileSystem().exists(path),
        "Delete operation should delete the file");
  }

  @Test
  @Timeout(value = 30)
  public void testNNBenchCreateAndRename() throws Exception {
    runNNBench(createJobConf(), "create_write");
    Path path = new Path(BASE_DIR + "/data/file_0_0");
    assertTrue(getFileSystem().exists(path), "create_write should create the file");
    runNNBench(createJobConf(), "rename");
    Path renamedPath = new Path(BASE_DIR + "/data/file_0_r_0");
    assertFalse(getFileSystem().exists(path), "Rename should rename the file");
    assertTrue(getFileSystem().exists(renamedPath), "Rename should rename the file");
  }

  @Test
  @Timeout(value = 30)
  public void testNNBenchCreateControlFilesWithPool() throws Exception {
    runNNBench(createJobConf(), "create_write", BASE_DIR, "5");
    Path path = new Path(BASE_DIR, CONTROL_DIR_NAME);

    FileStatus[] fileStatuses = getFileSystem().listStatus(path);
    assertEquals(5, fileStatuses.length);
  }

  @Test
  @Timeout(value = 30)
  public void testNNBenchCrossCluster() throws Exception {
    MiniDFSCluster dfsCluster = new MiniDFSCluster.Builder(new JobConf())
            .numDataNodes(1).build();
    dfsCluster.waitClusterUp();
    String nnAddress = dfsCluster.getNameNode(0).getHostAndPort();
    String baseDir = "hdfs://" + nnAddress + BASE_DIR;
    runNNBench(createJobConf(), "create_write", baseDir);

    Path path = new Path(BASE_DIR + "/data/file_0_0");
    assertTrue(dfsCluster.getFileSystem().exists(path),
        "create_write should create the file");
    dfsCluster.shutdown();
  }

  private void runNNBench(Configuration conf, String operation, String baseDir)
      throws Exception {
    String[] genArgs = {"-operation", operation, "-baseDir", baseDir,
        "-startTime", "" + (Time.now() / 1000 + 3), "-blockSize", "1024"};

    assertEquals(0, ToolRunner.run(conf, new NNBench(), genArgs));
  }

  private void runNNBench(Configuration conf, String operation, String baseDir, String numMaps)
      throws Exception {
    String[] genArgs = {"-operation", operation, "-baseDir", baseDir,
        "-startTime", "" + (Time.now() / 1000 + 3), "-blockSize", "1024", "-maps", numMaps};

    assertEquals(0, ToolRunner.run(conf, new NNBench(), genArgs));
  }

  private void runNNBench(Configuration conf, String operation)
      throws Exception {
    runNNBench(conf, operation, BASE_DIR);
  }

}
