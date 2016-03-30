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

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.HadoopTestCase;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.util.ToolRunner;
import org.junit.After;
import org.junit.Test;

public class TestNNBench extends HadoopTestCase {
  private static final String BASE_DIR =
      new File(System.getProperty("test.build.data", "build/test/data"),
          "NNBench").getAbsolutePath();

  public TestNNBench() throws IOException {
    super(LOCAL_MR, LOCAL_FS, 1, 1);
  }

  @After
  public void tearDown() throws Exception {
    getFileSystem().delete(new Path(BASE_DIR), true);
    getFileSystem().delete(new Path(NNBench.DEFAULT_RES_FILE_NAME), true);
    super.tearDown();
  }

  @Test(timeout = 30000)
  public void testNNBenchCreateReadAndDelete() throws Exception {
    runNNBench(createJobConf(), "create_write");
    Path path = new Path(BASE_DIR + "/data/file_0_0");
    assertTrue("create_write should create the file",
        getFileSystem().exists(path));
    runNNBench(createJobConf(), "open_read");
    runNNBench(createJobConf(), "delete");
    assertFalse("Delete operation should delete the file",
        getFileSystem().exists(path));
  }

  @Test(timeout = 30000)
  public void testNNBenchCreateAndRename() throws Exception {
    runNNBench(createJobConf(), "create_write");
    Path path = new Path(BASE_DIR + "/data/file_0_0");
    assertTrue("create_write should create the file",
        getFileSystem().exists(path));
    runNNBench(createJobConf(), "rename");
    Path renamedPath = new Path(BASE_DIR + "/data/file_0_r_0");
    assertFalse("Rename should rename the file", getFileSystem().exists(path));
    assertTrue("Rename should rename the file",
        getFileSystem().exists(renamedPath));
  }

  private void runNNBench(Configuration conf, String operation)
      throws Exception {
    String[] genArgs = { "-operation", operation, "-baseDir", BASE_DIR,
        "-startTime", "" + (Time.now() / 1000 + 3) };

    assertEquals(0, ToolRunner.run(conf, new NNBench(), genArgs));
  }

}
