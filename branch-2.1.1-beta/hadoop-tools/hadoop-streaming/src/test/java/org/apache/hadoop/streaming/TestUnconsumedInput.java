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

package org.apache.hadoop.streaming;

import static org.junit.Assert.*;

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.Test;

public class TestUnconsumedInput {
  protected final int EXPECTED_OUTPUT_SIZE = 10000;
  protected File INPUT_FILE = new File("stream_uncinput_input.txt");
  protected File OUTPUT_DIR = new File("stream_uncinput_out");
  // map parses input lines and generates count entries for each word.
  protected String input = "roses.are.red\nviolets.are.blue\nbunnies.are.pink\n";
  protected String map = UtilTest.makeJavaCommand(OutputOnlyApp.class,
      new String[]{Integer.toString(EXPECTED_OUTPUT_SIZE)});

  private StreamJob job;

  public TestUnconsumedInput() throws IOException
  {
    UtilTest utilTest = new UtilTest(getClass().getName());
    utilTest.checkUserDir();
    utilTest.redirectIfAntJunit();
  }

  protected void createInput() throws IOException
  {
      DataOutputStream out = new DataOutputStream(
          new FileOutputStream(INPUT_FILE.getAbsoluteFile()));
      for (int i=0; i<10000; ++i) {
        out.write(input.getBytes("UTF-8"));
      }
      out.close();
  }

  protected String[] genArgs() {
    return new String[] {
      "-input", INPUT_FILE.getAbsolutePath(),
      "-output", OUTPUT_DIR.getAbsolutePath(),
      "-mapper", map,
      "-reducer", "org.apache.hadoop.mapred.lib.IdentityReducer",
      "-numReduceTasks", "0",
      "-jobconf", "mapreduce.task.files.preserve.failedtasks=true",
      "-jobconf", "stream.tmpdir="+System.getProperty("test.build.data","/tmp")
    };
  }

  @Test
  public void testUnconsumedInput() throws Exception
  {
    String outFileName = "part-00000";
    File outFile = null;
    try {
      try {
        FileUtil.fullyDelete(OUTPUT_DIR.getAbsoluteFile());
      } catch (Exception e) {
      }

      createInput();

      // setup config to ignore unconsumed input
      Configuration conf = new Configuration();
      conf.set("stream.minRecWrittenToEnableSkip_", "0");

      job = new StreamJob();
      job.setConf(conf);
      int exitCode = job.run(genArgs());
      assertEquals("Job failed", 0, exitCode);
      outFile = new File(OUTPUT_DIR, outFileName).getAbsoluteFile();
      String output = StreamUtil.slurp(outFile);
      assertEquals("Output was truncated", EXPECTED_OUTPUT_SIZE,
          StringUtils.countMatches(output, "\t"));
    } finally {
      INPUT_FILE.delete();
      FileUtil.fullyDelete(OUTPUT_DIR.getAbsoluteFile());
    }
  }
}
