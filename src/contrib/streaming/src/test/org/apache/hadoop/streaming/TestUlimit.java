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

import java.io.*;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.dfs.MiniDFSCluster;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.MiniMRCluster;
import org.apache.hadoop.mapred.TestMiniMRWithDFS;
import org.apache.hadoop.util.*;

import junit.framework.TestCase;

/**
 * This tests the setting of memory limit for streaming processes.
 * This will launch a streaming app which will allocate 10MB memory.
 * First, program is launched with sufficient memory. And test expects
 * it to succeed. Then program is launched with insufficient memory and 
 * is expected to be a failure.  
 */
public class TestUlimit extends TestCase {
  private static final Log LOG =
         LogFactory.getLog(TestUlimit.class.getName());
  enum RESULT { FAILURE, SUCCESS };
  String input = "the dummy input";
  Path inputPath = new Path("/testing/in");
  Path outputPath = new Path("/testing/out");
  String map = null;
  MiniDFSCluster dfs = null;
  MiniMRCluster mr = null;
  FileSystem fs = null;

  String[] genArgs(String memLimit) {
    return new String[] {
      "-input", inputPath.toString(),
      "-output", outputPath.toString(),
      "-mapper", map,
      "-reducer", "org.apache.hadoop.mapred.lib.IdentityReducer",
      "-numReduceTasks", "0",
      "-jobconf", "mapred.child.java.opts=" + memLimit,
      "-jobconf", "mapred.job.tracker=" + "localhost:" +
                                           mr.getJobTrackerPort(),
      "-jobconf", "fs.default.name=" + "localhost:" + dfs.getNameNodePort(),
      "-jobconf", "stream.tmpdir=" + 
                   System.getProperty("test.build.data","/tmp")
    };
  }

  /**
   * This tests the setting of memory limit for streaming processes.
   * This will launch a streaming app which will allocate 10MB memory.
   * First, program is launched with sufficient memory. And test expects
   * it to succeed. Then program is launched with insufficient memory and 
   * is expected to be a failure.  
   */
  public void testCommandLine() {
    if (StreamUtil.isCygwin()) {
      return;
    }
    try {
      final int numSlaves = 2;
      Configuration conf = new Configuration();
      dfs = new MiniDFSCluster(conf, numSlaves, true, null);
      fs = dfs.getFileSystem();
      
      mr = new MiniMRCluster(numSlaves, fs.getUri().toString(), 1);
      writeInputFile(fs, inputPath);
      map = StreamUtil.makeJavaCommand(CatApp.class, new String[]{});  
      runProgram("-Xmx2048m", RESULT.SUCCESS);
      FileUtil.fullyDelete(fs, outputPath);
      assertFalse("output not cleaned up", fs.exists(outputPath));
      // 100MB is not sufficient for launching jvm. This launch should fail.
      runProgram("-Xmx0.5m", RESULT.FAILURE);
      mr.waitUntilIdle();
    } catch(IOException e) {
      fail(e.toString());
    } finally {
      mr.shutdown();
      dfs.shutdown();
    }
  }

  private void writeInputFile(FileSystem fs, Path dir) throws IOException {
    DataOutputStream out = fs.create(new Path(dir, "part0"));
    out.writeBytes(input);
    out.close();
  }

  /**
   * Runs the streaming program. and asserts the result of the program.
   * @param memLimit memory limit to set for mapred child.
   * @param result Expected result
   * @throws IOException
   */
  private void runProgram(String memLimit, RESULT result
                          ) throws IOException {
    boolean mayExit = false;
    int ret = 1;
    try {
      StreamJob job = new StreamJob(genArgs(memLimit), mayExit);
      ret = job.go();
    } catch (IOException ioe) {
      LOG.warn("Job Failed! " + StringUtils.stringifyException(ioe));
      ioe.printStackTrace();
    }
    String output = TestMiniMRWithDFS.readOutput(outputPath,
                                        mr.createJobConf());
    if (RESULT.SUCCESS.name().equals(result.name())){
      assertEquals("output is wrong", input, output.trim());
    } else {
      assertTrue("output is correct", !input.equals(output.trim()));
    }
  }
  
  public static void main(String[]args) throws Exception
  {
    new TestUlimit().testCommandLine();
  }

}
