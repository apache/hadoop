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

import org.junit.Test;
import static org.junit.Assert.*;

import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;


/**
 * This class tests hadoopStreaming in MapReduce local mode.
 */
public class TestStreaming
{

  // "map" command: grep -E (red|green|blue)
  // reduce command: uniq
  protected File TEST_DIR;
  protected File INPUT_FILE;
  protected File OUTPUT_DIR;
  protected String input = "roses.are.red\nviolets.are.blue\nbunnies.are.pink\n";
  // map behaves like "/usr/bin/tr . \\n"; (split words into lines)
  protected String map = StreamUtil.makeJavaCommand(TrApp.class, new String[]{".", "\\n"});
  // reduce behave like /usr/bin/uniq. But also prepend lines with R.
  // command-line combiner does not have any effect any more.
  protected String reduce = StreamUtil.makeJavaCommand(UniqApp.class, new String[]{"R"});
  protected String outputExpect = "Rare\t\nRblue\t\nRbunnies\t\nRpink\t\nRred\t\nRroses\t\nRviolets\t\n";

  protected StreamJob job;

  public TestStreaming() throws IOException
  {
    UtilTest utilTest = new UtilTest(getClass().getName());
    utilTest.checkUserDir();
    utilTest.redirectIfAntJunit();
    TEST_DIR = new File(getClass().getName()).getAbsoluteFile();
    OUTPUT_DIR = new File(TEST_DIR, "out");
    INPUT_FILE = new File(TEST_DIR, "input.txt");
  }

  protected String getInputData() {
    return input;
  }

  protected void createInput() throws IOException
  {
    DataOutputStream out = getFileSystem().create(
      new Path(INPUT_FILE.getAbsolutePath()));
    out.write(getInputData().getBytes("UTF-8"));
    out.close();
  }

  protected String[] genArgs() {
    return new String[] {
      "-input", INPUT_FILE.getAbsolutePath(),
      "-output", OUTPUT_DIR.getAbsolutePath(),
      "-mapper", map,
      "-reducer", reduce,
      //"-verbose",
      //"-jobconf", "stream.debug=set"
      "-jobconf", "mapreduce.task.files.preserve.failedtasks=true",
      "-jobconf", "stream.tmpdir="+System.getProperty("test.build.data","/tmp")
    };
  }

  protected Configuration getConf() {
    return new Configuration();
  }

  protected FileSystem getFileSystem() throws IOException {
    return FileSystem.get(getConf());
  }

  protected String getExpectedOutput() {
    return outputExpect;
  }

  protected void checkOutput() throws IOException {
    Path outPath = new Path(OUTPUT_DIR.getAbsolutePath(), "part-00000");
    FileSystem fs = getFileSystem();
    String output = StreamUtil.slurpHadoop(outPath, fs);
    fs.delete(outPath, true);
    System.err.println("outEx1=" + getExpectedOutput());
    System.err.println("  out1=" + output);
    assertEquals(getExpectedOutput(), output);
  }

  @Test
  public void testCommandLine() throws Exception
  {
    UtilTest.recursiveDelete(TEST_DIR);
    assertTrue("Creating " + TEST_DIR, TEST_DIR.mkdirs());
    createInput();
    boolean mayExit = false;

    // During tests, the default Configuration will use a local mapred
    // So don't specify -config or -cluster
    job = new StreamJob(genArgs(), mayExit);
    int ret = job.go();
    assertEquals(0, ret);
    checkOutput();
  }

  public static void main(String[]args) throws Exception
  {
    new TestStreaming().testCommandLine();
  }

}
