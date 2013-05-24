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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.util.JarFinder;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Shell;

/**
 * This class tests hadoopStreaming in MapReduce local mode.
 */
public class TestStreaming
{

  public static final String STREAMING_JAR = JarFinder.getJar(StreamJob.class);

  /**
   * cat command used for copying stdin to stdout as mapper or reducer function.
   * On Windows, use a cmd script that approximates the functionality of cat.
   */
  static final String CAT = Shell.WINDOWS ?
    "cmd /c " + new File("target/bin/cat.cmd").getAbsolutePath() : "cat";

  /**
   * Command used for iterating through file names on stdin and copying each
   * file's contents to stdout, used as mapper or reducer function.  On Windows,
   * use a cmd script that approximates the functionality of xargs cat.
   */
  static final String XARGS_CAT = Shell.WINDOWS ?
    "cmd /c " + new File("target/bin/xargs_cat.cmd").getAbsolutePath() :
    "xargs cat";

  // "map" command: grep -E (red|green|blue)
  // reduce command: uniq
  protected File TEST_DIR;
  protected File INPUT_FILE;
  protected File OUTPUT_DIR;
  protected String inputFile;
  protected String outDir;
  protected String input = "roses.are.red\nviolets.are.blue\nbunnies.are.pink\n";
  // map behaves like "/usr/bin/tr . \\n"; (split words into lines)
  protected String map = UtilTest.makeJavaCommand(TrApp.class, new String[]{".", "\\n"});
  // reduce behave like /usr/bin/uniq. But also prepend lines with R.
  // command-line combiner does not have any effect any more.
  protected String reduce = UtilTest.makeJavaCommand(UniqApp.class, new String[]{"R"});
  protected String outputExpect = "Rare\t\nRblue\t\nRbunnies\t\nRpink\t\nRred\t\nRroses\t\nRviolets\t\n";

  protected ArrayList<String> args = new ArrayList<String>();
  protected StreamJob job;

  public TestStreaming() throws IOException
  {
    UtilTest utilTest = new UtilTest(getClass().getName());
    utilTest.checkUserDir();
    utilTest.redirectIfAntJunit();
    setTestDir(new File("target/TestStreaming").getAbsoluteFile());
  }

  /**
   * Sets root of test working directory and resets any other paths that must be
   * children of the test working directory.  Typical usage is for subclasses
   * that use HDFS to override the test directory to the form "/tmp/<test name>"
   * so that on Windows, tests won't attempt to use paths containing a ':' from
   * the drive specifier.  The ':' character is considered invalid by HDFS.
   * 
   * @param testDir File to set
   */
  protected void setTestDir(File testDir) {
    TEST_DIR = testDir;
    OUTPUT_DIR = new File(testDir, "out");
    INPUT_FILE = new File(testDir, "input.txt");
  }

  @Before
  public void setUp() throws IOException {
    UtilTest.recursiveDelete(TEST_DIR);
    assertTrue("Creating " + TEST_DIR, TEST_DIR.mkdirs());
    args.clear();
  }

  @After
  public void tearDown() {
    UtilTest.recursiveDelete(TEST_DIR);
  }

  protected String getInputData() {
    return input;
  }

  protected void createInput() throws IOException
  {
    DataOutputStream out = getFileSystem().create(new Path(
      INPUT_FILE.getPath()));
    out.write(getInputData().getBytes("UTF-8"));
    out.close();
  }

  protected void setInputOutput() {
    inputFile = INPUT_FILE.getPath();
    outDir = OUTPUT_DIR.getPath();
  }

  protected String[] genArgs() {
    args.add("-input");args.add(inputFile);
    args.add("-output");args.add(outDir);
    args.add("-mapper");args.add(map);
    args.add("-reducer");args.add(reduce);
    args.add("-jobconf");
    args.add("mapreduce.task.files.preserve.failedtasks=true");
    args.add("-jobconf");
    args.add("stream.tmpdir="+System.getProperty("test.build.data","/tmp"));

    String str[] = new String [args.size()];
    args.toArray(str);
    return str;
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
    Path outPath = new Path(OUTPUT_DIR.getPath(), "part-00000");
    FileSystem fs = getFileSystem();
    String output = StreamUtil.slurpHadoop(outPath, fs);
    fs.delete(outPath, true);
    System.err.println("outEx1=" + getExpectedOutput());
    System.err.println("  out1=" + output);
    assertOutput(getExpectedOutput(), output);
  }

  protected void assertOutput(String expectedOutput, String output) throws IOException {
    String[] words = expectedOutput.split("\t\n");
    Set<String> expectedWords = new HashSet<String>(Arrays.asList(words));
    words = output.split("\t\n");
    Set<String> returnedWords = new HashSet<String>(Arrays.asList(words));
//    PrintWriter writer = new PrintWriter(new OutputStreamWriter(new FileOutputStream(new File("/tmp/tucu.txt"), true)), true);
//    writer.println("** Expected: " + expectedOutput);
//    writer.println("** Output  : " + output);
    assertTrue(returnedWords.containsAll(expectedWords));
  }

  /**
   * Runs a streaming job with the given arguments
   * @return the streaming job return status
   * @throws IOException
   */
  protected int runStreamJob() throws IOException {
    setInputOutput();
    createInput();
    boolean mayExit = false;

    // During tests, the default Configuration will use a local mapred
    // So don't specify -config or -cluster
    job = new StreamJob(genArgs(), mayExit);
    return job.go();
  }

  @Test
  public void testCommandLine() throws Exception
  {
    int ret = runStreamJob();
    assertEquals(0, ret);
    checkOutput();
  }
}
