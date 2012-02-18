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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

import org.junit.Before;
import org.junit.Test;

/**
 * This class tests if hadoopStreaming background works fine. A DelayEchoApp
 * with 10 seconds delay is submited. 
 */
public class TestStreamingBackground {
  protected File TEST_DIR = new File("target/TestStreamingBackground")
      .getAbsoluteFile();
  protected File INPUT_FILE = new File(TEST_DIR, "input.txt");
  protected File OUTPUT_DIR = new File(TEST_DIR, "out");

  protected String tenSecondsTask = UtilTest.makeJavaCommand(
      DelayEchoApp.class, new String[] { "10" });

  public TestStreamingBackground() throws IOException {
    UtilTest utilTest = new UtilTest(getClass().getName());
    utilTest.checkUserDir();
    utilTest.redirectIfAntJunit();
  }

  protected String[] args = new String[] { 
      "-background",
      "-input", INPUT_FILE.getAbsolutePath(), 
      "-output", OUTPUT_DIR.getAbsolutePath(),
      "-mapper", tenSecondsTask, 
      "-reducer", tenSecondsTask, 
      "-jobconf", "stream.tmpdir=" + System.getProperty("test.build.data", "/tmp"),
      "-jobconf", "mapreduce.task.io.sort.mb=10" 
  };

  @Before
  public void setUp() throws IOException {
    UtilTest.recursiveDelete(TEST_DIR);
    assertTrue(TEST_DIR.mkdirs());

    FileOutputStream out = new FileOutputStream(INPUT_FILE.getAbsoluteFile());
    out.write("hello\n".getBytes());
    out.close();
  }

  public void runStreamJob() throws Exception {
    boolean mayExit = false;
    int returnStatus = 0;

    StreamJob job = new StreamJob(args, mayExit);
    returnStatus = job.go();

    assertEquals("Streaming Job expected to succeed", 0, returnStatus);
    job.running_.killJob();
    job.running_.waitForCompletion();
  }

  @Test
  public void testBackgroundSubmitOk() throws Exception {
    runStreamJob();
  }

}
