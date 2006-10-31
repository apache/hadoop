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

import junit.framework.TestCase;
import java.io.*;
import java.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * This class tests hadoopStreaming in MapReduce local mode.
 */
public class TestStreaming extends TestCase
{

  // "map" command: grep -E (red|green|blue)
  // reduce command: uniq
  String INPUT_FILE = "input.txt";
  String OUTPUT_DIR = "out";
  String input = "roses.are.red\nviolets.are.blue\nbunnies.are.pink\n";
  // map behaves like "/usr/bin/tr . \\n"; (split words into lines)
  String map = StreamUtil.makeJavaCommand(TrApp.class, new String[]{".", "\\n"});
  // combine, reduce behave like /usr/bin/uniq. But also prepend lines with C, R.
  String combine  = StreamUtil.makeJavaCommand(UniqApp.class, new String[]{"C"});
  String reduce = StreamUtil.makeJavaCommand(UniqApp.class, new String[]{"R"});
  String outputExpect = "RCare\t\nRCblue\t\nRCbunnies\t\nRCpink\t\nRCred\t\nRCroses\t\nRCviolets\t\n";

  StreamJob job;

  public TestStreaming() throws IOException
  {
    UtilTest utilTest = new UtilTest(getClass().getName());
    utilTest.checkUserDir();
    utilTest.redirectIfAntJunit();
  }

  void createInput() throws IOException
  {
    String path = new File(".", INPUT_FILE).getAbsolutePath();// needed from junit forked vm
    DataOutputStream out = new DataOutputStream(new FileOutputStream(path));
    out.write(input.getBytes("UTF-8"));
    out.close();
  }

  public void testCommandLine()
  {
    try {
      createInput();
      boolean mayExit = false;

      // During tests, the default Configuration will use a local mapred
      // So don't specify -config or -cluster
      String argv[] = new String[] {
          "-input", INPUT_FILE,
          "-output", OUTPUT_DIR,
          "-mapper", map,
          "-combiner", combine,
          "-reducer", reduce,
          //"-verbose",
          //"-jobconf", "stream.debug=set"
          "-jobconf", "keep.failed.task.files=true",
      };
      job = new StreamJob(argv, mayExit);      
      job.go();
      File outFile = new File(".", OUTPUT_DIR + "/part-00000").getAbsoluteFile();
      String output = StreamUtil.slurp(outFile);
      System.err.println("outEx1=" + outputExpect);
      System.err.println("  out1=" + output);
      assertEquals(outputExpect, output);

    } catch(Exception e) {
      failTrace(e);
    }
  }

  void failTrace(Exception e)
  {
    StringWriter sw = new StringWriter();
    e.printStackTrace(new PrintWriter(sw));
    fail(sw.toString());
  }

  public static void main(String[]args) throws Exception
  {
    new TestStreaming().testCommandLine();
  }

}
