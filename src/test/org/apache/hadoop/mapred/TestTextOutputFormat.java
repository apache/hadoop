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

package org.apache.hadoop.mapred;

import java.io.*;
import java.util.*;
import junit.framework.TestCase;

import org.apache.commons.logging.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.io.compress.*;
import org.apache.hadoop.util.ReflectionUtils;

public class TestTextOutputFormat extends TestCase {
  private static final Log LOG = LogFactory.getLog(TestTextOutputFormat.class
                                                   .getName());

  private static JobConf defaultConf = new JobConf();

  private static FileSystem localFs = null;
  static {
    try {
      localFs = FileSystem.getLocal(defaultConf);
    } catch (IOException e) {
      throw new RuntimeException("init failure", e);
    }
  }

  private static Path workDir = new Path(new Path(System.getProperty(
                                                                     "test.build.data", "."), "data"), "TestTextOutputFormat");

  public void testFormat() throws Exception {
    JobConf job = new JobConf();
    job.setOutputPath(workDir);
    String file = "test.txt";
    
    // A reporter that does nothing
    Reporter reporter = Reporter.NULL;

    TextOutputFormat<Text, Text> theOutputFormat =
      new TextOutputFormat<Text, Text>();
    RecordWriter<Text, Text> theRecordWriter =
      theOutputFormat.getRecordWriter(localFs, job, file, reporter);

    Text key1 = new Text("key1");
    Text key2 = new Text("key2");
    Text val1 = new Text("val1");
    Text val2 = new Text("val2");

    try {
      theRecordWriter.write(key1, val1);
      theRecordWriter.write(null, val1);
      theRecordWriter.write(key1, null);
      theRecordWriter.write(null, null);
      theRecordWriter.write(key2, val2);

    } finally {
      theRecordWriter.close(reporter);
    }
    File expectedFile = new File(new Path(workDir, file).toString()); 
    StringBuffer expectedOutput = new StringBuffer();
    expectedOutput.append(key1).append('\t').append(val1).append("\n");
    expectedOutput.append(val1).append("\n");
    expectedOutput.append(key1).append("\n");
    expectedOutput.append(key2).append('\t').append(val2).append("\n");
    String output = UtilsForTests.slurp(expectedFile);
    assertEquals(output, expectedOutput.toString());
    
  }

  public static void main(String[] args) throws Exception {
    new TestTextOutputFormat().testFormat();
  }
}
