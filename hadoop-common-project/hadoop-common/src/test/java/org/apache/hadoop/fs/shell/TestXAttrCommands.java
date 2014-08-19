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
package org.apache.hadoop.fs.shell;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FsShell;
import org.apache.hadoop.util.ToolRunner;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestXAttrCommands {
  private final ByteArrayOutputStream errContent = 
      new ByteArrayOutputStream();
  private Configuration conf = null;
  private PrintStream initialStdErr;

  @Before
  public void setup() throws IOException {
    errContent.reset();
    initialStdErr = System.err;
    System.setErr(new PrintStream(errContent));
    conf = new Configuration();
  }
  
  @After
  public void cleanUp() throws Exception {
    errContent.reset();
    System.setErr(initialStdErr);
  }

  @Test
  public void testGetfattrValidations() throws Exception {
    errContent.reset();
    assertFalse("getfattr should fail without path",
        0 == runCommand(new String[] { "-getfattr", "-d"}));
    assertTrue(errContent.toString().contains("<path> is missing"));

    errContent.reset();
    assertFalse("getfattr should fail with extra argument",
        0 == runCommand(new String[] { "-getfattr", "extra", "-d", "/test"}));
    assertTrue(errContent.toString().contains("Too many arguments"));
    
    errContent.reset();
    assertFalse("getfattr should fail without \"-n name\" or \"-d\"",
        0 == runCommand(new String[] { "-getfattr", "/test"}));
    assertTrue(errContent.toString().contains("Must specify '-n name' or '-d' option"));
    
    errContent.reset();
    assertFalse("getfattr should fail with invalid encoding",
        0 == runCommand(new String[] { "-getfattr", "-d", "-e", "aaa", "/test"}));
    assertTrue(errContent.toString().contains("Invalid/unsupported encoding option specified: aaa"));
  }

  @Test
  public void testSetfattrValidations() throws Exception {
    errContent.reset();
    assertFalse("setfattr should fail without path",
        0 == runCommand(new String[] { "-setfattr", "-n", "user.a1" }));
    assertTrue(errContent.toString().contains("<path> is missing"));
    
    errContent.reset();
    assertFalse("setfattr should fail with extra arguments",
        0 == runCommand(new String[] { "-setfattr", "extra", "-n", "user.a1", "/test"}));
    assertTrue(errContent.toString().contains("Too many arguments"));
    
    errContent.reset();
    assertFalse("setfattr should fail without \"-n name\" or \"-x name\"",
        0 == runCommand(new String[] { "-setfattr", "/test"}));
    assertTrue(errContent.toString().contains("Must specify '-n name' or '-x name' option"));
  }

  private int runCommand(String[] commands) throws Exception {
    return ToolRunner.run(conf, new FsShell(), commands);
  }
}
