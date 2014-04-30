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
import org.junit.Before;
import org.junit.Test;

public class TestXAttrCommands {

  private Configuration conf = null;

  @Before
  public void setup() throws IOException {
    conf = new Configuration();
  }

  @Test
  public void testGetfattrValidations() throws Exception {
    assertFalse("getfattr should fail without path",
        0 == runCommand(new String[] { "-getfattr" }));
    assertFalse("getfattr should fail with extra argument",
        0 == runCommand(new String[] { "-getfattr", "extra", "/test"}));
    assertFalse("getfattr should fail without \"-n name\" or \"-d\"",
        0 == runCommand(new String[] { "-getfattr", "/test"}));
  }

  @Test
  public void testGetfattrWithInvalidEncoding() throws Exception {
    final PrintStream backup = System.err;
    final ByteArrayOutputStream out = new ByteArrayOutputStream();
    System.setErr(new PrintStream(out));
    try {
      runCommand(new String[] { "-getfattr", "-e", "invalid", "-n",
                                "xattrname", "/file1" });
      assertTrue("getfattr should fail with \"-getfattr: Invalid/unsupported "
        + "econding option specified: invalid\". But the output is: "
        + out.toString(), out.toString().contains("-getfattr: "
          + "Invalid/unsupported encoding option specified: invalid"));
    } finally {
      System.setErr(backup);
    }
  }

  @Test
  public void testSetfattrValidations() throws Exception {
    assertFalse("setfattr should fail without path",
        0 == runCommand(new String[] { "-setfattr" }));
    assertFalse("setfattr should fail with extra arguments",
        0 == runCommand(new String[] { "-setfattr", "extra", "/test"}));
    assertFalse("setfattr should fail without \"-n name\" or \"-x name\"",
        0 == runCommand(new String[] { "-setfattr", "/test"}));
  }

  private int runCommand(String[] commands) throws Exception {
    return ToolRunner.run(conf, new FsShell(), commands);
  }
}
