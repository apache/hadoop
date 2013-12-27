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

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FsShell;
import org.apache.hadoop.util.ToolRunner;
import org.junit.Before;
import org.junit.Test;

public class TestAclCommands {

  private Configuration conf = null;

  @Before
  public void setup() throws IOException {
    conf = new Configuration();
  }

  @Test
  public void testGetfaclValidations() throws Exception {
    assertFalse("getfacl should fail without path",
        0 == runCommand(new String[] { "-getfacl" }));
    assertFalse("getfacl should fail with extra argument",
        0 == runCommand(new String[] { "-getfacl", "/test", "extraArg" }));
  }

  @Test
  public void testSetfaclValidations() throws Exception {
    assertFalse("setfacl should fail without path",
        0 == runCommand(new String[] { "-setfacl" }));
    assertFalse("setfacl should fail without aclSpec",
        0 == runCommand(new String[] { "-setfacl", "-m", "/path" }));
    assertFalse("setfacl should fail with conflicting options",
        0 == runCommand(new String[] { "-setfacl", "-m", "/path" }));
    assertFalse("setfacl should fail with extra arguments",
        0 == runCommand(new String[] { "-setfacl", "/path", "extra" }));
    assertFalse("setfacl should fail with extra arguments",
        0 == runCommand(new String[] { "-setfacl", "--set",
            "default:user::rwx", "/path", "extra" }));
  }

  private int runCommand(String[] commands) throws Exception {
    return ToolRunner.run(conf, new FsShell(), commands);
  }
}
