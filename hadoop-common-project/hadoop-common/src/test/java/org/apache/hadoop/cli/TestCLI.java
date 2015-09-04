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

package org.apache.hadoop.cli;

import org.apache.hadoop.cli.util.CLICommand;
import org.apache.hadoop.cli.util.CommandExecutor;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Tests for the Command Line Interface (CLI)
 */
public class TestCLI extends CLITestHelper {
  @Before
  @Override
  public void setUp() throws Exception {
    super.setUp();
  }

  @After
  @Override
  public void tearDown() throws Exception {
    super.tearDown();
  }

  @Override
  protected CommandExecutor.Result execute(CLICommand cmd) throws Exception {
    return cmd.getExecutor("", conf).executeCommand(cmd.getCmd());

  }
  
  @Override
  protected String getTestFile() {
    return "testConf.xml";
  }
  
  @Test
  @Override
  public void testAll() {
    super.testAll();
  }
}
