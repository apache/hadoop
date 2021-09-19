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
package org.apache.hadoop.fs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.shell.Command;
import org.apache.hadoop.fs.shell.CommandFactory;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.util.ToolRunner;
import org.assertj.core.api.Assertions;
import org.junit.Test;
import org.mockito.Mockito;

public class TestFsShell {

  @Test
  public void testConfWithInvalidFile() throws Throwable {
    String[] args = new String[1];
    args[0] = "--conf=invalidFile";
    Throwable th = null;
    try {
      FsShell.main(args);
    } catch (Exception e) {
      th = e;
    }

    if (!(th instanceof RuntimeException)) {
      throw new AssertionError("Expected Runtime exception, got: " + th)
          .initCause(th);
    }
  }

  @Test
  public void testTracing() throws Throwable {
    Configuration conf = new Configuration();
    String prefix = "fs.shell.htrace.";
    conf.setQuietMode(false);
    FsShell shell = new FsShell(conf);
    int res;
    try {
      res = ToolRunner.run(shell, new String[]{"-help", "ls", "cat"});
    } finally {
      shell.close();
    }
  }

  @Test
  public void testDFSWithInvalidCommmand() throws Throwable {
    FsShell shell = new FsShell(new Configuration());
    try (GenericTestUtils.SystemErrCapturer capture =
             new GenericTestUtils.SystemErrCapturer()) {
      ToolRunner.run(shell, new String[]{"dfs -mkdirs"});
      Assertions.assertThat(capture.getOutput())
          .as("FSShell dfs command did not print the error " +
              "message when invalid command is passed")
          .contains("-mkdirs: Unknown command");
      Assertions.assertThat(capture.getOutput())
          .as("FSShell dfs command did not print help " +
              "message when invalid command is passed")
          .contains("Usage: hadoop fs [generic options]");
    }
  }

  @Test
  public void testExceptionNullMessage() throws Exception {
    final String cmdName = "-cmdExNullMsg";
    final Command cmd = Mockito.mock(Command.class);
    Mockito.when(cmd.run(Mockito.any())).thenThrow(
        new IllegalArgumentException());
    Mockito.when(cmd.getUsage()).thenReturn(cmdName);

    final CommandFactory cmdFactory = Mockito.mock(CommandFactory.class);
    final String[] names = {cmdName};
    Mockito.when(cmdFactory.getNames()).thenReturn(names);
    Mockito.when(cmdFactory.getInstance(cmdName)).thenReturn(cmd);

    FsShell shell = new FsShell(new Configuration());
    shell.commandFactory = cmdFactory;
    try (GenericTestUtils.SystemErrCapturer capture =
             new GenericTestUtils.SystemErrCapturer()) {
      ToolRunner.run(shell, new String[]{cmdName});
      Assertions.assertThat(capture.getOutput())
          .contains(cmdName + ": Null exception message");
    }
  }
}
