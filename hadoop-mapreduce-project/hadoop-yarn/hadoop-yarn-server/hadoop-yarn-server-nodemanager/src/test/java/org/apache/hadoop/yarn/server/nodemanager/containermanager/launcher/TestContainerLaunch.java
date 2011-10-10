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

package org.apache.hadoop.yarn.server.nodemanager.containermanager.launcher;

import static org.junit.Assert.*;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Shell;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.launcher.ContainerLaunch;
import org.junit.Test;

public class TestContainerLaunch {

  @Test
  public void testSpecialCharSymlinks() throws IOException  {

    String rootDir = new File(System.getProperty(
        "test.build.data", "/tmp")).getAbsolutePath();
    File shellFile = null;
    File tempFile = null;
    String badSymlink = "foo@zz%_#*&!-+= bar()";
    File symLinkFile = null;

    try {
      shellFile = new File(rootDir, "hello.sh");
      tempFile = new File(rootDir, "temp.sh");
      String timeoutCommand = "echo \"hello\"";
      PrintWriter writer = new PrintWriter(new FileOutputStream(shellFile));    
      shellFile.setExecutable(true);
      writer.println(timeoutCommand);
      writer.close();

      Map<Path, String> resources = new HashMap<Path, String>();
      Path path = new Path(shellFile.getAbsolutePath());
      resources.put(path, badSymlink);

      FileOutputStream fos = new FileOutputStream(tempFile);

      Map<String, String> env = new HashMap<String, String>();
      List<String> commands = new ArrayList<String>();
      commands.add("/bin/sh ./\\\"" + badSymlink + "\\\"");

      ContainerLaunch.writeLaunchEnv(fos, env, resources, commands);
      fos.flush();
      fos.close();
      tempFile.setExecutable(true);

      File rootDirFile = new File(rootDir);
      Shell.ShellCommandExecutor shexc 
      = new Shell.ShellCommandExecutor(new String[]{tempFile.getAbsolutePath()}, rootDirFile);

      shexc.execute();
      assertEquals(shexc.getExitCode(), 0);
      assert(shexc.getOutput().contains("hello"));

      symLinkFile = new File(rootDir, badSymlink);      
    }
    finally {
      // cleanup
      if (shellFile != null
          && shellFile.exists()) {
        shellFile.delete();
      }
      if (tempFile != null 
          && tempFile.exists()) {
        tempFile.delete();
      }
      if (symLinkFile != null
          && symLinkFile.exists()) {
        symLinkFile.delete();
      } 
    }
  }

}
