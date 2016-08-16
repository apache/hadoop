/*
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

package org.apache.slider.server.services.workflow;

import org.apache.hadoop.util.Shell;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * A source of commands, with the goal being to allow for adding different
 * implementations for different platforms
 */
public class ProcessCommandFactory {

  protected ProcessCommandFactory() {
  }

  /**
   * The command to list a directory
   * @param dir directory
   * @return commands
   */
  public List<String> ls(File dir) {
    List<String> commands;
    if (!Shell.WINDOWS) {
      commands = Arrays.asList("ls","-1", dir.getAbsolutePath());
    } else {
      commands = Arrays.asList("cmd", "/c", "dir", dir.getAbsolutePath());
    }
    return commands;
  }

  /**
   * Echo some text to stdout
   * @param text text
   * @return commands
   */
  public List<String> echo(String text) {
    List<String> commands = new ArrayList<String>(5);
    commands.add("echo");
    commands.add(text);
    return commands;
  }

  /**
   * print env variables
   * @return commands
   */
  public List<String> env() {
    List<String> commands;
    if (!Shell.WINDOWS) {
      commands = Arrays.asList("env");
    } else {
      commands = Arrays.asList("cmd", "/c", "set");
    }
    return commands;
  }

  /**
   * execute a command that returns with an error code that will
   * be converted into a number
   * @return commands
   */
  public List<String> exitFalse() {
    List<String> commands = new ArrayList<String>(2);
    commands.add("false");
    return commands;
  }

  /**
   * Create a process command factory for this OS
   * @return
   */
  public static ProcessCommandFactory createProcessCommandFactory() {
    return new ProcessCommandFactory();
  }
}
