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

import java.io.IOException;
import java.util.LinkedList;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.shell.PathExceptions.PathNotFoundException;

/**
 * Perform shell-like file tests 
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable

class Test extends FsCommand {  
  public static void registerCommands(CommandFactory factory) {
    factory.addClass(Test.class, "-test");
  }

  public static final String NAME = "test";
  public static final String USAGE = "-[ezd] <path>";
  public static final String DESCRIPTION =
    "If file exists, has zero length, is a directory\n" +
    "then return 0, else return 1.";

  private char flag;
  
  @Override
  protected void processOptions(LinkedList<String> args) {
    CommandFormat cf = new CommandFormat(null, 1, 1, "e", "d", "z");
    cf.parse(args);
    
    String[] opts = cf.getOpts().toArray(new String[0]);
    switch (opts.length) {
      case 0:
        throw new IllegalArgumentException("No test flag given");
      case 1:
        flag = opts[0].charAt(0);
        break;
      default:
        throw new IllegalArgumentException("Only one test flag is allowed");
    }
  }

  @Override
  protected void processPath(PathData item) throws IOException {
    boolean test = false;
    switch (flag) {
      case 'e':
        test = true;
        break;
      case 'd':
        test = item.stat.isDirectory();
        break;
      case 'z':
        test = (item.stat.getLen() == 0);
        break;
    }
    if (!test) exitCode = 1;
  }

  @Override
  protected void processNonexistentPath(PathData item) throws IOException {
    // NOTE: errors for FNF is not how the shell works!
    if (flag != 'e') displayError(new PathNotFoundException(item.toString()));
    exitCode = 1;
  }
}
