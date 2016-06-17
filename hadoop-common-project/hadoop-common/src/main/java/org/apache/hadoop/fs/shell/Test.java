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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.LinkedList;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.security.AccessControlException;

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
  public static final String USAGE = "-[defsz] <path>";
  public static final String DESCRIPTION =
      "Answer various questions about <path>, with result via exit status.\n"
          + "  -d  return 0 if <path> is a directory.\n"
          + "  -e  return 0 if <path> exists.\n"
          + "  -f  return 0 if <path> is a file.\n"
          + "  -s  return 0 if file <path> is greater "
          + "        than zero bytes in size.\n"
          + "  -w  return 0 if file <path> exists "
          + "        and write permission is granted.\n"
          + "  -r  return 0 if file <path> exists "
          + "        and read permission is granted.\n"
          + "  -z  return 0 if file <path> is "
          + "        zero bytes in size, else return 1.";

  private char flag;
  
  @Override
  protected void processOptions(LinkedList<String> args) {
    CommandFormat cf = new CommandFormat(1, 1,
        "e", "d", "f", "s", "z", "w", "r");
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
    case 'f':
      test = item.stat.isFile();
      break;
    case 's':
      test = (item.stat.getLen() > 0);
      break;
    case 'z':
      test = (item.stat.getLen() == 0);
      break;
    case 'w':
      test = testAccess(item, FsAction.WRITE);
      break;
    case 'r':
      test = testAccess(item, FsAction.READ);
      break;
    default:
      break;
    }
    if (!test) {
      exitCode = 1;
    }
  }

  private boolean testAccess(PathData item, FsAction action)
      throws IOException {
    try {
      item.fs.access(item.path, action);
      return true;
    } catch (AccessControlException | FileNotFoundException e) {
      return false;
    }
  }

  @Override
  protected void processNonexistentPath(PathData item) throws IOException {
    exitCode = 1;
  }
}