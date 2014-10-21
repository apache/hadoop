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

package org.apache.hadoop.util;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.shell.CommandFormat;
import org.apache.hadoop.fs.shell.CommandFormat.UnknownOptionException;

/**
 * Command-line utility for getting the full classpath needed to launch a Hadoop
 * client application.  If the hadoop script is called with "classpath" as the
 * command, then it simply prints the classpath and exits immediately without
 * launching a JVM.  The output likely will include wildcards in the classpath.
 * If there are arguments passed to the classpath command, then this class gets
 * called.  With the --glob argument, it prints the full classpath with wildcards
 * expanded.  This is useful in situations where wildcard syntax isn't usable.
 * With the --jar argument, it writes the classpath as a manifest in a jar file.
 * This is useful in environments with short limitations on the maximum command
 * line length, where it may not be possible to specify the full classpath in a
 * command.  For example, the maximum command line length on Windows is 8191
 * characters.
 */
@InterfaceAudience.Private
public final class Classpath {
  private static final String usage =
    "classpath [--glob|--jar <path>|-h|--help] :\n"
    + "  Prints the classpath needed to get the Hadoop jar and the required\n"
    + "  libraries.\n"
    + "  Options:\n"
    + "\n"
    + "  --glob       expand wildcards\n"
    + "  --jar <path> write classpath as manifest in jar named <path>\n"
    + "  -h, --help   print help\n";

  /**
   * Main entry point.
   *
   * @param args command-line arguments
   */
  public static void main(String[] args) {
    if (args.length < 1 || args[0].equals("-h") || args[0].equals("--help")) {
      System.out.println(usage);
      return;
    }

    // Copy args, because CommandFormat mutates the list.
    List<String> argsList = new ArrayList<String>(Arrays.asList(args));
    CommandFormat cf = new CommandFormat(0, Integer.MAX_VALUE, "-glob", "-jar");
    try {
      cf.parse(argsList);
    } catch (UnknownOptionException e) {
      terminate(1, "unrecognized option");
      return;
    }

    String classPath = System.getProperty("java.class.path");

    if (cf.getOpt("-glob")) {
      // The classpath returned from the property has been globbed already.
      System.out.println(classPath);
    } else if (cf.getOpt("-jar")) {
      if (argsList.isEmpty() || argsList.get(0) == null ||
          argsList.get(0).isEmpty()) {
        terminate(1, "-jar option requires path of jar file to write");
        return;
      }

      // Write the classpath into the manifest of a temporary jar file.
      Path workingDir = new Path(System.getProperty("user.dir"));
      final String tmpJarPath;
      try {
        tmpJarPath = FileUtil.createJarWithClassPath(classPath, workingDir,
          System.getenv())[0];
      } catch (IOException e) {
        terminate(1, "I/O error creating jar: " + e.getMessage());
        return;
      }

      // Rename the temporary file to its final location.
      String jarPath = argsList.get(0);
      try {
        FileUtil.replaceFile(new File(tmpJarPath), new File(jarPath));
      } catch (IOException e) {
        terminate(1, "I/O error renaming jar temporary file to path: " +
          e.getMessage());
        return;
      }
    }
  }

  /**
   * Prints a message to stderr and exits with a status code.
   *
   * @param status exit code
   * @param msg message
   */
  private static void terminate(int status, String msg) {
    System.err.println(msg);
    ExitUtil.terminate(status, msg);
  }
}
