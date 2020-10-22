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

import org.apache.hadoop.thirdparty.com.google.common.annotations.VisibleForTesting;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathIOException;

/**
 * Concat the given files.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class Concat extends FsCommand {
  public static void registerCommands(CommandFactory factory) {
    factory.addClass(Concat.class, "-concat");
  }

  public static final String NAME = "concat";
  public static final String USAGE = "<target path> <src path> <src path> ...";
  public static final String DESCRIPTION = "Concatenate existing source files"
      + " into the target file. Target file and source files should be in the"
      + " same directory.";
  private static FileSystem testFs; // test only.

  @Override
  protected void processArguments(LinkedList<PathData> args)
      throws IOException {
    if (args.size() < 1) {
      throw new IOException("Target path not specified. " + USAGE);
    }
    if (args.size() < 3) {
      throw new IOException(
          "The number of source paths is less than 2. " + USAGE);
    }
    PathData target = args.removeFirst();
    LinkedList<PathData> srcList = args;
    if (!target.exists || !target.stat.isFile()) {
      throw new FileNotFoundException(String
          .format("Target path %s does not exist or is" + " not file.",
              target.path));
    }
    Path[] srcArray = new Path[srcList.size()];
    for (int i = 0; i < args.size(); i++) {
      PathData src = srcList.get(i);
      if (!src.exists || !src.stat.isFile()) {
        throw new FileNotFoundException(
            String.format("%s does not exist or is not file.", src.path));
      }
      srcArray[i] = src.path;
    }
    FileSystem fs = target.fs;
    if (testFs != null) {
      fs = testFs;
    }
    try {
      fs.concat(target.path, srcArray);
    } catch (UnsupportedOperationException exception) {
      throw new PathIOException("Dest filesystem '" + fs.getUri().getScheme()
          + "' doesn't support concat.", exception);
    }
  }

  @VisibleForTesting
  static void setTestFs(FileSystem fs) {
    testFs = fs;
  }
}
