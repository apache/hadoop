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

/**
 * Create the given dir
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable

class Mkdir extends FsCommand {
  public static void registerCommands(CommandFactory factory) {
    factory.addClass(Mkdir.class, "-mkdir");
  }
  
  public static final String NAME = "mkdir";
  public static final String USAGE = "<path> ...";
  public static final String DESCRIPTION =
    "Create a directory in specified location.";

  @Override
  protected void processOptions(LinkedList<String> args) {
    CommandFormat cf = new CommandFormat(null, 1, Integer.MAX_VALUE);
    cf.parse(args);
  }

  @Override
  protected void processPath(PathData item) throws IOException {
    if (item.stat.isDirectory()) {
      throw new IOException("cannot create directory " + item + ": File exists");
    } else {
      throw new IOException(item + " exists but is not a directory");
    }
  }

  @Override
  protected void processNonexistentPath(PathData item) throws IOException {
    if (!item.fs.mkdirs(item.path)) {
      throw new IOException("failed to create " + item);
    }
  }
}
