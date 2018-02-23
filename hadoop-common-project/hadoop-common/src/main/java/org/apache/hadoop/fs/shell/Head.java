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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.PathIsDirectoryException;
import org.apache.hadoop.io.IOUtils;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

/**
 * Show the first 1KB of the file.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable

class Head extends FsCommand {
  public static void registerCommands(CommandFactory factory) {
    factory.addClass(Head.class, "-head");
  }
  public static final String NAME = "head";
  public static final String USAGE = "<file>";
  public static final String DESCRIPTION =
      "Show the first 1KB of the file.\n";

  private long endingOffset = 1024;

  @Override
  protected void processOptions(LinkedList<String> args) throws IOException {
    CommandFormat cf = new CommandFormat(1, 1);
    cf.parse(args);
  }

  @Override
  protected List<PathData> expandArgument(String arg) throws IOException {
    List<PathData> items = new LinkedList<PathData>();
    items.add(new PathData(arg, getConf()));
    return items;
  }

  @Override
  protected void processPath(PathData item) throws IOException {
    if (item.stat.isDirectory()) {
      throw new PathIsDirectoryException(item.toString());
    }

    dumpToOffset(item);
  }

  private void dumpToOffset(PathData item) throws IOException {
    FSDataInputStream in = item.fs.open(item.path);
    try {
      IOUtils.copyBytes(in, System.out, endingOffset, false);
    } finally {
      in.close();
    }
  }
}
