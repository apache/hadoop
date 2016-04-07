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
import org.apache.hadoop.fs.PathIOException;
import org.apache.hadoop.fs.PathIsDirectoryException;
import org.apache.hadoop.fs.PathNotFoundException;

/**
 * Unix touch like commands
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable

class Touch extends FsCommand {
  public static void registerCommands(CommandFactory factory) {
    factory.addClass(Touchz.class, "-touchz");
  }

  /**
   * (Re)create zero-length file at the specified path.
   * This will be replaced by a more UNIX-like touch when files may be
   * modified.
   */
  public static class Touchz extends Touch {
    public static final String NAME = "touchz";
    public static final String USAGE = "<path> ...";
    public static final String DESCRIPTION =
      "Creates a file of zero length " +
      "at <path> with current time as the timestamp of that <path>. " +
      "An error is returned if the file exists with non-zero length\n";

    @Override
    protected void processOptions(LinkedList<String> args) {
      CommandFormat cf = new CommandFormat(1, Integer.MAX_VALUE);
      cf.parse(args);
    }

    @Override
    protected void processPath(PathData item) throws IOException {
      if (item.stat.isDirectory()) {
        // TODO: handle this
        throw new PathIsDirectoryException(item.toString());
      }
      if (item.stat.getLen() != 0) {
        throw new PathIOException(item.toString(), "Not a zero-length file");
      }
      touchz(item);
    }

    @Override
    protected void processNonexistentPath(PathData item) throws IOException {
      if (!item.parentExists()) {
        throw new PathNotFoundException(item.toString())
            .withFullyQualifiedPath(item.path.toUri().toString());
      }
      touchz(item);
    }

    private void touchz(PathData item) throws IOException {
      item.fs.create(item.path).close();
    }
  }
}
