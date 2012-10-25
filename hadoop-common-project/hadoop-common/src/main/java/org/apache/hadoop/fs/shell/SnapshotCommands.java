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
import org.apache.hadoop.fs.shell.PathExceptions.PathIsNotDirectoryException;

/**
 * Snapshot related operations
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable

class SnapshotCommands extends FsCommand {
  private final static String CREATE_SNAPSHOT = "createSnapshot";
  
  public static void registerCommands(CommandFactory factory) {
    factory.addClass(CreateSnapshot.class, "-" + CREATE_SNAPSHOT);
  }
  
  /**
   *  Create a snapshot
   */
  public static class CreateSnapshot extends FsCommand {
    public static final String NAME = CREATE_SNAPSHOT;
    public static final String USAGE = "<snapshotName> <snapshotRoot>";
    public static final String DESCRIPTION = "Create a snapshot on a directory";

    private static String snapshotName;

    @Override
    protected void processPath(PathData item) throws IOException {
      if (!item.stat.isDirectory()) {
        throw new PathIsNotDirectoryException(item.toString());
      }
    }
    
    @Override
    protected void processOptions(LinkedList<String> args) throws IOException {
      if (args.size() != 2) {
        throw new IOException("args number not 2:" + args.size());
      }
      snapshotName = args.removeFirst();
      // TODO: name length check  

    }

    @Override
    protected void processArguments(LinkedList<PathData> items)
    throws IOException {
      super.processArguments(items);
      if (exitCode != 0) { // check for error collecting paths
        return;
      }
      assert(items.size() == 1);
      PathData sroot = items.getFirst();
      String snapshotRoot = sroot.path.toString();
      sroot.fs.createSnapshot(snapshotName, snapshotRoot);
    }    
  }
}

