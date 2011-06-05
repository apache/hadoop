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
import org.apache.hadoop.fs.Trash;
import org.apache.hadoop.fs.shell.PathExceptions.PathIOException;
import org.apache.hadoop.fs.shell.PathExceptions.PathIsDirectoryException;

/**
 * Classes that delete paths
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving

class Delete extends FsCommand {
  public static void registerCommands(CommandFactory factory) {
    factory.addClass(Rm.class, "-rm");
    factory.addClass(Rmr.class, "-rmr");
    factory.addClass(Expunge.class, "-expunge");
  }

  /** remove non-directory paths */
  public static class Rm extends FsCommand {
    public static final String NAME = "rm";
    public static final String USAGE = "[-r|-R] [-skipTrash] <src> ...";
    public static final String DESCRIPTION =
      "Delete all files that match the specified file pattern.\n" +
      "Equivalent to the Unix command \"rm <src>\"\n" +
      "-skipTrash option bypasses trash, if enabled, and immediately\n" +
      "deletes <src>\n" +
      "  -[rR]  Recursively deletes directories";

    private boolean skipTrash = false;
    private boolean deleteDirs = false;
    
    @Override
    protected void processOptions(LinkedList<String> args) throws IOException {
      CommandFormat cf = new CommandFormat(
          1, Integer.MAX_VALUE, "r", "R", "skipTrash");
      cf.parse(args);
      deleteDirs = cf.getOpt("r") || cf.getOpt("R");
      skipTrash = cf.getOpt("skipTrash");
    }
    
    @Override
    protected void processPath(PathData item) throws IOException {
      if (item.stat.isDirectory() && !deleteDirs) {
        throw new PathIsDirectoryException(item.toString());
      }

      // TODO: if the user wants the trash to be used but there is any
      // problem (ie. creating the trash dir, moving the item to be deleted,
      // etc), then the path will just be deleted because moveToTrash returns
      // false and it falls thru to fs.delete.  this doesn't seem right
      if (moveToTrash(item)) {
        return;
      }
      if (!item.fs.delete(item.path, deleteDirs)) {
        throw new PathIOException(item.toString());
      }
      out.println("Deleted " + item);
    }

    private boolean moveToTrash(PathData item) throws IOException {
      boolean success = false;
      if (!skipTrash) {
        success = Trash.moveToAppropriateTrash(item.fs, item.path, getConf());
      }
      return success;
    }
  }
  
  /** remove any path */
  static class Rmr extends Rm {
    public static final String NAME = "rmr";
    
    protected void processOptions(LinkedList<String> args) throws IOException {
      args.addFirst("-r");
      super.processOptions(args);
    }

    @Override
    public String getReplacementCommand() {
      return "rm -r";
    }
  }
  
  /** empty the trash */
  static class Expunge extends FsCommand {
    public static final String NAME = "expunge";
    public static final String USAGE = "";
    public static final String DESCRIPTION = "Empty the Trash";

    // TODO: should probably allow path arguments for the filesystems
    @Override
    protected void processOptions(LinkedList<String> args) throws IOException {
      CommandFormat cf = new CommandFormat(0, 0);
      cf.parse(args);
    }

    @Override
    protected void processArguments(LinkedList<PathData> args)
    throws IOException {
      Trash trash = new Trash(getConf());
      trash.expunge();
      trash.checkpoint();    
    }
  }
}
