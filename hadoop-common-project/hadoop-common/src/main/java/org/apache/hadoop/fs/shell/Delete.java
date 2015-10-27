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
import java.util.List;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.PathIOException;
import org.apache.hadoop.fs.PathIsDirectoryException;
import org.apache.hadoop.fs.PathIsNotDirectoryException;
import org.apache.hadoop.fs.PathIsNotEmptyDirectoryException;
import org.apache.hadoop.fs.PathNotFoundException;
import org.apache.hadoop.fs.Trash;
import org.apache.hadoop.util.ToolRunner;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.HADOOP_SHELL_SAFELY_DELETE_LIMIT_NUM_FILES;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.HADOOP_SHELL_SAFELY_DELETE_LIMIT_NUM_FILES_DEFAULT;

/**
 * Classes that delete paths.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving

class Delete {
  public static void registerCommands(CommandFactory factory) {
    factory.addClass(Rm.class, "-rm");
    factory.addClass(Rmdir.class, "-rmdir");
    factory.addClass(Rmr.class, "-rmr");
    factory.addClass(Expunge.class, "-expunge");
  }

  /** remove non-directory paths */
  public static class Rm extends FsCommand {
    public static final String NAME = "rm";
    public static final String USAGE = "[-f] [-r|-R] [-skipTrash] " +
        "[-safely] <src> ...";
    public static final String DESCRIPTION =
        "Delete all files that match the specified file pattern. " +
            "Equivalent to the Unix command \"rm <src>\"\n" +
            "-f: If the file does not exist, do not display a diagnostic " +
            "message or modify the exit status to reflect an error.\n" +
            "-[rR]:  Recursively deletes directories.\n" +
            "-skipTrash: option bypasses trash, if enabled, and immediately " +
            "deletes <src>.\n" +
            "-safely: option requires safety confirmation, if enabled, " +
            "requires confirmation before deleting large directory with more " +
            "than <hadoop.shell.delete.limit.num.files> files. Delay is " +
            "expected when walking over large directory recursively to count " +
            "the number of files to be deleted before the confirmation.\n";

    private boolean skipTrash = false;
    private boolean deleteDirs = false;
    private boolean ignoreFNF = false;
    private boolean safeDelete = false;

    @Override
    protected void processOptions(LinkedList<String> args) throws IOException {
      CommandFormat cf = new CommandFormat(
          1, Integer.MAX_VALUE, "f", "r", "R", "skipTrash", "safely");
      cf.parse(args);
      ignoreFNF = cf.getOpt("f");
      deleteDirs = cf.getOpt("r") || cf.getOpt("R");
      skipTrash = cf.getOpt("skipTrash");
      safeDelete = cf.getOpt("safely");
    }

    @Override
    protected List<PathData> expandArgument(String arg) throws IOException {
      try {
        return super.expandArgument(arg);
      } catch (PathNotFoundException e) {
        if (!ignoreFNF) {
          throw e;
        }
        // prevent -f on a non-existent glob from failing
        return new LinkedList<PathData>();
      }
    }

    @Override
    protected void processNonexistentPath(PathData item) throws IOException {
      if (!ignoreFNF) super.processNonexistentPath(item);
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
      if (moveToTrash(item) || !canBeSafelyDeleted(item)) {
        return;
      }
      if (!item.fs.delete(item.path, deleteDirs)) {
        throw new PathIOException(item.toString());
      }
      out.println("Deleted " + item);
    }

    private boolean canBeSafelyDeleted(PathData item)
        throws IOException {
      boolean shouldDelete = true;
      if (safeDelete) {
        final long deleteLimit = getConf().getLong(
            HADOOP_SHELL_SAFELY_DELETE_LIMIT_NUM_FILES,
            HADOOP_SHELL_SAFELY_DELETE_LIMIT_NUM_FILES_DEFAULT);
        if (deleteLimit > 0) {
          ContentSummary cs = item.fs.getContentSummary(item.path);
          final long numFiles = cs.getFileCount();
          if (numFiles > deleteLimit) {
            if (!ToolRunner.confirmPrompt("Proceed deleting " + numFiles +
                " files?")) {
              System.err.println("Delete aborted at user request.\n");
              shouldDelete = false;
            }
          }
        }
      }
      return shouldDelete;
    }

    private boolean moveToTrash(PathData item) throws IOException {
      boolean success = false;
      if (!skipTrash) {
        try {
          success = Trash.moveToAppropriateTrash(item.fs, item.path, getConf());
        } catch(FileNotFoundException fnfe) {
          throw fnfe;
        } catch (IOException ioe) {
          String msg = ioe.getMessage();
          if (ioe.getCause() != null) {
            msg += ": " + ioe.getCause().getMessage();
          }
          throw new IOException(msg + ". Consider using -skipTrash option", ioe);
        }
      }
      return success;
    }
  }
  
  /** remove any path */
  static class Rmr extends Rm {
    public static final String NAME = "rmr";
    
    @Override
    protected void processOptions(LinkedList<String> args) throws IOException {
      args.addFirst("-r");
      super.processOptions(args);
    }

    @Override
    public String getReplacementCommand() {
      return "-rm -r";
    }
  }

  /** remove only empty directories */
  static class Rmdir extends FsCommand {
    public static final String NAME = "rmdir";
    public static final String USAGE =
      "[--ignore-fail-on-non-empty] <dir> ...";
    public static final String DESCRIPTION =
      "Removes the directory entry specified by each directory argument, " +
      "provided it is empty.\n"; 
    
    private boolean ignoreNonEmpty = false;
    
    @Override
    protected void processOptions(LinkedList<String> args) throws IOException {
      CommandFormat cf = new CommandFormat(
          1, Integer.MAX_VALUE, "-ignore-fail-on-non-empty");
      cf.parse(args);
      ignoreNonEmpty = cf.getOpt("-ignore-fail-on-non-empty");
    }

    @Override
    protected void processPath(PathData item) throws IOException {
      if (!item.stat.isDirectory()) {
        throw new PathIsNotDirectoryException(item.toString());
      }      
      if (item.fs.listStatus(item.path).length == 0) {
        if (!item.fs.delete(item.path, false)) {
          throw new PathIOException(item.toString());
        }
      } else if (!ignoreNonEmpty) {
        throw new PathIsNotEmptyDirectoryException(item.toString());
      }
    }
  }

  // delete files from the trash that are older
  // than the retention threshold.
  static class Expunge extends FsCommand {
    public static final String NAME = "expunge";
    public static final String USAGE = "";
    public static final String DESCRIPTION =
        "Delete files from the trash that are older " +
            "than the retention threshold";

    // TODO: should probably allow path arguments for the filesystems
    @Override
    protected void processOptions(LinkedList<String> args) throws IOException {
      CommandFormat cf = new CommandFormat(0, 0);
      cf.parse(args);
    }

    @Override
    protected void processArguments(LinkedList<PathData> args)
    throws IOException {
      FileSystem[] childFileSystems =
          FileSystem.get(getConf()).getChildFileSystems();
      if (null != childFileSystems) {
        for (FileSystem fs : childFileSystems) {
          Trash trash = new Trash(fs, getConf());
          trash.expunge();
          trash.checkpoint();
        }
      } else {
        Trash trash = new Trash(getConf());
        trash.expunge();
        trash.checkpoint();
      }
    }
  }
}
