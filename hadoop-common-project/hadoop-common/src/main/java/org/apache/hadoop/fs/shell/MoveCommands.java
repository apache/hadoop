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
import org.apache.hadoop.fs.PathExistsException;
import org.apache.hadoop.fs.shell.CopyCommands.CopyFromLocal;

/** Various commands for moving files */
@InterfaceAudience.Private
@InterfaceStability.Evolving

class MoveCommands {
  public static void registerCommands(CommandFactory factory) {
    factory.addClass(MoveFromLocal.class, "-moveFromLocal");
    factory.addClass(MoveToLocal.class, "-moveToLocal");
    factory.addClass(Rename.class, "-mv");
  }

  /**
   *  Move local files to a remote filesystem
   */
  public static class MoveFromLocal extends CopyFromLocal {
    public static final String NAME = "moveFromLocal";
    public static final String USAGE = "<localsrc> ... <dst>";
    public static final String DESCRIPTION = 
      "Same as -put, except that the source is " +
      "deleted after it's copied.";

    @Override
    protected void processPath(PathData src, PathData target) throws IOException {
      // unlike copy, don't merge existing dirs during move
      if (target.exists && target.stat.isDirectory()) {
        throw new PathExistsException(target.toString());
      }
      super.processPath(src, target);
    }
    
    @Override
    protected void postProcessPath(PathData src) throws IOException {
      if (!src.fs.delete(src.path, false)) {
        // we have no way to know the actual error...
        PathIOException e = new PathIOException(src.toString());
        e.setOperation("remove");
        throw e;
      }
    }
  }

  /**
   *  Move remote files to a local filesystem
   */
  public static class MoveToLocal extends FsCommand { 
    public static final String NAME = "moveToLocal";
    public static final String USAGE = "<src> <localdst>";
    public static final String DESCRIPTION = "Not implemented yet";

    @Override
    protected void processOptions(LinkedList<String> args) throws IOException {
      throw new IOException("Option '-moveToLocal' is not implemented yet.");
    }
  }

  /** move/rename paths on the same filesystem */
  public static class Rename extends CommandWithDestination {
    public static final String NAME = "mv";
    public static final String USAGE = "<src> ... <dst>";
    public static final String DESCRIPTION = 
      "Move files that match the specified file pattern <src> " +
      "to a destination <dst>.  When moving multiple files, the " +
      "destination must be a directory.";

    @Override
    protected void processOptions(LinkedList<String> args) throws IOException {
      CommandFormat cf = new CommandFormat(2, Integer.MAX_VALUE);
      cf.parse(args);
      getRemoteDestination(args);
    }

    @Override
    protected void processPath(PathData src, PathData target) throws IOException {
      String srcUri = src.fs.getUri().getScheme() + "://" +
          src.fs.getUri().getHost();
      String dstUri = target.fs.getUri().getScheme() + "://" +
          target.fs.getUri().getHost();
      if (!srcUri.equals(dstUri)) {
        throw new PathIOException(src.toString(),
            "Does not match target filesystem");
      }
      if (target.exists) {
        throw new PathExistsException(target.toString());
      }
      if (!target.fs.rename(src.path, target.path)) {
        // we have no way to know the actual error...
        throw new PathIOException(src.toString());
      }
    }
  }
}
