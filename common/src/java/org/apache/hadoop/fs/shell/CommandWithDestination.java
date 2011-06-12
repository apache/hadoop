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

import java.io.File;
import java.io.IOException;
import java.util.LinkedList;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.shell.PathExceptions.PathExistsException;
import org.apache.hadoop.fs.shell.PathExceptions.PathIOException;
import org.apache.hadoop.fs.shell.PathExceptions.PathIsNotDirectoryException;
import org.apache.hadoop.fs.shell.PathExceptions.PathNotFoundException;

/**
 * Provides: argument processing to ensure the destination is valid
 * for the number of source arguments.  A processPaths that accepts both
 * a source and resolved target.  Sources are resolved as children of
 * a destination directory.
 */
abstract class CommandWithDestination extends FsCommand {  
  protected PathData dst;
  protected boolean overwrite = false;
  
  // TODO: commands should implement a -f to enable this
  protected void setOverwrite(boolean flag) {
    overwrite = flag;
  }
  
  /**
   *  The last arg is expected to be a local path, if only one argument is
   *  given then the destination will be the current directory 
   *  @param args is the list of arguments
   */
  protected void getLocalDestination(LinkedList<String> args)
  throws IOException {
    String pathString = (args.size() < 2) ? Path.CUR_DIR : args.removeLast();
    dst = new PathData(new File(pathString), getConf());
  }

  /**
   *  The last arg is expected to be a remote path, if only one argument is
   *  given then the destination will be the remote user's directory 
   *  @param args is the list of arguments
   *  @throws PathIOException if path doesn't exist or matches too many times 
   */
  protected void getRemoteDestination(LinkedList<String> args)
  throws IOException {
    if (args.size() < 2) {
      dst = new PathData(Path.CUR_DIR, getConf());
    } else {
      String pathString = args.removeLast();
      // if the path is a glob, then it must match one and only one path
      PathData[] items = PathData.expandAsGlob(pathString, getConf());
      switch (items.length) {
        case 0:
          throw new PathNotFoundException(pathString);
        case 1:
          dst = items[0];
          break;
        default:
          throw new PathIOException(pathString, "Too many matches");
      }
    }
  }

  @Override
  protected void processArguments(LinkedList<PathData> args)
  throws IOException {
    // if more than one arg, the destination must be a directory
    // if one arg, the dst must not exist or must be a directory
    if (args.size() > 1) {
      if (!dst.exists) {
        throw new PathNotFoundException(dst.toString());
      }
      if (!dst.stat.isDirectory()) {
        throw new PathIsNotDirectoryException(dst.toString());
      }
    } else {
      if (dst.exists && !dst.stat.isDirectory() && !overwrite) {
        throw new PathExistsException(dst.toString());
      }
    }
    super.processArguments(args);
  }

  @Override
  protected void processPaths(PathData parent, PathData ... items)
  throws IOException {
    PathData savedDst = dst;
    try {
      // modify dst as we descend to append the basename of the
      // current directory being processed
      if (parent != null) dst = dst.getPathDataForChild(parent);
      super.processPaths(parent, items);
    } finally {
      dst = savedDst;
    }
  }
  
  @Override
  protected void processPath(PathData src) throws IOException {
    PathData target;
    // if the destination is a directory, make target a child path,
    // else use the destination as-is
    if (dst.exists && dst.stat.isDirectory()) {
      target = dst.getPathDataForChild(src);
    } else {
      target = dst;
    }
    if (target.exists && !overwrite) {
      throw new PathExistsException(target.toString());
    }

    try { 
      // invoke processPath with both a source and resolved target
      processPath(src, target);
    } catch (PathIOException e) {
      // add the target unless it already has one
      if (e.getTargetPath() == null) {
        e.setTargetPath(target.toString());
      }
      throw e;
    }
  }

  /**
   * Called with a source and target destination pair
   * @param src for the operation
   * @param target for the operation
   * @throws IOException if anything goes wrong
   */
  protected abstract void processPath(PathData src, PathData target)
  throws IOException;
}