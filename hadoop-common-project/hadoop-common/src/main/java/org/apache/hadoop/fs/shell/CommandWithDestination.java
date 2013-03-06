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
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.LinkedList;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FilterFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathExistsException;
import org.apache.hadoop.fs.PathIOException;
import org.apache.hadoop.fs.PathIsDirectoryException;
import org.apache.hadoop.fs.PathIsNotDirectoryException;
import org.apache.hadoop.fs.PathNotFoundException;
import org.apache.hadoop.fs.PathOperationException;
import org.apache.hadoop.io.IOUtils;

/**
 * Provides: argument processing to ensure the destination is valid
 * for the number of source arguments.  A processPaths that accepts both
 * a source and resolved target.  Sources are resolved as children of
 * a destination directory.
 */
abstract class CommandWithDestination extends FsCommand {  
  protected PathData dst;
  private boolean overwrite = false;
  private boolean verifyChecksum = true;
  private boolean writeChecksum = true;
  
  /**
   * 
   * This method is used to enable the force(-f)  option while copying the files.
   * 
   * @param flag true/false
   */
  protected void setOverwrite(boolean flag) {
    overwrite = flag;
  }
  
  protected void setVerifyChecksum(boolean flag) {
    verifyChecksum = flag;
  }
  
  protected void setWriteChecksum(boolean flag) {
    writeChecksum = flag;
  }
  
  /**
   *  The last arg is expected to be a local path, if only one argument is
   *  given then the destination will be the current directory 
   *  @param args is the list of arguments
   */
  protected void getLocalDestination(LinkedList<String> args)
  throws IOException {
    try {
      String pathString = (args.size() < 2) ? Path.CUR_DIR : args.removeLast();
      dst = new PathData(new URI(pathString), getConf());
    } catch (URISyntaxException e) {
      throw new IOException("unexpected URISyntaxException", e);
    }
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
    } else if (dst.exists) {
      if (!dst.stat.isDirectory() && !overwrite) {
        throw new PathExistsException(dst.toString());
      }
    } else if (!dst.parentExists()) {
      throw new PathNotFoundException(dst.toString());
    }
    super.processArguments(args);
  }

  @Override
  protected void processPathArgument(PathData src)
  throws IOException {
    if (src.stat.isDirectory() && src.fs.equals(dst.fs)) {
      PathData target = getTargetPath(src);
      String srcPath = src.fs.makeQualified(src.path).toString();
      String dstPath = dst.fs.makeQualified(target.path).toString();
      if (dstPath.equals(srcPath)) {
        PathIOException e = new PathIOException(src.toString(),
            "are identical");
        e.setTargetPath(dstPath.toString());
        throw e;
      }
      if (dstPath.startsWith(srcPath+Path.SEPARATOR)) {
        PathIOException e = new PathIOException(src.toString(),
            "is a subdirectory of itself");
        e.setTargetPath(target.toString());
        throw e;
      }
    }
    super.processPathArgument(src);
  }

  @Override
  protected void processPath(PathData src) throws IOException {
    processPath(src, getTargetPath(src));
  }
  
  /**
   * Called with a source and target destination pair
   * @param src for the operation
   * @param target for the operation
   * @throws IOException if anything goes wrong
   */
  protected void processPath(PathData src, PathData dst) throws IOException {
    if (src.stat.isSymlink()) {
      // TODO: remove when FileContext is supported, this needs to either
      // copy the symlink or deref the symlink
      throw new PathOperationException(src.toString());        
    } else if (src.stat.isFile()) {
      copyFileToTarget(src, dst);
    } else if (src.stat.isDirectory() && !isRecursive()) {
      throw new PathIsDirectoryException(src.toString());
    }
  }

  @Override
  protected void recursePath(PathData src) throws IOException {
    PathData savedDst = dst;
    try {
      // modify dst as we descend to append the basename of the
      // current directory being processed
      dst = getTargetPath(src);
      if (dst.exists) {
        if (!dst.stat.isDirectory()) {
          throw new PathIsNotDirectoryException(dst.toString());
        }
      } else {
        if (!dst.fs.mkdirs(dst.path)) {
          // too bad we have no clue what failed
          PathIOException e = new PathIOException(dst.toString());
          e.setOperation("mkdir");
          throw e;
        }    
        dst.refreshStatus(); // need to update stat to know it exists now
      }      
      super.recursePath(src);
    } finally {
      dst = savedDst;
    }
  }
  
  protected PathData getTargetPath(PathData src) throws IOException {
    PathData target;
    // on the first loop, the dst may be directory or a file, so only create
    // a child path if dst is a dir; after recursion, it's always a dir
    if ((getDepth() > 0) || (dst.exists && dst.stat.isDirectory())) {
      target = dst.getPathDataForChild(src);
    } else if (dst.representsDirectory()) { // see if path looks like a dir
      target = dst.getPathDataForChild(src);
    } else {
      target = dst;
    }
    return target;
  }
  
  /**
   * Copies the source file to the target.
   * @param src item to copy
   * @param target where to copy the item
   * @throws IOException if copy fails
   */ 
  protected void copyFileToTarget(PathData src, PathData target) throws IOException {
    src.fs.setVerifyChecksum(verifyChecksum);
    copyStreamToTarget(src.fs.open(src.path), target);
  }
  
  /**
   * Copies the stream contents to a temporary file.  If the copy is
   * successful, the temporary file will be renamed to the real path,
   * else the temporary file will be deleted.
   * @param in the input stream for the copy
   * @param target where to store the contents of the stream
   * @throws IOException if copy fails
   */ 
  protected void copyStreamToTarget(InputStream in, PathData target)
  throws IOException {
    if (target.exists && (target.stat.isDirectory() || !overwrite)) {
      throw new PathExistsException(target.toString());
    }
    TargetFileSystem targetFs = new TargetFileSystem(target.fs);
    try {
      PathData tempTarget = target.suffix("._COPYING_");
      targetFs.setWriteChecksum(writeChecksum);
      targetFs.writeStreamToFile(in, tempTarget);
      targetFs.rename(tempTarget, target);
    } finally {
      targetFs.close(); // last ditch effort to ensure temp file is removed
    }
  }

  // Helper filter filesystem that registers created files as temp files to
  // be deleted on exit unless successfully renamed
  private static class TargetFileSystem extends FilterFileSystem {
    TargetFileSystem(FileSystem fs) {
      super(fs);
    }

    void writeStreamToFile(InputStream in, PathData target) throws IOException {
      FSDataOutputStream out = null;
      try {
        out = create(target);
        IOUtils.copyBytes(in, out, getConf(), true);
      } finally {
        IOUtils.closeStream(out); // just in case copyBytes didn't
      }
    }
    
    // tag created files as temp files
    FSDataOutputStream create(PathData item) throws IOException {
      try {
        return create(item.path, true);
      } finally { // might have been created but stream was interrupted
        deleteOnExit(item.path);
      }
    }

    void rename(PathData src, PathData target) throws IOException {
      // the rename method with an option to delete the target is deprecated
      if (target.exists && !delete(target.path, false)) {
        // too bad we don't know why it failed
        PathIOException e = new PathIOException(target.toString());
        e.setOperation("delete");
        throw e;
      }
      if (!rename(src.path, target.path)) {
        // too bad we don't know why it failed
        PathIOException e = new PathIOException(src.toString());
        e.setOperation("rename");
        e.setTargetPath(target.toString());
        throw e;
      }
      // cancel delete on exit if rename is successful
      cancelDeleteOnExit(src.path);
    }
    @Override
    public void close() {
      // purge any remaining temp files, but don't close underlying fs
      processDeleteOnExit();
    }
  }
}
