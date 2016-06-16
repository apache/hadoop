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

package org.apache.hadoop.fs.s3;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.ParentNotDirectoryException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.s3native.NativeS3FileSystem;
import org.apache.hadoop.fs.s3native.S3xLoginHelper;
import org.apache.hadoop.io.retry.RetryPolicies;
import org.apache.hadoop.io.retry.RetryPolicy;
import org.apache.hadoop.io.retry.RetryProxy;
import org.apache.hadoop.util.Progressable;

/**
 * A block-based {@link FileSystem} backed by
 * <a href="http://aws.amazon.com/s3">Amazon S3</a>.
 *
 * @see NativeS3FileSystem
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class S3FileSystem extends FileSystem {

  private URI uri;

  private FileSystemStore store;

  private Path workingDir;

  public S3FileSystem() {
    // set store in initialize()
  }

  public S3FileSystem(FileSystemStore store) {
    this.store = store;
  }

  /**
   * Return the protocol scheme for the FileSystem.
   *
   * @return <code>s3</code>
   */
  @Override
  public String getScheme() {
    return "s3";
  }

  @Override
  public URI getUri() {
    return uri;
  }

  @Override
  public void initialize(URI uri, Configuration conf) throws IOException {
    super.initialize(uri, conf);
    if (store == null) {
      store = createDefaultStore(conf);
    }
    store.initialize(uri, conf);
    setConf(conf);
    this.uri = S3xLoginHelper.buildFSURI(uri);
    this.workingDir =
      new Path("/user", System.getProperty("user.name")).makeQualified(this);
  }

  private static FileSystemStore createDefaultStore(Configuration conf) {
    FileSystemStore store = new Jets3tFileSystemStore();

    RetryPolicy basePolicy = RetryPolicies.retryUpToMaximumCountWithFixedSleep(
                                                                               conf.getInt("fs.s3.maxRetries", 4),
                                                                               conf.getLong("fs.s3.sleepTimeSeconds", 10), TimeUnit.SECONDS);
    Map<Class<? extends Exception>,RetryPolicy> exceptionToPolicyMap =
      new HashMap<Class<? extends Exception>, RetryPolicy>();
    exceptionToPolicyMap.put(IOException.class, basePolicy);
    exceptionToPolicyMap.put(S3Exception.class, basePolicy);

    RetryPolicy methodPolicy = RetryPolicies.retryByException(
                                                              RetryPolicies.TRY_ONCE_THEN_FAIL, exceptionToPolicyMap);
    Map<String,RetryPolicy> methodNameToPolicyMap = new HashMap<String,RetryPolicy>();
    methodNameToPolicyMap.put("storeBlock", methodPolicy);
    methodNameToPolicyMap.put("retrieveBlock", methodPolicy);

    return (FileSystemStore) RetryProxy.create(FileSystemStore.class,
                                               store, methodNameToPolicyMap);
  }

  @Override
  public Path getWorkingDirectory() {
    return workingDir;
  }

  @Override
  public void setWorkingDirectory(Path dir) {
    workingDir = makeAbsolute(dir);
  }

  private Path makeAbsolute(Path path) {
    if (path.isAbsolute()) {
      return path;
    }
    return new Path(workingDir, path);
  }

  /**
   * Check that a Path belongs to this FileSystem.
   * Unlike the superclass, this version does not look at authority,
   * only hostnames.
   * @param path to check
   * @throws IllegalArgumentException if there is an FS mismatch
   */
  @Override
  protected void checkPath(Path path) {
    S3xLoginHelper.checkPath(getConf(), getUri(), path, getDefaultPort());
  }

  @Override
  protected URI canonicalizeUri(URI rawUri) {
    return S3xLoginHelper.canonicalizeUri(rawUri, getDefaultPort());
  }

  /**
   * @param permission Currently ignored.
   */
  @Override
  public boolean mkdirs(Path path, FsPermission permission) throws IOException {
    Path absolutePath = makeAbsolute(path);
    List<Path> paths = new ArrayList<Path>();
    do {
      paths.add(0, absolutePath);
      absolutePath = absolutePath.getParent();
    } while (absolutePath != null);

    boolean result = true;
    for (int i = 0; i < paths.size(); i++) {
      Path p = paths.get(i);
      try {
        result &= mkdir(p);
      } catch(FileAlreadyExistsException e) {
        if (i + 1 < paths.size()) {
          throw new ParentNotDirectoryException(e.getMessage());
        }
        throw e;
      }
    }
    return result;
  }

  private boolean mkdir(Path path) throws IOException {
    Path absolutePath = makeAbsolute(path);
    INode inode = store.retrieveINode(absolutePath);
    if (inode == null) {
      store.storeINode(absolutePath, INode.DIRECTORY_INODE);
    } else if (inode.isFile()) {
      throw new FileAlreadyExistsException(String.format(
          "Can't make directory for path %s since it is a file.",
          absolutePath));
    }
    return true;
  }

  @Override
  public boolean isFile(Path path) throws IOException {
    INode inode = store.retrieveINode(makeAbsolute(path));
    if (inode == null) {
      return false;
    }
    return inode.isFile();
  }

  private INode checkFile(Path path) throws IOException {
    INode inode = store.retrieveINode(makeAbsolute(path));
    String message = String.format("No such file: '%s'", path.toString());
    if (inode == null) {
      throw new FileNotFoundException(message + " does not exist");
    }
    if (inode.isDirectory()) {
      throw new FileNotFoundException(message + " is a directory");
    }
    return inode;
  }

  @Override
  public FileStatus[] listStatus(Path f) throws IOException {
    Path absolutePath = makeAbsolute(f);
    INode inode = store.retrieveINode(absolutePath);
    if (inode == null) {
      throw new FileNotFoundException("File " + f + " does not exist.");
    }
    if (inode.isFile()) {
      return new FileStatus[] {
        new S3FileStatus(f.makeQualified(this), inode)
      };
    }
    ArrayList<FileStatus> ret = new ArrayList<FileStatus>();
    for (Path p : store.listSubPaths(absolutePath)) {
      ret.add(getFileStatus(p.makeQualified(this)));
    }
    return ret.toArray(new FileStatus[0]);
  }

  /** This optional operation is not yet supported. */
  @Override
  public FSDataOutputStream append(Path f, int bufferSize,
      Progressable progress) throws IOException {
    throw new IOException("Not supported");
  }

  /**
   * @param permission Currently ignored.
   */
  @Override
  public FSDataOutputStream create(Path file, FsPermission permission,
      boolean overwrite, int bufferSize,
      short replication, long blockSize, Progressable progress)
    throws IOException {

    INode inode = store.retrieveINode(makeAbsolute(file));
    if (inode != null) {
      if (overwrite && !inode.isDirectory()) {
        delete(file, true);
      } else {
        String message = String.format("File already exists: '%s'", file);
        if (inode.isDirectory()) {
          message = message + " is a directory";
        }
        throw new FileAlreadyExistsException(message);
      }
    } else {
      Path parent = file.getParent();
      if (parent != null) {
        if (!mkdirs(parent)) {
          throw new IOException("Mkdirs failed to create " + parent.toString());
        }
      }
    }
    return new FSDataOutputStream
        (new S3OutputStream(getConf(), store, makeAbsolute(file),
                            blockSize, progress, bufferSize),
         statistics);
  }

  @Override
  public FSDataInputStream open(Path path, int bufferSize) throws IOException {
    INode inode = checkFile(path);
    return new FSDataInputStream(new S3InputStream(getConf(), store, inode,
                                                   statistics));
  }

  @Override
  public boolean rename(Path src, Path dst) throws IOException {
    Path absoluteSrc = makeAbsolute(src);
    final String debugPreamble = "Renaming '" + src + "' to '" + dst + "' - ";
    INode srcINode = store.retrieveINode(absoluteSrc);
    boolean debugEnabled = LOG.isDebugEnabled();
    if (srcINode == null) {
      // src path doesn't exist
      if (debugEnabled) {
        LOG.debug(debugPreamble + "returning false as src does not exist");
      }
      return false;
    }

    Path absoluteDst = makeAbsolute(dst);

    //validate the parent dir of the destination
    Path dstParent = absoluteDst.getParent();
    if (dstParent != null) {
      //if the dst parent is not root, make sure it exists
      INode dstParentINode = store.retrieveINode(dstParent);
      if (dstParentINode == null) {
        // dst parent doesn't exist
        if (debugEnabled) {
          LOG.debug(debugPreamble +
                    "returning false as dst parent does not exist");
        }
        return false;
      }
      if (dstParentINode.isFile()) {
        // dst parent exists but is a file
        if (debugEnabled) {
          LOG.debug(debugPreamble +
                    "returning false as dst parent exists and is a file");
        }
        return false;
      }
    }

    //get status of source
    boolean srcIsFile = srcINode.isFile();

    INode dstINode = store.retrieveINode(absoluteDst);
    boolean destExists = dstINode != null;
    boolean destIsDir = destExists && !dstINode.isFile();
    if (srcIsFile) {

      //source is a simple file
      if (destExists) {
        if (destIsDir) {
          //outcome #1 dest exists and is dir -filename to subdir of dest
          if (debugEnabled) {
            LOG.debug(debugPreamble +
                      "copying src file under dest dir to " + absoluteDst);
          }
          absoluteDst = new Path(absoluteDst, absoluteSrc.getName());
        } else {
          //outcome #2 dest it's a file: fail iff different from src
          boolean renamingOnToSelf = absoluteSrc.equals(absoluteDst);
          if (debugEnabled) {
            LOG.debug(debugPreamble +
                      "copying file onto file, outcome is " + renamingOnToSelf);
          }
          return renamingOnToSelf;
        }
      } else {
        // #3 dest does not exist: use dest as path for rename
        if (debugEnabled) {
          LOG.debug(debugPreamble +
                    "copying file onto file");
        }
      }
    } else {
      //here the source exists and is a directory
      // outcomes (given we know the parent dir exists if we get this far)
      // #1 destination is a file: fail
      // #2 destination is a directory: create a new dir under that one
      // #3 destination doesn't exist: create a new dir with that name
      // #3 and #4 are only allowed if the dest path is not == or under src

      if (destExists) {
        if (!destIsDir) {
          // #1 destination is a file: fail
          if (debugEnabled) {
            LOG.debug(debugPreamble +
                      "returning false as src is a directory, but not dest");
          }
          return false;
        } else {
          // the destination dir exists
          // destination for rename becomes a subdir of the target name
          absoluteDst = new Path(absoluteDst, absoluteSrc.getName());
          if (debugEnabled) {
            LOG.debug(debugPreamble +
                      "copying src dir under dest dir to " + absoluteDst);
          }
        }
      }
      //the final destination directory is now know, so validate it for
      //illegal moves

      if (absoluteSrc.equals(absoluteDst)) {
        //you can't rename a directory onto itself
        if (debugEnabled) {
          LOG.debug(debugPreamble +
                    "Dest==source && isDir -failing");
        }
        return false;
      }
      if (absoluteDst.toString().startsWith(absoluteSrc.toString() + "/")) {
        //you can't move a directory under itself
        if (debugEnabled) {
          LOG.debug(debugPreamble +
                    "dst is equal to or under src dir -failing");
        }
        return false;
      }
    }
    //here the dest path is set up -so rename
    return renameRecursive(absoluteSrc, absoluteDst);
  }

  private boolean renameRecursive(Path src, Path dst) throws IOException {
    INode srcINode = store.retrieveINode(src);
    store.storeINode(dst, srcINode);
    store.deleteINode(src);
    if (srcINode.isDirectory()) {
      for (Path oldSrc : store.listDeepSubPaths(src)) {
        INode inode = store.retrieveINode(oldSrc);
        if (inode == null) {
          return false;
        }
        String oldSrcPath = oldSrc.toUri().getPath();
        String srcPath = src.toUri().getPath();
        String dstPath = dst.toUri().getPath();
        Path newDst = new Path(oldSrcPath.replaceFirst(srcPath, dstPath));
        store.storeINode(newDst, inode);
        store.deleteINode(oldSrc);
      }
    }
    return true;
  }

  @Override
  public boolean delete(Path path, boolean recursive) throws IOException {
   Path absolutePath = makeAbsolute(path);
   INode inode = store.retrieveINode(absolutePath);
   if (inode == null) {
     return false;
   }
   if (inode.isFile()) {
     store.deleteINode(absolutePath);
     for (Block block: inode.getBlocks()) {
       store.deleteBlock(block);
     }
   } else {
     FileStatus[] contents = null;
     try {
       contents = listStatus(absolutePath);
     } catch(FileNotFoundException fnfe) {
       return false;
     }

     if ((contents.length !=0) && (!recursive)) {
       throw new IOException("Directory " + path.toString()
           + " is not empty.");
     }
     for (FileStatus p:contents) {
       if (!delete(p.getPath(), recursive)) {
         return false;
       }
     }
     store.deleteINode(absolutePath);
   }
   return true;
  }

  /**
   * FileStatus for S3 file systems.
   */
  @Override
  public FileStatus getFileStatus(Path f)  throws IOException {
    INode inode = store.retrieveINode(makeAbsolute(f));
    if (inode == null) {
      throw new FileNotFoundException(f + ": No such file or directory.");
    }
    return new S3FileStatus(f.makeQualified(this), inode);
  }

  @Override
  public long getDefaultBlockSize() {
    return getConf().getLong("fs.s3.block.size", 64 * 1024 * 1024);
  }

  @Override
  public String getCanonicalServiceName() {
    // Does not support Token
    return null;
  }

  // diagnostic methods

  void dump() throws IOException {
    store.dump();
  }

  void purge() throws IOException {
    store.purge();
  }

  private static class S3FileStatus extends FileStatus {

    S3FileStatus(Path f, INode inode) throws IOException {
      super(findLength(inode), inode.isDirectory(), 1,
            findBlocksize(inode), 0, f);
    }

    private static long findLength(INode inode) {
      if (!inode.isDirectory()) {
        long length = 0L;
        for (Block block : inode.getBlocks()) {
          length += block.getLength();
        }
        return length;
      }
      return 0;
    }

    private static long findBlocksize(INode inode) {
      final Block[] ret = inode.getBlocks();
      return ret == null ? 0L : ret[0].getLength();
    }
  }
}
