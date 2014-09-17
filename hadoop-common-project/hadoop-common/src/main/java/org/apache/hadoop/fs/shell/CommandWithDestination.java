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
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NoSuchElementException;

import org.apache.hadoop.fs.CreateFlag;
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
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.AclUtil;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.IOUtils;

import static org.apache.hadoop.fs.CreateFlag.CREATE;
import static org.apache.hadoop.fs.CreateFlag.LAZY_PERSIST;

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
  private boolean lazyPersist = false;
  
  /**
   * The name of the raw xattr namespace. It would be nice to use
   * XAttr.RAW.name() but we can't reference the hadoop-hdfs project.
   */
  private static final String RAW = "raw.";

  /**
   * The name of the reserved raw directory.
   */
  private static final String RESERVED_RAW = "/.reserved/raw";

  /**
   * 
   * This method is used to enable the force(-f)  option while copying the files.
   * 
   * @param flag true/false
   */
  protected void setOverwrite(boolean flag) {
    overwrite = flag;
  }
  
  protected void setLazyPersist(boolean flag) {
    lazyPersist = flag;
  }

  protected void setVerifyChecksum(boolean flag) {
    verifyChecksum = flag;
  }
  
  protected void setWriteChecksum(boolean flag) {
    writeChecksum = flag;
  }
  
  /**
   * If true, the last modified time, last access time,
   * owner, group and permission information of the source
   * file will be preserved as far as target {@link FileSystem}
   * implementation allows.
   */
  protected void setPreserve(boolean preserve) {
    if (preserve) {
      preserve(FileAttribute.TIMESTAMPS);
      preserve(FileAttribute.OWNERSHIP);
      preserve(FileAttribute.PERMISSION);
    } else {
      preserveStatus.clear();
    }
  }
  
  protected static enum FileAttribute {
    TIMESTAMPS, OWNERSHIP, PERMISSION, ACL, XATTR;

    public static FileAttribute getAttribute(char symbol) {
      for (FileAttribute attribute : values()) {
        if (attribute.name().charAt(0) == Character.toUpperCase(symbol)) {
          return attribute;
        }
      }
      throw new NoSuchElementException("No attribute for " + symbol);
    }
  }
  
  private EnumSet<FileAttribute> preserveStatus = 
      EnumSet.noneOf(FileAttribute.class);
  
  /**
   * Checks if the input attribute should be preserved or not
   *
   * @param attribute - Attribute to check
   * @return boolean true if attribute should be preserved, false otherwise
   */
  private boolean shouldPreserve(FileAttribute attribute) {
    return preserveStatus.contains(attribute);
  }
  
  /**
   * Add file attributes that need to be preserved. This method may be
   * called multiple times to add attributes.
   *
   * @param fileAttribute - Attribute to add, one at a time
   */
  protected void preserve(FileAttribute fileAttribute) {
    for (FileAttribute attribute : preserveStatus) {
      if (attribute.equals(fileAttribute)) {
        return;
      }
    }
    preserveStatus.add(fileAttribute);
  }

  /**
   *  The last arg is expected to be a local path, if only one argument is
   *  given then the destination will be the current directory 
   *  @param args is the list of arguments
   */
  protected void getLocalDestination(LinkedList<String> args)
  throws IOException {
    String pathString = (args.size() < 2) ? Path.CUR_DIR : args.removeLast();
    try {
      dst = new PathData(new URI(pathString), getConf());
    } catch (URISyntaxException e) {
      if (Path.WINDOWS) {
        // Unlike URI, PathData knows how to parse Windows drive-letter paths.
        dst = new PathData(pathString, getConf());
      } else {
        throw new IOException("unexpected URISyntaxException", e);
      }
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
   * @param dst for the operation
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
      final boolean preserveRawXattrs =
          checkPathsForReservedRaw(src.path, dst.path);
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
      if (dst.stat.isDirectory()) {
        preserveAttributes(src, dst, preserveRawXattrs);
      }
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
  protected void copyFileToTarget(PathData src, PathData target)
      throws IOException {
    final boolean preserveRawXattrs =
        checkPathsForReservedRaw(src.path, target.path);
    src.fs.setVerifyChecksum(verifyChecksum);
    InputStream in = null;
    try {
      in = src.fs.open(src.path);
      copyStreamToTarget(in, target);
      preserveAttributes(src, target, preserveRawXattrs);
    } finally {
      IOUtils.closeStream(in);
    }
  }
  
  /**
   * Check the source and target paths to ensure that they are either both in
   * /.reserved/raw or neither in /.reserved/raw. If neither src nor target are
   * in /.reserved/raw, then return false, indicating not to preserve raw.*
   * xattrs. If both src/target are in /.reserved/raw, then return true,
   * indicating raw.* xattrs should be preserved. If only one of src/target is
   * in /.reserved/raw then throw an exception.
   *
   * @param src The source path to check. This should be a fully-qualified
   *            path, not relative.
   * @param target The target path to check. This should be a fully-qualified
   *               path, not relative.
   * @return true if raw.* xattrs should be preserved.
   * @throws PathOperationException is only one of src/target are in
   * /.reserved/raw.
   */
  private boolean checkPathsForReservedRaw(Path src, Path target)
      throws PathOperationException {
    final boolean srcIsRR = Path.getPathWithoutSchemeAndAuthority(src).
        toString().startsWith(RESERVED_RAW);
    final boolean dstIsRR = Path.getPathWithoutSchemeAndAuthority(target).
        toString().startsWith(RESERVED_RAW);
    boolean preserveRawXattrs = false;
    if (srcIsRR && !dstIsRR) {
      final String s = "' copy from '" + RESERVED_RAW + "' to non '" +
          RESERVED_RAW + "'. Either both source and target must be in '" +
          RESERVED_RAW + "' or neither.";
      throw new PathOperationException("'" + src.toString() + s);
    } else if (!srcIsRR && dstIsRR) {
      final String s = "' copy from non '" + RESERVED_RAW +"' to '" +
          RESERVED_RAW + "'. Either both source and target must be in '" +
          RESERVED_RAW + "' or neither.";
      throw new PathOperationException("'" + dst.toString() + s);
    } else if (srcIsRR && dstIsRR) {
      preserveRawXattrs = true;
    }
    return preserveRawXattrs;
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
      targetFs.writeStreamToFile(in, tempTarget, lazyPersist);
      targetFs.rename(tempTarget, target);
    } finally {
      targetFs.close(); // last ditch effort to ensure temp file is removed
    }
  }

  /**
   * Preserve the attributes of the source to the target.
   * The method calls {@link #shouldPreserve(FileAttribute)} to check what
   * attribute to preserve.
   * @param src source to preserve
   * @param target where to preserve attributes
   * @param preserveRawXAttrs true if raw.* xattrs should be preserved
   * @throws IOException if fails to preserve attributes
   */
  protected void preserveAttributes(PathData src, PathData target,
      boolean preserveRawXAttrs)
      throws IOException {
    if (shouldPreserve(FileAttribute.TIMESTAMPS)) {
      target.fs.setTimes(
          target.path,
          src.stat.getModificationTime(),
          src.stat.getAccessTime());
    }
    if (shouldPreserve(FileAttribute.OWNERSHIP)) {
      target.fs.setOwner(
          target.path,
          src.stat.getOwner(),
          src.stat.getGroup());
    }
    if (shouldPreserve(FileAttribute.PERMISSION) ||
        shouldPreserve(FileAttribute.ACL)) {
      target.fs.setPermission(
          target.path,
          src.stat.getPermission());
    }
    if (shouldPreserve(FileAttribute.ACL)) {
      FsPermission perm = src.stat.getPermission();
      if (perm.getAclBit()) {
        List<AclEntry> srcEntries =
            src.fs.getAclStatus(src.path).getEntries();
        List<AclEntry> srcFullEntries =
            AclUtil.getAclFromPermAndEntries(perm, srcEntries);
        target.fs.setAcl(target.path, srcFullEntries);
      }
    }
    final boolean preserveXAttrs = shouldPreserve(FileAttribute.XATTR);
    if (preserveXAttrs || preserveRawXAttrs) {
      Map<String, byte[]> srcXAttrs = src.fs.getXAttrs(src.path);
      if (srcXAttrs != null) {
        Iterator<Entry<String, byte[]>> iter = srcXAttrs.entrySet().iterator();
        while (iter.hasNext()) {
          Entry<String, byte[]> entry = iter.next();
          final String xattrName = entry.getKey();
          if (xattrName.startsWith(RAW) || preserveXAttrs) {
            target.fs.setXAttr(target.path, entry.getKey(), entry.getValue());
          }
        }
      }
    }
  }

  // Helper filter filesystem that registers created files as temp files to
  // be deleted on exit unless successfully renamed
  private static class TargetFileSystem extends FilterFileSystem {
    TargetFileSystem(FileSystem fs) {
      super(fs);
    }

    void writeStreamToFile(InputStream in, PathData target,
                           boolean lazyPersist) throws IOException {
      FSDataOutputStream out = null;
      try {
        out = create(target, lazyPersist);
        IOUtils.copyBytes(in, out, getConf(), true);
      } finally {
        IOUtils.closeStream(out); // just in case copyBytes didn't
      }
    }
    
    // tag created files as temp files
    FSDataOutputStream create(PathData item, boolean lazyPersist)
        throws IOException {
      try {
        if (lazyPersist) {
          EnumSet<CreateFlag> createFlags = EnumSet.of(CREATE, LAZY_PERSIST);
          return create(item.path,
                        FsPermission.getFileDefault().applyUMask(
                            FsPermission.getUMask(getConf())),
                        createFlags,
                        getConf().getInt("io.file.buffer.size", 4096),
                        lazyPersist ? 1 : getDefaultReplication(item.path),
                        getDefaultBlockSize(),
                        null,
                        null);
        } else {
          return create(item.path, true);
        }
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
