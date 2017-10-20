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

package org.apache.hadoop.yarn.util;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URISyntaxException;
import java.security.PrivilegedExceptionAction;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.regex.Pattern;

import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.LimitedPrivate;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Options.Rename;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.RunJar;
import org.apache.hadoop.util.Shell;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.util.concurrent.Futures;

/**
 * Download a single URL to the local disk.
 *
 */
@LimitedPrivate({"YARN", "MapReduce"})
public class FSDownload implements Callable<Path> {

  private static final Log LOG = LogFactory.getLog(FSDownload.class);

  private FileContext files;
  private final UserGroupInformation userUgi;
  private Configuration conf;
  private LocalResource resource;
  private final LoadingCache<Path,Future<FileStatus>> statCache;
  
  /** The local FS dir path under which this resource is to be localized to */
  private Path destDirPath;

  private static final FsPermission cachePerms = new FsPermission(
      (short) 0755);
  static final FsPermission PUBLIC_FILE_PERMS = new FsPermission((short) 0555);
  static final FsPermission PRIVATE_FILE_PERMS = new FsPermission(
      (short) 0500);
  static final FsPermission PUBLIC_DIR_PERMS = new FsPermission((short) 0755);
  static final FsPermission PRIVATE_DIR_PERMS = new FsPermission((short) 0700);


  public FSDownload(FileContext files, UserGroupInformation ugi, Configuration conf,
      Path destDirPath, LocalResource resource) {
    this(files, ugi, conf, destDirPath, resource, null);
  }

  public FSDownload(FileContext files, UserGroupInformation ugi, Configuration conf,
      Path destDirPath, LocalResource resource,
      LoadingCache<Path,Future<FileStatus>> statCache) {
    this.conf = conf;
    this.destDirPath = destDirPath;
    this.files = files;
    this.userUgi = ugi;
    this.resource = resource;
    this.statCache = statCache;
  }

  LocalResource getResource() {
    return resource;
  }

  private void createDir(Path path, FsPermission perm) throws IOException {
    files.mkdir(path, perm, false);
    if (!perm.equals(files.getUMask().applyUMask(perm))) {
      files.setPermission(path, perm);
    }
  }

  /**
   * Creates the cache loader for the status loading cache. This should be used
   * to create an instance of the status cache that is passed into the
   * FSDownload constructor.
   */
  public static CacheLoader<Path,Future<FileStatus>>
      createStatusCacheLoader(final Configuration conf) {
    return new CacheLoader<Path,Future<FileStatus>>() {
      public Future<FileStatus> load(Path path) {
        try {
          FileSystem fs = path.getFileSystem(conf);
          return Futures.immediateFuture(fs.getFileStatus(path));
        } catch (Throwable th) {
          // report failures so it can be memoized
          return Futures.immediateFailedFuture(th);
        }
      }
    };
  }

  /**
   * Returns a boolean to denote whether a cache file is visible to all (public)
   * or not
   *
   * @return true if the path in the current path is visible to all, false
   * otherwise
   */
  @Private
  public static boolean isPublic(FileSystem fs, Path current, FileStatus sStat,
      LoadingCache<Path,Future<FileStatus>> statCache) throws IOException {
    current = fs.makeQualified(current);
    //the leaf level file should be readable by others
    if (!checkPublicPermsForAll(fs, sStat, FsAction.READ_EXECUTE, FsAction.READ)) {
      return false;
    }

    if (Shell.WINDOWS && fs instanceof LocalFileSystem) {
      // Relax the requirement for public cache on LFS on Windows since default
      // permissions are "700" all the way up to the drive letter. In this
      // model, the only requirement for a user is to give EVERYONE group
      // permission on the file and the file will be considered public.
      // This code path is only hit when fs.default.name is file:/// (mainly
      // in tests).
      return true;
    }
    return ancestorsHaveExecutePermissions(fs, current.getParent(), statCache);
  }

  private static boolean checkPublicPermsForAll(FileSystem fs, 
        FileStatus status, FsAction dir, FsAction file) 
    throws IOException {
    FsPermission perms = status.getPermission();
    FsAction otherAction = perms.getOtherAction();
    if (status.isDirectory()) {
      if (!otherAction.implies(dir)) {
        return false;
      }
      
      for (FileStatus child : fs.listStatus(status.getPath())) {
        if(!checkPublicPermsForAll(fs, child, dir, file)) {
          return false;
        }
      }
      return true;
    }
    return (otherAction.implies(file));
  }

  /**
   * Returns true if all ancestors of the specified path have the 'execute'
   * permission set for all users (i.e. that other users can traverse
   * the directory hierarchy to the given path)
   */
  @VisibleForTesting
  static boolean ancestorsHaveExecutePermissions(FileSystem fs,
      Path path, LoadingCache<Path,Future<FileStatus>> statCache)
      throws IOException {
    Path current = path;
    while (current != null) {
      //the subdirs in the path should have execute permissions for others
      if (!checkPermissionOfOther(fs, current, FsAction.EXECUTE, statCache)) {
        return false;
      }
      current = current.getParent();
    }
    return true;
  }

  /**
   * Checks for a given path whether the Other permissions on it 
   * imply the permission in the passed FsAction
   * @param fs
   * @param path
   * @param action
   * @return true if the path in the uri is visible to all, false otherwise
   * @throws IOException
   */
  private static boolean checkPermissionOfOther(FileSystem fs, Path path,
      FsAction action, LoadingCache<Path,Future<FileStatus>> statCache)
      throws IOException {
    FileStatus status = getFileStatus(fs, path, statCache);
    FsPermission perms = status.getPermission();
    FsAction otherAction = perms.getOtherAction();
    return otherAction.implies(action);
  }

  /**
   * Obtains the file status, first by checking the stat cache if it is
   * available, and then by getting it explicitly from the filesystem. If we got
   * the file status from the filesystem, it is added to the stat cache.
   *
   * The stat cache is expected to be managed by callers who provided it to
   * FSDownload.
   */
  private static FileStatus getFileStatus(final FileSystem fs, final Path path,
      LoadingCache<Path,Future<FileStatus>> statCache) throws IOException {
    // if the stat cache does not exist, simply query the filesystem
    if (statCache == null) {
      return fs.getFileStatus(path);
    }

    try {
      // get or load it from the cache
      return statCache.get(path).get();
    } catch (ExecutionException e) {
      Throwable cause = e.getCause();
      // the underlying exception should normally be IOException
      if (cause instanceof IOException) {
        throw (IOException)cause;
      } else {
        throw new IOException(cause);
      }
    } catch (InterruptedException e) { // should not happen
      Thread.currentThread().interrupt();
      throw new IOException(e);
    }
  }

  private Path copy(Path sCopy, Path dstdir) throws IOException {
    FileSystem sourceFs = sCopy.getFileSystem(conf);
    Path dCopy = new Path(dstdir, "tmp_"+sCopy.getName());
    FileStatus sStat = sourceFs.getFileStatus(sCopy);
    if (sStat.getModificationTime() != resource.getTimestamp()) {
      throw new IOException("Resource " + sCopy +
          " changed on src filesystem (expected " + resource.getTimestamp() +
          ", was " + sStat.getModificationTime());
    }
    if (resource.getVisibility() == LocalResourceVisibility.PUBLIC) {
      if (!isPublic(sourceFs, sCopy, sStat, statCache)) {
        throw new IOException("Resource " + sCopy +
            " is not publicly accessible and as such cannot be part of the" +
            " public cache.");
      }
    }

    FileUtil.copy(sourceFs, sStat, FileSystem.getLocal(conf), dCopy, false,
        true, conf);
    return dCopy;
  }

  private long unpack(File localrsrc, File dst) throws IOException {
    switch (resource.getType()) {
    case ARCHIVE: {
      String lowerDst = StringUtils.toLowerCase(dst.getName());
      if (lowerDst.endsWith(".jar")) {
        RunJar.unJar(localrsrc, dst);
      } else if (lowerDst.endsWith(".zip")) {
        FileUtil.unZip(localrsrc, dst);
      } else if (lowerDst.endsWith(".tar.gz") ||
                 lowerDst.endsWith(".tgz") ||
                 lowerDst.endsWith(".tar")) {
        FileUtil.unTar(localrsrc, dst);
      } else {
        LOG.warn("Cannot unpack " + localrsrc);
        if (!localrsrc.renameTo(dst)) {
            throw new IOException("Unable to rename file: [" + localrsrc
              + "] to [" + dst + "]");
        }
      }
    }
    break;
    case PATTERN: {
      String lowerDst = StringUtils.toLowerCase(dst.getName());
      if (lowerDst.endsWith(".jar")) {
        String p = resource.getPattern();
        RunJar.unJar(localrsrc, dst,
            p == null ? RunJar.MATCH_ANY : Pattern.compile(p));
        File newDst = new File(dst, dst.getName());
        if (!dst.exists() && !dst.mkdir()) {
          throw new IOException("Unable to create directory: [" + dst + "]");
        }
        if (!localrsrc.renameTo(newDst)) {
          throw new IOException("Unable to rename file: [" + localrsrc
              + "] to [" + newDst + "]");
        }
      } else if (lowerDst.endsWith(".zip")) {
        LOG.warn("Treating [" + localrsrc + "] as an archive even though it " +
        		"was specified as PATTERN");
        FileUtil.unZip(localrsrc, dst);
      } else if (lowerDst.endsWith(".tar.gz") ||
                 lowerDst.endsWith(".tgz") ||
                 lowerDst.endsWith(".tar")) {
        LOG.warn("Treating [" + localrsrc + "] as an archive even though it " +
        "was specified as PATTERN");
        FileUtil.unTar(localrsrc, dst);
      } else {
        LOG.warn("Cannot unpack " + localrsrc);
        if (!localrsrc.renameTo(dst)) {
          throw new IOException("Unable to rename file: [" + localrsrc
              + "] to [" + dst + "]");
        }
      }
    }
    break;
    case FILE:
    default:
      if (!localrsrc.renameTo(dst)) {
        throw new IOException("Unable to rename file: [" + localrsrc
          + "] to [" + dst + "]");
      }
      break;
    }
    if(localrsrc.isFile()){
      try {
        files.delete(new Path(localrsrc.toString()), false);
      } catch (IOException ignore) {
      }
    }
    return 0;
    // TODO Should calculate here before returning
    //return FileUtil.getDU(destDir);
  }

  @Override
  public Path call() throws Exception {
    final Path sCopy;
    try {
      sCopy = resource.getResource().toPath();
    } catch (URISyntaxException e) {
      throw new IOException("Invalid resource", e);
    }

    if (LOG.isDebugEnabled()) {
      LOG.debug("Starting to download " + sCopy);
    }

    createDir(destDirPath, cachePerms);
    final Path dst_work = new Path(destDirPath + "_tmp");
    createDir(dst_work, cachePerms);
    Path dFinal = files.makeQualified(new Path(dst_work, sCopy.getName()));
    try {
      Path dTmp = null == userUgi ? files.makeQualified(copy(sCopy, dst_work))
          : userUgi.doAs(new PrivilegedExceptionAction<Path>() {
            public Path run() throws Exception {
              return files.makeQualified(copy(sCopy, dst_work));
            };
          });
      unpack(new File(dTmp.toUri()), new File(dFinal.toUri()));
      changePermissions(dFinal.getFileSystem(conf), dFinal);
      files.rename(dst_work, destDirPath, Rename.OVERWRITE);

      if (LOG.isDebugEnabled()) {
        LOG.debug("File has been downloaded to " +
            new Path(destDirPath, sCopy.getName()));
      }
    } catch (Exception e) {
      try {
        files.delete(destDirPath, true);
      } catch (IOException ignore) {
      }
      throw e;
    } finally {
      try {
        files.delete(dst_work, true);
      } catch (FileNotFoundException ignore) {
      }
      conf = null;
      resource = null;
    }
    return files.makeQualified(new Path(destDirPath, sCopy.getName()));
  }

  /**
   * Recursively change permissions of all files/dirs on path based 
   * on resource visibility.
   * Change to 755 or 700 for dirs, 555 or 500 for files.
   * @param fs FileSystem
   * @param path Path to modify perms for
   * @throws IOException
   * @throws InterruptedException 
   */
  private void changePermissions(FileSystem fs, final Path path)
      throws IOException, InterruptedException {
    File f = new File(path.toUri());
    if (FileUtils.isSymlink(f)) {
      // avoid following symlinks when changing permissions
      return;
    }
    boolean isDir = f.isDirectory();
    FsPermission perm = cachePerms;
    // set public perms as 755 or 555 based on dir or file
    if (resource.getVisibility() == LocalResourceVisibility.PUBLIC) {
      perm = isDir ? PUBLIC_DIR_PERMS : PUBLIC_FILE_PERMS;
    }
    // set private perms as 700 or 500
    else {
      // PRIVATE:
      // APPLICATION:
      perm = isDir ? PRIVATE_DIR_PERMS : PRIVATE_FILE_PERMS;
    }

    if (LOG.isDebugEnabled()) {
      LOG.debug("Changing permissions for path " + path + " to perm " + perm);
    }

    final FsPermission fPerm = perm;
    if (null == userUgi) {
      files.setPermission(path, perm);
    }
    else {
      userUgi.doAs(new PrivilegedExceptionAction<Void>() {
        public Void run() throws Exception {
          files.setPermission(path, fPerm);
          return null;
        }
      });
    }
    if (isDir) {
      FileStatus[] statuses = fs.listStatus(path);
      for (FileStatus status : statuses) {
        changePermissions(fs, status.getPath());
      }
    }
  }

}
