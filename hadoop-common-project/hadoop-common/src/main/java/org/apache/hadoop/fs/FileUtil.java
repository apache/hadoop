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

package org.apache.hadoop.fs;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.URI;
import java.net.UnknownHostException;
import java.nio.file.AccessDeniedException;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.Map;
import java.util.jar.Attributes;
import java.util.jar.JarOutputStream;
import java.util.jar.Manifest;
import java.util.zip.GZIPInputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

import org.apache.commons.collections.map.CaseInsensitiveMap;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.nativeio.NativeIO;
import org.apache.hadoop.util.Shell;
import org.apache.hadoop.util.Shell.ShellCommandExecutor;
import org.apache.hadoop.util.StringUtils;

/**
 * A collection of file-processing util methods
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class FileUtil {

  private static final Log LOG = LogFactory.getLog(FileUtil.class);

  /* The error code is defined in winutils to indicate insufficient
   * privilege to create symbolic links. This value need to keep in
   * sync with the constant of the same name in:
   * "src\winutils\common.h"
   * */
  public static final int SYMLINK_NO_PRIVILEGE = 2;

  /**
   * convert an array of FileStatus to an array of Path
   *
   * @param stats
   *          an array of FileStatus objects
   * @return an array of paths corresponding to the input
   */
  public static Path[] stat2Paths(FileStatus[] stats) {
    if (stats == null)
      return null;
    Path[] ret = new Path[stats.length];
    for (int i = 0; i < stats.length; ++i) {
      ret[i] = stats[i].getPath();
    }
    return ret;
  }

  /**
   * convert an array of FileStatus to an array of Path.
   * If stats if null, return path
   * @param stats
   *          an array of FileStatus objects
   * @param path
   *          default path to return in stats is null
   * @return an array of paths corresponding to the input
   */
  public static Path[] stat2Paths(FileStatus[] stats, Path path) {
    if (stats == null)
      return new Path[]{path};
    else
      return stat2Paths(stats);
  }

  /**
   * Register all files recursively to be deleted on exit.
   * @param file File/directory to be deleted
   */
  public static void fullyDeleteOnExit(final File file) {
    file.deleteOnExit();
    if (file.isDirectory()) {
      File[] files = file.listFiles();
      for (File child : files) {
        fullyDeleteOnExit(child);
      }
    }
  }

  /**
   * Delete a directory and all its contents.  If
   * we return false, the directory may be partially-deleted.
   * (1) If dir is symlink to a file, the symlink is deleted. The file pointed
   *     to by the symlink is not deleted.
   * (2) If dir is symlink to a directory, symlink is deleted. The directory
   *     pointed to by symlink is not deleted.
   * (3) If dir is a normal file, it is deleted.
   * (4) If dir is a normal directory, then dir and all its contents recursively
   *     are deleted.
   */
  public static boolean fullyDelete(final File dir) {
    return fullyDelete(dir, false);
  }

  /**
   * Delete a directory and all its contents.  If
   * we return false, the directory may be partially-deleted.
   * (1) If dir is symlink to a file, the symlink is deleted. The file pointed
   *     to by the symlink is not deleted.
   * (2) If dir is symlink to a directory, symlink is deleted. The directory
   *     pointed to by symlink is not deleted.
   * (3) If dir is a normal file, it is deleted.
   * (4) If dir is a normal directory, then dir and all its contents recursively
   *     are deleted.
   * @param dir the file or directory to be deleted
   * @param tryGrantPermissions true if permissions should be modified to delete a file.
   * @return true on success false on failure.
   */
  public static boolean fullyDelete(final File dir, boolean tryGrantPermissions) {
    if (tryGrantPermissions) {
      // try to chmod +rwx the parent folder of the 'dir':
      File parent = dir.getParentFile();
      grantPermissions(parent);
    }
    if (deleteImpl(dir, false)) {
      // dir is (a) normal file, (b) symlink to a file, (c) empty directory or
      // (d) symlink to a directory
      return true;
    }
    // handle nonempty directory deletion
    if (!fullyDeleteContents(dir, tryGrantPermissions)) {
      return false;
    }
    return deleteImpl(dir, true);
  }

  /**
   * Returns the target of the given symlink. Returns the empty string if
   * the given path does not refer to a symlink or there is an error
   * accessing the symlink.
   * @param f File representing the symbolic link.
   * @return The target of the symbolic link, empty string on error or if not
   *         a symlink.
   */
  public static String readLink(File f) {
    /* NB: Use readSymbolicLink in java.nio.file.Path once available. Could
     * use getCanonicalPath in File to get the target of the symlink but that
     * does not indicate if the given path refers to a symlink.
     */
    try {
      return Shell.execCommand(
          Shell.getReadlinkCommand(f.toString())).trim();
    } catch (IOException x) {
      return "";
    }
  }

  /*
   * Pure-Java implementation of "chmod +rwx f".
   */
  private static void grantPermissions(final File f) {
      FileUtil.setExecutable(f, true);
      FileUtil.setReadable(f, true);
      FileUtil.setWritable(f, true);
  }

  private static boolean deleteImpl(final File f, final boolean doLog) {
    if (f == null) {
      LOG.warn("null file argument.");
      return false;
    }
    final boolean wasDeleted = f.delete();
    if (wasDeleted) {
      return true;
    }
    final boolean ex = f.exists();
    if (doLog && ex) {
      LOG.warn("Failed to delete file or dir ["
          + f.getAbsolutePath() + "]: it still exists.");
    }
    return !ex;
  }

  /**
   * Delete the contents of a directory, not the directory itself.  If
   * we return false, the directory may be partially-deleted.
   * If dir is a symlink to a directory, all the contents of the actual
   * directory pointed to by dir will be deleted.
   */
  public static boolean fullyDeleteContents(final File dir) {
    return fullyDeleteContents(dir, false);
  }

  /**
   * Delete the contents of a directory, not the directory itself.  If
   * we return false, the directory may be partially-deleted.
   * If dir is a symlink to a directory, all the contents of the actual
   * directory pointed to by dir will be deleted.
   * @param tryGrantPermissions if 'true', try grant +rwx permissions to this
   * and all the underlying directories before trying to delete their contents.
   */
  public static boolean fullyDeleteContents(final File dir, final boolean tryGrantPermissions) {
    if (tryGrantPermissions) {
      // to be able to list the dir and delete files from it
      // we must grant the dir rwx permissions:
      grantPermissions(dir);
    }
    boolean deletionSucceeded = true;
    final File[] contents = dir.listFiles();
    if (contents != null) {
      for (int i = 0; i < contents.length; i++) {
        if (contents[i].isFile()) {
          if (!deleteImpl(contents[i], true)) {// normal file or symlink to another file
            deletionSucceeded = false;
            continue; // continue deletion of other files/dirs under dir
          }
        } else {
          // Either directory or symlink to another directory.
          // Try deleting the directory as this might be a symlink
          boolean b = false;
          b = deleteImpl(contents[i], false);
          if (b){
            //this was indeed a symlink or an empty directory
            continue;
          }
          // if not an empty directory or symlink let
          // fullydelete handle it.
          if (!fullyDelete(contents[i], tryGrantPermissions)) {
            deletionSucceeded = false;
            // continue deletion of other files/dirs under dir
          }
        }
      }
    }
    return deletionSucceeded;
  }

  /**
   * Recursively delete a directory.
   *
   * @param fs {@link FileSystem} on which the path is present
   * @param dir directory to recursively delete
   * @throws IOException
   * @deprecated Use {@link FileSystem#delete(Path, boolean)}
   */
  @Deprecated
  public static void fullyDelete(FileSystem fs, Path dir)
  throws IOException {
    fs.delete(dir, true);
  }

  //
  // If the destination is a subdirectory of the source, then
  // generate exception
  //
  private static void checkDependencies(FileSystem srcFS,
                                        Path src,
                                        FileSystem dstFS,
                                        Path dst)
                                        throws IOException {
    if (srcFS == dstFS) {
      String srcq = src.makeQualified(srcFS).toString() + Path.SEPARATOR;
      String dstq = dst.makeQualified(dstFS).toString() + Path.SEPARATOR;
      if (dstq.startsWith(srcq)) {
        if (srcq.length() == dstq.length()) {
          throw new IOException("Cannot copy " + src + " to itself.");
        } else {
          throw new IOException("Cannot copy " + src + " to its subdirectory " +
                                dst);
        }
      }
    }
  }

  /** Copy files between FileSystems. */
  public static boolean copy(FileSystem srcFS, Path src,
                             FileSystem dstFS, Path dst,
                             boolean deleteSource,
                             Configuration conf) throws IOException {
    return copy(srcFS, src, dstFS, dst, deleteSource, true, conf);
  }

  public static boolean copy(FileSystem srcFS, Path[] srcs,
                             FileSystem dstFS, Path dst,
                             boolean deleteSource,
                             boolean overwrite, Configuration conf)
                             throws IOException {
    boolean gotException = false;
    boolean returnVal = true;
    StringBuilder exceptions = new StringBuilder();

    if (srcs.length == 1)
      return copy(srcFS, srcs[0], dstFS, dst, deleteSource, overwrite, conf);

    // Check if dest is directory
    try {
      FileStatus sdst = dstFS.getFileStatus(dst);
      if (!sdst.isDirectory())
        throw new IOException("copying multiple files, but last argument `" +
                              dst + "' is not a directory");
    } catch (FileNotFoundException e) {
      throw new IOException(
          "`" + dst + "': specified destination directory " +
              "does not exist", e);
    }

    for (Path src : srcs) {
      try {
        if (!copy(srcFS, src, dstFS, dst, deleteSource, overwrite, conf))
          returnVal = false;
      } catch (IOException e) {
        gotException = true;
        exceptions.append(e.getMessage());
        exceptions.append("\n");
      }
    }
    if (gotException) {
      throw new IOException(exceptions.toString());
    }
    return returnVal;
  }

  /** Copy files between FileSystems. */
  public static boolean copy(FileSystem srcFS, Path src,
                             FileSystem dstFS, Path dst,
                             boolean deleteSource,
                             boolean overwrite,
                             Configuration conf) throws IOException {
    FileStatus fileStatus = srcFS.getFileStatus(src);
    return copy(srcFS, fileStatus, dstFS, dst, deleteSource, overwrite, conf);
  }

  /** Copy files between FileSystems. */
  public static boolean copy(FileSystem srcFS, FileStatus srcStatus,
                             FileSystem dstFS, Path dst,
                             boolean deleteSource,
                             boolean overwrite,
                             Configuration conf) throws IOException {
    Path src = srcStatus.getPath();
    dst = checkDest(src.getName(), dstFS, dst, overwrite);
    if (srcStatus.isDirectory()) {
      checkDependencies(srcFS, src, dstFS, dst);
      if (!dstFS.mkdirs(dst)) {
        return false;
      }
      FileStatus contents[] = srcFS.listStatus(src);
      for (int i = 0; i < contents.length; i++) {
        copy(srcFS, contents[i], dstFS,
             new Path(dst, contents[i].getPath().getName()),
             deleteSource, overwrite, conf);
      }
    } else {
      InputStream in=null;
      OutputStream out = null;
      try {
        in = srcFS.open(src);
        out = dstFS.create(dst, overwrite);
        IOUtils.copyBytes(in, out, conf, true);
      } catch (IOException e) {
        IOUtils.closeStream(out);
        IOUtils.closeStream(in);
        throw e;
      }
    }
    if (deleteSource) {
      return srcFS.delete(src, true);
    } else {
      return true;
    }

  }

  /** Copy local files to a FileSystem. */
  public static boolean copy(File src,
                             FileSystem dstFS, Path dst,
                             boolean deleteSource,
                             Configuration conf) throws IOException {
    dst = checkDest(src.getName(), dstFS, dst, false);

    if (src.isDirectory()) {
      if (!dstFS.mkdirs(dst)) {
        return false;
      }
      File contents[] = listFiles(src);
      for (int i = 0; i < contents.length; i++) {
        copy(contents[i], dstFS, new Path(dst, contents[i].getName()),
             deleteSource, conf);
      }
    } else if (src.isFile()) {
      InputStream in = null;
      OutputStream out =null;
      try {
        in = new FileInputStream(src);
        out = dstFS.create(dst);
        IOUtils.copyBytes(in, out, conf);
      } catch (IOException e) {
        IOUtils.closeStream( out );
        IOUtils.closeStream( in );
        throw e;
      }
    } else if (!src.canRead()) {
      throw new IOException(src.toString() +
                            ": Permission denied");

    } else {
      throw new IOException(src.toString() +
                            ": No such file or directory");
    }
    if (deleteSource) {
      return FileUtil.fullyDelete(src);
    } else {
      return true;
    }
  }

  /** Copy FileSystem files to local files. */
  public static boolean copy(FileSystem srcFS, Path src,
                             File dst, boolean deleteSource,
                             Configuration conf) throws IOException {
    FileStatus filestatus = srcFS.getFileStatus(src);
    return copy(srcFS, filestatus, dst, deleteSource, conf);
  }

  /** Copy FileSystem files to local files. */
  private static boolean copy(FileSystem srcFS, FileStatus srcStatus,
                              File dst, boolean deleteSource,
                              Configuration conf) throws IOException {
    Path src = srcStatus.getPath();
    if (srcStatus.isDirectory()) {
      if (!dst.mkdirs()) {
        return false;
      }
      FileStatus contents[] = srcFS.listStatus(src);
      for (int i = 0; i < contents.length; i++) {
        copy(srcFS, contents[i],
             new File(dst, contents[i].getPath().getName()),
             deleteSource, conf);
      }
    } else {
      InputStream in = srcFS.open(src);
      IOUtils.copyBytes(in, new FileOutputStream(dst), conf);
    }
    if (deleteSource) {
      return srcFS.delete(src, true);
    } else {
      return true;
    }
  }

  private static Path checkDest(String srcName, FileSystem dstFS, Path dst,
      boolean overwrite) throws IOException {
    FileStatus sdst;
    try {
      sdst = dstFS.getFileStatus(dst);
    } catch (FileNotFoundException e) {
      sdst = null;
    }
    if (null != sdst) {
      if (sdst.isDirectory()) {
        if (null == srcName) {
          throw new IOException("Target " + dst + " is a directory");
        }
        return checkDest(null, dstFS, new Path(dst, srcName), overwrite);
      } else if (!overwrite) {
        throw new IOException("Target " + dst + " already exists");
      }
    }
    return dst;
  }

  /**
   * Convert a os-native filename to a path that works for the shell.
   * @param filename The filename to convert
   * @return The unix pathname
   * @throws IOException on windows, there can be problems with the subprocess
   */
  public static String makeShellPath(String filename) throws IOException {
    return filename;
  }

  /**
   * Convert a os-native filename to a path that works for the shell.
   * @param file The filename to convert
   * @return The unix pathname
   * @throws IOException on windows, there can be problems with the subprocess
   */
  public static String makeShellPath(File file) throws IOException {
    return makeShellPath(file, false);
  }

  /**
   * Convert a os-native filename to a path that works for the shell.
   * @param file The filename to convert
   * @param makeCanonicalPath
   *          Whether to make canonical path for the file passed
   * @return The unix pathname
   * @throws IOException on windows, there can be problems with the subprocess
   */
  public static String makeShellPath(File file, boolean makeCanonicalPath)
  throws IOException {
    if (makeCanonicalPath) {
      return makeShellPath(file.getCanonicalPath());
    } else {
      return makeShellPath(file.toString());
    }
  }

  /**
   * Takes an input dir and returns the du on that local directory. Very basic
   * implementation.
   *
   * @param dir
   *          The input dir to get the disk space of this local dir
   * @return The total disk space of the input local directory
   */
  public static long getDU(File dir) {
    long size = 0;
    if (!dir.exists())
      return 0;
    if (!dir.isDirectory()) {
      return dir.length();
    } else {
      File[] allFiles = dir.listFiles();
      if(allFiles != null) {
         for (int i = 0; i < allFiles.length; i++) {
           boolean isSymLink;
           try {
             isSymLink = org.apache.commons.io.FileUtils.isSymlink(allFiles[i]);
           } catch(IOException ioe) {
             isSymLink = true;
           }
           if(!isSymLink) {
             size += getDU(allFiles[i]);
           }
         }
      }
      return size;
    }
  }

  /**
   * Given a File input it will unzip the file in a the unzip directory
   * passed as the second parameter
   * @param inFile The zip file as input
   * @param unzipDir The unzip directory where to unzip the zip file.
   * @throws IOException
   */
  public static void unZip(File inFile, File unzipDir) throws IOException {
    Enumeration<? extends ZipEntry> entries;
    ZipFile zipFile = new ZipFile(inFile);

    try {
      entries = zipFile.entries();
      while (entries.hasMoreElements()) {
        ZipEntry entry = entries.nextElement();
        if (!entry.isDirectory()) {
          InputStream in = zipFile.getInputStream(entry);
          try {
            File file = new File(unzipDir, entry.getName());
            if (!file.getParentFile().mkdirs()) {
              if (!file.getParentFile().isDirectory()) {
                throw new IOException("Mkdirs failed to create " +
                                      file.getParentFile().toString());
              }
            }
            OutputStream out = new FileOutputStream(file);
            try {
              byte[] buffer = new byte[8192];
              int i;
              while ((i = in.read(buffer)) != -1) {
                out.write(buffer, 0, i);
              }
            } finally {
              out.close();
            }
          } finally {
            in.close();
          }
        }
      }
    } finally {
      zipFile.close();
    }
  }

  /**
   * Given a Tar File as input it will untar the file in a the untar directory
   * passed as the second parameter
   *
   * This utility will untar ".tar" files and ".tar.gz","tgz" files.
   *
   * @param inFile The tar file as input.
   * @param untarDir The untar directory where to untar the tar file.
   * @throws IOException
   */
  public static void unTar(File inFile, File untarDir) throws IOException {
    if (!untarDir.mkdirs()) {
      if (!untarDir.isDirectory()) {
        throw new IOException("Mkdirs failed to create " + untarDir);
      }
    }

    boolean gzipped = inFile.toString().endsWith("gz");
    if(Shell.WINDOWS) {
      // Tar is not native to Windows. Use simple Java based implementation for
      // tests and simple tar archives
      unTarUsingJava(inFile, untarDir, gzipped);
    }
    else {
      // spawn tar utility to untar archive for full fledged unix behavior such
      // as resolving symlinks in tar archives
      unTarUsingTar(inFile, untarDir, gzipped);
    }
  }

  private static void unTarUsingTar(File inFile, File untarDir,
      boolean gzipped) throws IOException {
    StringBuffer untarCommand = new StringBuffer();
    if (gzipped) {
      untarCommand.append(" gzip -dc '");
      untarCommand.append(FileUtil.makeShellPath(inFile));
      untarCommand.append("' | (");
    }
    untarCommand.append("cd '");
    untarCommand.append(FileUtil.makeShellPath(untarDir));
    untarCommand.append("' ; ");
    untarCommand.append("tar -xf ");

    if (gzipped) {
      untarCommand.append(" -)");
    } else {
      untarCommand.append(FileUtil.makeShellPath(inFile));
    }
    String[] shellCmd = { "bash", "-c", untarCommand.toString() };
    ShellCommandExecutor shexec = new ShellCommandExecutor(shellCmd);
    shexec.execute();
    int exitcode = shexec.getExitCode();
    if (exitcode != 0) {
      throw new IOException("Error untarring file " + inFile +
                  ". Tar process exited with exit code " + exitcode);
    }
  }

  private static void unTarUsingJava(File inFile, File untarDir,
      boolean gzipped) throws IOException {
    InputStream inputStream = null;
    TarArchiveInputStream tis = null;
    try {
      if (gzipped) {
        inputStream = new BufferedInputStream(new GZIPInputStream(
            new FileInputStream(inFile)));
      } else {
        inputStream = new BufferedInputStream(new FileInputStream(inFile));
      }

      tis = new TarArchiveInputStream(inputStream);

      for (TarArchiveEntry entry = tis.getNextTarEntry(); entry != null;) {
        unpackEntries(tis, entry, untarDir);
        entry = tis.getNextTarEntry();
      }
    } finally {
      IOUtils.cleanup(LOG, tis, inputStream);
    }
  }

  private static void unpackEntries(TarArchiveInputStream tis,
      TarArchiveEntry entry, File outputDir) throws IOException {
    if (entry.isDirectory()) {
      File subDir = new File(outputDir, entry.getName());
      if (!subDir.mkdirs() && !subDir.isDirectory()) {
        throw new IOException("Mkdirs failed to create tar internal dir "
            + outputDir);
      }

      for (TarArchiveEntry e : entry.getDirectoryEntries()) {
        unpackEntries(tis, e, subDir);
      }

      return;
    }

    File outputFile = new File(outputDir, entry.getName());
    if (!outputFile.getParentFile().exists()) {
      if (!outputFile.getParentFile().mkdirs()) {
        throw new IOException("Mkdirs failed to create tar internal dir "
            + outputDir);
      }
    }

    if (entry.isLink()) {
      File src = new File(outputDir, entry.getLinkName());
      HardLink.createHardLink(src, outputFile);
      return;
    }

    int count;
    byte data[] = new byte[2048];
    try (BufferedOutputStream outputStream = new BufferedOutputStream(
        new FileOutputStream(outputFile));) {

      while ((count = tis.read(data)) != -1) {
        outputStream.write(data, 0, count);
      }

      outputStream.flush();
    }
  }

  /**
   * Class for creating hardlinks.
   * Supports Unix, WindXP.
   * @deprecated Use {@link org.apache.hadoop.fs.HardLink}
   */
  @Deprecated
  public static class HardLink extends org.apache.hadoop.fs.HardLink {
    // This is a stub to assist with coordinated change between
    // COMMON and HDFS projects.  It will be removed after the
    // corresponding change is committed to HDFS.
  }

  /**
   * Create a soft link between a src and destination
   * only on a local disk. HDFS does not support this.
   * On Windows, when symlink creation fails due to security
   * setting, we will log a warning. The return code in this
   * case is 2.
   *
   * @param target the target for symlink
   * @param linkname the symlink
   * @return 0 on success
   */
  public static int symLink(String target, String linkname) throws IOException{
    // Run the input paths through Java's File so that they are converted to the
    // native OS form
    File targetFile = new File(
        Path.getPathWithoutSchemeAndAuthority(new Path(target)).toString());
    File linkFile = new File(
        Path.getPathWithoutSchemeAndAuthority(new Path(linkname)).toString());

    String[] cmd = Shell.getSymlinkCommand(
        targetFile.toString(),
        linkFile.toString());

    ShellCommandExecutor shExec;
    try {
      if (Shell.WINDOWS &&
          linkFile.getParentFile() != null &&
          !new Path(target).isAbsolute()) {
        // Relative links on Windows must be resolvable at the time of
        // creation. To ensure this we run the shell command in the directory
        // of the link.
        //
        shExec = new ShellCommandExecutor(cmd, linkFile.getParentFile());
      } else {
        shExec = new ShellCommandExecutor(cmd);
      }
      shExec.execute();
    } catch (Shell.ExitCodeException ec) {
      int returnVal = ec.getExitCode();
      if (Shell.WINDOWS && returnVal == SYMLINK_NO_PRIVILEGE) {
        LOG.warn("Fail to create symbolic links on Windows. "
            + "The default security settings in Windows disallow non-elevated "
            + "administrators and all non-administrators from creating symbolic links. "
            + "This behavior can be changed in the Local Security Policy management console");
      } else if (returnVal != 0) {
        LOG.warn("Command '" + StringUtils.join(" ", cmd) + "' failed "
            + returnVal + " with: " + ec.getMessage());
      }
      return returnVal;
    } catch (IOException e) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Error while create symlink " + linkname + " to " + target
            + "." + " Exception: " + StringUtils.stringifyException(e));
      }
      throw e;
    }
    return shExec.getExitCode();
  }

  /**
   * Change the permissions on a filename.
   * @param filename the name of the file to change
   * @param perm the permission string
   * @return the exit code from the command
   * @throws IOException
   * @throws InterruptedException
   */
  public static int chmod(String filename, String perm
                          ) throws IOException, InterruptedException {
    return chmod(filename, perm, false);
  }

  /**
   * Change the permissions on a file / directory, recursively, if
   * needed.
   * @param filename name of the file whose permissions are to change
   * @param perm permission string
   * @param recursive true, if permissions should be changed recursively
   * @return the exit code from the command.
   * @throws IOException
   */
  public static int chmod(String filename, String perm, boolean recursive)
                            throws IOException {
    String [] cmd = Shell.getSetPermissionCommand(perm, recursive);
    String[] args = new String[cmd.length + 1];
    System.arraycopy(cmd, 0, args, 0, cmd.length);
    args[cmd.length] = new File(filename).getPath();
    ShellCommandExecutor shExec = new ShellCommandExecutor(args);
    try {
      shExec.execute();
    }catch(IOException e) {
      if(LOG.isDebugEnabled()) {
        LOG.debug("Error while changing permission : " + filename
                  +" Exception: " + StringUtils.stringifyException(e));
      }
    }
    return shExec.getExitCode();
  }

  /**
   * Set the ownership on a file / directory. User name and group name
   * cannot both be null.
   * @param file the file to change
   * @param username the new user owner name
   * @param groupname the new group owner name
   * @throws IOException
   */
  public static void setOwner(File file, String username,
      String groupname) throws IOException {
    if (username == null && groupname == null) {
      throw new IOException("username == null && groupname == null");
    }
    String arg = (username == null ? "" : username)
        + (groupname == null ? "" : ":" + groupname);
    String [] cmd = Shell.getSetOwnerCommand(arg);
    execCommand(file, cmd);
  }

  /**
   * Platform independent implementation for {@link File#setReadable(boolean)}
   * File#setReadable does not work as expected on Windows.
   * @param f input file
   * @param readable
   * @return true on success, false otherwise
   */
  public static boolean setReadable(File f, boolean readable) {
    if (Shell.WINDOWS) {
      try {
        String permission = readable ? "u+r" : "u-r";
        FileUtil.chmod(f.getCanonicalPath(), permission, false);
        return true;
      } catch (IOException ex) {
        return false;
      }
    } else {
      return f.setReadable(readable);
    }
  }

  /**
   * Platform independent implementation for {@link File#setWritable(boolean)}
   * File#setWritable does not work as expected on Windows.
   * @param f input file
   * @param writable
   * @return true on success, false otherwise
   */
  public static boolean setWritable(File f, boolean writable) {
    if (Shell.WINDOWS) {
      try {
        String permission = writable ? "u+w" : "u-w";
        FileUtil.chmod(f.getCanonicalPath(), permission, false);
        return true;
      } catch (IOException ex) {
        return false;
      }
    } else {
      return f.setWritable(writable);
    }
  }

  /**
   * Platform independent implementation for {@link File#setExecutable(boolean)}
   * File#setExecutable does not work as expected on Windows.
   * Note: revoking execute permission on folders does not have the same
   * behavior on Windows as on Unix platforms. Creating, deleting or renaming
   * a file within that folder will still succeed on Windows.
   * @param f input file
   * @param executable
   * @return true on success, false otherwise
   */
  public static boolean setExecutable(File f, boolean executable) {
    if (Shell.WINDOWS) {
      try {
        String permission = executable ? "u+x" : "u-x";
        FileUtil.chmod(f.getCanonicalPath(), permission, false);
        return true;
      } catch (IOException ex) {
        return false;
      }
    } else {
      return f.setExecutable(executable);
    }
  }

  /**
   * Platform independent implementation for {@link File#canRead()}
   * @param f input file
   * @return On Unix, same as {@link File#canRead()}
   *         On Windows, true if process has read access on the path
   */
  public static boolean canRead(File f) {
    if (Shell.WINDOWS) {
      try {
        return NativeIO.Windows.access(f.getCanonicalPath(),
            NativeIO.Windows.AccessRight.ACCESS_READ);
      } catch (IOException e) {
        return false;
      }
    } else {
      return f.canRead();
    }
  }

  /**
   * Platform independent implementation for {@link File#canWrite()}
   * @param f input file
   * @return On Unix, same as {@link File#canWrite()}
   *         On Windows, true if process has write access on the path
   */
  public static boolean canWrite(File f) {
    if (Shell.WINDOWS) {
      try {
        return NativeIO.Windows.access(f.getCanonicalPath(),
            NativeIO.Windows.AccessRight.ACCESS_WRITE);
      } catch (IOException e) {
        return false;
      }
    } else {
      return f.canWrite();
    }
  }

  /**
   * Platform independent implementation for {@link File#canExecute()}
   * @param f input file
   * @return On Unix, same as {@link File#canExecute()}
   *         On Windows, true if process has execute access on the path
   */
  public static boolean canExecute(File f) {
    if (Shell.WINDOWS) {
      try {
        return NativeIO.Windows.access(f.getCanonicalPath(),
            NativeIO.Windows.AccessRight.ACCESS_EXECUTE);
      } catch (IOException e) {
        return false;
      }
    } else {
      return f.canExecute();
    }
  }

  /**
   * Set permissions to the required value. Uses the java primitives instead
   * of forking if group == other.
   * @param f the file to change
   * @param permission the new permissions
   * @throws IOException
   */
  public static void setPermission(File f, FsPermission permission
                                   ) throws IOException {
    FsAction user = permission.getUserAction();
    FsAction group = permission.getGroupAction();
    FsAction other = permission.getOtherAction();

    // use the native/fork if the group/other permissions are different
    // or if the native is available or on Windows
    if (group != other || NativeIO.isAvailable() || Shell.WINDOWS) {
      execSetPermission(f, permission);
      return;
    }

    boolean rv = true;

    // read perms
    rv = f.setReadable(group.implies(FsAction.READ), false);
    checkReturnValue(rv, f, permission);
    if (group.implies(FsAction.READ) != user.implies(FsAction.READ)) {
      rv = f.setReadable(user.implies(FsAction.READ), true);
      checkReturnValue(rv, f, permission);
    }

    // write perms
    rv = f.setWritable(group.implies(FsAction.WRITE), false);
    checkReturnValue(rv, f, permission);
    if (group.implies(FsAction.WRITE) != user.implies(FsAction.WRITE)) {
      rv = f.setWritable(user.implies(FsAction.WRITE), true);
      checkReturnValue(rv, f, permission);
    }

    // exec perms
    rv = f.setExecutable(group.implies(FsAction.EXECUTE), false);
    checkReturnValue(rv, f, permission);
    if (group.implies(FsAction.EXECUTE) != user.implies(FsAction.EXECUTE)) {
      rv = f.setExecutable(user.implies(FsAction.EXECUTE), true);
      checkReturnValue(rv, f, permission);
    }
  }

  private static void checkReturnValue(boolean rv, File p,
                                       FsPermission permission
                                       ) throws IOException {
    if (!rv) {
      throw new IOException("Failed to set permissions of path: " + p +
                            " to " +
                            String.format("%04o", permission.toShort()));
    }
  }

  private static void execSetPermission(File f,
                                        FsPermission permission
                                       )  throws IOException {
    if (NativeIO.isAvailable()) {
      NativeIO.POSIX.chmod(f.getCanonicalPath(), permission.toShort());
    } else {
      execCommand(f, Shell.getSetPermissionCommand(
                  String.format("%04o", permission.toShort()), false));
    }
  }

  static String execCommand(File f, String... cmd) throws IOException {
    String[] args = new String[cmd.length + 1];
    System.arraycopy(cmd, 0, args, 0, cmd.length);
    args[cmd.length] = f.getCanonicalPath();
    String output = Shell.execCommand(args);
    return output;
  }

  /**
   * Create a tmp file for a base file.
   * @param basefile the base file of the tmp
   * @param prefix file name prefix of tmp
   * @param isDeleteOnExit if true, the tmp will be deleted when the VM exits
   * @return a newly created tmp file
   * @exception IOException If a tmp file cannot created
   * @see java.io.File#createTempFile(String, String, File)
   * @see java.io.File#deleteOnExit()
   */
  public static final File createLocalTempFile(final File basefile,
                                               final String prefix,
                                               final boolean isDeleteOnExit)
    throws IOException {
    File tmp = File.createTempFile(prefix + basefile.getName(),
                                   "", basefile.getParentFile());
    if (isDeleteOnExit) {
      tmp.deleteOnExit();
    }
    return tmp;
  }

  /**
   * Move the src file to the name specified by target.
   * @param src the source file
   * @param target the target file
   * @exception IOException If this operation fails
   */
  public static void replaceFile(File src, File target) throws IOException {
    /* renameTo() has two limitations on Windows platform.
     * src.renameTo(target) fails if
     * 1) If target already exists OR
     * 2) If target is already open for reading/writing.
     */
    if (!src.renameTo(target)) {
      int retries = 5;
      while (target.exists() && !target.delete() && retries-- >= 0) {
        try {
          Thread.sleep(1000);
        } catch (InterruptedException e) {
          throw new IOException("replaceFile interrupted.");
        }
      }
      if (!src.renameTo(target)) {
        throw new IOException("Unable to rename " + src +
                              " to " + target);
      }
    }
  }

  /**
   * A wrapper for {@link File#listFiles()}. This java.io API returns null
   * when a dir is not a directory or for any I/O error. Instead of having
   * null check everywhere File#listFiles() is used, we will add utility API
   * to get around this problem. For the majority of cases where we prefer
   * an IOException to be thrown.
   * @param dir directory for which listing should be performed
   * @return list of files or empty list
   * @exception IOException for invalid directory or for a bad disk.
   */
  public static File[] listFiles(File dir) throws IOException {
    File[] files = dir.listFiles();
    if(files == null) {
      throw new IOException("Invalid directory or I/O error occurred for dir: "
                + dir.toString());
    }
    return files;
  }

  /**
   * A wrapper for {@link File#list()}. This java.io API returns null
   * when a dir is not a directory or for any I/O error. Instead of having
   * null check everywhere File#list() is used, we will add utility API
   * to get around this problem. For the majority of cases where we prefer
   * an IOException to be thrown.
   * @param dir directory for which listing should be performed
   * @return list of file names or empty string list
   * @exception AccessDeniedException for unreadable directory
   * @exception IOException for invalid directory or for bad disk
   */
  public static String[] list(File dir) throws IOException {
    if (!canRead(dir)) {
      throw new AccessDeniedException(dir.toString(), null,
          FSExceptionMessages.PERMISSION_DENIED);
    }
    String[] fileNames = dir.list();
    if(fileNames == null) {
      throw new IOException("Invalid directory or I/O error occurred for dir: "
                + dir.toString());
    }
    return fileNames;
  }

  public static String[] createJarWithClassPath(String inputClassPath, Path pwd,
      Map<String, String> callerEnv) throws IOException {
    return createJarWithClassPath(inputClassPath, pwd, pwd, callerEnv);
  }

  /**
   * Create a jar file at the given path, containing a manifest with a classpath
   * that references all specified entries.
   *
   * Some platforms may have an upper limit on command line length.  For example,
   * the maximum command line length on Windows is 8191 characters, but the
   * length of the classpath may exceed this.  To work around this limitation,
   * use this method to create a small intermediate jar with a manifest that
   * contains the full classpath.  It returns the absolute path to the new jar,
   * which the caller may set as the classpath for a new process.
   *
   * Environment variable evaluation is not supported within a jar manifest, so
   * this method expands environment variables before inserting classpath entries
   * to the manifest.  The method parses environment variables according to
   * platform-specific syntax (%VAR% on Windows, or $VAR otherwise).  On Windows,
   * environment variables are case-insensitive.  For example, %VAR% and %var%
   * evaluate to the same value.
   *
   * Specifying the classpath in a jar manifest does not support wildcards, so
   * this method expands wildcards internally.  Any classpath entry that ends
   * with * is translated to all files at that path with extension .jar or .JAR.
   *
   * @param inputClassPath String input classpath to bundle into the jar manifest
   * @param pwd Path to working directory to save jar
   * @param targetDir path to where the jar execution will have its working dir
   * @param callerEnv Map<String, String> caller's environment variables to use
   *   for expansion
   * @return String[] with absolute path to new jar in position 0 and
   *   unexpanded wild card entry path in position 1
   * @throws IOException if there is an I/O error while writing the jar file
   */
  public static String[] createJarWithClassPath(String inputClassPath, Path pwd,
      Path targetDir,
      Map<String, String> callerEnv) throws IOException {
    // Replace environment variables, case-insensitive on Windows
    @SuppressWarnings("unchecked")
    Map<String, String> env = Shell.WINDOWS ? new CaseInsensitiveMap(callerEnv) :
      callerEnv;
    String[] classPathEntries = inputClassPath.split(File.pathSeparator);
    for (int i = 0; i < classPathEntries.length; ++i) {
      classPathEntries[i] = StringUtils.replaceTokens(classPathEntries[i],
        StringUtils.ENV_VAR_PATTERN, env);
    }
    File workingDir = new File(pwd.toString());
    if (!workingDir.mkdirs()) {
      // If mkdirs returns false because the working directory already exists,
      // then this is acceptable.  If it returns false due to some other I/O
      // error, then this method will fail later with an IOException while saving
      // the jar.
      LOG.debug("mkdirs false for " + workingDir + ", execution will continue");
    }

    StringBuilder unexpandedWildcardClasspath = new StringBuilder();
    // Append all entries
    List<String> classPathEntryList = new ArrayList<String>(
      classPathEntries.length);
    for (String classPathEntry: classPathEntries) {
      if (classPathEntry.length() == 0) {
        continue;
      }
      if (classPathEntry.endsWith("*")) {
        // Append all jars that match the wildcard
        List<Path> jars = getJarsInDirectory(classPathEntry);
        if (!jars.isEmpty()) {
          for (Path jar: jars) {
            classPathEntryList.add(jar.toUri().toURL().toExternalForm());
          }
        } else {
          unexpandedWildcardClasspath.append(File.pathSeparator);
          unexpandedWildcardClasspath.append(classPathEntry);
        }
      } else {
        // Append just this entry
        File fileCpEntry = null;
        if(!new Path(classPathEntry).isAbsolute()) {
          fileCpEntry = new File(targetDir.toString(), classPathEntry);
        }
        else {
          fileCpEntry = new File(classPathEntry);
        }
        String classPathEntryUrl = fileCpEntry.toURI().toURL()
          .toExternalForm();

        // File.toURI only appends trailing '/' if it can determine that it is a
        // directory that already exists.  (See JavaDocs.)  If this entry had a
        // trailing '/' specified by the caller, then guarantee that the
        // classpath entry in the manifest has a trailing '/', and thus refers to
        // a directory instead of a file.  This can happen if the caller is
        // creating a classpath jar referencing a directory that hasn't been
        // created yet, but will definitely be created before running.
        if (classPathEntry.endsWith(Path.SEPARATOR) &&
            !classPathEntryUrl.endsWith(Path.SEPARATOR)) {
          classPathEntryUrl = classPathEntryUrl + Path.SEPARATOR;
        }
        classPathEntryList.add(classPathEntryUrl);
      }
    }
    String jarClassPath = StringUtils.join(" ", classPathEntryList);

    // Create the manifest
    Manifest jarManifest = new Manifest();
    jarManifest.getMainAttributes().putValue(
        Attributes.Name.MANIFEST_VERSION.toString(), "1.0");
    jarManifest.getMainAttributes().putValue(
        Attributes.Name.CLASS_PATH.toString(), jarClassPath);

    // Write the manifest to output JAR file
    File classPathJar = File.createTempFile("classpath-", ".jar", workingDir);
    FileOutputStream fos = null;
    BufferedOutputStream bos = null;
    JarOutputStream jos = null;
    try {
      fos = new FileOutputStream(classPathJar);
      bos = new BufferedOutputStream(fos);
      jos = new JarOutputStream(bos, jarManifest);
    } finally {
      IOUtils.cleanup(LOG, jos, bos, fos);
    }
    String[] jarCp = {classPathJar.getCanonicalPath(),
                        unexpandedWildcardClasspath.toString()};
    return jarCp;
  }

  /**
   * Returns all jars that are in the directory. It is useful in expanding a
   * wildcard path to return all jars from the directory to use in a classpath.
   * It operates only on local paths.
   *
   * @param path the path to the directory. The path may include the wildcard.
   * @return the list of jars as URLs, or an empty list if there are no jars, or
   * the directory does not exist locally
   */
  public static List<Path> getJarsInDirectory(String path) {
    return getJarsInDirectory(path, true);
  }

  /**
   * Returns all jars that are in the directory. It is useful in expanding a
   * wildcard path to return all jars from the directory to use in a classpath.
   *
   * @param path the path to the directory. The path may include the wildcard.
   * @return the list of jars as URLs, or an empty list if there are no jars, or
   * the directory does not exist
   */
  public static List<Path> getJarsInDirectory(String path, boolean useLocal) {
    List<Path> paths = new ArrayList<>();
    try {
      // add the wildcard if it is not provided
      if (!path.endsWith("*")) {
        path += File.separator + "*";
      }
      Path globPath = new Path(path).suffix("{.jar,.JAR}");
      FileContext context = useLocal ?
          FileContext.getLocalFSFileContext() :
          FileContext.getFileContext(globPath.toUri());
      FileStatus[] files = context.util().globStatus(globPath);
      if (files != null) {
        for (FileStatus file: files) {
          paths.add(file.getPath());
        }
      }
    } catch (IOException ignore) {} // return the empty list
    return paths;
  }

  public static boolean compareFs(FileSystem srcFs, FileSystem destFs) {
    if (srcFs==null || destFs==null) {
      return false;
    }
    URI srcUri = srcFs.getUri();
    URI dstUri = destFs.getUri();
    if (srcUri.getScheme()==null) {
      return false;
    }
    if (!srcUri.getScheme().equals(dstUri.getScheme())) {
      return false;
    }
    String srcHost = srcUri.getHost();
    String dstHost = dstUri.getHost();
    if ((srcHost!=null) && (dstHost!=null)) {
      if (srcHost.equals(dstHost)) {
        return srcUri.getPort()==dstUri.getPort();
      }
      try {
        srcHost = InetAddress.getByName(srcHost).getCanonicalHostName();
        dstHost = InetAddress.getByName(dstHost).getCanonicalHostName();
      } catch (UnknownHostException ue) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Could not compare file-systems. Unknown host: ", ue);
        }
        return false;
      }
      if (!srcHost.equals(dstHost)) {
        return false;
      }
    } else if (srcHost==null && dstHost!=null) {
      return false;
    } else if (srcHost!=null) {
      return false;
    }
    // check for ports
    return srcUri.getPort()==dstUri.getPort();
  }
}
