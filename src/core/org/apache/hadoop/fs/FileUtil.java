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

import java.io.*;
import java.net.URL;
import java.util.Enumeration;
import java.util.List;
import java.util.jar.Attributes;
import java.util.jar.JarOutputStream;
import java.util.jar.Manifest;
import java.util.zip.GZIPInputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.nativeio.NativeIO;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Shell;
import org.apache.hadoop.util.Shell.ShellCommandExecutor;

/**
 * A collection of file-processing util methods
 */
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
   * Delete a directory and all its contents.  If
   * we return false, the directory may be partially-deleted.
   */
  public static boolean fullyDelete(File dir) throws IOException {
    if (!fullyDeleteContents(dir)) {
      return false;
    }
    return dir.delete();
  }

  /**
   * Delete the contents of a directory, not the directory itself.  If
   * we return false, the directory may be partially-deleted.
   */
  public static boolean fullyDeleteContents(File dir) throws IOException {
    boolean deletionSucceeded = true;
    File contents[] = dir.listFiles();
    if (contents != null) {
      for (int i = 0; i < contents.length; i++) {
        if (contents[i].isFile()) {
          if (!contents[i].delete()) {
            deletionSucceeded = false;
            continue; // continue deletion of other files/dirs under dir
          }
        } else {
          //try deleting the directory
          // this might be a symlink
          boolean b = false;
          b = contents[i].delete();
          if (b){
            //this was indeed a symlink or an empty directory
            continue;
          }
          // if not an empty directory or symlink let
          // fullydelete handle it.
          if (!fullyDelete(contents[i])) {
            deletionSucceeded = false;
            continue; // continue deletion of other files/dirs under dir
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
    StringBuffer exceptions = new StringBuffer();

    if (srcs.length == 1)
      return copy(srcFS, srcs[0], dstFS, dst, deleteSource, overwrite, conf);

    // Check if dest is directory
    if (!dstFS.exists(dst)) {
      throw new IOException("`" + dst +"': specified destination directory " +
                            "does not exist");
    } else {
      FileStatus sdst = dstFS.getFileStatus(dst);
      if (!sdst.isDir()) 
        throw new IOException("copying multiple files, but last argument `" +
                              dst + "' is not a directory");
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
    dst = checkDest(src.getName(), dstFS, dst, overwrite);

    if (srcFS.getFileStatus(src).isDir()) {
      checkDependencies(srcFS, src, dstFS, dst);
      if (!dstFS.mkdirs(dst)) {
        return false;
      }
      FileStatus contents[] = srcFS.listStatus(src);
      for (int i = 0; i < contents.length; i++) {
        copy(srcFS, contents[i].getPath(), dstFS, 
             new Path(dst, contents[i].getPath().getName()),
             deleteSource, overwrite, conf);
      }
    } else if (srcFS.isFile(src)) {
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
    } else {
      throw new IOException(src.toString() + ": No such file or directory");
    }
    if (deleteSource) {
      return srcFS.delete(src, true);
    } else {
      return true;
    }
  
  }

  /** Copy all files in a directory to one output file (merge). */
  public static boolean copyMerge(FileSystem srcFS, Path srcDir, 
                                  FileSystem dstFS, Path dstFile, 
                                  boolean deleteSource,
                                  Configuration conf, String addString) throws IOException {
    dstFile = checkDest(srcDir.getName(), dstFS, dstFile, false);

    if (!srcFS.getFileStatus(srcDir).isDir())
      return false;
   
    OutputStream out = dstFS.create(dstFile);
    
    try {
      FileStatus contents[] = srcFS.listStatus(srcDir);
      for (int i = 0; i < contents.length; i++) {
        if (!contents[i].isDir()) {
          InputStream in = srcFS.open(contents[i].getPath());
          try {
            IOUtils.copyBytes(in, out, conf, false);
            if (addString!=null)
              out.write(addString.getBytes("UTF-8"));
                
          } finally {
            in.close();
          } 
        }
      }
    } finally {
      out.close();
    }
    

    if (deleteSource) {
      return srcFS.delete(srcDir, true);
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
    if (srcFS.getFileStatus(src).isDir()) {
      if (!dst.mkdirs()) {
        return false;
      }
      FileStatus contents[] = srcFS.listStatus(src);
      for (int i = 0; i < contents.length; i++) {
        copy(srcFS, contents[i].getPath(), 
             new File(dst, contents[i].getPath().getName()),
             deleteSource, conf);
      }
    } else if (srcFS.isFile(src)) {
      InputStream in = srcFS.open(src);
      IOUtils.copyBytes(in, new FileOutputStream(dst), conf);
    } else {
      throw new IOException(src.toString() + 
                            ": No such file or directory");
    }
    if (deleteSource) {
      return srcFS.delete(src, true);
    } else {
      return true;
    }
  }

  private static Path checkDest(String srcName, FileSystem dstFS, Path dst,
      boolean overwrite) throws IOException {
    if (dstFS.exists(dst)) {
      FileStatus sdst = dstFS.getFileStatus(dst);
      if (sdst.isDir()) {
        if (null == srcName) {
          throw new IOException("Target " + dst + " is a directory");
        }
        return checkDest(null, dstFS, new Path(dst, srcName), overwrite);
      } else if (!overwrite) {
        throw new IOException("Target " + dst + " already exists");
      }
    } else if (dst.toString().isEmpty()) {
      return checkDest(null, dstFS, new Path(srcName), overwrite);
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

    // Force the archive path as local on Windows as it can have a colon
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
    if (gzipped) {
      inputStream = new BufferedInputStream(new GZIPInputStream(
          new FileInputStream(inFile)));
    } else {
      inputStream = new BufferedInputStream(new FileInputStream(inFile));
    }

    TarArchiveInputStream tis = new TarArchiveInputStream(inputStream);

    for (TarArchiveEntry entry = tis.getNextTarEntry(); entry != null;) {
      unpackEntries(tis, entry, untarDir);
      entry = tis.getNextTarEntry();
    }
  }
  
  private static void unpackEntries(TarArchiveInputStream tis,
      TarArchiveEntry entry, File outputDir) throws IOException {
    if (entry.isDirectory()) {
      File subDir = new File(outputDir, entry.getName());
      if (!subDir.mkdir() && !subDir.isDirectory()) {
        throw new IOException("Mkdirs failed to create tar internal dir "
            + outputDir);
      }

      for (TarArchiveEntry e : entry.getDirectoryEntries()) {
        unpackEntries(tis, e, subDir);
      }

      return;
    }

    File outputFile = new File(outputDir, entry.getName());
    if (!outputDir.exists()) {
      if (!outputDir.mkdirs()) {
        throw new IOException("Mkdirs failed to create tar internal dir "
            + outputDir);
      }
    }

    int count;
    byte data[] = new byte[2048];
    BufferedOutputStream outputStream = new BufferedOutputStream(
        new FileOutputStream(outputFile));

    while ((count = tis.read(data)) != -1) {
      outputStream.write(data, 0, count);
    }

    outputStream.flush();
    outputStream.close();
  }
  
  /**
   * Create a soft link between a src and destination
   * only on a local disk. HDFS does not support this.
   * On Windows, when symlink creation fails due to security
   * setting, we will log a warning. The return code in this
   * case is 2.
   * @param target the target for symlink 
   * @param linkname the symlink
   * @return value returned by the command
   */
  public static int symLink(String target, String linkname) throws IOException{
    // Run the input paths through Java's File so that they are converted to the
    // native OS form
    File targetFile = new File(target);
    File linkFile = new File(linkname);

    // If not on Java7+, copy a file instead of creating a symlink since
    // Java6 has close to no support for symlinks on Windows. Specifically
    // File#length and File#renameTo do not work as expected.
    // (see HADOOP-9061 for additional details)
    // We still create symlinks for directories, since the scenario in this
    // case is different. The directory content could change in which
    // case the symlink loses its purpose (for example task attempt log folder
    // is symlinked under userlogs and userlogs are generated afterwards).
    if (Shell.WINDOWS && !Shell.isJava7OrAbove() && targetFile.isFile()) {
      try {
        LOG.info("FileUtil#symlink: On Java6, copying file instead "
            + linkname + " -> " + target);
        org.apache.commons.io.FileUtils.copyFile(targetFile, linkFile);
      } catch (IOException ex) {
        LOG.warn("FileUtil#symlink failed to copy the file with error: "
            + ex.getMessage());
        // Exit with non-zero exit code
        return 1;
      }
      return 0;
    }

    String[] cmd = Shell.getSymlinkCommand(targetFile.getPath(),
        linkFile.getPath());
    ShellCommandExecutor shExec = new ShellCommandExecutor(cmd);
    try {
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
      f.setReadable(user.implies(FsAction.READ), true);
      checkReturnValue(rv, f, permission);
    }

    // write perms
    rv = f.setWritable(group.implies(FsAction.WRITE), false);
    checkReturnValue(rv, f, permission);
    if (group.implies(FsAction.WRITE) != user.implies(FsAction.WRITE)) {
      f.setWritable(user.implies(FsAction.WRITE), true);
      checkReturnValue(rv, f, permission);
    }

    // exec perms
    rv = f.setExecutable(group.implies(FsAction.EXECUTE), false);
    checkReturnValue(rv, f, permission);
    if (group.implies(FsAction.EXECUTE) != user.implies(FsAction.EXECUTE)) {
      f.setExecutable(user.implies(FsAction.EXECUTE), true);
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
   * @exception IOException for invalid directory or for a bad disk.
   */
  public static String[] list(File dir) throws IOException {
    String[] fileNames = dir.list();
    if(fileNames == null) {
      throw new IOException("Invalid directory or I/O error occurred for dir: "
                + dir.toString());
    }
    return fileNames;
  }  
  
  /**
   * Create a JAR file under at the given path, referencing all entries in the classpath list.
   * @param jarFile file to create with classpath entries in its manifest
   * @param classPaths entries to be added in the manifest of the created Jar 
   * @return jarFile created with classpath entries
   */
  public static File createJarWithClassPath(File jarFile, List<String> classPaths) 
      throws IOException {
    StringBuffer jarClsPath = new StringBuffer();

    // Append all entries in classpath list to the Jar
    for (String clsEntry : classPaths) {
      URL fileUrl = new URL(new File(clsEntry).toURI().toString());
      jarClsPath.append(fileUrl.toExternalForm());
      jarClsPath.append(" ");
    }

    // Create the manifest
    Manifest jarManifest = new Manifest();
    jarManifest.getMainAttributes().putValue(
        Attributes.Name.MANIFEST_VERSION.toString(), "1.0");
    
    jarManifest.getMainAttributes().putValue(
        Attributes.Name.CLASS_PATH.toString(), jarClsPath.toString().trim());

    // Write the manifest to output JAR file
    JarOutputStream jarStream = new JarOutputStream(
        new FileOutputStream(jarFile), jarManifest);
    
    jarStream.close();
    return jarFile;
  }
}
