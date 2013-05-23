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

package org.apache.hadoop.util;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Shell;

/**
 * Class that provides utility functions for checking disk problem
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class DiskChecker {

  private static final long SHELL_TIMEOUT = 10 * 1000;

  public static class DiskErrorException extends IOException {
    public DiskErrorException(String msg) {
      super(msg);
    }

    public DiskErrorException(String msg, Throwable cause) {
      super(msg, cause);
    }
  }
    
  public static class DiskOutOfSpaceException extends IOException {
    public DiskOutOfSpaceException(String msg) {
      super(msg);
    }
  }
      
  /** 
   * The semantics of mkdirsWithExistsCheck method is different from the mkdirs
   * method provided in the Sun's java.io.File class in the following way:
   * While creating the non-existent parent directories, this method checks for
   * the existence of those directories if the mkdir fails at any point (since
   * that directory might have just been created by some other process).
   * If both mkdir() and the exists() check fails for any seemingly 
   * non-existent directory, then we signal an error; Sun's mkdir would signal
   * an error (return false) if a directory it is attempting to create already
   * exists or the mkdir fails.
   * @param dir
   * @return true on success, false on failure
   */
  public static boolean mkdirsWithExistsCheck(File dir) {
    if (dir.mkdir() || dir.exists()) {
      return true;
    }
    File canonDir = null;
    try {
      canonDir = dir.getCanonicalFile();
    } catch (IOException e) {
      return false;
    }
    String parent = canonDir.getParent();
    return (parent != null) && 
           (mkdirsWithExistsCheck(new File(parent)) &&
                                      (canonDir.mkdir() || canonDir.exists()));
  }
  
  /**
   * Create the directory if it doesn't exist and check that dir is readable,
   * writable and executable
   *  
   * @param dir
   * @throws DiskErrorException
   */
  public static void checkDir(File dir) throws DiskErrorException {
    if (!mkdirsWithExistsCheck(dir)) {
      throw new DiskErrorException("Can not create directory: "
                                   + dir.toString());
    }
    checkDirAccess(dir);
  }

  /**
   * Create the directory or check permissions if it already exists.
   *
   * The semantics of mkdirsWithExistsAndPermissionCheck method is different
   * from the mkdirs method provided in the Sun's java.io.File class in the
   * following way:
   * While creating the non-existent parent directories, this method checks for
   * the existence of those directories if the mkdir fails at any point (since
   * that directory might have just been created by some other process).
   * If both mkdir() and the exists() check fails for any seemingly
   * non-existent directory, then we signal an error; Sun's mkdir would signal
   * an error (return false) if a directory it is attempting to create already
   * exists or the mkdir fails.
   *
   * @param localFS local filesystem
   * @param dir directory to be created or checked
   * @param expected expected permission
   * @throws IOException
   */
  public static void mkdirsWithExistsAndPermissionCheck(
      LocalFileSystem localFS, Path dir, FsPermission expected)
      throws IOException {
    File directory = localFS.pathToFile(dir);
    boolean created = false;

    if (!directory.exists())
      created = mkdirsWithExistsCheck(directory);

    if (created || !localFS.getFileStatus(dir).getPermission().equals(expected))
        localFS.setPermission(dir, expected);
  }

  /**
   * Create the local directory if necessary, check permissions and also ensure
   * it can be read from and written into.
   *
   * @param localFS local filesystem
   * @param dir directory
   * @param expected permission
   * @throws DiskErrorException
   * @throws IOException
   */
  public static void checkDir(LocalFileSystem localFS, Path dir,
                              FsPermission expected)
  throws DiskErrorException, IOException {
    mkdirsWithExistsAndPermissionCheck(localFS, dir, expected);
    checkDirAccess(localFS.pathToFile(dir));
  }

  /**
   * Checks that the given file is a directory and that the current running
   * process can read, write, and execute it.
   * 
   * @param dir File to check
   * @throws DiskErrorException if dir is not a directory, not readable, not
   *   writable, or not executable
   */
  private static void checkDirAccess(File dir) throws DiskErrorException {
    if (!dir.isDirectory()) {
      throw new DiskErrorException("Not a directory: "
                                   + dir.toString());
    }

    if (Shell.WINDOWS) {
      checkAccessByFileSystemInteraction(dir);
    } else {
      checkAccessByFileMethods(dir);
    }
  }

  /**
   * Checks that the current running process can read, write, and execute the
   * given directory by using methods of the File object.
   * 
   * @param dir File to check
   * @throws DiskErrorException if dir is not readable, not writable, or not
   *   executable
   */
  private static void checkAccessByFileMethods(File dir)
      throws DiskErrorException {
    if (!dir.canRead()) {
      throw new DiskErrorException("Directory is not readable: "
                                   + dir.toString());
    }

    if (!dir.canWrite()) {
      throw new DiskErrorException("Directory is not writable: "
                                   + dir.toString());
    }

    if (!dir.canExecute()) {
      throw new DiskErrorException("Directory is not executable: "
                                   + dir.toString());
    }
  }

  /**
   * Checks that the current running process can read, write, and execute the
   * given directory by attempting each of those operations on the file system.
   * This method contains several workarounds to known JVM bugs that cause
   * File.canRead, File.canWrite, and File.canExecute to return incorrect results
   * on Windows with NTFS ACLs.  See:
   * http://bugs.sun.com/bugdatabase/view_bug.do?bug_id=6203387
   * These bugs are supposed to be fixed in JDK7.
   * 
   * @param dir File to check
   * @throws DiskErrorException if dir is not readable, not writable, or not
   *   executable
   */
  private static void checkAccessByFileSystemInteraction(File dir)
      throws DiskErrorException {
    // Make sure we can read the directory by listing it.
    if (dir.list() == null) {
      throw new DiskErrorException("Directory is not readable: "
                                   + dir.toString());
    }

    // Make sure we can write to the directory by creating a temp file in it.
    try {
      File tempFile = File.createTempFile("checkDirAccess", null, dir);
      if (!tempFile.delete()) {
        throw new DiskErrorException("Directory is not writable: "
                                     + dir.toString());
      }
    } catch (IOException e) {
      throw new DiskErrorException("Directory is not writable: "
                                   + dir.toString(), e);
    }

    // Make sure the directory is executable by trying to cd into it.  This
    // launches a separate process.  It does not change the working directory of
    // the current process.
    try {
      String[] cdCmd = new String[] { "cmd", "/C", "cd",
          dir.getAbsolutePath() };
      Shell.execCommand(null, cdCmd, SHELL_TIMEOUT);
    } catch (Shell.ExitCodeException e) {
      throw new DiskErrorException("Directory is not executable: "
                                   + dir.toString(), e);
    } catch (IOException e) {
      throw new DiskErrorException("Directory is not executable: "
                                   + dir.toString(), e);
    }
  }
}
