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
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class that provides utility functions for checking disk problem
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class DiskChecker {
  public static final Logger LOG = LoggerFactory.getLogger(DiskChecker.class);

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

  // Provider that abstracts some FileOutputStream operations for
  // testability.
  private static AtomicReference<FileIoProvider> fileIoProvider =
      new AtomicReference<>(new DefaultFileIoProvider());

  /**
   * Create the directory if it doesn't exist and check that dir is readable,
   * writable and executable
   *  
   * @param dir
   * @throws DiskErrorException
   */
  public static void checkDir(File dir) throws DiskErrorException {
    if (!mkdirsWithExistsCheck(dir)) {
      throw new DiskErrorException("Cannot create directory: "
                                   + dir.toString());
    }
    checkAccessByFileMethods(dir);
    doDiskIo(dir);
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
    checkAccessByFileMethods(localFS.pathToFile(dir));
    doDiskIo(localFS.pathToFile(dir));
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
    if (!dir.isDirectory()) {
      throw new DiskErrorException("Not a directory: "
          + dir.toString());
    }

    if (!FileUtil.canRead(dir)) {
      throw new DiskErrorException("Directory is not readable: "
                                   + dir.toString());
    }

    if (!FileUtil.canWrite(dir)) {
      throw new DiskErrorException("Directory is not writable: "
                                   + dir.toString());
    }

    if (!FileUtil.canExecute(dir)) {
      throw new DiskErrorException("Directory is not executable: "
                                   + dir.toString());
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
  private static boolean mkdirsWithExistsCheck(File dir) {
    if (dir.mkdir() || dir.exists()) {
      return true;
    }
    File canonDir;
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
  static void mkdirsWithExistsAndPermissionCheck(
      LocalFileSystem localFS, Path dir, FsPermission expected)
      throws IOException {
    File directory = localFS.pathToFile(dir);
    boolean created = false;

    if (!directory.exists())
      created = mkdirsWithExistsCheck(directory);

    if (created || !localFS.getFileStatus(dir).getPermission().equals(expected))
      localFS.setPermission(dir, expected);
  }

  // State related to running disk IO checks.
  private static final String DISK_IO_FILE_PREFIX =
      "DiskChecker.OK_TO_DELETE_.";

  @VisibleForTesting
  static final int DISK_IO_MAX_ITERATIONS = 3;

  /**
   * Performs some disk IO by writing to a new file in the given directory
   * and sync'ing file contents to disk.
   *
   * This increases the likelihood of catching catastrophic disk/controller
   * failures sooner.
   *
   * @param dir directory to be checked.
   * @throws DiskErrorException if we hit an error while trying to perform
   *         disk IO against the file.
   */
  private static void doDiskIo(File dir) throws DiskErrorException {
    try {
      IOException ioe = null;

      for (int i = 0; i < DISK_IO_MAX_ITERATIONS; ++i) {
        final File file = getFileNameForDiskIoCheck(dir, i+1);
        try {
          diskIoCheckWithoutNativeIo(file);
          return;
        } catch (IOException e) {
          // Let's retry a few times before we really give up and
          // declare the disk as bad.
          ioe = e;
        }
      }
      throw ioe;  // Just rethrow the last exception to signal failure.
    } catch(IOException e) {
      throw new DiskErrorException("Error checking directory " + dir, e);
    }
  }

  /**
   * Try to perform some disk IO by writing to the given file
   * without using Native IO.
   *
   * @param file
   * @throws IOException if there was a non-retriable error.
   */
  private static void diskIoCheckWithoutNativeIo(File file)
      throws IOException {
    FileOutputStream fos = null;

    try {
      final FileIoProvider provider = fileIoProvider.get();
      fos = provider.get(file);
      provider.write(fos, new byte[1]);
      fos.getFD().sync();
      fos.close();
      fos = null;
      if (!file.delete() && file.exists()) {
        throw new IOException("Failed to delete " + file);
      }
      file = null;
    } finally {
      IOUtils.cleanup(null, fos);
      FileUtils.deleteQuietly(file);
    }
  }

  /**
   * Generate a path name for a test file under the given directory.
   *
   * @return file object.
   */
  @VisibleForTesting
  static File getFileNameForDiskIoCheck(File dir, int iterationCount) {
    if (iterationCount < DISK_IO_MAX_ITERATIONS) {
      // Use file names of the format prefix.001 by default.
      return new File(dir,
          DISK_IO_FILE_PREFIX + String.format("%03d", iterationCount));
    } else {
      // If the first few checks then fail, try using a randomly generated
      // file name.
      return new File(dir, DISK_IO_FILE_PREFIX + UUID.randomUUID());
    }
  }

  /**
   * An interface that abstracts operations on {@link FileOutputStream}
   * objects for testability.
   */
  interface FileIoProvider {
    FileOutputStream get(File f) throws FileNotFoundException;
    void write(FileOutputStream fos, byte[] data) throws IOException;
  }

  /**
   * The default implementation of {@link FileIoProvider}.
   */
  private static class DefaultFileIoProvider implements FileIoProvider {
    /**
     * See {@link FileOutputStream#FileOutputStream(File)}.
     */
    @Override
    public FileOutputStream get(File f) throws FileNotFoundException {
      return new FileOutputStream(f);
    }

    /**
     * See {@link FileOutputStream#write(byte[])}.
     */
    @Override
    public void write(FileOutputStream fos, byte[] data) throws IOException {
      fos.write(data);
    }
  }

  /**
   * Replace the {@link FileIoProvider} for tests.
   * This method MUST NOT be used outside of unit tests.
   *
   * @param newFosProvider
   * @return the old FileIoProvider.
   */
  @VisibleForTesting
  static FileIoProvider replaceFileOutputStreamProvider(
      FileIoProvider newFosProvider) {
    return fileIoProvider.getAndSet(newFosProvider);
  }

  /**
   * Retrieve the current {@link FileIoProvider}.
   * This method MUST NOT be used outside of unit tests.
   *
   * @return the current FileIoProvider.
   */
  @VisibleForTesting
  static FileIoProvider getFileOutputStreamProvider() {
    return fileIoProvider.get();
  }
}
