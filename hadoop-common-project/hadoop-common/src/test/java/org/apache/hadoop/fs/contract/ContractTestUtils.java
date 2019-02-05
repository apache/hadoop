/*
 * Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.fs.contract;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.StreamCapabilities;
import org.apache.hadoop.io.IOUtils;
import org.junit.Assert;
import org.junit.internal.AssumptionViolatedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.EOFException;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.NoSuchElementException;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.IO_FILE_BUFFER_SIZE_DEFAULT;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.IO_FILE_BUFFER_SIZE_KEY;

/**
 * Utilities used across test cases.
 */
public class ContractTestUtils extends Assert {

  private static final Logger LOG =
      LoggerFactory.getLogger(ContractTestUtils.class);

  // For scale testing, we can repeatedly write small chunk data to generate
  // a large file.
  public static final String IO_CHUNK_BUFFER_SIZE = "io.chunk.buffer.size";
  public static final int DEFAULT_IO_CHUNK_BUFFER_SIZE = 128;
  public static final String IO_CHUNK_MODULUS_SIZE = "io.chunk.modulus.size";
  public static final int DEFAULT_IO_CHUNK_MODULUS_SIZE = 128;

  /**
   * Assert that a property in the property set matches the expected value.
   * @param props property set
   * @param key property name
   * @param expected expected value. If null, the property must not be in the
   *                 set
   */
  public static void assertPropertyEquals(Properties props,
                                          String key,
                                          String expected) {
    String val = props.getProperty(key);
    if (expected == null) {
      assertNull("Non null property " + key + " = " + val, val);
    } else {
      assertEquals("property " + key + " = " + val,
                          expected,
                          val);
    }
  }

  /**
   *
   * Write a file and read it in, validating the result. Optional flags control
   * whether file overwrite operations should be enabled, and whether the
   * file should be deleted afterwards.
   *
   * If there is a mismatch between what was written and what was expected,
   * a small range of bytes either side of the first error are logged to aid
   * diagnosing what problem occurred -whether it was a previous file
   * or a corrupting of the current file. This assumes that two
   * sequential runs to the same path use datasets with different character
   * moduli.
   *
   * @param fs filesystem
   * @param path path to write to
   * @param len length of data
   * @param overwrite should the create option allow overwrites?
   * @param delete should the file be deleted afterwards? -with a verification
   * that it worked. Deletion is not attempted if an assertion has failed
   * earlier -it is not in a <code>finally{}</code> block.
   * @throws IOException IO problems
   */
  public static void writeAndRead(FileSystem fs,
                                  Path path,
                                  byte[] src,
                                  int len,
                                  int blocksize,
                                  boolean overwrite,
                                  boolean delete) throws IOException {
    fs.mkdirs(path.getParent());

    writeDataset(fs, path, src, len, blocksize, overwrite);

    byte[] dest = readDataset(fs, path, len);

    compareByteArrays(src, dest, len);

    if (delete) {
      rejectRootOperation(path);
      boolean deleted = fs.delete(path, false);
      assertTrue("Deleted", deleted);
      assertPathDoesNotExist(fs, "Cleanup failed", path);
    }
  }

  /**
   * Write a file.
   * Optional flags control
   * whether file overwrite operations should be enabled
   * @param fs filesystem
   * @param path path to write to
   * @param len length of data
   * @param overwrite should the create option allow overwrites?
   * @throws IOException IO problems
   */
  public static void writeDataset(FileSystem fs,
                                   Path path,
                                   byte[] src,
                                   int len,
                                   int buffersize,
                                   boolean overwrite) throws IOException {
    writeDataset(fs, path, src, len, buffersize, overwrite, false);
  }

  /**
   * Write a file.
   * Optional flags control
   * whether file overwrite operations should be enabled
   * Optional using {@link org.apache.hadoop.fs.FSDataOutputStreamBuilder}
   *
   * @param fs filesystem
   * @param path path to write to
   * @param len length of data
   * @param overwrite should the create option allow overwrites?
   * @param useBuilder should use builder API to create file?
   * @throws IOException IO problems
   */
  public static void writeDataset(FileSystem fs, Path path, byte[] src,
      int len, int buffersize, boolean overwrite, boolean useBuilder)
      throws IOException {
    assertTrue(
      "Not enough data in source array to write " + len + " bytes",
      src.length >= len);
    FSDataOutputStream out;
    if (useBuilder) {
      out = fs.createFile(path)
          .overwrite(overwrite)
          .replication((short) 1)
          .bufferSize(buffersize)
          .blockSize(buffersize)
          .build();
    } else {
      out = fs.create(path,
          overwrite,
          fs.getConf()
              .getInt(IO_FILE_BUFFER_SIZE_KEY,
                  IO_FILE_BUFFER_SIZE_DEFAULT),
          (short) 1,
          buffersize);
    }
    try {
      out.write(src, 0, len);
    } finally {
      out.close();
    }
    assertFileHasLength(fs, path, len);
  }

  /**
   * Read the file and convert to a byte dataset.
   * This implements readfully internally, so that it will read
   * in the file without ever having to seek()
   * @param fs filesystem
   * @param path path to read from
   * @param len length of data to read
   * @return the bytes
   * @throws IOException IO problems
   */
  public static byte[] readDataset(FileSystem fs, Path path, int len)
      throws IOException {
    byte[] dest = new byte[len];
    int offset =0;
    int nread = 0;
    try (FSDataInputStream in = fs.open(path)) {
      while (nread < len) {
        int nbytes = in.read(dest, offset + nread, len - nread);
        if (nbytes < 0) {
          throw new EOFException("End of file reached before reading fully.");
        }
        nread += nbytes;
      }
    }
    return dest;
  }

  /**
   * Read a file, verify its length and contents match the expected array.
   * @param fs filesystem
   * @param path path to file
   * @param original original dataset
   * @throws IOException IO Problems
   */
  public static void verifyFileContents(FileSystem fs,
                                        Path path,
                                        byte[] original) throws IOException {
    assertIsFile(fs, path);
    FileStatus stat = fs.getFileStatus(path);
    String statText = stat.toString();
    assertEquals("wrong length " + statText, original.length, stat.getLen());
    byte[] bytes = readDataset(fs, path, original.length);
    compareByteArrays(original, bytes, original.length);
  }

  /**
   * Verify that the read at a specific offset in a stream
   * matches that expected.
   * @param stm stream
   * @param fileContents original file contents
   * @param seekOff seek offset
   * @param toRead number of bytes to read
   * @throws IOException IO problems
   */
  public static void verifyRead(FSDataInputStream stm, byte[] fileContents,
                                int seekOff, int toRead) throws IOException {
    byte[] out = new byte[toRead];
    stm.seek(seekOff);
    stm.readFully(out);
    byte[] expected = Arrays.copyOfRange(fileContents, seekOff,
                                         seekOff + toRead);
    compareByteArrays(expected, out, toRead);
  }

  /**
   * Assert that tthe array original[0..len] and received[] are equal.
   * A failure triggers the logging of the bytes near where the first
   * difference surfaces.
   * @param original source data
   * @param received actual
   * @param len length of bytes to compare
   */
  public static void compareByteArrays(byte[] original,
                                       byte[] received,
                                       int len) {
    assertEquals("Number of bytes read != number written",
                        len, received.length);
    int errors = 0;
    int firstErrorByte = -1;
    for (int i = 0; i < len; i++) {
      if (original[i] != received[i]) {
        if (errors == 0) {
          firstErrorByte = i;
        }
        errors++;
      }
    }

    if (errors > 0) {
      String message = String.format(" %d errors in file of length %d",
                                     errors, len);
      LOG.warn(message);
      // the range either side of the first error to print
      // this is a purely arbitrary number, to aid user debugging
      final int overlap = 10;
      for (int i = Math.max(0, firstErrorByte - overlap);
           i < Math.min(firstErrorByte + overlap, len);
           i++) {
        byte actual = received[i];
        byte expected = original[i];
        String letter = toChar(actual);
        String line = String.format("[%04d] %2x %s%n", i, actual, letter);
        if (expected != actual) {
          line = String.format("[%04d] %2x %s -expected %2x %s%n",
                               i,
                               actual,
                               letter,
                               expected,
                               toChar(expected));
        }
        LOG.warn(line);
      }
      fail(message);
    }
  }

  /**
   * Convert a byte to a character for printing. If the
   * byte value is < 32 -and hence unprintable- the byte is
   * returned as a two digit hex value
   * @param b byte
   * @return the printable character string
   */
  public static String toChar(byte b) {
    if (b >= 0x20) {
      return Character.toString((char) b);
    } else {
      return String.format("%02x", b);
    }
  }

  /**
   * Convert a buffer to a string, character by character.
   * @param buffer input bytes
   * @return a string conversion
   */
  public static String toChar(byte[] buffer) {
    StringBuilder builder = new StringBuilder(buffer.length);
    for (byte b : buffer) {
      builder.append(toChar(b));
    }
    return builder.toString();
  }

  public static byte[] toAsciiByteArray(String s) {
    char[] chars = s.toCharArray();
    int len = chars.length;
    byte[] buffer = new byte[len];
    for (int i = 0; i < len; i++) {
      buffer[i] = (byte) (chars[i] & 0xff);
    }
    return buffer;
  }

  /**
   * Cleanup at the end of a test run.
   * @param action action triggering the operation (for use in logging)
   * @param fileSystem filesystem to work with. May be null
   * @param cleanupPath path to delete as a string
   */
  public static void cleanup(String action,
                             FileSystem fileSystem,
                             String cleanupPath) {
    if (fileSystem == null) {
      return;
    }
    Path path = new Path(cleanupPath).makeQualified(fileSystem.getUri(),
        fileSystem.getWorkingDirectory());
    cleanup(action, fileSystem, path);
  }

  /**
   * Cleanup at the end of a test run.
   * @param action action triggering the operation (for use in logging)
   * @param fileSystem filesystem to work with. May be null
   * @param path path to delete
   */
  public static void cleanup(String action, FileSystem fileSystem, Path path) {
    noteAction(action);
    try {
      rm(fileSystem, path, true, false);
    } catch (Exception e) {
      LOG.error("Error deleting in "+ action + " - "  + path + ": " + e, e);
    }
  }

  /**
   * Delete a directory. There's a safety check for operations against the
   * root directory -these are intercepted and rejected with an IOException
   * unless the allowRootDelete flag is true
   * @param fileSystem filesystem to work with. May be null
   * @param path path to delete
   * @param recursive flag to enable recursive delete
   * @param allowRootDelete can the root directory be deleted?
   * @throws IOException on any problem.
   */
  public static boolean rm(FileSystem fileSystem,
      Path path,
      boolean recursive,
      boolean allowRootDelete) throws
      IOException {
    if (fileSystem != null) {
      rejectRootOperation(path, allowRootDelete);
      if (fileSystem.exists(path)) {
        return fileSystem.delete(path, recursive);
      }
    }
    return false;

  }

  /**
   * Rename operation. Safety check for attempts to rename the root directory.
   * Verifies that src no longer exists after rename.
   * @param fileSystem filesystem to work with
   * @param src source path
   * @param dst destination path
   * @throws IOException If rename fails or src is the root directory.
   */
  public static void rename(FileSystem fileSystem, Path src, Path dst)
      throws IOException {
    rejectRootOperation(src, false);
    assertTrue(fileSystem.rename(src, dst));
    assertPathDoesNotExist(fileSystem, "renamed", src);
  }

  /**
   * Block any operation on the root path. This is a safety check
   * @param path path in the filesystem
   * @param allowRootOperation can the root directory be manipulated?
   * @throws IOException if the operation was rejected
   */
  public static void rejectRootOperation(Path path,
      boolean allowRootOperation) throws IOException {
    if (path.isRoot() && !allowRootOperation) {
      throw new IOException("Root directory operation rejected: " + path);
    }
  }

  /**
   * Block any operation on the root path. This is a safety check
   * @param path path in the filesystem
   * @throws IOException if the operation was rejected
   */
  public static void rejectRootOperation(Path path) throws IOException {
    rejectRootOperation(path, false);
  }

  /**
   * List then delete the children of a path, but not the path itself.
   * This can be used to delete the entries under a root path when that
   * FS does not support {@code delete("/")}.
   * @param fileSystem filesystem
   * @param path path to delete
   * @param recursive flag to indicate child entry deletion should be recursive
   * @return the immediate child entries found and deleted (not including
   * any recursive children of those entries)
   * @throws IOException problem in the deletion process.
   */
  public static FileStatus[] deleteChildren(FileSystem fileSystem,
      Path path,
      boolean recursive) throws IOException {
    FileStatus[] children = listChildren(fileSystem, path);
    for (FileStatus entry : children) {
      fileSystem.delete(entry.getPath(), recursive);
    }
    return children;
  }

  /**
   * List all children of a path, but not the path itself in the case
   * that the path refers to a file or empty directory.
   * @param fileSystem FS
   * @param path path
   * @return a list of children, and never the path itself.
   * @throws IOException problem in the list process
   */
  public static FileStatus[] listChildren(FileSystem fileSystem,
      Path path) throws IOException {
    FileStatus[] entries = fileSystem.listStatus(path);
    if (entries.length == 1 && path.equals(entries[0].getPath())) {
      // this is the path: ignore
      return new FileStatus[]{};
    } else {
      return entries;
    }
  }

  public static void noteAction(String action) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("==============  "+ action +" =============");
    }
  }

  /**
   * downgrade a failure to a message and a warning, then an
   * exception for the Junit test runner to mark as failed.
   * @param message text message
   * @param failure what failed
   * @throws AssumptionViolatedException always
   */
  public static void downgrade(String message, Throwable failure) {
    LOG.warn("Downgrading test " + message, failure);
    AssumptionViolatedException ave =
        new AssumptionViolatedException(failure, null);
    throw ave;
  }

  /**
   * report an overridden test as unsupported.
   * @param message message to use in the text
   * @throws AssumptionViolatedException always
   */
  public static void unsupported(String message) {
    skip(message);
  }

  /**
   * report a test has been skipped for some reason.
   * @param message message to use in the text
   * @throws AssumptionViolatedException always
   */
  public static void skip(String message) {
    LOG.info("Skipping: {}", message);
    throw new AssumptionViolatedException(message);
  }

  /**
   * Fail with an exception that was received.
   * @param text text to use in the exception
   * @param thrown a (possibly null) throwable to init the cause with
   * @throws AssertionError with the text and throwable -always
   */
  public static void fail(String text, Throwable thrown) {
    throw new AssertionError(text, thrown);
  }

  /**
   * Make an assertion about the length of a file.
   * @param fs filesystem
   * @param path path of the file
   * @param expected expected length
   * @throws IOException on File IO problems
   */
  public static void assertFileHasLength(FileSystem fs, Path path,
                                         int expected) throws IOException {
    FileStatus status = fs.getFileStatus(path);
    assertEquals(
        "Wrong file length of file " + path + " status: " + status,
        expected,
        status.getLen());
  }

  /**
   * Assert that a path refers to a directory.
   * @param fs filesystem
   * @param path path of the directory
   * @throws IOException on File IO problems
   */
  public static void assertIsDirectory(FileSystem fs,
                                       Path path) throws IOException {
    FileStatus fileStatus = fs.getFileStatus(path);
    assertIsDirectory(fileStatus);
  }

  /**
   * Assert that a path refers to a directory.
   * @param fileStatus stats to check
   */
  public static void assertIsDirectory(FileStatus fileStatus) {
    assertTrue("Should be a directory -but isn't: " + fileStatus,
               fileStatus.isDirectory());
  }

  /**
   * Assert that a path is Erasure Coded.
   *
   * @param fs filesystem
   * @param path path of the file or directory
   * @throws IOException on File IO problems
   */
  public static void assertErasureCoded(final FileSystem fs, final Path path)
      throws IOException {
    FileStatus fileStatus = fs.getFileStatus(path);
    assertTrue(path + " must be erasure coded!", fileStatus.isErasureCoded());
  }

  /**
   * Assert that a path is not Erasure Coded.
   *
   * @param fs filesystem
   * @param path path of the file or directory
   * @throws IOException on File IO problems
   */
  public static void assertNotErasureCoded(final FileSystem fs,
      final Path path) throws IOException {
    FileStatus fileStatus = fs.getFileStatus(path);
    assertFalse(path + " should not be erasure coded!",
        fileStatus.isErasureCoded());
  }

  /**
   * Write the text to a file, returning the converted byte array
   * for use in validating the round trip.
   * @param fs filesystem
   * @param path path of file
   * @param text text to write
   * @param overwrite should the operation overwrite any existing file?
   * @return the read bytes
   * @throws IOException on IO problems
   */
  public static byte[] writeTextFile(FileSystem fs,
                                   Path path,
                                   String text,
                                   boolean overwrite) throws IOException {
    byte[] bytes = new byte[0];
    if (text != null) {
      bytes = toAsciiByteArray(text);
    }
    createFile(fs, path, overwrite, bytes);
    return bytes;
  }

  /**
   * Create a file.
   * @param fs filesystem
   * @param path       path to write
   * @param overwrite overwrite flag
   * @param data source dataset. Can be null
   * @throws IOException on any problem
   */
  public static void createFile(FileSystem fs,
                                 Path path,
                                 boolean overwrite,
                                 byte[] data) throws IOException {
    FSDataOutputStream stream = fs.create(path, overwrite);
    try {
      if (data != null && data.length > 0) {
        stream.write(data);
      }
      stream.close();
    } finally {
      IOUtils.closeStream(stream);
    }
  }

  /**
   * Append to an existing file.
   * @param fs filesystem
   * @param path path to file
   * @param data data to append. Can be null
   * @throws IOException On any error
   */
  public static void appendFile(FileSystem fs,
                                Path path,
                                byte[] data) throws IOException {
    try (FSDataOutputStream stream = fs.appendFile(path).build()) {
      if (data != null && data.length > 0) {
        stream.write(data);
      }
    }
  }

  /**
   * Touch a file.
   * @param fs filesystem
   * @param path path
   * @throws IOException IO problems
   */
  public static void touch(FileSystem fs,
                           Path path) throws IOException {
    createFile(fs, path, true, null);
  }

  /**
   * Delete a file/dir and assert that delete() returned true
   * <i>and</i> that the path no longer exists. This variant rejects
   * all operations on root directories.
   * @param fs filesystem
   * @param file path to delete
   * @param recursive flag to enable recursive delete
   * @throws IOException IO problems
   */
  public static void assertDeleted(FileSystem fs,
                                   Path file,
                                   boolean recursive) throws IOException {
    assertDeleted(fs, file, recursive, false);
  }

  /**
   * Delete a file/dir and assert that delete() returned true
   * <i>and</i> that the path no longer exists. This variant rejects
   * all operations on root directories
   * @param fs filesystem
   * @param file path to delete
   * @param recursive flag to enable recursive delete
   * @param allowRootOperations can the root dir be deleted?
   * @throws IOException IO problems
   */
  public static void assertDeleted(FileSystem fs,
      Path file,
      boolean recursive,
      boolean allowRootOperations) throws IOException {
    rejectRootOperation(file, allowRootOperations);
    assertPathExists(fs, "about to be deleted file", file);
    boolean deleted = fs.delete(file, recursive);
    String dir = ls(fs, file.getParent());
    assertTrue("Delete failed on " + file + ": " + dir, deleted);
    assertPathDoesNotExist(fs, "Deleted file", file);
  }

  /**
   * Execute a {@link FileSystem#rename(Path, Path)}, and verify that the
   * outcome was as expected. There is no preflight checking of arguments;
   * everything is left to the rename() command.
   * @param fs filesystem
   * @param source source path
   * @param dest destination path
   * @param expectedResult expected return code
   * @throws IOException on any IO failure.
   */
  public static void assertRenameOutcome(FileSystem fs,
      Path source,
      Path dest,
      boolean expectedResult) throws IOException {
    boolean result = fs.rename(source, dest);
    if (expectedResult != result) {
      fail(String.format("Expected rename(%s, %s) to return %b,"
              + " but result was %b", source, dest, expectedResult, result));
    }
  }

  /**
   * Read in "length" bytes, convert to an ascii string.
   * This uses {@link #toChar(byte)} to escape bytes, so cannot be used
   * for round trip operations.
   * @param fs filesystem
   * @param path path to read
   * @param length #of bytes to read.
   * @return the bytes read and converted to a string
   * @throws IOException IO problems
   */
  public static String readBytesToString(FileSystem fs,
                                  Path path,
                                  int length) throws IOException {
    try (FSDataInputStream in = fs.open(path)) {
      byte[] buf = new byte[length];
      in.readFully(0, buf);
      return toChar(buf);
    }
  }

  /**
   * Read in "length" bytes, convert to UTF8 string.
   * @param fs filesystem
   * @param path path to read
   * @param length #of bytes to read. If -1: use file length.
   * @return the bytes read and converted to a string
   * @throws IOException IO problems
   */
  public static String readUTF8(FileSystem fs,
                                  Path path,
                                  int length) throws IOException {
    if (length < 0) {
      FileStatus status = fs.getFileStatus(path);
      length = (int) status.getLen();
    }
    try (FSDataInputStream in = fs.open(path)) {
      byte[] buf = new byte[length];
      in.readFully(0, buf);
      return new String(buf, "UTF-8");
    }
  }

  /**
   * Take an array of filestats and convert to a string
   * (prefixed with/ a [%02d] counter).
   * @param stats array of stats
   * @param separator separator after every entry
   * @return a stringified set
   */
  public static String fileStatsToString(FileStatus[] stats, String separator) {
    StringBuilder buf = new StringBuilder(stats.length * 128);
    for (int i = 0; i < stats.length; i++) {
      buf.append(String.format("[%02d] %s", i, stats[i])).append(separator);
    }
    return buf.toString();
  }

  /**
   * List a directory.
   * @param fileSystem FS
   * @param path path
   * @return a directory listing or failure message
   * @throws IOException
   */
  public static String ls(FileSystem fileSystem, Path path) throws IOException {
    if (path == null) {
      // surfaces when someone calls getParent() on something at the top of the
      // path
      return "/";
    }
    FileStatus[] stats;
    String pathtext = "ls " + path;
    try {
      stats = fileSystem.listStatus(path);
    } catch (FileNotFoundException e) {
      return pathtext + " -file not found";
    } catch (IOException e) {
      return pathtext + " -failed: " + e;
    }
    return dumpStats(pathtext, stats);
  }

  public static String dumpStats(String pathname, FileStatus[] stats) {
    return pathname + ' ' + fileStatsToString(stats,
        System.lineSeparator());
  }

   /**
   * Assert that a file exists and whose {@link FileStatus} entry
   * declares that this is a file and not a symlink or directory.
   * @param fileSystem filesystem to resolve path against
   * @param filename name of the file
   * @throws IOException IO problems during file operations
   */
  public static void assertIsFile(FileSystem fileSystem, Path filename)
      throws IOException {
    assertPathExists(fileSystem, "Expected file", filename);
    FileStatus status = fileSystem.getFileStatus(filename);
    assertIsFile(filename, status);
  }

  /**
   * Assert that a file exists and whose {@link FileStatus} entry
   * declares that this is a file and not a symlink or directory.
   *
   * @param fileContext filesystem to resolve path against
   * @param filename    name of the file
   * @throws IOException IO problems during file operations
   */
  public static void assertIsFile(FileContext fileContext, Path filename)
      throws IOException {
    assertPathExists(fileContext, "Expected file", filename);
    FileStatus status = fileContext.getFileStatus(filename);
    assertIsFile(filename, status);
  }

  /**
   * Assert that a file exists and whose {@link FileStatus} entry
   * declares that this is a file and not a symlink or directory.
   * @param filename name of the file
   * @param status file status
   */
  public static void assertIsFile(Path filename, FileStatus status) {
    String fileInfo = filename + "  " + status;
    assertFalse("File claims to be a directory " + fileInfo,
                status.isDirectory());
    assertFalse("File claims to be a symlink " + fileInfo,
                       status.isSymlink());
  }

  /**
   * Assert that a varargs list of paths exist.
   * @param fs filesystem
   * @param message message for exceptions
   * @param paths paths
   * @throws IOException IO failure
   */
  public static void assertPathsExist(FileSystem fs,
      String message,
      Path... paths) throws IOException {
    for (Path path : paths) {
      assertPathExists(fs, message, path);
    }
  }

  /**
   * Assert that a varargs list of paths do not exist.
   * @param fs filesystem
   * @param message message for exceptions
   * @param paths paths
   * @throws IOException IO failure
   */
  public static void assertPathsDoNotExist(FileSystem fs,
      String message,
      Path... paths) throws IOException {
    for (Path path : paths) {
      assertPathDoesNotExist(fs, message, path);
    }
  }

  /**
   * Create a dataset for use in the tests; all data is in the range
   * base to (base+modulo-1) inclusive.
   * @param len length of data
   * @param base base of the data
   * @param modulo the modulo
   * @return the newly generated dataset
   */
  public static byte[] dataset(int len, int base, int modulo) {
    byte[] dataset = new byte[len];
    for (int i = 0; i < len; i++) {
      dataset[i] = (byte) (base + (i % modulo));
    }
    return dataset;
  }

  /**
   * Assert that a path exists -but make no assertions as to the
   * type of that entry.
   *
   * @param fileSystem filesystem to examine
   * @param message message to include in the assertion failure message
   * @param path path in the filesystem
   * @throws FileNotFoundException raised if the path is missing
   * @throws IOException IO problems
   */
  public static void assertPathExists(FileSystem fileSystem, String message,
                               Path path) throws IOException {
    verifyPathExists(fileSystem, message, path);
  }

  /**
   * Verify that a path exists, returning the file status of the path.
   *
   * @param fileSystem filesystem to examine
   * @param message message to include in the assertion failure message
   * @param path path in the filesystem
   * @throws FileNotFoundException raised if the path is missing
   * @throws IOException IO problems
   */
  public static FileStatus verifyPathExists(FileSystem fileSystem,
      String message,
      Path path) throws IOException {
    try {
      return fileSystem.getFileStatus(path);
    } catch (FileNotFoundException e) {
      //failure, report it
      LOG.error("{}: not found {}; parent listing is:\n{}",
          message, path, ls(fileSystem, path.getParent()));
      throw (IOException)new FileNotFoundException(
          message + ": not found " + path + " in " + path.getParent())
          .initCause(e);
    }
  }

  /**
   * Assert that a path exists -but make no assertions as to the
   * type of that entry.
   *
   * @param fileContext fileContext to examine
   * @param message     message to include in the assertion failure message
   * @param path        path in the filesystem
   * @throws FileNotFoundException raised if the path is missing
   * @throws IOException           IO problems
   */
  public static void assertPathExists(FileContext fileContext, String message,
      Path path) throws IOException {
    if (!fileContext.util().exists(path)) {
      //failure, report it
      throw new FileNotFoundException(
          message + ": not found " + path + " in " + path.getParent());
    }
  }

  /**
   * Assert that a path does not exist.
   *
   * @param fileSystem filesystem to examine
   * @param message message to include in the assertion failure message
   * @param path path in the filesystem
   * @throws IOException IO problems
   */
  public static void assertPathDoesNotExist(FileSystem fileSystem,
                                            String message,
                                            Path path) throws IOException {
    try {
      FileStatus status = fileSystem.getFileStatus(path);
      fail(message + ": unexpectedly found " + path + " as  " + status);
    } catch (FileNotFoundException expected) {
      //this is expected

    }
  }

  /**
   * Assert that a path does not exist.
   *
   * @param fileContext fileContext to examine
   * @param message     message to include in the assertion failure message
   * @param path        path in the filesystem
   * @throws IOException IO problems
   */
  public static void assertPathDoesNotExist(FileContext fileContext,
      String message, Path path) throws IOException {
    try {
      FileStatus status = fileContext.getFileStatus(path);
      fail(message + ": unexpectedly found " + path + " as  " + status);
    } catch (FileNotFoundException expected) {
      //this is expected

    }
  }

  /**
   * Assert that a FileSystem.listStatus on a dir finds the subdir/child entry.
   * @param fs filesystem
   * @param dir directory to scan
   * @param subdir full path to look for
   * @throws IOException IO probles
   */
  public static void assertListStatusFinds(FileSystem fs,
                                           Path dir,
                                           Path subdir) throws IOException {
    FileStatus[] stats = fs.listStatus(dir);
    boolean found = false;
    StringBuilder builder = new StringBuilder();
    for (FileStatus stat : stats) {
      builder.append(stat.toString()).append(System.lineSeparator());
      if (stat.getPath().equals(subdir)) {
        found = true;
      }
    }
    assertTrue("Path " + subdir
                      + " not found in directory " + dir + ":" + builder,
                      found);
  }

  /**
   * Execute {@link FileSystem#mkdirs(Path)}; expect {@code true} back.
   * (Note: does not work for localFS if the directory already exists)
   * Does not perform any validation of the created directory.
   * @param fs filesystem
   * @param dir directory to create
   * @throws IOException IO Problem
   */
  public static void assertMkdirs(FileSystem fs, Path dir) throws IOException {
    assertTrue("mkdirs(" + dir + ") returned false", fs.mkdirs(dir));
  }

  /**
   * Test for the host being an OSX machine.
   * @return true if the JVM thinks that is running on OSX
   */
  public static boolean isOSX() {
    return System.getProperty("os.name").contains("OS X");
  }

  /**
   * compare content of file operations using a double byte array.
   * @param concat concatenated files
   * @param bytes bytes
   */
  public static void validateFileContent(byte[] concat, byte[][] bytes) {
    int idx = 0;
    boolean mismatch = false;

    for (byte[] bb : bytes) {
      for (byte b : bb) {
        if (b != concat[idx++]) {
          mismatch = true;
          break;
        }
      }
      if (mismatch) {
        break;
      }
    }
    assertFalse("File content of file is not as expected at offset " + idx,
                mismatch);
  }

  /**
   * Receives test data from the given input file and checks the size of the
   * data as well as the pattern inside the received data.
   *
   * @param fs FileSystem
   * @param path Input file to be checked
   * @param expectedSize the expected size of the data to be read from the
   *        input file in bytes
   * @param bufferLen Pattern length
   * @param modulus   Pattern modulus
   * @throws IOException
   *         thrown if an error occurs while reading the data
   */
  public static void verifyReceivedData(FileSystem fs, Path path,
                                      final long expectedSize,
                                      final int bufferLen,
                                      final int modulus) throws IOException {
    final byte[] testBuffer = new byte[bufferLen];

    long totalBytesRead = 0;
    int nextExpectedNumber = 0;
    NanoTimer timer = new NanoTimer();
    try (InputStream inputStream = fs.open(path)) {
      while (true) {
        final int bytesRead = inputStream.read(testBuffer);
        if (bytesRead < 0) {
          break;
        }

        totalBytesRead += bytesRead;

        for (int i = 0; i < bytesRead; ++i) {
          if (testBuffer[i] != nextExpectedNumber) {
            throw new IOException("Read number " + testBuffer[i]
                + " but expected " + nextExpectedNumber);
          }

          ++nextExpectedNumber;

          if (nextExpectedNumber == modulus) {
            nextExpectedNumber = 0;
          }
        }
      }

      if (totalBytesRead != expectedSize) {
        throw new IOException("Expected to read " + expectedSize +
            " bytes but only received " + totalBytesRead);
      }
    }
    timer.end("Time to read %d bytes", expectedSize);
    bandwidth(timer, expectedSize);
  }

  /**
   * Generates test data of the given size according to some specific pattern
   * and writes it to the provided output file.
   *
   * @param fs FileSystem
   * @param path Test file to be generated
   * @param size The size of the test data to be generated in bytes
   * @param bufferLen Pattern length
   * @param modulus   Pattern modulus
   * @throws IOException
   *         thrown if an error occurs while writing the data
   */
  public static long generateTestFile(FileSystem fs, Path path,
                                      final long size,
                                      final int bufferLen,
                                      final int modulus) throws IOException {
    final byte[] testBuffer = new byte[bufferLen];
    for (int i = 0; i < testBuffer.length; ++i) {
      testBuffer[i] = (byte) (i % modulus);
    }

    long bytesWritten = 0;
    try (OutputStream outputStream = fs.create(path, false)) {
      while (bytesWritten < size) {
        final long diff = size - bytesWritten;
        if (diff < testBuffer.length) {
          outputStream.write(testBuffer, 0, (int) diff);
          bytesWritten += diff;
        } else {
          outputStream.write(testBuffer);
          bytesWritten += testBuffer.length;
        }
      }

      return bytesWritten;
    }
  }

  /**
   * Creates and reads a file with the given size. The test file is generated
   * according to a specific pattern so it can be easily verified even if it's
   * a multi-GB one.
   * During the read phase the incoming data stream is also checked against
   * this pattern.
   *
   * @param fs FileSystem
   * @param parent Test file parent dir path
   * @throws IOException
   *    thrown if an I/O error occurs while writing or reading the test file
   */
  public static void createAndVerifyFile(FileSystem fs,
                                         Path parent,
                                         final long fileSize)
      throws IOException {
    int testBufferSize = fs.getConf()
        .getInt(IO_CHUNK_BUFFER_SIZE, DEFAULT_IO_CHUNK_BUFFER_SIZE);
    int modulus = fs.getConf()
        .getInt(IO_CHUNK_MODULUS_SIZE, DEFAULT_IO_CHUNK_MODULUS_SIZE);

    final String objectName = UUID.randomUUID().toString();
    final Path objectPath = new Path(parent, objectName);

    // Write test file in a specific pattern
    NanoTimer timer = new NanoTimer();
    assertEquals(fileSize,
        generateTestFile(fs, objectPath, fileSize, testBufferSize, modulus));
    assertPathExists(fs, "not created successful", objectPath);
    timer.end("Time to write %d bytes", fileSize);
    bandwidth(timer, fileSize);

    // Now read the same file back and verify its content
    try {
      verifyReceivedData(fs, objectPath, fileSize, testBufferSize, modulus);
    } finally {
      // Delete test file
      fs.delete(objectPath, false);
    }
  }

  /**
   * Make times more readable, by adding a "," every three digits.
   * @param nanos nanos or other large number
   * @return a string for logging
   */
  public static String toHuman(long nanos) {
    return String.format(Locale.ENGLISH, "%,d", nanos);
  }

  /**
   * Log the bandwidth of a timer as inferred from the number of
   * bytes processed.
   * @param timer timer
   * @param bytes bytes processed in the time period
   */
  public static void bandwidth(NanoTimer timer, long bytes) {
    LOG.info("Bandwidth = {}  MB/S",
        timer.bandwidthDescription(bytes));
  }

  /**
   * Work out the bandwidth in MB/s.
   * @param bytes bytes
   * @param durationNS duration in nanos
   * @return the number of megabytes/second of the recorded operation
   */
  public static double bandwidthMBs(long bytes, long durationNS) {
    return bytes / (1024.0 * 1024) * 1.0e9 / durationNS;
  }

  /**
   * Recursively create a directory tree.
   * Return the details about the created tree. The files and directories
   * are those created under the path, not the base directory created. That
   * is retrievable via {@link TreeScanResults#getBasePath()}.
   * @param fs filesystem
   * @param current parent dir
   * @param depth depth of directory tree
   * @param width width: subdirs per entry
   * @param files number of files per entry
   * @param filesize size of files to create in bytes.
   * @return the details about the created tree.
   * @throws IOException IO Problems
   */
  public static TreeScanResults createSubdirs(FileSystem fs,
      Path current,
      int depth,
      int width,
      int files,
      int filesize) throws IOException {
    return createSubdirs(fs, current, depth, width, files,
        filesize, "dir-", "file-", "0");
  }

  /**
   * Recursively create a directory tree.
   * @param fs filesystem
   * @param current the current dir in the walk
   * @param depth depth of directory tree
   * @param width width: subdirs per entry
   * @param files number of files per entry
   * @param filesize size of files to create in bytes.
   * @param dirPrefix prefix for directory entries
   * @param filePrefix prefix for file entries
   * @param marker string which is slowly built up to uniquely name things
   * @return the details about the created tree.
   * @throws IOException IO Problems
   */
  public static TreeScanResults createSubdirs(FileSystem fs,
      Path current,
      int depth,
      int width,
      int files,
      int filesize,
      String dirPrefix,
      String filePrefix,
      String marker) throws IOException {
    fs.mkdirs(current);
    TreeScanResults results = new TreeScanResults(current);
    if (depth > 0) {
      byte[] data = dataset(filesize, 'a', 'z');
      for (int i = 0; i < files; i++) {
        String name = String.format("%s-%s-%04d.txt", filePrefix, marker, i);
        Path path = new Path(current, name);
        createFile(fs, path, true, data);
        results.add(fs, path);
      }
      for (int w = 0; w < width; w++) {
        String marker2 = String.format("%s-%04d", marker, w);
        Path child = new Path(current, dirPrefix + marker2);
        results.add(createSubdirs(fs, child, depth - 1, width, files,
            filesize, dirPrefix, filePrefix, marker2));
        results.add(fs, child);
      }
    }
    return results;
  }

  /**
   * Predicate to determine if two lists are equivalent, that is, they
   * contain the same entries.
   * @param left first collection of paths
   * @param right second collection of paths
   * @return true if all entries are in each collection of path.
   */
  public static boolean collectionsEquivalent(Collection<Path> left,
      Collection<Path> right) {
    Set<Path> leftSet = new HashSet<>(left);
    Set<Path> rightSet = new HashSet<>(right);
    return leftSet.containsAll(right) && rightSet.containsAll(left);
  }

  /**
   * Take a collection of paths and build a string from them: useful
   * for assertion messages.
   * @param paths paths to stringify
   * @return a string representation
   */
  public static String pathsToString(Collection<Path> paths) {
    StringBuilder builder = new StringBuilder(paths.size() * 100);
    String nl = System.lineSeparator();
    builder.append(nl);
    for (Path path : paths) {
      builder.append("  \"").append(path.toString())
          .append("\"").append(nl);
    }
    builder.append("]");
    return builder.toString();
  }

  /**
   * Predicate to determine if two lists are equivalent, that is, they
   * contain the same entries.
   * @param left first collection of paths
   * @param right second collection of paths
   * @return true if all entries are in each collection of path.
   */
  public static boolean collectionsEquivalentNoDuplicates(Collection<Path> left,
      Collection<Path> right) {
    return collectionsEquivalent(left, right) &&
        !containsDuplicates(left) && !containsDuplicates(right);
  }


  /**
   * Predicate to test for a collection of paths containing duplicate entries.
   * @param paths collection of paths
   * @return true if there are duplicates.
   */
  public static boolean containsDuplicates(Collection<Path> paths) {
    return new HashSet<>(paths).size() != paths.size();
  }

  /**
   * Get the status of a path eventually, even if the FS doesn't have create
   * consistency. If the path is not there by the time the timeout completes,
   * an assertion is raised.
   * @param fs FileSystem
   * @param path path to look for
   * @param timeout timeout in milliseconds
   * @return the status
   * @throws IOException if an I/O error occurs while writing or reading the
   * test file <i>other than file not found</i>
   */
  public static FileStatus getFileStatusEventually(FileSystem fs, Path path,
      int timeout) throws IOException, InterruptedException {
    long endTime = System.currentTimeMillis() + timeout;
    FileStatus stat = null;
    do {
      try {
        stat = fs.getFileStatus(path);
      } catch (FileNotFoundException e) {
        if (System.currentTimeMillis() > endTime) {
          // timeout, raise an assert with more diagnostics
          assertPathExists(fs, "Path not found after " + timeout + " mS", path);
        } else {
          Thread.sleep(50);
        }
      }
    } while (stat == null);
    return stat;
  }

  /**
   * Recursively list all entries, with a depth first traversal of the
   * directory tree.
   * @param path path
   * @return the number of entries listed
   * @throws IOException IO problems
   */
  public static TreeScanResults treeWalk(FileSystem fs, Path path)
      throws IOException {
    TreeScanResults dirsAndFiles = new TreeScanResults();

    FileStatus[] statuses = fs.listStatus(path);
    for (FileStatus status : statuses) {
      LOG.info("{}{}", status.getPath(), status.isDirectory() ? "*" : "");
    }
    for (FileStatus status : statuses) {
      dirsAndFiles.add(status);
      if (status.isDirectory()) {
        dirsAndFiles.add(treeWalk(fs, status.getPath()));
      }
    }
    return dirsAndFiles;
  }

  /**
   * Convert a remote iterator over file status results into a list.
   * The utility equivalents in commons collection and guava cannot be
   * used here, as this is a different interface, one whose operators
   * can throw IOEs.
   * @param iterator input iterator
   * @return the status entries as a list.
   * @throws IOException
   */
  public static List<LocatedFileStatus> toList(
      RemoteIterator<LocatedFileStatus> iterator) throws IOException {
    ArrayList<LocatedFileStatus> list = new ArrayList<>();
    while (iterator.hasNext()) {
      list.add(iterator.next());
    }
    return list;
  }

  /**
   * Convert a remote iterator over file status results into a list.
   * This uses {@link RemoteIterator#next()} calls only, expecting
   * a raised {@link NoSuchElementException} exception to indicate that
   * the end of the listing has been reached. This iteration strategy is
   * designed to verify that the implementation of the remote iterator
   * generates results and terminates consistently with the {@code hasNext/next}
   * iteration. More succinctly "verifies that the {@code next()} operator
   * isn't relying on {@code hasNext()} to always be called during an iteration.
   * @param iterator input iterator
   * @return the status entries as a list.
   * @throws IOException IO problems
   */
  @SuppressWarnings("InfiniteLoopStatement")
  public static List<LocatedFileStatus> toListThroughNextCallsAlone(
      RemoteIterator<LocatedFileStatus> iterator) throws IOException {
    ArrayList<LocatedFileStatus> list = new ArrayList<>();
    try {
      while (true) {
        list.add(iterator.next());
      }
    } catch (NoSuchElementException expected) {
      // ignored
    }
    return list;
  }

  /**
   * Custom assert to test {@link StreamCapabilities}.
   *
   * @param stream The stream to test for StreamCapabilities
   * @param shouldHaveCapabilities The array of expected capabilities
   * @param shouldNotHaveCapabilities The array of unexpected capabilities
   */
  public static void assertCapabilities(
      Object stream, String[] shouldHaveCapabilities,
      String[] shouldNotHaveCapabilities) {
    assertTrue("Stream should be instanceof StreamCapabilities",
        stream instanceof StreamCapabilities);

    if (shouldHaveCapabilities!=null) {
      for (String shouldHaveCapability : shouldHaveCapabilities) {
        assertTrue("Should have capability: " + shouldHaveCapability,
            ((StreamCapabilities) stream).hasCapability(shouldHaveCapability));
      }
    }

    if (shouldNotHaveCapabilities!=null) {
      for (String shouldNotHaveCapability : shouldNotHaveCapabilities) {
        assertFalse("Should not have capability: " + shouldNotHaveCapability,
            ((StreamCapabilities) stream)
                .hasCapability(shouldNotHaveCapability));
      }
    }
  }

  /**
   * Function which calls {@code InputStream.read()} and
   * downgrades an IOE to a runtime exception.
   * @param in input
   * @return the read value
   * @throws AssertionError on any IOException
   */
  public static int read(InputStream in) {
    try {
      return in.read();
    } catch (IOException ex) {
      throw new AssertionError(ex);
    }
  }

  /**
   * Read a whole stream; downgrades an IOE to a runtime exception.
   * @param in input
   * @return the number of bytes read.
   * @throws AssertionError on any IOException
   */
  public static long readStream(InputStream in) {
    long count = 0;

    while (read(in) >= 0) {
      count++;
    }
    return count;
  }


  /**
   * Results of recursive directory creation/scan operations.
   */
  public static final class TreeScanResults {

    private Path basePath;
    private final List<Path> files = new ArrayList<>();
    private final List<Path> directories = new ArrayList<>();
    private final List<Path> other = new ArrayList<>();


    public TreeScanResults() {
    }

    public TreeScanResults(Path basePath) {
      this.basePath = basePath;
    }

    /**
     * Build from a located file status iterator.
     * @param results results of the listFiles/listStatus call.
     * @throws IOException IO problems during the iteration.
     */
    public TreeScanResults(RemoteIterator<LocatedFileStatus> results)
        throws IOException {
      while (results.hasNext()) {
        add(results.next());
      }
    }

    /**
     * Construct results from an array of statistics.
     * @param stats statistics array. Must not be null.
     */
    public TreeScanResults(FileStatus[] stats) {
      assertNotNull("Null file status array", stats);
      for (FileStatus stat : stats) {
        add(stat);
      }
    }

    /**
     * Construct results from an iterable collection of statistics.
     * @param stats statistics source. Must not be null.
     */
    public <F extends FileStatus> TreeScanResults(Iterable<F> stats) {
      for (FileStatus stat : stats) {
        add(stat);
      }
    }

    /**
     * Add all paths in the other set of results to this instance.
     * @param that the other instance
     * @return this instance
     */
    public TreeScanResults add(TreeScanResults that) {
      files.addAll(that.files);
      directories.addAll(that.directories);
      other.addAll(that.other);
      return this;
    }

    /**
     * Increment the counters based on the file status.
     * @param status path status to count.
     */
    public void add(FileStatus status) {
      if (status.isFile()) {
        files.add(status.getPath());
      } else if (status.isDirectory()) {
        directories.add(status.getPath());
      } else {
        other.add(status.getPath());
      }
    }

    public void add(FileSystem fs, Path path) throws IOException {
      add(fs.getFileStatus(path));
    }

    @Override
    public String toString() {
      return String.format("%d director%s and %d file%s",
          getDirCount(),
          getDirCount() == 1 ? "y" : "ies",
          getFileCount(),
          getFileCount() == 1 ? "" : "s");
    }

    /**
     * Equality check compares files and directory counts.
     * As these are non-final fields, this class cannot be used in
     * hash tables.
     * @param o other object
     * @return true iff the file and dir count match.
     */
    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      TreeScanResults that = (TreeScanResults) o;
      return getFileCount() == that.getFileCount() &&
          getDirCount() == that.getDirCount();
    }

    /**
     * This is a spurious hash code subclass to keep findbugs quiet.
     * @return the base {@link Object#hashCode()}
     */
    @Override
    public int hashCode() {
      return super.hashCode();
    }

    /**
     * Assert that the state of a listing has the specific number of files,
     * directories and other entries. The error text will include
     * the {@code text} param, the field in question, and the entire object's
     * string value.
     * @param text text prefix for assertions.
     * @param f file count
     * @param d expected directory count
     * @param o expected other entries.
     */
    public void assertSizeEquals(String text, long f, long d, long o) {
      String self = toString();
      Assert.assertEquals(text + ": file count in " + self,
          f, getFileCount());
      Assert.assertEquals(text + ": directory count in " + self,
          d, getDirCount());
      Assert.assertEquals(text + ": 'other' count in " + self,
          o, getOtherCount());
    }

    /**
     * Assert that the trees are equivalent: that every list matches (and
     * that neither has any duplicates).
     * @param that the other entry
     */
    public void assertEquivalent(TreeScanResults that) {
      assertFieldsEquivalent("files", that, files, that.files);
      assertFieldsEquivalent("directories", that,
          directories, that.directories);
      assertFieldsEquivalent("other", that, other, that.other);
    }

    /**
     * Assert that a field in two instances are equivalent.
     * @param fieldname field name for error messages
     * @param that the other instance to scan
     * @param ours our field's contents
     * @param theirs the other instance's field constants
     */
    public void assertFieldsEquivalent(String fieldname,
        TreeScanResults that,
        List<Path> ours, List<Path> theirs) {
      String ourList = pathsToString(ours);
      String theirList = pathsToString(theirs);
      assertFalse("Duplicate  " + fieldname + " in " + this
          +": " + ourList,
          containsDuplicates(ours));
      assertFalse("Duplicate  " + fieldname + " in other " + that
              + ": " + theirList,
          containsDuplicates(theirs));
      assertTrue(fieldname + " mismatch: between " + ourList
          + " and " + theirList,
          collectionsEquivalent(ours, theirs));
    }

    public List<Path> getFiles() {
      return files;
    }

    public List<Path> getDirectories() {
      return directories;
    }

    public List<Path> getOther() {
      return other;
    }

    public Path getBasePath() {
      return basePath;
    }

    public long getFileCount() {
      return files.size();
    }

    public long getDirCount() {
      return directories.size();
    }

    public long getOtherCount() {
      return other.size();
    }

    /**
     * Total count of entries.
     * @return the total number of entries
     */
    public long totalCount() {
      return getFileCount() + getDirCount() + getOtherCount();
    }

  }

  /**
   * A simple class for timing operations in nanoseconds, and for
   * printing some useful results in the process.
   */
  public static final class NanoTimer {
    private long startTime;
    private long endTime;

    public NanoTimer() {
      startTime = now();
    }

    /**
     * Reset the timer.  Equivalent to the reset button of a stopwatch.
     */
    public void reset() {
      endTime = 0;
      startTime = now();
    }

    /**
     * End the operation.
     * @return the duration of the operation
     */
    public long end() {
      endTime = now();
      return duration();
    }

    /**
     * End the operation; log the duration.
     * @param format message
     * @param args any arguments
     * @return the duration of the operation
     */
    public long end(String format, Object... args) {
      long d = end();
      LOG.info("Duration of {}: {} nS",
          String.format(format, args), toHuman(d));
      return d;
    }

    public long now() {
      return System.nanoTime();
    }

    public long duration() {
      return endTime - startTime;
    }

    /**
     * Intermediate duration of the operation.
     * @return how much time has passed since the start (in nanos).
     */
    public long elapsedTime() {
      return now() - startTime;
    }

    /**
     * Elapsed time in milliseconds; no rounding.
     * @return elapsed time
     */
    public long elapsedTimeMs() {
      return elapsedTime() / 1000000;
    }

    public double bandwidth(long bytes) {
      return bandwidthMBs(bytes, duration());
    }

    /**
     * Bandwidth as bytes per second.
     * @param bytes bytes in
     * @return the number of bytes per second this operation.
     *         0 if duration == 0.
     */
    public double bandwidthBytes(long bytes) {
      double duration = duration();
      return duration > 0 ? bytes / duration : 0;
    }

    /**
     * How many nanoseconds per IOP, byte, etc.
     * @param operations operations processed in this time period
     * @return the nanoseconds it took each byte to be processed
     */
    public long nanosPerOperation(long operations) {
      return duration() / operations;
    }

    /**
     * Get a description of the bandwidth, even down to fractions of
     * a MB.
     * @param bytes bytes processed
     * @return bandwidth
     */
    public String bandwidthDescription(long bytes) {
      return String.format("%,.6f", bandwidth(bytes));
    }

    public long getStartTime() {
      return startTime;
    }

    public long getEndTime() {
      return endTime;
    }
  }

}
