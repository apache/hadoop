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
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Assert;
import org.junit.internal.AssumptionViolatedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.EOFException;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.Properties;
import java.util.UUID;

/**
 * Utilities used across test cases
 */
public class ContractTestUtils extends Assert {

  private static final Logger LOG =
      LoggerFactory.getLogger(ContractTestUtils.class);

  public static final String IO_FILE_BUFFER_SIZE = "io.file.buffer.size";

  // For scale testing, we can repeatedly write small chunk data to generate
  // a large file.
  public static final String IO_CHUNK_BUFFER_SIZE = "io.chunk.buffer.size";
  public static final int DEFAULT_IO_CHUNK_BUFFER_SIZE = 128;
  public static final String IO_CHUNK_MODULUS_SIZE = "io.chunk.modulus.size";
  public static final int DEFAULT_IO_CHUNK_MODULUS_SIZE = 128;

  /**
   * Assert that a property in the property set matches the expected value
   * @param props property set
   * @param key property name
   * @param expected expected value. If null, the property must not be in the set
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
    assertTrue(
      "Not enough data in source array to write " + len + " bytes",
      src.length >= len);
    FSDataOutputStream out = fs.create(path,
                                       overwrite,
                                       fs.getConf()
                                         .getInt(IO_FILE_BUFFER_SIZE,
                                                 4096),
                                       (short) 1,
                                       buffersize);
    out.write(src, 0, len);
    out.close();
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
    FSDataInputStream in = fs.open(path);
    byte[] dest = new byte[len];
    int offset =0;
    int nread = 0;
    try {
      while (nread < len) {
        int nbytes = in.read(dest, offset + nread, len - nread);
        if (nbytes < 0) {
          throw new EOFException("End of file reached before reading fully.");
        }
        nread += nbytes;
      }
    } finally {
      in.close();
    }
    return dest;
  }

  /**
   * Read a file, verify its length and contents match the expected array
   * @param fs filesystem
   * @param path path to file
   * @param original original dataset
   * @throws IOException IO Problems
   */
  public static void verifyFileContents(FileSystem fs,
                                        Path path,
                                        byte[] original) throws IOException {
    FileStatus stat = fs.getFileStatus(path);
    String statText = stat.toString();
    assertTrue("not a file " + statText, stat.isFile());
    assertEquals("wrong length " + statText, original.length, stat.getLen());
    byte[] bytes = readDataset(fs, path, original.length);
    compareByteArrays(original,bytes,original.length);
  }

  /**
   * Verify that the read at a specific offset in a stream
   * matches that expected
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
    compareByteArrays(expected, out,toRead);
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
    int first_error_byte = -1;
    for (int i = 0; i < len; i++) {
      if (original[i] != received[i]) {
        if (errors == 0) {
          first_error_byte = i;
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
      for (int i = Math.max(0, first_error_byte - overlap);
           i < Math.min(first_error_byte + overlap, len);
           i++) {
        byte actual = received[i];
        byte expected = original[i];
        String letter = toChar(actual);
        String line = String.format("[%04d] %2x %s\n", i, actual, letter);
        if (expected != actual) {
          line = String.format("[%04d] %2x %s -expected %2x %s\n",
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
   * Convert a buffer to a string, character by character
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
   * Cleanup at the end of a test run
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
   * Cleanup at the end of a test run
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


  public static void noteAction(String action) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("==============  "+ action +" =============");
    }
  }

  /**
   * downgrade a failure to a message and a warning, then an
   * exception for the Junit test runner to mark as failed
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
   * report an overridden test as unsupported
   * @param message message to use in the text
   * @throws AssumptionViolatedException always
   */
  public static void unsupported(String message) {
    skip(message);
  }

  /**
   * report a test has been skipped for some reason
   * @param message message to use in the text
   * @throws AssumptionViolatedException always
   */
  public static void skip(String message) {
    LOG.info("Skipping: {}", message);
    throw new AssumptionViolatedException(message);
  }

  /**
   * Fail with an exception that was received
   * @param text text to use in the exception
   * @param thrown a (possibly null) throwable to init the cause with
   * @throws AssertionError with the text and throwable -always
   */
  public static void fail(String text, Throwable thrown) {
    AssertionError e = new AssertionError(text);
    e.initCause(thrown);
    throw e;
  }

  /**
   * Make an assertion about the length of a file
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
   * Assert that a path refers to a directory
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
   * Assert that a path refers to a directory
   * @param fileStatus stats to check
   */
  public static void assertIsDirectory(FileStatus fileStatus) {
    assertTrue("Should be a directory -but isn't: " + fileStatus,
               fileStatus.isDirectory());
  }

  /**
   * Write the text to a file, returning the converted byte array
   * for use in validating the round trip
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
   * Create a file
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
    if (data != null && data.length > 0) {
      stream.write(data);
    }
    stream.close();
  }

  /**
   * Touch a file
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
   * all operations on root directories
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
   * Read in "length" bytes, convert to an ascii string
   * @param fs filesystem
   * @param path path to read
   * @param length #of bytes to read.
   * @return the bytes read and converted to a string
   * @throws IOException IO problems
   */
  public static String readBytesToString(FileSystem fs,
                                  Path path,
                                  int length) throws IOException {
    FSDataInputStream in = fs.open(path);
    try {
      byte[] buf = new byte[length];
      in.readFully(0, buf);
      return toChar(buf);
    } finally {
      in.close();
    }
  }

  /**
   * Take an array of filestats and convert to a string (prefixed w/ a [01] counter
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
   * List a directory
   * @param fileSystem FS
   * @param path path
   * @return a directory listing or failure message
   * @throws IOException
   */
  public static String ls(FileSystem fileSystem, Path path) throws IOException {
    if (path == null) {
      //surfaces when someone calls getParent() on something at the top of the path
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
    return pathname + fileStatsToString(stats, "\n");
  }

   /**
   * Assert that a file exists and whose {@link FileStatus} entry
   * declares that this is a file and not a symlink or directory.
   * @param fileSystem filesystem to resolve path against
   * @param filename name of the file
   * @throws IOException IO problems during file operations
   */
  public static void assertIsFile(FileSystem fileSystem, Path filename) throws
                                                                 IOException {
    assertPathExists(fileSystem, "Expected file", filename);
    FileStatus status = fileSystem.getFileStatus(filename);
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
   * Create a dataset for use in the tests; all data is in the range
   * base to (base+modulo-1) inclusive
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
   * type of that entry
   *
   * @param fileSystem filesystem to examine
   * @param message message to include in the assertion failure message
   * @param path path in the filesystem
   * @throws FileNotFoundException raised if the path is missing
   * @throws IOException IO problems
   */
  public static void assertPathExists(FileSystem fileSystem, String message,
                               Path path) throws IOException {
    if (!fileSystem.exists(path)) {
      //failure, report it
      ls(fileSystem, path.getParent());
      throw new FileNotFoundException(message + ": not found " + path
                                      + " in " + path.getParent());
    }
  }

  /**
   * Assert that a path does not exist
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
   * Assert that a FileSystem.listStatus on a dir finds the subdir/child entry
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
      builder.append(stat.toString()).append('\n');
      if (stat.getPath().equals(subdir)) {
        found = true;
      }
    }
    assertTrue("Path " + subdir
                      + " not found in directory " + dir + ":" + builder,
                      found);
  }

  /**
   * Test for the host being an OSX machine
   * @return true if the JVM thinks that is running on OSX
   */
  public static boolean isOSX() {
    return System.getProperty("os.name").contains("OS X");
  }

  /**
   * compare content of file operations using a double byte array
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
      if (mismatch)
        break;
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
    final InputStream inputStream = fs.open(path);
    try {
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
    } finally {
      inputStream.close();
    }
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

    final OutputStream outputStream = fs.create(path, false);
    long bytesWritten = 0;
    try {
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
    } finally {
      outputStream.close();
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
  public static void createAndVerifyFile(FileSystem fs, Path parent, final long fileSize)
      throws IOException {
    int testBufferSize = fs.getConf()
        .getInt(IO_CHUNK_BUFFER_SIZE, DEFAULT_IO_CHUNK_BUFFER_SIZE);
    int modulus = fs.getConf()
        .getInt(IO_CHUNK_MODULUS_SIZE, DEFAULT_IO_CHUNK_MODULUS_SIZE);

    final String objectName = UUID.randomUUID().toString();
    final Path objectPath = new Path(parent, objectName);

    // Write test file in a specific pattern
    assertEquals(fileSize,
        generateTestFile(fs, objectPath, fileSize, testBufferSize, modulus));
    assertPathExists(fs, "not created successful", objectPath);

    // Now read the same file back and verify its content
    try {
      verifyReceivedData(fs, objectPath, fileSize, testBufferSize, modulus);
    } finally {
      // Delete test file
      fs.delete(objectPath, false);
    }
  }
}
