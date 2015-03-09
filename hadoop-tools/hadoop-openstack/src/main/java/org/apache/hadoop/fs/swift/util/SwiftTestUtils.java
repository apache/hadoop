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

package org.apache.hadoop.fs.swift.util;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.swift.exceptions.SwiftConfigurationException;
import org.junit.internal.AssumptionViolatedException;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Properties;

/**
 * Utilities used across test cases
 */
public class SwiftTestUtils extends org.junit.Assert {

  private static final Log LOG =
    LogFactory.getLog(SwiftTestUtils.class);

  public static final String TEST_FS_SWIFT = "test.fs.swift.name";
  public static final String IO_FILE_BUFFER_SIZE = "io.file.buffer.size";

  /**
   * Get the test URI
   * @param conf configuration
   * @throws SwiftConfigurationException missing parameter or bad URI
   */
  public static URI getServiceURI(Configuration conf) throws
                                                      SwiftConfigurationException {
    String instance = conf.get(TEST_FS_SWIFT);
    if (instance == null) {
      throw new SwiftConfigurationException(
        "Missing configuration entry " + TEST_FS_SWIFT);
    }
    try {
      return new URI(instance);
    } catch (URISyntaxException e) {
      throw new SwiftConfigurationException("Bad URI: " + instance);
    }
  }

  public static boolean hasServiceURI(Configuration conf) {
    String instance = conf.get(TEST_FS_SWIFT);
    return instance != null;
  }

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
                                   int blocksize,
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
                                       blocksize);
    out.write(src, 0, len);
    out.close();
    assertFileHasLength(fs, path, len);
  }

  /**
   * Read the file and convert to a byte dataaset
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
    try {
      in.readFully(0, dest);
    } finally {
      in.close();
    }
    return dest;
  }

  /**
   * Assert that tthe array src[0..len] and dest[] are equal
   * @param src source data
   * @param dest actual
   * @param len length of bytes to compare
   */
  public static void compareByteArrays(byte[] src,
                                       byte[] dest,
                                       int len) {
    assertEquals("Number of bytes read != number written",
                        len, dest.length);
    int errors = 0;
    int first_error_byte = -1;
    for (int i = 0; i < len; i++) {
      if (src[i] != dest[i]) {
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
        byte actual = dest[i];
        byte expected = src[i];
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
   * byte value is &lt; 32 -and hence unprintable- the byte is
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

  public static void cleanupInTeardown(FileSystem fileSystem,
                                       String cleanupPath) {
    cleanup("TEARDOWN", fileSystem, cleanupPath);
  }

  public static void cleanup(String action,
                             FileSystem fileSystem,
                             String cleanupPath) {
    noteAction(action);
    try {
      if (fileSystem != null) {
        fileSystem.delete(new Path(cleanupPath).makeQualified(fileSystem),
                          true);
      }
    } catch (Exception e) {
      LOG.error("Error deleting in "+ action + " - "  + cleanupPath + ": " + e, e);
    }
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
    throw new AssumptionViolatedException(message);
  }

  /**
   * report a test has been skipped for some reason
   * @param message message to use in the text
   * @throws AssumptionViolatedException always
   */
  public static void skip(String message) {
    throw new AssumptionViolatedException(message);
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
    assertTrue("Should be a dir -but isn't: " + fileStatus,
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
    FSDataOutputStream stream = fs.create(path, overwrite);
    byte[] bytes = new byte[0];
    if (text != null) {
      bytes = toAsciiByteArray(text);
      stream.write(bytes);
    }
    stream.close();
    return bytes;
  }

  /**
   * Touch a file: fails if it is already there
   * @param fs filesystem
   * @param path path
   * @throws IOException IO problems
   */
  public static void touch(FileSystem fs,
                           Path path) throws IOException {
    fs.delete(path, true);
    writeTextFile(fs, path, null, false);
  }

  public static void assertDeleted(FileSystem fs,
                                   Path file,
                                   boolean recursive) throws IOException {
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
   * @throws IOException
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

  public static String getDefaultWorkingDirectory() {
    return "/user/" + System.getProperty("user.name");
  }

  public static String ls(FileSystem fileSystem, Path path) throws IOException {
    return SwiftUtils.ls(fileSystem, path);
  }

  public static String dumpStats(String pathname, FileStatus[] stats) {
    return pathname + SwiftUtils.fileStatsToString(stats,"\n");
  }

  /**
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
    String fileInfo = filename + "  " + status;
    assertFalse("File claims to be a directory " + fileInfo,
                status.isDirectory());
/* disabled for Hadoop v1 compatibility
    assertFalse("File claims to be a symlink " + fileInfo,
                       status.isSymlink());
*/
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
   * @throws IOException IO problems
   */
  public static void assertPathExists(FileSystem fileSystem, String message,
                               Path path) throws IOException {
    if (!fileSystem.exists(path)) {
      //failure, report it
      fail(message + ": not found " + path + " in " + path.getParent());
           ls(fileSystem, path.getParent());
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

}
