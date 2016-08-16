/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.slider.tools;

import org.apache.commons.compress.archivers.zip.ZipArchiveEntry;
import org.apache.commons.compress.archivers.zip.ZipArchiveOutputStream;
import org.apache.commons.compress.utils.IOUtils;
import org.junit.Assert;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;

/**
 *  Various utility methods
 *  Byte comparison methods are from
 *  <code>org.apache.hadoop.fs.contract.ContractTestUtils</code>
 */
public class TestUtility {
  protected static final Logger log =
      LoggerFactory.getLogger(TestUtility.class);

  public static void addDir(File dirObj, ZipArchiveOutputStream zipFile, String prefix) throws IOException {
    for (File file : dirObj.listFiles()) {
      if (file.isDirectory()) {
        addDir(file, zipFile, prefix + file.getName() + File.separator);
      } else {
        log.info("Adding to zip - " + prefix + file.getName());
        zipFile.putArchiveEntry(new ZipArchiveEntry(prefix + file.getName()));
        IOUtils.copy(new FileInputStream(file), zipFile);
        zipFile.closeArchiveEntry();
      }
    }
  }

  public static void zipDir(String zipFile, String dir) throws IOException {
    File dirObj = new File(dir);
    ZipArchiveOutputStream out = new ZipArchiveOutputStream(new FileOutputStream(zipFile));
    log.info("Creating : {}", zipFile);
    try {
      addDir(dirObj, out, "");
    } finally {
      out.close();
    }
  }

  public static String createAppPackage(
      TemporaryFolder folder, String subDir, String pkgName, String srcPath) throws IOException {
    String zipFileName;
    File pkgPath = folder.newFolder(subDir);
    File zipFile = new File(pkgPath, pkgName).getAbsoluteFile();
    zipFileName = zipFile.getAbsolutePath();
    TestUtility.zipDir(zipFileName, srcPath);
    log.info("Created temporary zip file at {}", zipFileName);
    return zipFileName;
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
    Assert.assertEquals("Number of bytes read != number written",
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
      log.warn(message);
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
        log.warn(line);
      }
      Assert.fail(message);
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
}
