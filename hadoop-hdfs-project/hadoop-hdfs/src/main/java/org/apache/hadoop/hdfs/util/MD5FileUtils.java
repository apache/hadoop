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
package org.apache.hadoop.hdfs.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.MD5Hash;
import org.apache.hadoop.util.StringUtils;

import com.google.common.base.Charsets;

/**
 * Static functions for dealing with files of the same format
 * that the Unix "md5sum" utility writes.
 */
public abstract class MD5FileUtils {
  private static final Logger LOG = LoggerFactory.getLogger(
      MD5FileUtils.class);

  public static final String MD5_SUFFIX = ".md5";
  private static final Pattern LINE_REGEX =
    Pattern.compile("([0-9a-f]{32}) [ \\*](.+)");
  
  /**
   * Verify that the previously saved md5 for the given file matches
   * expectedMd5.
   * @throws IOException 
   */
  public static void verifySavedMD5(File dataFile, MD5Hash expectedMD5)
      throws IOException {
    MD5Hash storedHash = readStoredMd5ForFile(dataFile);
    // Check the hash itself
    if (!expectedMD5.equals(storedHash)) {
      throw new IOException(
          "File " + dataFile + " did not match stored MD5 checksum " +
          " (stored: " + storedHash + ", computed: " + expectedMD5);
    }
  }
  
  /**
   * Read the md5 file stored alongside the given data file
   * and match the md5 file content.
   * @param dataFile the file containing data
   * @return a matcher with two matched groups
   *   where group(1) is the md5 string and group(2) is the data file path.
   */
  private static Matcher readStoredMd5(File md5File) throws IOException {
    BufferedReader reader =
        new BufferedReader(new InputStreamReader(new FileInputStream(
            md5File), Charsets.UTF_8));
    String md5Line;
    try {
      md5Line = reader.readLine();
      if (md5Line == null) { md5Line = ""; }
      md5Line = md5Line.trim();
    } catch (IOException ioe) {
      throw new IOException("Error reading md5 file at " + md5File, ioe);
    } finally {
      IOUtils.cleanupWithLogger(LOG, reader);
    }
    
    Matcher matcher = LINE_REGEX.matcher(md5Line);
    if (!matcher.matches()) {
      throw new IOException("Invalid MD5 file " + md5File + ": the content \""
          + md5Line + "\" does not match the expected pattern.");
    }
    return matcher;
  }

  /**
   * Read the md5 checksum stored alongside the given data file.
   * @param dataFile the file containing data
   * @return the checksum stored in dataFile.md5
   */
  public static MD5Hash readStoredMd5ForFile(File dataFile) throws IOException {
    final File md5File = getDigestFileForFile(dataFile);
    if (!md5File.exists()) {
      return null;
    }

    final Matcher matcher = readStoredMd5(md5File);
    String storedHash = matcher.group(1);
    File referencedFile = new File(matcher.group(2));

    // Sanity check: Make sure that the file referenced in the .md5 file at
    // least has the same name as the file we expect
    if (!referencedFile.getName().equals(dataFile.getName())) {
      throw new IOException(
          "MD5 file at " + md5File + " references file named " +
          referencedFile.getName() + " but we expected it to reference " +
          dataFile);
    }
    return new MD5Hash(storedHash);
  }
  
  /**
   * Read dataFile and compute its MD5 checksum.
   */
  public static MD5Hash computeMd5ForFile(File dataFile) throws IOException {
    InputStream in = new FileInputStream(dataFile);
    try {
      MessageDigest digester = MD5Hash.getDigester();
      DigestInputStream dis = new DigestInputStream(in, digester);
      IOUtils.copyBytes(dis, new IOUtils.NullOutputStream(), 128*1024);
      
      return new MD5Hash(digester.digest());
    } finally {
      IOUtils.closeStream(in);
    }
  }

  /**
   * Save the ".md5" file that lists the md5sum of another file.
   * @param dataFile the original file whose md5 was computed
   * @param digest the computed digest
   * @throws IOException
   */
  public static void saveMD5File(File dataFile, MD5Hash digest)
      throws IOException {
    final String digestString = StringUtils.byteToHexString(digest.getDigest());
    saveMD5File(dataFile, digestString);
  }

  private static void saveMD5File(File dataFile, String digestString)
      throws IOException {
    File md5File = getDigestFileForFile(dataFile);
    String md5Line = digestString + " *" + dataFile.getName() + "\n";

    AtomicFileOutputStream afos = new AtomicFileOutputStream(md5File);
    afos.write(md5Line.getBytes(Charsets.UTF_8));
    afos.close();

    if (LOG.isDebugEnabled()) {
      LOG.debug("Saved MD5 " + digestString + " to " + md5File);
    }
  }

  public static void renameMD5File(File oldDataFile, File newDataFile)
      throws IOException {
    final File fromFile = getDigestFileForFile(oldDataFile);
    if (!fromFile.exists()) {
      throw new FileNotFoundException(fromFile + " does not exist.");
    }

    final String digestString = readStoredMd5(fromFile).group(1);
    saveMD5File(newDataFile, digestString);

    if (!fromFile.delete()) {
      LOG.warn("deleting  " + fromFile.getAbsolutePath() + " FAILED");
    }
  }

  /**
   * @return a reference to the file with .md5 suffix that will
   * contain the md5 checksum for the given data file.
   */
  public static File getDigestFileForFile(File file) {
    return new File(file.getParentFile(), file.getName() + MD5_SUFFIX);
  }
}
