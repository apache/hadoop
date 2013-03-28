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

package org.apache.hadoop.tools.distcp2.util;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileChecksum;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.tools.distcp2.DistCpOptions.FileAttribute;
import org.apache.hadoop.tools.distcp2.mapred.UniformSizeInputFormat;
import org.apache.hadoop.tools.distcp2.DistCpOptions;
import org.apache.hadoop.mapreduce.InputFormat;

import java.io.IOException;
import java.util.EnumSet;
import java.util.Locale;
import java.text.DecimalFormat;
import java.net.URI;
import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * Utility functions used in DistCp.
 */
public class DistCpUtils {

  private static final Log LOG = LogFactory.getLog(DistCpUtils.class);

  /**
   * Retrieves size of the file at the specified path.
   * @param path The path of the file whose size is sought.
   * @param configuration Configuration, to retrieve the appropriate FileSystem.
   * @return The file-size, in number of bytes.
   * @throws IOException, on failure.
   */
  public static long getFileSize(Path path, Configuration configuration)
                                            throws IOException {
    if (LOG.isDebugEnabled())
      LOG.debug("Retrieving file size for: " + path);
    return path.getFileSystem(configuration).getFileStatus(path).getLen();
  }

  /**
   * Utility to publish a value to a configuration.
   * @param configuration The Configuration to which the value must be written.
   * @param label The label for the value being published.
   * @param value The value being published.
   * @param <T> The type of the value.
   */
  public static <T> void publish(Configuration configuration,
                                 String label, T value) {
    configuration.set(label, String.valueOf(value));
  }

  /**
   * Utility to retrieve a specified key from a Configuration. Throw exception
   * if not found.
   * @param configuration The Configuration in which the key is sought.
   * @param label The key being sought.
   * @return Integer value of the key.
   */
  public static int getInt(Configuration configuration, String label) {
    int value = configuration.getInt(label, -1);
    assert value >= 0 : "Couldn't find " + label;
    return value;
  }

  public static int getNumMapTasks(Configuration configuration) {
    return getInt(configuration, "mapred.map.tasks");
  }

  /**
   * Utility to retrieve a specified key from a Configuration. Throw exception
   * if not found.
   * @param configuration The Configuration in which the key is sought.
   * @param label The key being sought.
   * @return Long value of the key.
   */
  public static long getLong(Configuration configuration, String label) {
    long value = configuration.getLong(label, -1);
    assert value >= 0 : "Couldn't find " + label;
    return value;
  }

  /**
   * Returns the class that implements a copy strategy. Looks up the implementation for
   * a particular strategy from distcp-default.xml
   *
   * @param conf - Configuration object
   * @param options - Handle to input options
   * @return Class implementing the strategy specified in options.
   */
  public static Class<? extends InputFormat> getStrategy(Configuration conf,
                                                                 DistCpOptions options) {
    String confLabel = "distcp." +
        options.getCopyStrategy().toLowerCase(Locale.getDefault()) + ".strategy.impl";
    return conf.getClass(confLabel, UniformSizeInputFormat.class, InputFormat.class);
  }

  /**
   * Gets relative path of child path with respect to a root path
   * For ex. If childPath = /tmp/abc/xyz/file and
   *            sourceRootPath = /tmp/abc
   * Relative path would be /xyz/file
   *         If childPath = /file and
   *            sourceRootPath = /
   * Relative path would be /file
   * @param sourceRootPath - Source root path
   * @param childPath - Path for which relative path is required
   * @return - Relative portion of the child path (always prefixed with /
   *           unless it is empty 
   */
  public static String getRelativePath(Path sourceRootPath, Path childPath) {
    String childPathString = childPath.toUri().getPath();
    String sourceRootPathString = sourceRootPath.toUri().getPath();
    return sourceRootPathString.equals("/") ? childPathString :
        childPathString.substring(sourceRootPathString.length());
  }

  /**
   * Pack file preservation attributes into a string, containing
   * just the first character of each preservation attribute
   * @param attributes - Attribute set to preserve
   * @return - String containing first letters of each attribute to preserve
   */
  public static String packAttributes(EnumSet<FileAttribute> attributes) {
    StringBuffer buffer = new StringBuffer(5);
    int len = 0;
    for (FileAttribute attribute : attributes) {
      buffer.append(attribute.name().charAt(0));
      len++;
    }
    return buffer.substring(0, len);
  }

  /**
   * Un packs preservation attribute string containing the first character of
   * each preservation attribute back to a set of attributes to preserve
   * @param attributes - Attribute string
   * @return - Attribute set
   */
  public static EnumSet<FileAttribute> unpackAttributes(String attributes) {
    EnumSet<FileAttribute> retValue = EnumSet.noneOf(FileAttribute.class);

    if (attributes != null) {
      for (int index = 0; index < attributes.length(); index++) {
        retValue.add(FileAttribute.getAttribute(attributes.charAt(index)));
      }
    }

    return retValue;
  }

  /**
   * Preserve attribute on file matching that of the file status being sent
   * as argument. Barring the block size, all the other attributes are preserved
   * by this function
   *
   * @param targetFS - File system
   * @param path - Path that needs to preserve original file status
   * @param srcFileStatus - Original file status
   * @param attributes - Attribute set that need to be preserved
   * @throws IOException - Exception if any (particularly relating to group/owner
   *                       change or any transient error)
   */
  public static void preserve(FileSystem targetFS, Path path,
                              FileStatus srcFileStatus,
                              EnumSet<FileAttribute> attributes) throws IOException {

    FileStatus targetFileStatus = targetFS.getFileStatus(path);
    String group = targetFileStatus.getGroup();
    String user = targetFileStatus.getOwner();
    boolean chown = false;

    if (attributes.contains(FileAttribute.PERMISSION) &&
      !srcFileStatus.getPermission().equals(targetFileStatus.getPermission())) {
      targetFS.setPermission(path, srcFileStatus.getPermission());
    }

    if (attributes.contains(FileAttribute.REPLICATION) && ! targetFileStatus.isDir() &&
        srcFileStatus.getReplication() != targetFileStatus.getReplication()) {
      targetFS.setReplication(path, srcFileStatus.getReplication());
    }

    if (attributes.contains(FileAttribute.GROUP) &&
            !group.equals(srcFileStatus.getGroup())) {
      group = srcFileStatus.getGroup();
      chown = true;
    }

    if (attributes.contains(FileAttribute.USER) &&
            !user.equals(srcFileStatus.getOwner())) {
      user = srcFileStatus.getOwner();
      chown = true;
    }

    if (chown) {
      targetFS.setOwner(path, user, group);
    }
  }

  /**
   * Sort sequence file containing FileStatus and Text as key and value respecitvely
   *
   * @param fs - File System
   * @param conf - Configuration
   * @param sourceListing - Source listing file
   * @return Path of the sorted file. Is source file with _sorted appended to the name
   * @throws IOException - Any exception during sort.
   */
  public static Path sortListing(FileSystem fs, Configuration conf, Path sourceListing)
      throws IOException {
    SequenceFile.Sorter sorter = new SequenceFile.Sorter(fs, Text.class, FileStatus.class, conf);
    Path output = new Path(sourceListing.toString() +  "_sorted");

    if (fs.exists(output)) {
      fs.delete(output, false);
    }

    sorter.sort(sourceListing, output);
    return output;
  }

  /**
   * String utility to convert a number-of-bytes to human readable format.
   */
  private static ThreadLocal<DecimalFormat> FORMATTER
                        = new ThreadLocal<DecimalFormat>() {
    @Override
    protected DecimalFormat initialValue() {
      return new DecimalFormat("0.0");
    }
  };

  public static DecimalFormat getFormatter() {
    return FORMATTER.get();
  }

  public static String getStringDescriptionFor(long nBytes) {

    char units [] = {'B', 'K', 'M', 'G', 'T', 'P'};

    double current = nBytes;
    double prev    = current;
    int index = 0;

    while ((current = current/1024) >= 1) {
      prev = current;
      ++index;
    }

    assert index < units.length : "Too large a number.";

    return getFormatter().format(prev) + units[index];
  }

  /**
   * Utility to compare checksums for the paths specified.
   *
   * If checksums's can't be retrieved, it doesn't fail the test
   * Only time the comparison would fail is when checksums are
   * available and they don't match
   *                                  
   * @param sourceFS FileSystem for the source path.
   * @param source The source path.
   * @param targetFS FileSystem for the target path.
   * @param target The target path.
   * @return If either checksum couldn't be retrieved, the function returns
   * false. If checksums are retrieved, the function returns true if they match,
   * and false otherwise.
   * @throws IOException if there's an exception while retrieving checksums.
   */
  public static boolean checksumsAreEqual(FileSystem sourceFS, Path source,
                                   FileSystem targetFS, Path target)
                                   throws IOException {
    FileChecksum sourceChecksum = null;
    FileChecksum targetChecksum = null;
    try {
      sourceChecksum = sourceFS.getFileChecksum(source);
      targetChecksum = targetFS.getFileChecksum(target);
    } catch (IOException e) {
      LOG.error("Unable to retrieve checksum for " + source + " or " + target, e);
    }
    return (sourceChecksum == null || targetChecksum == null ||
            sourceChecksum.equals(targetChecksum));
  }

  /* see if two file systems are the same or not
   *
   */
  public static boolean compareFs(FileSystem srcFs, FileSystem destFs) {
    URI srcUri = srcFs.getUri();
    URI dstUri = destFs.getUri();
    if (srcUri.getScheme() == null) {
      return false;
    }
    if (!srcUri.getScheme().equals(dstUri.getScheme())) {
      return false;
    }
    String srcHost = srcUri.getHost();
    String dstHost = dstUri.getHost();
    if ((srcHost != null) && (dstHost != null)) {
      try {
        srcHost = InetAddress.getByName(srcHost).getCanonicalHostName();
        dstHost = InetAddress.getByName(dstHost).getCanonicalHostName();
      } catch(UnknownHostException ue) {
        if (LOG.isDebugEnabled())
          LOG.debug("Could not compare file-systems. Unknown host: ", ue);
        return false;
      }
      if (!srcHost.equals(dstHost)) {
        return false;
      }
    }
    else if (srcHost == null && dstHost != null) {
      return false;
    }
    else if (srcHost != null) {
      return false;
    }

    //check for ports

    return srcUri.getPort() == dstUri.getPort();
  }
}
