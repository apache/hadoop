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

package org.apache.hadoop.sqoop.util;

import java.io.IOException;
import java.io.File;
import java.io.OutputStream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.sqoop.ImportOptions;
import org.apache.hadoop.util.Shell;

/**
 * Utility methods that are common to various the direct import managers.
 */
public final class DirectImportUtils {

  public static final Log LOG = LogFactory.getLog(DirectImportUtils.class.getName());

  private DirectImportUtils() {
  }

  /**
   * Executes chmod on the specified file, passing in the mode string 'modstr'
   * which may be e.g. "a+x" or "0600", etc.
   * @throws IOException if chmod failed.
   */
  public static void setFilePermissions(File file, String modstr) throws IOException {
    // Set this file to be 0600. Java doesn't have a built-in mechanism for this
    // so we need to go out to the shell to execute chmod.
    try {
      Shell.execCommand("chmod", modstr, file.toString());
    } catch (IOException ioe) {
      // Shell.execCommand will throw IOException on exit code != 0.
      LOG.error("Could not chmod " + modstr + " " + file.toString());
      throw new IOException("Could not ensure password file security.", ioe);
    }
  }

  /**
   * Open a file in HDFS for write to hold the data associated with a table.
   * Creates any necessary directories, and returns the OutputStream to write to.
   * The caller is responsible for calling the close() method on the returned
   * stream.
   */
  public static OutputStream createHdfsSink(Configuration conf, ImportOptions options,
      String tableName) throws IOException {

    FileSystem fs = FileSystem.get(conf);
    String warehouseDir = options.getWarehouseDir();
    Path destDir = null;
    if (null != warehouseDir) {
      destDir = new Path(new Path(warehouseDir), tableName);
    } else {
      destDir = new Path(tableName);
    }

    LOG.debug("Writing to filesystem: " + conf.get("fs.default.name"));
    LOG.debug("Creating destination directory " + destDir);
    fs.mkdirs(destDir);
    Path destFile = new Path(destDir, "data-00000");
    LOG.debug("Opening output file: " + destFile);
    if (fs.exists(destFile)) {
      Path canonicalDest = destFile.makeQualified(fs);
      throw new IOException("Destination file " + canonicalDest + " already exists");
    }

    // This OutputStream will be clsoed by the caller.
    return fs.create(destFile);
  }
}

