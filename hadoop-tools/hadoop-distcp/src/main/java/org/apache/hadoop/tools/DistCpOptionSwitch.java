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

package org.apache.hadoop.tools;

import org.apache.commons.cli.Option;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;

/**
 * Enumeration mapping configuration keys to distcp command line
 * options.
 */
public enum DistCpOptionSwitch {

  /**
   * Ignores any failures during copy, and continues with rest.
   * Logs failures in a file
   */
  IGNORE_FAILURES(DistCpConstants.CONF_LABEL_IGNORE_FAILURES,
      new Option("i", false, "Ignore failures during copy")),

  /**
   * Preserves status of file/path in the target.
   * Default behavior with -p, is to preserve replication,
   * block size, user, group, permission, checksum type and timestamps on the 
   * target file. Note that when preserving checksum type, block size is also 
   * preserved.
   *
   * If any of the optional switches are present among rbugpcaxt, then
   * only the corresponding file attribute is preserved.
   */
  PRESERVE_STATUS(DistCpConstants.CONF_LABEL_PRESERVE_STATUS,
      new Option("p", true, "preserve status (rbugpcaxt)(replication, " +
          "block-size, user, group, permission, checksum-type, ACL, XATTR, " +
          "timestamps). If -p is specified with no <arg>, then preserves " +
          "replication, block size, user, group, permission, checksum type " +
          "and timestamps. " +
          "raw.* xattrs are preserved when both the source and destination " +
          "paths are in the /.reserved/raw hierarchy (HDFS only). raw.* xattr" +
          "preservation is independent of the -p flag. " +
          "Refer to the DistCp documentation for more details.")),

  /**
   * Update target location by copying only files that are missing
   * in the target. This can be used to periodically sync two folders
   * across source and target. Typically used with DELETE_MISSING
   * Incompatible with ATOMIC_COMMIT
   */
  SYNC_FOLDERS(DistCpConstants.CONF_LABEL_SYNC_FOLDERS,
      new Option("update", false, "Update target, copying only missing" +
          " files or directories")),

  /**
   * Deletes missing files in target that are missing from source.
   * This allows the target to be in sync with the source contents
   * Typically used in conjunction with SYNC_FOLDERS
   * Incompatible with ATOMIC_COMMIT
   */
  DELETE_MISSING(DistCpConstants.CONF_LABEL_DELETE_MISSING,
      new Option("delete", false, "Delete from target, " +
          "files missing in source. Delete is applicable only with update or overwrite options")),

  /**
   * Track missing files in target that are missing from source
   * This allows for other applications to complete the synchronization,
   * possibly with object-store-specific delete algorithms.
   * Typically used in conjunction with SYNC_FOLDERS
   * Incompatible with ATOMIC_COMMIT
   */
  @InterfaceStability.Unstable
  TRACK_MISSING(DistCpConstants.CONF_LABEL_TRACK_MISSING,
      new Option("xtrack", true,
          "Save information about missing source files to the"
              + " specified directory")),


  /**
   * Number of threads for building source file listing (before map-reduce
   * phase, max one listStatus per thread at a time).
   */
  NUM_LISTSTATUS_THREADS(DistCpConstants.CONF_LABEL_LISTSTATUS_THREADS,
      new Option("numListstatusThreads", true, "Number of threads to " +
          "use for building file listing (max " +
          DistCpOptions.MAX_NUM_LISTSTATUS_THREADS + ").")),
  /**
   * Max number of maps to use during copy. DistCp will split work
   * as equally as possible among these maps
   */
  MAX_MAPS(DistCpConstants.CONF_LABEL_MAX_MAPS,
      new Option("m", true, "Max number of concurrent maps to use for copy")),

  /**
   * Source file listing can be provided to DistCp in a file.
   * This allows DistCp to copy random list of files from source
   * and copy them to target
   */
  SOURCE_FILE_LISTING(DistCpConstants.CONF_LABEL_SOURCE_LISTING,
      new Option("f", true, "List of files that need to be copied")),

  /**
   * Copy all the source files and commit them atomically to the target
   * This is typically useful in cases where there is a process
   * polling for availability of a file/dir. This option is incompatible
   * with SYNC_FOLDERS and DELETE_MISSING
   */
  ATOMIC_COMMIT(DistCpConstants.CONF_LABEL_ATOMIC_COPY,
      new Option("atomic", false, "Commit all changes or none")),

  /**
   * Work path to be used only in conjunction in Atomic commit
   */
  WORK_PATH(DistCpConstants.CONF_LABEL_WORK_PATH,
      new Option("tmp", true, "Intermediate work path to be used for atomic commit")),

  /**
   * Log path where distcp output logs are written to
   */
  LOG_PATH(DistCpConstants.CONF_LABEL_LOG_PATH,
      new Option("log", true, "Folder on DFS where distcp execution logs are saved")),

  /**
   * Log additional info (path, size) in the SKIP/COPY log.
   */
  VERBOSE_LOG(DistCpConstants.CONF_LABEL_VERBOSE_LOG,
      new Option("v", false,
          "Log additional info (path, size) in the SKIP/COPY log")),

  /**
   * Copy strategy is use. This could be dynamic or uniform size etc.
   * DistCp would use an appropriate input format based on this.
   */
  COPY_STRATEGY(DistCpConstants.CONF_LABEL_COPY_STRATEGY,
      new Option("strategy", true, "Copy strategy to use. Default is " +
          "dividing work based on file sizes")),

  /**
   * Skip CRC checks between source and target, when determining what
   * files need to be copied.
   */
  SKIP_CRC(DistCpConstants.CONF_LABEL_SKIP_CRC,
      new Option("skipcrccheck", false, "Whether to skip CRC checks between " +
          "source and target paths.")),

  /**
   * Overwrite target-files unconditionally.
   */
  OVERWRITE(DistCpConstants.CONF_LABEL_OVERWRITE,
      new Option("overwrite", false, "Choose to overwrite target files " +
          "unconditionally, even if they exist.")),

  APPEND(DistCpConstants.CONF_LABEL_APPEND,
      new Option("append", false,
          "Reuse existing data in target files and append new data to them if possible")),

  DIFF(DistCpConstants.CONF_LABEL_DIFF,
      new Option("diff", false,
      "Use snapshot diff report to identify the difference between source and target"),
      2),

  RDIFF(DistCpConstants.CONF_LABEL_RDIFF,
      new Option("rdiff", false,
      "Use target snapshot diff report to identify changes made on target"),
      2),

  /**
   * Should DisctpExecution be blocking
   */
  BLOCKING("",
      new Option("async", false, "Should distcp execution be blocking")),

  FILE_LIMIT("",
      new Option("filelimit", true, "(Deprecated!) Limit number of files " +
              "copied to <= n")),

  SIZE_LIMIT("",
      new Option("sizelimit", true, "(Deprecated!) Limit number of files " +
              "copied to <= n bytes")),

  BLOCKS_PER_CHUNK(DistCpConstants.CONF_LABEL_BLOCKS_PER_CHUNK,
      new Option("blocksperchunk", true, "If set to a positive value, files"
          + "with more blocks than this value will be split into chunks of "
          + "<blocksperchunk> blocks to be transferred in parallel, and "
          + "reassembled on the destination. By default, <blocksperchunk> is "
          + "0 and the files will be transmitted in their entirety without "
          + "splitting. This switch is only applicable when the source file "
          + "system implements getBlockLocations method and the target file "
          + "system implements concat method")),

  /**
   * Configurable copy buffer size.
   */
  COPY_BUFFER_SIZE(DistCpConstants.CONF_LABEL_COPY_BUFFER_SIZE,
      new Option("copybuffersize", true, "Size of the copy buffer to use. "
          + "By default <copybuffersize> is "
          + DistCpConstants.COPY_BUFFER_SIZE_DEFAULT + "B.")),

  /**
   * Specify bandwidth per map in MB, accepts bandwidth as a fraction
   */
  BANDWIDTH(DistCpConstants.CONF_LABEL_BANDWIDTH_MB,
      new Option("bandwidth", true, "Specify bandwidth per map in MB,"
          + " accepts bandwidth as a fraction.")),

  /**
   * Path containing a list of strings, which when found in the path of
   * a file to be copied excludes that file from the copy job.
   */
  FILTERS(DistCpConstants.CONF_LABEL_FILTERS_FILE,
      new Option("filters", true, "The path to a file containing a list of"
          + " strings for paths to be excluded from the copy.")),

  /**
   * Write directly to the final location, avoiding the creation and rename
   * of temporary files.
   * This is typically useful in cases where the target filesystem
   * implementation does not support atomic rename operations, such as with
   * the S3AFileSystem which translates file renames to potentially very
   * expensive copy-then-delete operations.
   */
  DIRECT_WRITE(DistCpConstants.CONF_LABEL_DIRECT_WRITE,
      new Option("direct", false, "Write files directly to the"
          + " target location, avoiding temporary file rename."));


  public static final String PRESERVE_STATUS_DEFAULT = "-prbugpct";
  private final String confLabel;
  private final Option option;

  DistCpOptionSwitch(String confLabel, Option option) {
    this.confLabel = confLabel;
    this.option = option;
  }

  DistCpOptionSwitch(String confLabel, Option option, int argNum) {
    this(confLabel, option);
    this.option.setArgs(argNum);
  }

  /**
   * Get Configuration label for the option
   * @return configuration label name
   */
  public String getConfigLabel() {
    return confLabel;
  }

  /**
   * Get CLI Option corresponding to the distcp option
   * @return option
   */
  public Option getOption() {
    return option;
  }

  /**
   * Get Switch symbol
   * @return switch symbol char
   */
  public String getSwitch() {
    return option.getOpt();
  }

  @Override
  public String toString() {
    return  super.name() + " {" +
        "confLabel='" + confLabel + '\'' +
        ", option=" + option + '}';
  }

  /**
   * Helper function to add an option to hadoop configuration object
   * @param conf - Configuration object to include the option
   * @param option - Option to add
   * @param value - Value
   */
  public static void addToConf(Configuration conf,
                               DistCpOptionSwitch option,
                               String value) {
    conf.set(option.getConfigLabel(), value);
  }

  /**
   * Helper function to set an option to hadoop configuration object
   * @param conf - Configuration object to include the option
   * @param option - Option to add
   */
  public static void addToConf(Configuration conf,
                               DistCpOptionSwitch option) {
    conf.set(option.getConfigLabel(), "true");
  }
}
