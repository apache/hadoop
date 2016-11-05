package org.apache.hadoop.tools;

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

import org.apache.hadoop.fs.Path;

/**
 * Utility class to hold commonly used constants.
 */
public class DistCpConstants {

  /* Default number of threads to use for building file listing */
  public static final int DEFAULT_LISTSTATUS_THREADS = 1;

  /* Default number of maps to use for DistCp */
  public static final int DEFAULT_MAPS = 20;

  /* Default bandwidth if none specified */
  public static final float DEFAULT_BANDWIDTH_MB = 100;

  /* Default strategy for copying. Implementation looked up
     from distcp-default.xml
   */
  public static final String UNIFORMSIZE = "uniformsize";

  /**
   *  Constants mapping to command line switches/input options
   */
  public static final String CONF_LABEL_ATOMIC_COPY = "distcp.atomic.copy";
  public static final String CONF_LABEL_WORK_PATH = "distcp.work.path";
  public static final String CONF_LABEL_LOG_PATH = "distcp.log.path";
  public static final String CONF_LABEL_IGNORE_FAILURES = "distcp.ignore.failures";
  public static final String CONF_LABEL_PRESERVE_STATUS = "distcp.preserve.status";
  public static final String CONF_LABEL_PRESERVE_RAWXATTRS =
      "distcp.preserve.rawxattrs";
  public static final String CONF_LABEL_SYNC_FOLDERS = "distcp.sync.folders";
  public static final String CONF_LABEL_DELETE_MISSING = "distcp.delete.missing.source";
  public static final String CONF_LABEL_LISTSTATUS_THREADS = "distcp.liststatus.threads";
  public static final String CONF_LABEL_MAX_MAPS = "distcp.max.maps";
  public static final String CONF_LABEL_SOURCE_LISTING = "distcp.source.listing";
  public static final String CONF_LABEL_COPY_STRATEGY = "distcp.copy.strategy";
  public static final String CONF_LABEL_SKIP_CRC = "distcp.skip.crc";
  public static final String CONF_LABEL_OVERWRITE = "distcp.copy.overwrite";
  public static final String CONF_LABEL_APPEND = "distcp.copy.append";
  public static final String CONF_LABEL_DIFF = "distcp.copy.diff";
  public static final String CONF_LABEL_RDIFF = "distcp.copy.rdiff";
  public static final String CONF_LABEL_BANDWIDTH_MB = "distcp.map.bandwidth.mb";
  public static final String CONF_LABEL_SIMPLE_LISTING_FILESTATUS_SIZE =
      "distcp.simplelisting.file.status.size";
  public static final String CONF_LABEL_SIMPLE_LISTING_RANDOMIZE_FILES =
      "distcp.simplelisting.randomize.files";
  public static final String CONF_LABEL_FILTERS_FILE =
      "distcp.filters.file";
  public static final String CONF_LABEL_MAX_CHUNKS_TOLERABLE =
      "distcp.dynamic.max.chunks.tolerable";
  public static final String CONF_LABEL_MAX_CHUNKS_IDEAL =
      "distcp.dynamic.max.chunks.ideal";
  public static final String CONF_LABEL_MIN_RECORDS_PER_CHUNK =
      "distcp.dynamic.min.records_per_chunk";
  public static final String CONF_LABEL_SPLIT_RATIO =
      "distcp.dynamic.split.ratio";
  
  /* Total bytes to be copied. Updated by copylisting. Unfiltered count */
  public static final String CONF_LABEL_TOTAL_BYTES_TO_BE_COPIED = "mapred.total.bytes.expected";

  /* Total number of paths to copy, includes directories. Unfiltered count */
  public static final String CONF_LABEL_TOTAL_NUMBER_OF_RECORDS = "mapred.number.of.records";

  /* If input is based -f <<source listing>>, file containing the src paths */
  public static final String CONF_LABEL_LISTING_FILE_PATH = "distcp.listing.file.path";

  /* Directory where the mapreduce job will write to. If not atomic commit, then same
    as CONF_LABEL_TARGET_FINAL_PATH
   */
  public static final String CONF_LABEL_TARGET_WORK_PATH = "distcp.target.work.path";

  /* Directory where the final data will be committed to. If not atomic commit, then same
    as CONF_LABEL_TARGET_WORK_PATH
   */
  public static final String CONF_LABEL_TARGET_FINAL_PATH = "distcp.target.final.path";

  /* Boolean to indicate whether the target of distcp exists. */
  public static final String CONF_LABEL_TARGET_PATH_EXISTS = "distcp.target.path.exists";
  
  /**
   * DistCp job id for consumers of the Disctp 
   */
  public static final String CONF_LABEL_DISTCP_JOB_ID = "distcp.job.id";

  /* Meta folder where the job's intermediate data is kept */
  public static final String CONF_LABEL_META_FOLDER = "distcp.meta.folder";

  /* DistCp CopyListing class override param */
  public static final String CONF_LABEL_COPY_LISTING_CLASS = "distcp.copy.listing.class";

  /**
   * Constants for DistCp return code to shell / consumer of ToolRunner's run
   */
  public static final int SUCCESS = 0;
  public static final int INVALID_ARGUMENT = -1;
  public static final int DUPLICATE_INPUT = -2;
  public static final int ACLS_NOT_SUPPORTED = -3;
  public static final int XATTRS_NOT_SUPPORTED = -4;
  public static final int UNKNOWN_ERROR = -999;
  
  /**
   * Constants for DistCp default values of configurable values
   */
  public static final int MAX_CHUNKS_TOLERABLE_DEFAULT = 400;
  public static final int MAX_CHUNKS_IDEAL_DEFAULT     = 100;
  public static final int MIN_RECORDS_PER_CHUNK_DEFAULT = 5;
  public static final int SPLIT_RATIO_DEFAULT  = 2;

  /**
   * Constants for NONE file deletion
   */
  public static final String NONE_PATH_NAME = "/NONE";
  public static final Path NONE_PATH = new Path(NONE_PATH_NAME);
  public static final Path RAW_NONE_PATH = new Path(
      DistCpConstants.HDFS_RESERVED_RAW_DIRECTORY_NAME + NONE_PATH_NAME);

  /**
   * Value of reserved raw HDFS directory when copying raw.* xattrs.
   */
  public static final String HDFS_RESERVED_RAW_DIRECTORY_NAME = "/.reserved/raw";

  static final String HDFS_DISTCP_DIFF_DIRECTORY_NAME = ".distcp.diff.tmp";
}
