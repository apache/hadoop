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

package org.apache.hadoop.fs.s3a.audit;

import java.io.PrintStream;
import java.net.URI;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.apache.hadoop.fs.s3a.s3guard.S3GuardTool;

import static org.apache.hadoop.service.launcher.LauncherExitCodes.EXIT_FAIL;
import static org.apache.hadoop.service.launcher.LauncherExitCodes.EXIT_SUCCESS;

/**
 * AuditTool is a Command Line Interface.
 * Its functionality is to parse the audit log files
 * and generate avro file.
 */
public class AuditTool extends S3GuardTool {

  private static final Logger LOG = LoggerFactory.getLogger(AuditTool.class);

  private final S3AAuditLogMergerAndParser s3AAuditLogMergerAndParser =
      new S3AAuditLogMergerAndParser();

  /**
   * Name of this tool: {@value}.
   */
  public static final String AUDIT = "s3audit";

  /**
   * Purpose of this tool: {@value}.
   */
  public static final String PURPOSE =
      "Merge, parse audit log files and convert into avro file for better "
          + "visualization";

  // Exit codes
  private static final int SUCCESS = EXIT_SUCCESS;
  private static final int FAILURE = EXIT_FAIL;

  private static final String USAGE =
      "s3guard " + AUDIT + " DestinationPath" + " SourcePath" + "\n" +
          "s3guard " + AUDIT + " s3a://BUCKET" + " s3a://BUCKET" + "\n";

  public AuditTool(Configuration conf) {
    super(conf);
  }

  /**
   * Tells us the usage of the AuditTool by commands.
   *
   * @return the string USAGE
   */
  @Override
  public String getUsage() {
    return USAGE;
  }

  @Override
  public String getName() {
    return AUDIT;
  }

  /**
   * This run method in AuditTool takes source and destination path of bucket,
   * and check if there are directories and pass these paths to merge and
   * parse audit log files.
   *
   * @param args argument list
   * @param out  output stream
   * @throws Exception on any failure.
   * @return SUCCESS i.e, '0', which is an exit code
   */
  @Override
  public int run(String[] args, PrintStream out)
      throws Exception {
    List<String> paths = parseArgs(args);
    if (paths.isEmpty()) {
      errorln(getUsage());
      throw invalidArgs("No bucket specified");
    }

    // Path of audit log files in s3 bucket
    Path s3LogsPath = new Path(paths.get(1));
    // Path of destination directory in s3 bucket
    Path s3DestPath = new Path(paths.get(0));

    // Setting the file system
    URI fsURI = toUri(String.valueOf(s3LogsPath));
    S3AFileSystem s3AFileSystem =
        bindFilesystem(FileSystem.get(fsURI, getConf()));

    FileStatus fileStatus = s3AFileSystem.getFileStatus(s3LogsPath);
    if (fileStatus.isFile()) {
      errorln("Expecting a directory, but " + s3LogsPath.getName() + " is a"
          + " file which was passed as an argument");
      throw invalidArgs(
          "Expecting a directory, but " + s3LogsPath.getName() + " is a"
              + " file which was passed as an argument");
    }
    FileStatus fileStatus1 = s3AFileSystem.getFileStatus(s3DestPath);
    if (fileStatus1.isFile()) {
      errorln("Expecting a directory, but " + s3DestPath.getName() + " is a"
          + " file which was passed as an argument");
      throw invalidArgs(
          "Expecting a directory, but " + s3DestPath.getName() + " is a"
              + " file which was passed as an argument");
    }

    // Calls S3AAuditLogMergerAndParser for implementing merging, passing of
    // audit log files and converting into avro file
    boolean mergeAndParseResult = s3AAuditLogMergerAndParser.mergeAndParseAuditLogFiles(
        s3AFileSystem, s3LogsPath, s3DestPath);
    if (!mergeAndParseResult) {
      return FAILURE;
    }
    return SUCCESS;
  }
}
