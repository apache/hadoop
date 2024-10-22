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

import java.io.IOException;
import java.io.PrintStream;
import java.util.Arrays;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3a.audit.mapreduce.S3AAuditLogMergerAndParser;
import org.apache.hadoop.fs.s3a.s3guard.S3GuardTool;
import org.apache.hadoop.util.ExitUtil;

import static org.apache.hadoop.service.launcher.LauncherExitCodes.EXIT_COMMAND_ARGUMENT_ERROR;
import static org.apache.hadoop.service.launcher.LauncherExitCodes.EXIT_FAIL;
import static org.apache.hadoop.service.launcher.LauncherExitCodes.EXIT_SUCCESS;

/**
 * AuditTool is a Command Line Interface.
 * Its functionality is to parse the audit log files
 * and generate avro file.
 */
public class AuditTool extends S3GuardTool {

  private static final Logger LOG = LoggerFactory.getLogger(AuditTool.class);

  /**
   * Name of audit tool: {@value}.
   */
  public static final String AUDIT = "audit";


  /**
   * Name of this tool: {@value}.
   */
  public static final String AUDIT_TOOL =
      "org.apache.hadoop.fs.s3a.audit.AuditTool";

  /**
   * Purpose of this tool: {@value}.
   */
  public static final String PURPOSE =
      "\n\nUSAGE:\nMerge and parse audit log files and convert into avro files "
          + "for "
          + "better "
          + "visualization";

  // Exit codes
  private static final int SUCCESS = EXIT_SUCCESS;

  private static final int FAILURE = EXIT_FAIL;

  private static final int INVALID_ARGUMENT = EXIT_COMMAND_ARGUMENT_ERROR;

  private static final int SAMPLE = 500;

  private static final String USAGE =
      "hadoop " + AUDIT_TOOL +
          " <path of source files>" +
          " <path of output file>"
          + "\n";

  private PrintStream out;

  public AuditTool(final Configuration conf) {
    super(conf);
  }

  /**
   * Tells us the usage of the AuditTool by commands.
   * @return the string USAGE
   */
  public String getUsage() {
    return USAGE + PURPOSE;
  }

  public String getName() {
    return AUDIT_TOOL;
  }

  /**
   * This run method in AuditTool takes source and destination path of bucket,
   * and checks if there are directories and pass these paths to merge and
   * parse audit log files.
   * @param args argument list
   * @param stream output stream
   * @return SUCCESS i.e, '0', which is an exit code
   * @throws Exception on any failure.
   */
  @Override
  public int run(final String[] args, final PrintStream stream)
      throws ExitUtil.ExitException, Exception {

    this.out = stream;

    preConditionArgsSizeCheck(args);
    List<String> paths = Arrays.asList(args);

    // Path of audit log files
    Path logsPath = new Path(paths.get(0));

    // Path of destination file
    Path destPath = new Path(paths.get(1));

    final S3AAuditLogMergerAndParser auditLogMergerAndParser =
        new S3AAuditLogMergerAndParser(getConf(), SAMPLE);

    // Calls S3AAuditLogMergerAndParser for implementing merging, passing of
    // audit log files and converting into avro file
    boolean mergeAndParseResult =
        auditLogMergerAndParser.mergeAndParseAuditLogFiles(
            logsPath, destPath);
    if (!mergeAndParseResult) {
      return FAILURE;
    }

    return SUCCESS;
  }

  private void preConditionArgsSizeCheck(String[] args) {
    if (args.length != 2) {
      errorln(getUsage());
      throw invalidArgs("Invalid number of arguments");
    }
  }


  /**
   * Flush all active output channels, including {@code System.err},
   * so as to stay in sync with any JRE log messages.
   */
  private void flush() {
    if (out != null) {
      out.flush();
    } else {
      System.out.flush();
    }
    System.err.flush();
  }


  public void closeOutput() throws IOException {
    flush();
    if (out != null) {
      out.close();
    }
  }

}
