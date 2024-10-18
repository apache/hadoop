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

import java.io.Closeable;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3a.audit.mapreduce.S3AAuditLogMergerAndParser;
import org.apache.hadoop.util.ExitUtil;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import static org.apache.hadoop.service.launcher.LauncherExitCodes.EXIT_COMMAND_ARGUMENT_ERROR;
import static org.apache.hadoop.service.launcher.LauncherExitCodes.EXIT_FAIL;
import static org.apache.hadoop.service.launcher.LauncherExitCodes.EXIT_SUCCESS;

/**
 * AuditTool is a Command Line Interface.
 * Its functionality is to parse the audit log files
 * and generate avro file.
 */
public class AuditTool extends Configured implements Tool, Closeable {

  private static final Logger LOG = LoggerFactory.getLogger(AuditTool.class);

  private final S3AAuditLogMergerAndParser s3AAuditLogMergerAndParser =
      new S3AAuditLogMergerAndParser();

  /**
   * Name of this tool: {@value}.
   */
  public static final String AUDITTOOL =
      "org.apache.hadoop.fs.s3a.audit.AuditTool";

  /**
   * Purpose of this tool: {@value}.
   */
  public static final String PURPOSE =
      "\n\nUSAGE:\nMerge, parse audit log files and convert into avro file "
          + "for "
          + "better "
          + "visualization";

  // Exit codes
  private static final int SUCCESS = EXIT_SUCCESS;
  private static final int FAILURE = EXIT_FAIL;
  private static final int INVALID_ARGUMENT = EXIT_COMMAND_ARGUMENT_ERROR;

  private static final String USAGE =
      "bin/hadoop " + "Class" + " DestinationPath" + " SourcePath" + "\n" +
          "bin/hadoop " + AUDITTOOL + " s3a://BUCKET" + " s3a://BUCKET" + "\n";

  private PrintWriter out;

  public AuditTool() {
    super();
  }

  /**
   * Tells us the usage of the AuditTool by commands.
   *
   * @return the string USAGE
   */
  public String getUsage() {
    return USAGE + PURPOSE;
  }

  public String getName() {
    return AUDITTOOL;
  }

  /**
   * This run method in AuditTool takes source and destination path of bucket,
   * and check if there are directories and pass these paths to merge and
   * parse audit log files.
   *
   * @param args argument list
   * @return SUCCESS i.e, '0', which is an exit code
   * @throws Exception on any failure.
   */
  @Override
  public int run(String[] args) throws Exception {
    List<String> paths = Arrays.asList(args);
    if(paths.size() == 2) {
      // Path of audit log files
      Path logsPath = new Path(paths.get(1));
      // Path of destination directory
      Path destPath = new Path(paths.get(0));

      // Setting the file system
      URI fsURI = new URI(logsPath.toString());
      FileSystem fileSystem = FileSystem.get(fsURI, new Configuration());

      FileStatus fileStatus = fileSystem.getFileStatus(logsPath);
      if (fileStatus.isFile()) {
        errorln("Expecting a directory, but " + logsPath.getName() + " is a"
            + " file which was passed as an argument");
        throw invalidArgs(
            "Expecting a directory, but " + logsPath.getName() + " is a"
                + " file which was passed as an argument");
      }
      FileStatus fileStatus1 = fileSystem.getFileStatus(destPath);
      if (fileStatus1.isFile()) {
        errorln("Expecting a directory, but " + destPath.getName() + " is a"
            + " file which was passed as an argument");
        throw invalidArgs(
            "Expecting a directory, but " + destPath.getName() + " is a"
                + " file which was passed as an argument");
      }

      // Calls S3AAuditLogMergerAndParser for implementing merging, passing of
      // audit log files and converting into avro file
      boolean mergeAndParseResult =
          s3AAuditLogMergerAndParser.mergeAndParseAuditLogFiles(
              fileSystem, logsPath, destPath);
      if (!mergeAndParseResult) {
        return FAILURE;
      }
    } else {
      errorln(getUsage());
      throw invalidArgs("Invalid number of arguments, please specify audit "
          + "log files directory as 1st argument and destination directory "
          + "as 2nd argument");
    }
    return SUCCESS;
  }

  protected static void errorln(String x) {
    System.err.println(x);
  }

  /**
   * Build the exception to raise on invalid arguments.
   *
   * @param format string format
   * @param args   optional arguments for the string
   * @return a new exception to throw
   */
  protected static ExitUtil.ExitException invalidArgs(
      String format, Object... args) {
    return exitException(INVALID_ARGUMENT, format, args);
  }

  /**
   * Build a exception to throw with a formatted message.
   *
   * @param exitCode exit code to use
   * @param format   string format
   * @param args     optional arguments for the string
   * @return a new exception to throw
   */
  protected static ExitUtil.ExitException exitException(
      final int exitCode,
      final String format,
      final Object... args) {
    return new ExitUtil.ExitException(exitCode,
        String.format(format, args));
  }

  /**
   * Convert a path to a URI, catching any {@code URISyntaxException}
   * and converting to an invalid args exception.
   *
   * @param s3Path path to convert to a URI
   * @return a URI of the path
   * @throws ExitUtil.ExitException INVALID_ARGUMENT if the URI is invalid
   */
  protected static URI toUri(String s3Path) {
    URI uri;
    try {
      uri = new URI(s3Path);
    } catch (URISyntaxException e) {
      throw invalidArgs("Not a valid fileystem path: %s", s3Path);
    }
    return uri;
  }

  /**
   * Flush all active output channels, including {@Code System.err},
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

  @Override
  public void close() throws IOException {
    flush();
    if (out != null) {
      out.close();
    }
  }

  /**
   * Inner entry point, with no logging or system exits.
   *
   * @param conf configuration
   * @param argv argument list
   * @return an exception
   * @throws Exception Exception.
   */
  public static int exec(Configuration conf, String... argv) throws Exception {
    try (AuditTool auditTool = new AuditTool()) {
      return ToolRunner.run(conf, auditTool, argv);
    }
  }

  /**
   * Main entry point.
   *
   * @param argv args list
   */
  public static void main(String[] argv) {
    try {
      ExitUtil.terminate(exec(new Configuration(), argv));
    } catch (ExitUtil.ExitException e) {
      LOG.error(e.toString());
      System.exit(e.status);
    } catch (Exception e) {
      LOG.error(e.toString(), e);
      ExitUtil.halt(-1, e);
    }
  }
}
