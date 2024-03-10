/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.fs.compat;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.security.PrivilegedExceptionAction;
import java.util.Collection;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.compat.common.HdfsCompatCommand;
import org.apache.hadoop.fs.compat.common.HdfsCompatIllegalArgumentException;
import org.apache.hadoop.fs.compat.common.HdfsCompatReport;
import org.apache.hadoop.fs.shell.CommandFormat;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.util.VersionInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tool for triggering a compatibility report
 * for a specific FileSystem implementation.
 */
public class HdfsCompatTool extends Configured implements Tool {
  private static final Logger LOG =
      LoggerFactory.getLogger(HdfsCompatTool.class);

  private static final String DESCRIPTION = "hadoop jar" +
      " hadoop-compat-bench-{version}.jar -uri <uri>" +
      " [-suite <suiteName>] [-output <outputFile>]:\n" +
      "\tTrigger a compatibility assessment" +
      " for a specific FileSystem implementation.\n" +
      "\tA compatibility report is generated after the command finished," +
      " showing how many interfaces/functions are implemented" +
      " and compatible with HDFS definition.\n" +
      "\t-uri is required to determine the target FileSystem.\n" +
      "\t-suite is optional for limiting the assessment to a subset." +
      " For example, 'shell' means only shell commands.\n" +
      "\t-output is optional for a detailed report," +
      " which should be a local file path if provided.";

  private final PrintStream out; // Stream for printing command output
  private final PrintStream err; // Stream for printing error
  private String uri = null;
  private String suite = null;
  private String output = null;

  public HdfsCompatTool(Configuration conf) {
    this(conf, System.out, System.err);
  }

  public HdfsCompatTool(Configuration conf, PrintStream out, PrintStream err) {
    super(conf);
    this.out = out;
    this.err = err;
  }

  @Override
  public int run(final String[] args) throws Exception {
    try {
      return UserGroupInformation.getCurrentUser().doAs(
          new PrivilegedExceptionAction<Integer>() {
            @Override
            public Integer run() {
              return runImpl(args);
            }
          });
    } catch (InterruptedException e) {
      throw new IOException(e);
    }
  }

  /**
   * Main method that runs the tool for given arguments.
   *
   * @param args arguments
   * @return return status of the command
   */
  private int runImpl(String[] args) {
    if (isHelp(args)) {
      printUsage();
      return 0;
    }
    try {
      parseArgs(args);
      return doRun();
    } catch (Exception e) {
      printError(e.getMessage());
      return -1;
    }
  }

  private int doRun() throws Exception {
    HdfsCompatCommand cmd = new HdfsCompatCommand(uri, suite, getConf());
    cmd.initialize();
    HdfsCompatReport report = cmd.apply();
    OutputStream outputFile = null;
    try {
      if (this.output != null) {
        outputFile = new FileOutputStream(new File(this.output));
      }
    } catch (Exception e) {
      LOG.error("Create output file failed", e);
      outputFile = null;
    }
    try {
      printReport(report, outputFile);
    } finally {
      IOUtils.closeStream(outputFile);
    }
    return 0;
  }

  private boolean isHelp(String[] args) {
    if ((args == null) || (args.length == 0)) {
      return true;
    }
    return (args.length == 1) && (args[0].equalsIgnoreCase("-h") ||
        args[0].equalsIgnoreCase("--help"));
  }

  private void parseArgs(String[] args) {
    CommandFormat cf = new CommandFormat(0, Integer.MAX_VALUE);
    cf.addOptionWithValue("uri");
    cf.addOptionWithValue("suite");
    cf.addOptionWithValue("output");
    cf.parse(args, 0);
    this.uri = cf.getOptValue("uri");
    this.suite = cf.getOptValue("suite");
    this.output = cf.getOptValue("output");
    if (isEmpty(this.uri)) {
      throw new HdfsCompatIllegalArgumentException("-uri is not specified.");
    }
    if (isEmpty(this.suite)) {
      this.suite = "ALL";
    }
  }

  private boolean isEmpty(final String value) {
    return (value == null) || value.isEmpty();
  }

  private void printError(String message) {
    err.println(message);
  }

  private void printOut(String message) {
    out.println(message);
  }

  public void printReport(HdfsCompatReport report, OutputStream detailStream)
      throws IOException {
    StringBuilder buffer = new StringBuilder();

    // Line 1:
    buffer.append("Hadoop Compatibility Report for ");
    buffer.append(report.getSuite().getSuiteName());
    buffer.append(":\n");

    // Line 2:
    long passed = report.getPassedCase().size();
    long failed = report.getFailedCase().size();
    String percent = (failed == 0) ? "100" : String.format("%.2f",
        ((double) passed) / ((double) (passed + failed)) * 100);
    buffer.append("\t");
    buffer.append(percent);
    buffer.append("%, PASSED ");
    buffer.append(passed);
    buffer.append(" OVER ");
    buffer.append(passed + failed);
    buffer.append("\n");

    // Line 3:
    buffer.append("\tURI: ");
    buffer.append(report.getUri());
    if (report.getSuite() != null) {
      buffer.append(" (suite: ");
      buffer.append(report.getSuite().getClass().getName());
      buffer.append(")");
    }
    buffer.append("\n");

    // Line 4:
    buffer.append("\tHadoop Version as Baseline: ");
    buffer.append(VersionInfo.getVersion());

    final String shortMessage = buffer.toString();
    printOut(shortMessage);

    if (detailStream != null) {
      detailStream.write(shortMessage.getBytes(StandardCharsets.UTF_8));
      BufferedWriter writer = new BufferedWriter(
          new OutputStreamWriter(detailStream, StandardCharsets.UTF_8));
      writer.newLine();
      writer.write("PASSED CASES:");
      writer.newLine();
      Collection<String> cases = report.getPassedCase();
      for (String c : cases) {
        writer.write('\t');
        writer.write(c);
        writer.newLine();
        writer.flush();
      }
      writer.write("FAILED CASES:");
      writer.newLine();
      cases = report.getFailedCase();
      for (String c : cases) {
        writer.write('\t');
        writer.write(c);
        writer.newLine();
        writer.flush();
      }
      writer.flush();
    }
  }

  private void printUsage() {
    printError(DESCRIPTION);
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new HdfsCompatTool(new Configuration()), args);
    System.exit(res);
  }
}
