/*
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

package org.apache.hadoop.mapreduce.lib.output.committer.manifest.files;

import java.io.IOException;
import java.io.PrintStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ExitUtil;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import static org.apache.hadoop.fs.statistics.IOStatisticsLogging.ioStatisticsToPrettyString;

/**
 * Tool to print a manifest.
 */
public class ManifestPrinter extends Configured implements Tool {

  private static final String USAGE = "ManifestPrinter <success-file>";

  /**
   * Output for printing.
   */
  private final PrintStream out;

  /**
   * Print to System.out.
   */
  public ManifestPrinter() {
    this(null, System.out);
  }

  /**
   * Print to the supplied stream.
   * @param conf configuration
   * @param out output
   */
  public ManifestPrinter(Configuration conf, PrintStream out) {
    super(conf);
    this.out = out;
  }

  @Override
  public int run(String[] args) throws Exception {
    if (args.length != 1) {
      printUsage();
      return -1;
    }
    Path path = new Path(args[0]);
    loadAndPrintManifest(path.getFileSystem(getConf()), path);
    return 0;
  }

  /**
   * Load and print a manifest.
   * @param fs filesystem.
   * @param path path
   * @throws IOException failure to load
   * @return the manifest
   */
  public ManifestSuccessData loadAndPrintManifest(FileSystem fs, Path path)
      throws IOException {
    // load the manifest
    println("Manifest file: %s", path);
    final ManifestSuccessData success = ManifestSuccessData.load(fs, path);

    printManifest(success);
    return success;
  }

  private void printManifest(ManifestSuccessData success) {
    field("succeeded", success.getSuccess());
    field("created", success.getDate());
    field("committer", success.getCommitter());
    field("hostname", success.getHostname());
    field("description", success.getDescription());
    field("jobId", success.getJobId());
    field("jobIdSource", success.getJobIdSource());
    field("stage", success.getStage());
    println("Diagnostics\n%s",
        success.dumpDiagnostics("  ", " = ", "\n"));
    println("Statistics:\n%s",
        ioStatisticsToPrettyString(success.getIOStatistics()));
    out.flush();
  }

  private void printUsage() {
    println(USAGE);
  }

  /**
   * Print a line to the output stream.
   * @param format format string
   * @param args arguments.
   */
  private void println(String format, Object... args) {
    out.format(format, args);
    out.println();
  }

  /**
   * Print a field, if non-null.
   * @param name field name.
   * @param value value.
   */
  private void field(String name, Object value) {
    if (value != null) {
      println("%s: %s", name, value);
    }
  }

  /**
   * Entry point.
   */
  public static void main(String[] argv) throws Exception {

    try {
      int res = ToolRunner.run(new ManifestPrinter(), argv);
      System.exit(res);
    } catch (ExitUtil.ExitException e) {
      ExitUtil.terminate(e);
    }
  }
}
