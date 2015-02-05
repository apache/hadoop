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
package org.apache.hadoop.fs.shell;

import java.io.IOException;
import java.util.Arrays;
import java.util.LinkedList;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FsShell;

/**
 * Count the number of directories, files, bytes, quota, and remaining quota.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving

public class Count extends FsCommand {
  /**
   * Register the names for the count command
   * @param factory the command factory that will instantiate this class
   */
  public static void registerCommands(CommandFactory factory) {
    factory.addClass(Count.class, "-count");
  }

  private static final String OPTION_QUOTA = "q";
  private static final String OPTION_HUMAN = "h";
  private static final String OPTION_HEADER = "v";

  public static final String NAME = "count";
  public static final String USAGE =
      "[-" + OPTION_QUOTA + "] [-" + OPTION_HUMAN + "] [-" + OPTION_HEADER
          + "] <path> ...";
  public static final String DESCRIPTION =
      "Count the number of directories, files and bytes under the paths\n" +
          "that match the specified file pattern.  The output columns are:\n" +
          StringUtils.join(ContentSummary.getHeaderFields(), ' ') +
          " PATHNAME\n" +
          "or, with the -" + OPTION_QUOTA + " option:\n" +
          StringUtils.join(ContentSummary.getQuotaHeaderFields(), ' ') + "\n" +
          "      " +
          StringUtils.join(ContentSummary.getHeaderFields(), ' ') +
          " PATHNAME\n" +
          "The -" + OPTION_HUMAN +
          " option shows file sizes in human readable format.\n" +
          "The -" + OPTION_HEADER + " option displays a header line.";

  private boolean showQuotas;
  private boolean humanReadable;

  /** Constructor */
  public Count() {}
  
  /** Constructor
   * @deprecated invoke via {@link FsShell}
   * @param cmd the count command
   * @param pos the starting index of the arguments
   * @param conf configuration
   */
  @Deprecated
  public Count(String[] cmd, int pos, Configuration conf) {
    super(conf);
    this.args = Arrays.copyOfRange(cmd, pos, cmd.length);
  }

  @Override
  protected void processOptions(LinkedList<String> args) {
    CommandFormat cf = new CommandFormat(1, Integer.MAX_VALUE,
        OPTION_QUOTA, OPTION_HUMAN, OPTION_HEADER);
    cf.parse(args);
    if (args.isEmpty()) { // default path is the current working directory
      args.add(".");
    }
    showQuotas = cf.getOpt(OPTION_QUOTA);
    humanReadable = cf.getOpt(OPTION_HUMAN);
    if (cf.getOpt(OPTION_HEADER)) {
      out.println(ContentSummary.getHeader(showQuotas) + "PATHNAME");
    }
  }

  @Override
  protected void processPath(PathData src) throws IOException {
    ContentSummary summary = src.fs.getContentSummary(src.path);
    out.println(summary.toString(showQuotas, isHumanReadable()) + src);
  }
  
  /**
   * Should quotas get shown as part of the report?
   * @return if quotas should be shown then true otherwise false
   */
  @InterfaceAudience.Private
  boolean isShowQuotas() {
    return showQuotas;
  }
  
  /**
   * Should sizes be shown in human readable format rather than bytes?
   * @return true if human readable format
   */
  @InterfaceAudience.Private
  boolean isHumanReadable() {
    return humanReadable;
  }
}
