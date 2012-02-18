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

package org.apache.hadoop.fs.slive;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;
import org.apache.hadoop.fs.slive.Constants.Distribution;
import org.apache.hadoop.fs.slive.Constants.OperationType;
import org.apache.hadoop.util.StringUtils;

/**
 * Class which abstracts the parsing of command line arguments for slive test
 */
class ArgumentParser {

  private Options optList;
  private String[] argumentList;
  private ParsedOutput parsed;

  /**
   * Result of a parse is the following object
   */
  static class ParsedOutput {
    private CommandLine parsedData;
    private ArgumentParser source;
    private boolean needHelp;

    ParsedOutput(CommandLine parsedData, ArgumentParser source,
        boolean needHelp) {
      this.parsedData = parsedData;
      this.source = source;
      this.needHelp = needHelp;
    }

    /**
     * @return whether the calling object should call output help and exit
     */
    boolean shouldOutputHelp() {
      return needHelp;
    }

    /**
     * Outputs the formatted help to standard out
     */
    void outputHelp() {
      if (!shouldOutputHelp()) {
        return;
      }
      if (source != null) {
        HelpFormatter hlp = new HelpFormatter();
        hlp.printHelp(Constants.PROG_NAME + " " + Constants.PROG_VERSION,
            source.getOptionList());
      }
    }

    /**
     * @param optName
     *          the option name to get the value for
     * 
     * @return the option value or null if it does not exist
     */
    String getValue(String optName) {
      if (parsedData == null) {
        return null;
      }
      return parsedData.getOptionValue(optName);
    }

    public String toString() {
      StringBuilder s = new StringBuilder();
      if (parsedData != null) {
        Option[] ops = parsedData.getOptions();
        for (int i = 0; i < ops.length; ++i) {
          s.append(ops[i].getOpt() + " = " + s.append(ops[i].getValue()) + ",");
        }
      }
      return s.toString();
    }

  }

  ArgumentParser(String[] args) {
    optList = getOptions();
    if (args == null) {
      args = new String[] {};
    }
    argumentList = args;
    parsed = null;
  }

  private Options getOptionList() {
    return optList;
  }

  /**
   * Parses the command line options
   * 
   * @return false if need to print help output
   * 
   * @throws Exception
   *           when parsing fails
   */
  ParsedOutput parse() throws Exception {
    if (parsed == null) {
      PosixParser parser = new PosixParser();
      CommandLine popts = parser.parse(getOptionList(), argumentList, true);
      if (popts.hasOption(ConfigOption.HELP.getOpt())) {
        parsed = new ParsedOutput(null, this, true);
      } else {
        parsed = new ParsedOutput(popts, this, false);
      }
    }
    return parsed;
  }

  /**
   * @return the option set to be used in command line parsing
   */
  private Options getOptions() {
    Options cliopt = new Options();
    cliopt.addOption(ConfigOption.MAPS);
    cliopt.addOption(ConfigOption.REDUCES);
    cliopt.addOption(ConfigOption.PACKET_SIZE);
    cliopt.addOption(ConfigOption.OPS);
    cliopt.addOption(ConfigOption.DURATION);
    cliopt.addOption(ConfigOption.EXIT_ON_ERROR);
    cliopt.addOption(ConfigOption.SLEEP_TIME);
    cliopt.addOption(ConfigOption.FILES);
    cliopt.addOption(ConfigOption.DIR_SIZE);
    cliopt.addOption(ConfigOption.BASE_DIR);
    cliopt.addOption(ConfigOption.RESULT_FILE);
    cliopt.addOption(ConfigOption.CLEANUP);
    {
      String distStrs[] = new String[Distribution.values().length];
      Distribution distValues[] = Distribution.values();
      for (int i = 0; i < distValues.length; ++i) {
        distStrs[i] = distValues[i].lowerName();
      }
      String opdesc = String.format(Constants.OP_DESCR, StringUtils
          .arrayToString(distStrs));
      for (OperationType type : OperationType.values()) {
        String opname = type.lowerName();
        cliopt.addOption(new Option(opname, true, opdesc));
      }
    }
    cliopt.addOption(ConfigOption.REPLICATION_AM);
    cliopt.addOption(ConfigOption.BLOCK_SIZE);
    cliopt.addOption(ConfigOption.READ_SIZE);
    cliopt.addOption(ConfigOption.WRITE_SIZE);
    cliopt.addOption(ConfigOption.APPEND_SIZE);
    cliopt.addOption(ConfigOption.RANDOM_SEED);
    cliopt.addOption(ConfigOption.QUEUE_NAME);
    cliopt.addOption(ConfigOption.HELP);
    return cliopt;
  }

}
