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

import org.apache.commons.cli.*;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.tools.DistCpOptions.FileAttribute;

import java.util.*;

/**
 * The OptionsParser parses out the command-line options passed to DistCp,
 * and interprets those specific to DistCp, to create an Options object.
 */
public class OptionsParser {

  private static final Log LOG = LogFactory.getLog(OptionsParser.class);

  private static final Options cliOptions = new Options();

  static {
    for (DistCpOptionSwitch option : DistCpOptionSwitch.values()) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Adding option " + option.getOption());
      }
      cliOptions.addOption(option.getOption());
    }
  }

  private static class CustomParser extends GnuParser {
    @Override
    protected String[] flatten(Options options, String[] arguments, boolean stopAtNonOption) {
      for (int index = 0; index < arguments.length; index++) {
        if (arguments[index].equals("-" + DistCpOptionSwitch.PRESERVE_STATUS.getSwitch())) {
          arguments[index] = "-prbugpc";
        }
      }
      return super.flatten(options, arguments, stopAtNonOption);
    }
  }

  /**
   * The parse method parses the command-line options, and creates
   * a corresponding Options object.
   * @param args Command-line arguments (excluding the options consumed
   *              by the GenericOptionsParser).
   * @return The Options object, corresponding to the specified command-line.
   * @throws IllegalArgumentException: Thrown if the parse fails.
   */
  public static DistCpOptions parse(String args[]) throws IllegalArgumentException {

    CommandLineParser parser = new CustomParser();

    CommandLine command;
    try {
      command = parser.parse(cliOptions, args, true);
    } catch (ParseException e) {
      throw new IllegalArgumentException("Unable to parse arguments. " +
        Arrays.toString(args), e);
    }

    DistCpOptions option;
    Path targetPath;
    List<Path> sourcePaths = new ArrayList<Path>();

    String leftOverArgs[] = command.getArgs();
    if (leftOverArgs == null || leftOverArgs.length < 1) {
      throw new IllegalArgumentException("Target path not specified");
    }

    //Last Argument is the target path
    targetPath = new Path(leftOverArgs[leftOverArgs.length -1].trim());

    //Copy any source paths in the arguments to the list
    for (int index = 0; index < leftOverArgs.length - 1; index++) {
      sourcePaths.add(new Path(leftOverArgs[index].trim()));
    }

    /* If command has source file listing, use it else, fall back on source paths in args
       If both are present, throw exception and bail */
    if (command.hasOption(DistCpOptionSwitch.SOURCE_FILE_LISTING.getSwitch())) {
      if (!sourcePaths.isEmpty()) {
        throw new IllegalArgumentException("Both source file listing and source paths present");
      }
      option = new DistCpOptions(new Path(getVal(command, DistCpOptionSwitch.
              SOURCE_FILE_LISTING.getSwitch())), targetPath);
    } else {
      if (sourcePaths.isEmpty()) {
        throw new IllegalArgumentException("Neither source file listing nor source paths present");
      }
      option = new DistCpOptions(sourcePaths, targetPath);
    }

    //Process all the other option switches and set options appropriately
    if (command.hasOption(DistCpOptionSwitch.IGNORE_FAILURES.getSwitch())) {
      option.setIgnoreFailures(true);
    }

    if (command.hasOption(DistCpOptionSwitch.ATOMIC_COMMIT.getSwitch())) {
      option.setAtomicCommit(true);
    }

    if (command.hasOption(DistCpOptionSwitch.WORK_PATH.getSwitch()) &&
        option.shouldAtomicCommit()) {
      String workPath = getVal(command, DistCpOptionSwitch.WORK_PATH.getSwitch());
      if (workPath != null && !workPath.isEmpty()) {
        option.setAtomicWorkPath(new Path(workPath));
      }
    } else if (command.hasOption(DistCpOptionSwitch.WORK_PATH.getSwitch())) {
      throw new IllegalArgumentException("-tmp work-path can only be specified along with -atomic");
    }

    if (command.hasOption(DistCpOptionSwitch.LOG_PATH.getSwitch())) {
      option.setLogPath(new Path(getVal(command, DistCpOptionSwitch.LOG_PATH.getSwitch())));
    }

    if (command.hasOption(DistCpOptionSwitch.SYNC_FOLDERS.getSwitch())) {
      option.setSyncFolder(true);
    }

    if (command.hasOption(DistCpOptionSwitch.OVERWRITE.getSwitch())) {
      option.setOverwrite(true);
    }

    if (command.hasOption(DistCpOptionSwitch.APPEND.getSwitch())) {
      option.setAppend(true);
    }

    if (command.hasOption(DistCpOptionSwitch.DELETE_MISSING.getSwitch())) {
      option.setDeleteMissing(true);
    }

    if (command.hasOption(DistCpOptionSwitch.SKIP_CRC.getSwitch())) {
      option.setSkipCRC(true);
    }

    if (command.hasOption(DistCpOptionSwitch.BLOCKING.getSwitch())) {
      option.setBlocking(false);
    }

    if (command.hasOption(DistCpOptionSwitch.BANDWIDTH.getSwitch())) {
      try {
        Integer mapBandwidth = Integer.parseInt(
            getVal(command, DistCpOptionSwitch.BANDWIDTH.getSwitch()).trim());
        if (mapBandwidth.intValue() <= 0) {
          throw new IllegalArgumentException("Bandwidth specified is not positive: " +
              mapBandwidth);
        }
        option.setMapBandwidth(mapBandwidth);
      } catch (NumberFormatException e) {
        throw new IllegalArgumentException("Bandwidth specified is invalid: " +
            getVal(command, DistCpOptionSwitch.BANDWIDTH.getSwitch()), e);
      }
    }

    if (command.hasOption(DistCpOptionSwitch.SSL_CONF.getSwitch())) {
      option.setSslConfigurationFile(command.
          getOptionValue(DistCpOptionSwitch.SSL_CONF.getSwitch()));
    }

    if (command.hasOption(DistCpOptionSwitch.MAX_MAPS.getSwitch())) {
      try {
        Integer maps = Integer.parseInt(
            getVal(command, DistCpOptionSwitch.MAX_MAPS.getSwitch()).trim());
        option.setMaxMaps(maps);
      } catch (NumberFormatException e) {
        throw new IllegalArgumentException("Number of maps is invalid: " +
            getVal(command, DistCpOptionSwitch.MAX_MAPS.getSwitch()), e);
      }
    }

    if (command.hasOption(DistCpOptionSwitch.COPY_STRATEGY.getSwitch())) {
      option.setCopyStrategy(
            getVal(command, DistCpOptionSwitch.COPY_STRATEGY.getSwitch()));
    }

    if (command.hasOption(DistCpOptionSwitch.PRESERVE_STATUS.getSwitch())) {
      String attributes =
          getVal(command, DistCpOptionSwitch.PRESERVE_STATUS.getSwitch());
      if (attributes == null || attributes.isEmpty()) {
        for (FileAttribute attribute : FileAttribute.values()) {
          option.preserve(attribute);
        }
      } else {
        for (int index = 0; index < attributes.length(); index++) {
          option.preserve(FileAttribute.
              getAttribute(attributes.charAt(index)));
        }
      }
    }

    if (command.hasOption(DistCpOptionSwitch.FILE_LIMIT.getSwitch())) {
      String fileLimitString = getVal(command,
                              DistCpOptionSwitch.FILE_LIMIT.getSwitch().trim());
      try {
        Integer.parseInt(fileLimitString);
      }
      catch (NumberFormatException e) {
        throw new IllegalArgumentException("File-limit is invalid: "
                                            + fileLimitString, e);
      }
      LOG.warn(DistCpOptionSwitch.FILE_LIMIT.getSwitch() + " is a deprecated" +
              " option. Ignoring.");
    }

    if (command.hasOption(DistCpOptionSwitch.SIZE_LIMIT.getSwitch())) {
      String sizeLimitString = getVal(command,
                              DistCpOptionSwitch.SIZE_LIMIT.getSwitch().trim());
      try {
        Long.parseLong(sizeLimitString);
      }
      catch (NumberFormatException e) {
        throw new IllegalArgumentException("Size-limit is invalid: "
                                            + sizeLimitString, e);
      }
      LOG.warn(DistCpOptionSwitch.SIZE_LIMIT.getSwitch() + " is a deprecated" +
              " option. Ignoring.");
    }

    return option;
  }

  private static String getVal(CommandLine command, String swtch) {
    String optionValue = command.getOptionValue(swtch);
    if (optionValue == null) {
      return null;
    } else {
      return optionValue.trim();
    }
  }

  public static void usage() {
    HelpFormatter formatter = new HelpFormatter();
    formatter.printHelp("distcp OPTIONS [source_path...] <target_path>\n\nOPTIONS", cliOptions);
  }
}
