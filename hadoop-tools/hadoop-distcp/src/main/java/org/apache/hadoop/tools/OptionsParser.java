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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;

import com.google.common.base.Preconditions;

/**
 * The OptionsParser parses out the command-line options passed to DistCp,
 * and interprets those specific to DistCp, to create an Options object.
 */
public class OptionsParser {

  static final Log LOG = LogFactory.getLog(OptionsParser.class);

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
          arguments[index] = DistCpOptionSwitch.PRESERVE_STATUS_DEFAULT;
        }
      }
      return super.flatten(options, arguments, stopAtNonOption);
    }
  }

  private static void checkSnapshotsArgs(final String[] snapshots) {
    Preconditions.checkArgument(snapshots != null && snapshots.length == 2
        && !StringUtils.isBlank(snapshots[0])
        && !StringUtils.isBlank(snapshots[1]),
        "Must provide both the starting and ending snapshot names");
  }

  /**
   * The parse method parses the command-line options, and creates
   * a corresponding Options object.
   * @param args Command-line arguments (excluding the options consumed
   *              by the GenericOptionsParser).
   * @return The Options object, corresponding to the specified command-line.
   * @throws IllegalArgumentException Thrown if the parse fails.
   */
  public static DistCpOptions parse(String[] args)
      throws IllegalArgumentException {

    CommandLineParser parser = new CustomParser();

    CommandLine command;
    try {
      command = parser.parse(cliOptions, args, true);
    } catch (ParseException e) {
      throw new IllegalArgumentException("Unable to parse arguments. " +
        Arrays.toString(args), e);
    }

    DistCpOptions.Builder builder = parseSourceAndTargetPaths(command);
    builder
        .withAtomicCommit(
            command.hasOption(DistCpOptionSwitch.ATOMIC_COMMIT.getSwitch()))
        .withSyncFolder(
            command.hasOption(DistCpOptionSwitch.SYNC_FOLDERS.getSwitch()))
        .withDeleteMissing(
            command.hasOption(DistCpOptionSwitch.DELETE_MISSING.getSwitch()))
        .withIgnoreFailures(
            command.hasOption(DistCpOptionSwitch.IGNORE_FAILURES.getSwitch()))
        .withOverwrite(
            command.hasOption(DistCpOptionSwitch.OVERWRITE.getSwitch()))
        .withAppend(
            command.hasOption(DistCpOptionSwitch.APPEND.getSwitch()))
        .withCRC(
            command.hasOption(DistCpOptionSwitch.SKIP_CRC.getSwitch()))
        .withBlocking(
            !command.hasOption(DistCpOptionSwitch.BLOCKING.getSwitch()))
        .withVerboseLog(
            command.hasOption(DistCpOptionSwitch.VERBOSE_LOG.getSwitch()));

    if (command.hasOption(DistCpOptionSwitch.DIFF.getSwitch())) {
      String[] snapshots = getVals(command,
          DistCpOptionSwitch.DIFF.getSwitch());
      checkSnapshotsArgs(snapshots);
      builder.withUseDiff(snapshots[0], snapshots[1]);
    }
    if (command.hasOption(DistCpOptionSwitch.RDIFF.getSwitch())) {
      String[] snapshots = getVals(command,
          DistCpOptionSwitch.RDIFF.getSwitch());
      checkSnapshotsArgs(snapshots);
      builder.withUseRdiff(snapshots[0], snapshots[1]);
    }

    if (command.hasOption(DistCpOptionSwitch.FILTERS.getSwitch())) {
      builder.withFiltersFile(
          getVal(command, DistCpOptionSwitch.FILTERS.getSwitch()));
    }

    if (command.hasOption(DistCpOptionSwitch.LOG_PATH.getSwitch())) {
      builder.withLogPath(
          new Path(getVal(command, DistCpOptionSwitch.LOG_PATH.getSwitch())));
    }

    if (command.hasOption(DistCpOptionSwitch.WORK_PATH.getSwitch())) {
      final String workPath = getVal(command,
          DistCpOptionSwitch.WORK_PATH.getSwitch());
      if (workPath != null && !workPath.isEmpty()) {
        builder.withAtomicWorkPath(new Path(workPath));
      }
    }
    if (command.hasOption(DistCpOptionSwitch.TRACK_MISSING.getSwitch())) {
      builder.withTrackMissing(
          new Path(getVal(
              command,
              DistCpOptionSwitch.TRACK_MISSING.getSwitch())));
    }

    if (command.hasOption(DistCpOptionSwitch.BANDWIDTH.getSwitch())) {
      try {
        final Float mapBandwidth = Float.parseFloat(
            getVal(command, DistCpOptionSwitch.BANDWIDTH.getSwitch()));
        builder.withMapBandwidth(mapBandwidth);
      } catch (NumberFormatException e) {
        throw new IllegalArgumentException("Bandwidth specified is invalid: " +
            getVal(command, DistCpOptionSwitch.BANDWIDTH.getSwitch()), e);
      }
    }

    if (command.hasOption(
        DistCpOptionSwitch.NUM_LISTSTATUS_THREADS.getSwitch())) {
      try {
        final Integer numThreads = Integer.parseInt(getVal(command,
            DistCpOptionSwitch.NUM_LISTSTATUS_THREADS.getSwitch()));
        builder.withNumListstatusThreads(numThreads);
      } catch (NumberFormatException e) {
        throw new IllegalArgumentException(
            "Number of liststatus threads is invalid: " + getVal(command,
                DistCpOptionSwitch.NUM_LISTSTATUS_THREADS.getSwitch()), e);
      }
    }

    if (command.hasOption(DistCpOptionSwitch.MAX_MAPS.getSwitch())) {
      try {
        final Integer maps = Integer.parseInt(
            getVal(command, DistCpOptionSwitch.MAX_MAPS.getSwitch()));
        builder.maxMaps(maps);
      } catch (NumberFormatException e) {
        throw new IllegalArgumentException("Number of maps is invalid: " +
            getVal(command, DistCpOptionSwitch.MAX_MAPS.getSwitch()), e);
      }
    }

    if (command.hasOption(DistCpOptionSwitch.COPY_STRATEGY.getSwitch())) {
      builder.withCopyStrategy(
            getVal(command, DistCpOptionSwitch.COPY_STRATEGY.getSwitch()));
    }

    if (command.hasOption(DistCpOptionSwitch.PRESERVE_STATUS.getSwitch())) {
      builder.preserve(
          getVal(command, DistCpOptionSwitch.PRESERVE_STATUS.getSwitch()));
    } else {
      // No "preserve" settings specified. Preserve block-size.
      builder.preserve(DistCpOptions.FileAttribute.BLOCKSIZE);
    }

    if (command.hasOption(DistCpOptionSwitch.FILE_LIMIT.getSwitch())) {
      LOG.warn(DistCpOptionSwitch.FILE_LIMIT.getSwitch() + " is a deprecated" +
          " option. Ignoring.");
    }

    if (command.hasOption(DistCpOptionSwitch.SIZE_LIMIT.getSwitch())) {
      LOG.warn(DistCpOptionSwitch.SIZE_LIMIT.getSwitch() + " is a deprecated" +
          " option. Ignoring.");
    }

    if (command.hasOption(DistCpOptionSwitch.BLOCKS_PER_CHUNK.getSwitch())) {
      final String chunkSizeStr = getVal(command,
          DistCpOptionSwitch.BLOCKS_PER_CHUNK.getSwitch().trim());
      try {
        int csize = Integer.parseInt(chunkSizeStr);
        csize = csize > 0 ? csize : 0;
        LOG.info("Set distcp blocksPerChunk to " + csize);
        builder.withBlocksPerChunk(csize);
      } catch (NumberFormatException e) {
        throw new IllegalArgumentException("blocksPerChunk is invalid: "
            + chunkSizeStr, e);
      }
    }

    if (command.hasOption(DistCpOptionSwitch.COPY_BUFFER_SIZE.getSwitch())) {
      final String copyBufferSizeStr = getVal(command,
          DistCpOptionSwitch.COPY_BUFFER_SIZE.getSwitch().trim());
      try {
        int copyBufferSize = Integer.parseInt(copyBufferSizeStr);
        builder.withCopyBufferSize(copyBufferSize);
      } catch (NumberFormatException e) {
        throw new IllegalArgumentException("copyBufferSize is invalid: "
            + copyBufferSizeStr, e);
      }
    }

    return builder.build();
  }

  /**
   * parseSourceAndTargetPaths is a helper method for parsing the source
   * and target paths.
   *
   * @param command command line arguments
   * @return        DistCpOptions
   */
  private static DistCpOptions.Builder parseSourceAndTargetPaths(
      CommandLine command) {
    Path targetPath;
    List<Path> sourcePaths = new ArrayList<Path>();

    String[] leftOverArgs = command.getArgs();
    if (leftOverArgs == null || leftOverArgs.length < 1) {
      throw new IllegalArgumentException("Target path not specified");
    }

    //Last Argument is the target path
    targetPath = new Path(leftOverArgs[leftOverArgs.length - 1].trim());

    //Copy any source paths in the arguments to the list
    for (int index = 0; index < leftOverArgs.length - 1; index++) {
      sourcePaths.add(new Path(leftOverArgs[index].trim()));
    }

    /* If command has source file listing, use it else, fall back on source
       paths in args.  If both are present, throw exception and bail */
    if (command.hasOption(
        DistCpOptionSwitch.SOURCE_FILE_LISTING.getSwitch())) {
      if (!sourcePaths.isEmpty()) {
        throw new IllegalArgumentException("Both source file listing and " +
            "source paths present");
      }
      return new DistCpOptions.Builder(new Path(getVal(command,
          DistCpOptionSwitch.SOURCE_FILE_LISTING.getSwitch())), targetPath);
    } else {
      if (sourcePaths.isEmpty()) {
        throw new IllegalArgumentException("Neither source file listing nor " +
            "source paths present");
      }
      return new DistCpOptions.Builder(sourcePaths, targetPath);
    }
  }

  private static String getVal(CommandLine command, String swtch) {
    if (swtch == null) {
      return null;
    }
    String optionValue = command.getOptionValue(swtch.trim());
    if (optionValue == null) {
      return null;
    } else {
      return optionValue.trim();
    }
  }

  private static String[] getVals(CommandLine command, String option) {
    return command.getOptionValues(option);
  }

  public static void usage() {
    HelpFormatter formatter = new HelpFormatter();
    formatter.printHelp("distcp OPTIONS [source_path...] <target_path>\n\nOPTIONS", cliOptions);
  }
}
