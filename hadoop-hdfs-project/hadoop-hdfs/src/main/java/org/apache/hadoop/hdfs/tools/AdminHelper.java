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
package org.apache.hadoop.hdfs.tools;

import com.google.common.base.Preconditions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.CachePoolInfo;
import org.apache.hadoop.tools.TableListing;

import java.io.IOException;
import java.util.List;

/**
 * Helper methods for CacheAdmin/CryptoAdmin/StoragePolicyAdmin
 */
public class AdminHelper {
  /**
   * Maximum length for printed lines
   */
  static final int MAX_LINE_WIDTH = 80;
  static final String HELP_COMMAND_NAME = "-help";

  static DistributedFileSystem getDFS(Configuration conf)
      throws IOException {
    FileSystem fs = FileSystem.get(conf);
    if (!(fs instanceof DistributedFileSystem)) {
      throw new IllegalArgumentException("FileSystem " + fs.getUri() +
          " is not an HDFS file system");
    }
    return (DistributedFileSystem)fs;
  }

  /**
   * NN exceptions contain the stack trace as part of the exception message.
   * When it's a known error, pretty-print the error and squish the stack trace.
   */
  static String prettifyException(Exception e) {
    return e.getClass().getSimpleName() + ": "
        + e.getLocalizedMessage().split("\n")[0];
  }

  static TableListing getOptionDescriptionListing() {
    return new TableListing.Builder()
        .addField("").addField("", true)
        .wrapWidth(MAX_LINE_WIDTH).hideHeaders().build();
  }

  /**
   * Parses a time-to-live value from a string
   * @return The ttl in milliseconds
   * @throws IOException if it could not be parsed
   */
  static Long parseTtlString(String maxTtlString) throws IOException {
    Long maxTtl = null;
    if (maxTtlString != null) {
      if (maxTtlString.equalsIgnoreCase("never")) {
        maxTtl = CachePoolInfo.RELATIVE_EXPIRY_NEVER;
      } else {
        maxTtl = DFSUtil.parseRelativeTime(maxTtlString);
      }
    }
    return maxTtl;
  }

  static Long parseLimitString(String limitString) {
    Long limit = null;
    if (limitString != null) {
      if (limitString.equalsIgnoreCase("unlimited")) {
        limit = CachePoolInfo.LIMIT_UNLIMITED;
      } else {
        limit = Long.parseLong(limitString);
      }
    }
    return limit;
  }

  static Command determineCommand(String commandName, Command[] commands) {
    Preconditions.checkNotNull(commands);
    if (HELP_COMMAND_NAME.equals(commandName)) {
      return new HelpCommand(commands);
    }
    for (Command command : commands) {
      if (command.getName().equals(commandName)) {
        return command;
      }
    }
    return null;
  }

  static void printUsage(boolean longUsage, String toolName,
      Command[] commands) {
    Preconditions.checkNotNull(commands);
    System.err.println("Usage: bin/hdfs " + toolName + " [COMMAND]");
    final HelpCommand helpCommand = new HelpCommand(commands);
    for (AdminHelper.Command command : commands) {
      if (longUsage) {
        System.err.print(command.getLongUsage());
      } else {
        System.err.print("          " + command.getShortUsage());
      }
    }
    System.err.print(longUsage ? helpCommand.getLongUsage() :
        ("          " + helpCommand.getShortUsage()));
    System.err.println();
  }

  interface Command {
    String getName();
    String getShortUsage();
    String getLongUsage();
    int run(Configuration conf, List<String> args) throws IOException;
  }

  static class HelpCommand implements Command {
    private final Command[] commands;

    public HelpCommand(Command[] commands) {
      Preconditions.checkNotNull(commands != null);
      this.commands = commands;
    }

    @Override
    public String getName() {
      return HELP_COMMAND_NAME;
    }

    @Override
    public String getShortUsage() {
      return "[-help <command-name>]\n";
    }

    @Override
    public String getLongUsage() {
      final TableListing listing = AdminHelper.getOptionDescriptionListing();
      listing.addRow("<command-name>", "The command for which to get " +
          "detailed help. If no command is specified, print detailed help for " +
          "all commands");
      return getShortUsage() + "\n" +
          "Get detailed help about a command.\n\n" +
          listing.toString();
    }

    @Override
    public int run(Configuration conf, List<String> args) throws IOException {
      if (args.size() == 0) {
        for (AdminHelper.Command command : commands) {
          System.err.println(command.getLongUsage());
        }
        return 0;
      }
      if (args.size() != 1) {
        System.out.println("You must give exactly one argument to -help.");
        return 0;
      }
      final String commandName = args.get(0);
      // prepend a dash to match against the command names
      final AdminHelper.Command command = AdminHelper
          .determineCommand("-" + commandName, commands);
      if (command == null) {
        System.err.print("Unknown command '" + commandName + "'.\n");
        System.err.print("Valid help command names are:\n");
        String separator = "";
        for (AdminHelper.Command c : commands) {
          System.err.print(separator + c.getName().substring(1));
          separator = ", ";
        }
        System.err.print("\n");
        return 1;
      }
      System.err.print(command.getLongUsage());
      return 0;
    }
  }
}
