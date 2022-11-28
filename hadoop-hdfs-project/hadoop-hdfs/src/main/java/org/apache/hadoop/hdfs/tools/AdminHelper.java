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
package org.apache.hadoop.hdfs.tools;

import org.apache.hadoop.util.Preconditions;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.viewfs.ViewFileSystemOverloadScheme;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.CachePoolInfo;
import org.apache.hadoop.tools.TableListing;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.net.URI;
import java.util.Arrays;
import java.util.LinkedList;
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

  public static DistributedFileSystem getDFS(Configuration conf)
      throws IOException {
    FileSystem fs = FileSystem.get(conf);
    return checkAndGetDFS(fs, conf);
  }

  static DistributedFileSystem getDFS(URI uri, Configuration conf)
      throws IOException {
    FileSystem fs = FileSystem.get(uri, conf);
    return checkAndGetDFS(fs, conf);
  }

  static DistributedFileSystem checkAndGetDFS(FileSystem fs, Configuration conf)
      throws IOException {
    if ((fs instanceof ViewFileSystemOverloadScheme)) {
      // With ViewFSOverloadScheme, the admin will pass -fs option with intended
      // child fs mount path. GenericOptionsParser would have set the given -fs
      // as FileSystem's defaultURI. So, we are using FileSystem.getDefaultUri
      // to use the given -fs path.
      fs = ((ViewFileSystemOverloadScheme) fs)
          .getRawFileSystem(new Path(FileSystem.getDefaultUri(conf)), conf);
    }
    if (!(fs instanceof DistributedFileSystem)) {
      throw new IllegalArgumentException("FileSystem " + fs.getUri()
          + " is not an HDFS file system. The fs class is: "
          + fs.getClass().getName());
    }
    return (DistributedFileSystem) fs;
  }

  /**
   * NN exceptions contain the stack trace as part of the exception message.
   * When it's a known error, pretty-print the error and squish the stack trace.
   */
  static String prettifyException(Exception e) {
    if (e.getLocalizedMessage() != null) {
      return e.getClass().getSimpleName() + ": "
          + e.getLocalizedMessage().split("\n")[0];
    } else if (e.getStackTrace() != null && e.getStackTrace().length > 0) {
      return e.getClass().getSimpleName() + " at " + e.getStackTrace()[0];
    } else {
      return e.getClass().getSimpleName();
    }
  }

  public static TableListing getOptionDescriptionListing() {
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

  /**
   * Find the subcommand from the given list
   * @param commandName the sub command name
   * @param commands the list of commands
   * @return the selected command or null if it isn't found
   */
  static Command determineCommand(String commandName,
                                  Command... commands) {
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

  /**
   * Run a command from a list of potential commands and the cli arguments.
   * @param toolName the name of the tool
   * @param conf the configuration
   * @param args the command line arguments
   * @param commands the list of potential commands
   * @return the return code (0 == success)
   */
  public static int runCommand(String toolName,
                               Configuration conf,
                               String[] args,
                               Command... commands) throws IOException {
    Command cmd = null;
    try {
      if (args.length == 0) {
        throw new IllegalArgumentException("Must supply a command argument");
      }
      String commandName = args[0];
      List<String> rest = new LinkedList<>(Arrays.asList(args))
          .subList(1, args.length);
      cmd = determineCommand(commandName, commands);
      if (cmd == null) {
        if (!commandName.startsWith("-")) {
          throw new IllegalArgumentException(commandName +
              ": Command names must start with dashes.");
        } else {
          throw new IllegalArgumentException("Can't understand command '" +
              commandName + "'");
        }
      }
      return cmd.run(conf, rest);
    } catch (IllegalArgumentException iae) {
      if (cmd == null) {
        System.err.printf("ERROR %s: %s%n%n", toolName, iae.getLocalizedMessage());
        printUsage(false, toolName, commands);
      } else {
        System.err.printf("ERROR %s %s: %s%n%n", toolName, cmd.getName(),
            iae.getLocalizedMessage());
        printUsage(cmd);
      }
      ToolRunner.printGenericCommandUsage(System.err);
      return -1;
    }
  }

  static void printUsage(Command command) {
    Preconditions.checkNotNull(command);
    System.err.printf(command.getLongUsage());
    System.err.println();
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

  public interface Command {
    String getName();
    String getShortUsage();
    String getLongUsage();
    int run(Configuration conf, List<String> args) throws IOException;
  }

  /**
   * The type for the lambda of the subcommands.
   */
  public interface CommandFunction {
    int apply(Configuration conf, List<String> args) throws IOException;
  }

  /**
   * Create a Command object without defining a new class.
   * @param name the name of the subcommand with the dash
   * @param description the brief description of what the subcommand does
   * @param shortUsage the short usage string (don't include the name)
   * @param runner the lambda to execute the subcommand
   * @param subopts a list of detailHelp for the long usage
   * @return a new command object
   */
  public static Command makeCommand(String name,
                                    String description,
                                    String shortUsage,
                                    CommandFunction runner,
                                    String[]... subopts) {
    return new Command() {
      @Override
      public String getName() {
        return name;
      }

      @Override
      public String getShortUsage() {
        return "[" + name + " " + shortUsage + "]\n";
      }

      @Override
      public String getLongUsage() {
        TableListing listing = AdminHelper.getOptionDescriptionListing();
        for(String[] opt: subopts){
          listing.addRow(new String[]{"  " + opt[0], opt[1]});
        }
        return String.format("%s: %s\n  %s\n%s", name, description, shortUsage,
            listing);
      }

      @Override
      public int run(Configuration conf, List<String> args) throws IOException {
        return runner.apply(conf, args);
      }
    };
  }

  /**
   * A helper function to create the detailed help for makeCommand.
   * @param values the pair of strings for the long help
   * @return an array of the strings
   */
  public static String[] detailHelp(String... values) {
    return values;
  }

  static class HelpCommand implements Command {
    private final Command[] commands;

    public HelpCommand(Command[] commands) {
      Preconditions.checkNotNull(commands, "commands cannot be null.");
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
      return getShortUsage() + "\n" + "Get detailed help about a command.\n\n" +
          listing;
    }

    @Override
    public int run(Configuration conf, List<String> args) throws IOException {
      if (args.size() == 0) {
        for (AdminHelper.Command command : commands) {
          System.err.println(command.getLongUsage());
        }
        return 1;
      }
      if (args.size() != 1) {
        System.err.println("You must give exactly one argument to -help.");
        return 1;
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
