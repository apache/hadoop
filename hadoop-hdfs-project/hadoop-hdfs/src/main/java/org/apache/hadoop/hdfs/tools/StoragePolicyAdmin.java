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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.BlockStoragePolicy;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockStoragePolicySuite;
import org.apache.hadoop.tools.TableListing;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;

import java.io.IOException;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

/**
 * This class implements block storage policy operations.
 */
public class StoragePolicyAdmin extends Configured implements Tool {
  private static final int MAX_LINE_WIDTH = 80;

  public static void main(String[] argsArray) throws Exception {
    final StoragePolicyAdmin admin = new StoragePolicyAdmin(new
        Configuration());
    System.exit(admin.run(argsArray));
  }

  private static DistributedFileSystem getDFS(Configuration conf)
      throws IOException {
    final FileSystem fs = FileSystem.get(conf);
    if (!(fs instanceof DistributedFileSystem)) {
      throw new IllegalArgumentException("FileSystem " + fs.getUri() +
          " is not an HDFS file system");
    }
    return (DistributedFileSystem) fs;
  }

  /**
   * NN exceptions contain the stack trace as part of the exception message.
   * When it's a known error, pretty-print the error and squish the stack trace.
   */
  private static String prettifyException(Exception e) {
    return e.getClass().getSimpleName() + ": " +
        e.getLocalizedMessage().split("\n")[0];
  }

  private static TableListing getOptionDescriptionListing() {
    return new TableListing.Builder()
        .addField("").addField("", true)
        .wrapWidth(MAX_LINE_WIDTH).hideHeaders().build();
  }

  public StoragePolicyAdmin(Configuration conf) {
    super(conf);
  }

  @Override
  public int run(String[] args) throws Exception {
    if (args.length == 0) {
      printUsage(false);
      return 1;
    }
    final Command command = determineCommand(args[0]);
    if (command == null) {
      System.err.println("Can't understand command '" + args[0] + "'");
      if (!args[0].startsWith("-")) {
        System.err.println("Command names must start with dashes.");
      }
      printUsage(false);
      return 1;
    }
    final List<String> argsList = new LinkedList<String>();
    argsList.addAll(Arrays.asList(args).subList(1, args.length));
    try {
      return command.run(getConf(), argsList);
    } catch (IllegalArgumentException e) {
      System.err.println(prettifyException(e));
      return -1;
    }
  }

  interface Command {
    String getName();
    String getShortUsage();
    String getLongUsage();
    int run(Configuration conf, List<String> args) throws IOException;
  }

  /** Command to list all the existing storage policies */
  private static class ListStoragePoliciesCommand implements Command {
    @Override
    public String getName() {
      return "-listPolicies";
    }

    @Override
    public String getShortUsage() {
      return "[" + getName() + "]\n";
    }

    @Override
    public String getLongUsage() {
      return getShortUsage() + "\n" +
          "List all the existing block storage policies.\n";
    }

    @Override
    public int run(Configuration conf, List<String> args) throws IOException {
      final DistributedFileSystem dfs = getDFS(conf);
      try {
        BlockStoragePolicy[] policies = dfs.getStoragePolicies();
        System.out.println("Block Storage Policies:");
        for (BlockStoragePolicy policy : policies) {
          if (policy != null) {
            System.out.println("\t" + policy);
          }
        }
      } catch (IOException e) {
        System.err.println(prettifyException(e));
        return 2;
      }
      return 0;
    }
  }

  /** Command to get the storage policy of a file/directory */
  private static class GetStoragePolicyCommand implements Command {
    @Override
    public String getName() {
      return "-getStoragePolicy";
    }

    @Override
    public String getShortUsage() {
      return "[" + getName() + " -path <path>]\n";
    }

    @Override
    public String getLongUsage() {
      final TableListing listing = getOptionDescriptionListing();
      listing.addRow("<path>",
          "The path of the file/directory for getting the storage policy");
      return getShortUsage() + "\n" +
          "Get the storage policy of a file/directory.\n\n" +
          listing.toString();
    }

    @Override
    public int run(Configuration conf, List<String> args) throws IOException {
      final String path = StringUtils.popOptionWithArgument("-path", args);
      if (path == null) {
        System.err.println("Please specify the path with -path.\nUsage:" +
            getLongUsage());
        return 1;
      }

      final DistributedFileSystem dfs = getDFS(conf);
      try {
        HdfsFileStatus status = dfs.getClient().getFileInfo(path);
        if (status == null) {
          System.err.println("File/Directory does not exist: " + path);
          return 2;
        }
        byte storagePolicyId = status.getStoragePolicy();
        if (storagePolicyId == BlockStoragePolicySuite.ID_UNSPECIFIED) {
          System.out.println("The storage policy of " + path + " is unspecified");
          return 0;
        }
        BlockStoragePolicy[] policies = dfs.getStoragePolicies();
        for (BlockStoragePolicy p : policies) {
          if (p.getId() == storagePolicyId) {
            System.out.println("The storage policy of " + path + ":\n" + p);
            return 0;
          }
        }
      } catch (Exception e) {
        System.err.println(prettifyException(e));
        return 2;
      }
      System.err.println("Cannot identify the storage policy for " + path);
      return 2;
    }
  }

  /** Command to set the storage policy to a file/directory */
  private static class SetStoragePolicyCommand implements Command {
    @Override
    public String getName() {
      return "-setStoragePolicy";
    }

    @Override
    public String getShortUsage() {
      return "[" + getName() + " -path <path> -policy <policy>]\n";
    }

    @Override
    public String getLongUsage() {
      TableListing listing = getOptionDescriptionListing();
      listing.addRow("<path>", "The path of the file/directory to set storage" +
          " policy");
      listing.addRow("<policy>", "The name of the block storage policy");
      return getShortUsage() + "\n" +
          "Set the storage policy to a file/directory.\n\n" +
          listing.toString();
    }

    @Override
    public int run(Configuration conf, List<String> args) throws IOException {
      final String path = StringUtils.popOptionWithArgument("-path", args);
      if (path == null) {
        System.err.println("Please specify the path for setting the storage " +
            "policy.\nUsage: " + getLongUsage());
        return 1;
      }

      final String policyName = StringUtils.popOptionWithArgument("-policy",
          args);
      if (policyName == null) {
        System.err.println("Please specify the policy name.\nUsage: " +
            getLongUsage());
        return 1;
      }

      final DistributedFileSystem dfs = getDFS(conf);
      try {
        dfs.setStoragePolicy(new Path(path), policyName);
        System.out.println("Set storage policy " + policyName + " on " + path);
      } catch (Exception e) {
        System.err.println(prettifyException(e));
        return 2;
      }
      return 0;
    }
  }

  private static class HelpCommand implements Command {
    @Override
    public String getName() {
      return "-help";
    }

    @Override
    public String getShortUsage() {
      return "[-help <command-name>]\n";
    }

    @Override
    public String getLongUsage() {
      final TableListing listing = getOptionDescriptionListing();
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
        for (Command command : COMMANDS) {
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
      final Command command = determineCommand("-" + commandName);
      if (command == null) {
        System.err.print("Unknown command '" + commandName + "'.\n");
        System.err.print("Valid help command names are:\n");
        String separator = "";
        for (Command c : COMMANDS) {
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

  private static final Command[] COMMANDS = {
      new ListStoragePoliciesCommand(),
      new SetStoragePolicyCommand(),
      new GetStoragePolicyCommand(),
      new HelpCommand()
  };

  private static void printUsage(boolean longUsage) {
    System.err.println(
        "Usage: bin/hdfs storagepolicies [COMMAND]");
    for (Command command : COMMANDS) {
      if (longUsage) {
        System.err.print(command.getLongUsage());
      } else {
        System.err.print("          " + command.getShortUsage());
      }
    }
    System.err.println();
  }

  private static Command determineCommand(String commandName) {
    for (Command COMMAND : COMMANDS) {
      if (COMMAND.getName().equals(commandName)) {
        return COMMAND;
      }
    }
    return null;
  }
}