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

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.PathBasedCacheDirective;
import org.apache.hadoop.hdfs.protocol.PathBasedCacheDescriptor;
import org.apache.hadoop.hdfs.tools.TableListing.Justification;
import org.apache.hadoop.util.Fallible;
import org.apache.hadoop.util.StringUtils;

/**
 * This class implements command-line operations on the HDFS Cache.
 */
@InterfaceAudience.Private
public class CacheAdmin {
  private static Configuration conf = new Configuration();

  private static DistributedFileSystem getDFS() throws IOException {
    FileSystem fs = FileSystem.get(conf);
    if (!(fs instanceof DistributedFileSystem)) {
      throw new IllegalArgumentException("FileSystem " + fs.getUri() + 
      " is not an HDFS file system");
    }
    return (DistributedFileSystem)fs;
  }

  interface Command {
    String getName();
    String getShortUsage();
    String getLongUsage();
    int run(List<String> args) throws IOException;
  }

  private static class AddPathBasedCacheDirectiveCommand implements Command {
    @Override
    public String getName() {
      return "-addPath";
    }

    @Override
    public String getShortUsage() {
      return "[-addPath -path <path> -pool <pool-name>]\n";
    }

    @Override
    public String getLongUsage() {
      return getShortUsage() +
        "Adds a new PathBasedCache directive.\n" +
        "<path>  The new path to cache.\n" + 
        "        Paths may be either directories or files.\n" +
        "<pool-name> The pool which this directive will reside in.\n" + 
        "        You must have write permission on the cache pool in order\n" +
        "        to add new entries to it.\n";
    }

    @Override
    public int run(List<String> args) throws IOException {
      String path = StringUtils.popOptionWithArgument("-path", args);
      if (path == null) {
        System.err.println("You must specify a path with -path.");
        return 1;
      }
      String poolName = StringUtils.popOptionWithArgument("-pool", args);
      if (poolName == null) {
        System.err.println("You must specify a pool name with -pool.");
        return 1;
      }
      if (!args.isEmpty()) {
        System.err.println("Can't understand argument: " + args.get(0));
        return 1;
      }
        
      DistributedFileSystem dfs = getDFS();
      List<PathBasedCacheDirective> directives =
          new LinkedList<PathBasedCacheDirective>();
      PathBasedCacheDirective directive = new PathBasedCacheDirective(path, poolName);
      directives.add(directive);
      List<Fallible<PathBasedCacheDescriptor>> results =
          dfs.addPathBasedCacheDirective(directives);
      try {
        PathBasedCacheDescriptor entry = results.get(0).get();
        System.out.println("Added PathBasedCache entry " + entry.getEntryId());
        return 0;
      } catch (IOException e) {
        System.err.println("Error adding cache directive " + directive + ": " +
          e.getMessage());
        return 1;
      }
    }
  }

  private static class RemovePathBasedCacheDirectiveCommand implements Command {
    @Override
    public String getName() {
      return "-removePath";
    }

    @Override
    public String getShortUsage() {
      return "[-removePath <id>]\n";
    }

    @Override
    public String getLongUsage() {
      return getShortUsage() +
        "Remove a cache directive.\n" +
        "<id>    The id of the cache directive to remove.\n" + 
        "        You must have write permission on the pool where the\n" +
        "        directive resides in order to remove it.  To see a list\n" +
        "        of PathBasedCache directive IDs, use the -list command.\n";
    }

    @Override
    public int run(List<String> args) throws IOException {
      String idString= StringUtils.popFirstNonOption(args);
      if (idString == null) {
        System.err.println("You must specify a directive ID to remove.");
        return 1;
      }
      long id = Long.valueOf(idString);
      if (id <= 0) {
        System.err.println("Invalid directive ID " + id + ": ids must " +
            "be greater than 0.");
        return 1;
      }
      if (!args.isEmpty()) {
        System.err.println("Can't understand argument: " + args.get(0));
        return 1;
      }
      DistributedFileSystem dfs = getDFS();
      List<Long> ids = new LinkedList<Long>();
      ids.add(id);
      List<Fallible<Long>> results = dfs.removePathBasedCacheDescriptors(ids);
      try {
        Long resultId = results.get(0).get();
        System.out.println("Removed PathBasedCache entry " + resultId);
        return 0;
      } catch (IOException e) {
        System.err.println("Error removing cache directive " + id + ": " +
          e.getMessage());
        return 1;
      }
    }
  }

  private static class ListPathBasedCacheDirectiveCommand implements Command {
    @Override
    public String getName() {
      return "-listPaths";
    }

    @Override
    public String getShortUsage() {
      return "[-listPaths [-path <path>] [-pool <pool-name>]]\n";
    }

    @Override
    public String getLongUsage() {
      return getShortUsage() +
        "List PathBasedCache directives.\n" +
        "<path> If a -path argument is given, we will list only\n" +
        "        PathBasedCache entries with this path.\n" +
        "        Note that if there is a PathBasedCache directive for <path>\n" +
        "        in a cache pool that we don't have read access for, it\n" + 
        "        not be listed.  If there are unreadable cache pools, a\n" +
        "        message will be printed.\n" +
        "        may be incomplete.\n" +
        "<pool-name> If a -pool argument is given, we will list only path\n" +
        "        cache entries in that pool.\n";
    }

    @Override
    public int run(List<String> args) throws IOException {
      String pathFilter = StringUtils.popOptionWithArgument("-path", args);
      String poolFilter = StringUtils.popOptionWithArgument("-pool", args);
      if (!args.isEmpty()) {
        System.err.println("Can't understand argument: " + args.get(0));
        return 1;
      }
      TableListing tableListing = new TableListing.Builder().
          addField("ID", Justification.RIGHT).
          addField("POOL", Justification.LEFT).
          addField("PATH", Justification.LEFT).
          build();
      DistributedFileSystem dfs = getDFS();
      RemoteIterator<PathBasedCacheDescriptor> iter =
          dfs.listPathBasedCacheDescriptors(poolFilter, pathFilter);
      int numEntries = 0;
      while (iter.hasNext()) {
        PathBasedCacheDescriptor entry = iter.next();
        String row[] = new String[] {
            "" + entry.getEntryId(), entry.getPool(), entry.getPath(),
        };
        tableListing.addRow(row);
        numEntries++;
      }
      System.out.print(String.format("Found %d entr%s\n",
          numEntries, numEntries == 1 ? "y" : "ies"));
      if (numEntries > 0) {
        System.out.print(tableListing.build());
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
      return getShortUsage() +
        "Get detailed help about a command.\n" +
        "<command-name> The command to get detailed help for.  If no " +
        "        command-name is specified, we will print detailed help " +
        "        about all commands";
    }

    @Override
    public int run(List<String> args) throws IOException {
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
      String commandName = args.get(0);
      commandName.replaceAll("^[-]*", "");
      Command command = determineCommand(commandName);
      if (command == null) {
        System.err.print("Sorry, I don't know the command '" +
          commandName + "'.\n");
        System.err.print("Valid command names are:\n");
        String separator = "";
        for (Command c : COMMANDS) {
          System.err.print(separator + c.getName());
          separator = ", ";
        }
        return 1;
      }
      System.err.print(command.getLongUsage());
      return 0;
    }
  }

  private static Command[] COMMANDS = {
    new AddPathBasedCacheDirectiveCommand(),
    new RemovePathBasedCacheDirectiveCommand(),
    new ListPathBasedCacheDirectiveCommand(),
    new HelpCommand(),
  };

  private static void printUsage(boolean longUsage) {
    System.err.println(
        "Usage: bin/hdfs cacheadmin [COMMAND]");
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
    for (int i = 0; i < COMMANDS.length; i++) {
      if (COMMANDS[i].getName().equals(commandName)) {
        return COMMANDS[i];
      }
    }
    return null;
  }

  public static void main(String[] argsArray) throws IOException {
    if (argsArray.length == 0) {
      printUsage(false);
      System.exit(1);
    }
    Command command = determineCommand(argsArray[0]);
    if (command == null) {
      System.err.println("Can't understand command '" + argsArray[0] + "'");
      if (!argsArray[0].startsWith("-")) {
        System.err.println("Command names must start with dashes.");
      }
      printUsage(false);
      System.exit(1);
    }
    List<String> args = new LinkedList<String>();
    for (int j = 1; j < argsArray.length; j++) {
      args.add(argsArray[j]);
    }
    System.exit(command.run(args));
  }
}
