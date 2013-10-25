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

import org.apache.commons.lang.WordUtils;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.AddPathBasedCacheDirectiveException;
import org.apache.hadoop.hdfs.protocol.CachePoolInfo;
import org.apache.hadoop.hdfs.protocol.PathBasedCacheDescriptor;
import org.apache.hadoop.hdfs.protocol.PathBasedCacheDirective;
import org.apache.hadoop.hdfs.protocol.RemovePathBasedCacheDescriptorException;
import org.apache.hadoop.hdfs.server.namenode.CachePool;
import org.apache.hadoop.hdfs.tools.TableListing.Justification;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;

import com.google.common.base.Joiner;

/**
 * This class implements command-line operations on the HDFS Cache.
 */
@InterfaceAudience.Private
public class CacheAdmin extends Configured implements Tool {

  /**
   * Maximum length for printed lines
   */
  private static final int MAX_LINE_WIDTH = 80;

  public CacheAdmin() {
    this(null);
  }

  public CacheAdmin(Configuration conf) {
    super(conf);
  }

  @Override
  public int run(String[] args) throws IOException {
    if (args.length == 0) {
      printUsage(false);
      return 1;
    }
    Command command = determineCommand(args[0]);
    if (command == null) {
      System.err.println("Can't understand command '" + args[0] + "'");
      if (!args[0].startsWith("-")) {
        System.err.println("Command names must start with dashes.");
      }
      printUsage(false);
      return 1;
    }
    List<String> argsList = new LinkedList<String>();
    for (int j = 1; j < args.length; j++) {
      argsList.add(args[j]);
    }
    return command.run(getConf(), argsList);
  }

  public static void main(String[] argsArray) throws IOException {
    CacheAdmin cacheAdmin = new CacheAdmin(new Configuration());
    System.exit(cacheAdmin.run(argsArray));
  }

  private static DistributedFileSystem getDFS(Configuration conf)
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
  private static String prettifyException(Exception e) {
    return e.getClass().getSimpleName() + ": "
        + e.getLocalizedMessage().split("\n")[0];
  }

  private static TableListing getOptionDescriptionListing() {
    TableListing listing = new TableListing.Builder()
    .addField("").addField("", true)
    .wrapWidth(MAX_LINE_WIDTH).hideHeaders().build();
    return listing;
  }

  interface Command {
    String getName();
    String getShortUsage();
    String getLongUsage();
    int run(Configuration conf, List<String> args) throws IOException;
  }

  private static class AddPathBasedCacheDirectiveCommand implements Command {
    @Override
    public String getName() {
      return "-addDirective";
    }

    @Override
    public String getShortUsage() {
      return "[" + getName() +
          " -path <path> -replication <replication> -pool <pool-name>]\n";
    }

    @Override
    public String getLongUsage() {
      TableListing listing = getOptionDescriptionListing();
      listing.addRow("<path>", "A path to cache. The path can be " +
          "a directory or a file.");
      listing.addRow("<replication>", "The cache replication factor to use. " +
          "Defaults to 1.");
      listing.addRow("<pool-name>", "The pool to which the directive will be " +
          "added. You must have write permission on the cache pool "
          + "in order to add new directives.");
      return getShortUsage() + "\n" +
        "Add a new PathBasedCache directive.\n\n" +
        listing.toString();
    }

    @Override
    public int run(Configuration conf, List<String> args) throws IOException {
      String path = StringUtils.popOptionWithArgument("-path", args);
      if (path == null) {
        System.err.println("You must specify a path with -path.");
        return 1;
      }
      short replication = 1;
      String replicationString =
          StringUtils.popOptionWithArgument("-replication", args);
      if (replicationString != null) {
        replication = Short.parseShort(replicationString);
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
        
      DistributedFileSystem dfs = getDFS(conf);
      PathBasedCacheDirective directive = new PathBasedCacheDirective.Builder().
          setPath(new Path(path)).
          setReplication(replication).
          setPool(poolName).
          build();
      try {
        PathBasedCacheDescriptor descriptor =
            dfs.addPathBasedCacheDirective(directive);
        System.out.println("Added PathBasedCache entry "
            + descriptor.getEntryId());
      } catch (AddPathBasedCacheDirectiveException e) {
        System.err.println(prettifyException(e));
        return 2;
      }

      return 0;
    }
  }

  private static class RemovePathBasedCacheDirectiveCommand implements Command {
    @Override
    public String getName() {
      return "-removeDirective";
    }

    @Override
    public String getShortUsage() {
      return "[" + getName() + " <id>]\n";
    }

    @Override
    public String getLongUsage() {
      TableListing listing = getOptionDescriptionListing();
      listing.addRow("<id>", "The id of the cache directive to remove.  " + 
        "You must have write permission on the pool of the " +
        "directive in order to remove it.  To see a list " +
        "of PathBasedCache directive IDs, use the -listDirectives command.");
      return getShortUsage() + "\n" +
        "Remove a cache directive.\n\n" +
        listing.toString();
    }

    @Override
    public int run(Configuration conf, List<String> args) throws IOException {
      String idString= StringUtils.popFirstNonOption(args);
      if (idString == null) {
        System.err.println("You must specify a directive ID to remove.");
        return 1;
      }
      long id;
      try {
        id = Long.valueOf(idString);
      } catch (NumberFormatException e) {
        System.err.println("Invalid directive ID " + idString + ": expected " +
            "a numeric value.");
        return 1;
      }
      if (id <= 0) {
        System.err.println("Invalid directive ID " + id + ": ids must " +
            "be greater than 0.");
        return 1;
      }
      if (!args.isEmpty()) {
        System.err.println("Can't understand argument: " + args.get(0));
        System.err.println("Usage is " + getShortUsage());
        return 1;
      }
      DistributedFileSystem dfs = getDFS(conf);
      try {
        dfs.getClient().removePathBasedCacheDescriptor(id);
        System.out.println("Removed PathBasedCache directive " + id);
      } catch (RemovePathBasedCacheDescriptorException e) {
        System.err.println(prettifyException(e));
        return 2;
      }
      return 0;
    }
  }

  private static class RemovePathBasedCacheDirectivesCommand implements Command {
    @Override
    public String getName() {
      return "-removeDirectives";
    }

    @Override
    public String getShortUsage() {
      return "[" + getName() + " <path>]\n";
    }

    @Override
    public String getLongUsage() {
      TableListing listing = getOptionDescriptionListing();
      listing.addRow("<path>", "The path of the cache directives to remove.  " +
        "You must have write permission on the pool of the directive in order " +
        "to remove it.  To see a list of cache directives, use the " +
        "-listDirectives command.");
      return getShortUsage() + "\n" +
        "Remove every cache directive with the specified path.\n\n" +
        listing.toString();
    }

    @Override
    public int run(Configuration conf, List<String> args) throws IOException {
      String path = StringUtils.popOptionWithArgument("-path", args);
      if (path == null) {
        System.err.println("You must specify a path with -path.");
        return 1;
      }
      if (!args.isEmpty()) {
        System.err.println("Can't understand argument: " + args.get(0));
        System.err.println("Usage is " + getShortUsage());
        return 1;
      }
      DistributedFileSystem dfs = getDFS(conf);
      RemoteIterator<PathBasedCacheDescriptor> iter =
          dfs.listPathBasedCacheDescriptors(null, new Path(path));
      int exitCode = 0;
      while (iter.hasNext()) {
        PathBasedCacheDescriptor entry = iter.next();
        try {
          dfs.removePathBasedCacheDescriptor(entry);
          System.out.println("Removed PathBasedCache directive " +
              entry.getEntryId());
        } catch (RemovePathBasedCacheDescriptorException e) {
          System.err.println(prettifyException(e));
          exitCode = 2;
        }
      }
      if (exitCode == 0) {
        System.out.println("Removed every PathBasedCache directive with path " +
            path);
      }
      return exitCode;
    }
  }

  private static class ListPathBasedCacheDirectiveCommand implements Command {
    @Override
    public String getName() {
      return "-listDirectives";
    }

    @Override
    public String getShortUsage() {
      return "[" + getName() + " [-path <path>] [-pool <pool>]]\n";
    }

    @Override
    public String getLongUsage() {
      TableListing listing = getOptionDescriptionListing();
      listing.addRow("<path>", "List only " +
          "PathBasedCache directives with this path. " +
          "Note that if there is a PathBasedCache directive for <path> " +
          "in a cache pool that we don't have read access for, it " + 
          "will not be listed.");
      listing.addRow("<pool>", "List only path cache directives in that pool.");
      return getShortUsage() + "\n" +
        "List PathBasedCache directives.\n\n" +
        listing.toString();
    }

    @Override
    public int run(Configuration conf, List<String> args) throws IOException {
      String pathFilter = StringUtils.popOptionWithArgument("-path", args);
      String poolFilter = StringUtils.popOptionWithArgument("-pool", args);
      if (!args.isEmpty()) {
        System.err.println("Can't understand argument: " + args.get(0));
        return 1;
      }
      TableListing tableListing = new TableListing.Builder().
          addField("ID", Justification.LEFT).
          addField("POOL", Justification.LEFT).
          addField("PATH", Justification.LEFT).
          build();
      DistributedFileSystem dfs = getDFS(conf);
      RemoteIterator<PathBasedCacheDescriptor> iter =
          dfs.listPathBasedCacheDescriptors(poolFilter, pathFilter != null ?
              new Path(pathFilter) : null);
      int numEntries = 0;
      while (iter.hasNext()) {
        PathBasedCacheDescriptor entry = iter.next();
        String row[] = new String[] {
            "" + entry.getEntryId(), entry.getPool(),
            entry.getPath().toUri().getPath(),
        };
        tableListing.addRow(row);
        numEntries++;
      }
      System.out.print(String.format("Found %d entr%s\n",
          numEntries, numEntries == 1 ? "y" : "ies"));
      if (numEntries > 0) {
        System.out.print(tableListing);
      }
      return 0;
    }
  }

  private static class AddCachePoolCommand implements Command {

    private static final String NAME = "-addPool";

    @Override
    public String getName() {
      return NAME;
    }

    @Override
    public String getShortUsage() {
      return "[" + NAME + " <name> [-owner <owner>] " +
          "[-group <group>] [-mode <mode>] [-weight <weight>]]\n";
    }

    @Override
    public String getLongUsage() {
      TableListing listing = getOptionDescriptionListing();

      listing.addRow("<name>", "Name of the new pool.");
      listing.addRow("<owner>", "Username of the owner of the pool. " +
          "Defaults to the current user.");
      listing.addRow("<group>", "Group of the pool. " +
          "Defaults to the primary group name of the current user.");
      listing.addRow("<mode>", "UNIX-style permissions for the pool. " +
          "Permissions are specified in octal, e.g. 0755. " +
          "By default, this is set to " + String.format("0%03o",
          FsPermission.getCachePoolDefault().toShort()));
      listing.addRow("<weight>", "Weight of the pool. " +
          "This is a relative measure of the importance of the pool used " +
          "during cache resource management. By default, it is set to " +
          CachePool.DEFAULT_WEIGHT);

      return getShortUsage() + "\n" +
          "Add a new cache pool.\n\n" + 
          listing.toString();
    }

    @Override
    public int run(Configuration conf, List<String> args) throws IOException {
      String owner = StringUtils.popOptionWithArgument("-owner", args);
      if (owner == null) {
        owner = UserGroupInformation.getCurrentUser().getShortUserName();
      }
      String group = StringUtils.popOptionWithArgument("-group", args);
      if (group == null) {
        group = UserGroupInformation.getCurrentUser().getGroupNames()[0];
      }
      String modeString = StringUtils.popOptionWithArgument("-mode", args);
      int mode;
      if (modeString == null) {
        mode = FsPermission.getCachePoolDefault().toShort();
      } else {
        mode = Integer.parseInt(modeString, 8);
      }
      String weightString = StringUtils.popOptionWithArgument("-weight", args);
      int weight;
      if (weightString == null) {
        weight = CachePool.DEFAULT_WEIGHT;
      } else {
        weight = Integer.parseInt(weightString);
      }
      String name = StringUtils.popFirstNonOption(args);
      if (name == null) {
        System.err.println("You must specify a name when creating a " +
            "cache pool.");
        return 1;
      }
      if (!args.isEmpty()) {
        System.err.print("Can't understand arguments: " +
          Joiner.on(" ").join(args) + "\n");
        System.err.println("Usage is " + getShortUsage());
        return 1;
      }
      DistributedFileSystem dfs = getDFS(conf);
      CachePoolInfo info = new CachePoolInfo(name).
          setOwnerName(owner).
          setGroupName(group).
          setMode(new FsPermission((short)mode)).
          setWeight(weight);
      try {
        dfs.addCachePool(info);
      } catch (IOException e) {
        throw new RemoteException(e.getClass().getName(), e.getMessage());
      }
      System.out.println("Successfully added cache pool " + name + ".");
      return 0;
    }
  }

  private static class ModifyCachePoolCommand implements Command {

    @Override
    public String getName() {
      return "-modifyPool";
    }

    @Override
    public String getShortUsage() {
      return "[" + getName() + " <name> [-owner <owner>] " +
          "[-group <group>] [-mode <mode>] [-weight <weight>]]\n";
    }

    @Override
    public String getLongUsage() {
      TableListing listing = getOptionDescriptionListing();

      listing.addRow("<name>", "Name of the pool to modify.");
      listing.addRow("<owner>", "Username of the owner of the pool");
      listing.addRow("<group>", "Groupname of the group of the pool.");
      listing.addRow("<mode>", "Unix-style permissions of the pool in octal.");
      listing.addRow("<weight>", "Weight of the pool.");

      return getShortUsage() + "\n" +
          WordUtils.wrap("Modifies the metadata of an existing cache pool. " +
          "See usage of " + AddCachePoolCommand.NAME + " for more details",
          MAX_LINE_WIDTH) + "\n\n" +
          listing.toString();
    }

    @Override
    public int run(Configuration conf, List<String> args) throws IOException {
      String owner = StringUtils.popOptionWithArgument("-owner", args);
      String group = StringUtils.popOptionWithArgument("-group", args);
      String modeString = StringUtils.popOptionWithArgument("-mode", args);
      Integer mode = (modeString == null) ?
          null : Integer.parseInt(modeString, 8);
      String weightString = StringUtils.popOptionWithArgument("-weight", args);
      Integer weight = (weightString == null) ?
          null : Integer.parseInt(weightString);
      String name = StringUtils.popFirstNonOption(args);
      if (name == null) {
        System.err.println("You must specify a name when creating a " +
            "cache pool.");
        return 1;
      }
      if (!args.isEmpty()) {
        System.err.print("Can't understand arguments: " +
          Joiner.on(" ").join(args) + "\n");
        System.err.println("Usage is " + getShortUsage());
        return 1;
      }
      boolean changed = false;
      CachePoolInfo info = new CachePoolInfo(name);
      if (owner != null) {
        info.setOwnerName(owner);
        changed = true;
      }
      if (group != null) {
        info.setGroupName(group);
        changed = true;
      }
      if (mode != null) {
        info.setMode(new FsPermission(mode.shortValue()));
        changed = true;
      }
      if (weight != null) {
        info.setWeight(weight);
        changed = true;
      }
      if (!changed) {
        System.err.println("You must specify at least one attribute to " +
            "change in the cache pool.");
        return 1;
      }
      DistributedFileSystem dfs = getDFS(conf);
      try {
        dfs.modifyCachePool(info);
      } catch (IOException e) {
        throw new RemoteException(e.getClass().getName(), e.getMessage());
      }
      System.out.print("Successfully modified cache pool " + name);
      String prefix = " to have ";
      if (owner != null) {
        System.out.print(prefix + "owner name " + owner);
        prefix = " and ";
      }
      if (group != null) {
        System.out.print(prefix + "group name " + group);
        prefix = " and ";
      }
      if (mode != null) {
        System.out.print(prefix + "mode " + new FsPermission(mode.shortValue()));
        prefix = " and ";
      }
      if (weight != null) {
        System.out.print(prefix + "weight " + weight);
        prefix = " and ";
      }
      System.out.print("\n");
      return 0;
    }
  }

  private static class RemoveCachePoolCommand implements Command {

    @Override
    public String getName() {
      return "-removePool";
    }

    @Override
    public String getShortUsage() {
      return "[" + getName() + " <name>]\n";
    }

    @Override
    public String getLongUsage() {
      return getShortUsage() + "\n" +
          WordUtils.wrap("Remove a cache pool. This also uncaches paths " +
              "associated with the pool.\n\n", MAX_LINE_WIDTH) +
          "<name>  Name of the cache pool to remove.\n";
    }

    @Override
    public int run(Configuration conf, List<String> args) throws IOException {
      String name = StringUtils.popFirstNonOption(args);
      if (name == null) {
        System.err.println("You must specify a name when deleting a " +
            "cache pool.");
        return 1;
      }
      if (!args.isEmpty()) {
        System.err.print("Can't understand arguments: " +
          Joiner.on(" ").join(args) + "\n");
        System.err.println("Usage is " + getShortUsage());
        return 1;
      }
      DistributedFileSystem dfs = getDFS(conf);
      try {
        dfs.removeCachePool(name);
      } catch (IOException e) {
        throw new RemoteException(e.getClass().getName(), e.getMessage());
      }
      System.out.println("Successfully removed cache pool " + name + ".");
      return 0;
    }
  }

  private static class ListCachePoolsCommand implements Command {

    @Override
    public String getName() {
      return "-listPools";
    }

    @Override
    public String getShortUsage() {
      return "[" + getName() + " [name]]\n";
    }

    @Override
    public String getLongUsage() {
      TableListing listing = getOptionDescriptionListing();
      listing.addRow("[name]", "If specified, list only the named cache pool.");

      return getShortUsage() + "\n" +
          WordUtils.wrap("Display information about one or more cache pools, " +
              "e.g. name, owner, group, permissions, etc.", MAX_LINE_WIDTH) +
          "\n\n" +
          listing.toString();
    }

    @Override
    public int run(Configuration conf, List<String> args) throws IOException {
      String name = StringUtils.popFirstNonOption(args);
      if (!args.isEmpty()) {
        System.err.print("Can't understand arguments: " +
          Joiner.on(" ").join(args) + "\n");
        System.err.println("Usage is " + getShortUsage());
        return 1;
      }
      DistributedFileSystem dfs = getDFS(conf);
      TableListing listing = new TableListing.Builder().
          addField("NAME", Justification.LEFT).
          addField("OWNER", Justification.LEFT).
          addField("GROUP", Justification.LEFT).
          addField("MODE", Justification.LEFT).
          addField("WEIGHT", Justification.LEFT).
          build();
      int numResults = 0;
      try {
        RemoteIterator<CachePoolInfo> iter = dfs.listCachePools();
        while (iter.hasNext()) {
          CachePoolInfo info = iter.next();
          if (name == null || info.getPoolName().equals(name)) {
            listing.addRow(new String[] {
                info.getPoolName(),
                info.getOwnerName(),
                info.getGroupName(),
                info.getMode().toString(),
                info.getWeight().toString(),
            });
            ++numResults;
            if (name != null) {
              break;
            }
          }
        }
      } catch (IOException e) {
        throw new RemoteException(e.getClass().getName(), e.getMessage());
      }
      System.out.print(String.format("Found %d result%s.\n", numResults,
          (numResults == 1 ? "" : "s")));
      if (numResults > 0) { 
        System.out.print(listing);
      }
      // If there are no results, we return 1 (failure exit code);
      // otherwise we return 0 (success exit code).
      return (numResults == 0) ? 1 : 0;
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
      TableListing listing = getOptionDescriptionListing();
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
      String commandName = args.get(0);
      commandName = commandName.replaceAll("^[-]*", "");
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
        System.err.print("\n");
        return 1;
      }
      System.err.print(command.getLongUsage());
      return 0;
    }
  }

  private static Command[] COMMANDS = {
    new AddPathBasedCacheDirectiveCommand(),
    new RemovePathBasedCacheDirectiveCommand(),
    new RemovePathBasedCacheDirectivesCommand(),
    new ListPathBasedCacheDirectiveCommand(),
    new AddCachePoolCommand(),
    new ModifyCachePoolCommand(),
    new RemoveCachePoolCommand(),
    new ListCachePoolsCommand(),
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
}
