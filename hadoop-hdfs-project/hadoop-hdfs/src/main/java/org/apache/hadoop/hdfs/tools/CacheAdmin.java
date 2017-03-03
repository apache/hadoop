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
import java.util.EnumSet;
import java.util.LinkedList;
import java.util.List;

import org.apache.commons.lang.WordUtils;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.CacheFlag;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.CacheDirectiveEntry;
import org.apache.hadoop.hdfs.protocol.CacheDirectiveInfo;
import org.apache.hadoop.hdfs.protocol.CacheDirectiveInfo.Expiration;
import org.apache.hadoop.hdfs.protocol.CacheDirectiveStats;
import org.apache.hadoop.hdfs.protocol.CachePoolEntry;
import org.apache.hadoop.hdfs.protocol.CachePoolInfo;
import org.apache.hadoop.hdfs.protocol.CachePoolStats;
import org.apache.hadoop.tools.TableListing;
import org.apache.hadoop.tools.TableListing.Justification;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;

import com.google.common.base.Joiner;
import org.apache.hadoop.util.ToolRunner;

/**
 * This class implements command-line operations on the HDFS Cache.
 */
@InterfaceAudience.Private
public class CacheAdmin extends Configured implements Tool {

  public CacheAdmin() {
    this(null);
  }

  public CacheAdmin(Configuration conf) {
    super(conf);
  }

  @Override
  public int run(String[] args) throws IOException {
    if (args.length == 0) {
      AdminHelper.printUsage(false, "cacheadmin", COMMANDS);
      ToolRunner.printGenericCommandUsage(System.err);
      return 1;
    }
    AdminHelper.Command command = AdminHelper.determineCommand(args[0],
        COMMANDS);
    if (command == null) {
      System.err.println("Can't understand command '" + args[0] + "'");
      if (!args[0].startsWith("-")) {
        System.err.println("Command names must start with dashes.");
      }
      AdminHelper.printUsage(false, "cacheadmin", COMMANDS);
      ToolRunner.printGenericCommandUsage(System.err);
      return 1;
    }
    List<String> argsList = new LinkedList<String>();
    for (int j = 1; j < args.length; j++) {
      argsList.add(args[j]);
    }
    try {
      return command.run(getConf(), argsList);
    } catch (IllegalArgumentException e) {
      System.err.println(AdminHelper.prettifyException(e));
      return -1;
    }
  }

  public static void main(String[] argsArray) throws Exception {
    CacheAdmin cacheAdmin = new CacheAdmin(new Configuration());
    int res = ToolRunner.run(cacheAdmin, argsArray);
    System.exit(res);
  }

  private static CacheDirectiveInfo.Expiration parseExpirationString(String ttlString)
      throws IOException {
    CacheDirectiveInfo.Expiration ex = null;
    if (ttlString != null) {
      if (ttlString.equalsIgnoreCase("never")) {
        ex = CacheDirectiveInfo.Expiration.NEVER;
      } else {
        long ttl = DFSUtil.parseRelativeTime(ttlString);
        ex = CacheDirectiveInfo.Expiration.newRelative(ttl);
      }
    }
    return ex;
  }

  private static class AddCacheDirectiveInfoCommand
      implements AdminHelper.Command {
    @Override
    public String getName() {
      return "-addDirective";
    }

    @Override
    public String getShortUsage() {
      return "[" + getName() +
          " -path <path> -pool <pool-name> " +
          "[-force] " +
          "[-replication <replication>] [-ttl <time-to-live>]]\n";
    }

    @Override
    public String getLongUsage() {
      TableListing listing = AdminHelper.getOptionDescriptionListing();
      listing.addRow("<path>", "A path to cache. The path can be " +
          "a directory or a file.");
      listing.addRow("<pool-name>", "The pool to which the directive will be " +
          "added. You must have write permission on the cache pool "
          + "in order to add new directives.");
      listing.addRow("-force",
          "Skips checking of cache pool resource limits.");
      listing.addRow("<replication>", "The cache replication factor to use. " +
          "Defaults to 1.");
      listing.addRow("<time-to-live>", "How long the directive is " +
          "valid. Can be specified in minutes, hours, and days, e.g. " +
          "30m, 4h, 2d. Valid units are [smhd]." +
          " \"never\" indicates a directive that never expires." +
          " If unspecified, the directive never expires.");
      return getShortUsage() + "\n" +
        "Add a new cache directive.\n\n" +
        listing.toString();
    }

    @Override
    public int run(Configuration conf, List<String> args) throws IOException {
      CacheDirectiveInfo.Builder builder = new CacheDirectiveInfo.Builder();

      String path = StringUtils.popOptionWithArgument("-path", args);
      if (path == null) {
        System.err.println("You must specify a path with -path.");
        return 1;
      }
      builder.setPath(new Path(path));

      String poolName = StringUtils.popOptionWithArgument("-pool", args);
      if (poolName == null) {
        System.err.println("You must specify a pool name with -pool.");
        return 1;
      }
      builder.setPool(poolName);
      boolean force = StringUtils.popOption("-force", args);
      String replicationString =
          StringUtils.popOptionWithArgument("-replication", args);
      if (replicationString != null) {
        Short replication = Short.parseShort(replicationString);
        builder.setReplication(replication);
      }

      String ttlString = StringUtils.popOptionWithArgument("-ttl", args);
      try {
        Expiration ex = parseExpirationString(ttlString);
        if (ex != null) {
          builder.setExpiration(ex);
        }
      } catch (IOException e) {
        System.err.println(
            "Error while parsing ttl value: " + e.getMessage());
        return 1;
      }

      if (!args.isEmpty()) {
        System.err.println("Can't understand argument: " + args.get(0));
        return 1;
      }

      DistributedFileSystem dfs =
          AdminHelper.getDFS(new Path(path).toUri(), conf);
      CacheDirectiveInfo directive = builder.build();
      EnumSet<CacheFlag> flags = EnumSet.noneOf(CacheFlag.class);
      if (force) {
        flags.add(CacheFlag.FORCE);
      }
      try {
        long id = dfs.addCacheDirective(directive, flags);
        System.out.println("Added cache directive " + id);
      } catch (IOException e) {
        System.err.println(AdminHelper.prettifyException(e));
        return 2;
      }

      return 0;
    }
  }

  private static class RemoveCacheDirectiveInfoCommand
      implements AdminHelper.Command {
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
      TableListing listing = AdminHelper.getOptionDescriptionListing();
      listing.addRow("<id>", "The id of the cache directive to remove.  " + 
        "You must have write permission on the pool of the " +
        "directive in order to remove it.  To see a list " +
        "of cache directive IDs, use the -listDirectives command.");
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
        id = Long.parseLong(idString);
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
      DistributedFileSystem dfs = AdminHelper.getDFS(conf);
      try {
        dfs.getClient().removeCacheDirective(id);
        System.out.println("Removed cached directive " + id);
      } catch (IOException e) {
        System.err.println(AdminHelper.prettifyException(e));
        return 2;
      }
      return 0;
    }
  }

  private static class ModifyCacheDirectiveInfoCommand
      implements AdminHelper.Command {
    @Override
    public String getName() {
      return "-modifyDirective";
    }

    @Override
    public String getShortUsage() {
      return "[" + getName() +
          " -id <id> [-path <path>] [-force] [-replication <replication>] " +
          "[-pool <pool-name>] [-ttl <time-to-live>]]\n";
    }

    @Override
    public String getLongUsage() {
      TableListing listing = AdminHelper.getOptionDescriptionListing();
      listing.addRow("<id>", "The ID of the directive to modify (required)");
      listing.addRow("<path>", "A path to cache. The path can be " +
          "a directory or a file. (optional)");
      listing.addRow("-force",
          "Skips checking of cache pool resource limits.");
      listing.addRow("<replication>", "The cache replication factor to use. " +
          "(optional)");
      listing.addRow("<pool-name>", "The pool to which the directive will be " +
          "added. You must have write permission on the cache pool "
          + "in order to move a directive into it. (optional)");
      listing.addRow("<time-to-live>", "How long the directive is " +
          "valid. Can be specified in minutes, hours, and days, e.g. " +
          "30m, 4h, 2d. Valid units are [smhd]." +
          " \"never\" indicates a directive that never expires.");
      return getShortUsage() + "\n" +
        "Modify a cache directive.\n\n" +
        listing.toString();
    }

    @Override
    public int run(Configuration conf, List<String> args) throws IOException {
      CacheDirectiveInfo.Builder builder =
        new CacheDirectiveInfo.Builder();
      boolean modified = false;
      String idString = StringUtils.popOptionWithArgument("-id", args);
      if (idString == null) {
        System.err.println("You must specify a directive ID with -id.");
        return 1;
      }
      builder.setId(Long.parseLong(idString));
      String path = StringUtils.popOptionWithArgument("-path", args);
      if (path != null) {
        builder.setPath(new Path(path));
        modified = true;
      }
      boolean force = StringUtils.popOption("-force", args);
      String replicationString =
        StringUtils.popOptionWithArgument("-replication", args);
      if (replicationString != null) {
        builder.setReplication(Short.parseShort(replicationString));
        modified = true;
      }
      String poolName =
        StringUtils.popOptionWithArgument("-pool", args);
      if (poolName != null) {
        builder.setPool(poolName);
        modified = true;
      }
      String ttlString = StringUtils.popOptionWithArgument("-ttl", args);
      try {
        Expiration ex = parseExpirationString(ttlString);
        if (ex != null) {
          builder.setExpiration(ex);
          modified = true;
        }
      } catch (IOException e) {
        System.err.println(
            "Error while parsing ttl value: " + e.getMessage());
        return 1;
      }
      if (!args.isEmpty()) {
        System.err.println("Can't understand argument: " + args.get(0));
        System.err.println("Usage is " + getShortUsage());
        return 1;
      }
      if (!modified) {
        System.err.println("No modifications were specified.");
        return 1;
      }
      DistributedFileSystem dfs = AdminHelper.getDFS(conf);
      EnumSet<CacheFlag> flags = EnumSet.noneOf(CacheFlag.class);
      if (force) {
        flags.add(CacheFlag.FORCE);
      }
      try {
        dfs.modifyCacheDirective(builder.build(), flags);
        System.out.println("Modified cache directive " + idString);
      } catch (IOException e) {
        System.err.println(AdminHelper.prettifyException(e));
        return 2;
      }
      return 0;
    }
  }

  private static class RemoveCacheDirectiveInfosCommand
      implements AdminHelper.Command {
    @Override
    public String getName() {
      return "-removeDirectives";
    }

    @Override
    public String getShortUsage() {
      return "[" + getName() + " -path <path>]\n";
    }

    @Override
    public String getLongUsage() {
      TableListing listing = AdminHelper.getOptionDescriptionListing();
      listing.addRow("-path <path>", "The path of the cache directives to remove.  " +
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
      int exitCode = 0;
      try {
        DistributedFileSystem dfs =
            AdminHelper.getDFS(new Path(path).toUri(), conf);
        RemoteIterator<CacheDirectiveEntry> iter =
            dfs.listCacheDirectives(
                new CacheDirectiveInfo.Builder().
                    setPath(new Path(path)).build());
        while (iter.hasNext()) {
          CacheDirectiveEntry entry = iter.next();
          try {
            dfs.removeCacheDirective(entry.getInfo().getId());
            System.out.println("Removed cache directive " +
                entry.getInfo().getId());
          } catch (IOException e) {
            System.err.println(AdminHelper.prettifyException(e));
            exitCode = 2;
          }
        }
      } catch (IOException e) {
        System.err.println(AdminHelper.prettifyException(e));
        exitCode = 2;
      }
      if (exitCode == 0) {
        System.out.println("Removed every cache directive with path " +
            path);
      }
      return exitCode;
    }
  }

  private static class ListCacheDirectiveInfoCommand
      implements AdminHelper.Command {
    @Override
    public String getName() {
      return "-listDirectives";
    }

    @Override
    public String getShortUsage() {
      return "[" + getName()
          + " [-stats] [-path <path>] [-pool <pool>] [-id <id>]"
          + "]\n";
    }

    @Override
    public String getLongUsage() {
      TableListing listing = AdminHelper.getOptionDescriptionListing();
      listing.addRow("-stats", "List path-based cache directive statistics.");
      listing.addRow("<path>", "List only " +
          "cache directives with this path. " +
          "Note that if there is a cache directive for <path> " +
          "in a cache pool that we don't have read access for, it " + 
          "will not be listed.");
      listing.addRow("<pool>", "List only path cache directives in that pool.");
      listing.addRow("<id>", "List the cache directive with this id.");
      return getShortUsage() + "\n" +
        "List cache directives.\n\n" +
        listing.toString();
    }

    @Override
    public int run(Configuration conf, List<String> args) throws IOException {
      CacheDirectiveInfo.Builder builder =
          new CacheDirectiveInfo.Builder();
      String pathFilter = StringUtils.popOptionWithArgument("-path", args);
      if (pathFilter != null) {
        builder.setPath(new Path(pathFilter));
      }
      String poolFilter = StringUtils.popOptionWithArgument("-pool", args);
      if (poolFilter != null) {
        builder.setPool(poolFilter);
      }
      boolean printStats = StringUtils.popOption("-stats", args);
      String idFilter = StringUtils.popOptionWithArgument("-id", args);
      if (idFilter != null) {
        builder.setId(Long.parseLong(idFilter));
      }
      if (!args.isEmpty()) {
        System.err.println("Can't understand argument: " + args.get(0));
        return 1;
      }
      TableListing.Builder tableBuilder = new TableListing.Builder().
          addField("ID", Justification.RIGHT).
          addField("POOL", Justification.LEFT).
          addField("REPL", Justification.RIGHT).
          addField("EXPIRY", Justification.LEFT).
          addField("PATH", Justification.LEFT);
      if (printStats) {
        tableBuilder.addField("BYTES_NEEDED", Justification.RIGHT).
                    addField("BYTES_CACHED", Justification.RIGHT).
                    addField("FILES_NEEDED", Justification.RIGHT).
                    addField("FILES_CACHED", Justification.RIGHT);
      }
      TableListing tableListing = tableBuilder.build();
      try {
        DistributedFileSystem dfs = AdminHelper.getDFS(conf);
        RemoteIterator<CacheDirectiveEntry> iter =
            dfs.listCacheDirectives(builder.build());
        int numEntries = 0;
        while (iter.hasNext()) {
          CacheDirectiveEntry entry = iter.next();
          CacheDirectiveInfo directive = entry.getInfo();
          CacheDirectiveStats stats = entry.getStats();
          List<String> row = new LinkedList<String>();
          row.add("" + directive.getId());
          row.add(directive.getPool());
          row.add("" + directive.getReplication());
          String expiry;
          // This is effectively never, round for nice printing
          if (directive.getExpiration().getMillis() >
              Expiration.MAX_RELATIVE_EXPIRY_MS / 2) {
            expiry = "never";
          } else {
            expiry = directive.getExpiration().toString();
          }
          row.add(expiry);
          row.add(directive.getPath().toUri().getPath());
          if (printStats) {
            row.add("" + stats.getBytesNeeded());
            row.add("" + stats.getBytesCached());
            row.add("" + stats.getFilesNeeded());
            row.add("" + stats.getFilesCached());
          }
          tableListing.addRow(row.toArray(new String[row.size()]));
          numEntries++;
        }
        System.out.print(String.format("Found %d entr%s%n",
            numEntries, numEntries == 1 ? "y" : "ies"));
        if (numEntries > 0) {
          System.out.print(tableListing);
        }
      } catch (IOException e) {
        System.err.println(AdminHelper.prettifyException(e));
        return 2;
      }
      return 0;
    }
  }

  private static class AddCachePoolCommand implements AdminHelper.Command {

    private static final String NAME = "-addPool";

    @Override
    public String getName() {
      return NAME;
    }

    @Override
    public String getShortUsage() {
      return "[" + NAME + " <name> [-owner <owner>] " +
          "[-group <group>] [-mode <mode>] [-limit <limit>] " +
          "[-defaultReplication <defaultReplication>] [-maxTtl <maxTtl>]" +
          "]\n";
    }

    @Override
    public String getLongUsage() {
      TableListing listing = AdminHelper.getOptionDescriptionListing();

      listing.addRow("<name>", "Name of the new pool.");
      listing.addRow("<owner>", "Username of the owner of the pool. " +
          "Defaults to the current user.");
      listing.addRow("<group>", "Group of the pool. " +
          "Defaults to the primary group name of the current user.");
      listing.addRow("<mode>", "UNIX-style permissions for the pool. " +
          "Permissions are specified in octal, e.g. 0755. " +
          "By default, this is set to " + String.format("0%03o",
          FsPermission.getCachePoolDefault().toShort()) + ".");
      listing.addRow("<limit>", "The maximum number of bytes that can be " +
          "cached by directives in this pool, in aggregate. By default, " +
          "no limit is set.");
      listing.addRow("<defaultReplication>", "The default replication " +
          "number for cache directive in the pool. " +
          "If not set, the replication is set to 1");
      listing.addRow("<maxTtl>", "The maximum allowed time-to-live for " +
          "directives being added to the pool. This can be specified in " +
          "seconds, minutes, hours, and days, e.g. 120s, 30m, 4h, 2d. " +
          "Valid units are [smhd]. By default, no maximum is set. " +
          "A value of \"never\" specifies that there is no limit.");
      return getShortUsage() + "\n" +
          "Add a new cache pool.\n\n" + 
          listing.toString();
    }

    @Override
    public int run(Configuration conf, List<String> args) throws IOException {
      String name = StringUtils.popFirstNonOption(args);
      if (name == null) {
        System.err.println("You must specify a name when creating a " +
            "cache pool.");
        return 1;
      }
      CachePoolInfo info = new CachePoolInfo(name);

      String owner = StringUtils.popOptionWithArgument("-owner", args);
      if (owner != null) {
        info.setOwnerName(owner);
      }
      String group = StringUtils.popOptionWithArgument("-group", args);
      if (group != null) {
        info.setGroupName(group);
      }
      String modeString = StringUtils.popOptionWithArgument("-mode", args);
      if (modeString != null) {
        short mode = Short.parseShort(modeString, 8);
        info.setMode(new FsPermission(mode));
      }
      String limitString = StringUtils.popOptionWithArgument("-limit", args);
      Long limit = AdminHelper.parseLimitString(limitString);
      if (limit != null) {
        info.setLimit(limit);
      }
      String replicationString = StringUtils.
              popOptionWithArgument("-defaultReplication", args);
      if (replicationString != null) {
        short defaultReplication = Short.parseShort(replicationString);
        info.setDefaultReplication(defaultReplication);
      }
      String maxTtlString = StringUtils.popOptionWithArgument("-maxTtl", args);
      try {
        Long maxTtl = AdminHelper.parseTtlString(maxTtlString);
        if (maxTtl != null) {
          info.setMaxRelativeExpiryMs(maxTtl);
        }
      } catch (IOException e) {
        System.err.println(
            "Error while parsing maxTtl value: " + e.getMessage());
        return 1;
      }

      if (!args.isEmpty()) {
        System.err.print("Can't understand arguments: " +
          Joiner.on(" ").join(args) + "\n");
        System.err.println("Usage is " + getShortUsage());
        return 1;
      }
      DistributedFileSystem dfs = AdminHelper.getDFS(conf);
      try {
        dfs.addCachePool(info);
      } catch (IOException e) {
        System.err.println(AdminHelper.prettifyException(e));
        return 2;
      }
      System.out.println("Successfully added cache pool " + name + ".");
      return 0;
    }
  }

  private static class ModifyCachePoolCommand implements AdminHelper.Command {

    @Override
    public String getName() {
      return "-modifyPool";
    }

    @Override
    public String getShortUsage() {
      return "[" + getName() + " <name> [-owner <owner>] " +
          "[-group <group>] [-mode <mode>] [-limit <limit>] " +
          "[-defaultReplication <defaultReplication>] [-maxTtl <maxTtl>]]\n";
    }

    @Override
    public String getLongUsage() {
      TableListing listing = AdminHelper.getOptionDescriptionListing();

      listing.addRow("<name>", "Name of the pool to modify.");
      listing.addRow("<owner>", "Username of the owner of the pool");
      listing.addRow("<group>", "Groupname of the group of the pool.");
      listing.addRow("<mode>", "Unix-style permissions of the pool in octal.");
      listing.addRow("<limit>", "Maximum number of bytes that can be cached " +
          "by this pool.");
      listing.addRow("<defaultReplication>", "Default replication num for " +
          "directives in this pool");
      listing.addRow("<maxTtl>", "The maximum allowed time-to-live for " +
          "directives being added to the pool.");

      return getShortUsage() + "\n" +
          WordUtils.wrap("Modifies the metadata of an existing cache pool. " +
          "See usage of " + AddCachePoolCommand.NAME + " for more details.",
          AdminHelper.MAX_LINE_WIDTH) + "\n\n" +
          listing.toString();
    }

    @Override
    public int run(Configuration conf, List<String> args) throws IOException {
      String owner = StringUtils.popOptionWithArgument("-owner", args);
      String group = StringUtils.popOptionWithArgument("-group", args);
      String modeString = StringUtils.popOptionWithArgument("-mode", args);
      Integer mode = (modeString == null) ?
          null : Integer.parseInt(modeString, 8);
      String limitString = StringUtils.popOptionWithArgument("-limit", args);
      Long limit = AdminHelper.parseLimitString(limitString);
      String replicationString =
              StringUtils.popOptionWithArgument("-defaultReplication", args);
      Short defaultReplication = null;
      if (replicationString != null) {
        defaultReplication = Short.parseShort(replicationString);
      }
      String maxTtlString = StringUtils.popOptionWithArgument("-maxTtl", args);
      Long maxTtl;
      try {
        maxTtl = AdminHelper.parseTtlString(maxTtlString);
      } catch (IOException e) {
        System.err.println(
            "Error while parsing maxTtl value: " + e.getMessage());
        return 1;
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
      if (limit != null) {
        info.setLimit(limit);
        changed = true;
      }
      if (defaultReplication != null) {
        info.setDefaultReplication(defaultReplication);
        changed = true;
      }
      if (maxTtl != null) {
        info.setMaxRelativeExpiryMs(maxTtl);
        changed = true;
      }
      if (!changed) {
        System.err.println("You must specify at least one attribute to " +
            "change in the cache pool.");
        return 1;
      }
      DistributedFileSystem dfs = AdminHelper.getDFS(conf);
      try {
        dfs.modifyCachePool(info);
      } catch (IOException e) {
        System.err.println(AdminHelper.prettifyException(e));
        return 2;
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
      if (limit != null) {
        System.out.print(prefix + "limit " + limit);
        prefix = " and ";
      }
      if (defaultReplication != null) {
        System.out.println(prefix + "replication " + defaultReplication);
        prefix = " replication ";
      }
      if (maxTtl != null) {
        System.out.print(prefix + "max time-to-live " + maxTtlString);
      }
      System.out.print("\n");
      return 0;
    }
  }

  private static class RemoveCachePoolCommand implements AdminHelper.Command {

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
              "associated with the pool.\n\n", AdminHelper.MAX_LINE_WIDTH) +
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
      DistributedFileSystem dfs = AdminHelper.getDFS(conf);
      try {
        dfs.removeCachePool(name);
      } catch (IOException e) {
        System.err.println(AdminHelper.prettifyException(e));
        return 2;
      }
      System.out.println("Successfully removed cache pool " + name + ".");
      return 0;
    }
  }

  private static class ListCachePoolsCommand implements AdminHelper.Command {

    @Override
    public String getName() {
      return "-listPools";
    }

    @Override
    public String getShortUsage() {
      return "[" + getName() + " [-stats] [<name>]]\n";
    }

    @Override
    public String getLongUsage() {
      TableListing listing = AdminHelper.getOptionDescriptionListing();
      listing.addRow("-stats", "Display additional cache pool statistics.");
      listing.addRow("<name>", "If specified, list only the named cache pool.");

      return getShortUsage() + "\n" +
          WordUtils.wrap("Display information about one or more cache pools, " +
              "e.g. name, owner, group, permissions, etc.",
              AdminHelper.MAX_LINE_WIDTH) + "\n\n" + listing.toString();
    }

    @Override
    public int run(Configuration conf, List<String> args) throws IOException {
      String name = StringUtils.popFirstNonOption(args);
      final boolean printStats = StringUtils.popOption("-stats", args);
      if (!args.isEmpty()) {
        System.err.print("Can't understand arguments: " +
          Joiner.on(" ").join(args) + "\n");
        System.err.println("Usage is " + getShortUsage());
        return 1;
      }
      DistributedFileSystem dfs = AdminHelper.getDFS(conf);
      TableListing.Builder builder = new TableListing.Builder().
          addField("NAME", Justification.LEFT).
          addField("OWNER", Justification.LEFT).
          addField("GROUP", Justification.LEFT).
          addField("MODE", Justification.LEFT).
          addField("LIMIT", Justification.RIGHT).
          addField("MAXTTL", Justification.RIGHT).
          addField("DEFAULT_REPLICATION", Justification.RIGHT);
      if (printStats) {
        builder.
            addField("BYTES_NEEDED", Justification.RIGHT).
            addField("BYTES_CACHED", Justification.RIGHT).
            addField("BYTES_OVERLIMIT", Justification.RIGHT).
            addField("FILES_NEEDED", Justification.RIGHT).
            addField("FILES_CACHED", Justification.RIGHT);
      }
      TableListing listing = builder.build();
      int numResults = 0;
      try {
        RemoteIterator<CachePoolEntry> iter = dfs.listCachePools();
        while (iter.hasNext()) {
          CachePoolEntry entry = iter.next();
          CachePoolInfo info = entry.getInfo();
          LinkedList<String> row = new LinkedList<String>();
          if (name == null || info.getPoolName().equals(name)) {
            row.add(info.getPoolName());
            row.add(info.getOwnerName());
            row.add(info.getGroupName());
            row.add(info.getMode() != null ? info.getMode().toString() : null);
            Long limit = info.getLimit();
            String limitString;
            if (limit != null && limit.equals(CachePoolInfo.LIMIT_UNLIMITED)) {
              limitString = "unlimited";
            } else {
              limitString = "" + limit;
            }
            row.add(limitString);
            Long maxTtl = info.getMaxRelativeExpiryMs();
            String maxTtlString = null;

            if (maxTtl != null) {
              if (maxTtl == CachePoolInfo.RELATIVE_EXPIRY_NEVER) {
                maxTtlString  = "never";
              } else {
                maxTtlString = DFSUtil.durationToString(maxTtl);
              }
            }
            row.add(maxTtlString);
            row.add("" + info.getDefaultReplication());

            if (printStats) {
              CachePoolStats stats = entry.getStats();
              row.add(Long.toString(stats.getBytesNeeded()));
              row.add(Long.toString(stats.getBytesCached()));
              row.add(Long.toString(stats.getBytesOverlimit()));
              row.add(Long.toString(stats.getFilesNeeded()));
              row.add(Long.toString(stats.getFilesCached()));
            }
            listing.addRow(row.toArray(new String[row.size()]));
            ++numResults;
            if (name != null) {
              break;
            }
          }
        }
      } catch (IOException e) {
        System.err.println(AdminHelper.prettifyException(e));
        return 2;
      }
      System.out.print(String.format("Found %d result%s.%n", numResults,
          (numResults == 1 ? "" : "s")));
      if (numResults > 0) { 
        System.out.print(listing);
      }
      // If list pools succeed, we return 0 (success exit code)
      return 0;
    }
  }

  private static final AdminHelper.Command[] COMMANDS = {
    new AddCacheDirectiveInfoCommand(),
    new ModifyCacheDirectiveInfoCommand(),
    new ListCacheDirectiveInfoCommand(),
    new RemoveCacheDirectiveInfoCommand(),
    new RemoveCacheDirectiveInfosCommand(),
    new AddCachePoolCommand(),
    new ModifyCachePoolCommand(),
    new RemoveCachePoolCommand(),
    new ListCachePoolsCommand()
  };
}
