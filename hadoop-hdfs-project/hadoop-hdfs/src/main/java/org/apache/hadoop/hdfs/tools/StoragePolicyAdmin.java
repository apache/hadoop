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
import org.apache.hadoop.fs.BlockStoragePolicySpi;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.protocol.BlockStoragePolicy;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.tools.TableListing;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.FileNotFoundException;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

/**
 * This class implements block storage policy operations.
 */
public class StoragePolicyAdmin extends Configured implements Tool {

  public static void main(String[] argsArray) throws Exception {
    final StoragePolicyAdmin admin = new StoragePolicyAdmin(new
        Configuration());
    int res = ToolRunner.run(admin, argsArray);
    System.exit(res);
  }

  public StoragePolicyAdmin(Configuration conf) {
    super(conf);
  }

  @Override
  public int run(String[] args) throws Exception {
    if (args.length == 0) {
      AdminHelper.printUsage(false, "storagepolicies", COMMANDS);
      ToolRunner.printGenericCommandUsage(System.err);
      return 1;
    }
    final AdminHelper.Command command = AdminHelper.determineCommand(args[0],
        COMMANDS);
    if (command == null) {
      System.err.println("Can't understand command '" + args[0] + "'");
      if (!args[0].startsWith("-")) {
        System.err.println("Command names must start with dashes.");
      }
      AdminHelper.printUsage(false, "storagepolicies", COMMANDS);
      ToolRunner.printGenericCommandUsage(System.err);
      return 1;
    }
    final List<String> argsList = new LinkedList<>();
    argsList.addAll(Arrays.asList(args).subList(1, args.length));
    try {
      return command.run(getConf(), argsList);
    } catch (IllegalArgumentException e) {
      System.err.println(AdminHelper.prettifyException(e));
      return -1;
    }
  }

  /** Command to list all the existing storage policies */
  private static class ListStoragePoliciesCommand
      implements AdminHelper.Command {
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
      final FileSystem fs = FileSystem.get(conf);
      try {
        Collection<? extends BlockStoragePolicySpi> policies =
            fs.getAllStoragePolicies();
        System.out.println("Block Storage Policies:");
        for (BlockStoragePolicySpi policy : policies) {
          if (policy != null) {
            System.out.println("\t" + policy);
          }
        }
      } catch (IOException e) {
        System.err.println(AdminHelper.prettifyException(e));
        return 2;
      }
      return 0;
    }
  }

  /** Command to get the storage policy of a file/directory */
  private static class GetStoragePolicyCommand implements AdminHelper.Command {
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
      final TableListing listing = AdminHelper.getOptionDescriptionListing();
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
        System.err.println("Please specify the path with -path.\nUsage: " +
            getLongUsage());
        return 1;
      }

      Path p = new Path(path);
      final FileSystem fs = FileSystem.get(p.toUri(), conf);
      try {
        FileStatus status;
        try {
          status = fs.getFileStatus(p);
        } catch (FileNotFoundException e) {
          System.err.println("File/Directory does not exist: " + path);
          return 2;
        }

        if (status instanceof HdfsFileStatus) {
          byte storagePolicyId = ((HdfsFileStatus)status).getStoragePolicy();
          if (storagePolicyId ==
              HdfsConstants.BLOCK_STORAGE_POLICY_ID_UNSPECIFIED) {
            System.out.println("The storage policy of " + path
                + " is unspecified");
            return 0;
          }
          Collection<? extends BlockStoragePolicySpi> policies =
              fs.getAllStoragePolicies();
          for (BlockStoragePolicySpi policy : policies) {
            if (policy instanceof BlockStoragePolicy) {
              if (((BlockStoragePolicy)policy).getId() == storagePolicyId) {
                System.out.println("The storage policy of " + path
                    + ":\n" + policy);
                return 0;
              }
            }
          }
        }
        System.err.println(getName() + " is not supported for filesystem "
            + fs.getScheme() + " on path " + path);
        return 2;
      } catch (Exception e) {
        System.err.println(AdminHelper.prettifyException(e));
        return 2;
      }
    }
  }

  /** Command to set the storage policy to a file/directory */
  private static class SetStoragePolicyCommand implements AdminHelper.Command {
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
      TableListing listing = AdminHelper.getOptionDescriptionListing();
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
      Path p = new Path(path);
      final FileSystem fs = FileSystem.get(p.toUri(), conf);
      try {
        fs.setStoragePolicy(p, policyName);
        System.out.println("Set storage policy " + policyName + " on " + path);
      } catch (Exception e) {
        System.err.println(AdminHelper.prettifyException(e));
        return 2;
      }
      return 0;
    }
  }

  /** Command to schedule blocks to move based on specified policy. */
  private static class SatisfyStoragePolicyCommand
      implements AdminHelper.Command {
    @Override
    public String getName() {
      return "-satisfyStoragePolicy";
    }

    @Override
    public String getShortUsage() {
      return "[" + getName() + " -path <path>]\n";
    }

    @Override
    public String getLongUsage() {
      TableListing listing = AdminHelper.getOptionDescriptionListing();
      listing.addRow("<path>", "The path of the file/directory to satisfy"
          + " storage policy");
      return getShortUsage() + "\n" +
          "Schedule blocks to move based on file/directory policy.\n\n" +
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
      Path p = new Path(path);
      final FileSystem fs = FileSystem.get(p.toUri(), conf);
      try {
        fs.satisfyStoragePolicy(p);
        System.out.println("Scheduled blocks to move based on the current"
            + " storage policy on " + path);
      } catch (Exception e) {
        System.err.println(AdminHelper.prettifyException(e));
        return 2;
      }
      return 0;
    }
  }

  /* Command to unset the storage policy set for a file/directory */
  private static class UnsetStoragePolicyCommand
      implements AdminHelper.Command {

    @Override
    public String getName() {
      return "-unsetStoragePolicy";
    }

    @Override
    public String getShortUsage() {
      return "[" + getName() + " -path <path>]\n";
    }

    @Override
    public String getLongUsage() {
      TableListing listing = AdminHelper.getOptionDescriptionListing();
      listing.addRow("<path>", "The path of the file/directory "
          + "from which the storage policy will be unset.");
      return getShortUsage() + "\n"
          + "Unset the storage policy set for a file/directory.\n\n"
          + listing.toString();
    }

    @Override
    public int run(Configuration conf, List<String> args) throws IOException {
      final String path = StringUtils.popOptionWithArgument("-path", args);
      if (path == null) {
        System.err.println("Please specify the path from which "
            + "the storage policy will be unset.\nUsage: " + getLongUsage());
        return 1;
      }

      Path p = new Path(path);
      final FileSystem fs = FileSystem.get(p.toUri(), conf);
      try {
        fs.unsetStoragePolicy(p);
        System.out.println("Unset storage policy from " + path);
      } catch (Exception e) {
        System.err.println(AdminHelper.prettifyException(e));
        return 2;
      }
      return 0;
    }
  }

  private static final AdminHelper.Command[] COMMANDS = {
      new ListStoragePoliciesCommand(),
      new SetStoragePolicyCommand(),
      new GetStoragePolicyCommand(),
      new UnsetStoragePolicyCommand(),
      new SatisfyStoragePolicyCommand()
  };
}