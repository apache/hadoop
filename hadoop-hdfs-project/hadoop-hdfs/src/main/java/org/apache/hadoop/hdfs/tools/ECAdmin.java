/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.hdfs.tools;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.AddErasureCodingPolicyResponse;
import org.apache.hadoop.hdfs.protocol.ErasureCodingPolicy;
import org.apache.hadoop.hdfs.protocol.ErasureCodingPolicyInfo;
import org.apache.hadoop.hdfs.protocol.NoECPolicySetException;
import org.apache.hadoop.hdfs.util.ECPolicyLoader;
import org.apache.hadoop.io.erasurecode.ErasureCodeConstants;
import org.apache.hadoop.tools.TableListing;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * CLI for the erasure code encoding operations.
 */
@InterfaceAudience.Private
public class ECAdmin extends Configured implements Tool {

  public static final String NAME = "ec";

  public static void main(String[] args) throws Exception {
    final ECAdmin admin = new ECAdmin(new Configuration());
    int res = ToolRunner.run(admin, args);
    System.exit(res);
  }

  public ECAdmin(Configuration conf) {
    super(conf);
  }

  @Override
  public int run(String[] args) throws Exception {
    if (args.length == 0) {
      AdminHelper.printUsage(false, NAME, COMMANDS);
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
      AdminHelper.printUsage(false, NAME, COMMANDS);
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

  /** Command to list the set of enabled erasure coding policies. */
  private static class ListECPoliciesCommand
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
          "Get the list of all erasure coding policies.\n";
    }

    @Override
    public int run(Configuration conf, List<String> args) throws IOException {
      if (args.size() > 0) {
        System.err.println(getName() + ": Too many arguments");
        return 1;
      }

      final DistributedFileSystem dfs = AdminHelper.getDFS(conf);
      try {
        final Collection<ErasureCodingPolicyInfo> policies =
            dfs.getAllErasureCodingPolicies();
        if (policies.isEmpty()) {
          System.out.println("There is no erasure coding policies in the " +
              "cluster.");
        } else {
          System.out.println("Erasure Coding Policies:");
          for (ErasureCodingPolicyInfo policy : policies) {
            if (policy != null) {
              System.out.println(policy);
            }
          }
        }
      } catch (IOException e) {
        System.err.println(AdminHelper.prettifyException(e));
        return 2;
      }
      return 0;
    }
  }

  /** Command to add a set of erasure coding policies. */
  private static class AddECPoliciesCommand
      implements AdminHelper.Command {
    @Override
    public String getName() {
      return "-addPolicies";
    }

    @Override
    public String getShortUsage() {
      return "[" + getName() + " -policyFile <file>]\n";
    }

    @Override
    public String getLongUsage() {
      final TableListing listing = AdminHelper.getOptionDescriptionListing();
      listing.addRow("<file>",
          "The path of the xml file which defines the EC policies to add");
      return getShortUsage() + "\n" +
          "Add a list of user defined erasure coding policies.\n" +
          listing.toString();
    }

    @Override
    public int run(Configuration conf, List<String> args) throws IOException {
      final String filePath =
          StringUtils.popOptionWithArgument("-policyFile", args);
      if (filePath == null) {
        System.err.println("Please specify the path with -policyFile.\nUsage: "
            + getLongUsage());
        return 1;
      }

      if (args.size() > 0) {
        System.err.println(getName() + ": Too many arguments");
        return 1;
      }

      final DistributedFileSystem dfs = AdminHelper.getDFS(conf);
      try {
        List<ErasureCodingPolicy> policies =
            new ECPolicyLoader().loadPolicy(filePath);
        if (policies.size() > 0) {
          AddErasureCodingPolicyResponse[] responses =
              dfs.addErasureCodingPolicies(
            policies.toArray(new ErasureCodingPolicy[policies.size()]));
          for (AddErasureCodingPolicyResponse response : responses) {
            System.out.println(response);
          }
        } else {
          System.out.println("No EC policy parsed out from " + filePath);
        }

      } catch (IOException e) {
        System.err.println(AdminHelper.prettifyException(e));
        return 2;
      }
      return 0;
    }
  }

  /** Command to get the erasure coding policy for a file or directory. */
  private static class GetECPolicyCommand implements AdminHelper.Command {
    @Override
    public String getName() {
      return "-getPolicy";
    }

    @Override
    public String getShortUsage() {
      return "[" + getName() + " -path <path>]\n";
    }

    @Override
    public String getLongUsage() {
      final TableListing listing = AdminHelper.getOptionDescriptionListing();
      listing.addRow("<path>",
          "The path of the file/directory for getting the erasure coding " +
              "policy");
      return getShortUsage() + "\n" +
          "Get the erasure coding policy of a file/directory.\n\n" +
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

      if (args.size() > 0) {
        System.err.println(getName() + ": Too many arguments");
        return 1;
      }

      final Path p = new Path(path);
      final DistributedFileSystem dfs = AdminHelper.getDFS(p.toUri(), conf);
      try {
        ErasureCodingPolicy ecPolicy = dfs.getErasureCodingPolicy(p);
        if (ecPolicy != null) {
          System.out.println(ecPolicy.getName());
        } else {
          System.out.println("The erasure coding policy of " + path + " is " +
              "unspecified");
        }
      } catch (Exception e) {
        System.err.println(AdminHelper.prettifyException(e));
        return 2;
      }
      return 0;
    }
  }

  /** Command to remove an erasure coding policy. */
  private static class RemoveECPolicyCommand implements AdminHelper.Command {
    @Override
    public String getName() {
      return "-removePolicy";
    }

    @Override
    public String getShortUsage() {
      return "[" + getName() + " -policy <policy>]\n";
    }

    @Override
    public String getLongUsage() {
      TableListing listing = AdminHelper.getOptionDescriptionListing();
      listing.addRow("<policy>", "The name of the erasure coding policy");
      return getShortUsage() + "\n" +
          "Remove an user defined erasure coding policy.\n" +
          listing.toString();
    }

    @Override
    public int run(Configuration conf, List<String> args) throws IOException {
      final String ecPolicyName = StringUtils.popOptionWithArgument(
          "-policy", args);
      if (ecPolicyName == null) {
        System.err.println("Please specify the policy name.\nUsage: " +
            getLongUsage());
        return 1;
      }
      if (args.size() > 0) {
        System.err.println(getName() + ": Too many arguments");
        return 1;
      }
      final DistributedFileSystem dfs = AdminHelper.getDFS(conf);
      try {
        dfs.removeErasureCodingPolicy(ecPolicyName);
        System.out.println("Erasure coding policy " + ecPolicyName +
            "is removed");
      } catch (IOException e) {
        System.err.println(AdminHelper.prettifyException(e));
        return 2;
      }
      return 0;
    }
  }

  /** Command to set the erasure coding policy to a file/directory. */
  private static class SetECPolicyCommand implements AdminHelper.Command {
    @Override
    public String getName() {
      return "-setPolicy";
    }

    @Override
    public String getShortUsage() {
      return "[" + getName() +
          " -path <path> [-policy <policy>] [-replicate]]\n";
    }

    @Override
    public String getLongUsage() {
      TableListing listing = AdminHelper.getOptionDescriptionListing();
      listing.addRow("<path>", "The path of the file/directory to set " +
          "the erasure coding policy");
      listing.addRow("<policy>", "The name of the erasure coding policy");
      listing.addRow("-replicate",
          "force 3x replication scheme on the directory");
      return getShortUsage() + "\n" +
          "Set the erasure coding policy for a file/directory.\n\n" +
          listing.toString() + "\n" +
          "-replicate and -policy are optional arguments. They cannot been " +
          "used at the same time";
    }

    @Override
    public int run(Configuration conf, List<String> args) throws IOException {
      final String path = StringUtils.popOptionWithArgument("-path", args);
      if (path == null) {
        System.err.println("Please specify the path for setting the EC " +
            "policy.\nUsage: " + getLongUsage());
        return 1;
      }

      String ecPolicyName = StringUtils.popOptionWithArgument("-policy",
          args);
      final boolean replicate = StringUtils.popOption("-replicate", args);

      if (args.size() > 0) {
        System.err.println(getName() + ": Too many arguments");
        return 1;
      }

      if (replicate) {
        if (ecPolicyName != null) {
          System.err.println(getName() +
              ": -replicate and -policy cannot been used at the same time");
          return 2;
        }
        ecPolicyName = ErasureCodeConstants.REPLICATION_POLICY_NAME;
      }

      final Path p = new Path(path);
      final DistributedFileSystem dfs = AdminHelper.getDFS(p.toUri(), conf);
      try {
        dfs.setErasureCodingPolicy(p, ecPolicyName);

        String actualECPolicyName = dfs.getErasureCodingPolicy(p).getName();

        System.out.println("Set " + actualECPolicyName +
            " erasure coding policy on "+ path);
        RemoteIterator<FileStatus> dirIt = dfs.listStatusIterator(p);
        if (dirIt.hasNext()) {
          System.out.println("Warning: setting erasure coding policy on a " +
              "non-empty directory will not automatically convert existing " +
              "files to " + actualECPolicyName + " erasure coding policy");
        }
      } catch (Exception e) {
        System.err.println(AdminHelper.prettifyException(e));
        return 3;
      }
      return 0;
    }
  }

  /** Command to unset the erasure coding policy set for a file/directory. */
  private static class UnsetECPolicyCommand
      implements AdminHelper.Command {

    @Override
    public String getName() {
      return "-unsetPolicy";
    }

    @Override
    public String getShortUsage() {
      return "[" + getName() + " -path <path>]\n";
    }

    @Override
    public String getLongUsage() {
      TableListing listing = AdminHelper.getOptionDescriptionListing();
      listing.addRow("<path>", "The path of the directory "
          + "from which the erasure coding policy will be unset.");
      return getShortUsage() + "\n"
          + "Unset the erasure coding policy for a directory.\n\n"
          + listing.toString();
    }

    @Override
    public int run(Configuration conf, List<String> args) throws IOException {
      final String path = StringUtils.popOptionWithArgument("-path", args);
      if (path == null) {
        System.err.println("Please specify a path.\nUsage: " + getLongUsage());
        return 1;
      }

      if (args.size() > 0) {
        System.err.println(getName() + ": Too many arguments");
        return 1;
      }

      final Path p = new Path(path);
      final DistributedFileSystem dfs = AdminHelper.getDFS(p.toUri(), conf);
      try {
        dfs.unsetErasureCodingPolicy(p);
        System.out.println("Unset erasure coding policy from " + path);
        RemoteIterator<FileStatus> dirIt = dfs.listStatusIterator(p);
        if (dirIt.hasNext()) {
          System.out.println("Warning: unsetting erasure coding policy on a " +
              "non-empty directory will not automatically convert existing" +
              " files to replicated data.");
        }
      } catch (NoECPolicySetException e) {
        System.err.println(AdminHelper.prettifyException(e));
        System.err.println("Use '-setPolicy -path <PATH> -replicate' to enforce"
            + " default replication policy irrespective of EC policy"
            + " defined on parent.");
        return 2;
      } catch (Exception e) {
        System.err.println(AdminHelper.prettifyException(e));
        return 2;
      }
      return 0;
    }
  }

  /** Command to list the set of supported erasure coding codecs and coders. */
  private static class ListECCodecsCommand
      implements AdminHelper.Command {
    @Override
    public String getName() {
      return "-listCodecs";
    }

    @Override
    public String getShortUsage() {
      return "[" + getName() + "]\n";
    }

    @Override
    public String getLongUsage() {
      return getShortUsage() + "\n" +
          "Get the list of supported erasure coding codecs and coders.\n" +
          "A coder is an implementation of a codec. A codec can have " +
          "different implementations, thus different coders.\n" +
          "The coders for a codec are listed in a fall back order.\n";
    }

    @Override
    public int run(Configuration conf, List<String> args) throws IOException {
      if (args.size() > 0) {
        System.err.println(getName() + ": Too many arguments");
        return 1;
      }

      final DistributedFileSystem dfs = AdminHelper.getDFS(conf);
      try {
        Map<String, String> codecs =
            dfs.getAllErasureCodingCodecs();
        if (codecs.isEmpty()) {
          System.out.println("No erasure coding codecs are supported on the " +
              "cluster.");
        } else {
          System.out.println("Erasure Coding Codecs: Codec [Coder List]");
          for (Map.Entry<String, String> codec : codecs.entrySet()) {
            if (codec != null) {
              System.out.println("\t" + codec.getKey().toUpperCase() + " ["
                  + codec.getValue().toUpperCase() +"]");
            }
          }
        }
      } catch (IOException e) {
        System.err.println(AdminHelper.prettifyException(e));
        return 2;
      }
      return 0;
    }
  }

  /** Command to enable an existing erasure coding policy. */
  private static class EnableECPolicyCommand implements AdminHelper.Command {
    @Override
    public String getName() {
      return "-enablePolicy";
    }

    @Override
    public String getShortUsage() {
      return "[" + getName() + " -policy <policy>]\n";
    }

    @Override
    public String getLongUsage() {
      TableListing listing = AdminHelper.getOptionDescriptionListing();
      listing.addRow("<policy>", "The name of the erasure coding policy");
      return getShortUsage() + "\n" +
          "Enable the erasure coding policy.\n\n" +
          listing.toString();
    }

    @Override
    public int run(Configuration conf, List<String> args) throws IOException {
      final String ecPolicyName = StringUtils.popOptionWithArgument("-policy",
          args);
      if (ecPolicyName == null) {
        System.err.println("Please specify the policy name.\nUsage: " +
            getLongUsage());
        return 1;
      }
      if (args.size() > 0) {
        System.err.println(getName() + ": Too many arguments");
        return 1;
      }

      final DistributedFileSystem dfs = AdminHelper.getDFS(conf);
      try {
        dfs.enableErasureCodingPolicy(ecPolicyName);
        System.out.println("Erasure coding policy " + ecPolicyName +
            " is enabled");
      } catch (IOException e) {
        System.err.println(AdminHelper.prettifyException(e));
        return 2;
      }
      return 0;
    }
  }

  /** Command to disable an existing erasure coding policy. */
  private static class DisableECPolicyCommand implements AdminHelper.Command {
    @Override
    public String getName() {
      return "-disablePolicy";
    }

    @Override
    public String getShortUsage() {
      return "[" + getName() + " -policy <policy>]\n";
    }

    @Override
    public String getLongUsage() {
      TableListing listing = AdminHelper.getOptionDescriptionListing();
      listing.addRow("<policy>", "The name of the erasure coding policy");
      return getShortUsage() + "\n" +
          "Disable the erasure coding policy.\n\n" +
          listing.toString();
    }

    @Override
    public int run(Configuration conf, List<String> args) throws IOException {
      final String ecPolicyName = StringUtils.popOptionWithArgument("-policy",
          args);
      if (ecPolicyName == null) {
        System.err.println("Please specify the policy name.\nUsage: " +
            getLongUsage());
        return 1;
      }
      if (args.size() > 0) {
        System.err.println(getName() + ": Too many arguments");
        return 1;
      }

      final DistributedFileSystem dfs = AdminHelper.getDFS(conf);
      try {
        dfs.disableErasureCodingPolicy(ecPolicyName);
        System.out.println("Erasure coding policy " + ecPolicyName +
            " is disabled");
      } catch (IOException e) {
        System.err.println(AdminHelper.prettifyException(e));
        return 2;
      }
      return 0;
    }
  }

  private static final AdminHelper.Command[] COMMANDS = {
      new ListECPoliciesCommand(),
      new AddECPoliciesCommand(),
      new GetECPolicyCommand(),
      new RemoveECPolicyCommand(),
      new SetECPolicyCommand(),
      new UnsetECPolicyCommand(),
      new ListECCodecsCommand(),
      new EnableECPolicyCommand(),
      new DisableECPolicyCommand()
  };
}
