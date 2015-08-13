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
package org.apache.hadoop.hdfs.tools.erasurecode;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.shell.Command;
import org.apache.hadoop.fs.shell.CommandFactory;
import org.apache.hadoop.fs.shell.PathData;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.ErasureCodingZone;
import org.apache.hadoop.hdfs.server.namenode.UnsupportedActionException;
import org.apache.hadoop.hdfs.protocol.ErasureCodingPolicy;
import org.apache.hadoop.util.StringUtils;

/**
 * Erasure Coding CLI commands
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public abstract class ECCommand extends Command {

  public static void registerCommands(CommandFactory factory) {
    // Register all commands of Erasure CLI, with a '-' at the beginning in name
    // of the command.
    factory.addClass(CreateECZoneCommand.class, "-" + CreateECZoneCommand.NAME);
    factory.addClass(GetECZoneCommand.class, "-"
        + GetECZoneCommand.NAME);
    factory.addClass(ListPolicies.class, "-" + ListPolicies.NAME);
  }

  @Override
  public String getCommandName() {
    return getName();
  }

  @Override
  protected void run(Path path) throws IOException {
    throw new RuntimeException("Not suppose to get here");
  }

  @Deprecated
  @Override
  public int runAll() {
    return run(args);
  }

  @Override
  protected void processPath(PathData item) throws IOException {
    if (!(item.fs instanceof DistributedFileSystem)) {
      throw new UnsupportedActionException(
          "Erasure commands are only supported for the HDFS paths");
    }
  }

  /**
   * A command to create an EC zone for a path, with a erasure coding policy name.
   */
  static class CreateECZoneCommand extends ECCommand {
    public static final String NAME = "createZone";
    public static final String USAGE = "[-s <policyName>] <path>";
    public static final String DESCRIPTION = 
        "Create a zone to encode files using a specified policy\n"
        + "Options :\n"
        + "  -s <policyName> : erasure coding policy name to encode files. "
        + "If not passed the default policy will be used\n"
        + "  <path>  : Path to an empty directory. Under this directory "
        + "files will be encoded using specified erasure coding policy";
    private String ecPolicyName;
    private ErasureCodingPolicy ecPolicy = null;

    @Override
    protected void processOptions(LinkedList<String> args) throws IOException {
      ecPolicyName = StringUtils.popOptionWithArgument("-s", args);
      if (args.isEmpty()) {
        throw new HadoopIllegalArgumentException("<path> is missing");
      }
      if (args.size() > 1) {
        throw new HadoopIllegalArgumentException("Too many arguments");
      }
    }

    @Override
    protected void processPath(PathData item) throws IOException {
      super.processPath(item);
      DistributedFileSystem dfs = (DistributedFileSystem) item.fs;
      try {
        if (ecPolicyName != null) {
          ErasureCodingPolicy[] ecPolicies = dfs.getClient().getErasureCodingPolicies();
          for (ErasureCodingPolicy ecPolicy : ecPolicies) {
            if (ecPolicyName.equals(ecPolicy.getName())) {
              this.ecPolicy = ecPolicy;
              break;
            }
          }
          if (ecPolicy == null) {
            StringBuilder sb = new StringBuilder();
            sb.append("Policy '");
            sb.append(ecPolicyName);
            sb.append("' does not match any of the supported policies.");
            sb.append(" Please select any one of ");
            List<String> ecPolicyNames = new ArrayList<String>();
            for (ErasureCodingPolicy ecPolicy : ecPolicies) {
              ecPolicyNames.add(ecPolicy.getName());
            }
            sb.append(ecPolicyNames);
            throw new HadoopIllegalArgumentException(sb.toString());
          }
        }
        dfs.createErasureCodingZone(item.path, ecPolicy);
        out.println("EC Zone created successfully at " + item.path);
      } catch (IOException e) {
        throw new IOException("Unable to create EC zone for the path "
            + item.path + ". " + e.getMessage());
      }
    }
  }

  /**
   * Get the information about the zone
   */
  static class GetECZoneCommand extends ECCommand {
    public static final String NAME = "getZone";
    public static final String USAGE = "<path>";
    public static final String DESCRIPTION =
        "Get information about the EC zone at specified path\n";

    @Override
    protected void processOptions(LinkedList<String> args) throws IOException {
      if (args.isEmpty()) {
        throw new HadoopIllegalArgumentException("<path> is missing");
      }
      if (args.size() > 1) {
        throw new HadoopIllegalArgumentException("Too many arguments");
      }
    }

    @Override
    protected void processPath(PathData item) throws IOException {
      super.processPath(item);
      DistributedFileSystem dfs = (DistributedFileSystem) item.fs;
      try {
        ErasureCodingZone ecZone = dfs.getErasureCodingZone(item.path);
        if (ecZone != null) {
          out.println(ecZone.toString());
        } else {
          out.println("Path " + item.path + " is not in EC zone");
        }
      } catch (IOException e) {
        throw new IOException("Unable to get EC zone for the path "
            + item.path + ". " + e.getMessage());
      }
    }
  }

  /**
   * List all supported erasure coding policies
   */
  static class ListPolicies extends ECCommand {
    public static final String NAME = "listPolicies";
    public static final String USAGE = "";
    public static final String DESCRIPTION = 
        "Get the list of erasure coding policies supported\n";

    @Override
    protected void processOptions(LinkedList<String> args) throws IOException {
      if (!args.isEmpty()) {
        throw new HadoopIllegalArgumentException("Too many parameters");
      }

      FileSystem fs = FileSystem.get(getConf());
      if (fs instanceof DistributedFileSystem == false) {
        throw new UnsupportedActionException(
            "Erasure commands are only supported for the HDFS");
      }
      DistributedFileSystem dfs = (DistributedFileSystem) fs;

      ErasureCodingPolicy[] ecPolicies = dfs.getClient().getErasureCodingPolicies();
      StringBuilder sb = new StringBuilder();
      int i = 0;
      while (i < ecPolicies.length) {
        ErasureCodingPolicy ecPolicy = ecPolicies[i];
        sb.append(ecPolicy.getName());
        i++;
        if (i < ecPolicies.length) {
          sb.append(", ");
        }
      }
      out.println(sb.toString());
    }
  }
}
