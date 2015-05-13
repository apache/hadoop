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
import org.apache.hadoop.hdfs.protocol.ErasureCodingZoneInfo;
import org.apache.hadoop.hdfs.server.namenode.UnsupportedActionException;
import org.apache.hadoop.io.erasurecode.ECSchema;
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
    factory.addClass(GetECZoneInfoCommand.class, "-"
        + GetECZoneInfoCommand.NAME);
    factory.addClass(ListECSchemas.class, "-" + ListECSchemas.NAME);
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
   * Create EC encoding zone command. Zones are created to use specific EC
   * encoding schema, other than default while encoding the files under some
   * specific directory.
   */
  static class CreateECZoneCommand extends ECCommand {
    public static final String NAME = "createZone";
    public static final String USAGE = "[-s <schemaName>] <path>";
    public static final String DESCRIPTION = 
        "Create a zone to encode files using a specified schema\n"
        + "Options :\n"
        + "  -s <schemaName> : EC schema name to encode files. "
        + "If not passed default schema will be used\n"
        + "  <path>  : Path to an empty directory. Under this directory "
        + "files will be encoded using specified schema";
    private String schemaName;
    private ECSchema schema = null;

    @Override
    protected void processOptions(LinkedList<String> args) throws IOException {
      schemaName = StringUtils.popOptionWithArgument("-s", args);
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
        if (schemaName != null) {
          ECSchema[] ecSchemas = dfs.getClient().getECSchemas();
          for (ECSchema ecSchema : ecSchemas) {
            if (schemaName.equals(ecSchema.getSchemaName())) {
              schema = ecSchema;
              break;
            }
          }
          if (schema == null) {
            StringBuilder sb = new StringBuilder();
            sb.append("Schema '");
            sb.append(schemaName);
            sb.append("' does not match any of the supported schemas.");
            sb.append(" Please select any one of ");
            List<String> schemaNames = new ArrayList<String>();
            for (ECSchema ecSchema : ecSchemas) {
              schemaNames.add(ecSchema.getSchemaName());
            }
            sb.append(schemaNames);
            throw new HadoopIllegalArgumentException(sb.toString());
          }
        }
        dfs.createErasureCodingZone(item.path, schema);
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
  static class GetECZoneInfoCommand extends ECCommand {
    public static final String NAME = "getZoneInfo";
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
        ErasureCodingZoneInfo ecZoneInfo = dfs.getErasureCodingZoneInfo(item.path);
        if (ecZoneInfo != null) {
          out.println(ecZoneInfo.toString());
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
   * List all supported EC Schemas
   */
  static class ListECSchemas extends ECCommand {
    public static final String NAME = "listSchemas";
    public static final String USAGE = "";
    public static final String DESCRIPTION = 
        "Get the list of ECSchemas supported\n";

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

      ECSchema[] ecSchemas = dfs.getClient().getECSchemas();
      StringBuilder sb = new StringBuilder();
      int i = 0;
      while (i < ecSchemas.length) {
        ECSchema ecSchema = ecSchemas[i];
        sb.append(ecSchema.getSchemaName());
        i++;
        if (i < ecSchemas.length) {
          sb.append(", ");
        }
      }
      out.println(sb.toString());
    }
  }
}