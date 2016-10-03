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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileEncryptionInfo;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.hdfs.client.CreateEncryptionZoneFlag;
import org.apache.hadoop.hdfs.client.HdfsAdmin;
import org.apache.hadoop.hdfs.protocol.EncryptionZone;
import org.apache.hadoop.tools.TableListing;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;

/**
 * This class implements crypto command-line operations.
 */
@InterfaceAudience.Private
public class CryptoAdmin extends Configured implements Tool {

  public CryptoAdmin() {
    this(null);
  }

  public CryptoAdmin(Configuration conf) {
    super(conf);
  }

  @Override
  public int run(String[] args) throws IOException {
    if (args.length == 0) {
      AdminHelper.printUsage(false, "crypto", COMMANDS);
      return 1;
    }
    final AdminHelper.Command command = AdminHelper.determineCommand(args[0],
        COMMANDS);
    if (command == null) {
      System.err.println("Can't understand command '" + args[0] + "'");
      if (!args[0].startsWith("-")) {
        System.err.println("Command names must start with dashes.");
      }
      AdminHelper.printUsage(false, "crypto", COMMANDS);
      return 1;
    }
    final List<String> argsList = new LinkedList<String>();
    for (int j = 1; j < args.length; j++) {
      argsList.add(args[j]);
    }
    try {
      return command.run(getConf(), argsList);
    } catch (IllegalArgumentException e) {
      System.err.println(prettifyException(e));
      return -1;
    }
  }

  public static void main(String[] argsArray) throws IOException {
    final CryptoAdmin cryptoAdmin = new CryptoAdmin(new Configuration());
    System.exit(cryptoAdmin.run(argsArray));
  }

  /**
   * NN exceptions contain the stack trace as part of the exception message.
   * When it's a known error, pretty-print the error and squish the stack trace.
   */
  private static String prettifyException(Exception e) {
    return e.getClass().getSimpleName() + ": " +
      e.getLocalizedMessage().split("\n")[0];
  }

  private static class CreateZoneCommand implements AdminHelper.Command {
    @Override
    public String getName() {
      return "-createZone";
    }

    @Override
    public String getShortUsage() {
      return "[" + getName() + " -keyName <keyName> -path <path>]\n";
    }

    @Override
    public String getLongUsage() {
      final TableListing listing = AdminHelper.getOptionDescriptionListing();
      listing.addRow("<path>", "The path of the encryption zone to create. " +
          "It must be an empty directory. A trash directory is provisioned " +
          "under this path.");
      listing.addRow("<keyName>", "Name of the key to use for the " +
          "encryption zone.");
      return getShortUsage() + "\n" +
          "Create a new encryption zone.\n\n" +
          listing.toString();
    }

    @Override
    public int run(Configuration conf, List<String> args) throws IOException {
      final String path = StringUtils.popOptionWithArgument("-path", args);
      if (path == null) {
        System.err.println("You must specify a path with -path.");
        return 1;
      }

      final String keyName =
          StringUtils.popOptionWithArgument("-keyName", args);
      if (keyName == null) {
        System.err.println("You must specify a key name with -keyName.");
        return 1;
      }

      if (!args.isEmpty()) {
        System.err.println("Can't understand argument: " + args.get(0));
        return 1;
      }

      HdfsAdmin admin = new HdfsAdmin(FileSystem.getDefaultUri(conf), conf);
      EnumSet<CreateEncryptionZoneFlag> flags =
          EnumSet.of(CreateEncryptionZoneFlag.PROVISION_TRASH);
      try {
        admin.createEncryptionZone(new Path(path), keyName, flags);
        System.out.println("Added encryption zone " + path);
      } catch (IOException e) {
        System.err.println(prettifyException(e));
        return 2;
      }
      return 0;
    }
  }

  private static class ListZonesCommand implements AdminHelper.Command {
    @Override
    public String getName() {
      return "-listZones";
    }

    @Override
    public String getShortUsage() {
      return "[" + getName()+ "]\n";
    }

    @Override
    public String getLongUsage() {
      return getShortUsage() + "\n" +
        "List all encryption zones. Requires superuser permissions.\n\n";
    }

    @Override
    public int run(Configuration conf, List<String> args) throws IOException {
      if (!args.isEmpty()) {
        System.err.println("Can't understand argument: " + args.get(0));
        return 1;
      }

      HdfsAdmin admin = new HdfsAdmin(FileSystem.getDefaultUri(conf), conf);
      try {
        final TableListing listing = new TableListing.Builder()
          .addField("").addField("", true)
          .wrapWidth(AdminHelper.MAX_LINE_WIDTH).hideHeaders().build();
        final RemoteIterator<EncryptionZone> it = admin.listEncryptionZones();
        while (it.hasNext()) {
          EncryptionZone ez = it.next();
          listing.addRow(ez.getPath(), ez.getKeyName());
        }
        System.out.println(listing.toString());
      } catch (IOException e) {
        System.err.println(prettifyException(e));
        return 2;
      }

      return 0;
    }
  }

  private static class GetFileEncryptionInfoCommand
      implements AdminHelper.Command {
    @Override
    public String getName() {
      return "-getFileEncryptionInfo";
    }

    @Override
    public String getShortUsage() {
      return "[" + getName() + " -path <path>]\n";
    }

    @Override
    public String getLongUsage() {
      final TableListing listing = AdminHelper.getOptionDescriptionListing();
      listing.addRow("<path>", "The path to the file to show encryption info.");
      return getShortUsage() + "\n" + "Get encryption info of a file.\n\n" +
          listing.toString();
    }

    @Override
    public int run(Configuration conf, List<String> args) throws IOException {
      final String path = StringUtils.popOptionWithArgument("-path", args);

      if (!args.isEmpty()) {
        System.err.println("Can't understand argument: " + args.get(0));
        return 1;
      }

      final HdfsAdmin admin =
          new HdfsAdmin(FileSystem.getDefaultUri(conf), conf);
      try {
        final FileEncryptionInfo fei =
            admin.getFileEncryptionInfo(new Path(path));
        if (fei == null) {
          System.out.println("No FileEncryptionInfo found for path " + path);
          return 2;
        }
        System.out.println(fei.toStringStable());
      } catch (IOException e) {
        System.err.println(prettifyException(e));
        return 3;
      }
      return 0;
    }
  }

  private static class ProvisionTrashCommand implements AdminHelper.Command {
    @Override
    public String getName() {
      return "-provisionTrash";
    }

    @Override
    public String getShortUsage() {
      return "[" + getName() + " -path <path>]\n";
    }

    @Override
    public String getLongUsage() {
      final TableListing listing = AdminHelper.getOptionDescriptionListing();
      listing.addRow("<path>", "The path to the root of the encryption zone. ");
      return getShortUsage() + "\n" +
          "Provision a trash directory for an encryption zone.\n\n" +
          listing.toString();
    }

    @Override
    public int run(Configuration conf, List<String> args) throws IOException {
      final String path = StringUtils.popOptionWithArgument("-path", args);

      if (!args.isEmpty()) {
        System.err.println("Can't understand argument: " + args.get(0));
        return 1;
      }

      HdfsAdmin admin = new HdfsAdmin(FileSystem.getDefaultUri(conf), conf);
      try {
        admin.provisionEncryptionZoneTrash(new Path(path));
        System.out.println("Created a trash directory for " + path);
      } catch (IOException ioe) {
        System.err.println(prettifyException(ioe));
        return 2;
      }
      return 0;
    }
  }

  private static final AdminHelper.Command[] COMMANDS = {
      new CreateZoneCommand(),
      new ListZonesCommand(),
      new ProvisionTrashCommand(),
      new GetFileEncryptionInfoCommand()
  };
}
