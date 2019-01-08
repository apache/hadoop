/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.ozone.freon;

import java.io.IOException;
import java.io.PrintStream;
import java.net.InetSocketAddress;
import java.security.PrivilegedExceptionAction;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdds.HddsUtils;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.OmUtils;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * CLI utility to print out ozone related configuration.
 */
public class OzoneGetConf extends Configured implements Tool {

  private static final String DESCRIPTION = "ozone getconf is utility for "
      + "getting configuration information from the config file.\n";

  enum Command {
    INCLUDE_FILE("-includeFile",
        "gets the include file path that defines the datanodes " +
            "that can join the cluster."),
    EXCLUDE_FILE("-excludeFile",
        "gets the exclude file path that defines the datanodes " +
            "that need to decommissioned."),
    OZONEMANAGER("-ozonemanagers",
        "gets list of Ozone Manager nodes in the cluster"),
    STORAGECONTAINERMANAGER("-storagecontainermanagers",
        "gets list of ozone storage container manager nodes in the cluster"),
    CONFKEY("-confKey [key]", "gets a specific key from the configuration");

    private static final Map<String, OzoneGetConf.CommandHandler> HANDLERS;

    static {
      HANDLERS = new HashMap<String, OzoneGetConf.CommandHandler>();
      HANDLERS.put(StringUtils.toLowerCase(OZONEMANAGER.getName()),
          new OzoneManagersCommandHandler());
      HANDLERS.put(StringUtils.toLowerCase(STORAGECONTAINERMANAGER.getName()),
          new StorageContainerManagersCommandHandler());
      HANDLERS.put(StringUtils.toLowerCase(CONFKEY.getName()),
          new PrintConfKeyCommandHandler());
    }

    private final String cmd;
    private final String description;

    Command(String cmd, String description) {
      this.cmd = cmd;
      this.description = description;
    }

    public String getName() {
      return cmd.split(" ")[0];
    }

    public String getUsage() {
      return cmd;
    }

    public String getDescription() {
      return description;
    }

    public static OzoneGetConf.CommandHandler getHandler(String cmd) {
      return HANDLERS.get(StringUtils.toLowerCase(cmd));
    }
  }

  static final String USAGE;
  static {
    HdfsConfiguration.init();

    /* Initialize USAGE based on Command values */
    StringBuilder usage = new StringBuilder(DESCRIPTION);
    usage.append("\nozone getconf \n");
    for (OzoneGetConf.Command cmd : OzoneGetConf.Command.values()) {
      usage.append("\t[" + cmd.getUsage() + "]\t\t\t" + cmd.getDescription()
          + "\n");
    }
    USAGE = usage.toString();
  }

  /**
   * Handler to return value for key corresponding to the
   * {@link OzoneGetConf.Command}.
   */
  static class CommandHandler {

    @SuppressWarnings("visibilitymodifier")
    protected String key; // Configuration key to lookup

    CommandHandler() {
      this(null);
    }

    CommandHandler(String key) {
      this.key = key;
    }

    final int doWork(OzoneGetConf tool, String[] args) {
      try {
        checkArgs(args);

        return doWorkInternal(tool, args);
      } catch (Exception e) {
        tool.printError(e.getMessage());
      }
      return -1;
    }

    protected void checkArgs(String[] args) {
      if (args.length > 0) {
        throw new HadoopIllegalArgumentException(
            "Did not expect argument: " + args[0]);
      }
    }


    /** Method to be overridden by sub classes for specific behavior. */
    int doWorkInternal(OzoneGetConf tool, String[] args) throws Exception {

      String value = tool.getConf().getTrimmed(key);
      if (value != null) {
        tool.printOut(value);
        return 0;
      }
      tool.printError("Configuration " + key + " is missing.");
      return -1;
    }
  }

  static class PrintConfKeyCommandHandler extends OzoneGetConf.CommandHandler {
    @Override
    protected void checkArgs(String[] args) {
      if (args.length != 1) {
        throw new HadoopIllegalArgumentException(
            "usage: " + OzoneGetConf.Command.CONFKEY.getUsage());
      }
    }

    @Override
    int doWorkInternal(OzoneGetConf tool, String[] args) throws Exception {
      this.key = args[0];
      return super.doWorkInternal(tool, args);
    }
  }

  private final PrintStream out; // Stream for printing command output
  private final PrintStream err; // Stream for printing error

  protected OzoneGetConf(Configuration conf) {
    this(conf, System.out, System.err);
  }

  protected OzoneGetConf(Configuration conf, PrintStream out, PrintStream err) {
    super(conf);
    this.out = out;
    this.err = err;
  }

  void printError(String message) {
    err.println(message);
  }

  void printOut(String message) {
    out.println(message);
  }

  private void printUsage() {
    printError(USAGE);
  }

  /**
   * Main method that runs the tool for given arguments.
   * @param args arguments
   * @return return status of the command
   */
  private int doWork(String[] args) {
    if (args.length >= 1) {
      OzoneGetConf.CommandHandler handler =
          OzoneGetConf.Command.getHandler(args[0]);
      if (handler != null) {
        return handler.doWork(this, Arrays.copyOfRange(args, 1, args.length));
      }
    }
    printUsage();
    return -1;
  }

  @Override
  public int run(final String[] args) throws Exception {
    return SecurityUtil.doAsCurrentUser(
          new PrivilegedExceptionAction<Integer>() {
            @Override
            public Integer run() throws Exception {
              return doWork(args);
            }
          });
  }

  /**
   * Handler for {@link Command#STORAGECONTAINERMANAGER}.
   */
  static class StorageContainerManagersCommandHandler extends CommandHandler {

    @Override
    public int doWorkInternal(OzoneGetConf tool, String[] args)
        throws IOException {
      Collection<InetSocketAddress> addresses = HddsUtils
          .getSCMAddresses(tool.getConf());

      for (InetSocketAddress addr : addresses) {
        tool.printOut(addr.getHostName());
      }
      return 0;
    }
  }

  /**
   * Handler for {@link Command#OZONEMANAGER}.
   */
  static class OzoneManagersCommandHandler extends CommandHandler {
    @Override
    public int doWorkInternal(OzoneGetConf tool, String[] args)
        throws IOException {
      tool.printOut(OmUtils.getOmAddress(tool.getConf()).getHostName());
      return 0;
    }
  }

  public static void main(String[] args) throws Exception {
    if (DFSUtil.parseHelpArgument(args, USAGE, System.out, true)) {
      System.exit(0);
    }

    Configuration conf = new Configuration();
    conf.addResource(new OzoneConfiguration());
    int res = ToolRunner.run(new OzoneGetConf(conf), args);
    System.exit(res);
  }
}
