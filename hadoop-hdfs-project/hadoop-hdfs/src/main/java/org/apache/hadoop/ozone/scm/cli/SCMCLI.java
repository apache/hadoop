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
package org.apache.hadoop.ozone.scm.cli;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.ipc.Client;
import org.apache.hadoop.ipc.ProtobufRpcEngine;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.client.OzoneClientUtils;
import org.apache.hadoop.ozone.scm.cli.container.ContainerCommandHandler;
import org.apache.hadoop.ozone.client.rest.OzoneException;
import org.apache.hadoop.scm.XceiverClientManager;
import org.apache.hadoop.scm.client.ContainerOperationClient;
import org.apache.hadoop.scm.client.ScmClient;
import org.apache.hadoop.scm.protocolPB.StorageContainerLocationProtocolClientSideTranslatorPB;
import org.apache.hadoop.scm.protocolPB.StorageContainerLocationProtocolPB;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.io.PrintStream;
import java.net.InetSocketAddress;
import java.net.URISyntaxException;
import java.util.Arrays;

import static org.apache.hadoop.ozone.scm.cli.ResultCode.EXECUTION_ERROR;
import static org.apache.hadoop.ozone.scm.cli.ResultCode.SUCCESS;
import static org.apache.hadoop.ozone.scm.cli.ResultCode.UNRECOGNIZED_CMD;
import static org.apache.hadoop.ozone.scm.cli.container.ContainerCommandHandler.CONTAINER_CMD;
import static org.apache.hadoop.scm.ScmConfigKeys.OZONE_SCM_CONTAINER_SIZE_DEFAULT;
import static org.apache.hadoop.scm.ScmConfigKeys.OZONE_SCM_CONTAINER_SIZE_GB;

/**
 * This class is the CLI of SCM.
 */
public class SCMCLI extends OzoneBaseCLI {

  public static final String HELP_OP = "help";
  public static final int CMD_WIDTH = 80;

  private final ScmClient scmClient;
  private final PrintStream out;
  private final PrintStream err;

  private final Options options;

  public SCMCLI(ScmClient scmClient) {
    this(scmClient, System.out, System.err);
  }

  public SCMCLI(ScmClient scmClient, PrintStream out, PrintStream err) {
    this.scmClient = scmClient;
    this.out = out;
    this.err = err;
    this.options = getOptions();
  }

  /**
   * Main for the scm shell Command handling.
   *
   * @param argv - System Args Strings[]
   * @throws Exception
   */
  public static void main(String[] argv) throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    ScmClient scmClient = getScmClient(conf);
    SCMCLI shell = new SCMCLI(scmClient);
    conf.setQuietMode(false);
    shell.setConf(conf);
    int res = 0;
    try {
      res = ToolRunner.run(shell, argv);
    } catch (Exception ex) {
      System.exit(1);
    }
    System.exit(res);
  }

  private static ScmClient getScmClient(OzoneConfiguration ozoneConf)
      throws IOException {
    long version = RPC.getProtocolVersion(
        StorageContainerLocationProtocolPB.class);
    InetSocketAddress scmAddress =
        OzoneClientUtils.getScmAddressForClients(ozoneConf);
    int containerSizeGB = ozoneConf.getInt(OZONE_SCM_CONTAINER_SIZE_GB,
        OZONE_SCM_CONTAINER_SIZE_DEFAULT);
    ContainerOperationClient.setContainerSizeB(containerSizeGB*OzoneConsts.GB);

    RPC.setProtocolEngine(ozoneConf, StorageContainerLocationProtocolPB.class,
        ProtobufRpcEngine.class);
    StorageContainerLocationProtocolClientSideTranslatorPB client =
        new StorageContainerLocationProtocolClientSideTranslatorPB(
            RPC.getProxy(StorageContainerLocationProtocolPB.class, version,
                scmAddress, UserGroupInformation.getCurrentUser(), ozoneConf,
                NetUtils.getDefaultSocketFactory(ozoneConf),
                Client.getRpcTimeout(ozoneConf)));
    ScmClient storageClient = new ContainerOperationClient(
        client, new XceiverClientManager(ozoneConf));
    return storageClient;
  }

  /**
   * Adds ALL the options that hdfs scm command supports. Given the hierarchy
   * of commands, the options are added in a cascading manner, e.g.:
   * {@link SCMCLI} asks {@link ContainerCommandHandler} to add it's options,
   * which then asks it's sub command, such as
   * {@link org.apache.hadoop.ozone.scm.cli.container.CreateContainerHandler}
   * to add it's own options.
   *
   * We need to do this because {@link BasicParser} need to take all the options
   * when paring args.
   * @return ALL the options supported by this CLI.
   */
  @Override
  protected Options getOptions() {
    Options newOptions = new Options();
    // add the options
    addTopLevelOptions(newOptions);
    ContainerCommandHandler.addOptions(newOptions);
    // TODO : add pool, node and pipeline commands.
    addHelpOption(newOptions);
    return newOptions;
  }

  private static void addTopLevelOptions(Options options) {
    Option containerOps = new Option(
        CONTAINER_CMD, false, "Container related options");
    options.addOption(containerOps);
    // TODO : add pool, node and pipeline commands.
  }

  private static void addHelpOption(Options options) {
    Option helpOp = new Option(HELP_OP, false, "display help message");
    options.addOption(helpOp);
  }

  @Override
  protected void displayHelp() {
    HelpFormatter helpFormatter = new HelpFormatter();
    Options topLevelOptions = new Options();
    addTopLevelOptions(topLevelOptions);
    helpFormatter.printHelp(CMD_WIDTH, "hdfs scmcli <commands> [<options>]",
        "where <commands> can be one of the following",
        topLevelOptions, "");
  }

  @Override
  public int run(String[] args) throws Exception {
    CommandLine cmd = parseArgs(args, options);
    if (cmd == null) {
      err.println("Unrecognized options:" + Arrays.asList(args));
      displayHelp();
      return UNRECOGNIZED_CMD;
    }
    return dispatch(cmd, options);
  }

  /**
   * This function parses all command line arguments
   * and returns the appropriate values.
   *
   * @param argv - Argv from main
   *
   * @return CommandLine
   */
  @Override
  protected CommandLine parseArgs(String[] argv, Options opts)
      throws ParseException {
    try {
      BasicParser parser = new BasicParser();
      return parser.parse(opts, argv);
    } catch (ParseException ex) {
      err.println(ex.getMessage());
    }
    return null;
  }

  @Override
  protected int dispatch(CommandLine cmd, Options opts)
      throws IOException, OzoneException, URISyntaxException {
    OzoneCommandHandler handler = null;
    try {
      if (cmd.hasOption(CONTAINER_CMD)) {
        handler = new ContainerCommandHandler(scmClient);
      }

      if (handler == null) {
        if (cmd.hasOption(HELP_OP)) {
          displayHelp();
          return SUCCESS;
        } else {
          displayHelp();
          err.println("Unrecognized command: " + Arrays.asList(cmd.getArgs()));
          return UNRECOGNIZED_CMD;
        }
      } else {
        // Redirect stdout and stderr if necessary.
        handler.setOut(this.out);
        handler.setErr(this.err);
        handler.execute(cmd);
        return SUCCESS;
      }
    } catch (IOException ioe) {
      err.println("Error executing command:" + ioe);
      return EXECUTION_ERROR;
    }
  }
}
