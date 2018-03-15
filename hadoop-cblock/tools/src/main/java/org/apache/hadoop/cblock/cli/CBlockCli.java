/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package org.apache.hadoop.cblock.cli;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import org.apache.hadoop.cblock.CblockUtils;
import org.apache.hadoop.cblock.client.CBlockVolumeClient;
import org.apache.hadoop.cblock.meta.VolumeInfo;
import org.apache.hadoop.cblock.protocolPB.CBlockServiceProtocolPB;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.ipc.ProtobufRpcEngine;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.hdsl.conf.OzoneConfiguration;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.PrintStream;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.List;

/**
 * The command line tool class.
 */
public class CBlockCli extends Configured implements Tool {

  private static final String CREATE_VOLUME = "createVolume";

  private static final String DELETE_VOLUME = "deleteVolume";

  private static final String INFO_VOLUME = "infoVolume";

  private static final String LIST_VOLUME = "listVolume";

  private static final String SERVER_ADDR = "serverAddr";

  private static final String HELP = "help";

  private static final Logger LOG =
      LoggerFactory.getLogger(CBlockCli.class);
  private OzoneConfiguration conf;

  private PrintStream printStream;

  private Options options;

  private BasicParser parser;

  private CBlockVolumeClient localProxy;

  public CBlockCli(OzoneConfiguration conf, PrintStream printStream)
      throws IOException {
    this.printStream = printStream;
    this.conf = conf;
    this.options = getOptions();
    this.parser = new BasicParser();
  }

  public CBlockCli(OzoneConfiguration conf) throws IOException{
    this(conf, System.out);
  }

  private CommandLine parseArgs(String[] argv)
      throws ParseException {
    return parser.parse(options, argv);
  }

  private static Options getOptions() {
    Options options = new Options();
    Option serverAddress = OptionBuilder
        .withArgName("serverAddress>:<serverPort")
        .withLongOpt(SERVER_ADDR)
        .withValueSeparator(':')
        .hasArgs(2)
        .withDescription("specify server address:port")
        .create("s");
    options.addOption(serverAddress);

    // taking 4 args: userName, volumeName, volumeSize, blockSize
    Option createVolume = OptionBuilder
        .withArgName("user> <volume> <volumeSize in [GB/TB]> <blockSize")
        .withLongOpt(CREATE_VOLUME)
        .withValueSeparator(' ')
        .hasArgs(4)
        .withDescription("create a fresh new volume")
        .create("c");
    options.addOption(createVolume);

    // taking 2 args: userName, volumeName
    Option deleteVolume = OptionBuilder
        .withArgName("user> <volume")
        .withLongOpt(DELETE_VOLUME)
        .hasArgs(2)
        .withDescription("delete a volume")
        .create("d");
    options.addOption(deleteVolume);

    // taking 2 args: userName, volumeName
    Option infoVolume = OptionBuilder
        .withArgName("user> <volume")
        .withLongOpt(INFO_VOLUME)
        .hasArgs(2)
        .withDescription("info a volume")
        .create("i");
    options.addOption(infoVolume);

    // taking 1 arg: userName
    Option listVolume = OptionBuilder
        .withArgName("user")
        .withLongOpt(LIST_VOLUME)
        .hasOptionalArgs(1)
        .withDescription("list all volumes")
        .create("l");
    options.addOption(listVolume);

    Option help = OptionBuilder
        .withLongOpt(HELP)
        .withDescription("help")
        .create("h");
    options.addOption(help);

    return options;
  }

  @Override
  public int run(String[] args) throws ParseException, IOException {
    CommandLine commandLine = parseArgs(args);
    if (commandLine.hasOption("s")) {
      String[] serverAddrArgs = commandLine.getOptionValues("s");
      LOG.info("server address" + Arrays.toString(serverAddrArgs));
      String serverHost = serverAddrArgs[0];
      int serverPort = Integer.parseInt(serverAddrArgs[1]);
      InetSocketAddress serverAddress =
          new InetSocketAddress(serverHost, serverPort);
      this.localProxy = new CBlockVolumeClient(conf, serverAddress);
    } else {
      this.localProxy = new CBlockVolumeClient(conf);
    }

    if (commandLine.hasOption("h")) {
      LOG.info("help");
      help();
    }

    if (commandLine.hasOption("c")) {
      String[] createArgs = commandLine.getOptionValues("c");
      LOG.info("create volume:" + Arrays.toString(createArgs));
      createVolume(createArgs);
    }

    if (commandLine.hasOption("d")) {
      String[] deleteArgs = commandLine.getOptionValues("d");
      LOG.info("delete args:" + Arrays.toString(deleteArgs));
      deleteVolume(deleteArgs);
    }

    if (commandLine.hasOption("l")) {
      String[] listArg = commandLine.getOptionValues("l");
      LOG.info("list args:" + Arrays.toString(listArg));
      listVolume(listArg);
    }

    if (commandLine.hasOption("i")) {
      String[] infoArgs = commandLine.getOptionValues("i");
      LOG.info("info args:" + Arrays.toString(infoArgs));
      infoVolume(infoArgs);
    }
    return 0;
  }

  public static void main(String[] argv) throws Exception {
    CblockUtils.activateConfigs();
    OzoneConfiguration cblockConf = new OzoneConfiguration();
    RPC.setProtocolEngine(cblockConf, CBlockServiceProtocolPB.class,
        ProtobufRpcEngine.class);
    int res = 0;
    Tool shell = new CBlockCli(cblockConf, System.out);
    try {
      ToolRunner.run(shell, argv);
    } catch (Exception ex) {
      LOG.error(ex.toString());
      res = 1;
    }
    System.exit(res);
  }



  private void createVolume(String[] createArgs) throws IOException {
    String userName = createArgs[0];
    String volumeName = createArgs[1];
    long volumeSize = CblockUtils.parseSize(createArgs[2]);
    int blockSize = Integer.parseInt(createArgs[3])*1024;
    localProxy.createVolume(userName, volumeName, volumeSize, blockSize);
  }

  private void deleteVolume(String[] deleteArgs) throws IOException {
    String userName = deleteArgs[0];
    String volumeName = deleteArgs[1];
    boolean force = false;
    if (deleteArgs.length > 2) {
      force = Boolean.parseBoolean(deleteArgs[2]);
    }
    localProxy.deleteVolume(userName, volumeName, force);
  }

  private void infoVolume(String[] infoArgs) throws IOException {
    String userName = infoArgs[0];
    String volumeName = infoArgs[1];
    VolumeInfo volumeInfo = localProxy.infoVolume(userName, volumeName);
    printStream.println(volumeInfo.toString());
  }

  private void listVolume(String[] listArgs) throws IOException {
    StringBuilder stringBuilder = new StringBuilder();
    List<VolumeInfo> volumeResponse;
    if (listArgs == null) {
      volumeResponse = localProxy.listVolume(null);
    } else {
      volumeResponse = localProxy.listVolume(listArgs[0]);
    }
    for (int i = 0; i<volumeResponse.size(); i++) {
      stringBuilder.append(
          String.format("%s:%s\t%d\t%d", volumeResponse.get(i).getUserName(),
              volumeResponse.get(i).getVolumeName(),
          volumeResponse.get(i).getVolumeSize(),
          volumeResponse.get(i).getBlockSize()));
      if (i < volumeResponse.size() - 1) {
        stringBuilder.append("\n");
      }
    }
    printStream.println(stringBuilder);
  }

  private void help() {
    HelpFormatter formatter = new HelpFormatter();
    formatter.printHelp(100, "cblock", "", options, "");
  }
}
