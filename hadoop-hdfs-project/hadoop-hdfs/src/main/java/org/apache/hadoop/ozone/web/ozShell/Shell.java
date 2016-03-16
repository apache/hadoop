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

package org.apache.hadoop.ozone.web.ozShell;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.ozone.web.exceptions.OzoneException;
import org.apache.hadoop.ozone.web.ozShell.volume.CreateVolumeHandler;
import org.apache.hadoop.ozone.web.ozShell.volume.DeleteVolumeHandler;
import org.apache.hadoop.ozone.web.ozShell.volume.InfoVolumeHandler;
import org.apache.hadoop.ozone.web.ozShell.volume.ListVolumeHandler;
import org.apache.hadoop.ozone.web.ozShell.volume.UpdateVolumeHandler;
import org.apache.hadoop.ozone.web.ozShell.bucket.CreateBucketHandler;
import org.apache.hadoop.ozone.web.ozShell.bucket.DeleteBucketHandler;
import org.apache.hadoop.ozone.web.ozShell.bucket.InfoBucketHandler;
import org.apache.hadoop.ozone.web.ozShell.bucket.ListBucketHandler;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.net.URISyntaxException;

/**
 * Ozone user interface commands.
 *
 * This class uses dispatch method to make calls
 * to appropriate handlers that execute the ozone functions.
 */
public class Shell extends Configured implements Tool {

  // General options
  public static final int DEFAULT_OZONE_PORT = 50070;
  public static final String VERBOSE = "v";

  // volume related command line arguments
  public static final String RUNAS = "root";
  public static final String USER = "user";
  public static final String OWNER = "owner";
  public static final String QUOTA = "quota";
  public static final String CREATE_VOLUME = "createVolume";
  public static final String UPDATE_VOLUME = "updateVolume";
  public static final String DELETE_VOLUME = "deleteVolume";
  public static final String LIST_VOLUME = "listVolume";
  public static final String INFO_VOLUME = "infoVolume";

  // bucket related command line arguments
  public static final String CREATE_BUCKET = "createBucket";
  public static final String UPDATE_BUCKET = "updateBucket";
  public static final String DELETE_BUCKET = "deleteBucket";
  public static final String LIST_BUCKET = "listBucket";
  public static final String INFO_BUCKET = "infoBucket";
  public static final String ADD_ACLS = "addAcl";
  public static final String REMOVE_ACLS = "removeAcl";


  /**
   * Execute the command with the given arguments.
   *
   * @param args command specific arguments.
   *
   * @return exit code.
   *
   * @throws Exception
   */
  @Override
  public int run(String[] args) throws Exception {
    Options opts = getOpts();
    CommandLine cmd = parseArgs(args, opts);
    return dispatch(cmd, opts);
  }

  /**
   * Construct an ozShell.
   */
  public Shell() {
  }

  /**
   * returns the Command Line Options.
   *
   * @return Options
   */
  private Options getOpts() {
    Options opts = new Options();
    addVolumeCommands(opts);
    addBucketCommands(opts);
    return opts;
  }

  /**
   * This function parses all command line arguments
   * and returns the appropriate values.
   *
   * @param argv - Argv from main
   *
   * @return CommandLine
   */
  private CommandLine parseArgs(String[] argv, Options opts)
      throws org.apache.commons.cli.ParseException {
    BasicParser parser = new BasicParser();
    return parser.parse(opts, argv);
  }


  /**
   * All volume related commands are added in this function for the command
   * parser.
   *
   * @param options - Command Options class.
   */
  private void addVolumeCommands(Options options) {
    Option verbose = new Option(VERBOSE, false, "verbose information output.");
    options.addOption(verbose);

    Option runas = new Option(RUNAS, false, "Run the command as \"hdfs\" user");
    options.addOption(runas);

    Option userName = new Option(USER, true,
                                 "Name of the user in volume management " +
                                     "functions");
    options.addOption(userName);

    Option quota = new Option(QUOTA, true, "Quota for the volume. E.g. 10TB");
    options.addOption(quota);


    Option createVolume = new Option(CREATE_VOLUME, true, "creates a volume" +
        "for the specified user.\n \t For example : hdfs oz  -createVolume " +
        "<volumeURI> -root -user <userName>\n");
    options.addOption(createVolume);

    Option deleteVolume = new Option(DELETE_VOLUME, true, "deletes a volume" +
        "if it is empty.\n \t For example : hdfs oz -deleteVolume <volumeURI>" +
        " -root \n");
    options.addOption(deleteVolume);

    Option listVolume =
        new Option(LIST_VOLUME, true, "List the volumes of a given user.\n" +
            "For example : hdfs oz -listVolume <ozoneURI>" +
            "-user <username> -root or hdfs oz " +
            "-listVolume");
    options.addOption(listVolume);

    Option updateVolume =
        new Option(UPDATE_VOLUME, true, "updates an existing volume.\n" +
            "\t For example : hdfs oz " +
            "-updateVolume <volumeURI> -quota " +
            "100TB\n");
    options.addOption(updateVolume);

    Option infoVolume = new Option(INFO_VOLUME, true,
                                   "returns information about a specific " +
                                       "volume.");
    options.addOption(infoVolume);
  }

  /**
   * All bucket related commands for ozone.
   *
   * @param opts - Options
   */
  private void addBucketCommands(Options opts) {
    Option createBucket = new Option(CREATE_BUCKET, true,
        "creates a bucket in a given volume.\n" +
            "\t For example : hdfs oz " +
            "-createBucket " +
            "<volumeName/bucketName>");
    opts.addOption(createBucket);

    Option infoBucket =
        new Option(INFO_BUCKET, true, "returns information about a bucket.");
    opts.addOption(infoBucket);

    Option deleteBucket =
        new Option(DELETE_BUCKET, true, "deletes an empty bucket.");
    opts.addOption(deleteBucket);

    Option listBucket =
        new Option(LIST_BUCKET, true, "Lists the buckets in a volume.");
    opts.addOption(listBucket);

  }


  /**
   * Main for the ozShell Command handling.
   *
   * @param argv - System Args Strings[]
   *
   * @throws Exception
   */
  public static void main(String[] argv) throws Exception {
    Shell shell = new Shell();
    Configuration conf = new Configuration();
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

  /**
   * Dispatches calls to the right command Handler classes.
   *
   * @param cmd - CommandLine
   *
   * @throws IOException
   * @throws OzoneException
   * @throws URISyntaxException
   */
  private int dispatch(CommandLine cmd, Options opts)
      throws IOException, OzoneException, URISyntaxException {
    Handler handler = null;
    final int eightyColumn = 80;

    try {

      // volume functions
      if (cmd.hasOption(Shell.CREATE_VOLUME)) {
        handler = new CreateVolumeHandler();
      }

      if (cmd.hasOption(Shell.DELETE_VOLUME)) {
        handler = new DeleteVolumeHandler();
      }

      if (cmd.hasOption(Shell.LIST_VOLUME)) {
        handler = new ListVolumeHandler();
      }

      if (cmd.hasOption(Shell.UPDATE_VOLUME)) {
        handler = new UpdateVolumeHandler();
      }

      if (cmd.hasOption(Shell.INFO_VOLUME)) {
        handler = new InfoVolumeHandler();
      }

      // bucket functions
      if (cmd.hasOption(Shell.CREATE_BUCKET)) {
        handler = new CreateBucketHandler();
      }

      if (cmd.hasOption(Shell.DELETE_BUCKET)) {
        handler = new DeleteBucketHandler();
      }

      if (cmd.hasOption(Shell.INFO_BUCKET)) {
        handler = new InfoBucketHandler();
      }

      if (cmd.hasOption(Shell.LIST_BUCKET)) {
        handler = new ListBucketHandler();
      }


      if (handler != null) {
        handler.execute(cmd);
        return 0;
      } else {
        HelpFormatter helpFormatter = new HelpFormatter();
        helpFormatter.printHelp(eightyColumn, "hdfs oz -command uri [args]",
                "Ozone Commands",
                opts, "Please correct your command and try again.");
        return 1;
      }
    } catch (IOException | OzoneException | URISyntaxException ex) {
      System.err.printf("Command Failed : %s%n", ex.getMessage());
      return 1;
    }
  }
}

