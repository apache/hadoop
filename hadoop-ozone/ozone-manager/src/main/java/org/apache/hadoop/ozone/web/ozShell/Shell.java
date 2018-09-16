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

import org.apache.hadoop.hdds.cli.GenericCli;
import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.ozone.web.ozShell.bucket.BucketCommands;
import org.apache.hadoop.ozone.web.ozShell.keys.KeyCommands;
import org.apache.hadoop.ozone.web.ozShell.volume.VolumeCommands;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine.Command;

/**
 * Ozone user interface commands.
 *
 * This class uses dispatch method to make calls
 * to appropriate handlers that execute the ozone functions.
 */
@Command(name = "ozone sh",
    description = "Shell for Ozone object store",
    subcommands = {
        VolumeCommands.class,
        BucketCommands.class,
        KeyCommands.class
    },
    versionProvider = HddsVersionProvider.class,
    mixinStandardHelpOptions = true)
public class Shell extends GenericCli {


  private static final Logger LOG = LoggerFactory.getLogger(Shell.class);

  public static final String OZONE_URI_DESCRIPTION = "Ozone URI could start "
      + "with o3:// or http(s):// or without prefix. REST protocol will "
      + "be used for http(s), RPC otherwise. URI may contain the host and port "
      + "of the SCM server. Both are optional. "
      + "If they are not specified it will be identified from "
      + "the config files.";

  public static final String OZONE_VOLUME_URI_DESCRIPTION =
      "URI of the volume.\n" + OZONE_URI_DESCRIPTION;

  public static final String OZONE_BUCKET_URI_DESCRIPTION =
      "URI of the volume/bucket.\n" + OZONE_URI_DESCRIPTION;

  public static final String OZONE_KEY_URI_DESCRIPTION =
      "URI of the volume/bucket/key.\n" + OZONE_URI_DESCRIPTION;

  // General options
  public static final int DEFAULT_OZONE_PORT = 50070;

  /**
   * Main for the ozShell Command handling.
   *
   * @param argv - System Args Strings[]
   * @throws Exception
   */
  public static void main(String[] argv) throws Exception {
    new Shell().run(argv);
  }
}

