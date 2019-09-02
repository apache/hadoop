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
package org.apache.hadoop.ozone.insight;

import org.apache.hadoop.hdds.cli.GenericCli;
import org.apache.hadoop.hdds.cli.HddsVersionProvider;

import picocli.CommandLine;

/**
 * Command line utility to check logs/metrics of internal ozone components.
 */
@CommandLine.Command(name = "ozone insight",
    hidden = true, description = "Show debug information about a selected "
    + "Ozone component",
    versionProvider = HddsVersionProvider.class,
    subcommands = {ListSubCommand.class, LogSubcommand.class,
        MetricsSubCommand.class, ConfigurationSubCommand.class},
    mixinStandardHelpOptions = true)
public class Insight extends GenericCli {

  public static void main(String[] args) throws Exception {
    new Insight().run(args);
  }

}
