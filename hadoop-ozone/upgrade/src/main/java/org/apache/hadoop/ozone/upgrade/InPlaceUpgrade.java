/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.ozone.upgrade;

import org.apache.hadoop.hdds.cli.GenericCli;
import org.apache.hadoop.hdds.cli.HddsVersionProvider;

import picocli.CommandLine.Command;

/**
 * Command  line interface for the In-Place upgrade utility.
 * <p>
 * In-Place upgrade can convert HDFS cluster data to Ozone data without
 * (or minimal) data moving.
 */
@Command(name = "ozone upgrade",
    description = "Convert raw HDFS data to Ozone data without data movement.",
    subcommands = {
        Plan.class,
        Balance.class,
        Execute.class,
    },
    versionProvider = HddsVersionProvider.class,
    mixinStandardHelpOptions = true)
public class InPlaceUpgrade extends GenericCli {

  public static void main(String[] args) {
    new InPlaceUpgrade().run(args);
  }
}
