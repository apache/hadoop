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

import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;

import picocli.CommandLine;

import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.Callable;

/**
 * Subcommand to list of the available insight points.
 */
@CommandLine.Command(
    name = "list",
    description = "Show available insight points.",
    mixinStandardHelpOptions = true,
    versionProvider = HddsVersionProvider.class)
public class ListSubCommand extends BaseInsightSubCommand
    implements Callable<Void> {

  @CommandLine.Parameters(defaultValue = "")
  private String insightPrefix;

  @Override
  public Void call() throws Exception {

    System.out.println("Available insight points:\n\n");

    Map<String, InsightPoint> insightPoints =
        createInsightPoints(new OzoneConfiguration());
    for (Entry<String, InsightPoint> entry : insightPoints.entrySet()) {
      if (insightPrefix == null || entry.getKey().startsWith(insightPrefix)) {
        System.out.println(String.format("  %-33s    %s", entry.getKey(),
            entry.getValue().getDescription()));
      }
    }
    return null;
  }

}
