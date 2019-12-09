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
package org.apache.hadoop.yarn.client.cli;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.nio.charset.Charset;
import java.text.DecimalFormat;
import java.util.Set;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.MissingArgumentException;
import org.apache.commons.cli.Options;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.yarn.api.records.NodeLabel;
import org.apache.hadoop.yarn.api.records.QueueInfo;
import org.apache.hadoop.yarn.exceptions.YarnException;

import com.google.common.annotations.VisibleForTesting;

@Private
@Unstable
public class QueueCLI extends YarnCLI {
  public static final String QUEUE = "queue";

  public static void main(String[] args) throws Exception {
    QueueCLI cli = new QueueCLI();
    cli.setSysOutPrintStream(System.out);
    cli.setSysErrPrintStream(System.err);
    int res = ToolRunner.run(cli, args);
    cli.stop();
    System.exit(res);
  }

  @Override
  public int run(String[] args) throws Exception {
    Options opts = new Options();

    opts.addOption(STATUS_CMD, true,
        "List queue information about given queue.");
    opts.addOption(HELP_CMD, false, "Displays help for all commands.");
    opts.getOption(STATUS_CMD).setArgName("Queue Name");

    CommandLine cliParser = null;
    try {
      cliParser = new GnuParser().parse(opts, args);
    } catch (MissingArgumentException ex) {
      sysout.println("Missing argument for options");
      printUsage(opts);
      return -1;
    }
    createAndStartYarnClient();
    if (cliParser.hasOption(STATUS_CMD)) {
      if (args.length != 2) {
        printUsage(opts);
        return -1;
      }
      return listQueue(cliParser.getOptionValue(STATUS_CMD));
    } else if (cliParser.hasOption(HELP_CMD)) {
      printUsage(opts);
      return 0;
    } else {
      syserr.println("Invalid Command Usage : ");
      printUsage(opts);
      return -1;
    }
  }

  /**
   * It prints the usage of the command
   * 
   * @param opts
   */
  @VisibleForTesting
  void printUsage(Options opts) {
    new HelpFormatter().printHelp(QUEUE, opts);
  }
  
  /**
   * Lists the Queue Information matching the given queue name
   * 
   * @param queueName
   * @throws YarnException
   * @throws IOException
   */
  private int listQueue(String queueName) throws YarnException, IOException {
    int rc;
    PrintWriter writer = new PrintWriter(
        new OutputStreamWriter(sysout, Charset.forName("UTF-8")));

    QueueInfo queueInfo = client.getQueueInfo(queueName);
    if (queueInfo != null) {
      writer.println("Queue Information : ");
      printQueueInfo(writer, queueInfo);
      rc = 0;
    } else {
      writer.println("Cannot get queue from RM by queueName = " + queueName
          + ", please check.");
      rc = -1;
    }
    writer.flush();
    return rc;
  }

  private void printQueueInfo(PrintWriter writer, QueueInfo queueInfo) {
    writer.print("Queue Name : ");
    writer.println(queueInfo.getQueueName());

    writer.print("\tState : ");
    writer.println(queueInfo.getQueueState());
    DecimalFormat df = new DecimalFormat("#.0");
    writer.print("\tCapacity : ");
    writer.println(df.format(queueInfo.getCapacity() * 100) + "%");
    writer.print("\tCurrent Capacity : ");
    writer.println(df.format(queueInfo.getCurrentCapacity() * 100) + "%");
    writer.print("\tMaximum Capacity : ");
    writer.println(df.format(queueInfo.getMaximumCapacity() * 100) + "%");
    writer.print("\tDefault Node Label expression : ");
    String nodeLabelExpression = queueInfo.getDefaultNodeLabelExpression();
    nodeLabelExpression =
        (nodeLabelExpression == null || nodeLabelExpression.trim().isEmpty())
            ? NodeLabel.DEFAULT_NODE_LABEL_PARTITION : nodeLabelExpression;
    writer.println(nodeLabelExpression);

    Set<String> nodeLabels = queueInfo.getAccessibleNodeLabels();
    StringBuilder labelList = new StringBuilder();
    writer.print("\tAccessible Node Labels : ");
    for (String nodeLabel : nodeLabels) {
      if (labelList.length() > 0) {
        labelList.append(',');
      }
      labelList.append(nodeLabel);
    }
    writer.println(labelList.toString());

    Boolean preemptStatus = queueInfo.getPreemptionDisabled();
    if (preemptStatus != null) {
      writer.print("\tPreemption : ");
      writer.println(preemptStatus ? "disabled" : "enabled");
    }

    Boolean intraQueuePreemption = queueInfo.getIntraQueuePreemptionDisabled();
    if (intraQueuePreemption != null) {
      writer.print("\tIntra-queue Preemption : ");
      writer.println(intraQueuePreemption ? "disabled" : "enabled");
    }
  }
}
