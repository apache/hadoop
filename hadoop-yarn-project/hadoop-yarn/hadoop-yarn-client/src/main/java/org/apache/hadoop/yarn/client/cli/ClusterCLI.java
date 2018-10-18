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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.MissingArgumentException;
import org.apache.commons.cli.Options;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.yarn.api.records.NodeAttributeInfo;
import org.apache.hadoop.yarn.api.records.NodeLabel;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.nodelabels.CommonNodeLabelsManager;

import com.google.common.annotations.VisibleForTesting;

/**
 * Cluster CLI used to get over all information of the cluster
 */
@Private
public class ClusterCLI extends YarnCLI {
  private static final String TITLE = "yarn cluster";
  public static final String LIST_LABELS_CMD = "list-node-labels";
  public static final String DIRECTLY_ACCESS_NODE_LABEL_STORE =
      "directly-access-node-label-store";
  public static final String LIST_CLUSTER_ATTRIBUTES="list-node-attributes";
  public static final String CMD = "cluster";
  private boolean accessLocal = false;
  static CommonNodeLabelsManager localNodeLabelsManager = null;

  public static void main(String[] args) throws Exception {
    ClusterCLI cli = new ClusterCLI();
    cli.setSysOutPrintStream(System.out);
    cli.setSysErrPrintStream(System.err);
    int res = ToolRunner.run(cli, args);
    cli.stop();
    System.exit(res);
  }

  @Override
  public int run(String[] args) throws Exception {    
    Options opts = new Options();

    opts.addOption("lnl", LIST_LABELS_CMD, false,
        "List cluster node-label collection");
    opts.addOption("lna", LIST_CLUSTER_ATTRIBUTES, false,
        "List cluster node-attribute collection");
    opts.addOption("h", HELP_CMD, false, "Displays help for all commands.");
    opts.addOption("dnl", DIRECTLY_ACCESS_NODE_LABEL_STORE, false,
        "This is DEPRECATED, will be removed in future releases. Directly access node label store, "
            + "with this option, all node label related operations"
            + " will NOT connect RM. Instead, they will"
            + " access/modify stored node labels directly."
            + " By default, it is false (access via RM)."
            + " AND PLEASE NOTE: if you configured "
            + YarnConfiguration.FS_NODE_LABELS_STORE_ROOT_DIR
            + " to a local directory"
            + " (instead of NFS or HDFS), this option will only work"
            + " when the command run on the machine where RM is running."
            + " Also, this option is UNSTABLE, could be removed in future"
            + " releases.");

    int exitCode = -1;
    CommandLine parsedCli = null;
    try {
      parsedCli = new GnuParser().parse(opts, args);
    } catch (MissingArgumentException ex) {
      sysout.println("Missing argument for options");
      printUsage(opts);
      return exitCode;
    }

    if (parsedCli.hasOption(DIRECTLY_ACCESS_NODE_LABEL_STORE)) {
      accessLocal = true;
    }

    if (parsedCli.hasOption(LIST_LABELS_CMD)) {
      printClusterNodeLabels();
    } else if(parsedCli.hasOption(LIST_CLUSTER_ATTRIBUTES)){
      printClusterNodeAttributes();
    } else if (parsedCli.hasOption(HELP_CMD)) {
      printUsage(opts);
      return 0;
    } else {
      syserr.println("Invalid Command Usage : ");
      printUsage(opts);
    }
    return 0;
  }

  private void printClusterNodeAttributes() throws IOException, YarnException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    PrintWriter pw = new PrintWriter(
        new OutputStreamWriter(baos, Charset.forName("UTF-8")));
    for (NodeAttributeInfo attribute : client.getClusterAttributes()) {
      pw.println(attribute.toString());
    }
    pw.close();
    sysout.println(baos.toString("UTF-8"));
  }

  void printClusterNodeLabels() throws YarnException, IOException {
    List<NodeLabel> nodeLabels = null;
    if (accessLocal) {
      nodeLabels =
          new ArrayList<>(getNodeLabelManagerInstance(getConf()).getClusterNodeLabels());
    } else {
      nodeLabels = new ArrayList<>(client.getClusterNodeLabels());
    }
    sysout.println(String.format("Node Labels: %s",
        StringUtils.join(nodeLabels.iterator(), ",")));
  }

  @VisibleForTesting
  static synchronized CommonNodeLabelsManager
      getNodeLabelManagerInstance(Configuration conf) {
    if (localNodeLabelsManager == null) {
      localNodeLabelsManager = new CommonNodeLabelsManager();
      localNodeLabelsManager.init(conf);
      localNodeLabelsManager.start();
    }
    return localNodeLabelsManager;
  }

  @VisibleForTesting
  void printUsage(Options opts) throws UnsupportedEncodingException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    PrintWriter pw =
        new PrintWriter(new OutputStreamWriter(baos, Charset.forName("UTF-8")));
    new HelpFormatter().printHelp(pw, HelpFormatter.DEFAULT_WIDTH, TITLE, null,
        opts, HelpFormatter.DEFAULT_LEFT_PAD, HelpFormatter.DEFAULT_DESC_PAD,
        null);
    pw.close();
    sysout.println(baos.toString("UTF-8"));
  }
}