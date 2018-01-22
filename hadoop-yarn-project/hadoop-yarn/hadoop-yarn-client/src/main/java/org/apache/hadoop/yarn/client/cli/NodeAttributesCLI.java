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
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.MissingArgumentException;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.ha.HAAdmin.UsageInfo;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.yarn.api.records.NodeAttribute;
import org.apache.hadoop.yarn.api.records.NodeAttributeType;
import org.apache.hadoop.yarn.client.ClientRMProxy;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.api.ResourceManagerAdministrationProtocol;
import org.apache.hadoop.yarn.server.api.protocolrecords.AttributeMappingOperationType;
import org.apache.hadoop.yarn.server.api.protocolrecords.NodeToAttributes;
import org.apache.hadoop.yarn.server.api.protocolrecords.NodesToAttributesMappingRequest;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;

/**
 * CLI to map attributes to Nodes.
 *
 */
public class NodeAttributesCLI extends Configured implements Tool {

  protected static final String INVALID_MAPPING_ERR_MSG =
      "Invalid Node to attribute mapping : ";

  protected static final String USAGE_YARN_NODE_ATTRIBUTES =
      "Usage: yarn node-attributes ";

  protected static final String NO_MAPPING_ERR_MSG =
      "No node-to-attributes mappings are specified";

  protected final static Map<String, UsageInfo> NODE_ATTRIB_USAGE =
      ImmutableMap.<String, UsageInfo>builder()
          .put("-replace",
              new UsageInfo(
                  "<\"node1:attribute[(type)][=value],attribute1[=value],"
                      + "attribute2  node2:attribute2[=value],attribute3\">",
                  " Replace the node to attributes mapping information at the"
                      + " ResourceManager with the new mapping. Currently"
                      + " supported attribute type. And string is the default"
                      + " type too. Attribute value if not specified for string"
                      + " type value will be considered as empty string."
                      + " Replaced node-attributes should not violate the"
                      + " existing attribute to attribute type mapping."))
          .put("-add",
              new UsageInfo(
                  "<\"node1:attribute[(type)][=value],attribute1[=value],"
                      + "attribute2  node2:attribute2[=value],attribute3\">",
                  " Adds or updates the node to attributes mapping information"
                      + " at the ResourceManager. Currently supported attribute"
                      + " type is string. And string is the default type too."
                      + " Attribute value if not specified for string type"
                      + " value will be considered as empty string. Added or"
                      + " updated node-attributes should not violate the"
                      + " existing attribute to attribute type mapping."))
          .put("-remove",
              new UsageInfo("<\"node1:attribute,attribute1 node2:attribute2\">",
                  " Removes the specified node to attributes mapping"
                      + " information at the ResourceManager"))
          .put("-failOnUnknownNodes",
              new UsageInfo("",
                  "Can be used optionally along with other options. When its"
                      + " set, it will fail if specified nodes are unknown."))
          .build();

  /** Output stream for errors, for use in tests. */
  private PrintStream errOut = System.err;

  public NodeAttributesCLI() {
    super();
  }

  public NodeAttributesCLI(Configuration conf) {
    super(conf);
  }

  protected void setErrOut(PrintStream errOut) {
    this.errOut = errOut;
  }

  private void printHelpMsg(String cmd) {
    StringBuilder builder = new StringBuilder();
    UsageInfo usageInfo = null;
    if (cmd != null && !(cmd.trim().isEmpty())) {
      usageInfo = NODE_ATTRIB_USAGE.get(cmd);
    }
    if (usageInfo != null) {
      if (usageInfo.args == null) {
        builder.append("   " + cmd + ":\n" + usageInfo.help);
      } else {
        String space = (usageInfo.args == "") ? "" : " ";
        builder.append(
            "   " + cmd + space + usageInfo.args + " :\n" + usageInfo.help);
      }
    } else {
      // help for all commands
      builder.append("Usage: yarn node-attributes\n");
      for (Map.Entry<String, UsageInfo> cmdEntry : NODE_ATTRIB_USAGE
          .entrySet()) {
        usageInfo = cmdEntry.getValue();
        builder.append("   " + cmdEntry.getKey() + " " + usageInfo.args
            + " :\n " + usageInfo.help + "\n");
      }
      builder.append("   -help" + " [cmd]\n");
    }
    errOut.println(builder);
  }

  private static void buildIndividualUsageMsg(String cmd,
      StringBuilder builder) {
    UsageInfo usageInfo = NODE_ATTRIB_USAGE.get(cmd);
    if (usageInfo == null) {
      return;
    }
    if (usageInfo.args == null) {
      builder.append(USAGE_YARN_NODE_ATTRIBUTES + cmd + "\n");
    } else {
      String space = (usageInfo.args == "") ? "" : " ";
      builder.append(
          USAGE_YARN_NODE_ATTRIBUTES + cmd + space + usageInfo.args + "\n");
    }
  }

  private static void buildUsageMsgForAllCmds(StringBuilder builder) {
    builder.append("Usage: yarn node-attributes\n");
    for (Map.Entry<String, UsageInfo> cmdEntry : NODE_ATTRIB_USAGE.entrySet()) {
      UsageInfo usageInfo = cmdEntry.getValue();
      builder.append("   " + cmdEntry.getKey() + " " + usageInfo.args + "\n");
    }
    builder.append("   -help" + " [cmd]\n");
  }

  /**
   * Displays format of commands.
   *
   * @param cmd The command that is being executed.
   */
  private void printUsage(String cmd) {
    StringBuilder usageBuilder = new StringBuilder();
    if (NODE_ATTRIB_USAGE.containsKey(cmd)) {
      buildIndividualUsageMsg(cmd, usageBuilder);
    } else {
      buildUsageMsgForAllCmds(usageBuilder);
    }
    errOut.println(usageBuilder);
  }

  private void printUsage() {
    printUsage("");
  }

  protected ResourceManagerAdministrationProtocol createAdminProtocol()
      throws IOException {
    // Get the current configuration
    final YarnConfiguration conf = new YarnConfiguration(getConf());
    return ClientRMProxy.createRMProxy(conf,
        ResourceManagerAdministrationProtocol.class);
  }

  @Override
  public void setConf(Configuration conf) {
    if (conf != null) {
      conf = addSecurityConfiguration(conf);
    }
    super.setConf(conf);
  }

  /**
   * Add the requisite security principal settings to the given Configuration,
   * returning a copy.
   *
   * @param conf the original config
   * @return a copy with the security settings added
   */
  private static Configuration addSecurityConfiguration(Configuration conf) {
    // Make a copy so we don't mutate it. Also use an YarnConfiguration to
    // force loading of yarn-site.xml.
    conf = new YarnConfiguration(conf);
    conf.set(CommonConfigurationKeys.HADOOP_SECURITY_SERVICE_USER_NAME_KEY,
        conf.get(YarnConfiguration.RM_PRINCIPAL, ""));
    return conf;
  }

  @Override
  public int run(String[] args) throws Exception {
    if (args.length < 1) {
      printUsage();
      return -1;
    }

    int exitCode = -1;
    int i = 0;
    String cmd = args[i++];

    if ("-help".equals(cmd)) {
      exitCode = 0;
      if (args.length >= 2) {
        printHelpMsg(args[i]);
      } else {
        printHelpMsg("");
      }
      return exitCode;
    }

    try {
      if ("-replace".equals(cmd)) {
        exitCode = handleNodeAttributeMapping(args,
            AttributeMappingOperationType.REPLACE);
      } else if ("-add".equals(cmd)) {
        exitCode =
            handleNodeAttributeMapping(args, AttributeMappingOperationType.ADD);
      } else if ("-remove".equals(cmd)) {
        exitCode = handleNodeAttributeMapping(args,
            AttributeMappingOperationType.REMOVE);
      } else {
        exitCode = -1;
        errOut.println(cmd.substring(1) + ": Unknown command");
        printUsage();
      }
    } catch (IllegalArgumentException arge) {
      exitCode = -1;
      errOut.println(cmd.substring(1) + ": " + arge.getLocalizedMessage());
      printUsage(cmd);
    } catch (RemoteException e) {
      //
      // This is a error returned by hadoop server. Print
      // out the first line of the error message, ignore the stack trace.
      exitCode = -1;
      try {
        String[] content;
        content = e.getLocalizedMessage().split("\n");
        errOut.println(cmd.substring(1) + ": " + content[0]);
      } catch (Exception ex) {
        errOut.println(cmd.substring(1) + ": " + ex.getLocalizedMessage());
      }
    } catch (Exception e) {
      exitCode = -1;
      errOut.println(cmd.substring(1) + ": " + e.getLocalizedMessage());
    }
    return exitCode;
  }

  private int handleNodeAttributeMapping(String args[],
      AttributeMappingOperationType operation)
      throws IOException, YarnException, ParseException {
    Options opts = new Options();
    opts.addOption(operation.name().toLowerCase(), true,
        operation.name().toLowerCase());
    opts.addOption("failOnUnknownNodes", false, "Fail on unknown nodes.");
    int exitCode = -1;
    CommandLine cliParser = null;
    try {
      cliParser = new GnuParser().parse(opts, args);
    } catch (MissingArgumentException ex) {
      errOut.println(NO_MAPPING_ERR_MSG);
      printUsage(args[0]);
      return exitCode;
    }
    List<NodeToAttributes> buildNodeLabelsMapFromStr =
        buildNodeLabelsMapFromStr(
            cliParser.getOptionValue(operation.name().toLowerCase()),
            operation != AttributeMappingOperationType.REPLACE, operation);
    NodesToAttributesMappingRequest request = NodesToAttributesMappingRequest
        .newInstance(operation, buildNodeLabelsMapFromStr,
            cliParser.hasOption("failOnUnknownNodes"));
    ResourceManagerAdministrationProtocol adminProtocol = createAdminProtocol();
    adminProtocol.mapAttributesToNodes(request);
    return 0;
  }

  /**
   * args are expected to be of the format
   * node1:java(string)=8,ssd(boolean)=false node2:ssd(boolean)=true
   */
  private List<NodeToAttributes> buildNodeLabelsMapFromStr(String args,
      boolean validateForAttributes, AttributeMappingOperationType operation) {
    List<NodeToAttributes> nodeToAttributesList = new ArrayList<>();
    for (String nodeToAttributesStr : args.split("[ \n]")) {
      // for each node to attribute mapping
      nodeToAttributesStr = nodeToAttributesStr.trim();
      if (nodeToAttributesStr.isEmpty()
          || nodeToAttributesStr.startsWith("#")) {
        continue;
      }
      if (nodeToAttributesStr.indexOf(":") == -1) {
        throw new IllegalArgumentException(
            INVALID_MAPPING_ERR_MSG + nodeToAttributesStr);
      }
      String[] nodeToAttributes = nodeToAttributesStr.split(":");
      Preconditions.checkArgument(!nodeToAttributes[0].trim().isEmpty(),
          "Node name cannot be empty");
      String node = nodeToAttributes[0];
      String[] attributeNameValueType = null;
      List<NodeAttribute> attributesList = new ArrayList<>();
      NodeAttributeType attributeType = NodeAttributeType.STRING;
      String attributeValue;
      String attributeName;
      Set<String> attributeNamesMapped = new HashSet<>();

      String attributesStr[];
      if (nodeToAttributes.length == 2) {
        // fetching multiple attributes for a node
        attributesStr = nodeToAttributes[1].split(",");
        for (String attributeStr : attributesStr) {
          // get information about each attribute.
          attributeNameValueType = attributeStr.split("="); // to find name
                                                            // value
          Preconditions.checkArgument(
              !(attributeNameValueType[0] == null
                  || attributeNameValueType[0].isEmpty()),
              "Attribute name cannot be null or empty");
          attributeValue = attributeNameValueType.length > 1
              ? attributeNameValueType[1] : "";
          int indexOfOpenBracket = attributeNameValueType[0].indexOf("(");
          if (indexOfOpenBracket == -1) {
            attributeName = attributeNameValueType[0];
          } else if (indexOfOpenBracket == 0) {
            throw new IllegalArgumentException("Attribute for node " + node
                + " is not properly configured : " + attributeStr);
          } else {
            // attribute type has been explicitly configured
            int indexOfCloseBracket = attributeNameValueType[0].indexOf(")");
            if (indexOfCloseBracket == -1
                || indexOfCloseBracket < indexOfOpenBracket) {
              throw new IllegalArgumentException("Attribute for node " + node
                  + " is not properly Configured : " + attributeStr);
            }
            String attributeTypeStr;
            attributeName =
                attributeNameValueType[0].substring(0, indexOfOpenBracket);
            attributeTypeStr = attributeNameValueType[0]
                .substring(indexOfOpenBracket + 1, indexOfCloseBracket);
            try {
              attributeType = NodeAttributeType
                  .valueOf(attributeTypeStr.trim().toUpperCase());
            } catch (IllegalArgumentException e) {
              throw new IllegalArgumentException(
                  "Invalid Attribute type configuration : " + attributeTypeStr
                      + " in " + attributeStr);
            }
          }
          if (attributeNamesMapped.contains(attributeName)) {
            throw new IllegalArgumentException("Attribute " + attributeName
                + " has been mapped more than once in  : "
                + nodeToAttributesStr);
          }
          // TODO when we support different type of attribute type we need to
          // cross verify whether input attributes itself is not violating
          // attribute Name to Type mapping.
          attributesList.add(NodeAttribute.newInstance(attributeName.trim(),
              attributeType, attributeValue.trim()));
        }
      }
      if (validateForAttributes) {
        Preconditions.checkArgument((attributesList.size() > 0),
            "Attributes cannot be null or empty for Operation "
                + operation.name() + " on the node " + node);
      }
      nodeToAttributesList
          .add(NodeToAttributes.newInstance(node, attributesList));
    }

    if (nodeToAttributesList.isEmpty()) {
      throw new IllegalArgumentException(NO_MAPPING_ERR_MSG);
    }
    return nodeToAttributesList;
  }

  public static void main(String[] args) throws Exception {
    int result = ToolRunner.run(new NodeAttributesCLI(), args);
    System.exit(result);
  }
}
