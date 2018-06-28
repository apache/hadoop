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

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.MissingArgumentException;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionGroup;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.UnrecognizedOptionException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.yarn.api.ApplicationClientProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.GetAttributesToNodesRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetAttributesToNodesResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetClusterNodeAttributesRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetClusterNodeAttributesResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetNodesToAttributesRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetNodesToAttributesResponse;
import org.apache.hadoop.yarn.api.records.NodeAttribute;
import org.apache.hadoop.yarn.api.records.NodeAttributeInfo;
import org.apache.hadoop.yarn.api.records.NodeAttributeKey;
import org.apache.hadoop.yarn.api.records.NodeAttributeType;
import org.apache.hadoop.yarn.client.ClientRMProxy;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.api.ResourceManagerAdministrationProtocol;
import org.apache.hadoop.yarn.server.api.protocolrecords.AttributeMappingOperationType;
import org.apache.hadoop.yarn.server.api.protocolrecords.NodeToAttributes;
import org.apache.hadoop.yarn.server.api.protocolrecords.NodesToAttributesMappingRequest;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * CLI to map attributes to Nodes.
 */
public class NodeAttributesCLI extends Configured implements Tool {

  protected static final String INVALID_MAPPING_ERR_MSG =
      "Invalid Node to attribute mapping : ";

  protected static final String USAGE_YARN_NODE_ATTRIBUTES =
      "Usage: yarn nodeattributes ";

  protected static final String MISSING_ARGUMENT =
      "Missing argument for command";

  protected static final String NO_MAPPING_ERR_MSG =
      "No node-to-attributes mappings are specified";

  private static final String DEFAULT_SEPARATOR = System.lineSeparator();
  public static final String INVALID_COMMAND_USAGE = "Invalid Command Usage : ";
  /**
   * Output stream for errors, for use in tests.
   */
  private PrintStream errOut = System.err;

  public NodeAttributesCLI() {
    super();
  }

  protected void setErrOut(PrintStream errOut) {
    this.errOut = errOut;
  }

  protected AdminCommandHandler getAdminCommandHandler() {
    return new AdminCommandHandler();
  }

  protected ClientCommandHandler getClientCommandHandler() {
    return new ClientCommandHandler();
  }

  void printUsage(String cmd, boolean desc, CommandHandler... handlers)
      throws UnsupportedEncodingException {
    StringBuilder usageBuilder = new StringBuilder();
    usageBuilder.append(USAGE_YARN_NODE_ATTRIBUTES);
    boolean satisfied = false;
    for (CommandHandler cmdHandlers : handlers) {
      satisfied |= cmdHandlers.getHelp(cmd, usageBuilder, desc);
    }
    if (!satisfied) {
      printUsage(desc, handlers);
    } else {
      print(usageBuilder);
    }
  }

  private void printUsage(boolean desc, CommandHandler... handlers)
      throws UnsupportedEncodingException {
    StringBuilder usageBuilder = new StringBuilder();
    usageBuilder.append(USAGE_YARN_NODE_ATTRIBUTES);
    for (CommandHandler cmdHandlers : handlers) {
      cmdHandlers.getHelp(usageBuilder, desc);
    }

    // append help with usage
    usageBuilder.append(DEFAULT_SEPARATOR)
        .append(" -help [cmd] List help of commands");
    print(usageBuilder);
  }

  private void print(StringBuilder usageBuilder)
      throws UnsupportedEncodingException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    PrintWriter pw =
        new PrintWriter(new OutputStreamWriter(baos, Charset.forName("UTF-8")));
    pw.write(usageBuilder.toString());
    pw.close();
    errOut.println(baos.toString("UTF-8"));
  }

  private Options buildOptions(CommandHandler... handlers) {
    Options opts = new Options();
    for (CommandHandler handler : handlers) {
      Options handlerOpts = handler.getOptions();
      handlerOpts.getOptions().iterator()
          .forEachRemaining(option -> opts.addOption((Option) option));
    }
    return opts;
  }

  public int run(String[] args) throws Exception {

    int exitCode = -1;

    AdminCommandHandler adminCmdHandler = getAdminCommandHandler();
    ClientCommandHandler clientCmdHandler = getClientCommandHandler();

    // Build options
    Options opts = buildOptions(adminCmdHandler, clientCmdHandler);

    if (args.length < 1) {
      printUsage(false, adminCmdHandler, clientCmdHandler);
      return -1;
    }

    // Handle command separate
    if (handleHelpCommand(args, adminCmdHandler, clientCmdHandler)) {
      return 0;
    }

    CommandLine cliParser;
    CommandHandler handler = null;
    try {
      cliParser = new GnuParser().parse(opts, args);
      handler = adminCmdHandler.canHandleCommand(cliParser) ?
          adminCmdHandler :
          clientCmdHandler.canHandleCommand(cliParser) ?
              clientCmdHandler :
              null;
      if (handler == null) {
        errOut.println(INVALID_COMMAND_USAGE);
        printUsage(false, adminCmdHandler, clientCmdHandler);
        return exitCode;
      } else {
        return handler.handleCommand(cliParser);
      }
    } catch (UnrecognizedOptionException e) {
      errOut.println(INVALID_COMMAND_USAGE);
      printUsage(false, adminCmdHandler, clientCmdHandler);
      return exitCode;
    } catch (MissingArgumentException ex) {
      errOut.println(MISSING_ARGUMENT);
      printUsage(true, adminCmdHandler, clientCmdHandler);
      return exitCode;
    } catch (IllegalArgumentException arge) {
      errOut.println(arge.getLocalizedMessage());
      // print admin command detail
      printUsage(true, handler);
      return exitCode;
    } catch (Exception e) {
      errOut.println(e.toString());
      printUsage(true, handler);
      return exitCode;
    }
  }

  private boolean handleHelpCommand(String[] args, CommandHandler... handlers)
      throws UnsupportedEncodingException {
    if (args[0].equals("-help")) {
      if (args.length == 2) {
        printUsage(args[1], true, handlers);
      } else {
        printUsage(true, handlers);
      }
      return true;
    }
    return false;
  }

  public static void main(String[] args) throws Exception {
    int result = ToolRunner.run(new NodeAttributesCLI(), args);
    System.exit(result);
  }

  /**
   * Abstract class for command handler.
   */
  public static abstract class CommandHandler extends Configured {

    private Options options;

    private LinkedList<String> order = new LinkedList<>();
    private String header;

    protected CommandHandler(String header) {
      this(new YarnConfiguration());
      this.header = header;
    }

    protected CommandHandler(Configuration conf) {
      super(conf);
      options = buildOptions();
    }

    public boolean canHandleCommand(CommandLine parse) {
      ArrayList<Option> arrayList = new ArrayList<Option>(options.getOptions());
      return arrayList.stream().anyMatch(opt -> parse.hasOption(opt.getOpt()));
    }

    public abstract int handleCommand(CommandLine parse)
        throws IOException, YarnException;

    public abstract Options buildOptions();

    public Options getOptions() {
      return options;
    }

    public boolean getHelp(String cmd, StringBuilder strcnd, boolean addDesc) {
      Option opt = options.getOption(cmd);
      if (opt != null) {
        strcnd.append(DEFAULT_SEPARATOR).append(" -").append(opt.getOpt());
        if (opt.hasArg()) {
          strcnd.append(" <").append(opt.getArgName()).append(">");
        }
        if (addDesc) {
          strcnd.append(DEFAULT_SEPARATOR).append("\t")
              .append(opt.getDescription());
        }
      }
      return opt == null;
    }

    public void getHelp(StringBuilder builder, boolean description) {
      builder.append(DEFAULT_SEPARATOR).append(DEFAULT_SEPARATOR)
          .append(header);
      for (String option : order) {
        getHelp(option, builder, description);
      }
    }

    protected void addOrder(String key){
      order.add(key);
    }
  }

  /**
   * Client commands handler.
   */
  public static class ClientCommandHandler extends CommandHandler {

    private static final String LIST_ALL_ATTRS = "list";

    private static final String NODESTOATTR = "nodestoattributes";
    private static final String NODES = "nodes";

    private static final String ATTRTONODES = "attributestonodes";
    private static final String ATTRIBUTES = "attributes";

    public static final String SPLITPATTERN = "/";

    private static final String NODEATTRIBUTE =
        "%40s\t%10s\t%20s" + DEFAULT_SEPARATOR;
    private static final String NODEATTRIBUTEINFO =
        "%40s\t%15s" + DEFAULT_SEPARATOR;
    private static final String HOSTNAMEVAL = "%40s\t%15s" + DEFAULT_SEPARATOR;

    private PrintStream sysOut = System.out;

    public ClientCommandHandler() {
      super("Client Commands:");

    }

    public void setSysOut(PrintStream out) {
      this.sysOut = out;
    }

    @Override
    public int handleCommand(CommandLine parse)
        throws IOException, YarnException {
      if (parse.hasOption(LIST_ALL_ATTRS)) {
        return printClusterAttributes();
      } else if (parse.hasOption(NODESTOATTR)) {
        String[] nodes = new String[0];
        if (parse.hasOption(NODES)) {
          nodes = parse.getOptionValues(NODES);
        }
        return printAttributesByNode(nodes);
      } else if (parse.hasOption(ATTRTONODES)) {
        String[] attrKeys = {};
        if (parse.hasOption(ATTRIBUTES)) {
          attrKeys = parse.getOptionValues(ATTRIBUTES);
        }
        return printNodesByAttributes(attrKeys);
      }
      return 0;
    }

    protected ApplicationClientProtocol createApplicationProtocol()
        throws IOException {
      // Get the current configuration
      final YarnConfiguration conf = new YarnConfiguration(getConf());
      return ClientRMProxy.createRMProxy(conf, ApplicationClientProtocol.class);
    }

    public int printNodesByAttributes(String[] attrs)
        throws YarnException, IOException {
      ApplicationClientProtocol protocol = createApplicationProtocol();
      HashSet<NodeAttributeKey> set = new HashSet<>();

      for (String attr : attrs) {
        String[] attrFields = attr.split(SPLITPATTERN);
        if (attrFields.length == 1) {
          set.add(NodeAttributeKey.newInstance(attrFields[0]));
        } else if (attrFields.length == 2) {
          set.add(NodeAttributeKey.newInstance(attrFields[0], attrFields[1]));
        } else {
          throw new IllegalArgumentException(
              " Attribute format not correct. Should be <[prefix]/[name]> :"
                  + attr);
        }
      }

      GetAttributesToNodesRequest request =
          GetAttributesToNodesRequest.newInstance(set);
      GetAttributesToNodesResponse response =
          protocol.getAttributesToNodes(request);
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      PrintWriter writer = new PrintWriter(
          new OutputStreamWriter(baos, Charset.forName("UTF-8")));
      writer.format(HOSTNAMEVAL, "Hostname", "Attribute-value");
      response.getAttributesToNodes().forEach((attributeKey, v) -> {
        writer.println(getKeyString(attributeKey) + " :");
        v.iterator().forEachRemaining(attrVal -> writer
            .format(HOSTNAMEVAL, attrVal.getHostname(),
                attrVal.getAttributeValue()));
      });
      writer.close();
      sysOut.println(baos.toString("UTF-8"));
      return 0;
    }

    private int printAttributesByNode(String[] nodeArray)
        throws YarnException, IOException {
      ApplicationClientProtocol protocol = createApplicationProtocol();
      HashSet<String> nodes = new HashSet<>(Arrays.asList(nodeArray));
      GetNodesToAttributesRequest request =
          GetNodesToAttributesRequest.newInstance(nodes);
      GetNodesToAttributesResponse response =
          protocol.getNodesToAttributes(request);
      Map<String, Set<NodeAttribute>> nodeToAttrs =
          response.getNodeToAttributes();
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      PrintWriter writer = new PrintWriter(
          new OutputStreamWriter(baos, Charset.forName("UTF-8")));
      writer.printf(NODEATTRIBUTE, "Attribute", "Type", "Value");
      nodeToAttrs.forEach((node, v) -> {
        // print node header
        writer.println(node + ":");
        v.iterator().forEachRemaining(attr -> writer
            .format(NODEATTRIBUTE, getKeyString(attr.getAttributeKey()),
                attr.getAttributeType().name(), attr.getAttributeValue()));
      });
      writer.close();
      sysOut.println(baos.toString("UTF-8"));
      return 0;
    }

    private int printClusterAttributes() throws IOException, YarnException {
      ApplicationClientProtocol protocol = createApplicationProtocol();
      GetClusterNodeAttributesRequest request =
          GetClusterNodeAttributesRequest.newInstance();
      GetClusterNodeAttributesResponse response =
          protocol.getClusterNodeAttributes(request);
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      PrintWriter writer = new PrintWriter(
          new OutputStreamWriter(baos, Charset.forName("UTF-8")));
      writer.format(NODEATTRIBUTEINFO, "Attribute", "Type");
      for (NodeAttributeInfo attr : response.getNodeAttributes()) {
        writer.format(NODEATTRIBUTEINFO, getKeyString(attr.getAttributeKey()),
            attr.getAttributeType().name());
      }
      writer.close();
      sysOut.println(baos.toString("UTF-8"));
      return 0;
    }

    private String getKeyString(NodeAttributeKey key) {
      StringBuilder sb = new StringBuilder();
      sb.append(key.getAttributePrefix()).append("/")
          .append(key.getAttributeName());
      return sb.toString();
    }

    @Override
    public Options buildOptions() {
      Options clientOptions = new Options();
      clientOptions.addOption(
          new Option(LIST_ALL_ATTRS, false, "List all attributes in cluster"));

      // group by command
      OptionGroup nodeToAttr = new OptionGroup();
      Option attrtonodes = new Option(NODESTOATTR, false,
          "Lists all mapping to nodes to attributes");
      Option nodes = new Option(NODES,
          "Works with [" + LIST_ALL_ATTRS + "] to specify node hostnames "
              + "whose mappings are required to be displayed.");
      nodes.setValueSeparator(',');
      nodes.setArgName("Host Names");
      nodes.setArgs(Option.UNLIMITED_VALUES);
      nodeToAttr.addOption(attrtonodes);
      nodeToAttr.addOption(nodes);
      clientOptions.addOptionGroup(nodeToAttr);

      // Defines as groups to add extendability for later
      OptionGroup attrToNodes = new OptionGroup();
      attrToNodes.addOption(new Option(ATTRTONODES, false,
          "Displays mapping of "
              + "attributes to nodes and attribute values grouped by "
              + "attributes"));
      Option attrs = new Option(ATTRIBUTES, "Works with [" + ATTRTONODES
          + "] to specify attributes whose mapping "
          + "are required to be displayed.");
      attrs.setValueSeparator(',');
      attrs.setArgName("Attributes");
      attrs.setArgs(Option.UNLIMITED_VALUES);
      attrToNodes.addOption(attrs);
      clientOptions.addOptionGroup(attrToNodes);

      // DEFINE ORDER
      addOrder(LIST_ALL_ATTRS);
      addOrder(NODESTOATTR);
      addOrder(NODES);
      addOrder(ATTRTONODES);
      addOrder(ATTRIBUTES);
      return clientOptions;
    }
  }

  /**
   * Admin commands handler.
   */
  public static class AdminCommandHandler extends CommandHandler {

    private static final String ADD = "add";
    private static final String REMOVE = "remove";
    private static final String REPLACE = "replace";
    private static final String FAILUNKNOWNNODES = "failOnUnknownNodes";

    AdminCommandHandler() {
      super("Admin Commands:");
    }

    @Override
    public Options buildOptions() {
      Options adminOptions = new Options();
      Option replace = new Option(REPLACE, true,
          "Replace the node to attributes mapping information at the"
              + " ResourceManager with the new mapping. Currently"
              + " supported attribute type. And string is the default"
              + " type too. Attribute value if not specified for string"
              + " type value will be considered as empty string."
              + " Replaced node-attributes should not violate the"
              + " existing attribute to attribute type mapping.");
      replace.setArgName("\"node1:attribute[(type)][=value],attribute1[=value],"
          + "attribute2  node2:attribute2[=value],attribute3\"");
      replace.setArgs(1);
      adminOptions.addOption(replace);

      Option add = new Option(ADD, true,
          "Adds or updates the node to attributes mapping information"
              + " at the ResourceManager. Currently supported attribute"
              + " type is string. And string is the default type too."
              + " Attribute value if not specified for string type"
              + " value will be considered as empty string. Added or"
              + " updated node-attributes should not violate the"
              + " existing attribute to attribute type mapping.");
      add.setArgName("\"node1:attribute[(type)][=value],attribute1[=value],"
          + "attribute2  node2:attribute2[=value],attribute3\"");
      add.setArgs(1);
      adminOptions.addOption(add);

      Option remove = new Option(REMOVE, true,
          "Removes the specified node to attributes mapping"
              + " information at the ResourceManager");
      remove.setArgName("\"node1:attribute,attribute1 node2:attribute2\"");
      remove.setArgs(1);
      adminOptions.addOption(remove);

      adminOptions.addOption(new Option(FAILUNKNOWNNODES, false,
          "Can be used optionally along with [add,remove,replace] options. "
              + "When set, command will fail if specified nodes are unknown."));

      // DEFINE ORDER
      addOrder(REPLACE);
      addOrder(ADD);
      addOrder(REMOVE);
      addOrder(FAILUNKNOWNNODES);

      return adminOptions;
    }

    protected ResourceManagerAdministrationProtocol createAdminProtocol()
        throws IOException {
      // Get the current configuration
      final YarnConfiguration conf = new YarnConfiguration(getConf());
      return ClientRMProxy
          .createRMProxy(conf, ResourceManagerAdministrationProtocol.class);
    }

    public int handleCommand(CommandLine cliParser)
        throws IOException, YarnException {
      String operation = null;
      if (cliParser.hasOption(ADD)) {
        operation = ADD;
      } else if (cliParser.hasOption(REMOVE)) {
        operation = REMOVE;
      } else if (cliParser.hasOption(REPLACE)) {
        operation = REPLACE;
      }
      if (operation != null) {
        List<NodeToAttributes> buildNodeLabelsListFromStr =
            buildNodeLabelsListFromStr(cliParser.getOptionValue(operation),
                !operation.equals(REPLACE), operation);
        NodesToAttributesMappingRequest request =
            NodesToAttributesMappingRequest.newInstance(
                AttributeMappingOperationType.valueOf(operation.toUpperCase()),
                buildNodeLabelsListFromStr,
                cliParser.hasOption(FAILUNKNOWNNODES));
        ResourceManagerAdministrationProtocol adminProtocol =
            createAdminProtocol();
        adminProtocol.mapAttributesToNodes(request);
      } else {
        // Handle case for only failOnUnknownNodes passed
        throw new IllegalArgumentException(
            getOptions().getOption(FAILUNKNOWNNODES).getDescription());
      }
      return 0;
    }

    /**
     * args are expected to be of the format
     * node1:java(string)=8,ssd(boolean)=false node2:ssd(boolean)=true.
     */
    private List<NodeToAttributes> buildNodeLabelsListFromStr(String args,
        boolean validateForAttributes, String operation) {
      Map<String, NodeToAttributes> nodeToAttributesMap = new HashMap<>();
      for (String nodeToAttributesStr : args.split("[ \n]")) {
        // for each node to attribute mapping
        nodeToAttributesStr = nodeToAttributesStr.trim();
        if (nodeToAttributesStr.isEmpty() || nodeToAttributesStr
            .startsWith("#")) {
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

        String[] attributesStr;
        if (nodeToAttributes.length == 2) {
          // fetching multiple attributes for a node
          attributesStr = nodeToAttributes[1].split(",");
          for (String attributeStr : attributesStr) {
            // get information about each attribute.
            attributeNameValueType = attributeStr.split("="); // to find name
            // value
            Preconditions.checkArgument(
                !(attributeNameValueType[0] == null || attributeNameValueType[0]
                    .isEmpty()), "Attribute name cannot be null or empty");
            attributeValue = attributeNameValueType.length > 1 ?
                attributeNameValueType[1] :
                "";
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
            attributesList.add(NodeAttribute
                .newInstance(NodeAttribute.PREFIX_CENTRALIZED,
                    attributeName.trim(), attributeType,
                    attributeValue.trim()));
          }
        }
        if (validateForAttributes) {
          Preconditions.checkArgument((attributesList.size() > 0),
              "Attributes cannot be null or empty for Operation [" + operation
                  + "] on the node " + node);
        }
        nodeToAttributesMap
            .put(node, NodeToAttributes.newInstance(node, attributesList));
      }

      if (nodeToAttributesMap.isEmpty()) {
        throw new IllegalArgumentException(NO_MAPPING_ERR_MSG);
      }
      return Lists.newArrayList(nodeToAttributesMap.values());
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
    private Configuration addSecurityConfiguration(Configuration conf) {
      // Make a copy so we don't mutate it. Also use an YarnConfiguration to
      // force loading of yarn-site.xml.
      conf = new YarnConfiguration(conf);
      conf.set(CommonConfigurationKeys.HADOOP_SECURITY_SERVICE_USER_NAME_KEY,
          conf.get(YarnConfiguration.RM_PRINCIPAL, ""));
      return conf;
    }

  }
}
