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

import org.apache.hadoop.thirdparty.com.google.common.annotations.VisibleForTesting;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.api.client.WebResource.Builder;
import com.sun.jersey.client.urlconnection.HttpURLConnectionFactory;
import com.sun.jersey.client.urlconnection.URLConnectionClientHandler;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.MissingArgumentException;
import org.apache.commons.cli.Options;
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.security.authentication.client.AuthenticatedURL;
import org.apache.hadoop.security.ssl.SSLFactory;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.webapp.dao.ConfInfo;
import org.apache.hadoop.yarn.webapp.dao.QueueConfigInfo;
import org.apache.hadoop.yarn.webapp.dao.SchedConfUpdateInfo;
import org.apache.hadoop.yarn.webapp.util.WebAppUtils;
import org.apache.hadoop.yarn.webapp.util.YarnWebServiceUtils;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.Marshaller;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Source;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.stream.StreamResult;
import javax.xml.transform.stream.StreamSource;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response.Status;
import java.io.StringReader;
import java.io.StringWriter;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * CLI for modifying scheduler configuration.
 */
@Public
@Unstable
public class SchedConfCLI extends Configured implements Tool {

  private static final String ADD_QUEUES_OPTION = "addQueues";
  private static final String REMOVE_QUEUES_OPTION = "removeQueues";
  private static final String UPDATE_QUEUES_OPTION = "updateQueues";
  private static final String GLOBAL_OPTIONS = "globalUpdates";
  private static final String GET_SCHEDULER_CONF = "getConf";
  private static final String FORMAT_CONF = "formatConfig";
  private static final String HELP_CMD = "help";
  private static final String SPLIT_BY_SLASH_COMMA = "(?<!\\\\)\\,";

  private static final String CONF_ERR_MSG = "Specify configuration key " +
      "value as confKey=confVal.";

  private SSLFactory sslFactory;
  private Client client;

  public SchedConfCLI() {
    super(new YarnConfiguration());
  }

  public static void main(String[] args) throws Exception {
    SchedConfCLI cli = new SchedConfCLI();
    int exitCode = cli.run(args);
    System.exit(exitCode);
  }

  @Override
  public int run(String[] args) throws Exception {
    Options opts = new Options();

    opts.addOption("add", ADD_QUEUES_OPTION, true,
        "Add queues with configurations");
    opts.addOption("remove", REMOVE_QUEUES_OPTION, true,
        "Remove queues");
    opts.addOption("update", UPDATE_QUEUES_OPTION, true,
        "Update queue configurations");
    opts.addOption("global", GLOBAL_OPTIONS, true,
        "Update global scheduler configurations");
    opts.addOption("getconf", GET_SCHEDULER_CONF, false,
        "Get current scheduler configurations");
    opts.addOption("format", FORMAT_CONF, false,
        "Format Scheduler Configuration and reload from" +
        " capacity-scheduler.xml");
    opts.addOption("h", HELP_CMD, false, "Displays help for all commands.");

    int exitCode = -1;
    CommandLine parsedCli = null;
    try {
      parsedCli = new GnuParser().parse(opts, args);
    } catch (MissingArgumentException ex) {
      System.err.println("Missing argument for options");
      printUsage();
      return exitCode;
    }

    if (parsedCli.hasOption(HELP_CMD)) {
      printUsage();
      return 0;
    }

    boolean hasOption = false;
    boolean format = false;
    boolean getConf = false;
    SchedConfUpdateInfo updateInfo = new SchedConfUpdateInfo();
    try {
      if (parsedCli.hasOption(ADD_QUEUES_OPTION)) {
        hasOption = true;
        addQueues(parsedCli.getOptionValue(ADD_QUEUES_OPTION), updateInfo);
      }
      if (parsedCli.hasOption(REMOVE_QUEUES_OPTION)) {
        hasOption = true;
        removeQueues(parsedCli.getOptionValue(REMOVE_QUEUES_OPTION),
            updateInfo);
      }
      if (parsedCli.hasOption(UPDATE_QUEUES_OPTION)) {
        hasOption = true;
        updateQueues(parsedCli.getOptionValue(UPDATE_QUEUES_OPTION),
            updateInfo);
      }
      if (parsedCli.hasOption(GLOBAL_OPTIONS)) {
        hasOption = true;
        globalUpdates(parsedCli.getOptionValue(GLOBAL_OPTIONS), updateInfo);
      }
      if (parsedCli.hasOption((FORMAT_CONF))) {
        hasOption = true;
        format = true;
      }
      if (parsedCli.hasOption(GET_SCHEDULER_CONF)) {
        hasOption = true;
        getConf = true;
      }

    } catch (IllegalArgumentException e) {
      System.err.println(e.getMessage());
      return -1;
    }

    if (!hasOption) {
      System.err.println("Invalid Command Usage: ");
      printUsage();
      return -1;
    }

    Configuration conf = getConf();
    if (format) {
      return WebAppUtils.execOnActiveRM(conf, this::formatSchedulerConf, null);
    } else if (getConf) {
      return WebAppUtils.execOnActiveRM(conf, this::getSchedulerConf, null);
    } else {
      return WebAppUtils.execOnActiveRM(conf,
          this::updateSchedulerConfOnRMNode, updateInfo);
    }
  }

  private static void prettyFormatWithIndent(String input, int indent)
      throws Exception {
    Source xmlInput = new StreamSource(new StringReader(input));
    StringWriter sw = new StringWriter();
    StreamResult xmlOutput = new StreamResult(sw);
    TransformerFactory transformerFactory = TransformerFactory.newInstance();
    transformerFactory.setAttribute("indent-number", indent);
    Transformer transformer = transformerFactory.newTransformer();
    transformer.setOutputProperty(OutputKeys.INDENT, "yes");
    transformer.transform(xmlInput, xmlOutput);
    System.out.println(xmlOutput.getWriter().toString());
  }

  private WebResource initializeWebResource(String webAppAddress) {
    Configuration conf = getConf();
    if (YarnConfiguration.useHttps(conf)) {
      sslFactory = new SSLFactory(SSLFactory.Mode.CLIENT, conf);
    }
    client = createWebServiceClient(sslFactory);
    return client.resource(webAppAddress);
  }

  private void destroyClient() {
    if (client != null) {
      client.destroy();
    }
    if (sslFactory != null) {
      sslFactory.destroy();
    }
  }

  @VisibleForTesting
  int getSchedulerConf(String webAppAddress, WebResource resource)
      throws Exception {
    ClientResponse response = null;
    resource = (resource != null) ? resource :
        initializeWebResource(webAppAddress);
    try {
      Builder builder;
      if (UserGroupInformation.isSecurityEnabled()) {
        builder = resource
            .path("ws").path("v1").path("cluster")
            .path("scheduler-conf").accept(MediaType.APPLICATION_XML);
      } else {
        builder = resource
            .path("ws").path("v1").path("cluster").path("scheduler-conf")
            .queryParam("user.name", UserGroupInformation.getCurrentUser()
            .getShortUserName()).accept(MediaType.APPLICATION_XML);
      }
      response = builder.get(ClientResponse.class);
      if (response != null) {
        if (response.getStatus() == Status.OK.getStatusCode()) {
          ConfInfo schedulerConf = response.getEntity(ConfInfo.class);
          JAXBContext jaxbContext = JAXBContext.newInstance(ConfInfo.class);
          Marshaller jaxbMarshaller = jaxbContext.createMarshaller();
          StringWriter sw = new StringWriter();
          jaxbMarshaller.marshal(schedulerConf, sw);
          prettyFormatWithIndent(sw.toString(), 2);
          return 0;
        } else {
          System.err.println("Failed to get scheduler configuration: "
              + response.getEntity(String.class));
        }
      } else {
        System.err.println("Failed to get scheduler configuration: " +
            "null response");
      }
      return -1;
    } finally {
      if (response != null) {
        response.close();
      }
      destroyClient();
    }
  }

  @VisibleForTesting
  int formatSchedulerConf(String webAppAddress, WebResource resource)
      throws Exception {
    ClientResponse response = null;
    resource = (resource != null) ? resource :
        initializeWebResource(webAppAddress);
    try {
      Builder builder;
      if (UserGroupInformation.isSecurityEnabled()) {
        builder = resource
            .path("ws").path("v1").path("cluster")
            .path("/scheduler-conf/format")
            .accept(MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON);
      } else {
        builder = resource
            .path("ws").path("v1").path("cluster")
            .path("/scheduler-conf/format").queryParam("user.name",
            UserGroupInformation.getCurrentUser().getShortUserName())
            .accept(MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON);
      }

      response = builder.get(ClientResponse.class);
      if (response != null) {
        if (response.getStatus() == Status.OK.getStatusCode()) {
          System.out.println(response.getEntity(String.class));
          return 0;
        } else {
          System.err.println("Failed to format scheduler configuration: " +
              response.getEntity(String.class));
        }
      } else {
        System.err.println("Failed to format scheduler configuration: " +
            "null response");
      }
      return -1;
    } finally {
      if (response != null) {
        response.close();
      }
      destroyClient();
    }
  }

  @VisibleForTesting
  int updateSchedulerConfOnRMNode(String webAppAddress,
      SchedConfUpdateInfo updateInfo) throws Exception {
    ClientResponse response = null;
    WebResource resource = initializeWebResource(webAppAddress);
    try {
      Builder builder = null;
      if (UserGroupInformation.isSecurityEnabled()) {
        builder = resource.path("ws").path("v1").path("cluster")
            .path("scheduler-conf").accept(MediaType.APPLICATION_JSON);
      } else {
        builder = resource.path("ws").path("v1").path("cluster")
            .queryParam("user.name",
            UserGroupInformation.getCurrentUser().getShortUserName())
            .path("scheduler-conf").accept(MediaType.APPLICATION_JSON);
      }

      builder.entity(YarnWebServiceUtils.toJson(updateInfo,
          SchedConfUpdateInfo.class), MediaType.APPLICATION_JSON);
      response = builder.put(ClientResponse.class);
      if (response != null) {
        if (response.getStatus() == Status.OK.getStatusCode()) {
          System.out.println("Configuration changed successfully.");
          return 0;
        } else {
          System.err.println("Configuration change unsuccessful: "
              + response.getEntity(String.class));
        }
      } else {
        System.err.println("Configuration change unsuccessful: null response");
      }
      return -1;
    } finally {
      if (response != null) {
        response.close();
      }
      destroyClient();
    }
  }

  private Client createWebServiceClient(SSLFactory clientSslFactory) {
    Client webServiceClient = new Client(new URLConnectionClientHandler(
        new HttpURLConnectionFactory() {
        @Override
        public HttpURLConnection getHttpURLConnection(URL url)
            throws IOException {
          AuthenticatedURL.Token token = new AuthenticatedURL.Token();
          AuthenticatedURL aUrl;
          HttpURLConnection conn = null;
          try {
            if (clientSslFactory != null) {
              clientSslFactory.init();
              aUrl = new AuthenticatedURL(null, clientSslFactory);
            } else {
              aUrl = new AuthenticatedURL();
            }
            conn = aUrl.openConnection(url, token);
          } catch (Exception e) {
            throw new IOException(e);
          }
          return conn;
        }
      }));
    webServiceClient.setChunkedEncodingSize(null);
    return webServiceClient;
  }


  @VisibleForTesting
  void addQueues(String args, SchedConfUpdateInfo updateInfo) {
    if (args == null) {
      return;
    }
    ArrayList<QueueConfigInfo> queueConfigInfos = new ArrayList<>();
    for (String arg : args.split(";")) {
      queueConfigInfos.add(getQueueConfigInfo(arg));
    }
    updateInfo.setAddQueueInfo(queueConfigInfos);
  }

  @VisibleForTesting
  void removeQueues(String args, SchedConfUpdateInfo updateInfo) {
    if (args == null) {
      return;
    }
    List<String> queuesToRemove = Arrays.asList(args.split(";"));
    updateInfo.setRemoveQueueInfo(new ArrayList<>(queuesToRemove));
  }

  @VisibleForTesting
  void updateQueues(String args, SchedConfUpdateInfo updateInfo) {
    if (args == null) {
      return;
    }
    ArrayList<QueueConfigInfo> queueConfigInfos = new ArrayList<>();
    for (String arg : args.split(";")) {
      queueConfigInfos.add(getQueueConfigInfo(arg));
    }
    updateInfo.setUpdateQueueInfo(queueConfigInfos);
  }

  @VisibleForTesting
  void globalUpdates(String args, SchedConfUpdateInfo updateInfo) {
    if (args == null) {
      return;
    }
    HashMap<String, String> globalUpdates = new HashMap<>();
    for (String globalUpdate : args.split(SPLIT_BY_SLASH_COMMA)) {
      globalUpdate = globalUpdate.replace("\\", "");
      putKeyValuePair(globalUpdates, globalUpdate);
    }
    updateInfo.setGlobalParams(globalUpdates);
  }

  private QueueConfigInfo getQueueConfigInfo(String arg) {
    String[] args = arg.split(":");
    String queuePath = args[0];
    Map<String, String> queueConfigs = new HashMap<>();
    if (args.length > 1) {
      String[] queueArgs = args[1].split(SPLIT_BY_SLASH_COMMA);
      for (int i = 0; i < queueArgs.length; ++i) {
        queueArgs[i] = queueArgs[i].replace("\\", "");
        putKeyValuePair(queueConfigs, queueArgs[i]);
      }
    }
    return new QueueConfigInfo(queuePath, queueConfigs);
  }

  private void putKeyValuePair(Map<String, String> kv, String args) {
    String[] argParts = args.split("=");
    if (argParts.length == 1) {
      if (argParts[0].isEmpty() || !args.contains("=")) {
        throw new IllegalArgumentException(CONF_ERR_MSG);
      } else {
        // key specified, but no value e.g. "confKey="
        kv.put(argParts[0], null);
      }
    } else if (argParts.length > 2) {
      throw new IllegalArgumentException(CONF_ERR_MSG);
    } else {
      if (argParts[0].isEmpty()) {
        throw new IllegalArgumentException(CONF_ERR_MSG);
      }
      kv.put(argParts[0], argParts[1]);
    }
  }

  private void printUsage() {
    System.out.println("yarn schedulerconf [-add "
        + "\"queueAddPath1:confKey1=confVal1,confKey2=confVal2;"
        + "queueAddPath2:confKey3=confVal3\"] "
        + "[-remove \"queueRemovePath1;queueRemovePath2\"] "
        + "[-update \"queueUpdatePath1:confKey1=confVal1\"] "
        + "[-global globalConfKey1=globalConfVal1,"
        + "globalConfKey2=globalConfVal2] "
        + "[-format] "
        + "[-getconf]\n"
        + "Example (adding queues): yarn schedulerconf -add "
        + "\"root.a.a1:capacity=100,maximum-capacity=100;root.a.a2:capacity=0,"
        + "maximum-capacity=0\"\n"
        + "Example (adding queues with comma in value): yarn schedulerconf "
        + "-add \"root.default:acl_administer_queue=user1\\,user2 group1\\,"
        + "group2,maximum-capacity=100;root.a.a2:capacity=0\"\n"
        + "Example (removing queues): yarn schedulerconf -remove \"root.a.a1;"
        + "root.a.a2\"\n"
        + "Example (updating queues): yarn schedulerconf -update \"root.a.a1"
        + ":capacity=25,maximum-capacity=25;root.a.a2:capacity=75,"
        + "maximum-capacity=75\"\n"
        + "Example (updating queues with comma in value): yarn schedulerconf "
        + "-update \"root.default:acl_administer_queue=user1\\,user2 group1\\,"
        + "group2,maximum-capacity=25;root.a.a2:capacity=75\"\n"
        + "Example (global scheduler update): yarn schedulerconf "
        + "-global yarn.scheduler.capacity.maximum-applications=10000\n"
        + "Example (global scheduler update with comma in value): yarn "
        + "schedulerconf "
        + "-global \"acl_administer_queue=user1\\,user2 group1\\,group2\"\n"
        + "Example (format scheduler configuration): yarn schedulerconf "
        + "-format\n"
        + "Example (get scheduler configuration): yarn schedulerconf "
        + "-getconf\n"
        + "Note: This is an alpha feature, the syntax/options are subject to "
        + "change, please run at your own risk.");
  }
}
