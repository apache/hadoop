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
package org.apache.hadoop.tracing;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

import org.apache.commons.io.Charsets;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.ipc.ProtobufRpcEngine;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.tools.TableListing;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;

/**
 * A command-line tool for viewing and modifying tracing settings.
 */
@InterfaceAudience.Private
public class TraceAdmin extends Configured implements Tool {
  private TraceAdminProtocolPB proxy;
  private TraceAdminProtocolTranslatorPB remote;

  private void usage() {
    PrintStream err = System.err;
    err.print(
        "Hadoop tracing configuration commands:\n" +
            "  -add [-class classname] [-Ckey=value] [-Ckey2=value2] ...\n" +
            "    Add a span receiver with the provided class name.  Configuration\n" +
            "    keys for the span receiver can be specified with the -C options.\n" +
            "    The span receiver will also inherit whatever configuration keys\n" +
            "    exist in the daemon's configuration.\n" +
            "  -help: Print this help message.\n" +
            "  -host [hostname:port]\n" +
            "    Specify the hostname and port of the daemon to examine.\n" +
            "    Required for all commands.\n" +
            "  -list: List the current span receivers.\n" +
            "  -remove [id]\n" +
            "    Remove the span receiver with the specified id.  Use -list to\n" +
            "    find the id of each receiver.\n"
    );
  }

  private int listSpanReceivers(List<String> args) throws IOException {
    SpanReceiverInfo infos[] = remote.listSpanReceivers();
    if (infos.length == 0) {
      System.out.println("[no span receivers found]");
      return 0;
    }
    TableListing listing = new TableListing.Builder().
        addField("ID").
        addField("CLASS").
        showHeaders().
        build();
    for (SpanReceiverInfo info : infos) {
      listing.addRow("" + info.getId(), info.getClassName());
    }
    System.out.println(listing.toString());
    return 0;
  }

  private final static String CONFIG_PREFIX = "-C";

  private int addSpanReceiver(List<String> args) throws IOException {
    String className = StringUtils.popOptionWithArgument("-class", args);
    if (className == null) {
      System.err.println("You must specify the classname with -class.");
      return 1;
    }
    ByteArrayOutputStream configStream = new ByteArrayOutputStream();
    PrintStream configsOut = new PrintStream(configStream, false, "UTF-8");
    SpanReceiverInfoBuilder factory = new SpanReceiverInfoBuilder(className);
    String prefix = "";
    for (int i = 0; i < args.size(); ++i) {
      String str = args.get(i);
      if (!str.startsWith(CONFIG_PREFIX)) {
        System.err.println("Can't understand argument: " + str);
        return 1;
      }
      str = str.substring(CONFIG_PREFIX.length());
      int equalsIndex = str.indexOf("=");
      if (equalsIndex < 0) {
        System.err.println("Can't parse configuration argument " + str);
        System.err.println("Arguments must be in the form key=value");
        return 1;
      }
      String key = str.substring(0, equalsIndex);
      String value = str.substring(equalsIndex + 1);
      factory.addConfigurationPair(key, value);
      configsOut.print(prefix + key + " = " + value);
      prefix = ", ";
    }

    String configStreamStr = configStream.toString("UTF-8");
    try {
      long id = remote.addSpanReceiver(factory.build());
      System.out.println("Added trace span receiver " + id +
          " with configuration " + configStreamStr);
    } catch (IOException e) {
      System.out.println("addSpanReceiver error with configuration " +
                             configStreamStr);
      throw e;
    }
    return 0;
  }

  private int removeSpanReceiver(List<String> args) throws IOException {
    String indexStr = StringUtils.popFirstNonOption(args);
    long id = -1;
    try {
      id = Long.parseLong(indexStr);
    } catch (NumberFormatException e) {
      System.err.println("Failed to parse ID string " +
          indexStr + ": " + e.getMessage());
      return 1;
    }
    remote.removeSpanReceiver(id);
    System.err.println("Removed trace span receiver " + id);
    return 0;
  }

  @Override
  public int run(String argv[]) throws Exception {
    LinkedList<String> args = new LinkedList<String>();
    for (String arg : argv) {
      args.add(arg);
    }
    if (StringUtils.popOption("-h", args) ||
        StringUtils.popOption("-help", args)) {
      usage();
      return 0;
    } else if (args.size() == 0) {
      usage();
      return 0;
    }
    String hostPort = StringUtils.popOptionWithArgument("-host", args);
    if (hostPort == null) {
      System.err.println("You must specify a host with -host.");
      return 1;
    }
    if (args.size() < 0) {
      System.err.println("You must specify an operation.");
      return 1;
    }
    RPC.setProtocolEngine(getConf(), TraceAdminProtocolPB.class,
        ProtobufRpcEngine.class);
    InetSocketAddress address = NetUtils.createSocketAddr(hostPort);
    UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
    Class<?> xface = TraceAdminProtocolPB.class;
    proxy = (TraceAdminProtocolPB)RPC.getProxy(xface,
        RPC.getProtocolVersion(xface), address,
        ugi, getConf(), NetUtils.getDefaultSocketFactory(getConf()), 0);
    remote = new TraceAdminProtocolTranslatorPB(proxy);
    try {
      if (args.get(0).equals("-list")) {
        return listSpanReceivers(args.subList(1, args.size()));
      } else if (args.get(0).equals("-add")) {
        return addSpanReceiver(args.subList(1, args.size()));
      } else if (args.get(0).equals("-remove")) {
        return removeSpanReceiver(args.subList(1, args.size()));
      } else {
        System.err.println("Unrecognized tracing command: " + args.get(0));
        System.err.println("Use -help for help.");
        return 1;
      }
    } finally {
      remote.close();
    }
  }

  public static void main(String[] argv) throws Exception {
    TraceAdmin admin = new TraceAdmin();
    admin.setConf(new Configuration());
    System.exit(admin.run(argv));
  }
}
