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
package org.apache.hadoop.hdfs.tools;

import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Pattern;

import javax.management.AttributeNotFoundException;
import javax.management.MBeanAttributeInfo;
import javax.management.MBeanInfo;
import javax.management.MBeanServerConnection;
import javax.management.ObjectName;
import javax.management.ReflectionException;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.util.ExitUtil;

/**
 * tool to get data from NameNode or DataNode using MBeans currently the
 * following MBeans are available (under hadoop domain):
 * hadoop:service=NameNode,name=FSNamesystemState (static)
 * hadoop:service=NameNode,name=NameNodeActivity (dynamic)
 * hadoop:service=NameNode,name=RpcActivityForPort9000 (dynamic)
 * hadoop:service=DataNode,name=RpcActivityForPort9867 (dynamic)
 * hadoop:name=service=DataNode,FSDatasetState-UndefinedStorageId663800459
 * (static)
 * hadoop:service=DataNode,name=DataNodeActivity-UndefinedStorageId-520845215
 * (dynamic)
 * 
 * 
 * implementation note: all logging is sent to System.err (since it is a command
 * line tool)
 */
@InterfaceAudience.Private
public class JMXGet {

  private static final String format = "%s=%s%n";
  private ArrayList<ObjectName> hadoopObjectNames;
  private MBeanServerConnection mbsc;
  private String service = "NameNode", port = "", server = "localhost";
  private String localVMUrl = null;

  public JMXGet() {
  }

  public void setService(String service) {
    this.service = service;
  }

  public void setPort(String port) {
    this.port = port;
  }

  public void setServer(String server) {
    this.server = server;
  }

  public void setLocalVMUrl(String url) {
    this.localVMUrl = url;
  }

  /**
   * print all attributes' values
   */
  public void printAllValues() throws Exception {
    err("List of all the available keys:");

    Object val = null;

    for (ObjectName oname : hadoopObjectNames) {
      err(">>>>>>>>jmx name: " + oname.getCanonicalKeyPropertyListString());
      MBeanInfo mbinfo = mbsc.getMBeanInfo(oname);
      MBeanAttributeInfo[] mbinfos = mbinfo.getAttributes();

      for (MBeanAttributeInfo mb : mbinfos) {
        val = mbsc.getAttribute(oname, mb.getName());
        System.out.format(format, mb.getName(), (val==null)?"":val.toString());
      }
    }
  }

  public void printAllMatchedAttributes(String attrRegExp) throws Exception {
    err("List of the keys matching " + attrRegExp + " :");
    Object val = null;
    Pattern p = Pattern.compile(attrRegExp);
    for (ObjectName oname : hadoopObjectNames) {
      err(">>>>>>>>jmx name: " + oname.getCanonicalKeyPropertyListString());
      MBeanInfo mbinfo = mbsc.getMBeanInfo(oname);
      MBeanAttributeInfo[] mbinfos = mbinfo.getAttributes();
      for (MBeanAttributeInfo mb : mbinfos) {
        if (p.matcher(mb.getName()).lookingAt()) {
          val = mbsc.getAttribute(oname, mb.getName());
          System.out.format(format, mb.getName(), (val == null) ? "" : val.toString());
        }
      }
    }
  }

  /**
   * get single value by key
   */
  public String getValue(String key) throws Exception {

    Object val = null;

    for (ObjectName oname : hadoopObjectNames) {
      try {
        val = mbsc.getAttribute(oname, key);
      } catch (AttributeNotFoundException anfe) {
        /* just go to the next */
        continue;
      } catch (ReflectionException re) {
        if (re.getCause() instanceof NoSuchMethodException) {
          continue;
        }
      }
      err("Info: key = " + key + "; val = " +
          (val == null ? "null" : val.getClass()) + ":" + val);
      break;
    }

    return (val == null) ? "" : val.toString();
  }

  /**
   * @throws Exception
   *           initializes MBeanServer
   */
  public void init() throws Exception {

    err("init: server=" + server + ";port=" + port + ";service=" + service
        + ";localVMUrl=" + localVMUrl);

    String url_string = null;
    // build connection url
    if (localVMUrl != null) {
      // use
      // jstat -snap <vmpid> | grep sun.management.JMXConnectorServer.address
      // to get url
      url_string = localVMUrl;
      err("url string for local pid = " + localVMUrl + " = " + url_string);

    } else if (!port.isEmpty() && !server.isEmpty()) {
      // using server and port
      url_string = "service:jmx:rmi:///jndi/rmi://" + server + ":" + port
      + "/jmxrmi";
    } // else url stays null

    // Create an RMI connector client and
    // connect it to the RMI connector server

    if (url_string == null) { // assume local vm (for example for Testing)
      mbsc = ManagementFactory.getPlatformMBeanServer();
    } else {
      JMXServiceURL url = new JMXServiceURL(url_string);

      err("Create RMI connector and connect to the RMI connector server" + url);

      JMXConnector jmxc = JMXConnectorFactory.connect(url, null);
      // Get an MBeanServerConnection
      //
      err("\nGet an MBeanServerConnection");
      mbsc = jmxc.getMBeanServerConnection();
    }

    // Get domains from MBeanServer
    //
    err("\nDomains:");

    String domains[] = mbsc.getDomains();
    Arrays.sort(domains);
    for (String domain : domains) {
      err("\tDomain = " + domain);
    }

    // Get MBeanServer's default domain
    //
    err("\nMBeanServer default domain = " + mbsc.getDefaultDomain());

    // Get MBean count
    //
    err("\nMBean count = " + mbsc.getMBeanCount());

    // Query MBean names for specific domain "hadoop" and service
    ObjectName query = new ObjectName("Hadoop:service=" + service + ",*");
    hadoopObjectNames = new ArrayList<ObjectName>(5);
    err("\nQuery MBeanServer MBeans:");
    Set<ObjectName> names = new TreeSet<ObjectName>(mbsc
        .queryNames(query, null));

    for (ObjectName name : names) {
      hadoopObjectNames.add(name);
      err("Hadoop service: " + name);
    }

  }

  /**
   * Print JMXGet usage information
   */
  static void printUsage(Options opts) {
    HelpFormatter formatter = new HelpFormatter();
    formatter.printHelp("jmxget options are: ", opts);
  }

  /**
   * @param msg error message
   */
  private static void err(String msg) {
    System.err.println(msg);
  }

  /**
   * parse args
   */
  private static CommandLine parseArgs(Options opts, String... args)
  throws IllegalArgumentException {

    Option jmxService = Option.builder("service")
        .argName("NameNode|DataNode").hasArg()
        .desc("specify jmx service (NameNode by default)").build();

    Option jmxServer = Option.builder("server")
        .argName("mbean server").hasArg()
        .desc("specify mbean server (localhost by default)").build();

    Option jmxHelp = Option.builder("help").desc("print help").build();

    Option jmxPort = Option.builder("port")
        .argName("mbean server port")
        .hasArg().desc("specify mbean server port, "
        + "if missing - it will try to connect to MBean Server in the same VM").build();

    Option jmxLocalVM = Option.builder("localVM")
        .argName("VM's connector url").hasArg()
        .desc("connect to the VM on the same machine;"
        + "\n use:\n jstat -J-Djstat.showUnsupported=true -snap <vmpid> | "
        + "grep sun.management.JMXConnectorServer.address\n "
        + "to find the url").build();

    opts.addOption(jmxServer);
    opts.addOption(jmxHelp);
    opts.addOption(jmxService);
    opts.addOption(jmxPort);
    opts.addOption(jmxLocalVM);

    CommandLine commandLine = null;
    CommandLineParser parser = new GnuParser();
    try {
      commandLine = parser.parse(opts, args, true);
    } catch (ParseException e) {
      printUsage(opts);
      throw new IllegalArgumentException("invalid args: " + e.getMessage());
    }
    return commandLine;
  }

  public static void main(String[] args) {
    int res = -1;

    // parse arguments
    Options opts = new Options();
    CommandLine commandLine = null;
    try {
      commandLine = parseArgs(opts, args);
    } catch (IllegalArgumentException iae) {
      commandLine = null;
    }

    if (commandLine == null) {
      // invalid arguments
      err("Invalid args");
      printUsage(opts);
      ExitUtil.terminate(-1);      
    }

    JMXGet jm = new JMXGet();

    if (commandLine.hasOption("port")) {
      jm.setPort(commandLine.getOptionValue("port"));
    }
    if (commandLine.hasOption("service")) {
      jm.setService(commandLine.getOptionValue("service"));
    }
    if (commandLine.hasOption("server")) {
      jm.setServer(commandLine.getOptionValue("server"));
    }

    if (commandLine.hasOption("localVM")) {
      // from the file /tmp/hsperfdata*
      jm.setLocalVMUrl(commandLine.getOptionValue("localVM"));
    }

    if (commandLine.hasOption("help")) {
      printUsage(opts);
      ExitUtil.terminate(0);
    }

    // rest of args
    args = commandLine.getArgs();

    try {
      jm.init();

      if (args.length == 0) {
        jm.printAllValues();
      } else {
        for (String key : args) {
          err("key = " + key);
          String val = jm.getValue(key);
          if (val != null)
            System.out.format(JMXGet.format, key, val);
        }
      }
      res = 0;
    } catch (Exception re) {
      re.printStackTrace();
      res = -1;
    }

    ExitUtil.terminate(res);
  }
}
