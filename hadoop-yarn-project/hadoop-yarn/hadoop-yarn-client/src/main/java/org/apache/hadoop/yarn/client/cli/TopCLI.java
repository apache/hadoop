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
import java.io.InputStream;
import java.net.ConnectException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLSocketFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.commons.lang3.time.DurationFormatUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.http.HttpConfig.Policy;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authentication.client.AuthenticatedURL;
import org.apache.hadoop.security.authentication.client.KerberosAuthenticator;
import org.apache.hadoop.security.ssl.SSLFactory;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationsRequest;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.QueueInfo;
import org.apache.hadoop.yarn.api.records.QueueStatistics;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.api.records.YarnClusterMetrics;
import org.apache.hadoop.yarn.conf.HAUtil;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.webapp.util.WebAppUtils;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TopCLI extends YarnCLI {

  private static final String CLUSTER_INFO_URL = "/ws/v1/cluster/info";

  private static final Logger LOG = LoggerFactory
          .getLogger(TopCLI.class);
  private String CLEAR = "\u001b[2J";
  private String CLEAR_LINE = "\u001b[2K";
  private String SET_CURSOR_HOME = "\u001b[H";
  private String CHANGE_BACKGROUND = "\u001b[7m";
  private String RESET_BACKGROUND = "\u001b[0m";
  private String SET_CURSOR_LINE_7_COLUMN_0 = "\u001b[7;0f";

  // guava cache for getapplications call
  protected Cache<GetApplicationsRequest, List<ApplicationReport>>
      applicationReportsCache = CacheBuilder.newBuilder().maximumSize(1000)
        .expireAfterWrite(5, TimeUnit.SECONDS).build();

  enum DisplayScreen {
    TOP, HELP, SORT, FIELDS
  }

  enum Columns { // in the order in which they should be displayed
    APPID, USER, TYPE, QUEUE, PRIORITY, CONT, RCONT, VCORES, RVCORES, MEM,
    RMEM, VCORESECS, MEMSECS, PROGRESS, TIME, NAME
  }

  static class ColumnInformation {
    String header;
    String format;
    boolean display; // should we show this field or not
    String description;
    String key; // key to press for sorting/toggling field

    public ColumnInformation(String header, String format, boolean display,
        String description, String key) {
      this.header = header;
      this.format = format;
      this.display = display;
      this.description = description;
      this.key = key;
    }
  }

  private static class ApplicationInformation {
    final String appid;
    final String user;
    final String type;
    final int priority;
    final int usedContainers;
    final int reservedContainers;
    final long usedMemory;
    final long reservedMemory;
    final int usedVirtualCores;
    final int reservedVirtualCores;
    final int attempts;
    final float progress;
    final String state;
    long runningTime;
    final String time;
    final String name;
    final int nodes;
    final String queue;
    final long memorySeconds;
    final long vcoreSeconds;

    final EnumMap<Columns, String> displayStringsMap;

    ApplicationInformation(ApplicationReport appReport) {
      displayStringsMap = new EnumMap<>(Columns.class);
      appid = appReport.getApplicationId().toString();
      displayStringsMap.put(Columns.APPID, appid);
      user = appReport.getUser();
      displayStringsMap.put(Columns.USER, user);
      type = appReport.getApplicationType().toLowerCase();
      displayStringsMap.put(Columns.TYPE, type);
      state = appReport.getYarnApplicationState().toString().toLowerCase();
      name = appReport.getName();
      displayStringsMap.put(Columns.NAME, name);
      queue = appReport.getQueue();
      displayStringsMap.put(Columns.QUEUE, queue);
      Priority appPriority = appReport.getPriority();
      priority = null != appPriority ? appPriority.getPriority() : 0;
      displayStringsMap.put(Columns.PRIORITY, String.valueOf(priority));
      usedContainers =
          appReport.getApplicationResourceUsageReport().getNumUsedContainers();
      displayStringsMap.put(Columns.CONT, String.valueOf(usedContainers));
      reservedContainers =
          appReport.getApplicationResourceUsageReport()
            .getNumReservedContainers();
      displayStringsMap.put(Columns.RCONT, String.valueOf(reservedContainers));
      usedVirtualCores =
          appReport.getApplicationResourceUsageReport().getUsedResources()
            .getVirtualCores();
      displayStringsMap.put(Columns.VCORES, String.valueOf(usedVirtualCores));
      usedMemory =
          appReport.getApplicationResourceUsageReport().getUsedResources()
            .getMemorySize() / 1024;
      displayStringsMap.put(Columns.MEM, String.valueOf(usedMemory) + "G");
      reservedVirtualCores =
          appReport.getApplicationResourceUsageReport().getReservedResources()
            .getVirtualCores();
      displayStringsMap.put(Columns.RVCORES,
          String.valueOf(reservedVirtualCores));
      reservedMemory =
          appReport.getApplicationResourceUsageReport().getReservedResources()
            .getMemorySize() / 1024;
      displayStringsMap.put(Columns.RMEM, String.valueOf(reservedMemory) + "G");
      attempts = appReport.getCurrentApplicationAttemptId().getAttemptId();
      nodes = 0;
      runningTime = Time.now() - appReport.getStartTime();
      time = DurationFormatUtils.formatDuration(runningTime, "dd:HH:mm");
      displayStringsMap.put(Columns.TIME, String.valueOf(time));
      progress = appReport.getProgress() * 100;
      displayStringsMap.put(Columns.PROGRESS, String.format("%.2f", progress));
      // store in GBSeconds
      memorySeconds =
          appReport.getApplicationResourceUsageReport().getMemorySeconds() / 1024;
      displayStringsMap.put(Columns.MEMSECS, String.valueOf(memorySeconds));
      vcoreSeconds =
          appReport.getApplicationResourceUsageReport().getVcoreSeconds();
      displayStringsMap.put(Columns.VCORESECS, String.valueOf(vcoreSeconds));
    }
  }

  // all the sort comparators

  public static final Comparator<ApplicationInformation> AppIDComparator =
      new Comparator<ApplicationInformation>() {
        @Override
        public int
            compare(ApplicationInformation a1, ApplicationInformation a2) {
          return a1.appid.compareTo(a2.appid);
        }
      };
  public static final Comparator<ApplicationInformation> UserComparator =
      new Comparator<ApplicationInformation>() {
        @Override
        public int
            compare(ApplicationInformation a1, ApplicationInformation a2) {
          return a1.user.compareTo(a2.user);
        }
      };
  public static final Comparator<ApplicationInformation> AppTypeComparator =
      new Comparator<ApplicationInformation>() {
        @Override
        public int
            compare(ApplicationInformation a1, ApplicationInformation a2) {
          return a1.type.compareTo(a2.type);
        }
      };
  public static final Comparator<ApplicationInformation> QueueNameComparator =
      new Comparator<ApplicationInformation>() {
        @Override
        public int
            compare(ApplicationInformation a1, ApplicationInformation a2) {
          return a1.queue.compareTo(a2.queue);
        }
      };
  public static final Comparator<ApplicationInformation> UsedContainersComparator =
      new Comparator<ApplicationInformation>() {
        @Override
        public int
            compare(ApplicationInformation a1, ApplicationInformation a2) {
          return a1.usedContainers - a2.usedContainers;
        }
      };
  public static final Comparator<ApplicationInformation> ReservedContainersComparator =
      new Comparator<ApplicationInformation>() {
        @Override
        public int
            compare(ApplicationInformation a1, ApplicationInformation a2) {
          return a1.reservedContainers - a2.reservedContainers;
        }
      };
  public static final Comparator<ApplicationInformation> UsedMemoryComparator =
      new Comparator<ApplicationInformation>() {
        @Override
        public int
            compare(ApplicationInformation a1, ApplicationInformation a2) {
          return Long.compare(a1.usedMemory, a2.usedMemory);
        }
      };
  public static final Comparator<ApplicationInformation> ReservedMemoryComparator =
      new Comparator<ApplicationInformation>() {
        @Override
        public int
            compare(ApplicationInformation a1, ApplicationInformation a2) {
          return Long.compare(a1.reservedMemory, a2.reservedMemory);
        }
      };
  public static final Comparator<ApplicationInformation> UsedVCoresComparator =
      new Comparator<ApplicationInformation>() {
        @Override
        public int
            compare(ApplicationInformation a1, ApplicationInformation a2) {
          return a1.usedVirtualCores - a2.usedVirtualCores;
        }
      };
  public static final Comparator<ApplicationInformation> ReservedVCoresComparator =
      new Comparator<ApplicationInformation>() {
        @Override
        public int
            compare(ApplicationInformation a1, ApplicationInformation a2) {
          return a1.reservedVirtualCores - a2.reservedVirtualCores;
        }
      };
  public static final Comparator<ApplicationInformation> VCoreSecondsComparator =
      new Comparator<ApplicationInformation>() {
        @Override
        public int
            compare(ApplicationInformation a1, ApplicationInformation a2) {
          return Long.compare(a1.vcoreSeconds, a2.vcoreSeconds);
        }
      };
  public static final Comparator<ApplicationInformation> MemorySecondsComparator =
      new Comparator<ApplicationInformation>() {
        @Override
        public int
            compare(ApplicationInformation a1, ApplicationInformation a2) {
          return Long.compare(a1.memorySeconds, a2.memorySeconds);
        }
      };
  public static final Comparator<ApplicationInformation> ProgressComparator =
      new Comparator<ApplicationInformation>() {
        @Override
        public int
            compare(ApplicationInformation a1, ApplicationInformation a2) {
          return Float.compare(a1.progress, a2.progress);
        }
      };
  public static final Comparator<ApplicationInformation> RunningTimeComparator =
      new Comparator<ApplicationInformation>() {
        @Override
        public int
            compare(ApplicationInformation a1, ApplicationInformation a2) {
          return Long.compare(a1.runningTime, a2.runningTime);
        }
      };
  public static final Comparator<ApplicationInformation> AppNameComparator =
      new Comparator<ApplicationInformation>() {
        @Override
        public int
            compare(ApplicationInformation a1, ApplicationInformation a2) {
          return a1.name.compareTo(a2.name);
        }
      };
  public static final Comparator<ApplicationInformation> AppPriorityComparator =
      new Comparator<ApplicationInformation>() {
        @Override
        public int compare(ApplicationInformation a1,
            ApplicationInformation a2) {
          return a1.priority - a2.priority;
        }
      };

  private static class NodesInformation {
    int totalNodes;
    int runningNodes;
    int unhealthyNodes;
    int decommissionedNodes;
    int lostNodes;
    int rebootedNodes;
  }

  private static class QueueMetrics {
    long appsSubmitted;
    long appsRunning;
    long appsPending;
    long appsCompleted;
    long appsKilled;
    long appsFailed;
    long activeUsers;
    long availableMemoryGB;
    long allocatedMemoryGB;
    long pendingMemoryGB;
    long reservedMemoryGB;
    long availableVCores;
    long allocatedVCores;
    long pendingVCores;
    long reservedVCores;
    long allocatedContainers;
    long reservedContainers;
    long pendingContainers;
  }

  private class KeyboardMonitor extends Thread {

    public void run() {
      Scanner keyboard = new Scanner(System.in, "UTF-8");
      while (runKeyboardMonitor.get()) {
        String in = keyboard.next();
        try {
          if (displayScreen == DisplayScreen.SORT) {
            handleSortScreenKeyPress(in);
          } else if (displayScreen == DisplayScreen.TOP) {
            handleTopScreenKeyPress(in);
          } else if (displayScreen == DisplayScreen.FIELDS) {
            handleFieldsScreenKeyPress(in);
          } else {
            handleHelpScreenKeyPress();
          }
        } catch (Exception e) {
          LOG.error("Caught exception", e);
        }
      }
    }
  }

  long refreshPeriod = 3 * 1000;
  int terminalWidth = -1;
  int terminalHeight = -1;
  String appsHeader;
  boolean ascendingSort;
  long rmStartTime;
  Comparator<ApplicationInformation> comparator;
  Options opts;
  CommandLine cliParser;

  Set<String> queues;
  Set<String> users;
  Set<String> types;

  DisplayScreen displayScreen;
  AtomicBoolean showingTopScreen;
  AtomicBoolean runMainLoop;
  AtomicBoolean runKeyboardMonitor;
  final Object lock = new Object();

  String currentSortField;

  Map<String, Columns> keyFieldsMap;
  List<String> sortedKeys;

  Thread displayThread;

  final EnumMap<Columns, ColumnInformation> columnInformationEnumMap;

  public TopCLI() throws IOException, InterruptedException {
    super();
    queues = new HashSet<>();
    users = new HashSet<>();
    types = new HashSet<>();
    comparator = UsedContainersComparator;
    ascendingSort = false;
    displayScreen = DisplayScreen.TOP;
    showingTopScreen = new AtomicBoolean();
    showingTopScreen.set(true);
    currentSortField = "c";
    keyFieldsMap = new HashMap<>();
    runKeyboardMonitor = new AtomicBoolean();
    runMainLoop = new AtomicBoolean();
    runKeyboardMonitor.set(true);
    runMainLoop.set(true);
    displayThread = Thread.currentThread();
    columnInformationEnumMap = new EnumMap<>(Columns.class);
    generateColumnInformationMap();
    generateKeyFieldsMap();
    sortedKeys = new ArrayList<>(keyFieldsMap.keySet());
    Collections.sort(sortedKeys);
    setTerminalSequences();
  }

  public static void main(String[] args) throws Exception {
    TopCLI topImp = new TopCLI();
    topImp.setSysOutPrintStream(System.out);
    topImp.setSysErrPrintStream(System.err);
    int res = ToolRunner.run(topImp, args);
    topImp.stop();
    System.exit(res);
  }

  @Override
  public int run(String[] args) throws Exception {
    try {
      parseOptions(args);
      if (cliParser.hasOption("help")) {
        printUsage();
        return 0;
      }
    } catch (Exception e) {
      LOG.error("Unable to parse options", e);
      return 1;
    }
    createAndStartYarnClient();
    setAppsHeader();

    Thread keyboardMonitor = new KeyboardMonitor();
    keyboardMonitor.start();

    rmStartTime = getRMStartTime();
    clearScreen();

    while (runMainLoop.get()) {
      if (displayScreen == DisplayScreen.TOP) {
        showTopScreen();
        try {
          Thread.sleep(refreshPeriod);
        } catch (InterruptedException ie) {
          break;
        }
      } else if (displayScreen == DisplayScreen.SORT) {
        showSortScreen();
        Thread.sleep(100);
      } else if (displayScreen == DisplayScreen.FIELDS) {
        showFieldsScreen();
        Thread.sleep(100);
      }
      if (rmStartTime == -1) {
        // we were unable to get it the first time, try again
        rmStartTime = getRMStartTime();
      }
    }
    clearScreen();
    return 0;
  }

  private void parseOptions(String[] args) throws ParseException, IOException,
      InterruptedException {

    // Command line options
    opts = new Options();
    opts.addOption("queues", true,
      "Comma separated list of queues to restrict applications");
    opts.addOption("users", true,
      "Comma separated list of users to restrict applications");
    opts.addOption("types", true, "Comma separated list of types to restrict"
        + " applications, case sensitive(though the display is lower case)");
    opts.addOption("cols", true, "Number of columns on the terminal");
    opts.addOption("rows", true, "Number of rows on the terminal");
    opts.addOption("help", false,
      "Print usage; for help while the tool is running press 'h' + Enter");
    opts.addOption("delay", true,
      "The refresh delay(in seconds), default is 3 seconds");

    cliParser = new GnuParser().parse(opts, args);
    if (cliParser.hasOption("queues")) {
      String clqueues = cliParser.getOptionValue("queues");
      String[] queuesArray = clqueues.split(",");
      queues.addAll(Arrays.asList(queuesArray));
    }

    if (cliParser.hasOption("users")) {
      String clusers = cliParser.getOptionValue("users");
      users.addAll(Arrays.asList(clusers.split(",")));
    }

    if (cliParser.hasOption("types")) {
      String cltypes = cliParser.getOptionValue("types");
      types.addAll(Arrays.asList(cltypes.split(",")));
    }

    if (cliParser.hasOption("cols")) {
      terminalWidth = Integer.parseInt(cliParser.getOptionValue("cols"));
    } else {
      setTerminalWidth();
    }

    if (cliParser.hasOption("rows")) {
      terminalHeight = Integer.parseInt(cliParser.getOptionValue("rows"));
    } else {
      setTerminalHeight();
    }

    if (cliParser.hasOption("delay")) {
      int delay = Integer.parseInt(cliParser.getOptionValue("delay"));
      if (delay < 1) {
        LOG.warn("Delay set too low, using default");
      } else {
        refreshPeriod = delay * 1000;
      }
    }
  }

  private void printUsage() {
    new HelpFormatter().printHelp("yarn top", opts);
    System.out.println("");
    System.out.println("'yarn top' is a tool to help cluster administrators"
        + " understand cluster usage better.");
    System.out.println("Some notes about the implementation:");
    System.out.println("  1. Fetching information for all the apps is an"
        + " expensive call for the RM.");
    System.out.println("     To prevent a performance degradation, the results"
        + " are cached for 5 seconds,");
    System.out.println("     irrespective of the delay value. Information about"
        + " the NodeManager(s) and queue");
    System.out.println("     utilization stats are fetched at the specified"
        + " delay interval. Once we have a");
    System.out.println("     better understanding of the performance impact,"
        + " this might change.");
    System.out.println("  2. Since the tool is implemented in Java, you must"
        + " hit Enter for key presses to");
    System.out.println("     be processed.");
  }

  private void setAppsHeader() {
    List<String> formattedStrings = new ArrayList<>();
    for (EnumMap.Entry<Columns, ColumnInformation> entry :
        columnInformationEnumMap.entrySet()) {
      if (entry.getValue().display) {
        formattedStrings.add(String.format(entry.getValue().format,
          entry.getValue().header));
      }
    }
    appsHeader = StringUtils.join(formattedStrings.toArray(), " ");
    if (appsHeader.length() > terminalWidth) {
      appsHeader =
          appsHeader.substring(0, terminalWidth
              - System.lineSeparator().length());
    } else {
      appsHeader +=
          StringUtils.repeat(" ", terminalWidth - appsHeader.length()
              - System.lineSeparator().length());
    }
    appsHeader += System.lineSeparator();
  }

  private void setTerminalWidth() throws IOException, InterruptedException {
    if (terminalWidth != -1) {
      return;
    }
    String[] command = { "tput", "cols" };
    String op = getCommandOutput(command).trim();
    try {
      terminalWidth = Integer.parseInt(op);
    } catch (NumberFormatException ne) {
      LOG.warn("Couldn't determine terminal width, setting to 80", ne);
      terminalWidth = 80;
    }
  }

  private void setTerminalHeight() throws IOException, InterruptedException {
    if (terminalHeight != -1) {
      return;
    }
    String[] command = { "tput", "lines" };
    String op = getCommandOutput(command).trim();
    try {
      terminalHeight = Integer.parseInt(op);
    } catch (NumberFormatException ne) {
      LOG.warn("Couldn't determine terminal height, setting to 24", ne);
      terminalHeight = 24;
    }
  }

  protected void setTerminalSequences() throws IOException,
      InterruptedException {
    String[] tput_cursor_home = { "tput", "cup", "0", "0" };
    String[] tput_clear = { "tput", "clear" };
    String[] tput_clear_line = { "tput", "el" };
    String[] tput_set_cursor_line_7_column_0 = { "tput", "cup", "6", "0" };
    String[] tput_change_background = { "tput", "smso" };
    String[] tput_reset_background = { "tput", "rmso" };
    SET_CURSOR_HOME = getCommandOutput(tput_cursor_home);
    CLEAR = getCommandOutput(tput_clear);
    CLEAR_LINE = getCommandOutput(tput_clear_line);
    SET_CURSOR_LINE_7_COLUMN_0 =
        getCommandOutput(tput_set_cursor_line_7_column_0);
    CHANGE_BACKGROUND = getCommandOutput(tput_change_background);
    RESET_BACKGROUND = getCommandOutput(tput_reset_background);
  }

  private void generateColumnInformationMap() {
    columnInformationEnumMap.put(Columns.APPID, new ColumnInformation(
      "APPLICATIONID", "%31s", true, "Application Id", "a"));
    columnInformationEnumMap.put(Columns.USER, new ColumnInformation("USER",
      "%-10s", true, "Username", "u"));
    columnInformationEnumMap.put(Columns.TYPE, new ColumnInformation("TYPE",
      "%10s", true, "Application type", "t"));
    columnInformationEnumMap.put(Columns.QUEUE, new ColumnInformation("QUEUE",
      "%10s", true, "Application queue", "q"));
    columnInformationEnumMap.put(Columns.PRIORITY, new ColumnInformation(
        "PRIOR", "%5s", true, "Application priority", "l"));
    columnInformationEnumMap.put(Columns.CONT, new ColumnInformation("#CONT",
      "%7s", true, "Number of containers", "c"));
    columnInformationEnumMap.put(Columns.RCONT, new ColumnInformation("#RCONT",
      "%7s", true, "Number of reserved containers", "r"));
    columnInformationEnumMap.put(Columns.VCORES, new ColumnInformation(
      "VCORES", "%7s", true, "Allocated vcores", "v"));
    columnInformationEnumMap.put(Columns.RVCORES, new ColumnInformation(
      "RVCORES", "%7s", true, "Reserved vcores", "o"));
    columnInformationEnumMap.put(Columns.MEM, new ColumnInformation("MEM",
      "%7s", true, "Allocated memory", "m"));
    columnInformationEnumMap.put(Columns.RMEM, new ColumnInformation("RMEM",
      "%7s", true, "Reserved memory", "w"));
    columnInformationEnumMap.put(Columns.VCORESECS, new ColumnInformation(
      "VCORESECS", "%10s", true, "Vcore seconds", "s"));
    columnInformationEnumMap.put(Columns.MEMSECS, new ColumnInformation(
      "MEMSECS", "%10s", true, "Memory seconds(in GBseconds)", "y"));
    columnInformationEnumMap.put(Columns.PROGRESS, new ColumnInformation(
      "%PROGR", "%6s", true, "Progress(percentage)", "p"));
    columnInformationEnumMap.put(Columns.TIME, new ColumnInformation("TIME",
      "%10s", true, "Running time", "i"));
    columnInformationEnumMap.put(Columns.NAME, new ColumnInformation("NAME",
      "%s", true, "Application name", "n"));
  }

  private void generateKeyFieldsMap() {
    for (EnumMap.Entry<Columns, ColumnInformation> entry :
        columnInformationEnumMap.entrySet()) {
      keyFieldsMap.put(entry.getValue().key, entry.getKey());
    }
  }

  protected NodesInformation getNodesInfo() {
    NodesInformation nodeInfo = new NodesInformation();
    YarnClusterMetrics yarnClusterMetrics;
    try {
      yarnClusterMetrics = client.getYarnClusterMetrics();
    } catch (IOException ie) {
      LOG.error("Unable to fetch cluster metrics", ie);
      return nodeInfo;
    } catch (YarnException ye) {
      LOG.error("Unable to fetch cluster metrics", ye);
      return nodeInfo;
    }

    nodeInfo.decommissionedNodes =
        yarnClusterMetrics.getNumDecommissionedNodeManagers();
    nodeInfo.totalNodes = yarnClusterMetrics.getNumNodeManagers();
    nodeInfo.runningNodes = yarnClusterMetrics.getNumActiveNodeManagers();
    nodeInfo.lostNodes = yarnClusterMetrics.getNumLostNodeManagers();
    nodeInfo.unhealthyNodes = yarnClusterMetrics.getNumUnhealthyNodeManagers();
    nodeInfo.rebootedNodes = yarnClusterMetrics.getNumRebootedNodeManagers();
    return nodeInfo;
  }

  protected QueueMetrics getQueueMetrics() {
    QueueMetrics queueMetrics = new QueueMetrics();
    List<QueueInfo> queuesInfo;
    if (queues.isEmpty()) {
      try {
        queuesInfo = client.getRootQueueInfos();
      } catch (Exception ie) {
        LOG.error("Unable to get queue information", ie);
        return queueMetrics;
      }
    } else {
      queuesInfo = new ArrayList<>();
      for (String queueName : queues) {
        try {
          QueueInfo qInfo = client.getQueueInfo(queueName);
          queuesInfo.add(qInfo);
        } catch (Exception ie) {
          LOG.error("Unable to get queue information", ie);
          return queueMetrics;
        }
      }
    }

    for (QueueInfo childInfo : queuesInfo) {
      QueueStatistics stats = childInfo.getQueueStatistics();
      if (stats != null) {
        queueMetrics.appsSubmitted += stats.getNumAppsSubmitted();
        queueMetrics.appsRunning += stats.getNumAppsRunning();
        queueMetrics.appsPending += stats.getNumAppsPending();
        queueMetrics.appsCompleted += stats.getNumAppsCompleted();
        queueMetrics.appsKilled += stats.getNumAppsKilled();
        queueMetrics.appsFailed += stats.getNumAppsFailed();
        queueMetrics.activeUsers += stats.getNumActiveUsers();
        queueMetrics.availableMemoryGB += stats.getAvailableMemoryMB();
        queueMetrics.allocatedMemoryGB += stats.getAllocatedMemoryMB();
        queueMetrics.pendingMemoryGB += stats.getPendingMemoryMB();
        queueMetrics.reservedMemoryGB += stats.getReservedMemoryMB();
        queueMetrics.availableVCores += stats.getAvailableVCores();
        queueMetrics.allocatedVCores += stats.getAllocatedVCores();
        queueMetrics.pendingVCores += stats.getPendingVCores();
        queueMetrics.reservedVCores += stats.getReservedVCores();
        queueMetrics.allocatedContainers += stats.getAllocatedContainers();
        queueMetrics.pendingContainers += stats.getPendingContainers();
        queueMetrics.reservedContainers += stats.getReservedContainers();
      }
    }
    queueMetrics.availableMemoryGB = queueMetrics.availableMemoryGB / 1024;
    queueMetrics.allocatedMemoryGB = queueMetrics.allocatedMemoryGB / 1024;
    queueMetrics.pendingMemoryGB = queueMetrics.pendingMemoryGB / 1024;
    queueMetrics.reservedMemoryGB = queueMetrics.reservedMemoryGB / 1024;
    return queueMetrics;
  }

  long getRMStartTime() {
    try {
      // connect with url
      URL url = getClusterUrl();
      if (null == url) {
        return -1;
      }
      JSONObject clusterInfo = getJSONObject(connect(url));
      return clusterInfo.getLong("startedOn");
    } catch (Exception e) {
      LOG.error("Could not fetch RM start time", e);
    }
    return -1;
  }

  private JSONObject getJSONObject(URLConnection conn)
      throws IOException, JSONException {
    try(InputStream in = conn.getInputStream()) {
      String encoding = conn.getContentEncoding();
      encoding = encoding == null ? "UTF-8" : encoding;
      String body = IOUtils.toString(in, encoding);
      JSONObject obj = new JSONObject(body);
      JSONObject clusterInfo = obj.getJSONObject("clusterInfo");
      return clusterInfo;
    }
  }

  private URL getClusterUrl() throws Exception {
    URL url = null;
    Configuration conf = getConf();
    if (HAUtil.isHAEnabled(conf)) {
      Collection<String> haids = HAUtil.getRMHAIds(conf);
      for (String rmhid : haids) {
        try {
          url = getHAClusterUrl(conf, rmhid);
          if (isActive(url)) {
            break;
          }
        } catch (ConnectException e) {
          // ignore and try second one when one of RM is down
        }
      }
    } else {
      url = new URL(
          WebAppUtils.getRMWebAppURLWithScheme(conf) + CLUSTER_INFO_URL);
    }
    return url;
  }

  private boolean isActive(URL url) throws Exception {
    URLConnection connect = connect(url);
    JSONObject clusterInfo = getJSONObject(connect);
    return clusterInfo.getString("haState").equals("ACTIVE");
  }

  @VisibleForTesting
  public URL getHAClusterUrl(Configuration conf, String rmhid)
      throws MalformedURLException {
    return new URL(WebAppUtils.getHttpSchemePrefix(conf)
        + WebAppUtils.getResolvedRemoteRMWebAppURLWithoutScheme(conf,
            YarnConfiguration.useHttps(conf) ? Policy.HTTPS_ONLY
                : Policy.HTTP_ONLY,
            rmhid)
        + CLUSTER_INFO_URL);
  }

  private URLConnection connect(URL url) throws Exception {
    AuthenticatedURL.Token token = new AuthenticatedURL.Token();
    AuthenticatedURL authUrl;
    SSLFactory clientSslFactory;
    URLConnection connection;
    // If https is chosen, configures SSL client.
    if (YarnConfiguration.useHttps(getConf())) {
      clientSslFactory = new SSLFactory(SSLFactory.Mode.CLIENT, getConf());
      clientSslFactory.init();
      SSLSocketFactory sslSocktFact = clientSslFactory.createSSLSocketFactory();

      authUrl =
          new AuthenticatedURL(new KerberosAuthenticator(), clientSslFactory);
      connection = authUrl.openConnection(url, token);
      HttpsURLConnection httpsConn = (HttpsURLConnection) connection;
      httpsConn.setSSLSocketFactory(sslSocktFact);
    } else {
      authUrl = new AuthenticatedURL(new KerberosAuthenticator());
      connection = authUrl.openConnection(url, token);
    }
    connection.connect();
    return connection;
  }

  String getHeader(QueueMetrics queueMetrics, NodesInformation nodes) {
    StringBuilder ret = new StringBuilder();
    String queue = "root";
    if (!queues.isEmpty()) {
      queue = StringUtils.join(queues, ",");
    }
    long now = Time.now();
    long uptime = 0L;
    if (rmStartTime != -1) {
      uptime = now - rmStartTime;
    }
    long days = TimeUnit.MILLISECONDS.toDays(uptime);
    long hours =
        TimeUnit.MILLISECONDS.toHours(uptime)
            - TimeUnit.DAYS.toHours(TimeUnit.MILLISECONDS.toDays(uptime));
    long minutes =
        TimeUnit.MILLISECONDS.toMinutes(uptime)
            - TimeUnit.HOURS.toMinutes(TimeUnit.MILLISECONDS.toHours(uptime));
    String uptimeStr = String.format("%dd, %d:%d", days, hours, minutes);
    String currentTime = DateFormatUtils.ISO_8601_EXTENDED_TIME_FORMAT
        .format(now);

    ret.append(CLEAR_LINE)
        .append(limitLineLength(String.format(
            "YARN top - %s, up %s, %d active users, queue(s): %s%n",
            currentTime, uptimeStr, queueMetrics.activeUsers, queue),
            terminalWidth, true));

    ret.append(CLEAR_LINE)
        .append(limitLineLength(String.format(
            "NodeManager(s)"
                + ": %d total, %d active, %d unhealthy, %d decommissioned,"
                + " %d lost, %d rebooted%n",
            nodes.totalNodes, nodes.runningNodes, nodes.unhealthyNodes,
            nodes.decommissionedNodes, nodes.lostNodes,
            nodes.rebootedNodes), terminalWidth, true));

    ret.append(CLEAR_LINE)
        .append(limitLineLength(String.format(
            "Queue(s) Applications: %d running, %d submitted, %d pending,"
                + " %d completed, %d killed, %d failed%n",
            queueMetrics.appsRunning, queueMetrics.appsSubmitted,
            queueMetrics.appsPending, queueMetrics.appsCompleted,
            queueMetrics.appsKilled, queueMetrics.appsFailed), terminalWidth,
            true));

    ret.append(CLEAR_LINE)
        .append(limitLineLength(String.format("Queue(s) Mem(GB): %d available,"
            + " %d allocated, %d pending, %d reserved%n",
            queueMetrics.availableMemoryGB, queueMetrics.allocatedMemoryGB,
            queueMetrics.pendingMemoryGB, queueMetrics.reservedMemoryGB),
            terminalWidth, true));

    ret.append(CLEAR_LINE)
        .append(limitLineLength(String.format("Queue(s) VCores: %d available,"
            + " %d allocated, %d pending, %d reserved%n",
            queueMetrics.availableVCores, queueMetrics.allocatedVCores,
            queueMetrics.pendingVCores, queueMetrics.reservedVCores),
            terminalWidth, true));

    ret.append(CLEAR_LINE)
        .append(limitLineLength(String.format(
            "Queue(s) Containers: %d allocated, %d pending, %d reserved%n",
            queueMetrics.allocatedContainers, queueMetrics.pendingContainers,
            queueMetrics.reservedContainers), terminalWidth, true));
    return ret.toString();
  }

  String getPrintableAppInformation(List<ApplicationInformation> appsInfo) {
    StringBuilder ret = new StringBuilder();
    int limit = terminalHeight - 9;
    List<String> columns = new ArrayList<>();
    for (int i = 0; i < limit; ++i) {
      ret.append(CLEAR_LINE);
      if(i < appsInfo.size()) {
        ApplicationInformation appInfo = appsInfo.get(i);
        columns.clear();
        for (EnumMap.Entry<Columns, ColumnInformation> entry :
            columnInformationEnumMap.entrySet()) {
          if (entry.getValue().display) {
            String value = "";
            if (appInfo.displayStringsMap.containsKey(entry.getKey())) {
              value = appInfo.displayStringsMap.get(entry.getKey());
            }
            columns.add(String.format(entry.getValue().format, value));
          }
        }
        ret.append(limitLineLength(
            (StringUtils.join(columns.toArray(), " ") + System.lineSeparator()),
            terminalWidth, true));
      }
      else {
        ret.append(System.lineSeparator());
      }
    }
    return ret.toString();
  }

  protected void clearScreen() {
    System.out.print(CLEAR);
    System.out.flush();
  }

  protected void clearScreenWithoutScroll() {
    System.out.print(SET_CURSOR_HOME);
    for(int i = 0; i < terminalHeight; ++i) {
      System.out.println(CLEAR_LINE);
    }
  }

  protected void printHeader(String header) {
    System.out.print(SET_CURSOR_HOME);
    System.out.print(header);
    System.out.println("");
  }

  protected void printApps(String appInfo) {
    System.out.print(CLEAR_LINE);
    System.out.print(CHANGE_BACKGROUND + appsHeader + RESET_BACKGROUND);
    System.out.print(appInfo);
  }

  private void showHelpScreen() {
    synchronized (lock) {
      if (!showingTopScreen.get()) {
        // we've already printed the help screen
        return;
      }
      showingTopScreen.set(false);
      clearScreenWithoutScroll();
      System.out.print(SET_CURSOR_HOME);
      System.out.println("Help for yarn top.");
      System.out.println("Delay: " + (refreshPeriod / 1000)
          + " secs; Secure mode: " + UserGroupInformation.isSecurityEnabled());
      System.out.println("");
      System.out.println("  s + Enter: Select sort field");
      System.out.println("  f + Enter: Select fields to display");
      System.out.println("  R + Enter: Reverse current sort order");
      System.out.println("  h + Enter: Display this screen");
      System.out.println("  q + Enter: Quit");
      System.out.println("");
      System.out.println("Press any key followed by Enter to continue");
    }
  }

  private void showSortScreen() {
    synchronized (lock) {
      showingTopScreen.set(false);
      System.out.print(SET_CURSOR_HOME);
      System.out.println(CLEAR_LINE + "Current Sort Field: " + currentSortField);
      System.out.println(CLEAR_LINE + "Select sort field via letter followed by"
          + " Enter, type any other key followed by Enter to return");
      System.out.println(CLEAR_LINE);
      for (String key : sortedKeys) {
        String prefix = " ";
        if (key.equals(currentSortField)) {
          prefix = "*";
        }
        ColumnInformation value =
            columnInformationEnumMap.get(keyFieldsMap.get(key));
        System.out.print(CLEAR_LINE);
        System.out.println(String.format("%s %s: %-15s = %s", prefix, key,
          value.header, value.description));
      }
    }
  }

  protected void showFieldsScreen() {
    synchronized (lock) {
      showingTopScreen.set(false);
      System.out.print(SET_CURSOR_HOME);
      System.out.println(CLEAR_LINE + "Current Fields: ");
      System.out.println(CLEAR_LINE + "Toggle fields via field letter followed"
          + " by Enter, type any other key followed by Enter to return");
      for (String key : sortedKeys) {
        ColumnInformation info =
            columnInformationEnumMap.get(keyFieldsMap.get(key));
        String prefix = " ";
        String letter = key;
        if (info.display) {
          prefix = "*";
          letter = key.toUpperCase();
        }
        System.out.print(CLEAR_LINE);
        System.out.println(String.format("%s %s: %-15s = %s", prefix, letter,
          info.header, info.description));
      }
    }
  }

  protected void showTopScreen() {
    List<ApplicationInformation> appsInfo = new ArrayList<>();
    List<ApplicationReport> apps;
    try {
      apps = fetchAppReports();
    } catch (Exception e) {
      LOG.error("Unable to get application information", e);
      return;
    }

    for (ApplicationReport appReport : apps) {
      ApplicationInformation appInfo = new ApplicationInformation(appReport);
      appsInfo.add(appInfo);
    }
    if (ascendingSort) {
      Collections.sort(appsInfo, comparator);
    } else {
      Collections.sort(appsInfo, Collections.reverseOrder(comparator));
    }
    NodesInformation nodesInfo = getNodesInfo();
    QueueMetrics queueMetrics = getQueueMetrics();
    String header = getHeader(queueMetrics, nodesInfo);
    String appsStr = getPrintableAppInformation(appsInfo);
    synchronized (lock) {
      printHeader(header);
      printApps(appsStr);
      System.out.print(SET_CURSOR_LINE_7_COLUMN_0);
      System.out.print(CLEAR_LINE);
    }
  }

  private void handleSortScreenKeyPress(String input) {
    String f = currentSortField;
    currentSortField = input.toLowerCase();
    switch (input.toLowerCase()) {
    case "a":
      comparator = AppIDComparator;
      break;
    case "u":
      comparator = UserComparator;
      break;
    case "t":
      comparator = AppTypeComparator;
      break;
    case "q":
      comparator = QueueNameComparator;
      break;
    case "c":
      comparator = UsedContainersComparator;
      break;
    case "r":
      comparator = ReservedContainersComparator;
      break;
    case "v":
      comparator = UsedVCoresComparator;
      break;
    case "o":
      comparator = ReservedVCoresComparator;
      break;
    case "m":
      comparator = UsedMemoryComparator;
      break;
    case "w":
      comparator = ReservedMemoryComparator;
      break;
    case "s":
      comparator = VCoreSecondsComparator;
      break;
    case "y":
      comparator = MemorySecondsComparator;
      break;
    case "p":
      comparator = ProgressComparator;
      break;
    case "i":
      comparator = RunningTimeComparator;
      break;
    case "n":
      comparator = AppNameComparator;
      break;
    case "l":
      comparator = AppPriorityComparator;
      break;
    default:
      // it wasn't a sort key
      currentSortField = f;
      showTopScreen();
      showingTopScreen.set(true);
      displayScreen = DisplayScreen.TOP;
    }
  }

  private void handleFieldsScreenKeyPress(String input) {
    if (keyFieldsMap.containsKey(input.toLowerCase())) {
      toggleColumn(keyFieldsMap.get(input.toLowerCase()));
      setAppsHeader();
    } else {
      showTopScreen();
      showingTopScreen.set(true);
      displayScreen = DisplayScreen.TOP;
    }
  }

  private void handleTopScreenKeyPress(String input) {
    switch (input.toLowerCase()) {
    case "q":
      runMainLoop.set(false);
      runKeyboardMonitor.set(false);
      // wake up if it's sleeping
      displayThread.interrupt();
      break;
    case "s":
      displayScreen = DisplayScreen.SORT;
      showSortScreen();
      break;
    case "f":
      displayScreen = DisplayScreen.FIELDS;
      showFieldsScreen();
      break;
    case "r":
      ascendingSort = !ascendingSort;
      break;
    case "h":
      displayScreen = DisplayScreen.HELP;
      showHelpScreen();
      break;
    default:
      break;
    }
  }

  private void handleHelpScreenKeyPress() {
    showTopScreen();
    showingTopScreen.set(true);
    displayScreen = DisplayScreen.TOP;
  }

  String limitLineLength(String line, int length, boolean addNewline) {
    if (line.length() > length) {
      String tmp;
      if (addNewline) {
        tmp = line.substring(0, length - System.lineSeparator().length());
        tmp += System.lineSeparator();
      } else {
        tmp = line.substring(0, length);
      }
      return tmp;
    }
    return line;
  }

  void toggleColumn(Columns col) {
    columnInformationEnumMap.get(col).display =
        !columnInformationEnumMap.get(col).display;
  }

  protected List<ApplicationReport> fetchAppReports() throws YarnException,
      IOException {
    List<ApplicationReport> ret;
    EnumSet<YarnApplicationState> states =
        EnumSet.of(YarnApplicationState.ACCEPTED, YarnApplicationState.RUNNING);
    GetApplicationsRequest req =
        GetApplicationsRequest.newInstance(types, states);
    req.setQueues(queues);
    req.setUsers(users);
    ret = applicationReportsCache.getIfPresent(req);
    if (ret != null) {
      return ret;
    }
    ret = client.getApplications(queues, users, types, states);
    applicationReportsCache.put(req, ret);
    return ret;
  }

  private String getCommandOutput(String[] command) throws IOException,
      InterruptedException {
    Process p = Runtime.getRuntime().exec(command);
    p.waitFor();
    byte[] output = IOUtils.toByteArray(p.getInputStream());
    return new String(output, "ASCII");
  }
}
