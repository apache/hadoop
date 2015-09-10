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

package org.apache.hadoop.tools;

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.LogAggregationStatus;
import org.apache.hadoop.yarn.applications.distributedshell.ApplicationMaster;
import org.apache.hadoop.yarn.applications.distributedshell.Client;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.logaggregation.LogAggregationUtils;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;


/**
 * This tool moves Aggregated Log files into HAR archives using the
 * {@link HadoopArchives} tool and the Distributed Shell via the
 * {@link HadoopArchiveLogsRunner}.
 */
public class HadoopArchiveLogs implements Tool {
  private static final Log LOG = LogFactory.getLog(HadoopArchiveLogs.class);

  private static final String HELP_OPTION = "help";
  private static final String MAX_ELIGIBLE_APPS_OPTION = "maxEligibleApps";
  private static final String MIN_NUM_LOG_FILES_OPTION = "minNumberLogFiles";
  private static final String MAX_TOTAL_LOGS_SIZE_OPTION = "maxTotalLogsSize";
  private static final String MEMORY_OPTION = "memory";

  private static final int DEFAULT_MAX_ELIGIBLE = -1;
  private static final int DEFAULT_MIN_NUM_LOG_FILES = 20;
  private static final long DEFAULT_MAX_TOTAL_LOGS_SIZE = 1024L;
  private static final long DEFAULT_MEMORY = 1024L;

  @VisibleForTesting
  int maxEligible = DEFAULT_MAX_ELIGIBLE;
  @VisibleForTesting
  int minNumLogFiles = DEFAULT_MIN_NUM_LOG_FILES;
  @VisibleForTesting
  long maxTotalLogsSize = DEFAULT_MAX_TOTAL_LOGS_SIZE * 1024L * 1024L;
  @VisibleForTesting
  long memory = DEFAULT_MEMORY;

  @VisibleForTesting
  Set<ApplicationReport> eligibleApplications;

  private JobConf conf;

  public HadoopArchiveLogs(Configuration conf) {
    setConf(conf);
    eligibleApplications = new HashSet<>();
  }

  public static void main(String[] args) {
    JobConf job = new JobConf(HadoopArchiveLogs.class);

    HadoopArchiveLogs hal = new HadoopArchiveLogs(job);
    int ret = 0;

    try{
      ret = ToolRunner.run(hal, args);
    } catch(Exception e) {
      LOG.debug("Exception", e);
      System.err.println(e.getClass().getSimpleName());
      final String s = e.getLocalizedMessage();
      if (s != null) {
        System.err.println(s);
      } else {
        e.printStackTrace(System.err);
      }
      System.exit(1);
    }
    System.exit(ret);
  }

  @Override
  public int run(String[] args) throws Exception {
    handleOpts(args);

    findAggregatedApps();

    FileSystem fs = null;
    Path remoteRootLogDir = new Path(conf.get(
        YarnConfiguration.NM_REMOTE_APP_LOG_DIR,
        YarnConfiguration.DEFAULT_NM_REMOTE_APP_LOG_DIR));
    String suffix = LogAggregationUtils.getRemoteNodeLogDirSuffix(conf);
    Path workingDir = new Path(remoteRootLogDir, "archive-logs-work");
    try {
      fs = FileSystem.get(conf);
      checkFiles(fs, remoteRootLogDir, suffix);

      // Prepare working directory
      if (fs.exists(workingDir)) {
        fs.delete(workingDir, true);
      }
      fs.mkdirs(workingDir);
      fs.setPermission(workingDir,
          new FsPermission(FsAction.ALL, FsAction.NONE, FsAction.NONE));
    } finally {
      if (fs != null) {
        fs.close();
      }
    }

    checkMaxEligible();

    if (eligibleApplications.isEmpty()) {
      LOG.info("No eligible applications to process");
      System.exit(0);
    }

    StringBuilder sb =
        new StringBuilder("Will process the following applications:");
    for (ApplicationReport report : eligibleApplications) {
      sb.append("\n\t").append(report.getApplicationId());
    }
    LOG.info(sb.toString());

    File localScript = File.createTempFile("hadoop-archive-logs-", ".sh");
    generateScript(localScript, workingDir, remoteRootLogDir, suffix);

    if (runDistributedShell(localScript)) {
      return 0;
    }
    return -1;
  }

  private void handleOpts(String[] args) throws ParseException {
    Options opts = new Options();
    Option helpOpt = new Option(HELP_OPTION, false, "Prints this message");
    Option maxEligibleOpt = new Option(MAX_ELIGIBLE_APPS_OPTION, true,
        "The maximum number of eligible apps to process (default: "
            + DEFAULT_MAX_ELIGIBLE + " (all))");
    maxEligibleOpt.setArgName("n");
    Option minNumLogFilesOpt = new Option(MIN_NUM_LOG_FILES_OPTION, true,
        "The minimum number of log files required to be eligible (default: "
            + DEFAULT_MIN_NUM_LOG_FILES + ")");
    minNumLogFilesOpt.setArgName("n");
    Option maxTotalLogsSizeOpt = new Option(MAX_TOTAL_LOGS_SIZE_OPTION, true,
        "The maximum total logs size (in megabytes) required to be eligible" +
            " (default: " + DEFAULT_MAX_TOTAL_LOGS_SIZE + ")");
    maxTotalLogsSizeOpt.setArgName("megabytes");
    Option memoryOpt = new Option(MEMORY_OPTION, true,
        "The amount of memory (in megabytes) for each container (default: "
            + DEFAULT_MEMORY + ")");
    memoryOpt.setArgName("megabytes");
    opts.addOption(helpOpt);
    opts.addOption(maxEligibleOpt);
    opts.addOption(minNumLogFilesOpt);
    opts.addOption(maxTotalLogsSizeOpt);
    opts.addOption(memoryOpt);

    try {
      CommandLineParser parser = new GnuParser();
      CommandLine commandLine = parser.parse(opts, args);
      if (commandLine.hasOption(HELP_OPTION)) {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("yarn archive-logs", opts);
        System.exit(0);
      }
      if (commandLine.hasOption(MAX_ELIGIBLE_APPS_OPTION)) {
        maxEligible = Integer.parseInt(
            commandLine.getOptionValue(MAX_ELIGIBLE_APPS_OPTION));
        if (maxEligible == 0) {
          LOG.info("Setting " + MAX_ELIGIBLE_APPS_OPTION + " to 0 accomplishes "
              + "nothing. Please either set it to a negative value "
              + "(default, all) or a more reasonable value.");
          System.exit(0);
        }
      }
      if (commandLine.hasOption(MIN_NUM_LOG_FILES_OPTION)) {
        minNumLogFiles = Integer.parseInt(
            commandLine.getOptionValue(MIN_NUM_LOG_FILES_OPTION));
      }
      if (commandLine.hasOption(MAX_TOTAL_LOGS_SIZE_OPTION)) {
        maxTotalLogsSize = Long.parseLong(
            commandLine.getOptionValue(MAX_TOTAL_LOGS_SIZE_OPTION));
        maxTotalLogsSize *= 1024L * 1024L;
      }
      if (commandLine.hasOption(MEMORY_OPTION)) {
        memory = Long.parseLong(commandLine.getOptionValue(MEMORY_OPTION));
      }
    } catch (ParseException pe) {
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp("yarn archive-logs", opts);
      throw pe;
    }
  }

  @VisibleForTesting
  void findAggregatedApps() throws IOException, YarnException {
    YarnClient client = YarnClient.createYarnClient();
    try {
      client.init(getConf());
      client.start();
      List<ApplicationReport> reports = client.getApplications();
      for (ApplicationReport report : reports) {
        LogAggregationStatus aggStatus = report.getLogAggregationStatus();
        if (aggStatus.equals(LogAggregationStatus.SUCCEEDED) ||
            aggStatus.equals(LogAggregationStatus.FAILED)) {
          eligibleApplications.add(report);
        }
      }
    } finally {
      if (client != null) {
        client.stop();
      }
    }
  }

  @VisibleForTesting
  void checkFiles(FileSystem fs, Path remoteRootLogDir, String suffix) {
    for (Iterator<ApplicationReport> reportIt = eligibleApplications.iterator();
         reportIt.hasNext(); ) {
      ApplicationReport report = reportIt.next();
      long totalFileSize = 0L;
      try {
        FileStatus[] files = fs.listStatus(
            LogAggregationUtils.getRemoteAppLogDir(remoteRootLogDir,
                report.getApplicationId(), report.getUser(), suffix));
        if (files.length < minNumLogFiles) {
          reportIt.remove();
        } else {
          for (FileStatus file : files) {
            if (file.getPath().getName().equals(report.getApplicationId()
                + ".har")) {
              reportIt.remove();
              break;
            }
            totalFileSize += file.getLen();
          }
          if (totalFileSize > maxTotalLogsSize) {
            reportIt.remove();
          }
        }
      } catch (IOException ioe) {
        // If the user doesn't have permission or it doesn't exist, then skip it
        reportIt.remove();
      }
    }
  }

  @VisibleForTesting
  void checkMaxEligible() {
    // If we have too many eligible apps, remove the newest ones first
    if (maxEligible > 0 && eligibleApplications.size() > maxEligible) {
      List<ApplicationReport> sortedApplications =
          new ArrayList<ApplicationReport>(eligibleApplications);
      Collections.sort(sortedApplications, new Comparator<ApplicationReport>() {
        @Override
        public int compare(ApplicationReport o1, ApplicationReport o2) {
          return Long.compare(o1.getFinishTime(), o2.getFinishTime());
        }
      });
      for (int i = maxEligible; i < sortedApplications.size(); i++) {
        eligibleApplications.remove(sortedApplications.get(i));
      }
    }
  }

  /*
  The generated script looks like this:
  #!/bin/bash
  set -e
  set -x
  if [ "$YARN_SHELL_ID" == "1" ]; then
        appId="application_1440448768987_0001"
        user="rkanter"
  elif [ "$YARN_SHELL_ID" == "2" ]; then
        appId="application_1440448768987_0002"
        user="rkanter"
  else
        echo "Unknown Mapping!"
        exit 1
  fi
  export HADOOP_CLIENT_OPTS="-Xmx1024m"
  export HADOOP_CLASSPATH=/dist/share/hadoop/tools/lib/hadoop-archive-logs-2.8.0-SNAPSHOT.jar:/dist/share/hadoop/tools/lib/hadoop-archives-2.8.0-SNAPSHOT.jar
  "$HADOOP_HOME"/bin/hadoop org.apache.hadoop.tools.HadoopArchiveLogsRunner -appId "$appId" -user "$user" -workingDir /tmp/logs/archive-logs-work -remoteRootLogDir /tmp/logs -suffix logs
   */
  @VisibleForTesting
  void generateScript(File localScript, Path workingDir,
        Path remoteRootLogDir, String suffix) throws IOException {
    LOG.info("Generating script at: " + localScript.getAbsolutePath());
    String halrJarPath = HadoopArchiveLogsRunner.class.getProtectionDomain()
        .getCodeSource().getLocation().getPath();
    String harJarPath = HadoopArchives.class.getProtectionDomain()
        .getCodeSource().getLocation().getPath();
    String classpath = halrJarPath + File.pathSeparator + harJarPath;
    FileWriter fw = null;
    try {
      fw = new FileWriter(localScript);
      fw.write("#!/bin/bash\nset -e\nset -x\n");
      int containerCount = 1;
      for (ApplicationReport report : eligibleApplications) {
        fw.write("if [ \"$YARN_SHELL_ID\" == \"");
        fw.write(Integer.toString(containerCount));
        fw.write("\" ]; then\n\tappId=\"");
        fw.write(report.getApplicationId().toString());
        fw.write("\"\n\tuser=\"");
        fw.write(report.getUser());
        fw.write("\"\nel");
        containerCount++;
      }
      fw.write("se\n\techo \"Unknown Mapping!\"\n\texit 1\nfi\n");
      fw.write("export HADOOP_CLIENT_OPTS=\"-Xmx");
      fw.write(Long.toString(memory));
      fw.write("m\"\n");
      fw.write("export HADOOP_CLASSPATH=");
      fw.write(classpath);
      fw.write("\n\"$HADOOP_HOME\"/bin/hadoop ");
      fw.write(HadoopArchiveLogsRunner.class.getName());
      fw.write(" -appId \"$appId\" -user \"$user\" -workingDir ");
      fw.write(workingDir.toString());
      fw.write(" -remoteRootLogDir ");
      fw.write(remoteRootLogDir.toString());
      fw.write(" -suffix ");
      fw.write(suffix);
      fw.write("\n");
    } finally {
      if (fw != null) {
        fw.close();
      }
    }
  }

  private boolean runDistributedShell(File localScript) throws Exception {
    String[] dsArgs = {
        "--appname",
        "ArchiveLogs",
        "--jar",
        ApplicationMaster.class.getProtectionDomain().getCodeSource()
            .getLocation().getPath(),
        "--num_containers",
        Integer.toString(eligibleApplications.size()),
        "--container_memory",
        Long.toString(memory),
        "--shell_script",
        localScript.getAbsolutePath()
    };
    final Client dsClient = new Client(new Configuration(conf));
    dsClient.init(dsArgs);
    return dsClient.run();
  }

  @Override
  public void setConf(Configuration conf) {
    if (conf instanceof JobConf) {
      this.conf = (JobConf) conf;
    } else {
      this.conf = new JobConf(conf, HadoopArchiveLogs.class);
    }
  }

  @Override
  public Configuration getConf() {
    return this.conf;
  }
}
