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

import org.apache.hadoop.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.io.output.FileWriterWithEncoding;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.LogAggregationStatus;
import org.apache.hadoop.yarn.applications.distributedshell.ApplicationMaster;
import org.apache.hadoop.yarn.applications.distributedshell.Client;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.ApplicationNotFoundException;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.logaggregation.filecontroller.LogAggregationFileController;
import org.apache.hadoop.yarn.logaggregation.filecontroller.LogAggregationFileControllerFactory;
import org.apache.hadoop.yarn.util.ConverterUtils;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
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
  private static final Logger LOG = LoggerFactory.getLogger(HadoopArchiveLogs.class);

  private static final String HELP_OPTION = "help";
  private static final String MAX_ELIGIBLE_APPS_OPTION = "maxEligibleApps";
  private static final String MIN_NUM_LOG_FILES_OPTION = "minNumberLogFiles";
  private static final String MAX_TOTAL_LOGS_SIZE_OPTION = "maxTotalLogsSize";
  private static final String MEMORY_OPTION = "memory";
  private static final String VERBOSE_OPTION = "verbose";
  private static final String FORCE_OPTION = "force";
  private static final String NO_PROXY_OPTION = "noProxy";

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
  private boolean verbose = false;
  @VisibleForTesting
  boolean force = false;
  @VisibleForTesting
  boolean proxy = true;

  @VisibleForTesting
  Set<AppInfo> eligibleApplications;

  private Set<Path> workingDirs;

  private JobConf conf;

  public HadoopArchiveLogs(Configuration conf) {
    setConf(conf);
    eligibleApplications = new HashSet<>();
    workingDirs = new HashSet<>();
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
    int exitCode = 1;

    handleOpts(args);

    FileSystem fs = null;

    LogAggregationFileControllerFactory factory =
        new LogAggregationFileControllerFactory(conf);
    List<LogAggregationFileController> fileControllers = factory
        .getConfiguredLogAggregationFileControllerList();
    if (fileControllers == null || fileControllers.isEmpty()) {
      LOG.info("Can not find any valid fileControllers.");
      if (verbose) {
        LOG.info("The configurated fileControllers:"
            + YarnConfiguration.LOG_AGGREGATION_FILE_FORMATS);
      }
      return 0;
    }
    try {
      fs = FileSystem.get(conf);
      // find eligibleApplications for all the fileControllers
      int previousTotal = 0;
      for (LogAggregationFileController fileController : fileControllers) {
        Path remoteRootLogDir = fileController.getRemoteRootLogDir();
        String suffix = fileController.getRemoteRootLogDirSuffix();
        Path workingDir = new Path(remoteRootLogDir, "archive-logs-work");
        if (verbose) {
          LOG.info("LogAggregationFileController:" + fileController
              .getClass().getName());
          LOG.info("Remote Log Dir Root: " + remoteRootLogDir);
          LOG.info("Log Suffix: " + suffix);
          LOG.info("Working Dir: " + workingDir);
        }
        checkFilesAndSeedApps(fs, remoteRootLogDir, suffix, workingDir);

        filterAppsByAggregatedStatus();

        if (eligibleApplications.size() > previousTotal) {
          workingDirs.add(workingDir);
          previousTotal = eligibleApplications.size();
        }
      }
      checkMaxEligible();
      if (workingDirs.isEmpty() || eligibleApplications.isEmpty()) {
        LOG.info("No eligible applications to process");
        return 0;
      }
      for (Path workingDir : workingDirs) {
        if (!prepareWorkingDir(fs, workingDir)) {
          LOG.error("Failed to create the workingDir:"
              + workingDir.toString());
          return 1;
        }
      }
      StringBuilder sb =
          new StringBuilder("Will process the following applications:");
      for (AppInfo app : eligibleApplications) {
        sb.append("\n\t").append(app.getAppId());
      }
      LOG.info(sb.toString());
      File localScript = File.createTempFile("hadoop-archive-logs-", ".sh");
      generateScript(localScript);

      exitCode = runDistributedShell(localScript) ? 0 : 1;
    } finally {
      if (fs != null) {
        // Cleanup working directory
        for (Path workingDir : workingDirs) {
          fs.delete(workingDir, true);
        }
        fs.close();
      }
    }
    return exitCode;
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
    Option verboseOpt = new Option(VERBOSE_OPTION, false,
        "Print more details.");
    Option forceOpt = new Option(FORCE_OPTION, false,
        "Force recreating the working directory if an existing one is found. " +
            "This should only be used if you know that another instance is " +
            "not currently running");
    Option noProxyOpt = new Option(NO_PROXY_OPTION, false,
        "When specified, all processing will be done as the user running this" +
            " command (or the Yarn user if DefaultContainerExecutor is in " +
            "use). When not specified, all processing will be done as the " +
            "user who owns that application; if the user running this command" +
            " is not allowed to impersonate that user, it will fail");
    opts.addOption(helpOpt);
    opts.addOption(maxEligibleOpt);
    opts.addOption(minNumLogFilesOpt);
    opts.addOption(maxTotalLogsSizeOpt);
    opts.addOption(memoryOpt);
    opts.addOption(verboseOpt);
    opts.addOption(forceOpt);
    opts.addOption(noProxyOpt);

    try {
      CommandLineParser parser = new GnuParser();
      CommandLine commandLine = parser.parse(opts, args);
      if (commandLine.hasOption(HELP_OPTION)) {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("mapred archive-logs", opts);
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
      if (commandLine.hasOption(VERBOSE_OPTION)) {
        verbose = true;
      }
      if (commandLine.hasOption(FORCE_OPTION)) {
        force = true;
      }
      if (commandLine.hasOption(NO_PROXY_OPTION)) {
        proxy = false;
      }
    } catch (ParseException pe) {
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp("mapred archive-logs", opts);
      throw pe;
    }
  }

  @VisibleForTesting
  boolean prepareWorkingDir(FileSystem fs, Path workingDir) throws IOException {
    if (fs.exists(workingDir)) {
      if (force) {
        LOG.info("Existing Working Dir detected: -" + FORCE_OPTION +
            " specified -> recreating Working Dir");
        fs.delete(workingDir, true);
      } else {
        LOG.info("Existing Working Dir detected: -" + FORCE_OPTION +
            " not specified -> exiting");
        return false;
      }
    }
    fs.mkdirs(workingDir);
    fs.setPermission(workingDir,
        new FsPermission(FsAction.ALL, FsAction.ALL, FsAction.ALL, true));
    return true;
  }

  @VisibleForTesting
  void filterAppsByAggregatedStatus() throws IOException, YarnException {
    YarnClient client = YarnClient.createYarnClient();
    try {
      client.init(getConf());
      client.start();
      for (Iterator<AppInfo> it = eligibleApplications
          .iterator(); it.hasNext();) {
        AppInfo app = it.next();
        try {
          ApplicationReport report = client.getApplicationReport(
              ApplicationId.fromString(app.getAppId()));
          LogAggregationStatus aggStatus = report.getLogAggregationStatus();
          if (aggStatus.equals(LogAggregationStatus.RUNNING) ||
              aggStatus.equals(LogAggregationStatus.RUNNING_WITH_FAILURE) ||
              aggStatus.equals(LogAggregationStatus.NOT_START) ||
              aggStatus.equals(LogAggregationStatus.DISABLED) ||
              aggStatus.equals(LogAggregationStatus.FAILED)) {
            if (verbose) {
              LOG.info("Skipping " + app.getAppId() +
                  " due to aggregation status being " + aggStatus);
            }
            it.remove();
          } else {
            if (verbose) {
              LOG.info(app.getAppId() + " has aggregation status " + aggStatus);
            }
            app.setFinishTime(report.getFinishTime());
          }
        } catch (ApplicationNotFoundException e) {
          // Assume the aggregation has finished
          if (verbose) {
            LOG.info(app.getAppId() + " not in the ResourceManager");
          }
        }
      }
    } finally {
      if (client != null) {
        client.stop();
      }
    }
  }

  @VisibleForTesting
  void checkFilesAndSeedApps(FileSystem fs, Path remoteRootLogDir,
       String suffix, Path workingDir) throws IOException {
    for (RemoteIterator<FileStatus> userIt =
         fs.listStatusIterator(remoteRootLogDir); userIt.hasNext();) {
      Path userLogPath = userIt.next().getPath();
      try {
        for (RemoteIterator<FileStatus> appIt =
             fs.listStatusIterator(new Path(userLogPath, suffix));
             appIt.hasNext();) {
          Path appLogPath = appIt.next().getPath();
          try {
            FileStatus[] files = fs.listStatus(appLogPath);
            if (files.length >= minNumLogFiles) {
              boolean eligible = true;
              long totalFileSize = 0L;
              for (FileStatus file : files) {
                if (file.getPath().getName().equals(appLogPath.getName()
                    + ".har")) {
                  eligible = false;
                  if (verbose) {
                    LOG.info("Skipping " + appLogPath.getName() +
                        " due to existing .har file");
                  }
                  break;
                }
                totalFileSize += file.getLen();
                if (totalFileSize > maxTotalLogsSize) {
                  eligible = false;
                  if (verbose) {
                    LOG.info("Skipping " + appLogPath.getName() + " due to " +
                        "total file size being too large (" + totalFileSize +
                        " > " + maxTotalLogsSize + ")");
                  }
                  break;
                }
              }
              if (eligible) {
                if (verbose) {
                  LOG.info("Adding " + appLogPath.getName() + " for user " +
                      userLogPath.getName());
                }
                AppInfo context = new AppInfo();
                context.setAppId(appLogPath.getName());
                context.setUser(userLogPath.getName());
                context.setSuffix(suffix);
                context.setRemoteRootLogDir(remoteRootLogDir);
                context.setWorkingDir(workingDir);
                eligibleApplications.add(context);
              }
            } else {
              if (verbose) {
                LOG.info("Skipping " + appLogPath.getName() + " due to not " +
                    "having enough log files (" + files.length + " < " +
                    minNumLogFiles + ")");
              }
            }
          } catch (IOException ioe) {
            // Ignore any apps we can't read
            if (verbose) {
              LOG.info("Skipping logs under " + appLogPath + " due to " +
                  ioe.getMessage());
            }
          }
        }
      } catch (IOException ioe) {
        // Ignore any apps we can't read
        if (verbose) {
          LOG.info("Skipping all logs under " + userLogPath + " due to " +
              ioe.getMessage());
        }
      }
    }
  }

  @VisibleForTesting
  void checkMaxEligible() {
    // If we have too many eligible apps, remove the newest ones first
    if (maxEligible > 0 && eligibleApplications.size()
        > maxEligible) {
      if (verbose) {
        LOG.info("Too many applications (" + eligibleApplications
            .size() +
            " > " + maxEligible + ")");
      }
      List<AppInfo> sortedApplications =
          new ArrayList<AppInfo>(eligibleApplications);
      Collections.sort(sortedApplications, new Comparator<
          AppInfo>() {
        @Override
        public int compare(AppInfo o1, AppInfo o2) {
          int lCompare = Long.compare(o1.getFinishTime(), o2.getFinishTime());
          if (lCompare == 0) {
            return o1.getAppId().compareTo(o2.getAppId());
          }
          return lCompare;
        }
      });
      for (int i = maxEligible; i < sortedApplications.size(); i++) {
        if (verbose) {
          LOG.info("Removing " + sortedApplications.get(i));
        }
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
        workingDir="/tmp/logs/archive-logs-work"
        remoteRootLogDir="/tmp/logs"
        suffix="logs"
  elif [ "$YARN_SHELL_ID" == "2" ]; then
        appId="application_1440448768987_0002"
        user="rkanter"
        workingDir="/tmp/logs/archive-logs-work"
        remoteRootLogDir="/tmp/logs"
        suffix="logs"
  else
        echo "Unknown Mapping!"
        exit 1
  fi
  export HADOOP_CLIENT_OPTS="-Xmx1024m"
  export HADOOP_CLASSPATH=/dist/share/hadoop/tools/lib/hadoop-archive-logs-2.8.0-SNAPSHOT.jar:/dist/share/hadoop/tools/lib/hadoop-archives-2.8.0-SNAPSHOT.jar
  "$HADOOP_HOME"/bin/hadoop org.apache.hadoop.tools.HadoopArchiveLogsRunner -appId "$appId" -user "$user" -workingDir "$workingDir" -remoteRootLogDir "$remoteRootLogDir" -suffix "$suffix"
   */
  @VisibleForTesting
  void generateScript(File localScript) throws IOException {
    if (verbose) {
      LOG.info("Generating script at: " + localScript.getAbsolutePath());
    }
    String halrJarPath = HadoopArchiveLogsRunner.class.getProtectionDomain()
        .getCodeSource().getLocation().getPath();
    String harJarPath = HadoopArchives.class.getProtectionDomain()
        .getCodeSource().getLocation().getPath();
    String classpath = halrJarPath + File.pathSeparator + harJarPath;
    FileWriterWithEncoding fw = null;
    try {
      fw = new FileWriterWithEncoding(localScript, "UTF-8");
      fw.write("#!/bin/bash\nset -e\nset -x\n");
      int containerCount = 1;
      for (AppInfo context : eligibleApplications) {
        fw.write("if [ \"$YARN_SHELL_ID\" == \"");
        fw.write(Integer.toString(containerCount));
        fw.write("\" ]; then\n\tappId=\"");
        fw.write(context.getAppId());
        fw.write("\"\n\tuser=\"");
        fw.write(context.getUser());
        fw.write("\"\n\tworkingDir=\"");
        fw.write(context.getWorkingDir().toString());
        fw.write("\"\n\tremoteRootLogDir=\"");
        fw.write(context.getRemoteRootLogDir().toString());
        fw.write("\"\n\tsuffix=\"");
        fw.write(context.getSuffix());
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
      fw.write("\"$workingDir\"");
      fw.write(" -remoteRootLogDir ");
      fw.write("\"$remoteRootLogDir\"");
      fw.write(" -suffix ");
      fw.write("\"$suffix\"");
      if (!proxy) {
        fw.write(" -noProxy\n");
      }
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
    if (verbose) {
      LOG.info("Running Distributed Shell with arguments: " +
          Arrays.toString(dsArgs));
    }
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

  @VisibleForTesting
  static class AppInfo {
    private String appId;
    private Path remoteRootLogDir;
    private String suffix;
    private Path workingDir;
    private String user;
    private long finishTime;

    AppInfo() {}

    AppInfo(String appId, String user) {
      this.setAppId(appId);
      this.setUser(user);
    }

    public String getAppId() {
      return appId;
    }

    public void setAppId(String appId) {
      this.appId = appId;
    }

    public Path getRemoteRootLogDir() {
      return remoteRootLogDir;
    }

    public void setRemoteRootLogDir(Path remoteRootLogDir) {
      this.remoteRootLogDir = remoteRootLogDir;
    }

    public String getSuffix() {
      return suffix;
    }

    public void setSuffix(String suffix) {
      this.suffix = suffix;
    }

    public Path getWorkingDir() {
      return workingDir;
    }

    public void setWorkingDir(Path workingDir) {
      this.workingDir = workingDir;
    }

    public String getUser() {
      return user;
    }

    public void setUser(String user) {
      this.user = user;
    }

    public long getFinishTime() {
      return finishTime;
    }

    public void setFinishTime(long finishTime) {
      this.finishTime = finishTime;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      AppInfo appInfo = (AppInfo) o;

      if (appId != null
          ? !appId.equals(appInfo.appId) : appInfo.appId != null) {
        return false;
      }

      if (user != null
          ? !user.equals(appInfo.user) : appInfo.user != null) {
        return false;
      }

      if (suffix != null
          ? !suffix.equals(appInfo.suffix) : appInfo.suffix != null) {
        return false;
      }

      if (workingDir != null ? !workingDir.equals(
          appInfo.workingDir) : appInfo.workingDir != null) {
        return false;
      }

      if (remoteRootLogDir != null ? !remoteRootLogDir.equals(
          appInfo.remoteRootLogDir) : appInfo.remoteRootLogDir != null) {
        return false;
      }

      return Long.compare(finishTime, appInfo.finishTime) == 0;
    }

    @Override
    public int hashCode() {
      int result = appId != null ? appId.hashCode() : 0;
      result = 31 * result + (user != null ? user.hashCode() : 0);
      result = 31 * result + (suffix != null ? suffix.hashCode() : 0);
      result = 31 * result + (workingDir != null ? workingDir.hashCode() : 0);
      result = 31 * result + (remoteRootLogDir != null ?
          remoteRootLogDir.hashCode() : 0);
      result = 31 * result + Long.valueOf(finishTime).hashCode();
      return result;
    }
  }
}
