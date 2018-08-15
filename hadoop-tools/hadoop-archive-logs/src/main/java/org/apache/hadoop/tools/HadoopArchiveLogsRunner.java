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
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.File;
import java.security.PrivilegedExceptionAction;

/**
 * This is a child program designed to be used by the {@link HadoopArchiveLogs}
 * tool via the Distributed Shell.  It's not meant to be run directly.
 */
public class HadoopArchiveLogsRunner implements Tool {
  private static final Logger LOG =
      LoggerFactory.getLogger(HadoopArchiveLogsRunner.class);

  private static final String APP_ID_OPTION = "appId";
  private static final String USER_OPTION = "user";
  private static final String WORKING_DIR_OPTION = "workingDir";
  private static final String REMOTE_ROOT_LOG_DIR_OPTION = "remoteRootLogDir";
  private static final String SUFFIX_OPTION = "suffix";
  private static final String NO_PROXY_OPTION = "noProxy";

  private String appId;
  private String user;
  private String workingDir;
  private String remoteLogDir;
  private String suffix;
  private boolean proxy;

  private JobConf conf;

  @VisibleForTesting
  HadoopArchives hadoopArchives;

  private static final FsPermission HAR_DIR_PERM =
      new FsPermission(FsAction.ALL, FsAction.READ_EXECUTE, FsAction.NONE);
  private static final FsPermission HAR_INNER_FILES_PERM =
      new FsPermission(FsAction.READ_WRITE, FsAction.READ, FsAction.NONE);

  public HadoopArchiveLogsRunner(Configuration conf) {
    setConf(conf);
    hadoopArchives = new HadoopArchives(conf);
  }

  public static void main(String[] args) {
    JobConf job = new JobConf(HadoopArchiveLogsRunner.class);

    HadoopArchiveLogsRunner halr = new HadoopArchiveLogsRunner(job);
    int ret = 0;

    try{
      ret = ToolRunner.run(halr, args);
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

    Integer exitCode = 1;
    UserGroupInformation loginUser = UserGroupInformation.getLoginUser();
    // If we're running as the user, then no need to impersonate
    // (which might fail if user is not a proxyuser for themselves)
    // Also if !proxy is set
    if (!proxy || loginUser.getShortUserName().equals(user)) {
      LOG.info("Running as " + user);
      exitCode = runInternal();
    } else {
      // Otherwise impersonate user.  If we're not allowed to, then this will
      // fail with an Exception
      LOG.info("Running as " + loginUser.getShortUserName() + " but will " +
          "impersonate " + user);
      UserGroupInformation proxyUser =
          UserGroupInformation.createProxyUser(user, loginUser);
      exitCode = proxyUser.doAs(new PrivilegedExceptionAction<Integer>() {
        @Override
        public Integer run() throws Exception {
          return runInternal();
        }
      });
    }
    return exitCode;
  }

  private int runInternal() throws Exception {
    String remoteAppLogDir = remoteLogDir + File.separator + user
        + File.separator + suffix + File.separator + appId;
    // Run 'hadoop archives' command in local mode
    conf.set("mapreduce.framework.name", "local");
    // Set the umask so we get 640 files and 750 dirs
    conf.set("fs.permissions.umask-mode", "027");
    String harName = appId + ".har";
    String[] haArgs = {
        "-archiveName",
        harName,
        "-p",
        remoteAppLogDir,
        "*",
        workingDir
    };
    StringBuilder sb = new StringBuilder("Executing 'hadoop archives'");
    for (String haArg : haArgs) {
      sb.append("\n\t").append(haArg);
    }
    LOG.info(sb.toString());
    int exitCode = hadoopArchives.run(haArgs);
    if (exitCode != 0) {
      LOG.warn("Failed to create archives for " + appId);
      return -1;
    }

    FileSystem fs = null;
    // Move har file to correct location and delete original logs
    try {
      fs = FileSystem.get(conf);
      Path harPath = new Path(workingDir, harName);
      if (!fs.exists(harPath) ||
          fs.listStatus(harPath).length == 0) {
        LOG.warn("The created archive \"" + harName +
            "\" is missing or empty.");
        return -1;
      }
      Path harDest = new Path(remoteAppLogDir, harName);
      LOG.info("Moving har to original location");
      fs.rename(harPath, harDest);
      LOG.info("Deleting original logs");
      for (FileStatus original : fs.listStatus(new Path(remoteAppLogDir),
          new PathFilter() {
            @Override
            public boolean accept(Path path) {
              return !path.getName().endsWith(".har");
            }
          })) {
        fs.delete(original.getPath(), false);
      }
    } finally {
      if (fs != null) {
        fs.close();
      }
    }
    return 0;
  }

  private void handleOpts(String[] args) throws ParseException {
    Options opts = new Options();
    Option appIdOpt = new Option(APP_ID_OPTION, true, "Application ID");
    appIdOpt.setRequired(true);
    Option userOpt = new Option(USER_OPTION, true, "User");
    userOpt.setRequired(true);
    Option workingDirOpt = new Option(WORKING_DIR_OPTION, true,
        "Working Directory");
    workingDirOpt.setRequired(true);
    Option remoteLogDirOpt = new Option(REMOTE_ROOT_LOG_DIR_OPTION, true,
        "Remote Root Log Directory");
    remoteLogDirOpt.setRequired(true);
    Option suffixOpt = new Option(SUFFIX_OPTION, true, "Suffix");
    suffixOpt.setRequired(true);
    Option useProxyOpt = new Option(NO_PROXY_OPTION, false, "Use Proxy");
    opts.addOption(appIdOpt);
    opts.addOption(userOpt);
    opts.addOption(workingDirOpt);
    opts.addOption(remoteLogDirOpt);
    opts.addOption(suffixOpt);
    opts.addOption(useProxyOpt);

    CommandLineParser parser = new GnuParser();
    CommandLine commandLine = parser.parse(opts, args);
    appId = commandLine.getOptionValue(APP_ID_OPTION);
    user = commandLine.getOptionValue(USER_OPTION);
    workingDir = commandLine.getOptionValue(WORKING_DIR_OPTION);
    remoteLogDir = commandLine.getOptionValue(REMOTE_ROOT_LOG_DIR_OPTION);
    suffix = commandLine.getOptionValue(SUFFIX_OPTION);
    proxy = true;
    if (commandLine.hasOption(NO_PROXY_OPTION)) {
      proxy = false;
    }
  }

  @Override
  public void setConf(Configuration conf) {
    if (conf instanceof JobConf) {
      this.conf = (JobConf) conf;
    } else {
      this.conf = new JobConf(conf, HadoopArchiveLogsRunner.class);
    }
  }

  @Override
  public Configuration getConf() {
    return this.conf;
  }
}
