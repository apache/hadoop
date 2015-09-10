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

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.File;

/**
 * This is a child program designed to be used by the {@link HadoopArchiveLogs}
 * tool via the Distributed Shell.  It's not meant to be run directly.
 */
public class HadoopArchiveLogsRunner implements Tool {
  private static final Log LOG = LogFactory.getLog(HadoopArchiveLogsRunner.class);

  private static final String APP_ID_OPTION = "appId";
  private static final String USER_OPTION = "user";
  private static final String WORKING_DIR_OPTION = "workingDir";
  private static final String REMOTE_ROOT_LOG_DIR = "remoteRootLogDir";
  private static final String SUFFIX_OPTION = "suffix";

  private String appId;
  private String user;
  private String workingDir;
  private String remoteLogDir;
  private String suffix;

  private JobConf conf;

  public HadoopArchiveLogsRunner(Configuration conf) {
    setConf(conf);
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
    String remoteAppLogDir = remoteLogDir + File.separator + user
        + File.separator + suffix + File.separator + appId;

    // Run 'hadoop archives' command in local mode
    Configuration haConf = new Configuration(getConf());
    haConf.set("mapreduce.framework.name", "local");
    HadoopArchives ha = new HadoopArchives(haConf);
    String[] haArgs = {
        "-archiveName",
        appId + ".har",
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
    ha.run(haArgs);

    FileSystem fs = null;
    // Move har file to correct location and delete original logs
    try {
      fs = FileSystem.get(conf);
      LOG.info("Moving har to original location");
      fs.rename(new Path(workingDir, appId + ".har"),
          new Path(remoteAppLogDir, appId + ".har"));
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
    Option remoteLogDirOpt = new Option(REMOTE_ROOT_LOG_DIR, true,
        "Remote Root Log Directory");
    remoteLogDirOpt.setRequired(true);
    Option suffixOpt = new Option(SUFFIX_OPTION, true, "Suffix");
    suffixOpt.setRequired(true);
    opts.addOption(appIdOpt);
    opts.addOption(userOpt);
    opts.addOption(workingDirOpt);
    opts.addOption(remoteLogDirOpt);
    opts.addOption(suffixOpt);

    CommandLineParser parser = new GnuParser();
    CommandLine commandLine = parser.parse(opts, args);
    appId = commandLine.getOptionValue(APP_ID_OPTION);
    user = commandLine.getOptionValue(USER_OPTION);
    workingDir = commandLine.getOptionValue(WORKING_DIR_OPTION);
    remoteLogDir = commandLine.getOptionValue(REMOTE_ROOT_LOG_DIR);
    suffix = commandLine.getOptionValue(SUFFIX_OPTION);
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
