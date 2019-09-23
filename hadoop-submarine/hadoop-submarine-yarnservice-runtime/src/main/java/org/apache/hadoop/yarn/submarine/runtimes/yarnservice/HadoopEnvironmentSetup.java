/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.submarine.runtimes.yarnservice;

import org.apache.curator.shaded.com.google.common.collect.ImmutableList;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.service.api.records.Component;
import org.apache.hadoop.yarn.submarine.client.cli.param.runjob.RunJobParameters;
import org.apache.hadoop.yarn.submarine.common.ClientContext;
import org.apache.hadoop.yarn.submarine.common.conf.SubmarineLogs;
import org.apache.hadoop.yarn.submarine.common.fs.RemoteDirectoryManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.List;
import java.util.Objects;

import static org.apache.hadoop.yarn.submarine.runtimes.yarnservice.FileSystemOperations.needHdfs;
import static org.apache.hadoop.yarn.submarine.utils.ClassPathUtilities.findFileOnClassPath;
import static org.apache.hadoop.yarn.submarine.utils.EnvironmentUtilities.getValueOfEnvironment;

/**
 * This class contains helper methods to fill HDFS and Java environment
 * variables into scripts.
 */
public class HadoopEnvironmentSetup {
  private static final Logger LOG =
      LoggerFactory.getLogger(HadoopEnvironmentSetup.class);
  private static final String CORE_SITE_XML = "core-site.xml";
  private static final String HDFS_SITE_XML = "hdfs-site.xml";

  public static final String DOCKER_HADOOP_HDFS_HOME =
      "DOCKER_HADOOP_HDFS_HOME";
  public static final String DOCKER_JAVA_HOME = "DOCKER_JAVA_HOME";
  private final RemoteDirectoryManager remoteDirectoryManager;
  private final FileSystemOperations fsOperations;

  public HadoopEnvironmentSetup(ClientContext clientContext,
      FileSystemOperations fsOperations) {
    this.remoteDirectoryManager = clientContext.getRemoteDirectoryManager();
    this.fsOperations = fsOperations;
  }

  public void addHdfsClassPath(RunJobParameters parameters,
      PrintWriter fw, Component comp) throws IOException {
    // Find envs to use HDFS
    String hdfsHome = null;
    String javaHome = null;

    boolean hadoopEnv = false;

    for (String envVar : parameters.getEnvars()) {
      if (envVar.startsWith(DOCKER_HADOOP_HDFS_HOME + "=")) {
        hdfsHome = getValueOfEnvironment(envVar);
        hadoopEnv = true;
      } else if (envVar.startsWith(DOCKER_JAVA_HOME + "=")) {
        javaHome = getValueOfEnvironment(envVar);
      }
    }

    boolean hasHdfsEnvs = hdfsHome != null && javaHome != null;
    boolean needHdfs = doesNeedHdfs(parameters, hadoopEnv);
    if (needHdfs) {
      // HDFS is asked either in input or output, set LD_LIBRARY_PATH
      // and classpath
      if (hdfsHome != null) {
        appendHdfsHome(fw, hdfsHome);
      }

      // hadoop confs will be uploaded to HDFS and localized to container's
      // local folder, so here set $HADOOP_CONF_DIR to $WORK_DIR.
      fw.append("export HADOOP_CONF_DIR=$WORK_DIR\n");
      if (javaHome != null) {
        appendJavaHome(fw, javaHome);
      }

      fw.append(
          "export CLASSPATH=`$HADOOP_HDFS_HOME/bin/hadoop classpath --glob`\n");
    }

    if (needHdfs && !hasHdfsEnvs) {
      LOG.error("When HDFS is being used to read/write models/data, " +
          "the following environment variables are required: " +
          "1) {}=<HDFS_HOME inside docker container> " +
          "2) {}=<JAVA_HOME inside docker container>. " +
          "You can use --env to pass these environment variables.",
          DOCKER_HADOOP_HDFS_HOME, DOCKER_JAVA_HOME);
      throw new IOException("Failed to detect HDFS-related environments.");
    }

    // Trying to upload core-site.xml and hdfs-site.xml
    Path stagingDir =
        remoteDirectoryManager.getJobStagingArea(
            parameters.getName(), true);
    File coreSite = findFileOnClassPath(CORE_SITE_XML);
    File hdfsSite = findFileOnClassPath(HDFS_SITE_XML);
    if (coreSite == null || hdfsSite == null) {
      LOG.error("HDFS is being used, however we could not locate " +
          "{} nor {} on classpath! " +
          "Please double check your classpath setting and make sure these " +
          "setting files are included!", CORE_SITE_XML, HDFS_SITE_XML);
      throw new IOException(
          "Failed to locate core-site.xml / hdfs-site.xml on classpath!");
    }
    fsOperations.uploadToRemoteFileAndLocalizeToContainerWorkDir(stagingDir,
        coreSite.getAbsolutePath(), CORE_SITE_XML, comp);
    fsOperations.uploadToRemoteFileAndLocalizeToContainerWorkDir(stagingDir,
        hdfsSite.getAbsolutePath(), HDFS_SITE_XML, comp);

    // DEBUG
    if (SubmarineLogs.isVerbose()) {
      appendEchoOfEnvVars(fw);
    }
  }

  private boolean doesNeedHdfs(RunJobParameters parameters, boolean hadoopEnv) {
    List<String> launchCommands = parameters.getLaunchCommands();
    if (launchCommands != null) {
      launchCommands.removeIf(Objects::isNull);
    }

    ImmutableList.Builder<String> listBuilder = ImmutableList.builder();

    if (launchCommands != null && !launchCommands.isEmpty()) {
      listBuilder.addAll(launchCommands);
    }
    if (parameters.getInputPath() != null) {
      listBuilder.add(parameters.getInputPath());
    }
    List<String> stringsToCheck = listBuilder.build();

    return needHdfs(stringsToCheck) || hadoopEnv;
  }

  private void appendHdfsHome(PrintWriter fw, String hdfsHome) {
    // Unset HADOOP_HOME/HADOOP_YARN_HOME to make sure host machine's envs
    // won't pollute docker's env.
    fw.append("export HADOOP_HOME=\n");
    fw.append("export HADOOP_YARN_HOME=\n");
    fw.append("export HADOOP_HDFS_HOME=" + hdfsHome + "\n");
    fw.append("export HADOOP_COMMON_HOME=" + hdfsHome + "\n");
  }

  private void appendJavaHome(PrintWriter fw, String javaHome) {
    fw.append("export JAVA_HOME=" + javaHome + "\n");
    fw.append("export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:"
        + "$JAVA_HOME/lib/amd64/server\n");
  }

  private void appendEchoOfEnvVars(PrintWriter fw) {
    fw.append("echo \"CLASSPATH:$CLASSPATH\"\n");
    fw.append("echo \"HADOOP_CONF_DIR:$HADOOP_CONF_DIR\"\n");
    fw.append(
        "echo \"HADOOP_TOKEN_FILE_LOCATION:$HADOOP_TOKEN_FILE_LOCATION\"\n");
    fw.append("echo \"JAVA_HOME:$JAVA_HOME\"\n");
    fw.append("echo \"LD_LIBRARY_PATH:$LD_LIBRARY_PATH\"\n");
    fw.append("echo \"HADOOP_HDFS_HOME:$HADOOP_HDFS_HOME\"\n");
  }
}
