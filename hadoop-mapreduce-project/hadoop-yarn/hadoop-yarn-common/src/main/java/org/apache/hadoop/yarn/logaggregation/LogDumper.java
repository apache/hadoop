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

package org.apache.hadoop.yarn.logaggregation;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.logaggregation.AggregatedLogFormat.LogKey;
import org.apache.hadoop.yarn.logaggregation.AggregatedLogFormat.LogReader;
import org.apache.hadoop.yarn.util.ConverterUtils;

public class LogDumper extends Configured implements Tool {

  private static final String CONTAINER_ID_OPTION = "containerId";
  private static final String APPLICATION_ID_OPTION = "applicationId";
  private static final String NODE_ADDRESS_OPTION = "nodeAddress";
  private static final String APP_OWNER_OPTION = "appOwner";

  @Override
  public int run(String[] args) throws Exception {

    Options opts = new Options();
    opts.addOption(APPLICATION_ID_OPTION, true, "ApplicationId");
    opts.addOption(CONTAINER_ID_OPTION, true, "ContainerId");
    opts.addOption(NODE_ADDRESS_OPTION, true, "NodeAddress");
    opts.addOption(APP_OWNER_OPTION, true, "AppOwner");

    if (args.length < 1) {
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp("general options are: ", opts);
      return -1;
    }

    CommandLineParser parser = new GnuParser();
    String appIdStr = null;
    String containerIdStr = null;
    String nodeAddress = null;
    String appOwner = null;
    try {
      CommandLine commandLine = parser.parse(opts, args, true);
      appIdStr = commandLine.getOptionValue(APPLICATION_ID_OPTION);
      containerIdStr = commandLine.getOptionValue(CONTAINER_ID_OPTION);
      nodeAddress = commandLine.getOptionValue(NODE_ADDRESS_OPTION);
      appOwner = commandLine.getOptionValue(APP_OWNER_OPTION);
    } catch (ParseException e) {
      System.out.println("options parsing failed: " + e.getMessage());

      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp("general options are: ", opts);
      return -1;
    }

    if (appIdStr == null) {
      System.out.println("ApplicationId cannot be null!");
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp("general options are: ", opts);
      return -1;
    }

    RecordFactory recordFactory =
        RecordFactoryProvider.getRecordFactory(getConf());
    ApplicationId appId =
        ConverterUtils.toApplicationId(recordFactory, appIdStr);

    DataOutputStream out = new DataOutputStream(System.out);

    if (appOwner == null || appOwner.isEmpty()) {
      appOwner = UserGroupInformation.getCurrentUser().getShortUserName();
    }
    if (containerIdStr == null && nodeAddress == null) {
      dumpAllContainersLogs(appId, appOwner, out);
    } else if ((containerIdStr == null && nodeAddress != null)
        || (containerIdStr != null && nodeAddress == null)) {
      System.out.println("ContainerId or NodeAddress cannot be null!");
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp("general options are: ", opts);
      return -1;
    } else {
      Path remoteRootLogDir =
        new Path(getConf().get(YarnConfiguration.NM_REMOTE_APP_LOG_DIR,
            YarnConfiguration.DEFAULT_NM_REMOTE_APP_LOG_DIR));
      AggregatedLogFormat.LogReader reader =
          new AggregatedLogFormat.LogReader(getConf(),
              LogAggregationUtils.getRemoteNodeLogFileForApp(
                  remoteRootLogDir,
                  appId,
                  appOwner,
                  ConverterUtils.toNodeId(nodeAddress),
                  getConf().get(YarnConfiguration.NM_REMOTE_APP_LOG_DIR_SUFFIX,
                      YarnConfiguration.DEFAULT_NM_REMOTE_APP_LOG_DIR_SUFFIX)));
      return dumpAContainerLogs(containerIdStr, reader, out);
    }

    return 0;
  }

  public void dumpAContainersLogs(String appId, String containerId,
      String nodeId, String jobOwner) throws IOException {
    Path remoteRootLogDir =
        new Path(getConf().get(YarnConfiguration.NM_REMOTE_APP_LOG_DIR,
            YarnConfiguration.DEFAULT_NM_REMOTE_APP_LOG_DIR));
    String suffix = LogAggregationUtils.getRemoteNodeLogDirSuffix(getConf());
    AggregatedLogFormat.LogReader reader =
        new AggregatedLogFormat.LogReader(getConf(),
            LogAggregationUtils.getRemoteNodeLogFileForApp(remoteRootLogDir,
                ConverterUtils.toApplicationId(appId), jobOwner,
                ConverterUtils.toNodeId(nodeId), suffix));
    DataOutputStream out = new DataOutputStream(System.out);
    dumpAContainerLogs(containerId, reader, out);
  }

  private int dumpAContainerLogs(String containerIdStr,
      AggregatedLogFormat.LogReader reader, DataOutputStream out)
      throws IOException {
    DataInputStream valueStream;
    LogKey key = new LogKey();
    valueStream = reader.next(key);

    while (valueStream != null && !key.toString().equals(containerIdStr)) {
      // Next container
      key = new LogKey();
      valueStream = reader.next(key);
    }

    if (valueStream == null) {
      System.out.println("Logs for container " + containerIdStr
          + " are not present in this log-file.");
      return -1;
    }

    while (true) {
      try {
        LogReader.readAContainerLogsForALogType(valueStream, out);
      } catch (EOFException eof) {
        break;
      }
    }
    return 0;
  }

  private void dumpAllContainersLogs(ApplicationId appId, String appOwner,
      DataOutputStream out) throws IOException {
    Path remoteRootLogDir =
        new Path(getConf().get(YarnConfiguration.NM_REMOTE_APP_LOG_DIR,
            YarnConfiguration.DEFAULT_NM_REMOTE_APP_LOG_DIR));
    String user = appOwner;
    String logDirSuffix =
        getConf().get(YarnConfiguration.NM_REMOTE_APP_LOG_DIR,
            YarnConfiguration.DEFAULT_NM_REMOTE_APP_LOG_DIR_SUFFIX);
    //TODO Change this to get a list of files from the LAS.
    Path remoteAppLogDir =
        LogAggregationUtils.getRemoteAppLogDir(remoteRootLogDir, appId, user,
            logDirSuffix);
    RemoteIterator<FileStatus> nodeFiles =
        FileContext.getFileContext().listStatus(remoteAppLogDir);
    while (nodeFiles.hasNext()) {
      FileStatus thisNodeFile = nodeFiles.next();
      AggregatedLogFormat.LogReader reader =
          new AggregatedLogFormat.LogReader(getConf(),
              new Path(remoteAppLogDir, thisNodeFile.getPath().getName()));
      try {

        DataInputStream valueStream;
        LogKey key = new LogKey();
        valueStream = reader.next(key);

        while (valueStream != null) {
          while (true) {
            try {
              LogReader.readAContainerLogsForALogType(valueStream, out);
            } catch (EOFException eof) {
              break;
            }
          }

          // Next container
          key = new LogKey();
          valueStream = reader.next(key);
        }
      } finally {
        reader.close();
      }
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new YarnConfiguration();
    LogDumper logDumper = new LogDumper();
    logDumper.setConf(conf);
    logDumper.run(args);
  }
}
