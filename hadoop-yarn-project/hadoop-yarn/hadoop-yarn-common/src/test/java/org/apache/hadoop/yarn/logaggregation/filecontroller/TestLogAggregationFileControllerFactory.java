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

package org.apache.hadoop.yarn.logaggregation.filecontroller;

import static org.junit.Assert.*;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.Writer;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.ApplicationAccessType;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.logaggregation.AggregatedLogFormat.LogKey;
import org.apache.hadoop.yarn.logaggregation.AggregatedLogFormat.LogValue;
import org.apache.hadoop.yarn.logaggregation.ContainerLogMeta;
import org.apache.hadoop.yarn.logaggregation.ContainerLogsRequest;
import org.apache.hadoop.yarn.logaggregation.filecontroller.LogAggregationFileController;
import org.apache.hadoop.yarn.logaggregation.filecontroller.LogAggregationFileControllerContext;
import org.apache.hadoop.yarn.logaggregation.filecontroller.LogAggregationFileControllerFactory;
import org.apache.hadoop.yarn.logaggregation.filecontroller.tfile.LogAggregationTFileController;
import org.apache.hadoop.yarn.webapp.View.ViewContext;
import org.apache.hadoop.yarn.webapp.view.HtmlBlock.Block;
import org.junit.Test;

/**
 * Test LogAggregationFileControllerFactory.
 *
 */
public class TestLogAggregationFileControllerFactory {

  @Test(timeout = 10000)
  public void testLogAggregationFileControllerFactory() throws Exception {
    ApplicationId appId = ApplicationId.newInstance(
        System.currentTimeMillis(), 1);
    String appOwner = "test";
    String remoteLogRootDir = "target/app-logs/";
    Configuration conf = new YarnConfiguration();
    conf.setBoolean(YarnConfiguration.LOG_AGGREGATION_ENABLED, true);
    conf.set(YarnConfiguration.NM_REMOTE_APP_LOG_DIR, remoteLogRootDir);
    conf.set(YarnConfiguration.NM_REMOTE_APP_LOG_DIR_SUFFIX, "log");
    FileSystem fs = FileSystem.get(conf);

    LogAggregationFileControllerFactory factory =
        new LogAggregationFileControllerFactory(conf);
    LinkedList<LogAggregationFileController> list = factory
        .getConfiguredLogAggregationFileControllerList();
    assertTrue(list.size() == 1);
    assertTrue(list.getFirst() instanceof LogAggregationTFileController);
    assertTrue(factory.getFileControllerForWrite()
        instanceof LogAggregationTFileController);
    Path logPath = list.getFirst().getRemoteAppLogDir(appId, appOwner);
    try {
      if (fs.exists(logPath)) {
        fs.delete(logPath, true);
      }
      assertTrue(fs.mkdirs(logPath));
      Writer writer =
          new FileWriter(new File(logPath.toString(), "testLog"));
      writer.write("test");
      writer.close();
      assertTrue(factory.getFileControllerForRead(appId, appOwner)
          instanceof LogAggregationTFileController);
    } finally {
      fs.delete(logPath, true);
    }

    conf.set(YarnConfiguration.LOG_AGGREGATION_FILE_FORMATS,
        "TestLogAggregationFileController");
    // Did not set class for TestLogAggregationFileController,
    // should get the exception.
    try {
      factory =
          new LogAggregationFileControllerFactory(conf);
      fail();
    } catch (Exception ex) {
      // should get exception
    }

    conf.set(YarnConfiguration.LOG_AGGREGATION_FILE_FORMATS,
        "TestLogAggregationFileController,TFile");
    conf.setClass(
        "yarn.log-aggregation.file-controller.TestLogAggregationFileController"
        + ".class", TestLogAggregationFileController.class,
        LogAggregationFileController.class);

    conf.set(
        "yarn.log-aggregation.TestLogAggregationFileController"
        + ".remote-app-log-dir", remoteLogRootDir);
    conf.set(
        "yarn.log-aggregation.TestLogAggregationFileController"
        + ".remote-app-log-dir-suffix", "testLog");

    factory = new LogAggregationFileControllerFactory(conf);
    list = factory.getConfiguredLogAggregationFileControllerList();
    assertTrue(list.size() == 2);
    assertTrue(list.getFirst() instanceof TestLogAggregationFileController);
    assertTrue(list.getLast() instanceof LogAggregationTFileController);
    assertTrue(factory.getFileControllerForWrite()
        instanceof TestLogAggregationFileController);

    logPath = list.getFirst().getRemoteAppLogDir(appId, appOwner);
    try {
      if (fs.exists(logPath)) {
        fs.delete(logPath, true);
      }
      assertTrue(fs.mkdirs(logPath));
      Writer writer =
          new FileWriter(new File(logPath.toString(), "testLog"));
      writer.write("test");
      writer.close();
      assertTrue(factory.getFileControllerForRead(appId, appOwner)
          instanceof TestLogAggregationFileController);
    } finally {
      fs.delete(logPath, true);
    }
  }

  private static class TestLogAggregationFileController
      extends LogAggregationFileController {

    @Override
    public void initInternal(Configuration conf) {
      String remoteDirStr = String.format(
          YarnConfiguration.LOG_AGGREGATION_REMOTE_APP_LOG_DIR_FMT,
          this.fileControllerName);
      this.remoteRootLogDir = new Path(conf.get(remoteDirStr));
      String suffix = String.format(
          YarnConfiguration.LOG_AGGREGATION_REMOTE_APP_LOG_DIR_SUFFIX_FMT,
           this.fileControllerName);
      this.remoteRootLogDirSuffix = conf.get(suffix);
    }

    @Override
    public void closeWriter() {
      // Do Nothing
    }

    @Override
    public void write(LogKey logKey, LogValue logValue) throws IOException {
      // Do Nothing
    }

    @Override
    public void postWrite(LogAggregationFileControllerContext record)
        throws Exception {
      // Do Nothing
    }

    @Override
    public void initializeWriter(LogAggregationFileControllerContext context)
        throws IOException {
      // Do Nothing
    }

    @Override
    public boolean readAggregatedLogs(ContainerLogsRequest logRequest,
        OutputStream os) throws IOException {
      return false;
    }

    @Override
    public List<ContainerLogMeta> readAggregatedLogsMeta(
        ContainerLogsRequest logRequest) throws IOException {
      return null;
    }

    @Override
    public void renderAggregatedLogsBlock(Block html, ViewContext context) {
      // DO NOTHING
    }

    @Override
    public String getApplicationOwner(Path aggregatedLogPath,
        ApplicationId appId)
        throws IOException {
      return null;
    }

    @Override
    public Map<ApplicationAccessType, String> getApplicationAcls(
        Path aggregatedLogPath, ApplicationId appId) throws IOException {
      return null;
    }
  }
}
