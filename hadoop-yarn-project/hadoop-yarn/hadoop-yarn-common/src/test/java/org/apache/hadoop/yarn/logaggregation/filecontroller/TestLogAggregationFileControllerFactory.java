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

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.Writer;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.ApplicationAccessType;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.logaggregation.AggregatedLogFormat.LogKey;
import org.apache.hadoop.yarn.logaggregation.AggregatedLogFormat.LogValue;
import org.apache.hadoop.yarn.logaggregation.ContainerLogMeta;
import org.apache.hadoop.yarn.logaggregation.ContainerLogsRequest;
import org.apache.hadoop.yarn.logaggregation.filecontroller.ifile.LogAggregationIndexedFileController;
import org.apache.hadoop.yarn.logaggregation.filecontroller.tfile.LogAggregationTFileController;
import org.apache.hadoop.yarn.webapp.View.ViewContext;
import org.apache.hadoop.yarn.webapp.view.HtmlBlock.Block;

import static org.apache.hadoop.yarn.conf.YarnConfiguration.LOG_AGGREGATION_FILE_FORMATS;
import static org.apache.hadoop.yarn.logaggregation.LogAggregationTestUtils.REMOTE_LOG_ROOT;
import static org.apache.hadoop.yarn.logaggregation.LogAggregationTestUtils.enableFileControllers;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Test LogAggregationFileControllerFactory.
 */
public class TestLogAggregationFileControllerFactory extends Configured {
  private static final Logger LOG = LoggerFactory.getLogger(
      TestLogAggregationFileControllerFactory.class);

  private static final String REMOTE_DEFAULT_DIR = "default/";
  private static final String APP_OWNER = "test";

  private static final String WRONG_ROOT_LOG_DIR_MSG =
      "Wrong remote root log directory found.";
  private static final String WRONG_ROOT_LOG_DIR_SUFFIX_MSG =
      "Wrong remote root log directory suffix found.";

  private static final List<Class<? extends LogAggregationFileController>>
      ALL_FILE_CONTROLLERS = Arrays.asList(
          TestLogAggregationFileController.class,
          LogAggregationIndexedFileController.class,
          LogAggregationTFileController.class);
  private static final List<String> ALL_FILE_CONTROLLER_NAMES =
      Arrays.asList("TestLogAggregationFileController", "IFile", "TFile");

  private ApplicationId appId = ApplicationId.newInstance(
      System.currentTimeMillis(), 1);

  @BeforeEach
  public void setup() throws IOException {
    Configuration conf = new YarnConfiguration();
    conf.setBoolean(YarnConfiguration.LOG_AGGREGATION_ENABLED, true);
    conf.set(YarnConfiguration.NM_REMOTE_APP_LOG_DIR, REMOTE_LOG_ROOT + REMOTE_DEFAULT_DIR);
    conf.set(YarnConfiguration.NM_REMOTE_APP_LOG_DIR_SUFFIX, "log");
    setConf(conf);
  }

  private void verifyFileControllerInstance(
      LogAggregationFileControllerFactory factory,
      Class<? extends LogAggregationFileController> className)
      throws IOException {
    List<LogAggregationFileController> fileControllers =
        factory.getConfiguredLogAggregationFileControllerList();
    FileSystem fs = FileSystem.get(getConf());
    Path logPath = fileControllers.get(0).getRemoteAppLogDir(appId, APP_OWNER);
    LOG.debug("Checking " + logPath);

    try {
      if (fs.exists(logPath)) {
        fs.delete(logPath, true);
      }
      assertTrue(fs.mkdirs(logPath));
      try (Writer writer =
               new FileWriter(new File(logPath.toString(), "testLog"))) {
        writer.write("test");
      }
      assertTrue(className.isInstance(factory.getFileControllerForRead(appId, APP_OWNER)),
          "The used LogAggregationFileController is not instance of " + className.getSimpleName());
    } finally {
      fs.delete(logPath, true);
    }
  }

  @Test
  void testDefaultLogAggregationFileControllerFactory()
      throws IOException {
    LogAggregationFileControllerFactory factory =
        new LogAggregationFileControllerFactory(getConf());
    List<LogAggregationFileController> list = factory
        .getConfiguredLogAggregationFileControllerList();

    assertEquals(1,
        list.size(),
        "Only one LogAggregationFileController is expected!");
    assertTrue(list.get(0) instanceof
        LogAggregationTFileController, "TFile format is expected to be the first " +
        "LogAggregationFileController!");
    assertTrue(factory.getFileControllerForWrite() instanceof
            LogAggregationTFileController,
        "TFile format is expected to be used for writing!");

    verifyFileControllerInstance(factory, LogAggregationTFileController.class);
  }

  @Test
  void testLogAggregationFileControllerFactoryClassNotSet() {
    assertThrows(Exception.class, () -> {
      Configuration conf = getConf();
      conf.set(LOG_AGGREGATION_FILE_FORMATS, "TestLogAggregationFileController");
      new LogAggregationFileControllerFactory(conf);
      fail("TestLogAggregationFileController's class was not set, " +
          "but the factory creation did not fail.");
    });
  }

  @Test
  void testLogAggregationFileControllerFactory() throws Exception {
    enableFileControllers(getConf(), ALL_FILE_CONTROLLERS, ALL_FILE_CONTROLLER_NAMES);
    LogAggregationFileControllerFactory factory =
        new LogAggregationFileControllerFactory(getConf());
    List<LogAggregationFileController> list =
        factory.getConfiguredLogAggregationFileControllerList();

    assertEquals(3, list.size(), "The expected number of LogAggregationFileController " +
        "is not 3!");
    assertTrue(list.get(0) instanceof
        TestLogAggregationFileController, "Test format is expected to be the first " +
        "LogAggregationFileController!");
    assertTrue(list.get(1) instanceof
        LogAggregationIndexedFileController, "IFile format is expected to be the second " +
        "LogAggregationFileController!");
    assertTrue(list.get(2) instanceof
        LogAggregationTFileController, "TFile format is expected to be the first " +
        "LogAggregationFileController!");
    assertTrue(factory.getFileControllerForWrite() instanceof
            TestLogAggregationFileController,
        "Test format is expected to be used for writing!");

    verifyFileControllerInstance(factory,
        TestLogAggregationFileController.class);
  }

  @Test
  void testClassConfUsed() {
    enableFileControllers(getConf(), Collections.singletonList(LogAggregationTFileController.class),
        Collections.singletonList("TFile"));
    LogAggregationFileControllerFactory factory =
        new LogAggregationFileControllerFactory(getConf());
    LogAggregationFileController fc = factory.getFileControllerForWrite();

    assertEquals("target/app-logs/TFile",
        fc.getRemoteRootLogDir().toString(),
        WRONG_ROOT_LOG_DIR_MSG);
    assertEquals("TFile",
        fc.getRemoteRootLogDirSuffix(),
        WRONG_ROOT_LOG_DIR_SUFFIX_MSG);
  }

  @Test
  void testNodemanagerConfigurationIsUsed() {
    Configuration conf = getConf();
    conf.set(LOG_AGGREGATION_FILE_FORMATS, "TFile");
    LogAggregationFileControllerFactory factory =
        new LogAggregationFileControllerFactory(conf);
    LogAggregationFileController fc = factory.getFileControllerForWrite();

    assertEquals("target/app-logs/default",
        fc.getRemoteRootLogDir().toString(),
        WRONG_ROOT_LOG_DIR_MSG);
    assertEquals("log-tfile",
        fc.getRemoteRootLogDirSuffix(),
        WRONG_ROOT_LOG_DIR_SUFFIX_MSG);
  }

  @Test
  void testDefaultConfUsed() {
    Configuration conf = getConf();
    conf.unset(YarnConfiguration.NM_REMOTE_APP_LOG_DIR);
    conf.unset(YarnConfiguration.NM_REMOTE_APP_LOG_DIR_SUFFIX);
    conf.set(LOG_AGGREGATION_FILE_FORMATS, "TFile");

    LogAggregationFileControllerFactory factory =
        new LogAggregationFileControllerFactory(getConf());
    LogAggregationFileController fc = factory.getFileControllerForWrite();

    assertEquals("/tmp/logs",
        fc.getRemoteRootLogDir().toString(),
        WRONG_ROOT_LOG_DIR_MSG);
    assertEquals("logs-tfile",
        fc.getRemoteRootLogDirSuffix(),
        WRONG_ROOT_LOG_DIR_SUFFIX_MSG);
  }

  private static class TestLogAggregationFileController
      extends LogAggregationFileController {

    @Override
    public void initInternal(Configuration conf) {
      // Do Nothing
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
    public void initializeWriter(LogAggregationFileControllerContext context) {
      // Do Nothing
    }

    @Override
    public boolean readAggregatedLogs(ContainerLogsRequest logRequest,
        OutputStream os) {
      return false;
    }

    @Override
    public List<ContainerLogMeta> readAggregatedLogsMeta(
        ContainerLogsRequest logRequest) {
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
