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

import static org.apache.hadoop.yarn.conf.YarnConfiguration.LOG_AGGREGATION_FILE_CONTROLLER_FMT;
import static org.apache.hadoop.yarn.conf.YarnConfiguration.LOG_AGGREGATION_REMOTE_APP_LOG_DIR_FMT;
import static org.apache.hadoop.yarn.conf.YarnConfiguration.LOG_AGGREGATION_REMOTE_APP_LOG_DIR_SUFFIX_FMT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.Writer;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
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
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test LogAggregationFileControllerFactory.
 */
public class TestLogAggregationFileControllerFactory extends Configured {
  private static final Logger LOG = LoggerFactory.getLogger(
      TestLogAggregationFileControllerFactory.class);

  private static final String REMOTE_LOG_ROOT = "target/app-logs/";
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

  @Before
  public void setup() throws IOException {
    Configuration conf = new YarnConfiguration();
    conf.setBoolean(YarnConfiguration.LOG_AGGREGATION_ENABLED, true);
    conf.set(YarnConfiguration.NM_REMOTE_APP_LOG_DIR, REMOTE_LOG_ROOT +
        REMOTE_DEFAULT_DIR);
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
      assertTrue("The used LogAggregationFileController is not instance of "
          + className.getSimpleName(), className.isInstance(
              factory.getFileControllerForRead(appId, APP_OWNER)));
    } finally {
      fs.delete(logPath, true);
    }
  }

  @Test
  public void testDefaultLogAggregationFileControllerFactory()
      throws IOException {
    LogAggregationFileControllerFactory factory =
        new LogAggregationFileControllerFactory(getConf());
    List<LogAggregationFileController> list = factory
        .getConfiguredLogAggregationFileControllerList();

    assertEquals("Only one LogAggregationFileController is expected!", 1,
        list.size());
    assertTrue("TFile format is expected to be the first " +
        "LogAggregationFileController!", list.get(0) instanceof
        LogAggregationTFileController);
    assertTrue("TFile format is expected to be used for writing!",
        factory.getFileControllerForWrite() instanceof
            LogAggregationTFileController);

    verifyFileControllerInstance(factory, LogAggregationTFileController.class);
  }

  @Test(expected = Exception.class)
  public void testLogAggregationFileControllerFactoryClassNotSet() {
    Configuration conf = getConf();
    conf.set(YarnConfiguration.LOG_AGGREGATION_FILE_FORMATS,
        "TestLogAggregationFileController");
    new LogAggregationFileControllerFactory(conf);
    fail("TestLogAggregationFileController's class was not set, " +
        "but the factory creation did not fail.");
  }

  private void enableFileControllers(
      List<Class<? extends LogAggregationFileController>> fileControllers,
      List<String> fileControllerNames) {
    Configuration conf = getConf();
    conf.set(YarnConfiguration.LOG_AGGREGATION_FILE_FORMATS,
        StringUtils.join(fileControllerNames, ","));
    for (int i = 0; i < fileControllers.size(); i++) {
      Class<? extends LogAggregationFileController> fileController =
          fileControllers.get(i);
      String controllerName = fileControllerNames.get(i);

      conf.setClass(String.format(LOG_AGGREGATION_FILE_CONTROLLER_FMT,
          controllerName), fileController, LogAggregationFileController.class);
      conf.set(String.format(LOG_AGGREGATION_REMOTE_APP_LOG_DIR_FMT,
          controllerName), REMOTE_LOG_ROOT + controllerName + "/");
      conf.set(String.format(LOG_AGGREGATION_REMOTE_APP_LOG_DIR_SUFFIX_FMT,
          controllerName), controllerName);
    }
  }

  @Test
  public void testLogAggregationFileControllerFactory() throws Exception {
    enableFileControllers(ALL_FILE_CONTROLLERS, ALL_FILE_CONTROLLER_NAMES);
    LogAggregationFileControllerFactory factory =
        new LogAggregationFileControllerFactory(getConf());
    List<LogAggregationFileController> list =
        factory.getConfiguredLogAggregationFileControllerList();

    assertEquals("The expected number of LogAggregationFileController " +
        "is not 3!", 3, list.size());
    assertTrue("Test format is expected to be the first " +
        "LogAggregationFileController!", list.get(0) instanceof
        TestLogAggregationFileController);
    assertTrue("IFile format is expected to be the second " +
        "LogAggregationFileController!", list.get(1) instanceof
        LogAggregationIndexedFileController);
    assertTrue("TFile format is expected to be the first " +
        "LogAggregationFileController!", list.get(2) instanceof
        LogAggregationTFileController);
    assertTrue("Test format is expected to be used for writing!",
        factory.getFileControllerForWrite() instanceof
            TestLogAggregationFileController);

    verifyFileControllerInstance(factory,
        TestLogAggregationFileController.class);
  }

  @Test
  public void testClassConfUsed() {
    enableFileControllers(Collections.singletonList(
        LogAggregationTFileController.class),
        Collections.singletonList("TFile"));
    LogAggregationFileControllerFactory factory =
        new LogAggregationFileControllerFactory(getConf());
    LogAggregationFileController fc = factory.getFileControllerForWrite();

    assertEquals(WRONG_ROOT_LOG_DIR_MSG, "target/app-logs/TFile",
        fc.getRemoteRootLogDir().toString());
    assertEquals(WRONG_ROOT_LOG_DIR_SUFFIX_MSG, "TFile",
        fc.getRemoteRootLogDirSuffix());
  }

  @Test
  public void testNodemanagerConfigurationIsUsed() {
    Configuration conf = getConf();
    conf.set(YarnConfiguration.LOG_AGGREGATION_FILE_FORMATS, "TFile");
    LogAggregationFileControllerFactory factory =
        new LogAggregationFileControllerFactory(conf);
    LogAggregationFileController fc = factory.getFileControllerForWrite();

    assertEquals(WRONG_ROOT_LOG_DIR_MSG, "target/app-logs/default",
        fc.getRemoteRootLogDir().toString());
    assertEquals(WRONG_ROOT_LOG_DIR_SUFFIX_MSG, "log-tfile",
        fc.getRemoteRootLogDirSuffix());
  }

  @Test
  public void testDefaultConfUsed() {
    Configuration conf = getConf();
    conf.unset(YarnConfiguration.NM_REMOTE_APP_LOG_DIR);
    conf.unset(YarnConfiguration.NM_REMOTE_APP_LOG_DIR_SUFFIX);
    conf.set(YarnConfiguration.LOG_AGGREGATION_FILE_FORMATS, "TFile");

    LogAggregationFileControllerFactory factory =
        new LogAggregationFileControllerFactory(getConf());
    LogAggregationFileController fc = factory.getFileControllerForWrite();

    assertEquals(WRONG_ROOT_LOG_DIR_MSG, "/tmp/logs",
        fc.getRemoteRootLogDir().toString());
    assertEquals(WRONG_ROOT_LOG_DIR_SUFFIX_MSG, "logs-tfile",
        fc.getRemoteRootLogDirSuffix());
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
