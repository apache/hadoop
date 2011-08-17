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

package org.apache.hadoop.yarn.server.nodemanager.containermanager.logaggregation;

import java.io.DataInputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.Writer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import junit.framework.Assert;

import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.UnsupportedFileSystemException;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.yarn.api.protocolrecords.StartContainerRequest;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.URL;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.server.nodemanager.CMgrCompletedAppsEvent;
import org.apache.hadoop.yarn.server.nodemanager.DeletionService;
import org.apache.hadoop.yarn.server.nodemanager.NMConfig;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.BaseContainerManagerTest;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.logaggregation.AggregatedLogFormat.LogKey;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.logaggregation.AggregatedLogFormat.LogReader;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.logaggregation.event.LogAggregatorAppFinishedEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.logaggregation.event.LogAggregatorAppStartedEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.logaggregation.event.LogAggregatorContainerFinishedEvent;
import org.apache.hadoop.yarn.util.BuilderUtils;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.junit.Ignore;
import org.junit.Test;

@Ignore
public class TestLogAggregationService extends BaseContainerManagerTest {

  static {
    LOG = LogFactory.getLog(TestLogAggregationService.class);
  }

  private static RecordFactory recordFactory = RecordFactoryProvider
      .getRecordFactory(null);

  private File remoteRootLogDir = new File("target", this.getClass()
      .getName() + "-remoteLogDir");

  public TestLogAggregationService() throws UnsupportedFileSystemException {
    super();
    this.remoteRootLogDir.mkdir();
  }

  @Override
  public void tearDown() throws IOException, InterruptedException {
    super.tearDown();
    createContainerExecutor().deleteAsUser(user,
        new Path(remoteRootLogDir.getAbsolutePath()), new Path[] {});
  }

  @Test
  public void testLocalFileDeletionAfterUpload() throws IOException {
    this.delSrvc = new DeletionService(createContainerExecutor());
    this.delSrvc.init(conf);
    this.conf.set(NMConfig.NM_LOG_DIR, localLogDir.getAbsolutePath());
    this.conf.set(NMConfig.REMOTE_USER_LOG_DIR,
        this.remoteRootLogDir.getAbsolutePath());
    LogAggregationService logAggregationService =
        new LogAggregationService(this.delSrvc);
    logAggregationService.init(this.conf);
    logAggregationService.start();

    ApplicationId application1 = BuilderUtils.newApplicationId(1234, 1);

    // AppLogDir should be created
    File app1LogDir =
        new File(localLogDir, ConverterUtils.toString(application1));
    app1LogDir.mkdir();
    logAggregationService
        .handle(new LogAggregatorAppStartedEvent(
            application1, this.user, null,
            ContainerLogsRetentionPolicy.ALL_CONTAINERS));

    ContainerId container11 =
        BuilderUtils.newContainerId(recordFactory, application1, 1);
    // Simulate log-file creation
    writeContainerLogs(app1LogDir, container11);
    logAggregationService.handle(new LogAggregatorContainerFinishedEvent(
        container11, "0"));

    logAggregationService.handle(new LogAggregatorAppFinishedEvent(
        application1));

    logAggregationService.stop();

    String containerIdStr = ConverterUtils.toString(container11);
    File containerLogDir = new File(app1LogDir, containerIdStr);
    for (String fileType : new String[] { "stdout", "stderr", "syslog" }) {
      Assert.assertFalse(new File(containerLogDir, fileType).exists());
    }

    Assert.assertFalse(app1LogDir.exists());

    Assert.assertTrue(new File(logAggregationService
        .getRemoteNodeLogFileForApp(application1).toUri().getPath()).exists());
  }

  @Test
  public void testNoContainerOnNode() {
    this.conf.set(NMConfig.NM_LOG_DIR, localLogDir.getAbsolutePath());
    this.conf.set(NMConfig.REMOTE_USER_LOG_DIR,
        this.remoteRootLogDir.getAbsolutePath());
    LogAggregationService logAggregationService =
        new LogAggregationService(this.delSrvc);
    logAggregationService.init(this.conf);
    logAggregationService.start();

    ApplicationId application1 = BuilderUtils.newApplicationId(1234, 1);

    // AppLogDir should be created
    File app1LogDir =
      new File(localLogDir, ConverterUtils.toString(application1));
    app1LogDir.mkdir();
    logAggregationService
        .handle(new LogAggregatorAppStartedEvent(
            application1, this.user, null,
            ContainerLogsRetentionPolicy.ALL_CONTAINERS));

    logAggregationService.handle(new LogAggregatorAppFinishedEvent(
        application1));

    logAggregationService.stop();

    Assert
        .assertFalse(new File(logAggregationService
            .getRemoteNodeLogFileForApp(application1).toUri().getPath())
            .exists());
  }

  @Test
  public void testMultipleAppsLogAggregation() throws IOException {

    this.conf.set(NMConfig.NM_LOG_DIR, localLogDir.getAbsolutePath());
    this.conf.set(NMConfig.REMOTE_USER_LOG_DIR,
        this.remoteRootLogDir.getAbsolutePath());
    LogAggregationService logAggregationService =
        new LogAggregationService(this.delSrvc);
    logAggregationService.init(this.conf);
    logAggregationService.start();

    ApplicationId application1 = BuilderUtils.newApplicationId(1234, 1);

    // AppLogDir should be created
    File app1LogDir =
      new File(localLogDir, ConverterUtils.toString(application1));
    app1LogDir.mkdir();
    logAggregationService
        .handle(new LogAggregatorAppStartedEvent(
            application1, this.user, null,
            ContainerLogsRetentionPolicy.ALL_CONTAINERS));

    ContainerId container11 =
        BuilderUtils.newContainerId(recordFactory, application1, 1);
    // Simulate log-file creation
    writeContainerLogs(app1LogDir, container11);
    logAggregationService.handle(new LogAggregatorContainerFinishedEvent(
        container11, "0"));

    ApplicationId application2 = BuilderUtils.newApplicationId(1234, 2);

    File app2LogDir =
      new File(localLogDir, ConverterUtils.toString(application2));
    app2LogDir.mkdir();
    logAggregationService.handle(new LogAggregatorAppStartedEvent(
        application2, this.user, null,
        ContainerLogsRetentionPolicy.APPLICATION_MASTER_ONLY));

    ContainerId container21 =
        BuilderUtils.newContainerId(recordFactory, application2, 1);
    writeContainerLogs(app2LogDir, container21);
    logAggregationService.handle(new LogAggregatorContainerFinishedEvent(
        container21, "0"));

    ContainerId container12 =
        BuilderUtils.newContainerId(recordFactory, application1, 2);
    writeContainerLogs(app1LogDir, container12);
    logAggregationService.handle(new LogAggregatorContainerFinishedEvent(
        container12, "0"));

    ApplicationId application3 = BuilderUtils.newApplicationId(1234, 3);

    File app3LogDir =
      new File(localLogDir, ConverterUtils.toString(application3));
    app3LogDir.mkdir();
    logAggregationService.handle(new LogAggregatorAppStartedEvent(
        application3, this.user, null,
        ContainerLogsRetentionPolicy.AM_AND_FAILED_CONTAINERS_ONLY));

    ContainerId container31 =
        BuilderUtils.newContainerId(recordFactory, application3, 1);
    writeContainerLogs(app3LogDir, container31);
    logAggregationService.handle(new LogAggregatorContainerFinishedEvent(
        container31, "0"));

    ContainerId container32 =
        BuilderUtils.newContainerId(recordFactory, application3, 2);
    writeContainerLogs(app3LogDir, container32);
    logAggregationService.handle(new LogAggregatorContainerFinishedEvent(
        container32, "1")); // Failed container

    ContainerId container22 =
        BuilderUtils.newContainerId(recordFactory, application2, 2);
    writeContainerLogs(app2LogDir, container22);
    logAggregationService.handle(new LogAggregatorContainerFinishedEvent(
        container22, "0"));

    ContainerId container33 =
        BuilderUtils.newContainerId(recordFactory, application3, 3);
    writeContainerLogs(app3LogDir, container33);
    logAggregationService.handle(new LogAggregatorContainerFinishedEvent(
        container33, "0"));

    logAggregationService.handle(new LogAggregatorAppFinishedEvent(
        application2));
    logAggregationService.handle(new LogAggregatorAppFinishedEvent(
        application3));
    logAggregationService.handle(new LogAggregatorAppFinishedEvent(
        application1));

    logAggregationService.stop();

    verifyContainerLogs(logAggregationService, application1,
        new ContainerId[] { container11, container12 });
    verifyContainerLogs(logAggregationService, application2,
        new ContainerId[] { container21 });
    verifyContainerLogs(logAggregationService, application3,
        new ContainerId[] { container31, container32 });
  }

  private void writeContainerLogs(File appLogDir, ContainerId containerId)
      throws IOException {
    // ContainerLogDir should be created
    String containerStr = ConverterUtils.toString(containerId);
    File containerLogDir = new File(appLogDir, containerStr);
    containerLogDir.mkdir();
    for (String fileType : new String[] { "stdout", "stderr", "syslog" }) {
      Writer writer11 = new FileWriter(new File(containerLogDir, fileType));
      writer11.write(containerStr + " Hello " + fileType + "!");
      writer11.close();
    }
  }

  private void verifyContainerLogs(
      LogAggregationService logAggregationService, ApplicationId appId,
      ContainerId[] expectedContainerIds) throws IOException {
    AggregatedLogFormat.LogReader reader =
        new AggregatedLogFormat.LogReader(this.conf,
            logAggregationService.getRemoteNodeLogFileForApp(appId));
    try {
      Map<String, Map<String, String>> logMap =
          new HashMap<String, Map<String, String>>();
      DataInputStream valueStream;

      LogKey key = new LogKey();
      valueStream = reader.next(key);

      while (valueStream != null) {
        LOG.info("Found container " + key.toString());
        Map<String, String> perContainerMap = new HashMap<String, String>();
        logMap.put(key.toString(), perContainerMap);

        while (true) {
          try {
            DataOutputBuffer dob = new DataOutputBuffer();
            LogReader.readAContainerLogsForALogType(valueStream, dob);

            DataInputBuffer dib = new DataInputBuffer();
            dib.reset(dob.getData(), dob.getLength());

            Assert.assertEquals("\nLogType:", dib.readUTF());
            String fileType = dib.readUTF();

            Assert.assertEquals("\nLogLength:", dib.readUTF());
            String fileLengthStr = dib.readUTF();
            long fileLength = Long.parseLong(fileLengthStr);

            Assert.assertEquals("\nLog Contents:\n", dib.readUTF());
            byte[] buf = new byte[(int) fileLength]; // cast is okay in this
                                                     // test.
            dib.read(buf, 0, (int) fileLength);
            perContainerMap.put(fileType, new String(buf));

            LOG.info("LogType:" + fileType);
            LOG.info("LogType:" + fileLength);
            LOG.info("Log Contents:\n" + perContainerMap.get(fileType));
          } catch (EOFException eof) {
            break;
          }
        }

        // Next container
        key = new LogKey();
        valueStream = reader.next(key);
      }

      // 1 for each container
      Assert.assertEquals(expectedContainerIds.length, logMap.size());
      for (ContainerId cId : expectedContainerIds) {
        String containerStr = ConverterUtils.toString(cId);
        Map<String, String> thisContainerMap = logMap.remove(containerStr);
        Assert.assertEquals(3, thisContainerMap.size());
        for (String fileType : new String[] { "stdout", "stderr", "syslog" }) {
          String expectedValue = containerStr + " Hello " + fileType + "!";
          LOG.info("Expected log-content : " + new String(expectedValue));
          String foundValue = thisContainerMap.remove(fileType);
          Assert.assertNotNull(cId + " " + fileType
              + " not present in aggregated log-file!", foundValue);
          Assert.assertEquals(expectedValue, foundValue);
        }
        Assert.assertEquals(0, thisContainerMap.size());
      }
      Assert.assertEquals(0, logMap.size());
    } finally {
      reader.close();
    }
  }

  @Test
  public void testLogAggregationForRealContainerLaunch() throws IOException,
      InterruptedException {

    this.containerManager.start();

    File scriptFile = new File(tmpDir, "scriptFile.sh");
    PrintWriter fileWriter = new PrintWriter(scriptFile);
    fileWriter.write("\necho Hello World! Stdout! > "
        + new File(localLogDir, "stdout"));
    fileWriter.write("\necho Hello World! Stderr! > "
        + new File(localLogDir, "stderr"));
    fileWriter.write("\necho Hello World! Syslog! > "
        + new File(localLogDir, "syslog"));
    fileWriter.close();

    ContainerLaunchContext containerLaunchContext =
        recordFactory.newRecordInstance(ContainerLaunchContext.class);

    // ////// Construct the Container-id
    ApplicationId appId =
        recordFactory.newRecordInstance(ApplicationId.class);
    ContainerId cId = recordFactory.newRecordInstance(ContainerId.class);
    cId.setAppId(appId);
    containerLaunchContext.setContainerId(cId);

    containerLaunchContext.setUser(this.user);

    URL resource_alpha =
        ConverterUtils.getYarnUrlFromPath(localFS
            .makeQualified(new Path(scriptFile.getAbsolutePath())));
    LocalResource rsrc_alpha =
        recordFactory.newRecordInstance(LocalResource.class);
    rsrc_alpha.setResource(resource_alpha);
    rsrc_alpha.setSize(-1);
    rsrc_alpha.setVisibility(LocalResourceVisibility.APPLICATION);
    rsrc_alpha.setType(LocalResourceType.FILE);
    rsrc_alpha.setTimestamp(scriptFile.lastModified());
    String destinationFile = "dest_file";
    containerLaunchContext.setLocalResource(destinationFile, rsrc_alpha);
    containerLaunchContext.setUser(containerLaunchContext.getUser());
    containerLaunchContext.addCommand("/bin/bash");
    containerLaunchContext.addCommand(scriptFile.getAbsolutePath());
    containerLaunchContext.setResource(recordFactory
        .newRecordInstance(Resource.class));
    containerLaunchContext.getResource().setMemory(100 * 1024 * 1024);
    StartContainerRequest startRequest =
        recordFactory.newRecordInstance(StartContainerRequest.class);
    startRequest.setContainerLaunchContext(containerLaunchContext);
    this.containerManager.startContainer(startRequest);

    BaseContainerManagerTest.waitForContainerState(this.containerManager,
        cId, ContainerState.COMPLETE);

    this.containerManager.handle(new CMgrCompletedAppsEvent(Arrays
        .asList(appId)));
    this.containerManager.stop();
  }
}
