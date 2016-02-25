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

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.io.Writer;
import java.net.URL;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.ApplicationAccessType;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.impl.pb.ApplicationAttemptIdPBImpl;
import org.apache.hadoop.yarn.api.records.impl.pb.ApplicationIdPBImpl;
import org.apache.hadoop.yarn.api.records.impl.pb.ContainerIdPBImpl;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.webapp.YarnWebParams;
import org.apache.hadoop.yarn.webapp.log.AggregatedLogsBlockForTest;
import org.apache.hadoop.yarn.webapp.view.BlockForTest;
import org.apache.hadoop.yarn.webapp.view.HtmlBlock;
import org.apache.hadoop.yarn.webapp.view.HtmlBlockForTest;
import org.junit.Test;

import static org.mockito.Mockito.*;
import static org.junit.Assert.*;

/**
 * Test AggregatedLogsBlock. AggregatedLogsBlock should check user, aggregate a
 * logs into one file and show this logs or errors into html code
 * 
 */
public class TestAggregatedLogsBlock {
  /**
   * Bad user. User 'owner' is trying to read logs without access
   */
  @Test
  public void testAccessDenied() throws Exception {

    FileUtil.fullyDelete(new File("target/logs"));
    Configuration configuration = getConfiguration();

    writeLogs("target/logs/logs/application_0_0001/container_0_0001_01_000001");

    writeLog(configuration, "owner");

    AggregatedLogsBlockForTest aggregatedBlock = getAggregatedLogsBlockForTest(
        configuration, "owner", "container_0_0001_01_000001");
    ByteArrayOutputStream data = new ByteArrayOutputStream();
    PrintWriter printWriter = new PrintWriter(data);
    HtmlBlock html = new HtmlBlockForTest();
    HtmlBlock.Block block = new BlockForTest(html, printWriter, 10, false);
    aggregatedBlock.render(block);

    block.getWriter().flush();
    String out = data.toString();
    assertTrue(out
        .contains("User [owner] is not authorized to view the logs for entity"));

  }

  @Test
  public void testBlockContainsPortNumForUnavailableAppLog() {
    FileUtil.fullyDelete(new File("target/logs"));
    Configuration configuration = getConfiguration();

    String nodeName = configuration.get(YarnConfiguration.NM_WEBAPP_ADDRESS,
        YarnConfiguration.DEFAULT_NM_WEBAPP_ADDRESS);
    AggregatedLogsBlockForTest aggregatedBlock = getAggregatedLogsBlockForTest(
        configuration, "admin", "container_0_0001_01_000001", nodeName);
    ByteArrayOutputStream data = new ByteArrayOutputStream();
    PrintWriter printWriter = new PrintWriter(data);
    HtmlBlock html = new HtmlBlockForTest();
    HtmlBlock.Block block = new BlockForTest(html, printWriter, 10, false);
    aggregatedBlock.render(block);

    block.getWriter().flush();
    String out = data.toString();
    assertTrue(out.contains(nodeName));
  }

  /**
   * try to read bad logs
   * 
   * @throws Exception
   */
  @Test
  public void testBadLogs() throws Exception {

    FileUtil.fullyDelete(new File("target/logs"));
    Configuration configuration = getConfiguration();

    writeLogs("target/logs/logs/application_0_0001/container_0_0001_01_000001");

    writeLog(configuration, "owner");

    AggregatedLogsBlockForTest aggregatedBlock = getAggregatedLogsBlockForTest(
        configuration, "admin", "container_0_0001_01_000001");
    ByteArrayOutputStream data = new ByteArrayOutputStream();
    PrintWriter printWriter = new PrintWriter(data);
    HtmlBlock html = new HtmlBlockForTest();
    HtmlBlock.Block block = new BlockForTest(html, printWriter, 10, false);
    aggregatedBlock.render(block);

    block.getWriter().flush();
    String out = data.toString();
    assertTrue(out
        .contains("Logs not available for entity. Aggregation may not be complete, Check back later or try the nodemanager at localhost:1234"));

  }

  /**
   * Reading from logs should succeed and they should be shown in the
   * AggregatedLogsBlock html.
   * 
   * @throws Exception
   */
  @Test
  public void testAggregatedLogsBlock() throws Exception {

    FileUtil.fullyDelete(new File("target/logs"));
    Configuration configuration = getConfiguration();

    writeLogs("target/logs/logs/application_0_0001/container_0_0001_01_000001");

    writeLog(configuration, "admin");

    AggregatedLogsBlockForTest aggregatedBlock = getAggregatedLogsBlockForTest(
        configuration, "admin", "container_0_0001_01_000001");
    ByteArrayOutputStream data = new ByteArrayOutputStream();
    PrintWriter printWriter = new PrintWriter(data);
    HtmlBlock html = new HtmlBlockForTest();
    HtmlBlock.Block block = new BlockForTest(html, printWriter, 10, false);
    aggregatedBlock.render(block);

    block.getWriter().flush();
    String out = data.toString();
    assertTrue(out.contains("test log1"));
    assertTrue(out.contains("test log2"));
    assertTrue(out.contains("test log3"));
  }

  /**
   * Reading from logs should succeed (from a HAR archive) and they should be
   * shown in the AggregatedLogsBlock html.
   *
   * @throws Exception
   */
  @Test
  public void testAggregatedLogsBlockHar() throws Exception {
    FileUtil.fullyDelete(new File("target/logs"));
    Configuration configuration = getConfiguration();

    URL harUrl = ClassLoader.getSystemClassLoader()
        .getResource("application_1440536969523_0001.har");
    assertNotNull(harUrl);
    String path = "target/logs/admin/logs/application_1440536969523_0001" +
        "/application_1440536969523_0001.har";
    FileUtils.copyDirectory(new File(harUrl.getPath()), new File(path));

    AggregatedLogsBlockForTest aggregatedBlock = getAggregatedLogsBlockForTest(
        configuration, "admin",
        "container_1440536969523_0001_01_000001", "host1:1111");
    ByteArrayOutputStream data = new ByteArrayOutputStream();
    PrintWriter printWriter = new PrintWriter(data);
    HtmlBlock html = new HtmlBlockForTest();
    HtmlBlock.Block block = new BlockForTest(html, printWriter, 10, false);
    aggregatedBlock.render(block);

    block.getWriter().flush();
    String out = data.toString();
    assertTrue(out.contains("Hello stderr"));
    assertTrue(out.contains("Hello stdout"));
    assertTrue(out.contains("Hello syslog"));

    aggregatedBlock = getAggregatedLogsBlockForTest(
        configuration, "admin",
        "container_1440536969523_0001_01_000002", "host2:2222");
    data = new ByteArrayOutputStream();
    printWriter = new PrintWriter(data);
    html = new HtmlBlockForTest();
    block = new BlockForTest(html, printWriter, 10, false);
    aggregatedBlock.render(block);
    block.getWriter().flush();
    out = data.toString();
    assertTrue(out.contains("Goodbye stderr"));
    assertTrue(out.contains("Goodbye stdout"));
    assertTrue(out.contains("Goodbye syslog"));
  }

  /**
   * Log files was deleted.
   * @throws Exception
   */
  @Test
  public void testNoLogs() throws Exception {

    FileUtil.fullyDelete(new File("target/logs"));
    Configuration configuration = getConfiguration();

    File f = new File("target/logs/logs/application_0_0001/container_0_0001_01_000001");
    if (!f.exists()) {
      assertTrue(f.mkdirs());
    }
    writeLog(configuration, "admin");

    AggregatedLogsBlockForTest aggregatedBlock = getAggregatedLogsBlockForTest(
        configuration, "admin", "container_0_0001_01_000001");
    ByteArrayOutputStream data = new ByteArrayOutputStream();
    PrintWriter printWriter = new PrintWriter(data);
    HtmlBlock html = new HtmlBlockForTest();
    HtmlBlock.Block block = new BlockForTest(html, printWriter, 10, false);
    aggregatedBlock.render(block);

    block.getWriter().flush();
    String out = data.toString();
    assertTrue(out.contains("No logs available for container container_0_0001_01_000001"));

  }
  
  
  private Configuration getConfiguration() {
    Configuration configuration = new Configuration();
    configuration.setBoolean(YarnConfiguration.LOG_AGGREGATION_ENABLED, true);
    configuration.set(YarnConfiguration.NM_REMOTE_APP_LOG_DIR, "target/logs");
    configuration.setBoolean(YarnConfiguration.YARN_ACL_ENABLE, true);
    configuration.set(YarnConfiguration.YARN_ADMIN_ACL, "admin");
    return configuration;
  }

  private AggregatedLogsBlockForTest getAggregatedLogsBlockForTest(
      Configuration configuration, String user, String containerId) {
    return getAggregatedLogsBlockForTest(configuration, user, containerId,
        "localhost:1234");
  }

  private AggregatedLogsBlockForTest getAggregatedLogsBlockForTest(
      Configuration configuration, String user, String containerId,
      String nodeName) {
    HttpServletRequest request = mock(HttpServletRequest.class);
    when(request.getRemoteUser()).thenReturn(user);
    AggregatedLogsBlockForTest aggregatedBlock = new AggregatedLogsBlockForTest(
        configuration);
    aggregatedBlock.setRequest(request);
    aggregatedBlock.moreParams().put(YarnWebParams.CONTAINER_ID, containerId);
    aggregatedBlock.moreParams().put(YarnWebParams.NM_NODENAME, nodeName);
    aggregatedBlock.moreParams().put(YarnWebParams.APP_OWNER, user);
    aggregatedBlock.moreParams().put("start", "");
    aggregatedBlock.moreParams().put("end", "");
    aggregatedBlock.moreParams().put(YarnWebParams.ENTITY_STRING, "entity");
    return aggregatedBlock;
  }

  private void writeLog(Configuration configuration, String user)
      throws Exception {
    ApplicationId appId =  ApplicationIdPBImpl.newInstance(0, 1);
    ApplicationAttemptId appAttemptId =  ApplicationAttemptIdPBImpl.newInstance(appId, 1);
    ContainerId containerId = ContainerIdPBImpl.newContainerId(appAttemptId, 1);

    String path = "target/logs/" + user
        + "/logs/application_0_0001/localhost_1234";
    File f = new File(path);
    if (!f.getParentFile().exists()) {
     assertTrue(f.getParentFile().mkdirs());
    }
    List<String> rootLogDirs = Arrays.asList("target/logs/logs");
    UserGroupInformation ugi = UserGroupInformation.getCurrentUser();

    AggregatedLogFormat.LogWriter writer = new AggregatedLogFormat.LogWriter(
        configuration, new Path(path), ugi);
    writer.writeApplicationOwner(ugi.getUserName());

    Map<ApplicationAccessType, String> appAcls = new HashMap<ApplicationAccessType, String>();
    appAcls.put(ApplicationAccessType.VIEW_APP, ugi.getUserName());
    writer.writeApplicationACLs(appAcls);

    writer.append(new AggregatedLogFormat.LogKey("container_0_0001_01_000001"),
        new AggregatedLogFormat.LogValue(rootLogDirs, containerId,UserGroupInformation.getCurrentUser().getShortUserName()));
    writer.close();
  }

  private void writeLogs(String dirName) throws Exception {
    File f = new File(dirName + File.separator + "log1");
    if (!f.getParentFile().exists()) {
      assertTrue(f.getParentFile().mkdirs());
    }

    writeLog(dirName + File.separator + "log1", "test log1");
    writeLog(dirName + File.separator + "log2", "test log2");
    writeLog(dirName + File.separator + "log3", "test log3");
  }

  private void writeLog(String fileName, String text) throws Exception {
    File f = new File(fileName);
    Writer writer = new FileWriter(f);
    writer.write(text);
    writer.flush();
    writer.close();
  }

}
