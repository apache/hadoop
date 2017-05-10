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

package org.apache.hadoop.yarn.sls.web;

import org.junit.Assert;
import org.apache.commons.io.FileUtils;
import org.junit.Test;

import java.io.File;
import java.text.MessageFormat;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.HashMap;

public class TestSLSWebApp {

  @Test
  public void testSimulateInfoPageHtmlTemplate() throws Exception {
    String simulateInfoTemplate = FileUtils.readFileToString(
            new File("src/main/html/simulate.info.html.template"));

    Map<String, Object> simulateInfoMap = new HashMap<>();
    simulateInfoMap.put("Number of racks", 10);
    simulateInfoMap.put("Number of nodes", 100);
    simulateInfoMap.put("Node memory (MB)", 1024);
    simulateInfoMap.put("Node VCores", 1);
    simulateInfoMap.put("Number of applications", 100);
    simulateInfoMap.put("Number of tasks", 1000);
    simulateInfoMap.put("Average tasks per applicaion", 10);
    simulateInfoMap.put("Number of queues", 4);
    simulateInfoMap.put("Average applications per queue", 25);
    simulateInfoMap.put("Estimated simulate time (s)", 10000);

    StringBuilder info = new StringBuilder();
    for (Map.Entry<String, Object> entry :
        simulateInfoMap.entrySet()) {
      info.append("<tr>");
      info.append("<td class='td1'>" + entry.getKey() + "</td>");
      info.append("<td class='td2'>" + entry.getValue() + "</td>");
      info.append("</tr>");
    }

    String simulateInfo =
            MessageFormat.format(simulateInfoTemplate, info.toString());
    Assert.assertTrue("The simulate info html page should not be empty",
            simulateInfo.length() > 0);
    for (Map.Entry<String, Object> entry : simulateInfoMap.entrySet()) {
      Assert.assertTrue("The simulate info html page should have information "
              + "of " + entry.getKey(), simulateInfo.contains("<td class='td1'>"
              + entry.getKey() + "</td><td class='td2'>"
              + entry.getValue() + "</td>"));
    }
  }

  @Test
  public void testSimulatePageHtmlTemplate() throws Exception {
    String simulateTemplate = FileUtils.readFileToString(
            new File("src/main/html/simulate.html.template"));

    Set<String> queues = new HashSet<String>();
    queues.add("sls_queue_1");
    queues.add("sls_queue_2");
    queues.add("sls_queue_3");
    String queueInfo = "";
    int i = 0;
    for (String queue : queues) {
      queueInfo += "legends[4][" + i + "] = 'queue" + queue
              + ".allocated.memory'";
      queueInfo += "legends[5][" + i + "] = 'queue" + queue
              + ".allocated.vcores'";
      i ++;
    }
    String simulateInfo = MessageFormat.format(simulateTemplate,
            queueInfo, "s", 1000, 1000);
    Assert.assertTrue("The simulate page html page should not be empty",
            simulateInfo.length() > 0);
  }

  @Test
  public void testTrackPageHtmlTemplate() throws Exception {
    String trackTemplate = FileUtils.readFileToString(
            new File("src/main/html/track.html.template"));
    String trackedQueueInfo = "";
    Set<String> trackedQueues = new HashSet<String>();
    trackedQueues.add("sls_queue_1");
    trackedQueues.add("sls_queue_2");
    trackedQueues.add("sls_queue_3");
    for(String queue : trackedQueues) {
      trackedQueueInfo += "<option value='Queue " + queue + "'>"
              + queue + "</option>";
    }
    String trackedAppInfo = "";
    Set<String> trackedApps = new HashSet<String>();
    trackedApps.add("app_1");
    trackedApps.add("app_2");
    for(String job : trackedApps) {
      trackedAppInfo += "<option value='Job " + job + "'>" + job + "</option>";
    }
    String trackInfo = MessageFormat.format(trackTemplate, trackedQueueInfo,
            trackedAppInfo, "s", 1000, 1000);
    Assert.assertTrue("The queue/app tracking html page should not be empty",
            trackInfo.length() > 0);
  }
}
