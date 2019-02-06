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
package org.apache.hadoop.mapred;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.StringWriter;
import java.io.Writer;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.mapreduce.MRConfig;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

/**
 * TestCounters checks the sanity and recoverability of Queue
 */
public class TestQueue {
  private static File testDir = new File(System.getProperty("test.build.data",
      "/tmp"), TestJobConf.class.getSimpleName());

  @Before
  public void setup() {
    testDir.mkdirs();
  }

  @After
  public void cleanup() {
    FileUtil.fullyDelete(testDir);
  }

  /**
   * test QueueManager
   * configuration from file
   * 
   * @throws IOException
   */
  @Test (timeout=5000)
  public void testQueue() throws IOException {
    File f = null;
    try {
      f = writeFile();

      QueueManager manager = new QueueManager(f.getCanonicalPath(), true);
      manager.setSchedulerInfo("first", "queueInfo");
      manager.setSchedulerInfo("second", "queueInfoqueueInfo");
      Queue root = manager.getRoot();
      assertTrue(root.getChildren().size() == 2);
      Iterator<Queue> iterator = root.getChildren().iterator();
      Queue firstSubQueue = iterator.next();
      assertEquals("first", firstSubQueue.getName());
      assertEquals(
          firstSubQueue.getAcls().get("mapred.queue.first.acl-submit-job")
              .toString(),
          "Users [user1, user2] and members of the groups [group1, group2] are allowed");
      Queue secondSubQueue = iterator.next();
      assertEquals("second", secondSubQueue.getName());
      assertEquals(secondSubQueue.getProperties().getProperty("key"), "value");
      assertEquals(secondSubQueue.getProperties().getProperty("key1"), "value1");
      // test status
      assertEquals(firstSubQueue.getState().getStateName(), "running");
      assertEquals(secondSubQueue.getState().getStateName(), "stopped");

      Set<String> template = new HashSet<String>();
      template.add("first");
      template.add("second");
      assertEquals(manager.getLeafQueueNames(), template);

      // test user access

      UserGroupInformation mockUGI = mock(UserGroupInformation.class);
      when(mockUGI.getShortUserName()).thenReturn("user1");
      String[] groups = { "group1" };
      when(mockUGI.getGroupNames()).thenReturn(groups);
      assertTrue(manager.hasAccess("first", QueueACL.SUBMIT_JOB, mockUGI));
      assertFalse(manager.hasAccess("second", QueueACL.SUBMIT_JOB, mockUGI));
      assertFalse(manager.hasAccess("first", QueueACL.ADMINISTER_JOBS, mockUGI));
      when(mockUGI.getShortUserName()).thenReturn("user3");
      assertTrue(manager.hasAccess("first", QueueACL.ADMINISTER_JOBS, mockUGI));

      QueueAclsInfo[] qai = manager.getQueueAcls(mockUGI);
      assertEquals(qai.length, 1);
      // test refresh queue
      manager.refreshQueues(getConfiguration(), null);

      iterator = root.getChildren().iterator();
      Queue firstSubQueue1 = iterator.next();
      Queue secondSubQueue1 = iterator.next();
      // tets equal method
      assertTrue(firstSubQueue.equals(firstSubQueue1));
      assertEquals(firstSubQueue1.getState().getStateName(), "running");
      assertEquals(secondSubQueue1.getState().getStateName(), "stopped");

      assertEquals(firstSubQueue1.getSchedulingInfo(), "queueInfo");
      assertEquals(secondSubQueue1.getSchedulingInfo(), "queueInfoqueueInfo");

      // test JobQueueInfo
      assertEquals(firstSubQueue.getJobQueueInfo().getQueueName(), "first");
      assertEquals(firstSubQueue.getJobQueueInfo().getQueueState(), "running");
      assertEquals(firstSubQueue.getJobQueueInfo().getSchedulingInfo(),
          "queueInfo");
      assertEquals(secondSubQueue.getJobQueueInfo().getChildren().size(), 0);
      // test
      assertEquals(manager.getSchedulerInfo("first"), "queueInfo");
      Set<String> queueJobQueueInfos = new HashSet<String>();
      for(JobQueueInfo jobInfo : manager.getJobQueueInfos()){
    	  queueJobQueueInfos.add(jobInfo.getQueueName());
      }
      Set<String> rootJobQueueInfos = new HashSet<String>();
      for(Queue queue : root.getChildren()){
    	  rootJobQueueInfos.add(queue.getJobQueueInfo().getQueueName());
      }
      assertEquals(queueJobQueueInfos, rootJobQueueInfos);
      // test getJobQueueInfoMapping
      assertEquals(
          manager.getJobQueueInfoMapping().get("first").getQueueName(), "first");
      // test dumpConfiguration
      Writer writer = new StringWriter();

      Configuration conf = getConfiguration();
      conf.unset(DeprecatedQueueConfigurationParser.MAPRED_QUEUE_NAMES_KEY);
      QueueManager.dumpConfiguration(writer, f.getAbsolutePath(), conf);
      String result = writer.toString();
      assertTrue(result
          .indexOf("\"name\":\"first\",\"state\":\"running\",\"acl_submit_job\":\"user1,user2 group1,group2\",\"acl_administer_jobs\":\"user3,user4 group3,group4\",\"properties\":[],\"children\":[]") > 0);

      writer = new StringWriter();
      QueueManager.dumpConfiguration(writer, conf);
      result = writer.toString();
      assertTrue(result.contains("{\"queues\":[{\"name\":\"default\",\"state\":\"running\",\"acl_submit_job\":\"*\",\"acl_administer_jobs\":\"*\",\"properties\":[],\"children\":[]},{\"name\":\"q1\",\"state\":\"running\",\"acl_submit_job\":\" \",\"acl_administer_jobs\":\" \",\"properties\":[],\"children\":[{\"name\":\"q1:q2\",\"state\":\"running\",\"acl_submit_job\":\" \",\"acl_administer_jobs\":\" \",\"properties\":["));
      assertTrue(result.contains("{\"key\":\"capacity\",\"value\":\"20\"}"));
      assertTrue(result.contains("{\"key\":\"user-limit\",\"value\":\"30\"}"));
      assertTrue(result.contains("],\"children\":[]}]}]}"));
      // test constructor QueueAclsInfo
      QueueAclsInfo qi = new QueueAclsInfo();
      assertNull(qi.getQueueName());

    } finally {
      if (f != null) {
        f.delete();
      }
    }
  }

  private Configuration getConfiguration() {
    Configuration conf = new Configuration();
    conf.set(DeprecatedQueueConfigurationParser.MAPRED_QUEUE_NAMES_KEY,
        "first,second");
    conf.set(QueueManager.QUEUE_CONF_PROPERTY_NAME_PREFIX
        + "first.acl-submit-job", "user1,user2 group1,group2");
    conf.set(MRConfig.MR_ACLS_ENABLED, "true");
    conf.set(QueueManager.QUEUE_CONF_PROPERTY_NAME_PREFIX + "first.state",
        "running");
    conf.set(QueueManager.QUEUE_CONF_PROPERTY_NAME_PREFIX + "second.state",
        "stopped");
    return conf;
  }

  @Test (timeout=5000)
  public void testDefaultConfig() {
    QueueManager manager = new QueueManager(true);
    assertEquals(manager.getRoot().getChildren().size(), 2);
  }

  /**
   * test for Qmanager with empty configuration
   * 
   * @throws IOException
   */

  @Test (timeout=5000)
  public void test2Queue() throws IOException {
    Configuration conf = getConfiguration();

    QueueManager manager = new QueueManager(conf);
    manager.setSchedulerInfo("first", "queueInfo");
    manager.setSchedulerInfo("second", "queueInfoqueueInfo");

    Queue root = manager.getRoot();
    
    // test children queues
    assertTrue(root.getChildren().size() == 2);
    Iterator<Queue> iterator = root.getChildren().iterator();
    Queue firstSubQueue = iterator.next();
    assertEquals("first", firstSubQueue.getName());
    assertEquals(
        firstSubQueue.getAcls().get("mapred.queue.first.acl-submit-job")
            .toString(),
        "Users [user1, user2] and members of the groups [group1, group2] are allowed");
    Queue secondSubQueue = iterator.next();
    assertEquals("second", secondSubQueue.getName());

    assertEquals(firstSubQueue.getState().getStateName(), "running");
    assertEquals(secondSubQueue.getState().getStateName(), "stopped");
    assertTrue(manager.isRunning("first"));
    assertFalse(manager.isRunning("second"));

    assertEquals(firstSubQueue.getSchedulingInfo(), "queueInfo");
    assertEquals(secondSubQueue.getSchedulingInfo(), "queueInfoqueueInfo");
// test leaf queue
    Set<String> template = new HashSet<String>();
    template.add("first");
    template.add("second");
    assertEquals(manager.getLeafQueueNames(), template);

    
  }
/**
 * write cofiguration
 * @return
 * @throws IOException
 */
  private File writeFile() throws IOException {

    File f = new File(testDir, "tst.xml");
    BufferedWriter out = new BufferedWriter(new FileWriter(f));
    String properties = "<properties><property key=\"key\" value=\"value\"/><property key=\"key1\" value=\"value1\"/></properties>";
    out.write("<queues>");
    out.newLine();
    out.write("<queue><name>first</name><acl-submit-job>user1,user2 group1,group2</acl-submit-job><acl-administer-jobs>user3,user4 group3,group4</acl-administer-jobs><state>running</state></queue>");
    out.newLine();
    out.write("<queue><name>second</name><acl-submit-job>u1,u2 g1,g2</acl-submit-job>"
        + properties + "<state>stopped</state></queue>");
    out.newLine();
    out.write("</queues>");
    out.flush();
    out.close();
    return f;

  }
  
}
