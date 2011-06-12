/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.mapred;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import static org.apache.hadoop.mapred.QueueManagerTestUtils.*;
import static org.apache.hadoop.mapred.QueueConfigurationParser.*;
import static org.junit.Assert.*;

import org.apache.hadoop.mapreduce.QueueState;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.AccessControlList;
import org.codehaus.jackson.map.ObjectMapper;
import org.junit.After;
import org.junit.Test;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;

import java.io.File;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.Map.Entry;


public class TestQueueManager {

  private static final Log LOG = LogFactory.getLog(
    TestQueueManager.class);

  @After
  public void tearDown() throws Exception {
    new File(CONFIG).delete();
  }

  @Test
  public void testDefault() throws Exception {
    QueueManager qm = new QueueManager();
    Queue root = qm.getRoot();
    assertEquals(root.getChildren().size(), 1);
    assertEquals(root.getChildren().iterator().next().getName(), "default");
    assertFalse(qm.isAclsEnabled());
    assertNull(root.getChildren().iterator().next().getChildren());
  }

  @Test
  public void testXMLParsing() throws Exception {
    checkForConfigFile();
    Document doc = createDocument();
    createSimpleDocument(doc);
    writeToFile(doc, CONFIG);
    QueueManager qm = new QueueManager(CONFIG);
    Set<Queue> rootQueues = qm.getRoot().getChildren();
    List<String> names = new ArrayList<String>();
    for (Queue q : rootQueues) {
      names.add(q.getName());
    }

    //Size of root.
    assertEquals(rootQueues.size(), 2);

    //check root level queues
    assertTrue(names.contains("q1"));
    assertTrue(names.contains("p1"));


    //check for leaf names
    Set<String> leafNames = qm.getLeafQueueNames();
    Queue p = qm.getQueue("p1");
    Set<Queue> children = p.getChildren();
    assertTrue(children.size() == 2);

    //check leaf level queues
    assertTrue(
      leafNames.contains(
        "p1" + NAME_SEPARATOR + "p11"));
    assertTrue(
      leafNames.contains(
        "p1" + NAME_SEPARATOR + "p12"));


    Queue q = qm.getQueue(
      "p1" + NAME_SEPARATOR + "p12");

    assertTrue(
      q.getAcls().get(
        QueueManager.toFullPropertyName(
          q.getName(), ACL_SUBMIT_JOB_TAG)).isUserAllowed(
        UserGroupInformation.createRemoteUser("u1")));

    assertTrue(
      q.getAcls().get(
        QueueManager.toFullPropertyName(
          q.getName(),
          ACL_ADMINISTER_JOB_TAG))
        .isUserAllowed(UserGroupInformation.createRemoteUser("u2")));
    assertTrue(q.getState().equals(QueueState.STOPPED));
  }

  @Test
  public void testhasAccess() throws Exception {
    checkForConfigFile();
    Document doc = createDocument();
    createSimpleDocumentWithAcls(doc,"true");
    writeToFile(doc, CONFIG);
    QueueManager qm = new QueueManager(CONFIG);

    UserGroupInformation ugi;
    // test for acls access when acls are set with *
    ugi = UserGroupInformation.createRemoteUser("u1");
    assertTrue(qm.hasAccess("p1" + NAME_SEPARATOR + "p12",
        Queue.QueueOperation.SUBMIT_JOB, ugi));
    ugi = UserGroupInformation.createRemoteUser("u2");
    assertTrue(qm.hasAccess("p1" + NAME_SEPARATOR + "p12",
        Queue.QueueOperation.ADMINISTER_JOBS, ugi));
    
    // test for acls access when acls are not set with *
    ugi = UserGroupInformation.createRemoteUser("u1");
    assertTrue(qm.hasAccess("p1" + NAME_SEPARATOR + "p11",
        Queue.QueueOperation.SUBMIT_JOB, ugi));
    ugi = UserGroupInformation.createRemoteUser("u2");
    assertTrue(qm.hasAccess("p1" + NAME_SEPARATOR + "p11",
        Queue.QueueOperation.ADMINISTER_JOBS, ugi));
    
    // test for acls access when acls are not specified but acls is enabled
    ugi = UserGroupInformation.createRemoteUser("u1");
    assertTrue(qm.hasAccess("p1" + NAME_SEPARATOR + "p13",
        Queue.QueueOperation.SUBMIT_JOB, ugi));
    ugi = UserGroupInformation.createRemoteUser("u2");
    assertTrue(qm.hasAccess("p1" + NAME_SEPARATOR + "p13",
        Queue.QueueOperation.ADMINISTER_JOBS, ugi));
    
    assertTrue(qm.isRunning("p1" + NAME_SEPARATOR + "p13"));
  }
  
  @Test
  public void testQueueView() throws Exception {
    checkForConfigFile();
    Document doc = createDocument();
    createSimpleDocument(doc);
    writeToFile(doc, CONFIG);
    QueueManager qm = new QueueManager(CONFIG);
    
    for (Queue queue : qm.getRoot().getChildren()) {
      checkHierarchy(queue, qm);
    }
  }

  private void checkHierarchy(Queue queue, QueueManager queueManager) {
    JobQueueInfo jobQueueInfo = queueManager.getJobQueueInfo(queue.getName());
    assertEquals(queue.getName(),jobQueueInfo.getQueueName());
    assertEquals(queue.getState(),jobQueueInfo.getState());
    if (queue.getChildren() !=null && queue.getChildren().size() > 0) {
      for (Queue childQueue : queue.getChildren()) {
        checkHierarchy(childQueue, queueManager);
      }
    }
  }

  @Test
  public void testhasAccessForParent() throws Exception {
    checkForConfigFile();
    Document doc = createDocument();
    createSimpleDocument(doc);
    writeToFile(doc, CONFIG);
    QueueManager qm = new QueueManager(CONFIG);

    UserGroupInformation ugi =
      UserGroupInformation.createRemoteUser("u1");
    assertFalse(
      qm.hasAccess(
        "p1",
        Queue.QueueOperation.SUBMIT_JOB, ugi));
  }

  @Test
  public void testValidation() throws Exception {
    checkForConfigFile();
    Document doc = createDocument();
    Element queues = createQueuesNode(doc, "false");
    Element q1 = createQueue(doc, "q1");

    q1.appendChild(createAcls(doc, "acl-submit-job", "u1"));
    q1.appendChild(createAcls(doc, "acl-administer-jobs", "u2"));
    q1.appendChild(createQueue(doc, "p15"));
    q1.appendChild(createQueue(doc, "p16"));

    queues.appendChild(q1);
    writeToFile(doc, CONFIG);
    try {
      new QueueManager(CONFIG);
      fail("Should throw an exception as configuration is wrong ");
    } catch (RuntimeException re) {
      LOG.info(re.getMessage());
    }
  }

  @Test
  public void testInvalidName() throws Exception {
    checkForConfigFile();
    Document doc = createDocument();
    Element queues = createQueuesNode(doc, "false");
    Element q1 = createQueue(doc, "");
    queues.appendChild(q1);
    writeToFile(doc, CONFIG);
    try {
      new QueueManager(CONFIG);
      fail("Should throw an exception as configuration is wrong ");
    } catch (Exception re) {
      re.printStackTrace();
      LOG.info(re.getMessage());
    }
    checkForConfigFile();
    doc = createDocument();
    queues = createQueuesNode(doc, "false");
    q1 = doc.createElement("queue");
    queues.appendChild(q1);
    writeToFile(doc, CONFIG);
    try {
      new QueueManager(CONFIG);
      fail("Should throw an exception as configuration is wrong ");
    } catch (RuntimeException re) {
      re.printStackTrace();
      LOG.info(re.getMessage());
    }
  }

  @Test
  public void testMissingConfigFile() throws Exception {
    checkForConfigFile(); // deletes file

    try {
      new QueueManager(CONFIG);
      fail("Should throw an exception for missing file when " +
           "explicitly passed.");
    } catch (RuntimeException re) {
    }

    // If we just want to pick up the queues from the class loader
    // it should fall through to the default. The class loader is set to
    // load CONFIG for the "mapred-queues.xml" resource, but it's missing
    // so should fall through to mapred-queues-default.xml
    QueueManager qm = new QueueManager();
    List<JobQueueInfo> rootQueues =
      qm.getRoot().getJobQueueInfo().getChildren();
    assertEquals(1, rootQueues.size());
    assertEquals("default", rootQueues.get(0).getQueueName());
  }

  @Test
  public void testEmptyProperties() throws Exception {
    checkForConfigFile();
    Document doc = createDocument();
    Element queues = createQueuesNode(doc, "false");
    Element q1 = createQueue(doc, "q1");
    Element p = createProperties(doc, null);
    q1.appendChild(p);
    queues.appendChild(q1);
  }

  @Test
  public void testEmptyFile() throws Exception {
    checkForConfigFile();
    Document doc = createDocument();
    writeToFile(doc, CONFIG);
    try {
      new QueueManager(CONFIG);
      fail("Should throw an exception as configuration is wrong ");
    } catch (Exception re) {
      re.printStackTrace();
      LOG.info(re.getMessage());
    }
  }

  @Test
  public void testJobQueueInfoGeneration() throws Exception {
    checkForConfigFile();
    Document doc = createDocument();
    createSimpleDocument(doc);
    writeToFile(doc, CONFIG);
    QueueManager qm = new QueueManager(CONFIG);

    List<JobQueueInfo> rootQueues =
      qm.getRoot().getJobQueueInfo().getChildren();
    assertEquals(rootQueues.size(), 2);
    List<String> names = new ArrayList<String>();
    for (JobQueueInfo q : rootQueues) {
      names.add(q.getQueueName());
      if (q.getQueueName().equals("q1")) {
        Properties p = q.getProperties();
        assertEquals(p.getProperty("capacity"), "10");
        assertEquals(p.getProperty("maxCapacity"), "35");

        assertTrue(q.getChildren().isEmpty());
      } else if (q.getQueueName().equals("p1")) {
        List<JobQueueInfo> children = q.getChildren();
        assertEquals(children.size(), 2);
        for (JobQueueInfo child : children) {
          if (child.getQueueName().equals(
            "p1" + NAME_SEPARATOR + "p12")) {
            assertEquals(
              child.getQueueState(), QueueState.STOPPED.getStateName());
          } else if (child.getQueueName().equals(
            "p1" + NAME_SEPARATOR + "p11")) {
            assertEquals(
              child.getQueueState(), QueueState.RUNNING.getStateName());
          } else {
            fail("Only 2 children");
          }
        }
      } else {
        fail("Only 2 queues with q1 and p1 ");
      }
    }
  }

  /**
   * Test the refresh of queues.
   * 
   * @throws Exception
   */
  @Test
  public void testRefresh() throws Exception {
    checkForConfigFile();
    Document doc = createDocument();
    createSimpleDocument(doc);
    writeToFile(doc, CONFIG);
    QueueManager qm = new QueueManager(CONFIG);
    Queue beforeRefreshRoot = qm.getRoot();
    //remove the file and create new one.
    Set<Queue> rootQueues = beforeRefreshRoot.getChildren();
    for (Queue qs : rootQueues) {
      if (qs.getName().equals("q1")) {

        assertEquals(qs.getProperties().getProperty("capacity"), "10");
        assertEquals(qs.getProperties().getProperty("maxCapacity"), "35");

      } else if (qs.getName().equals("p1")) {

        Set<Queue> children = qs.getChildren();
        for (Queue child : children) {
          if (child.getName().equals(
            "p1" + NAME_SEPARATOR + "p12")) {
            assertTrue(
              child.getAcls().get(
                QueueManager.toFullPropertyName(
                  child.getName(), ACL_SUBMIT_JOB_TAG))
                .isUserAllowed(UserGroupInformation.createRemoteUser("u1")));

            assertTrue(
              child.getAcls().get(
                QueueManager.toFullPropertyName(
                  child.getName(),
                  ACL_ADMINISTER_JOB_TAG))
                .isUserAllowed(UserGroupInformation.createRemoteUser("u2")));
            assertTrue(child.getState().equals(QueueState.STOPPED));
          } else {
            assertTrue(child.getState().equals(QueueState.RUNNING));
          }
        }
      }
    }
    checkForConfigFile();
    doc = createDocument();
    refreshSimpleDocument(doc);
    writeToFile(doc, CONFIG);
    QueueConfigurationParser cp = new QueueConfigurationParser(CONFIG);
    qm.getRoot().isHierarchySameAs(cp.getRoot());
    qm.setQueues(
      cp.getRoot().getChildren().toArray(
        new Queue[cp.getRoot().getChildren().size()]));
    Queue afterRefreshRoot = qm.getRoot();
    //remove the file and create new one.
    rootQueues = afterRefreshRoot.getChildren();
    for (Queue qs : rootQueues) {
      if (qs.getName().equals("q1")) {

        assertEquals(qs.getProperties().getProperty("capacity"), "70");
        assertEquals(qs.getProperties().getProperty("maxCapacity"), "35");

      } else if (qs.getName().equals("p1")) {

        Set<Queue> children = qs.getChildren();
        for (Queue child : children) {
          if (child.getName().equals(
            "p1" + NAME_SEPARATOR + "p12")) {
            assertTrue(
              child.getAcls().get(
                QueueManager.toFullPropertyName(
                  child.getName(),
                  ACL_SUBMIT_JOB_TAG))
                .isUserAllowed(UserGroupInformation.createRemoteUser("u3")));

            assertTrue(
              child.getAcls().get(
                QueueManager.toFullPropertyName(
                  child.getName(),
                  ACL_ADMINISTER_JOB_TAG))
                .isUserAllowed(UserGroupInformation.createRemoteUser("u4")));
            assertTrue(child.getState().equals(QueueState.RUNNING));
          } else {
            assertTrue(child.getState().equals(QueueState.STOPPED));
          }
        }
      }
    }
  }

  @Test
  public void testRefreshWithInvalidFile() throws Exception {
    checkForConfigFile();
    Document doc = createDocument();
    createSimpleDocument(doc);
    writeToFile(doc, CONFIG);
    QueueManager qm = new QueueManager(CONFIG);

    checkForConfigFile();
    doc = createDocument();
    Element queues = createQueuesNode(doc, "false");
    Element q1 = createQueue(doc, "");
    queues.appendChild(q1);
    writeToFile(doc, CONFIG);
    try {
      QueueConfigurationParser cp = new QueueConfigurationParser(CONFIG);

      fail("Should throw an exception as configuration is wrong ");
    } catch (Throwable re) {
      re.printStackTrace();
      LOG.info(re.getMessage());
    }
  }

  /**
   * Class to store the array of queues retrieved by parsing the string 
   * that is dumped in Json format
   */
  static class JsonQueueTree {
    boolean acls_enabled;
    
    JsonQueue[] queues;

    public JsonQueue[] getQueues() {
      return queues;
    }

    public void setQueues(JsonQueue[] queues) {
      this.queues = queues;
    }

    public boolean isAcls_enabled() {
      return acls_enabled;
    }

    public void setAcls_enabled(boolean aclsEnabled) {
      acls_enabled = aclsEnabled;
    }
  }
  
  /**
   * Class to store the contents of each queue that is dumped in JSON format.
   */
  static class JsonQueue {
    String name;
    String state;
    String acl_submit_job;
    String acl_administer_jobs;
    JsonProperty[] properties;
    JsonQueue[] children;
    public String getName() {
      return name;
    }
    public String getState() {
      return state;
    }
    public JsonProperty[] getProperties() {
      return properties;
    }
    public JsonQueue[] getChildren() {
      return children;
    }
    public void setName(String name) {
      this.name = name;
    }
    public void setState(String state) {
      this.state = state;
    }
    public void setProperties(JsonProperty[] properties) {
      this.properties = properties;
    }
    public void setChildren(JsonQueue[] children) {
      this.children = children;
    }
    public String getAcl_submit_job() {
      return acl_submit_job;
    }
    public void setAcl_submit_job(String aclSubmitJob) {
      acl_submit_job = aclSubmitJob;
    }
    public String getAcl_administer_jobs() {
      return acl_administer_jobs;
    }
    public void setAcl_administer_jobs(String aclAdministerJobs) {
      acl_administer_jobs = aclAdministerJobs;
    }
  }
  
  /**
   * Class to store the contents of attribute "properties" in Json dump
   */
  static class JsonProperty {
    String key;
    String value;
    public String getKey() {
      return key;
    }
    public void setKey(String key) {
      this.key = key;
    }
    public String getValue() {
      return value;
    }
    public void setValue(String value) {
      this.value = value;
    }
  }

  /**
   * checks the format of the dump in JSON format when 
   * QueueManager.dumpConfiguration(Writer) is called.
   * @throws Exception
   */
  @Test
  public void testDumpConfiguration() throws Exception {
    checkForConfigFile();
    Document doc = createDocument();
    createSimpleDocument(doc);    
    writeToFile(doc, CONFIG);
    StringWriter out = new StringWriter();
    QueueManager.dumpConfiguration(out,CONFIG,null);
    ObjectMapper mapper = new ObjectMapper();
    // parse the Json dump
    JsonQueueTree queueTree =
      mapper.readValue(out.toString(), JsonQueueTree.class);
    
    // check if acls_enabled is correct
    assertEquals(true, queueTree.isAcls_enabled());
    // check for the number of top-level queues
    assertEquals(2, queueTree.getQueues().length);
    
    HashMap<String, JsonQueue> topQueues = new HashMap<String, JsonQueue>();
    for (JsonQueue topQueue : queueTree.getQueues()) {
      topQueues.put(topQueue.getName(), topQueue);
    }
    
    // check for consistency in number of children
    assertEquals(2, topQueues.get("p1").getChildren().length);
    
    HashMap<String, JsonQueue> childQueues = new HashMap<String, JsonQueue>();
    for (JsonQueue child : topQueues.get("p1").getChildren()) {
      childQueues.put(child.getName(), child);
    }
    
    // check for consistency in state
    assertEquals("stopped", childQueues.get("p1:p12").getState());
     
    // check for consistency in properties
    HashMap<String, JsonProperty> q1_properties =
      new HashMap<String, JsonProperty>();
    for (JsonProperty prop : topQueues.get("q1").getProperties()) {
      q1_properties.put(prop.getKey(), prop);
    }
    assertEquals("10", q1_properties.get("capacity").getValue());
    assertEquals("35", q1_properties.get("maxCapacity").getValue());
    
    // check for acls
    assertEquals("u1", childQueues.get("p1:p12").getAcl_submit_job());
    assertEquals("u2", childQueues.get("p1:p12").getAcl_administer_jobs());
  }
}
