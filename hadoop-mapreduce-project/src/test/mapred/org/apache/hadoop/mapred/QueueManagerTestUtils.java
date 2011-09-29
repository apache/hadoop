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

//import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Cluster;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.MRConfig;
import org.apache.hadoop.mapreduce.QueueState;
import org.apache.hadoop.mapreduce.SleepJob;
import org.apache.hadoop.mapreduce.server.jobtracker.JTConfig;
import org.apache.hadoop.security.UserGroupInformation;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.transform.TransformerException;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.stream.StreamResult;
import javax.xml.transform.dom.DOMSource;

import java.security.PrivilegedExceptionAction;
import java.util.Properties;
import java.util.Set;
import java.io.File;
import java.io.IOException;

//@Private
public class QueueManagerTestUtils {
  /**
   * Queue-configuration file for tests that start a cluster and wish to modify
   * the queue configuration. This file is always in the unit tests classpath,
   * so QueueManager started through JobTracker will automatically pick this up.
   */
  public static final String QUEUES_CONFIG_FILE_PATH = new File(System
      .getProperty("test.build.extraconf", "build/test/extraconf"),
      QueueManager.QUEUE_CONF_FILE_NAME).getAbsolutePath();

  private static final Log LOG = LogFactory.getLog(QueueManagerTestUtils.class);

  /**
   * Create and return a new instance of a DOM Document object to build a queue
   * tree with.
   * 
   * @return the created {@link Document}
   * @throws Exception
   */
  public static Document createDocument() throws Exception {
    Document doc = DocumentBuilderFactory.newInstance().newDocumentBuilder()
        .newDocument();
    return doc;
  }

  public static void createSimpleDocument(Document doc) throws Exception {
    Element queues = createQueuesNode(doc);

    // Create parent level queue q1.
    Element q1 = createQueue(doc, "q1");
    Properties props = new Properties();
    props.setProperty("capacity", "10");
    props.setProperty("maxCapacity", "35");
    q1.appendChild(createProperties(doc, props));
    queues.appendChild(q1);

    // Create another parent level p1
    Element p1 = createQueue(doc, "p1");

    // append child p11 to p1
    p1.appendChild(createQueue(doc, "p11"));

    Element p12 = createQueue(doc, "p12");

    p12.appendChild(createState(doc, QueueState.STOPPED.getStateName()));
    p12.appendChild(createAcls(doc,
        QueueConfigurationParser.ACL_SUBMIT_JOB_TAG, "u1"));
    p12.appendChild(createAcls(doc,
        QueueConfigurationParser.ACL_ADMINISTER_JOB_TAG, "u2"));

    // append p12 to p1.
    p1.appendChild(p12);

    queues.appendChild(p1);
  }

  static void createSimpleDocumentWithAcls(Document doc) {
    Element queues = createQueuesNode(doc);

    // Create parent level queue q1.
    Element q1 = createQueue(doc, "q1");
    Properties props = new Properties();
    props.setProperty("capacity", "10");
    props.setProperty("maxCapacity", "35");
    q1.appendChild(createProperties(doc, props));
    queues.appendChild(q1);

    // Create another parent level p1
    Element p1 = createQueue(doc, "p1");

    // append child p11 to p1
    Element p11 = createQueue(doc, "p11");
    p11.appendChild(createAcls(doc,
        QueueConfigurationParser.ACL_SUBMIT_JOB_TAG, "u1"));
    p11.appendChild(createAcls(doc,
        QueueConfigurationParser.ACL_ADMINISTER_JOB_TAG, "u2"));
    p1.appendChild(p11);

    // append child p12 to p1
    Element p12 = createQueue(doc, "p12");
    p12.appendChild(createState(doc, QueueState.RUNNING.getStateName()));
    p12.appendChild(createAcls(doc,
        QueueConfigurationParser.ACL_SUBMIT_JOB_TAG, "*"));
    p12.appendChild(createAcls(doc,
        QueueConfigurationParser.ACL_ADMINISTER_JOB_TAG, "*"));
    p1.appendChild(p12);

    // append child p13 to p1
    Element p13 = createQueue(doc, "p13");
    p13.appendChild(createState(doc, QueueState.RUNNING.getStateName()));
    p1.appendChild(p13);

    // append child p14 to p1
    Element p14 = createQueue(doc, "p14");
    p14.appendChild(createState(doc, QueueState.STOPPED.getStateName()));
    p1.appendChild(p14);

    queues.appendChild(p1);
  }

  /**
   * Creates all given queues as 1st level queues(no nesting)
   * @param doc         the queues config document
   * @param queueNames  the queues to be added to the queues config document
   * @param submitAcls  acl-submit-job acls for each of the queues
   * @param adminsAcls  acl-administer-jobs acls for each of the queues
   * @throws Exception
   */
  public static void createSimpleDocument(Document doc, String[] queueNames,
      String[] submitAcls, String[] adminsAcls) throws Exception {

    Element queues = createQueuesNode(doc);

    // Create all queues as 1st level queues(no nesting)
    for (int i = 0; i < queueNames.length; i++) {
      Element q = createQueue(doc, queueNames[i]);

      q.appendChild(createState(doc, QueueState.RUNNING.getStateName()));
      q.appendChild(createAcls(doc,
          QueueConfigurationParser.ACL_SUBMIT_JOB_TAG, submitAcls[i]));
      q.appendChild(createAcls(doc,
          QueueConfigurationParser.ACL_ADMINISTER_JOB_TAG, adminsAcls[i]));
      queues.appendChild(q);
    }
  }

  /**
   * Creates queues configuration file with given queues at 1st level(i.e.
   * no nesting of queues) and with the given queue acls.
   * @param queueNames        queue names which are to be configured
   * @param submitAclStrings  acl-submit-job acls for each of the queues
   * @param adminsAclStrings  acl-administer-jobs acls for each of the queues
   * @return Configuration    the queues configuration
   * @throws Exception
   */
  public static void createQueuesConfigFile(String[] queueNames,
      String[] submitAclStrings, String[] adminsAclStrings)
  throws Exception {
    if (queueNames.length > submitAclStrings.length ||
        queueNames.length > adminsAclStrings.length) {
      LOG.error("Number of queues is more than acls given.");
      return;
    }
    Document doc = createDocument();
    createSimpleDocument(doc, queueNames, submitAclStrings, adminsAclStrings);
    writeToFile(doc, QUEUES_CONFIG_FILE_PATH);
  }

  public static void refreshSimpleDocument(Document doc) throws Exception {
    Element queues = createQueuesNode(doc);

    // Create parent level queue q1.
    Element q1 = createQueue(doc, "q1");
    Properties props = new Properties();
    props.setProperty("capacity", "70");
    props.setProperty("maxCapacity", "35");
    q1.appendChild(createProperties(doc, props));
    queues.appendChild(q1);

    // Create another parent level p1
    Element p1 = createQueue(doc, "p1");

    // append child p11 to p1
    Element p11 = createQueue(doc, "p11");
    p11.appendChild(createState(doc, QueueState.STOPPED.getStateName()));
    p1.appendChild(p11);

    Element p12 = createQueue(doc, "p12");

    p12.appendChild(createState(doc, QueueState.RUNNING.getStateName()));
    p12.appendChild(createAcls(doc, "acl-submit-job", "u3"));
    p12.appendChild(createAcls(doc, "acl-administer-jobs", "u4"));

    // append p12 to p1.
    p1.appendChild(p12);

    queues.appendChild(p1);
  }

  /**
   * Create the root <queues></queues> element along with the
   * <aclsEnabled></aclsEnabled> element.
   * 
   * @param doc
   * @param enable
   * @return the created element.
   */
  public static Element createQueuesNode(Document doc) {
    Element queues = doc.createElement("queues");
    doc.appendChild(queues);
    return queues;
  }

  public static void writeToFile(Document doc, String filePath)
      throws TransformerException {
    Transformer trans = TransformerFactory.newInstance().newTransformer();
    trans.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION, "yes");
    trans.setOutputProperty(OutputKeys.INDENT, "yes");
    DOMSource source = new DOMSource(doc);
    trans.transform(source, new StreamResult(new File(filePath)));
  }

  public static Element createQueue(Document doc, String name) {
    Element queue = doc.createElement("queue");
    Element nameNode = doc.createElement("name");
    nameNode.setTextContent(name);
    queue.appendChild(nameNode);
    return queue;
  }

  public static Element createAcls(Document doc, String aclName,
      String listNames) {
    Element acls = doc.createElement(aclName);
    acls.setTextContent(listNames);
    return acls;
  }

  public static Element createState(Document doc, String state) {
    Element stateElement = doc.createElement("state");
    stateElement.setTextContent(state);
    return stateElement;
  }

  public static Element createProperties(Document doc, Properties props) {
    Element propsElement = doc.createElement("properties");
    if (props != null) {
      Set<String> propList = props.stringPropertyNames();
      for (String prop : propList) {
        Element property = doc.createElement("property");
        property.setAttribute("key", prop);
        property.setAttribute("value", (String) props.get(prop));
        propsElement.appendChild(property);
      }
    }
    return propsElement;
  }

  /**
   * Delete queues configuration file if exists
   */
  public static void deleteQueuesConfigFile() {
    if (new File(QUEUES_CONFIG_FILE_PATH).exists()) {
      new File(QUEUES_CONFIG_FILE_PATH).delete();
    }
  }

  /**
   * Write the given queueHierarchy to the given file.
   * 
   * @param filePath
   * 
   * @param rootQueues
   * @throws Exception
   */
  public static void writeQueueConfigurationFile(String filePath,
      JobQueueInfo[] rootQueues) throws Exception {
    Document doc = createDocument();
    Element queueElements = createQueuesNode(doc);
    for (JobQueueInfo rootQ : rootQueues) {
      queueElements.appendChild(QueueConfigurationParser.getQueueElement(doc,
          rootQ));
    }
    writeToFile(doc, filePath);
  }

  static Job submitSleepJob(final int numMappers, final int numReducers, final long mapSleepTime,
      final long reduceSleepTime, boolean shouldComplete, String userInfo,
      String queueName, Configuration clientConf) throws IOException,
      InterruptedException, ClassNotFoundException {
    clientConf.set(MRConfig.FRAMEWORK_NAME, MRConfig.CLASSIC_FRAMEWORK_NAME);
    clientConf.set(JTConfig.JT_IPC_ADDRESS, "localhost:"
        + miniMRCluster.getJobTrackerPort());
    UserGroupInformation ugi;
    if (userInfo != null) {
      String[] splits = userInfo.split(",");
      String[] groups = new String[splits.length - 1];
      System.arraycopy(splits, 1, groups, 0, splits.length - 1);
      ugi = UserGroupInformation.createUserForTesting(splits[0], groups);
    } else {
      ugi = UserGroupInformation.getCurrentUser();
    }
    if (queueName != null) {
      clientConf.set(JobContext.QUEUE_NAME, queueName);
    }
    final SleepJob sleep = new SleepJob();
    sleep.setConf(clientConf);
    
    Job job = ugi.doAs(new PrivilegedExceptionAction<Job>() {
        public Job run() throws IOException {
          return sleep.createJob(numMappers, numReducers, mapSleepTime,
              (int) mapSleepTime, reduceSleepTime, (int) reduceSleepTime);
      }});
    if (shouldComplete) {
      job.waitForCompletion(false);
    } else {
      job.submit();
      // miniMRCluster.getJobTrackerRunner().getJobTracker().jobsToComplete()[]
      Cluster cluster = new Cluster(miniMRCluster.createJobConf());
      JobStatus[] status = miniMRCluster.getJobTrackerRunner().getJobTracker()
          .jobsToComplete();
      JobID id = status[status.length -1].getJobID();
      Job newJob = cluster.getJob(id);
      cluster.close();
      return newJob;
    }
    return job;
  }

  static MiniMRCluster miniMRCluster;
}
