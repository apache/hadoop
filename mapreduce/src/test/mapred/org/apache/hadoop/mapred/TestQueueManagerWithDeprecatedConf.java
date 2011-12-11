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

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;

import javax.security.auth.login.LoginException;

import junit.framework.TestCase;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.MRConfig;
import org.apache.hadoop.mapreduce.server.jobtracker.JTConfig;
import org.apache.hadoop.mapreduce.QueueState;
import org.apache.hadoop.mapreduce.server.jobtracker.TaskTracker;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.mapreduce.SleepJob;
import org.apache.hadoop.security.UserGroupInformation;
import static org.apache.hadoop.mapred.DeprecatedQueueConfigurationParser.*;
import static org.apache.hadoop.mapred.QueueManagerTestUtils.*;
import static org.apache.hadoop.mapred.QueueManager.toFullPropertyName;

public class TestQueueManagerWithDeprecatedConf extends TestCase {

  static final Log LOG = LogFactory.getLog(
      TestQueueManagerWithDeprecatedConf.class);

  String submitAcl = QueueACL.SUBMIT_JOB.getAclName();
  String adminAcl  = QueueACL.ADMINISTER_JOBS.getAclName();

  
  public void testMultipleQueues() {
    JobConf conf = new JobConf();
    conf.set(DeprecatedQueueConfigurationParser.MAPRED_QUEUE_NAMES_KEY,
        "q1,q2,Q3");
    QueueManager qMgr = new QueueManager(conf);
    Set<String> expQueues = new TreeSet<String>();
    expQueues.add("q1");
    expQueues.add("q2");
    expQueues.add("Q3");
    verifyQueues(expQueues, qMgr.getLeafQueueNames());
  }

  public void testSchedulerInfo() {
    JobConf conf = new JobConf();
    conf.set(DeprecatedQueueConfigurationParser.MAPRED_QUEUE_NAMES_KEY,
        "qq1,qq2");
    QueueManager qMgr = new QueueManager(conf);
    qMgr.setSchedulerInfo("qq1", "queueInfoForqq1");
    qMgr.setSchedulerInfo("qq2", "queueInfoForqq2");
    assertEquals(qMgr.getSchedulerInfo("qq2"), "queueInfoForqq2");
    assertEquals(qMgr.getSchedulerInfo("qq1"), "queueInfoForqq1");
  }


  public void testQueueManagerWithDeprecatedConf() throws IOException {
    String queueConfigPath =
      System.getProperty("test.build.extraconf", "build/test/extraconf");

    File hadoopConfigFile = new File(queueConfigPath, "mapred-site.xml");
    try {
      // queue properties with which the cluster is started.
      Properties hadoopConfProps = new Properties();
      hadoopConfProps.put(DeprecatedQueueConfigurationParser.
          MAPRED_QUEUE_NAMES_KEY, "default,q1,q2");
      hadoopConfProps.put(MRConfig.MR_ACLS_ENABLED, "true");

      UserGroupInformation ugi =
        UserGroupInformation.createRemoteUser("unknownUser");
      hadoopConfProps.put(toFullPropertyName(
          "default", submitAcl), ugi.getUserName());
      hadoopConfProps.put(toFullPropertyName(
          "q1", submitAcl), "u1");
      hadoopConfProps.put(toFullPropertyName(
          "q2", submitAcl), "*");
      hadoopConfProps.put(toFullPropertyName(
          "default", adminAcl), ugi.getUserName());
      hadoopConfProps.put(toFullPropertyName(
          "q1", adminAcl), "u2");
      hadoopConfProps.put(toFullPropertyName(
          "q2", adminAcl), "*");

      UtilsForTests.setUpConfigFile(hadoopConfProps, hadoopConfigFile);

      Configuration conf = new JobConf();
      conf.setBoolean(MRConfig.MR_ACLS_ENABLED, true);
      QueueManager queueManager = new QueueManager(conf);
      //Testing job submission access to queues.
      assertTrue("User Job Submission failed.",
          queueManager.hasAccess("default", QueueACL.
              SUBMIT_JOB, ugi));
      assertFalse("User Job Submission failed.",
          queueManager.hasAccess("q1", QueueACL.
              SUBMIT_JOB, ugi));
      assertTrue("User Job Submission failed.",
          queueManager.hasAccess("q2", QueueACL.
              SUBMIT_JOB, ugi));
      //Testing the administer-jobs acls
      assertTrue("User Job Submission failed.",
          queueManager.hasAccess("default",
              QueueACL.ADMINISTER_JOBS, ugi));
      assertFalse("User Job Submission failed.",
          queueManager.hasAccess("q1", QueueACL.
              ADMINISTER_JOBS, ugi));
      assertTrue("User Job Submission failed.",
          queueManager.hasAccess("q2", QueueACL.
              ADMINISTER_JOBS, ugi));
 
    } finally {
      //Cleanup the configuration files in all cases
      if(hadoopConfigFile.exists()) {
        hadoopConfigFile.delete();
      }
    }
  }

  private void verifyQueues(Set<String> expectedQueues, 
                                          Set<String> actualQueues) {
    assertEquals(expectedQueues.size(), actualQueues.size());
    for (String queue : expectedQueues) {
      assertTrue(actualQueues.contains(queue));
    }
  }

}
