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

import java.io.IOException;
import junit.framework.TestCase;

import org.apache.hadoop.mapreduce.MRConfig;

import static org.apache.hadoop.mapred.QueueManagerTestUtils.*;
import org.apache.hadoop.security.UserGroupInformation;

/**
 * Unit test class to test queue acls
 *
 */
public class TestQueueAclsForCurrentUser extends TestCase {

  private QueueManager queueManager;
  private JobConf conf = null;
  UserGroupInformation currentUGI = null;
  String submitAcl = QueueACL.SUBMIT_JOB.getAclName();
  String adminAcl  = QueueACL.ADMINISTER_JOBS.getAclName();

  @Override
  protected void tearDown() {
    deleteQueuesConfigFile();
  }

  // No access for queues for the user currentUGI
  private void setupConfForNoAccess() throws Exception {
    currentUGI = UserGroupInformation.getLoginUser();
    String userName = currentUGI.getUserName();

    String[] queueNames = {"qu1", "qu2"};
    // Only user u1 has access for queue qu1
    // Only group g2 has acls for the queue qu2
    createQueuesConfigFile(
        queueNames, new String[]{"u1", " g2"}, new String[]{"u1", " g2"});

    conf = new JobConf();
    conf.setBoolean(MRConfig.MR_ACLS_ENABLED, true);

    queueManager = new QueueManager(conf);
  }

  /**
   *  sets up configuration for acls test.
   * @return
   */
  private void setupConf(boolean aclSwitch) throws Exception{
    currentUGI = UserGroupInformation.getLoginUser();
    String userName = currentUGI.getUserName();
    StringBuilder groupNames = new StringBuilder("");
    String[] ugiGroupNames = currentUGI.getGroupNames();
    int max = ugiGroupNames.length-1;
    for(int j=0;j< ugiGroupNames.length;j++) {
      groupNames.append(ugiGroupNames[j]);
      if(j<max) {
        groupNames.append(",");
      }
    }
    String groupsAcl = " " + groupNames.toString();

    //q1 Has acls for all the users, supports both submit and administer
    //q2 only u2 has acls for the queues
    //q3  Only u2 has submit operation access rest all have administer access
    //q4 Only u2 has administer access , anyone can do submit
    //qu5 only current user's groups has access
    //qu6 only current user has submit access
    //qu7 only current user has administrator access
    String[] queueNames =
        {"qu1", "qu2", "qu3", "qu4", "qu5", "qu6", "qu7"};
    String[] submitAcls =
        {"*", "u2", "u2", "*", groupsAcl, userName, "u2"};
    String[] adminsAcls =
        {"*", "u2", "*", "u2", groupsAcl, "u2", userName};
    createQueuesConfigFile(queueNames, submitAcls, adminsAcls);

    conf = new JobConf();
    conf.setBoolean(MRConfig.MR_ACLS_ENABLED, aclSwitch);

    queueManager = new QueueManager(conf);
  }

  public void testQueueAclsForCurrentuser() throws Exception {
    setupConf(true);
    QueueAclsInfo[] queueAclsInfoList =
            queueManager.getQueueAcls(currentUGI);
    checkQueueAclsInfo(queueAclsInfoList);
  }

  // Acls are disabled on the mapreduce cluster
  public void testQueueAclsForCurrentUserAclsDisabled() throws Exception {
    setupConf(false);
    //fetch the acls info for current user.
    QueueAclsInfo[] queueAclsInfoList = queueManager.
            getQueueAcls(currentUGI);
    checkQueueAclsInfo(queueAclsInfoList);
  }

  public void testQueueAclsForNoAccess() throws Exception {
    setupConfForNoAccess();
    QueueAclsInfo[] queueAclsInfoList = queueManager.
            getQueueAcls(currentUGI);
    assertTrue(queueAclsInfoList.length == 0);
  }

  private void checkQueueAclsInfo(QueueAclsInfo[] queueAclsInfoList)
          throws IOException {
    if (conf.get(MRConfig.MR_ACLS_ENABLED).equalsIgnoreCase("true")) {
      for (int i = 0; i < queueAclsInfoList.length; i++) {
        QueueAclsInfo acls = queueAclsInfoList[i];
        String queueName = acls.getQueueName();
        assertFalse(queueName.contains("qu2"));
        if (queueName.equals("qu1")) {
          assertTrue(acls.getOperations().length == 2);
          assertTrue(checkAll(acls.getOperations()));
        } else if (queueName.equals("qu3")) {
          assertTrue(acls.getOperations().length == 1);
          assertTrue(acls.getOperations()[0].equalsIgnoreCase(adminAcl));
        } else if (queueName.equals("qu4")) {
          assertTrue(acls.getOperations().length == 1);
          assertTrue(acls.getOperations()[0].equalsIgnoreCase(submitAcl));
        } else if (queueName.equals("qu5")) {
          assertTrue(acls.getOperations().length == 2);
          assertTrue(checkAll(acls.getOperations()));
        } else if(queueName.equals("qu6")) {
          assertTrue(acls.getOperations()[0].equals(submitAcl));
        } else if(queueName.equals("qu7")) {
          assertTrue(acls.getOperations()[0].equals(adminAcl));
        } 
      }
    } else {
      for (int i = 0; i < queueAclsInfoList.length; i++) {
        QueueAclsInfo acls = queueAclsInfoList[i];
        String queueName = acls.getQueueName();
        assertTrue(acls.getOperations().length == 2);
        assertTrue(checkAll(acls.getOperations()));
      }
    }
  }

  private boolean checkAll(String[] operations){
    boolean submit = false;
    boolean admin = false;

    for(String val: operations){
      if(val.equalsIgnoreCase(submitAcl))
        submit = true;
      else if(val.equalsIgnoreCase(adminAcl))
        admin = true;
    }
    if(submit && admin) return true;
    return false;
  }
}
