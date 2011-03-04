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
import java.io.PrintWriter;
import java.io.Writer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Set;
import java.util.TreeSet;
import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.SecurityUtil.AccessControlList;
import org.apache.hadoop.util.StringUtils;

/**
 * Class that exposes information about queues maintained by the Hadoop
 * Map/Reduce framework.
 * 
 * The Map/Reduce framework can be configured with one or more queues,
 * depending on the scheduler it is configured with. While some 
 * schedulers work only with one queue, some schedulers support multiple 
 * queues.
 *  
 * Queues can be configured with various properties. Some of these
 * properties are common to all schedulers, and those are handled by this
 * class. Schedulers might also associate several custom properties with 
 * queues. Where such a case exists, the queue name must be used to link 
 * the common properties with the scheduler specific ones.  
 */
class QueueManager {
  
  private static final Log LOG = LogFactory.getLog(QueueManager.class);
  
  // Prefix in configuration for queue related keys
  private static final String QUEUE_CONF_PROPERTY_NAME_PREFIX 
                                                        = "mapred.queue.";
  // Configured queues
  private Set<String> queueNames;
  // Map of a queue and ACL property name with an ACL
  private HashMap<String, AccessControlList> aclsMap;
  // Map of a queue name to any generic object that represents 
  // scheduler information 
  private HashMap<String, Object> schedulerInfoObjects;
  // Whether ACLs are enabled in the system or not.
  private boolean aclsEnabled;
  
  //Resource in which queue acls are configured.
  static final String QUEUE_ACLS_FILE_NAME = "mapred-queue-acls.xml";
  
  /**
   * Enum representing an operation that can be performed on a queue.
   */
  static enum QueueOperation {
    SUBMIT_JOB ("acl-submit-job", false),
    ADMINISTER_JOBS ("acl-administer-jobs", true);
    // TODO: Add ACL for LIST_JOBS when we have ability to authenticate 
    //       users in UI
    // TODO: Add ACL for CHANGE_ACL when we have an admin tool for 
    //       configuring queues.
    
    private final String aclName;
    private final boolean jobOwnerAllowed;
    
    QueueOperation(String aclName, boolean jobOwnerAllowed) {
      this.aclName = aclName;
      this.jobOwnerAllowed = jobOwnerAllowed;
    }

    final String getAclName() {
      return aclName;
    }
    
    final boolean isJobOwnerAllowed() {
      return jobOwnerAllowed;
    }
  }
  
  /**
   * Construct a new QueueManager using configuration specified in the passed
   * in {@link org.apache.hadoop.conf.Configuration} object.
   * 
   * @param conf Configuration object where queue configuration is specified.
   */
  public QueueManager(Configuration conf) {
    queueNames = new TreeSet<String>();
    aclsMap = new HashMap<String, AccessControlList>();
    schedulerInfoObjects = new HashMap<String, Object>();
    initialize(conf);
  }
  
  /**
   * Return the set of queues configured in the system.
   * 
   * The number of queues configured should be dependent on the Scheduler 
   * configured. Note that some schedulers work with only one queue, whereas
   * others can support multiple queues.
   *  
   * @return Set of queue names.
   */
  public synchronized Set<String> getQueues() {
    return queueNames;
  }
  
  /**
   * Return true if the given {@link QueueManager.QueueOperation} can be 
   * performed by the specified user on the given queue.
   * 
   * An operation is allowed if all users are provided access for this
   * operation, or if either the user or any of the groups specified is
   * provided access.
   * 
   * @param queueName Queue on which the operation needs to be performed. 
   * @param oper The operation to perform
   * @param ugi The user and groups who wish to perform the operation.
   * 
   * @return true if the operation is allowed, false otherwise.
   */
  public synchronized boolean hasAccess(String queueName, QueueOperation oper,
                                UserGroupInformation ugi) {
    return hasAccess(queueName, null, oper, ugi);
  }
  
  /**
   * Return true if the given {@link QueueManager.QueueOperation} can be 
   * performed by the specified user on the specified job in the given queue.
   * 
   * An operation is allowed either if the owner of the job is the user 
   * performing the task, all users are provided access for this
   * operation, or if either the user or any of the groups specified is
   * provided access.
   * 
   * If the {@link QueueManager.QueueOperation} is not job specific then the 
   * job parameter is ignored.
   * 
   * @param queueName Queue on which the operation needs to be performed.
   * @param job The {@link JobInProgress} on which the operation is being
   *            performed. 
   * @param oper The operation to perform
   * @param ugi The user and groups who wish to perform the operation.
   * 
   * @return true if the operation is allowed, false otherwise.
   */
  public synchronized boolean hasAccess(String queueName, JobInProgress job, 
                                QueueOperation oper, 
                                UserGroupInformation ugi) {
    if (!aclsEnabled) {
      return true;
    }
    
    if (LOG.isDebugEnabled()) {
      LOG.debug("checking access for : " + toFullPropertyName(queueName, 
                                            oper.getAclName()));      
    }
    
    if (oper.isJobOwnerAllowed()) {
      if (job != null && job.getJobConf().getUser().equals(ugi.getUserName())) {
        return true;
      }
    }
    
    AccessControlList acl = aclsMap.get(toFullPropertyName(queueName, oper.getAclName()));
    if (acl == null) {
      return false;
    }
    
    // Check the ACL list
    boolean allowed = acl.allAllowed();
    if (!allowed) {
      // Check the allowed users list
      if (acl.getUsers().contains(ugi.getUserName())) {
        allowed = true;
      } else {
        // Check the allowed groups list
        Set<String> allowedGroups = acl.getGroups();
        for (String group : ugi.getGroupNames()) {
          if (allowedGroups.contains(group)) {
            allowed = true;
            break;
          }
        }
      }
    }
    
    return allowed;    
  }
  
  /**
   * Set a generic Object that represents scheduling information relevant
   * to a queue.
   * 
   * A string representation of this Object will be used by the framework
   * to display in user facing applications like the JobTracker web UI and
   * the hadoop CLI.
   * 
   * @param queueName queue for which the scheduling information is to be set. 
   * @param queueInfo scheduling information for this queue.
   */
  public synchronized void setSchedulerInfo(String queueName, 
                                              Object queueInfo) {
    schedulerInfoObjects.put(queueName, queueInfo);
  }
  
  /**
   * Return the scheduler information configured for this queue.
   * 
   * @param queueName queue for which the scheduling information is required.
   * @return The scheduling information for this queue.
   * 
   * @see #setSchedulerInfo(String, Object)
   */
  public synchronized Object getSchedulerInfo(String queueName) {
    return schedulerInfoObjects.get(queueName);
  }
  
  /**
   * Refresh the acls for the configured queues in the system by reading
   * it from mapred-queue-acls.xml.
   * 
   * The previous acls are removed. Previously configured queues and
   * if or not acl is disabled is retained.
   * 
   * @throws IOException when queue ACL configuration file is invalid.
   */
  synchronized void refreshAcls(Configuration conf) throws IOException {
    try {
      HashMap<String, AccessControlList> newAclsMap = 
        getQueueAcls(conf);
      aclsMap = newAclsMap;
    } catch (Throwable t) {
      String exceptionString = StringUtils.stringifyException(t);
      LOG.warn("Queue ACLs could not be refreshed because there was an " +
      		"exception in parsing the configuration: "+ exceptionString +
      		". Existing ACLs are retained.");
      throw new IOException(exceptionString);
    }

  }
  
  private void checkDeprecation(Configuration conf) {
    for(String queue: queueNames) {
      for (QueueOperation oper : QueueOperation.values()) {
        String key = toFullPropertyName(queue, oper.getAclName());
        String aclString = conf.get(key);
        if(aclString != null) {
          LOG.warn("Configuring queue ACLs in mapred-site.xml or " +
          		"hadoop-site.xml is deprecated. Configure queue ACLs in " + 
          		QUEUE_ACLS_FILE_NAME);
          return;
        }
      }
    }
  }
  
  private HashMap<String, AccessControlList> getQueueAcls(Configuration conf)  {
    checkDeprecation(conf);
    conf.addResource(QUEUE_ACLS_FILE_NAME);
    HashMap<String, AccessControlList> aclsMap = 
      new HashMap<String, AccessControlList>();
    for (String queue : queueNames) {
      for (QueueOperation oper : QueueOperation.values()) {
        String key = toFullPropertyName(queue, oper.getAclName());
        String aclString = conf.get(key, "*");
        aclsMap.put(key, new AccessControlList(aclString));
      }
    } 
    return aclsMap;
  }
  
  private void initialize(Configuration conf) {
    aclsEnabled = conf.getBoolean("mapred.acls.enabled", false);
    String[] queues = conf.getStrings("mapred.queue.names", 
        new String[] {JobConf.DEFAULT_QUEUE_NAME});
    addToSet(queueNames, queues);
    aclsMap = getQueueAcls(conf);
  }
  
  private static final String toFullPropertyName(String queue, 
      String property) {
    return QUEUE_CONF_PROPERTY_NAME_PREFIX + queue + "." + property;
  }
  
  private static final void addToSet(Set<String> set, String[] elems) {
    for (String elem : elems) {
      set.add(elem);
    }
  }
  
  synchronized JobQueueInfo[] getJobQueueInfos() {
    ArrayList<JobQueueInfo> queueInfoList = new ArrayList<JobQueueInfo>();
    for(String queue : queueNames) {
      Object schedulerInfo = schedulerInfoObjects.get(queue);
      if(schedulerInfo != null) {
        queueInfoList.add(new JobQueueInfo(queue,schedulerInfo.toString()));
      }else {
        queueInfoList.add(new JobQueueInfo(queue,null));
      }
    }
    return (JobQueueInfo[]) queueInfoList.toArray(new JobQueueInfo[queueInfoList
        .size()]);
  }

  JobQueueInfo getJobQueueInfo(String queue) {
    Object schedulingInfo = schedulerInfoObjects.get(queue);
    if(schedulingInfo!=null){
      return new JobQueueInfo(queue,schedulingInfo.toString());
    }else {
      return new JobQueueInfo(queue,null);
    }
  }

  /**
   * Generates the array of QueueAclsInfo object. The array consists of only those queues
   * for which user <ugi.getUserName()> has acls
   *
   * @return QueueAclsInfo[]
   * @throws java.io.IOException
   */
  synchronized QueueAclsInfo[] getQueueAcls(UserGroupInformation
          ugi) throws IOException {
    //List of all QueueAclsInfo objects , this list is returned
    ArrayList<QueueAclsInfo> queueAclsInfolist =
            new ArrayList<QueueAclsInfo>();
    QueueOperation[] operations = QueueOperation.values();
    for (String queueName : queueNames) {
      QueueAclsInfo queueAclsInfo = null;
      ArrayList<String> operationsAllowed = null;
      for (QueueOperation operation : operations) {
        if (hasAccess(queueName, operation, ugi)) {
          if (operationsAllowed == null) {
            operationsAllowed = new ArrayList<String>();
          }
          operationsAllowed.add(operation.getAclName());
        }
      }
      if (operationsAllowed != null) {
        //There is atleast 1 operation supported for queue <queueName>
        //, hence initialize queueAclsInfo
        queueAclsInfo = new QueueAclsInfo(queueName, operationsAllowed.toArray
                (new String[operationsAllowed.size()]));
        queueAclsInfolist.add(queueAclsInfo);
      }
    }
    return queueAclsInfolist.toArray(new QueueAclsInfo[
            queueAclsInfolist.size()]);
  }

  /**
   * prints the configuration of QueueManager in Json format.
   * The method should be modified accordingly whenever
   * QueueManager(Configuration) constructor is modified.
   * @param writer {@link}Writer object to which the configuration properties 
   * are printed in json format
   * @throws IOException
   */
  static void dumpConfiguration(Writer writer) throws IOException {
    Configuration conf = new Configuration(false);
    conf.addResource(QUEUE_ACLS_FILE_NAME);
    Configuration.dumpConfiguration(conf, writer);
  }

}
